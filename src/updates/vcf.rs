use std::collections::HashMap;
use std::fmt::Debug;
use std::{io, str};

use crate::models::{
    block_group::{BlockGroup, BlockGroupData, PathCache, PathChange},
    file_types::FileTypes,
    metadata,
    node::Node,
    operations::{FileAddition, Operation, OperationSummary},
    path::{Path, PathBlock},
    sample::Sample,
    sequence::Sequence,
    strand::Strand,
};
use crate::{calculate_hash, operation_management, parse_genotype};
use noodles::vcf;
use noodles::vcf::variant::record::samples::series::value::genotype::Phasing;
use noodles::vcf::variant::record::samples::series::Value;
use noodles::vcf::variant::record::samples::Sample as NoodlesSample;
use noodles::vcf::variant::record::AlternateBases;
use noodles::vcf::variant::Record;
use rusqlite::{session, Connection};

#[derive(Debug)]
struct BlockGroupCache<'a> {
    pub cache: HashMap<BlockGroupData<'a>, i64>,
    pub conn: &'a Connection,
}

impl<'a> BlockGroupCache<'_> {
    pub fn new(conn: &Connection) -> BlockGroupCache {
        BlockGroupCache {
            cache: HashMap::<BlockGroupData, i64>::new(),
            conn,
        }
    }

    pub fn lookup(
        block_group_cache: &mut BlockGroupCache<'a>,
        collection_name: &'a str,
        sample_name: &'a str,
        name: String,
    ) -> i64 {
        let block_group_key = BlockGroupData {
            collection_name,
            sample_name: Some(sample_name),
            name: name.clone(),
        };
        let block_group_lookup = block_group_cache.cache.get(&block_group_key);
        if let Some(block_group_id) = block_group_lookup {
            *block_group_id
        } else {
            let new_block_group_id = BlockGroup::get_or_create_sample_block_group(
                block_group_cache.conn,
                collection_name,
                sample_name,
                &name,
            );
            block_group_cache
                .cache
                .insert(block_group_key, new_block_group_id);
            new_block_group_id
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SequenceKey<'a> {
    sequence_type: &'a str,
    sequence: String,
}

#[derive(Debug)]
pub struct SequenceCache<'a> {
    pub cache: HashMap<SequenceKey<'a>, Sequence>,
    pub conn: &'a Connection,
}

impl<'a> SequenceCache<'_> {
    pub fn new(conn: &Connection) -> SequenceCache {
        SequenceCache {
            cache: HashMap::<SequenceKey, Sequence>::new(),
            conn,
        }
    }

    pub fn lookup(
        sequence_cache: &mut SequenceCache<'a>,
        sequence_type: &'a str,
        sequence: String,
    ) -> Sequence {
        let sequence_key = SequenceKey {
            sequence_type,
            sequence: sequence.clone(),
        };
        let sequence_lookup = sequence_cache.cache.get(&sequence_key);
        if let Some(found_sequence) = sequence_lookup {
            found_sequence.clone()
        } else {
            let new_sequence = Sequence::new()
                .sequence_type("DNA")
                .sequence(&sequence)
                .save(sequence_cache.conn);

            sequence_cache
                .cache
                .insert(sequence_key, new_sequence.clone());
            new_sequence
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_change(
    sample_bg_id: i64,
    sample_path: &Path,
    ref_start: i64,
    ref_end: i64,
    chromosome_index: i64,
    phased: i64,
    block_sequence: String,
    sequence_length: i64,
    node_id: i64,
) -> PathChange {
    // TODO: new sequence may not be real and be <DEL> or some sort. Handle these.
    let new_block = PathBlock {
        id: 0,
        node_id,
        block_sequence,
        sequence_start: 0,
        sequence_end: sequence_length,
        path_start: ref_start,
        path_end: ref_end,
        strand: Strand::Forward,
    };
    PathChange {
        block_group_id: sample_bg_id,
        path: sample_path.clone(),
        start: ref_start,
        end: ref_end,
        block: new_block,
        chromosome_index,
        phased,
    }
}

#[derive(Debug)]
struct VcfEntry<'a> {
    block_group_id: i64,
    sample_name: String,
    path: Path,
    alt_seq: &'a str,
    chromosome_index: i64,
    phased: i64,
}

pub fn update_with_vcf(
    vcf_path: &String,
    collection_name: &str,
    fixed_genotype: String,
    fixed_sample: String,
    conn: &Connection,
    operation_conn: &Connection,
) {
    let db_uuid = metadata::get_db_uuid(conn);

    let mut session = session::Session::new(conn).unwrap();
    operation_management::attach_session(&mut session);

    let change = FileAddition::create(operation_conn, vcf_path, FileTypes::VCF);
    let operation = Operation::create(
        operation_conn,
        &db_uuid,
        collection_name.to_string(),
        "vcf_addition",
        change.id,
    );

    let mut reader = vcf::io::reader::Builder::default()
        .build_from_path(vcf_path)
        .expect("Unable to parse");
    let header = reader.read_header().unwrap();
    let sample_names = header.sample_names();
    for name in sample_names {
        Sample::create(conn, name);
    }
    if !fixed_sample.is_empty() {
        Sample::create(conn, &fixed_sample);
    }
    let mut genotype = vec![];
    if !fixed_genotype.is_empty() {
        genotype = parse_genotype(&fixed_genotype);
    }

    // Cache a bunch of data ahead of making changes
    let mut block_group_cache = BlockGroupCache::new(conn);
    let mut path_cache = PathCache::new(conn);
    let mut sequence_cache = SequenceCache::new(conn);

    let mut changes: HashMap<(Path, String), Vec<PathChange>> = HashMap::new();

    for result in reader.records() {
        let record = result.unwrap();
        let seq_name: String = record.reference_sequence_name().to_string();
        // this converts the coordinates to be zero based, start inclusive, end exclusive
        let ref_start = record.variant_start().unwrap().unwrap().get() - 1;
        let ref_end = record.variant_end(&header).unwrap().get();
        let alt_bases = record.alternate_bases();
        let alt_alleles: Vec<_> = alt_bases.iter().collect::<io::Result<_>>().unwrap();
        let mut vcf_entries = vec![];

        if !fixed_sample.is_empty() && !genotype.is_empty() {
            let sample_bg_id = BlockGroupCache::lookup(
                &mut block_group_cache,
                collection_name,
                &fixed_sample,
                seq_name.clone(),
            );
            let sample_path = PathCache::lookup(&mut path_cache, sample_bg_id, seq_name.clone());

            for (chromosome_index, genotype) in genotype.iter().enumerate() {
                if let Some(gt) = genotype {
                    if gt.allele != 0 {
                        let alt_seq = alt_alleles[chromosome_index - 1];
                        let phased = match gt.phasing {
                            Phasing::Phased => 1,
                            Phasing::Unphased => 0,
                        };
                        vcf_entries.push(VcfEntry {
                            block_group_id: sample_bg_id,
                            path: sample_path.clone(),
                            sample_name: fixed_sample.clone(),
                            alt_seq,
                            chromosome_index: chromosome_index as i64,
                            phased,
                        });
                    }
                }
            }
        } else {
            for (sample_index, sample) in record.samples().iter().enumerate() {
                let sample_bg_id = BlockGroupCache::lookup(
                    &mut block_group_cache,
                    collection_name,
                    &sample_names[sample_index],
                    seq_name.clone(),
                );
                let sample_path =
                    PathCache::lookup(&mut path_cache, sample_bg_id, seq_name.clone());

                let genotype = sample.get(&header, "GT");
                if genotype.is_some() {
                    if let Value::Genotype(genotypes) = genotype.unwrap().unwrap().unwrap() {
                        for (chromosome_index, gt) in genotypes.iter().enumerate() {
                            if gt.is_ok() {
                                let (allele, phasing) = gt.unwrap();
                                let phased = match phasing {
                                    Phasing::Phased => 1,
                                    Phasing::Unphased => 0,
                                };
                                if let Some(allele) = allele {
                                    if allele != 0 {
                                        let alt_seq = alt_alleles[allele - 1];
                                        vcf_entries.push(VcfEntry {
                                            block_group_id: sample_bg_id,
                                            path: sample_path.clone(),
                                            sample_name: sample_names[sample_index].clone(),
                                            alt_seq,
                                            chromosome_index: chromosome_index as i64,
                                            phased,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        for vcf_entry in vcf_entries {
            // * indicates this allele is removed by another deletion in the sample
            if vcf_entry.alt_seq == "*" {
                continue;
            }
            let sequence =
                SequenceCache::lookup(&mut sequence_cache, "DNA", vcf_entry.alt_seq.to_string());
            let sequence_string = sequence.get_sequence(None, None);
            let node_id = Node::create(
                conn,
                sequence.hash.as_str(),
                format!(
                    "{bg_id}.{path_id}:{ref_start}-{ref_end}->{sequence_hash}",
                    bg_id = vcf_entry.block_group_id,
                    path_id = vcf_entry.path.id,
                    sequence_hash = sequence.hash
                ),
            );
            let change = prepare_change(
                vcf_entry.block_group_id,
                &vcf_entry.path,
                ref_start as i64,
                ref_end as i64,
                vcf_entry.chromosome_index,
                vcf_entry.phased,
                sequence_string.clone(),
                sequence_string.len() as i64,
                node_id,
            );
            changes
                .entry((vcf_entry.path, vcf_entry.sample_name))
                .or_default()
                .push(change);
        }
    }
    let mut summary: HashMap<String, HashMap<String, i64>> = HashMap::new();
    for ((path, sample_name), path_changes) in changes {
        BlockGroup::insert_changes(conn, &path_changes, &path_cache);
        summary
            .entry(sample_name)
            .or_default()
            .entry(path.name)
            .or_insert(path_changes.len() as i64);
    }
    let mut summary_str = "".to_string();
    for (sample_name, sample_changes) in summary.iter() {
        summary_str.push_str(&format!("Sample {sample_name}\n"));
        for (path_name, change_count) in sample_changes.iter() {
            summary_str.push_str(&format!(" {path_name}: {change_count} changes.\n"));
        }
    }
    OperationSummary::create(operation_conn, operation.id, &summary_str);
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    operation_management::write_changeset(conn, &operation, &output);
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::models::node::Node;
    use crate::models::operations::setup_db;
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::time;

    #[test]
    fn test_update_fasta_with_vcf() {
        setup_gen_dir();
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/simple.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        let collection = "test".to_string();

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            op_conn,
        );
        assert_eq!(
            BlockGroup::get_all_sequences(conn, 1),
            HashSet::from_iter(vec!["ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string()])
        );
        // A homozygous set of variants should only return 1 sequence
        // TODO: resolve this case
        // assert_eq!(
        //     BlockGroup::get_all_sequences(conn, 2),
        //     HashSet::from_iter(vec!["ATCATCGATAGAGATCGATCGGGAACACACAGAGA".to_string()])
        // );
        // Blockgroup 3 belongs to the `G1` genotype and has no changes
        assert_eq!(
            BlockGroup::get_all_sequences(conn, 3),
            HashSet::from_iter(vec!["ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string()])
        );
        // This individual is homozygous for the first variant and does not contain the second
        assert_eq!(
            BlockGroup::get_all_sequences(conn, 4),
            HashSet::from_iter(vec![
                "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
                "ATCATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            ])
        );
    }

    #[test]
    fn test_update_fasta_with_vcf_custom_genotype() {
        setup_gen_dir();
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/general.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);
        let collection = "test".to_string();
        let db_uuid = metadata::get_db_uuid(conn);
        setup_db(op_conn, &db_uuid);

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "0/1".to_string(),
            "sample 1".to_string(),
            conn,
            op_conn,
        );
        assert_eq!(
            BlockGroup::get_all_sequences(conn, 1),
            HashSet::from_iter(vec!["ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string()])
        );
        assert_eq!(
            BlockGroup::get_all_sequences(conn, 2),
            HashSet::from_iter(
                [
                    "ATCGATCGATAGAGATCGATCGGGAACACACAGAGA",
                    "ATCATCGATAGAGATCGATCGGGAACACACAGAGA",
                    "ATCGATCGATCGATCGATCGGGAACACACAGAGA",
                    "ATCATCGATCGATCGATCGGGAACACACAGAGA"
                ]
                .iter()
                .map(|v| v.to_string())
            )
        );
    }

    #[test]
    fn test_handles_missing_allele() {
        setup_gen_dir();
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/simple_missing_allele.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);
        let collection = "test".to_string();
        let db_uuid = metadata::get_db_uuid(conn);
        setup_db(op_conn, &db_uuid);

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            op_conn,
        );
        assert_eq!(
            BlockGroup::get_all_sequences(conn, 2),
            HashSet::from_iter(
                [
                    "ATCGATCGATCGATCGATCGGGAACACACAGAGA",
                    "ATCGATCGATAGAGATCGATCGGGAACACACAGAGA",
                ]
                .iter()
                .map(|v| v.to_string())
            )
        );
    }

    #[test]
    fn test_handles_overlap_allele() {
        setup_gen_dir();
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/simple_overlap.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);
        let collection = "test".to_string();
        let db_uuid = metadata::get_db_uuid(conn);
        setup_db(op_conn, &db_uuid);

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            op_conn,
        );
        assert_eq!(
            BlockGroup::get_all_sequences(conn, 2),
            HashSet::from_iter(
                [
                    "ATCGATCGATCGATCGATCGGGAACACACAGAGA",
                    "ATCATCGATCGATCGATCGGGAACACACAGAGA",
                ]
                .iter()
                .map(|v| v.to_string())
            )
        );
    }

    #[test]
    fn test_deduplicates_nodes() {
        setup_gen_dir();
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/simple.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        let collection = "test".to_string();

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );

        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            op_conn,
        );

        let nodes = Node::query(conn, "select * from nodes;", vec![]);
        assert_eq!(nodes.len(), 6);

        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            op_conn,
        );
        let nodes = Node::query(conn, "select * from nodes;", vec![]);
        assert_eq!(nodes.len(), 6);
    }

    #[test]
    fn test_deduplicates_nodes_multiple_paths() {
        setup_gen_dir();
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/multiseq.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/multiseq.fa");
        let conn = &get_connection("test.db");
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        let collection = "test".to_string();

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );

        assert_eq!(Node::query(conn, "select * from nodes;", vec![]).len(), 5);

        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            op_conn,
        );

        let nodes = Node::query(conn, "select * from nodes;", vec![]);
        assert_eq!(nodes.len(), 9);

        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            op_conn,
        );
        let nodes = Node::query(conn, "select * from nodes;", vec![]);
        assert_eq!(nodes.len(), 9);

        let duplicates = Node::query(conn, "SELECT COUNT(*) FROM nodes JOIN edges ON nodes.id = edges.target_node_id GROUP BY source_node_id, source_coordinate, target_coordinate, sequence_hash HAVING COUNT(*) > 1", vec![]);
        assert_eq!(duplicates.len(), 0);

    }

    #[test]
    #[cfg(feature = "benchmark")]
    fn test_vcf_import_benchmark() {
        setup_gen_dir();
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/chr22_100k_no_samples.vcf.gz");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/chr22.fa.gz");
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        let collection = "test".to_string();

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );

        let s = time::Instant::now();
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "0|1".to_string(),
            "test".to_string(),
            conn,
            op_conn,
        );
        assert!(s.elapsed().as_secs() < 20);
    }
}
