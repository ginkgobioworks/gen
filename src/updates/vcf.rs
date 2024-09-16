use std::collections::HashMap;
use std::fmt::{format, Debug};
use std::{io, path::PathBuf, str};

use crate::migrations::run_migrations;
use crate::models::operations::OperationState;
use crate::models::{
    self,
    block_group::{BlockGroup, BlockGroupData, PathCache, PathChange},
    file_types::FileTypes,
    metadata,
    operations::{FileAddition, Operation, OperationSummary},
    path::{NewBlock, Path},
    sequence::Sequence,
    strand::Strand,
};
use crate::{config, operation_management, parse_genotype};
use noodles::vcf;
use noodles::vcf::variant::record::samples::series::value::genotype::Phasing;
use noodles::vcf::variant::record::samples::series::Value;
use noodles::vcf::variant::record::samples::Sample;
use noodles::vcf::variant::record::AlternateBases;
use noodles::vcf::variant::Record;
use rusqlite::{session, Connection};

#[derive(Debug)]
struct BlockGroupCache<'a> {
    pub cache: HashMap<BlockGroupData<'a>, i32>,
    pub conn: &'a Connection,
}

impl<'a> BlockGroupCache<'_> {
    pub fn new(conn: &Connection) -> BlockGroupCache {
        BlockGroupCache {
            cache: HashMap::<BlockGroupData, i32>::new(),
            conn,
        }
    }

    pub fn lookup(
        block_group_cache: &mut BlockGroupCache<'a>,
        collection_name: &'a str,
        sample_name: &'a str,
        name: String,
    ) -> i32 {
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
    conn: &Connection,
    sample_bg_id: i32,
    sample_path: &Path,
    ref_start: i32,
    ref_end: i32,
    chromosome_index: i32,
    phased: i32,
    sequence: Sequence,
) -> PathChange {
    // TODO: new sequence may not be real and be <DEL> or some sort. Handle these.
    let new_block = NewBlock {
        id: 0,
        sequence: sequence.clone(),
        block_sequence: sequence.get_sequence(None, None),
        sequence_start: 0,
        sequence_end: sequence.length,
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

struct VcfEntry<'a> {
    block_group_id: i32,
    sample_name: String,
    path: Path,
    alt_seq: &'a str,
    chromosome_index: i32,
    phased: i32,
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
        collection_name,
        "vcf_addition",
        change.id,
    );

    let mut reader = vcf::io::reader::Builder::default()
        .build_from_path(vcf_path)
        .expect("Unable to parse");
    let header = reader.read_header().unwrap();
    let sample_names = header.sample_names();
    for name in sample_names {
        models::Sample::create(conn, name);
    }
    if !fixed_sample.is_empty() {
        models::Sample::create(conn, &fixed_sample);
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
        let ref_allele = record.reference_bases();
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
                            chromosome_index: chromosome_index as i32,
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
                                let allele = allele.unwrap();
                                if allele != 0 {
                                    let alt_seq = alt_alleles[allele - 1];
                                    vcf_entries.push(VcfEntry {
                                        block_group_id: sample_bg_id,
                                        path: sample_path.clone(),
                                        sample_name: sample_names[sample_index].clone(),
                                        alt_seq,
                                        chromosome_index: chromosome_index as i32,
                                        phased,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        for vcf_entry in vcf_entries {
            let sequence =
                SequenceCache::lookup(&mut sequence_cache, "DNA", vcf_entry.alt_seq.to_string());
            let change = prepare_change(
                conn,
                vcf_entry.block_group_id,
                &vcf_entry.path,
                ref_start as i32,
                ref_end as i32,
                vcf_entry.chromosome_index,
                vcf_entry.phased,
                sequence,
            );
            changes
                .entry((vcf_entry.path, vcf_entry.sample_name))
                .or_default()
                .push(change);
        }
    }
    let mut summary: HashMap<String, HashMap<String, i32>> = HashMap::new();
    for ((path, sample_name), path_changes) in changes {
        BlockGroup::insert_changes(conn, &path_changes, &path_cache);
        summary
            .entry(sample_name)
            .or_default()
            .entry(path.name)
            .or_insert(path_changes.len() as i32);
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
    operation_management::write_changeset(&operation, &output);
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use std::collections::HashSet;

    #[test]
    fn test_update_fasta_with_vcf() {
        setup_gen_dir();
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/simple.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &get_connection(None);
        let op_conn = &get_operation_connection(None);
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
        assert_eq!(
            BlockGroup::get_all_sequences(conn, 2),
            HashSet::from_iter(vec!["ATCATCGATAGAGATCGATCGGGAACACACAGAGA".to_string()])
        );
        // This individual is homozygous for the first variant and does not contain the second
        assert_eq!(
            BlockGroup::get_all_sequences(conn, 3),
            HashSet::from_iter(vec!["ATCATCGATCGATCGATCGGGAACACACAGAGA".to_string()])
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
        let op_conn = &get_operation_connection(None);
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
}
