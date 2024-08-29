#![allow(warnings)]
use clap::{Parser, Subcommand};
use intervaltree::IntervalTree;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::PathBuf;
use std::{io, str};

use gen::migrations::run_migrations;
use gen::models::{
    self,
    block_group::{BlockGroup, BlockGroupData, PathCache, PathChange},
    block_group_edge::BlockGroupEdge,
    edge::Edge,
    path::{NewBlock, Path, PathData},
    path_edge::PathEdge,
    sequence::{NewSequence, Sequence},
};
use gen::{get_connection, parse_genotype};
use noodles::fasta;
use noodles::vcf;
use noodles::vcf::variant::record::samples::series::value::genotype::Phasing;
use noodles::vcf::variant::record::samples::series::Value;
use noodles::vcf::variant::record::samples::{Sample, Series};
use noodles::vcf::variant::record::{AlternateBases, ReferenceBases, Samples};
use noodles::vcf::variant::Record;
use rusqlite::{types::Value as SQLValue, Connection};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Import a new sequence collection.
    Import {
        /// Fasta file path
        #[arg(short, long)]
        fasta: String,
        /// The name of the collection to store the entry under
        #[arg(short, long)]
        name: String,
        /// The path to the database you wish to utilize
        #[arg(short, long)]
        db: String,
        /// Don't store the sequence in the database, instead store the filename
        #[arg(short, long, action)]
        shallow: bool,
    },
    /// Update a sequence collection with new data
    Update {
        /// The name of the collection to update
        #[arg(short, long)]
        name: String,
        /// The path to the database you wish to utilize
        #[arg(short, long)]
        db: String,
        /// A fasta file to insert
        #[arg(short, long)]
        fasta: Option<String>,
        /// A VCF file to incorporate
        #[arg(short, long)]
        vcf: String,
        /// If no genotype is provided, enter the genotype to assign variants
        #[arg(short, long)]
        genotype: Option<String>,
        /// If no sample is provided, enter the sample to associate variants to
        #[arg(short, long)]
        sample: Option<String>,
    },
}

fn import_fasta(fasta: &String, name: &str, shallow: bool, conn: &mut Connection) {
    // TODO: support gz
    let mut reader = fasta::io::reader::Builder.build_from_path(fasta).unwrap();

    if !models::Collection::exists(conn, name) {
        let collection = models::Collection::create(conn, name);

        for result in reader.records() {
            let record = result.expect("Error during fasta record parsing");
            let sequence = str::from_utf8(record.sequence().as_ref())
                .unwrap()
                .to_string();
            let name = String::from_utf8(record.name().to_vec()).unwrap();
            let sequence_length = record.sequence().len() as i32;
            let seq_hash = if shallow {
                Sequence::new()
                    .sequence_type("DNA")
                    .name(&name)
                    .file_path(fasta)
                    .save(conn)
            } else {
                Sequence::new()
                    .sequence_type("DNA")
                    .sequence(&sequence)
                    .save(conn)
            };
            let block_group = BlockGroup::create(conn, &collection.name, None, &name);
            let edge_into = Edge::create(
                conn,
                Edge::PATH_START_HASH.to_string(),
                0,
                "+".to_string(),
                seq_hash.to_string(),
                0,
                "+".to_string(),
                0,
                0,
            );
            let edge_out_of = Edge::create(
                conn,
                seq_hash.to_string(),
                sequence_length,
                "+".to_string(),
                Edge::PATH_END_HASH.to_string(),
                0,
                "+".to_string(),
                0,
                0,
            );
            BlockGroupEdge::bulk_create(conn, block_group.id, vec![edge_into.id, edge_out_of.id]);
            Path::create(
                conn,
                &name,
                block_group.id,
                vec![edge_into.id, edge_out_of.id],
            );
        }
        println!("Created it");
    } else {
        println!("Collection {:1} already exists", name);
    }
}

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
        name: &str,
    ) -> i32 {
        let block_group_key = BlockGroupData {
            collection_name,
            sample_name: Some(sample_name),
            name: name.to_string(),
        };
        let block_group_lookup = block_group_cache.cache.get(&block_group_key);
        if let Some(block_group_id) = block_group_lookup {
            *block_group_id
        } else {
            let new_block_group_id = BlockGroup::get_or_create_sample_block_group(
                block_group_cache.conn,
                collection_name,
                sample_name,
                name,
            );
            block_group_cache
                .cache
                .insert(block_group_key, new_block_group_id);
            new_block_group_id
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_change(
    conn: &Connection,
    sample_bg_id: i32,
    sample_path: &Path,
    alt_seq: &str,
    ref_start: i32,
    ref_end: i32,
    chromosome_index: i32,
    phased: i32,
) -> PathChange {
    // TODO: new sequence may not be real and be <DEL> or some sort. Handle these.
    let new_sequence_hash = Sequence::new()
        .sequence_type("DNA")
        .sequence(alt_seq)
        .save(conn);
    let sequence = Sequence::sequence_from_hash(conn, &new_sequence_hash).unwrap();
    let new_block = NewBlock {
        id: 0,
        sequence: sequence.clone(),
        block_sequence: alt_seq.to_string(),
        sequence_start: 0,
        sequence_end: alt_seq.len() as i32,
        path_start: ref_start,
        path_end: ref_end,
        strand: "+".to_string(),
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

fn update_with_vcf(
    vcf_path: &String,
    collection_name: &str,
    fixed_genotype: String,
    fixed_sample: String,
    conn: &mut Connection,
) {
    run_migrations(conn);

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

    let mut changes: Vec<PathChange> = vec![];

    for result in reader.records() {
        let record = result.unwrap();
        let seq_name: String = record.reference_sequence_name().to_string();
        let ref_allele = record.reference_bases();
        // this converts the coordinates to be zero based, start inclusive, end exclusive
        let ref_start = record.variant_start().unwrap().unwrap().get() - 1;
        let ref_end = record.variant_end(&header).unwrap().get();
        let alt_bases = record.alternate_bases();
        let alt_alleles: Vec<_> = alt_bases.iter().collect::<io::Result<_>>().unwrap();
        // TODO: fix this duplication of handling an insert
        if !fixed_sample.is_empty() && !genotype.is_empty() {
            let sample_bg_id = BlockGroupCache::lookup(
                &mut block_group_cache,
                collection_name,
                &fixed_sample,
                &seq_name,
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
                        let change = prepare_change(
                            conn,
                            sample_bg_id,
                            &sample_path,
                            alt_seq,
                            ref_start as i32,
                            ref_end as i32,
                            chromosome_index as i32,
                            phased,
                        );
                        changes.push(change);
                    }
                }
            }
        } else {
            for (sample_index, sample) in record.samples().iter().enumerate() {
                let sample_bg_id = BlockGroupCache::lookup(
                    &mut block_group_cache,
                    collection_name,
                    &sample_names[sample_index],
                    &seq_name,
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
                                    let change = prepare_change(
                                        conn,
                                        sample_bg_id,
                                        &sample_path,
                                        alt_seq,
                                        ref_start as i32,
                                        ref_end as i32,
                                        chromosome_index as i32,
                                        phased,
                                    );
                                    changes.push(change);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    BlockGroup::insert_changes(conn, &changes, &path_cache);
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Import {
            fasta,
            name,
            db,
            shallow,
        }) => {
            let mut conn = get_connection(db);
            conn.execute("BEGIN TRANSACTION", []).unwrap();
            import_fasta(fasta, name, *shallow, &mut conn);
            conn.execute("END TRANSACTION", []).unwrap();
        }
        Some(Commands::Update {
            name,
            db,
            fasta,
            vcf,
            genotype,
            sample,
        }) => {
            let mut conn = get_connection(db);
            conn.execute("BEGIN TRANSACTION", []).unwrap();
            update_with_vcf(
                vcf,
                name,
                genotype.clone().unwrap_or("".to_string()),
                sample.clone().unwrap_or("".to_string()),
                &mut conn,
            );
            conn.execute("END TRANSACTION", []).unwrap();
        }
        None => {}
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use std::fs;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use gen::migrations::run_migrations;

    fn get_connection<'a>(db_path: impl Into<Option<&'a str>>) -> Connection {
        let path: Option<&str> = db_path.into();
        let mut conn;
        if let Some(v) = path {
            if fs::metadata(v).is_ok() {
                fs::remove_file(v).unwrap();
            }
            conn = Connection::open(v).unwrap_or_else(|_| panic!("Error connecting to {}", v));
        } else {
            conn = Connection::open_in_memory()
                .unwrap_or_else(|_| panic!("Error opening in memory test db"));
        }
        rusqlite::vtab::array::load_module(&conn).unwrap();
        run_migrations(&mut conn);
        conn
    }

    #[test]
    fn test_add_fasta() {
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let mut conn = get_connection(None);
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            "test",
            false,
            &mut conn,
        );
        assert_eq!(
            BlockGroup::get_all_sequences(&conn, 1),
            HashSet::from_iter(vec!["ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string()])
        );

        let path = Path::get(&conn, 1);
        assert_eq!(
            Path::sequence(&conn, path),
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string()
        );
    }

    #[test]
    fn test_update_fasta_with_vcf() {
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/simple.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &mut get_connection("test.db");
        let collection = "test".to_string();
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
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
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/general.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &mut get_connection("test.db");
        let collection = "test".to_string();
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "0/1".to_string(),
            "sample 1".to_string(),
            conn,
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
