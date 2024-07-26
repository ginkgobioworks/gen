#![allow(warnings)]
use clap::{Parser, Subcommand};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::PathBuf;

use bio::io::fasta;
use gen::get_connection;
use gen::migrations::run_migrations;
use gen::models::{self, Block, Path};
use gfa_reader::Gfa;
use noodles::vcf;
use noodles::vcf::variant::record::samples::series::Value;
use noodles::vcf::variant::record::samples::{Sample, Series};
use noodles::vcf::variant::record::{AlternateBases, ReferenceBases, Samples};
use noodles::vcf::variant::Record;
use rusqlite::Connection;
use std::io;

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
    },
}

fn import_fasta(fasta: &String, name: &String, shallow: bool, conn: &mut Connection) {
    let mut reader = fasta::Reader::from_file(fasta).unwrap();

    run_migrations(conn);

    if !models::Collection::exists(conn, name) {
        let collection = models::Collection::create(conn, name);

        for result in reader.records() {
            let record = result.expect("Error during fasta record parsing");
            let sequence = String::from_utf8(record.seq().to_vec()).unwrap();
            let seq_hash = models::Sequence::create(conn, "DNA".to_string(), &sequence, !shallow);
            let path =
                models::Path::create(conn, &collection.name, None, &record.id().to_string(), None);
            let block = Block::create(
                conn,
                &seq_hash,
                path.id,
                0,
                (sequence.len() as i32),
                &"1".to_string(),
            );
            let edge = models::Edge::create(conn, block.id, None);
        }
        println!("Created it");
    } else {
        println!("Collection {:1} already exists", name);
    }
}

fn update_with_vcf(vcf_path: &String, collection_name: &String, conn: &mut Connection) {
    run_migrations(conn);

    let mut reader = vcf::io::reader::Builder::default()
        .build_from_path(vcf_path)
        .expect("Unable to parse");
    let header = reader.read_header().unwrap();
    let sample_names = header.sample_names();
    for name in sample_names {
        models::Sample::create(conn, name);
    }
    for result in reader.records() {
        let record = result.unwrap();
        let seq_name = record.reference_sequence_name().to_string();
        let ref_allele = record.reference_bases();
        // this converts the coordinates to be zero based, start inclusive, end exclusive
        let ref_start = record.variant_start().unwrap().unwrap().get() - 1;
        let ref_end = record.variant_end(&header).unwrap().get();
        let alt_bases = record.alternate_bases();
        let alt_alleles: Vec<_> = alt_bases.iter().collect::<io::Result<_>>().unwrap();
        for (sample_index, sample) in record.samples().iter().enumerate() {
            let genotype = sample.get(&header, "GT");
            let mut seen_haplotypes: HashSet<i32> = HashSet::new();
            if genotype.is_some() {
                if let Value::Genotype(genotypes) = genotype.unwrap().unwrap().unwrap() {
                    for gt in genotypes.iter() {
                        if gt.is_ok() {
                            let (haplotype, phasing) = gt.unwrap();
                            let haplotype = haplotype.unwrap();
                            if haplotype != 0 && !seen_haplotypes.contains(&(haplotype as i32)) {
                                let alt_seq = alt_alleles[haplotype - 1];
                                // TODO: new sequence may not be real and be <DEL> or some sort. Handle these.
                                let new_sequence_hash = models::Sequence::create(
                                    conn,
                                    "DNA".to_string(),
                                    &alt_seq.to_string(),
                                    true,
                                );
                                let sample_path_id = models::Path::get_or_create_sample_path(
                                    conn,
                                    collection_name,
                                    &sample_names[sample_index],
                                    &seq_name,
                                    haplotype as i32,
                                );
                                let new_block_id = Block::create(
                                    conn,
                                    &new_sequence_hash,
                                    sample_path_id,
                                    0,
                                    alt_seq.len() as i32,
                                    &"1".to_string(),
                                );
                                Path::insert_change(
                                    conn,
                                    sample_path_id,
                                    ref_start as i32,
                                    ref_end as i32,
                                    new_block_id.id,
                                );
                            }
                            seen_haplotypes.insert(haplotype as i32);
                        }
                    }
                }
            }
        }
    }
}

fn import_gfa(gfa_path: &str, collection_name: &String, conn: &mut Connection) {
    run_migrations(conn);

    let gfa: Gfa<u64, (), ()> = Gfa::parse_gfa_file(gfa_path);

    let collection = models::Collection::create(conn, collection_name);

    let mut blocks_by_segment_id: HashMap<u64, Block> = HashMap::new();

    for segment in &gfa.segments {
        let sequence = segment.sequence.get_string(&gfa.sequence);
        let seq_hash =
            models::Sequence::create(conn, "DNA".to_string(), &sequence.to_string(), true);
        let block = Block {
            id: 0,
            path_id: 0,
            sequence_hash: seq_hash,
            start: 0,
            end: (sequence.len() as i32),
            strand: "1".to_string(),
        };
        let segment_id = segment.id;
        blocks_by_segment_id.insert(segment_id, block);
    }

    let mut created_blocks_by_segment_id: HashMap<u64, Block> = HashMap::new();

    for input_path in &gfa.paths {
        let path_name = &input_path.name;
        // TODO: Fix Some(1)
        let path = models::Path::create(conn, &collection.name, None, path_name, Some(1));
        for segment_id in input_path.nodes.iter() {
            let block = blocks_by_segment_id.get(segment_id).unwrap();
            let created_block = Block::create(
                conn,
                &block.sequence_hash,
                path.id,
                block.start,
                block.end,
                &block.strand,
            );
            created_blocks_by_segment_id.insert(*segment_id, created_block);
        }
    }

    for input_walk in &gfa.walk {
        // TODO: Is this what we want to use for the path name?
        let walk_id = &input_walk.sample_id;
        // TODO: Fix Some(1)
        let path = models::Path::create(conn, &collection.name, None, walk_id, Some(1));
        for segment_id in input_walk.walk_id.iter() {
            let block = blocks_by_segment_id.get(segment_id).unwrap();
            let created_block = Block::create(
                conn,
                &block.sequence_hash,
                path.id,
                block.start,
                block.end,
                &block.strand,
            );
            created_blocks_by_segment_id.insert(*segment_id, created_block);
        }
    }

    let mut source_block_ids: HashSet<i32> = HashSet::new();
    let mut target_block_ids: HashSet<i32> = HashSet::new();

    for link in &gfa.links {
        let source_segment_id = link.from;
        let target_segment_id = link.to;
        let source_block = created_blocks_by_segment_id
            .get(&source_segment_id)
            .unwrap();
        let target_block = created_blocks_by_segment_id
            .get(&target_segment_id)
            .unwrap();
        models::Edge::create(conn, source_block.id, Some(target_block.id));
        source_block_ids.insert(source_block.id);
        target_block_ids.insert(target_block.id);
    }

    let end_block_ids = target_block_ids.difference(&source_block_ids);

    for end_block_id in end_block_ids {
        models::Edge::create(conn, *end_block_id, None);
    }
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Import {
            fasta,
            name,
            db,
            shallow,
        }) => import_fasta(fasta, name, *shallow, &mut get_connection(db)),
        Some(Commands::Update {
            name,
            db,
            fasta,
            vcf,
        }) => update_with_vcf(vcf, name, &mut get_connection(db)),
        None => {}
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use gen::migrations::run_migrations;

    fn get_connection() -> Connection {
        let mut conn = Connection::open_in_memory()
            .unwrap_or_else(|_| panic!("Error opening in memory test db"));
        rusqlite::vtab::array::load_module(&conn).unwrap();
        run_migrations(&mut conn);
        conn
    }

    #[test]
    fn test_add_fasta() {
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &"test".to_string(),
            false,
            &mut get_connection(),
        );
    }

    #[test]
    fn test_update_fasta_with_vcf() {
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/simple.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &mut get_connection();
        let collection = "test".to_string();
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
        );
        update_with_vcf(&vcf_path.to_str().unwrap().to_string(), &collection, conn);
        assert_eq!(
            Path::sequence(conn, &collection, Some(&"foo".to_string()), "m123", 1),
            "ATCATCGATCGATCGATCGGGAACACACAGAGA"
        );
    }

    #[test]
    fn test_import_simple_gfa() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/simple.gfa");

        let collection_name = "test".to_string();
        let conn = &mut get_connection();
        import_gfa(gfa_path.to_str().unwrap(), &collection_name, conn);

        let result = Path::sequence(conn, &collection_name, None, "124", 1);
        assert_eq!(result, "ATGGCATATTCGCAGCT");
    }

    #[test]
    fn test_import_gfa_with_walk() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/walk.gfa");

        let collection_name = "walk".to_string();
        let conn = &mut get_connection();
        import_gfa(gfa_path.to_str().unwrap(), &collection_name, conn);

        let result = Path::sequence(conn, &collection_name, None, "291344", 1);
        assert_eq!(result, "ACCTACAAATTCAAAC");
    }
}
