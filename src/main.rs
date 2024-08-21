#![allow(warnings)]
use clap::{Parser, Subcommand};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::PathBuf;

use bio::io::fasta;
use gen::migrations::run_migrations;
use gen::models::{
    self,
    block::Block,
    edge::Edge,
    new_edge::NewEdge,
    path::{NewBlock, Path},
    path_edge::PathEdge,
    sequence::Sequence,
    BlockGroup,
};
use gen::{get_connection, parse_genotype};
use noodles::vcf;
use noodles::vcf::variant::record::samples::series::value::genotype::Phasing;
use noodles::vcf::variant::record::samples::series::Value;
use noodles::vcf::variant::record::samples::{Sample, Series};
use noodles::vcf::variant::record::{AlternateBases, ReferenceBases, Samples};
use noodles::vcf::variant::Record;
use rusqlite::{types::Value as SQLValue, Connection};
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
    let mut reader = fasta::Reader::from_file(fasta).unwrap();

    if !models::Collection::exists(conn, name) {
        let collection = models::Collection::create(conn, name);

        for result in reader.records() {
            let record = result.expect("Error during fasta record parsing");
            let sequence = String::from_utf8(record.seq().to_vec()).unwrap();
            let seq_hash = Sequence::create(conn, "DNA", &sequence, !shallow);
            let block_group = BlockGroup::create(conn, &collection.name, None, record.id());
            let block = Block::create(
                conn,
                &seq_hash,
                block_group.id,
                0,
                (sequence.len() as i32),
                "+",
            );
            Edge::create(conn, None, Some(block.id), 0, 0);
            Edge::create(conn, Some(block.id), None, 0, 0);
            Path::create(conn, record.id(), block_group.id, vec![block.id]);
        }
        println!("Created it");
    } else {
        println!("Collection {:1} already exists", name);
    }
}

fn new_import_fasta(fasta: &String, name: &str, shallow: bool, conn: &mut Connection) {
    // TODO: support gz
    let mut reader = fasta::Reader::from_file(fasta).unwrap();

    if !models::Collection::exists(conn, name) {
        let collection = models::Collection::create(conn, name);

        for result in reader.records() {
            let record = result.expect("Error during fasta record parsing");
            let sequence = String::from_utf8(record.seq().to_vec()).unwrap();
            let seq_hash = Sequence::create(conn, "DNA", &sequence, !shallow);
            let block_group = BlockGroup::create(conn, &collection.name, None, record.id());
            let edge_into = NewEdge::create(
                conn,
                NewEdge::PATH_START_HASH.to_string(),
                0,
                seq_hash.to_string(),
                0,
                0,
                0,
            );
            let edge_out_of = NewEdge::create(
                conn,
                seq_hash.to_string(),
                sequence.len() as i32,
                NewEdge::PATH_END_HASH.to_string(),
                0,
                0,
                0,
            );
            Path::new_create(
                conn,
                record.id(),
                block_group.id,
                vec![edge_into.id, edge_out_of.id],
            );
        }
        println!("Created it");
    } else {
        println!("Collection {:1} already exists", name);
    }
}

fn update_with_vcf(
    vcf_path: &String,
    collection_name: &String,
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

    for result in reader.records() {
        let record = result.unwrap();
        let seq_name = record.reference_sequence_name().to_string();
        let ref_allele = record.reference_bases();
        // this converts the coordinates to be zero based, start inclusive, end exclusive
        let ref_start = record.variant_start().unwrap().unwrap().get() - 1;
        let ref_end = record.variant_end(&header).unwrap().get();
        let alt_bases = record.alternate_bases();
        let alt_alleles: Vec<_> = alt_bases.iter().collect::<io::Result<_>>().unwrap();
        // TODO: fix this duplication of handling an insert
        if !fixed_sample.is_empty() && !genotype.is_empty() {
            for (chromosome_index, genotype) in genotype.iter().enumerate() {
                if let Some(gt) = genotype {
                    if gt.allele != 0 {
                        let alt_seq = alt_alleles[chromosome_index - 1];
                        let phased = match gt.phasing {
                            Phasing::Phased => 1,
                            Phasing::Unphased => 0,
                        };
                        // TODO: new sequence may not be real and be <DEL> or some sort. Handle these.
                        let new_sequence_hash = Sequence::create(conn, "DNA", alt_seq, true);
                        let sample_bg_id = BlockGroup::get_or_create_sample_block_group(
                            conn,
                            collection_name,
                            &fixed_sample,
                            &seq_name,
                        );
                        let sample_path_id = Path::get_paths(
                            conn,
                            "select * from path where block_group_id = ?1 AND name = ?2",
                            vec![
                                SQLValue::from(sample_bg_id),
                                SQLValue::from(seq_name.clone()),
                            ],
                        );
                        let new_block_id = Block::create(
                            conn,
                            &new_sequence_hash,
                            sample_bg_id,
                            0,
                            alt_seq.len() as i32,
                            "+",
                        );
                        BlockGroup::insert_change(
                            conn,
                            sample_path_id[0].id,
                            ref_start as i32,
                            ref_end as i32,
                            &new_block_id,
                            chromosome_index as i32,
                            phased,
                        );
                    }
                }
            }
        } else {
            for (sample_index, sample) in record.samples().iter().enumerate() {
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
                                    // TODO: new sequence may not be real and be <DEL> or some sort. Handle these.
                                    let new_sequence_hash =
                                        Sequence::create(conn, "DNA", alt_seq, true);
                                    let sample_bg_id = BlockGroup::get_or_create_sample_block_group(
                                        conn,
                                        collection_name,
                                        &sample_names[sample_index],
                                        &seq_name,
                                    );
                                    let sample_path_id = Path::get_paths(
                                        conn,
                                        "select * from path where block_group_id = ?1 AND name = ?2",
                                        vec![
                                            SQLValue::from(sample_bg_id),
                                            SQLValue::from(seq_name.clone()),
                                        ],
                                    );
                                    let new_block_id = Block::create(
                                        conn,
                                        &new_sequence_hash,
                                        sample_bg_id,
                                        0,
                                        alt_seq.len() as i32,
                                        "+",
                                    );
                                    BlockGroup::insert_change(
                                        conn,
                                        sample_path_id[0].id,
                                        ref_start as i32,
                                        ref_end as i32,
                                        &new_block_id,
                                        chromosome_index as i32,
                                        phased,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn new_update_with_vcf(
    vcf_path: &String,
    collection_name: &String,
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

    for result in reader.records() {
        let record = result.unwrap();
        let seq_name = record.reference_sequence_name().to_string();
        let ref_allele = record.reference_bases();
        // this converts the coordinates to be zero based, start inclusive, end exclusive
        let ref_start = record.variant_start().unwrap().unwrap().get() - 1;
        let ref_end = record.variant_end(&header).unwrap().get();
        let alt_bases = record.alternate_bases();
        let alt_alleles: Vec<_> = alt_bases.iter().collect::<io::Result<_>>().unwrap();
        // TODO: fix this duplication of handling an insert
        if !fixed_sample.is_empty() && !genotype.is_empty() {
            for (chromosome_index, genotype) in genotype.iter().enumerate() {
                if let Some(gt) = genotype {
                    if gt.allele != 0 {
                        let alt_seq = alt_alleles[chromosome_index - 1];
                        let phased = match gt.phasing {
                            Phasing::Phased => 1,
                            Phasing::Unphased => 0,
                        };
                        // TODO: new sequence may not be real and be <DEL> or some sort. Handle these.
                        let new_sequence_hash = Sequence::create(conn, "DNA", alt_seq, true);
                        let sequences_by_hash = Sequence::sequences_by_hash(
                            conn,
                            vec![format!("\"{}\"", new_sequence_hash)],
                        );
                        let sequence = sequences_by_hash.get(&new_sequence_hash).unwrap();
                        let sample_bg_id = BlockGroup::get_or_create_sample_block_group(
                            conn,
                            collection_name,
                            &fixed_sample,
                            &seq_name,
                        );
                        let sample_paths = Path::get_paths(
                            conn,
                            "select * from path where block_group_id = ?1 AND name = ?2",
                            vec![
                                SQLValue::from(sample_bg_id),
                                SQLValue::from(seq_name.clone()),
                            ],
                        );
                        let new_block = NewBlock {
                            id: 0,
                            sequence: sequence.clone(),
                            block_sequence: alt_seq.to_string(),
                            sequence_start: 0,
                            sequence_end: alt_seq.len() as i32,
                            path_start: ref_start as i32,
                            path_end: ref_end as i32,
                            strand: "+".to_string(),
                        };
                        BlockGroup::new_insert_change(
                            conn,
                            &sample_paths[0],
                            ref_start as i32,
                            ref_end as i32,
                            &new_block,
                            chromosome_index as i32,
                            phased,
                        );
                    }
                }
            }
        } else {
            for (sample_index, sample) in record.samples().iter().enumerate() {
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
                                    // TODO: new sequence may not be real and be <DEL> or some sort. Handle these.
                                    let new_sequence_hash =
                                        Sequence::create(conn, "DNA", alt_seq, true);
                                    let sequences_by_hash = Sequence::sequences_by_hash(
                                        conn,
                                        vec![format!("\"{}\"", new_sequence_hash)],
                                    );
                                    let sequence =
                                        sequences_by_hash.get(&new_sequence_hash).unwrap();
                                    let sample_bg_id = BlockGroup::get_or_create_sample_block_group(
                                        conn,
                                        collection_name,
                                        &sample_names[sample_index],
                                        &seq_name,
                                    );
                                    let sample_paths = Path::get_paths(
                                        conn,
                                        "select * from path where block_group_id = ?1 AND name = ?2",
                                        vec![
                                            SQLValue::from(sample_bg_id),
                                            SQLValue::from(seq_name.clone()),
                                        ],
                                    );
                                    let new_block = NewBlock {
                                        id: 0,
                                        sequence: sequence.clone(),
                                        block_sequence: alt_seq.to_string(),
                                        sequence_start: 0,
                                        sequence_end: alt_seq.len() as i32,
                                        path_start: ref_start as i32,
                                        path_end: ref_end as i32,
                                        strand: "+".to_string(),
                                    };
                                    BlockGroup::new_insert_change(
                                        conn,
                                        &sample_paths[0],
                                        ref_start as i32,
                                        ref_end as i32,
                                        &new_block,
                                        chromosome_index as i32,
                                        phased,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
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
        }) => new_import_fasta(fasta, name, *shallow, &mut get_connection(db)),
        Some(Commands::Update {
            name,
            db,
            fasta,
            vcf,
            genotype,
            sample,
        }) => new_update_with_vcf(
            vcf,
            name,
            genotype.clone().unwrap_or("".to_string()),
            sample.clone().unwrap_or("".to_string()),
            &mut get_connection(db),
        ),
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
        assert_eq!(
            Path::sequence(&conn, 1),
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
