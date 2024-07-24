#![allow(warnings)]
use clap::{Parser, Subcommand};
use std::fmt::Debug;
use std::str;

use gen::get_connection;
use gen::imports::fasta::import_fasta;
use gen::updates::vcf::update_with_vcf;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum ChangeLogCommands {
    /// does testing things
    Add {},
    View {},
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
    ChangeLog {
        #[command(subcommand)]
        command: Option<ChangeLogCommands>,
    },
}

fn import_gfa(gfa_path: &String, collection_name: &String, conn: &mut Connection) {
    run_migrations(conn);

    let parser = GFAParser::new();
    let gfa: GFA<Vec<u8>, ()> = parser.parse_file(gfa_path).unwrap();

    let collection = models::Collection::create(conn, collection_name);

    let mut blocks_by_segment_name: HashMap<String, Block> = HashMap::new();

    for segment in &gfa.segments {
        let sequence = String::from_utf8(segment.sequence.clone()).unwrap();
        let seq_hash = models::Sequence::create(conn, "DNA".to_string(), &sequence, true);
        let block = Block {
            id: 0,
            path_id: 0,
            sequence_hash: seq_hash,
            start: 0,
            end: (sequence.len() as i32),
            strand: "1".to_string(),
        };
        let segment_name = String::from_utf8(segment.name.clone()).unwrap();
        blocks_by_segment_name.insert(segment_name, block);
    }

    let mut created_blocks_by_segment_name: HashMap<String, Block> = HashMap::new();

    for input_path in &gfa.paths {
        let path_name = String::from_utf8(input_path.path_name.clone()).unwrap();
        let path = models::Path::create(conn, &collection.name, None, &path_name, Some(1));
        for (name, _) in input_path.iter() {
            let i = 0;
            let block = blocks_by_segment_name.get(&name.to_string()).unwrap();
            let created_block = Block::create(
                conn,
                &block.sequence_hash,
                path.id,
                block.start,
                block.end,
                &block.strand,
            );
            created_blocks_by_segment_name.insert(name.to_string(), created_block);
        }
    }

    for link in &gfa.links {
        let source_name = String::from_utf8(link.from_segment.clone()).unwrap();
        let target_name = String::from_utf8(link.to_segment.clone()).unwrap();
        let source_block = created_blocks_by_segment_name.get(&source_name).unwrap();
        let target_block = created_blocks_by_segment_name.get(&target_name).unwrap();
        models::Edge::create(conn, source_block.id, Some(target_block.id));
    }

    if !gfa.links.is_empty() {
        let last_link = gfa.links.last().unwrap();
        let source_name = String::from_utf8(last_link.to_segment.clone()).unwrap();
        let source_block = created_blocks_by_segment_name.get(&source_name).unwrap();
        models::Edge::create(conn, source_block.id, None);
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
            conn.execute("PRAGMA cache_size=50000;", []).unwrap();
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
        Some(Commands::ChangeLog { command }) => match &command {
            Some(ChangeLogCommands::Add {}) => {}
            Some(ChangeLogCommands::View {}) => {}
            _ => {}
        },
        None => {}
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use std::fs;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use gen::test_helpers::get_connection;

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

    #[test]
    fn test_import_gfa() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/simple.gfa");

        let collection_name = "test".to_string();
        let conn = &mut get_connection();
        import_gfa(
            &gfa_path.to_str().unwrap().to_string(),
            &collection_name,
            conn,
        );

        let result = Path::sequence(conn, &collection_name, None, "124", 1);
        assert_eq!(result, "ATGGCATATTCGCAGCT");
    }
}
