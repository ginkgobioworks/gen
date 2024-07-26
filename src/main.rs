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
