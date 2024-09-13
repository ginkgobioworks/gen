#![allow(warnings)]
use clap::{Parser, Subcommand};
use std::fmt::Debug;
use std::str;

use gen::get_connection;
use gen::imports::fasta::import_fasta;
use gen::imports::gfa::import_gfa;
use gen::updates::vcf::update_with_vcf;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The path to the database you wish to utilize
    #[arg(short, long)]
    db: String,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Import a new sequence collection.
    Import {
        /// Fasta file path
        #[arg(short, long)]
        fasta: Option<String>,
        /// GFA file path
        #[arg(short, long)]
        gfa: Option<String>,
        /// The name of the collection to store the entry under
        #[arg(short, long)]
        name: String,
        /// Don't store the sequence in the database, instead store the filename
        #[arg(short, long, action)]
        shallow: bool,
    },
    /// Update a sequence collection with new data
    Update {
        /// The name of the collection to update
        #[arg(short, long)]
        name: String,
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

fn main() {
    let cli = Cli::parse();
    let db = cli.db.as_str();
    let mut conn = get_connection(db);

    match &cli.command {
        Some(Commands::Import {
            fasta,
            gfa,
            name,
            shallow,
        }) => {
            conn.execute("BEGIN TRANSACTION", []).unwrap();
            if fasta.is_some() {
                import_fasta(&fasta.clone().unwrap(), name, *shallow, &conn);
            } else if gfa.is_some() {
                import_gfa(&gfa.clone().unwrap(), name, &conn);
            } else {
                panic!(
                    "ERROR: Import command attempted but no recognized file format was specified"
                );
            }
            conn.execute("END TRANSACTION", []).unwrap();
        }
        Some(Commands::Update {
            name,
            fasta,
            vcf,
            genotype,
            sample,
        }) => {
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
