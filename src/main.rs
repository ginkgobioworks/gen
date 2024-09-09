#![allow(warnings)]
use clap::{Parser, Subcommand};
use std::fmt::Debug;
use std::str;

use gen::get_connection;
use gen::imports::fasta::import_fasta;
use gen::models::change_log;
use gen::updates::vcf::update_with_vcf;

#[path = "./commands/change_log/mod.rs"]
mod change_log_command;

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
enum ChangeLogCommands {
    /// Add changes to a changeset
    Add {},
    /// Create a new changeset
    Create {
        /// The name of the collection to store the entry under
        #[arg(short, long)]
        name: Option<String>,
        /// Who authored the changeset
        #[arg(short, long)]
        author: Option<String>,
        /// Description of the changeset
        #[arg(short, long)]
        message: Option<String>,
    },
    /// View changes within a changeset
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
    ChangeLog {
        #[command(subcommand)]
        command: Option<ChangeLogCommands>,
    },
}

fn main() {
    let cli = Cli::parse();
    let db = cli.db.as_str();
    let mut conn = get_connection(db);

    match &cli.command {
        Some(Commands::Import {
            fasta,
            name,
            shallow,
        }) => {
            conn.execute("BEGIN TRANSACTION", []).unwrap();
            import_fasta(fasta, name, *shallow, &mut conn);
            conn.execute("END TRANSACTION", []).unwrap();
        }
        Some(Commands::Update {
            name,
            fasta,
            vcf,
            genotype,
            sample,
        }) => {
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
            Some(ChangeLogCommands::Add {}) => {
                change_log_command::add::ui(&conn);
            }
            Some(ChangeLogCommands::Create {
                name,
                author,
                message,
            }) => {
                change_log_command::create::ui(&conn, name, author, message);
            }
            Some(ChangeLogCommands::View {}) => {
                change_log_command::view::ui(&conn);
            }
            _ => {}
        },
        None => {}
    }
}
