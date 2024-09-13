#![allow(warnings)]
use clap::{Parser, Subcommand};
use gen::config;
use gen::config::get_operation_connection;

use gen::exports::gfa::export_gfa;
use gen::get_connection;
use gen::imports::fasta::import_fasta;
use gen::imports::gfa::import_gfa;
use gen::models::metadata;
use gen::models::operations::{Operation, OperationState};
use gen::operation_management;
use gen::updates::vcf::update_with_vcf;
use rusqlite::types::Value;
use std::fmt::Debug;
use std::str;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The path to the database you wish to utilize
    #[arg(short, long)]
    db: Option<String>,
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
    /// Initialize a gen repository
    Init {},
    /// Migrate a database to a given operation
    Checkout {
        /// The operation id to move to
        #[clap(index = 1)]
        id: i32,
    },
    /// View operations carried out against a database
    Operations {},
    Export {
        /// The name of the collection to export
        #[arg(short, long)]
        name: String,
        /// The path to the database being exported from
        #[arg(short, long)]
        db: String,
        /// The name of the GFA file to export to
        #[arg(short, long)]
        gfa: String,
    },
}

fn main() {
    let cli = Cli::parse();
    // commands not requiring a db connection are handled here
    if let Some(Commands::Init {}) = &cli.command {
        config::get_or_create_gen_dir();
        println!("Gen repository initialized.");
        return;
    }

    let binding = cli.db.expect("No db specified.");
    let db = binding.as_str();
    let conn = get_connection(db);
    let db_uuid = metadata::get_db_uuid(&conn);
    let operation_conn = get_operation_connection();

    match &cli.command {
        Some(Commands::Import {
            fasta,
            gfa,
            name,
            shallow,
        }) => {
            conn.execute("BEGIN TRANSACTION", []).unwrap();
            if fasta.is_some() {
                import_fasta(
                    &fasta.clone().unwrap(),
                    name,
                    *shallow,
                    &conn,
                    &operation_conn,
                );
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
                &conn,
                &operation_conn,
            );

            conn.execute("END TRANSACTION", []).unwrap();
        }
        Some(Commands::Init {}) => {
            config::get_or_create_gen_dir();
            println!("Gen repository initialized.");
        }
        Some(Commands::Operations {}) => {
            let current_op = OperationState::get_operation(&operation_conn, &db_uuid)
                .expect("Unable to read operation.");
            let operations = Operation::query(
                &operation_conn,
                "select * from operation where db_uuid = ?1 order by id desc;",
                vec![Value::from(db_uuid)],
            );
            let mut indicator = "";
            println!(
                "{indicator:<3}{col1:>3}   {col2:<70}",
                col1 = "Id",
                col2 = "Summary"
            );
            for op in operations.iter() {
                if op.id == current_op {
                    indicator = ">";
                } else {
                    indicator = "";
                }
                println!(
                    "{indicator:<3}{col1:>3}   {col2:<70}",
                    col1 = op.id,
                    col2 = op.change_type
                );
            }
        }
        Some(Commands::Checkout { id }) => {
            operation_management::move_to(&conn, &Operation::get_by_id(&operation_conn, *id));
	}
        Some(Commands::Export { name, db, gfa }) => {
            let mut conn = get_connection(db);
            conn.execute("BEGIN TRANSACTION", []).unwrap();
            export_gfa(&conn, name, gfa);
            conn.execute("END TRANSACTION", []).unwrap();
        }
        None => {}
    }
}
