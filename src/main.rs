#![allow(warnings)]
use clap::{Parser, Subcommand};
use gen::config;
use gen::config::get_operation_connection;

use gen::exports::gfa::export_gfa;
use gen::get_connection;
use gen::imports::fasta::import_fasta;
use gen::imports::gfa::import_gfa;
use gen::models::metadata;
use gen::models::operations::{setup_db, Branch, Operation, OperationState};
use gen::operation_management;
use gen::updates::vcf::update_with_vcf;
use rusqlite::types::Value;
use std::fmt::Debug;
use std::path::PathBuf;
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
    /// Manage and create branches
    Branch {
        /// Create a branch with the given name
        #[arg(long, action)]
        create: bool,
        /// Delete a given branch
        #[arg(short, long, action)]
        delete: bool,
        /// Checkout a given branch
        #[arg(long, action)]
        checkout: bool,
        /// List all branches
        #[arg(short, long, action)]
        list: bool,
        /// The branch name
        #[clap(index = 1)]
        branch_name: Option<String>,
    },
    /// Migrate a database to a given operation
    Checkout {
        /// The branch identifier to migrate to
        #[arg(short, long)]
        branch: Option<String>,
        /// The operation id to move to
        #[clap(index = 1)]
        id: Option<i32>,
    },
    /// View operations carried out against a database
    Operations {
        /// The branch to list operations for
        #[arg(short, long)]
        branch: Option<String>,
    },
    Apply {
        /// The operation id to apply
        #[clap(index = 1)]
        id: i32,
    },
    Export {
        /// The name of the collection to export
        #[arg(short, long)]
        name: String,
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

    // initialize the selected database if needed.
    setup_db(&operation_conn, &db_uuid);

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
                import_gfa(&PathBuf::from(gfa.clone().unwrap()), name, &conn);
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
        Some(Commands::Operations { branch }) => {
            let current_op = OperationState::get_operation(&operation_conn, &db_uuid)
                .expect("Unable to read operation.");
            let branch_name = branch.clone().unwrap_or_else(|| {
                let current_branch_id =
                    OperationState::get_current_branch(&operation_conn, &db_uuid)
                        .expect("No current branch is set.");
                Branch::get_by_id(&operation_conn, current_branch_id)
                    .unwrap_or_else(|| panic!("No branch with id {current_branch_id}"))
                    .name
            });
            let operations = Branch::get_operations(
                &operation_conn,
                Branch::get_by_name(&operation_conn, &db_uuid, &branch_name)
                    .unwrap_or_else(|| panic!("No branch named {branch_name}."))
                    .id,
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
        Some(Commands::Branch {
            create,
            delete,
            checkout,
            list,
            branch_name,
        }) => {
            if *create {
                Branch::create(
                    &operation_conn,
                    &db_uuid,
                    &branch_name
                        .clone()
                        .expect("Must provide a branch name to create."),
                );
            } else if *delete {
                Branch::delete(
                    &operation_conn,
                    &db_uuid,
                    &branch_name
                        .clone()
                        .expect("Must provide a branch name to delete."),
                );
            } else if *checkout {
                operation_management::checkout(
                    &conn,
                    &operation_conn,
                    &db_uuid,
                    &Some(
                        branch_name
                            .clone()
                            .expect("Must provide a branch name to checkout.")
                            .to_string(),
                    ),
                    None,
                );
            } else if *list {
                let current_branch = OperationState::get_current_branch(&operation_conn, &db_uuid);
                let mut indicator = "";
                println!(
                    "{indicator:<3}{col1:<30}   {col2:<20}",
                    col1 = "Name",
                    col2 = "Operation",
                );
                for branch in Branch::query(
                    &operation_conn,
                    "select * from branch where db_uuid = ?1",
                    vec![Value::from(db_uuid.to_string())],
                )
                .iter()
                {
                    if let Some(current_branch_id) = current_branch {
                        if current_branch_id == branch.id {
                            indicator = ">";
                        } else {
                            indicator = "";
                        }
                    }
                    println!(
                        "{indicator:<3}{col1:<30}   {col2:<20}",
                        col1 = branch.name,
                        col2 = branch.current_operation_id.unwrap_or(-1)
                    );
                }
            } else {
                println!("No options selected.");
            }
        }
        Some(Commands::Apply { id }) => {
            operation_management::apply(&conn, &operation_conn, &db_uuid, *id);
        }
        Some(Commands::Checkout { branch, id }) => {
            operation_management::checkout(&conn, &operation_conn, &db_uuid, branch, *id);
        }
        Some(Commands::Export { name, gfa }) => {
            conn.execute("BEGIN TRANSACTION", []).unwrap();
            export_gfa(&conn, name, &PathBuf::from(gfa));
            conn.execute("END TRANSACTION", []).unwrap();
        }
        None => {}
    }
}
