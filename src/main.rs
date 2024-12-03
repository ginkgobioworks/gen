#![allow(warnings)]
use clap::{Parser, Subcommand};
use gen::config;
use gen::config::get_operation_connection;

use gen::annotations::gff::propagate_gff;
use gen::exports::fasta::export_fasta;
use gen::exports::gfa::export_gfa;
use gen::get_connection;
use gen::imports::fasta::import_fasta;
use gen::imports::genbank::import_genbank;
use gen::imports::gfa::import_gfa;
use gen::models::metadata;
use gen::models::operations::{setup_db, Branch, Operation, OperationState};
use gen::operation_management;
use gen::operation_management::parse_patch_operations;
use gen::patch;
use gen::updates::fasta::update_with_fasta;
use gen::updates::gaf::{transform_csv_to_fasta, update_with_gaf};
use gen::updates::library::update_with_library;
use gen::updates::vcf::update_with_vcf;
use itertools::Itertools;
use rusqlite::{types::Value, Connection};
use std::fmt::Debug;
use std::fs::File;
use std::ops::Deref;
use std::path::PathBuf;
use std::{io, str};

#[derive(Parser)]
#[command(version, about, long_about = None, arg_required_else_help(true))]
struct Cli {
    /// The path to the database you wish to utilize
    #[arg(short, long)]
    db: Option<String>,
    #[command(subcommand)]
    command: Option<Commands>,
}

fn get_default_collection(conn: &Connection) -> Option<String> {
    let mut stmt = conn
        .prepare("select collection_name from defaults where id = 1")
        .unwrap();
    stmt.query_row((), |row| row.get(0)).unwrap()
}

#[derive(Subcommand)]
enum Commands {
    /// Commands for transforming file types for input to Gen.
    Transform {
        /// For update-gaf, this transforms the csv to a fasta for use in alignments
        #[arg(long)]
        format_csv_for_gaf: Option<String>,
    },
    /// Import a new sequence collection.
    Import {
        /// Fasta file path
        #[arg(short, long)]
        fasta: Option<String>,
        /// Genbank file path
        #[arg(long)]
        gb: Option<String>,
        /// GFA file path
        #[arg(short, long)]
        gfa: Option<String>,
        /// The name of the collection to store the entry under
        #[arg(short, long)]
        name: Option<String>,
        /// Don't store the sequence in the database, instead store the filename
        #[arg(short, long, action)]
        shallow: bool,
    },
    /// Update a sequence collection with new data
    Update {
        /// The name of the collection to update
        #[arg(short, long)]
        name: Option<String>,
        /// A fasta file to insert
        #[arg(short, long)]
        fasta: Option<String>,
        /// A VCF file to incorporate
        #[arg(short, long)]
        vcf: Option<String>,
        /// If no genotype is provided, enter the genotype to assign variants
        #[arg(short, long)]
        genotype: Option<String>,
        /// If no sample is provided, enter the sample to associate variants to
        #[arg(short, long)]
        sample: Option<String>,
        /// New sample name if we are updating with intentional edits
        #[arg(long)]
        new_sample: Option<String>,
        /// Use the given sample as the parent sample for changes.
        #[arg(long, alias = "cf")]
        coordinate_frame: Option<String>,
        /// A CSV with combinatorial library information
        #[arg(short, long)]
        library: Option<String>,
        /// A fasta with the combinatorial library parts
        #[arg(long)]
        parts: Option<String>,
        /// The name of the path to add the library to
        #[arg(short, long)]
        path_name: Option<String>,
        /// The name of the region to update (eg "chr1")
        #[arg(long)]
        region_name: Option<String>,
        /// The start coordinate for the region to add the library to
        #[arg(long)]
        start: Option<i64>,
        /// The end coordinate for the region to add the library to
        #[arg(short, long)]
        end: Option<i64>,
    },
    /// Use a GAF
    #[command(name = "update-gaf")]
    UpdateGaf {
        /// The name of the collection to update
        #[arg(short, long)]
        name: Option<String>,
        /// The GAF input
        #[arg(short, long)]
        gaf: String,
        /// The GAF input
        #[arg(short, long)]
        csv: String,
        /// The sample to update or create
        #[arg(short, long)]
        sample: String,
        /// If specified, the newly created sample will inherit this sample's existing graph
        #[arg(short, long)]
        parent_sample: Option<String>,
    },
    #[command(name = "patch-create")]
    PatchCreate {
        /// To create a patch against a non-checked out branch.
        #[arg(short, long)]
        branch: Option<String>,
        /// The patch name
        #[arg(short, long)]
        name: String,
        /// The operation(s) to create a patch from. For a range, use 1..3 and for multiple use commas.
        #[clap(index = 1)]
        operation: String,
    },
    #[command(name = "patch-apply")]
    PatchApply {
        /// The patch file
        #[clap(index = 1)]
        patch: String,
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
        #[arg(short, long, action)]
        merge: bool,
        /// The branch name
        #[clap(index = 1)]
        branch_name: Option<String>,
    },
    /// Migrate a database to a given operation
    Checkout {
        /// The branch identifier to migrate to
        #[arg(short, long)]
        branch: Option<String>,
        /// The operation hash to move to
        #[clap(index = 1)]
        hash: Option<String>,
    },
    /// Reset a branch to a previous operation
    Reset {
        /// The operation hash to reset to
        #[clap(index = 1)]
        hash: String,
    },
    /// View operations carried out against a database
    Operations {
        /// The branch to list operations for
        #[arg(short, long)]
        branch: Option<String>,
    },
    /// Apply an operation to a branch
    Apply {
        /// The operation hash to apply
        #[clap(index = 1)]
        hash: String,
    },
    /// Export sequence data
    Export {
        /// The name of the collection to export
        #[arg(short, long)]
        name: Option<String>,
        /// The name of the GFA file to export to
        #[arg(short, long)]
        gfa: Option<String>,
        /// An optional sample name
        #[arg(short, long)]
        sample: Option<String>,
        /// The name of the fasta file to export to
        #[arg(short, long)]
        fasta: Option<String>,
    },
    /// Configure default options
    Defaults {
        /// The default database to use
        #[arg(short, long)]
        database: Option<String>,
        /// The default collection to use
        #[arg(short, long)]
        collection: Option<String>,
    },
    PropagateAnnotations {
        /// The name of the collection to annotate
        #[arg(short, long)]
        name: Option<String>,
        /// The name of the sample the annotations are referenced to (if not provided, the default)
        #[arg(short, long)]
        from_sample: Option<String>,
        /// The name of the sample to annotate
        #[arg(short, long)]
        to_sample: String,
        /// The name of the annotation file to propagate
        #[arg(short, long)]
        gff: String,
        /// The name of the output file
        #[arg(short, long)]
        output_gff: String,
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

    let operation_conn = get_operation_connection();
    if let Some(Commands::Defaults {
        database,
        collection,
    }) = &cli.command
    {
        if let Some(name) = database {
            operation_conn
                .execute("update defaults set db_name=?1 where id = 1", (name,))
                .unwrap();
            println!("Default database set to {name}");
        }
        if let Some(name) = collection {
            operation_conn
                .execute(
                    "update defaults set collection_name=?1 where id = 1",
                    (name,),
                )
                .unwrap();
            println!("Default collection set to {name}");
        }
        return;
    }

    if let Some(Commands::Transform { format_csv_for_gaf }) = &cli.command {
        let csv = format_csv_for_gaf
            .clone()
            .expect("csv for transformation not provided.");
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        let mut csv_file = File::open(csv).unwrap();
        transform_csv_to_fasta(&mut csv_file, &mut handle);
        return;
    }

    let binding = cli.db.unwrap_or_else(|| {
        let mut stmt = operation_conn
            .prepare("select db_name from defaults where id = 1;")
            .unwrap();
        let row: Option<String> = stmt.query_row((), |row| row.get(0)).unwrap();
        row.expect("No db specified and no default database chosen.")
    });
    let db = binding.as_str();
    let conn = get_connection(db);
    let db_uuid = metadata::get_db_uuid(&conn);

    // initialize the selected database if needed.
    setup_db(&operation_conn, &db_uuid);

    match &cli.command {
        Some(Commands::Import {
            fasta,
            gb,
            gfa,
            name,
            shallow,
        }) => {
            conn.execute("BEGIN TRANSACTION", []).unwrap();
            operation_conn.execute("BEGIN TRANSACTION", []).unwrap();
            let name = &name.clone().unwrap_or_else(|| {
                get_default_collection(&operation_conn)
                    .expect("No collection specified and default not setup.")
            });
            if fasta.is_some() {
                match import_fasta(
                    &fasta.clone().unwrap(),
                    name,
                    *shallow,
                    &conn,
                    &operation_conn,
                ) {
                    Ok(_) => println!("Fasta imported."),
                    Err("No changes.") => println!("Fasta contents already exist."),
                    Err(_) => {
                        conn.execute("ROLLBACK TRANSACTION;", []).unwrap();
                        operation_conn.execute("ROLLBACK TRANSACTION;", []).unwrap();
                        panic!("Import failed.");
                    }
                }
            } else if gfa.is_some() {
                import_gfa(&PathBuf::from(gfa.clone().unwrap()), name, None, &conn);
            } else if let Some(gb) = gb {
                let f = File::open(gb).unwrap();
                import_genbank(&conn, &f, name.deref());
                println!("Genbank imported.");
            } else {
                conn.execute("ROLLBACK TRANSACTION;", []).unwrap();
                operation_conn.execute("ROLLBACK TRANSACTION;", []).unwrap();
                panic!(
                    "ERROR: Import command attempted but no recognized file format was specified"
                );
            }
            conn.execute("END TRANSACTION", []).unwrap();
            operation_conn.execute("END TRANSACTION", []).unwrap();
        }
        Some(Commands::Update {
            name,
            fasta,
            vcf,
            library,
            parts,
            genotype,
            sample,
            new_sample,
            path_name,
            region_name,
            start,
            end,
            coordinate_frame,
        }) => {
            conn.execute("BEGIN TRANSACTION", []).unwrap();
            operation_conn.execute("BEGIN TRANSACTION", []).unwrap();
            let name = &name.clone().unwrap_or_else(|| {
                get_default_collection(&operation_conn)
                    .expect("No collection specified and default not setup.")
            });
            if let Some(library_path) = library {
                update_with_library(
                    &conn,
                    &operation_conn,
                    name,
                    sample.clone().as_deref(),
                    &new_sample.clone().unwrap(),
                    &path_name.clone().unwrap(),
                    start.unwrap(),
                    end.unwrap(),
                    &parts.clone().unwrap(),
                    library_path,
                )
                .unwrap();
            } else if let Some(fasta_path) = fasta {
                // NOTE: This has to go after library because the library update also uses a fasta
                // file
                update_with_fasta(
                    &conn,
                    &operation_conn,
                    name,
                    sample.clone().as_deref(),
                    &new_sample.clone().unwrap(),
                    &region_name.clone().unwrap(),
                    start.unwrap(),
                    end.unwrap(),
                    fasta_path,
                )
                .unwrap();
            } else if let Some(vcf_path) = vcf {
                update_with_vcf(
                    vcf_path,
                    name,
                    genotype.clone().unwrap_or("".to_string()),
                    sample.clone().unwrap_or("".to_string()),
                    &conn,
                    &operation_conn,
                    coordinate_frame.as_deref(),
                )
                .unwrap();
            } else {
                panic!("Unknown file type provided for update.");
            }

            conn.execute("END TRANSACTION", []).unwrap();
            operation_conn.execute("END TRANSACTION", []).unwrap();
        }
        Some(Commands::UpdateGaf {
            name,
            gaf,
            csv,
            sample,
            parent_sample,
        }) => {
            conn.execute("BEGIN TRANSACTION", []).unwrap();
            operation_conn.execute("BEGIN TRANSACTION", []).unwrap();
            let name = &name.clone().unwrap_or_else(|| {
                get_default_collection(&operation_conn)
                    .expect("No collection specified and default not setup.")
            });
            update_with_gaf(
                &conn,
                &operation_conn,
                gaf,
                csv,
                name,
                Some(sample.as_ref()),
                parent_sample.as_deref(),
            );
            conn.execute("END TRANSACTION", []).unwrap();
            operation_conn.execute("END TRANSACTION", []).unwrap();
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
                "{indicator:<3}{col1:>64}   {col2:<70}",
                col1 = "Id",
                col2 = "Summary"
            );
            for op in operations.iter() {
                if op.hash == current_op {
                    indicator = ">";
                } else {
                    indicator = "";
                }
                println!(
                    "{indicator:<3}{col1:>64}   {col2:<70}",
                    col1 = op.hash,
                    col2 = op.change_type
                );
            }
        }
        Some(Commands::Branch {
            create,
            delete,
            checkout,
            list,
            merge,
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
                        col2 = branch
                            .current_operation_hash
                            .clone()
                            .unwrap_or(String::new())
                    );
                }
            } else if *merge {
                let branch_name = branch_name.clone().expect("Branch name must be provided.");
                let other_branch = Branch::get_by_name(&operation_conn, &db_uuid, &branch_name)
                    .unwrap_or_else(|| panic!("Unable to find branch {branch_name}."));
                let current_branch = OperationState::get_current_branch(&operation_conn, &db_uuid)
                    .expect("Unable to find current branch.");
                operation_management::merge(
                    &conn,
                    &operation_conn,
                    &db_uuid,
                    current_branch,
                    other_branch.id,
                    None,
                );
            } else {
                println!("No options selected.");
            }
        }
        Some(Commands::Apply { hash }) => {
            operation_management::apply(&conn, &operation_conn, hash, None);
        }
        Some(Commands::Checkout { branch, hash }) => {
            operation_management::checkout(&conn, &operation_conn, &db_uuid, branch, hash.clone());
        }
        Some(Commands::Reset { hash }) => {
            operation_management::reset(&conn, &operation_conn, &db_uuid, hash);
        }
        Some(Commands::Export {
            name,
            gfa,
            sample,
            fasta,
        }) => {
            let name = &name.clone().unwrap_or_else(|| {
                get_default_collection(&operation_conn)
                    .expect("No collection specified and default not setup.")
            });
            conn.execute("BEGIN TRANSACTION", []).unwrap();
            operation_conn.execute("BEGIN TRANSACTION", []).unwrap();
            if let Some(gfa_path) = gfa {
                export_gfa(&conn, name, &PathBuf::from(gfa_path), sample.clone());
            } else if let Some(fasta_path) = fasta {
                export_fasta(
                    &conn,
                    name,
                    sample.clone().as_deref(),
                    &PathBuf::from(fasta_path),
                );
            } else {
                panic!("No file type specified for export.");
            }
            conn.execute("END TRANSACTION", []).unwrap();
            operation_conn.execute("END TRANSACTION", []).unwrap();
        }
        Some(Commands::PatchCreate {
            name,
            operation,
            branch,
        }) => {
            let branch = if let Some(branch_name) = branch {
                Branch::get_by_name(&operation_conn, &db_uuid, branch_name)
                    .unwrap_or_else(|| panic!("No branch with name {branch_name} found."))
            } else {
                let current_branch_id =
                    OperationState::get_current_branch(&operation_conn, &db_uuid)
                        .expect("No current branch is checked out.");
                Branch::get_by_id(&operation_conn, current_branch_id).unwrap()
            };
            let branch_ops = Branch::get_operations(&operation_conn, branch.id);
            let operations = parse_patch_operations(
                &branch_ops,
                &branch.current_operation_hash.unwrap(),
                operation,
            );
            let mut f = File::create(format!("{name}.gz")).unwrap();
            patch::create_patch(&operation_conn, &operations, &mut f);
        }
        Some(Commands::PatchApply { patch }) => {
            let mut f = File::open(patch).unwrap();
            let patches = patch::load_patches(&mut f);
            patch::apply_patches(&conn, &operation_conn, &patches);
        }
        None => {}
        // these will never be handled by this method as we search for them earlier.
        Some(Commands::Init {}) => {
            config::get_or_create_gen_dir();
            println!("Gen repository initialized.");
        }
        Some(Commands::Defaults {
            database,
            collection,
        }) => {}
        Some(Commands::Transform { format_csv_for_gaf }) => {}
        Some(Commands::PropagateAnnotations {
            name,
            from_sample,
            to_sample,
            gff,
            output_gff,
        }) => {
            let name = &name.clone().unwrap_or_else(|| {
                get_default_collection(&operation_conn)
                    .expect("No collection specified and default not setup.")
            });
            let from_sample_name = from_sample.clone();

            conn.execute("BEGIN TRANSACTION", []).unwrap();
            operation_conn.execute("BEGIN TRANSACTION", []).unwrap();

            propagate_gff(
                &conn,
                name,
                from_sample_name.as_deref(),
                to_sample,
                gff,
                output_gff,
            );

            conn.execute("END TRANSACTION", []).unwrap();
            operation_conn.execute("END TRANSACTION", []).unwrap();
        }
    }
}
