use noodles::fasta;
use rusqlite::{session, types::Value as SQLValue, Connection};

use crate::models::{
    file_types::FileTypes,
    metadata,
    operations::{FileAddition, Operation},
    path::Path,
};
use crate::operation_management;

pub fn update_with_fasta(
    conn: &Connection,
    operation_conn: &Connection,
    name: &str,
    path_name: &str,
    start_coordinate: i64,
    end_coordinate: i64,
    fasta_file_path: &str,
) -> std::io::Result<()> {
    let mut session = session::Session::new(conn).unwrap();
    operation_management::attach_session(&mut session);
    //    let change = FileAddition::create(operation_conn, library_file_path, FileTypes::CSV);
    let change = FileAddition::create(operation_conn, fasta_file_path, FileTypes::Fasta);

    let db_uuid = metadata::get_db_uuid(conn);

    let operation = Operation::create(
        operation_conn,
        &db_uuid,
        name.to_string(),
        "fasta_update",
        change.id,
    );

    let mut fasta_reader = fasta::io::reader::Builder.build_from_path(fasta_file_path)?;

    let path = Path::get_paths(
        conn,
        "select * from path where name = ?1",
        vec![SQLValue::from(path_name.to_string())],
    )[0]
    .clone();

    println!("Updated with fasta file: {}", fasta_file_path);
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    operation_management::write_changeset(conn, &operation, &output);

    Ok(())
}
