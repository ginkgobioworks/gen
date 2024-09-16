use petgraph::Direction;
use rusqlite::{session, types::Value, Connection};
use std::io::{IsTerminal, Read, Write};
use std::{env, fs, path::PathBuf};

use crate::config::get_changeset_path;
use crate::models::operations::{Operation, OperationState};

enum FileMode {
    Read,
    Write,
}

pub fn get_file(path: &PathBuf, mode: FileMode) -> fs::File {
    let mut file;
    match mode {
        FileMode::Read => {
            if fs::metadata(path).is_ok() {
                file = fs::File::open(path);
            } else {
                file = fs::File::create_new(path);
            }
        }
        FileMode::Write => {
            file = fs::File::create(path);
        }
    }

    file.unwrap()
}

pub fn write_changeset(operation: &Operation, changes: &[u8]) {
    let change_path =
        get_changeset_path(operation).join(format!("{op_id}.cs", op_id = operation.id));
    let mut file = fs::File::create_new(&change_path)
        .unwrap_or_else(|_| panic!("Unable to open {change_path:?}"));
    file.write_all(changes).unwrap()
}

pub fn apply_changeset(conn: &Connection, operation: &Operation) {
    let change_path =
        get_changeset_path(operation).join(format!("{op_id}.cs", op_id = operation.id));
    let mut file = fs::File::open(change_path).unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).unwrap();
    conn.pragma_update(None, "foreign_keys", "0").unwrap();

    conn.apply_strm(
        &mut &contents[..],
        None::<fn(&str) -> bool>,
        |_conflict_type, _item| session::ConflictAction::SQLITE_CHANGESET_OMIT,
    );
    conn.pragma_update(None, "foreign_keys", "1").unwrap();
}

pub fn revert_changeset(conn: &Connection, operation: &Operation) {
    let change_path =
        get_changeset_path(operation).join(format!("{op_id}.cs", op_id = operation.id));
    let mut file = fs::File::open(change_path).unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).unwrap();
    let mut inverted_contents: Vec<u8> = vec![];
    session::invert_strm(&mut &contents[..], &mut inverted_contents);

    conn.pragma_update(None, "foreign_keys", "0").unwrap();
    conn.apply_strm(
        &mut &inverted_contents[..],
        None::<fn(&str) -> bool>,
        |_conflict_type, _item| session::ConflictAction::SQLITE_CHANGESET_OMIT,
    );
    conn.pragma_update(None, "foreign_keys", "1").unwrap();
}

pub fn move_to(conn: &Connection, operation: &Operation) {
    let current_op_id = OperationState::get_operation(conn, &operation.db_uuid).unwrap();
    let op_id = operation.id;
    let path = Operation::get_path_between(conn, current_op_id, op_id);
    if path.is_empty() {
        println!("No path exists from {current_op_id} to {op_id}.");
        return;
    }
    for (operation_id, direction, next_op) in path.iter() {
        match direction {
            Direction::Outgoing => {
                println!("Reverting operation {operation_id}");
                revert_changeset(conn, &Operation::get_by_id(conn, *operation_id));
                OperationState::set_operation(conn, &operation.db_uuid, *next_op);
            }
            Direction::Incoming => {
                println!("Applying operation {operation_id}");
                apply_changeset(conn, &Operation::get_by_id(conn, *operation_id));
                OperationState::set_operation(conn, &operation.db_uuid, *operation_id);
            }
        }
    }
}

pub fn attach_session(session: &mut session::Session) {
    for table in [
        "collection",
        "sample",
        "sequence",
        "block_group",
        "path",
        "edges",
        "path_edges",
        "block_group_edges",
    ] {
        session.attach(Some(table)).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::models::operations::{Operation, OperationState};
    use crate::models::{edge::Edge, metadata, Sample};
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use crate::updates::vcf::update_with_vcf;

    #[test]
    fn test_writes_operation_id() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        OperationState::set_operation(op_conn, &db_uuid, 1);
        assert_eq!(OperationState::get_operation(op_conn, &db_uuid).unwrap(), 1);
    }

    #[test]
    fn test_round_trip() {
        setup_gen_dir();
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/simple.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &mut get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(None);
        let collection = "test".to_string();
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            operation_conn,
        );
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 2);
        assert_eq!(sample_count, 0);
        assert_eq!(op_count, 1);
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 10);
        assert_eq!(sample_count, 3);
        assert_eq!(op_count, 2);

        // revert back to state 1 where vcf samples and blockpaths do not exist
        revert_changeset(
            conn,
            &Operation::get_by_id(
                operation_conn,
                OperationState::get_operation(operation_conn, &db_uuid).unwrap(),
            ),
        );

        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 2);
        assert_eq!(sample_count, 0);
        assert_eq!(op_count, 2);

        apply_changeset(
            conn,
            &Operation::get_by_id(
                operation_conn,
                OperationState::get_operation(operation_conn, &db_uuid).unwrap(),
            ),
        );
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 10);
        assert_eq!(sample_count, 3);
        assert_eq!(op_count, 2);
    }
}
