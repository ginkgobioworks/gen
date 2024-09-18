use crate::config::get_changeset_path;
use crate::models::file_types::FileTypes;
use crate::models::operations::{
    Branch, FileAddition, Operation, OperationState, OperationSummary,
};
use crate::operation_management;
use petgraph::Direction;
use rusqlite::{session, Connection};
use std::fmt::format;
use std::io::{IsTerminal, Read, Write};
use std::{env, fs, path::PathBuf};

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

pub fn apply(conn: &Connection, operation_conn: &Connection, db_uuid: &str, op_id: i32) {
    let mut session = session::Session::new(conn).unwrap();
    attach_session(&mut session);
    let change = FileAddition::create(operation_conn, &format!("{op_id}.cs"), FileTypes::Changeset);
    apply_changeset(conn, &Operation::get_by_id(operation_conn, op_id));
    let operation = Operation::create(
        operation_conn,
        db_uuid,
        None,
        "changeset_application",
        change.id,
    );

    OperationSummary::create(
        operation_conn,
        operation.id,
        &format!("Applied changeset {op_id}."),
    );
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    write_changeset(&operation, &output);
}

pub fn move_to(conn: &Connection, operation_conn: &Connection, operation: &Operation) {
    let current_op_id = OperationState::get_operation(operation_conn, &operation.db_uuid).unwrap();
    let op_id = operation.id;
    if (current_op_id == op_id) {
        return;
    }
    let path = Operation::get_path_between(operation_conn, current_op_id, op_id);
    if path.is_empty() {
        println!("No path exists from {current_op_id} to {op_id}.");
        return;
    }
    for (operation_id, direction, next_op) in path.iter() {
        match direction {
            Direction::Outgoing => {
                println!("Reverting operation {operation_id}");
                revert_changeset(conn, &Operation::get_by_id(operation_conn, *operation_id));
                OperationState::set_operation(operation_conn, &operation.db_uuid, *next_op);
            }
            Direction::Incoming => {
                println!("Applying operation {operation_id}");
                apply_changeset(conn, &Operation::get_by_id(operation_conn, *operation_id));
                OperationState::set_operation(operation_conn, &operation.db_uuid, *operation_id);
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

pub fn checkout(
    conn: &Connection,
    operation_conn: &Connection,
    db_uuid: &str,
    branch_name: &Option<String>,
    operation_id: Option<i32>,
) {
    let mut branch_id = 0;
    let mut dest_op_id = operation_id.unwrap_or(0);
    if let Some(name) = branch_name {
        let current_branch = OperationState::get_current_branch(operation_conn, db_uuid)
            .expect("No current branch set");
        let branch = Branch::get_by_name(operation_conn, db_uuid, name)
            .unwrap_or_else(|| panic!("No branch named {name}"));
        branch_id = branch.id;
        if current_branch != branch_id {
            OperationState::set_branch(operation_conn, db_uuid, name);
        }
        if dest_op_id == 0 {
            dest_op_id = branch.current_operation_id.unwrap();
        }
    }
    if dest_op_id == 0 {
        panic!("No operation defined.");
    }
    move_to(
        conn,
        operation_conn,
        &Operation::get_by_id(operation_conn, dest_op_id),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::models::file_types::FileTypes;
    use crate::models::operations::{setup_db, Branch, FileAddition, Operation, OperationState};
    use crate::models::{edge::Edge, metadata, Sample};
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use crate::updates::vcf::update_with_vcf;
    use std::path::{Path, PathBuf};

    #[test]
    fn test_writes_operation_id() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);
        let change = FileAddition::create(op_conn, "test", FileTypes::Fasta);
        let operation = Operation::create(op_conn, &db_uuid, "test".to_string(), "test", change.id);
        OperationState::set_operation(op_conn, &db_uuid, operation.id);
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
        setup_db(operation_conn, &db_uuid);
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

    #[test]
    fn test_branch_movement() {
        setup_gen_dir();
        let fasta_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.fa");
        let vcf_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.vcf");
        let vcf2_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple2.vcf");
        let conn = &mut get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(None);
        setup_db(operation_conn, &db_uuid);
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

        let branch_1 = Branch::create(operation_conn, &db_uuid, "branch_1");

        let branch_2 = Branch::create(operation_conn, &db_uuid, "branch_2");

        OperationState::set_branch(operation_conn, &db_uuid, "branch_1");
        assert_eq!(
            OperationState::get_current_branch(operation_conn, &db_uuid).unwrap(),
            branch_1.id
        );

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

        // checkout branch 2
        checkout(
            conn,
            operation_conn,
            &db_uuid,
            &Some("branch_2".to_string()),
            None,
        );

        assert_eq!(
            OperationState::get_current_branch(operation_conn, &db_uuid).unwrap(),
            branch_2.id
        );

        // ensure branch 1 operations have been undone
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 2);
        assert_eq!(sample_count, 0);
        assert_eq!(op_count, 2);

        // apply vcf2
        update_with_vcf(
            &vcf2_path.to_str().unwrap().to_string(),
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
        assert_eq!(edge_count, 6);
        assert_eq!(sample_count, 1);
        assert_eq!(op_count, 3);

        // migrate to branch 1 again
        checkout(
            conn,
            operation_conn,
            &db_uuid,
            &Some("branch_1".to_string()),
            None,
        );
        assert_eq!(
            OperationState::get_current_branch(operation_conn, &db_uuid).unwrap(),
            branch_1.id
        );

        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 10);
        assert_eq!(sample_count, 3);
        assert_eq!(op_count, 3);
    }
}
