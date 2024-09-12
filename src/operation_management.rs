use std::io::{IsTerminal, Read, Write};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

use rusqlite::{session, Connection};

use crate::config::{get_gen_dir, get_operation_path};

pub fn read_operation_file() -> fs::File {
    let operation_path = get_operation_path();
    let mut file;
    if fs::metadata(&operation_path).is_ok() {
        file = fs::File::open(operation_path);
    } else {
        file = fs::File::create_new(operation_path);
    }
    file.unwrap()
}

pub fn write_operation_file() -> fs::File {
    let operation_path = get_operation_path();
    fs::File::create(operation_path).unwrap()
}

pub fn get_operation() -> Option<i32> {
    let mut file = read_operation_file();
    let mut contents: String = "".to_string();
    file.read_to_string(&mut contents).unwrap();
    match contents.parse::<i32>().unwrap_or(0) {
        0 => None,
        v => Some(v),
    }
}

pub fn set_operation(op_id: i32) {
    let mut file = write_operation_file();
    file.write_all(&format!("{op_id}").into_bytes());
}

pub fn write_changeset(op_id: i32, changes: &[u8]) {
    let change_path = Path::new(&get_gen_dir()).join(format!("{op_id}.cs"));
    let mut file = fs::File::create_new(change_path).unwrap();
    file.write_all(changes).unwrap()
}

pub fn apply_changeset(conn: &Connection, op_id: i32) {
    let change_path = Path::new(&get_gen_dir()).join(format!("{op_id}.cs"));
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

pub fn revert_changeset(conn: &Connection, op_id: i32) {
    let change_path = Path::new(&get_gen_dir()).join(format!("{op_id}.cs"));
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
    use crate::models::operations::Operation;
    use crate::models::{edge::Edge, Sample};
    use crate::test_helpers::{get_connection, setup_gen_dir};
    use crate::updates::vcf::update_with_vcf;

    #[test]
    fn test_writes_operation_id() {
        setup_gen_dir();
        set_operation(1);
        assert_eq!(get_operation().unwrap(), 1);
    }

    #[test]
    fn test_round_trip() {
        setup_gen_dir();
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/simple.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &mut get_connection(None);
        let collection = "test".to_string();
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
        );
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count = Operation::query(conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 2);
        assert_eq!(sample_count, 0);
        assert_eq!(op_count, 1);
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
        );
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count = Operation::query(conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 10);
        assert_eq!(sample_count, 3);
        assert_eq!(op_count, 2);

        // revert back to state 1 where vcf samples and blockpaths do not exist
        revert_changeset(conn, get_operation().unwrap());

        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count = Operation::query(conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 2);
        assert_eq!(sample_count, 0);
        assert_eq!(op_count, 2);

        apply_changeset(conn, get_operation().unwrap());
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count = Operation::query(conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 10);
        assert_eq!(sample_count, 3);
        assert_eq!(op_count, 2);
    }
}
