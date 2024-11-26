use crate::config::get_changeset_path;
use crate::models::operations::{FileAddition, Operation, OperationSummary};
use crate::models::traits::Query;
use crate::operation_management;
use crate::operation_management::{
    apply_changeset, end_operation, load_changeset, load_changeset_dependencies, start_operation,
};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use rusqlite::session::ChangesetIter;
use rusqlite::types::Value;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Write};

#[derive(Serialize, Deserialize, Debug)]
pub struct OperationPatch {
    operation: Operation,
    files: FileAddition,
    summary: OperationSummary,
    dependencies: Vec<u8>,
    changeset: Vec<u8>,
}

pub fn create_patch<W>(op_conn: &Connection, operations: &[String], write_stream: &mut W)
where
    W: Write,
{
    let mut patches = vec![];
    for operation in operations.iter() {
        let operation = Operation::get_by_hash(op_conn, operation);
        println!("Creating patch for Operation {id}", id = operation.hash);
        let dependency_path =
            get_changeset_path(&operation).join(format!("{op_id}.dep", op_id = operation.hash));
        let dependencies: operation_management::DependencyModels =
            serde_json::from_reader(File::open(dependency_path).unwrap()).unwrap();
        let change_path =
            get_changeset_path(&operation).join(format!("{op_id}.cs", op_id = operation.hash));
        let mut file = File::open(change_path).unwrap();
        let mut contents = vec![];
        file.read_to_end(&mut contents).unwrap();
        patches.push(OperationPatch {
            operation: operation.clone(),
            files: FileAddition::get(
                op_conn,
                "select * from file_addition where id = ?1",
                params![Value::from(operation.change_id)],
            )
            .unwrap(),
            summary: OperationSummary::get(
                op_conn,
                "select * from operation_summary where operation_hash = ?1",
                params![Value::from(operation.hash.clone())],
            )
            .unwrap(),
            dependencies: serde_json::to_vec(&dependencies).unwrap(),
            changeset: contents,
        })
    }
    let to_compress = serde_json::to_vec(&patches).unwrap();
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(&to_compress).unwrap();
    let compressed = e.finish().unwrap();
    write_stream.write_all(&compressed).unwrap();
}

pub fn load_patches<R>(reader: R) -> Vec<OperationPatch>
where
    R: Read,
{
    let mut d = GzDecoder::new(reader);
    let mut s = Vec::new();
    d.read_to_end(&mut s).unwrap();
    let patches: Vec<OperationPatch> = serde_json::from_slice(&s[..]).unwrap();
    patches
}

pub fn apply_patches(conn: &Connection, op_conn: &Connection, patches: &[OperationPatch]) {
    for patch in patches.iter() {
        let op_info = &patch.operation;
        let changeset = load_changeset(op_info);
        let input: &mut dyn Read = &mut changeset.as_slice();
        let mut iter = ChangesetIter::start_strm(&input).unwrap();
        let dependencies = load_changeset_dependencies(op_info);
        let mut session = start_operation(conn);
        apply_changeset(conn, &mut iter, &dependencies);
        match end_operation(
            conn,
            op_conn,
            &mut session,
            &patch.files.file_path,
            patch.files.file_type,
            &op_info.change_type,
            &patch.summary.summary,
            None,
        ) {
            Ok(_new_op) => {
                println!("Successfully applied operation.");
            }
            Err(e) => match e {
                "Operation already exists." => println!("Operation already applied. Skipping."),
                "No changes." => println!("No new changes present in operation. Skipping."),
                _ => panic!("error is {e:?}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::models::metadata::get_db_uuid;
    use crate::models::operations::{setup_db, Branch, OperationState};
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use crate::updates::vcf::update_with_vcf;
    use std::path::PathBuf;

    #[test]
    fn test_creates_patch() {
        setup_gen_dir();
        let vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.vcf");
        let fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.fa");
        let conn = &mut get_connection(None);
        let db_uuid = get_db_uuid(conn);
        let operation_conn = &get_operation_connection(None);
        setup_db(operation_conn, &db_uuid);
        let collection = "test".to_string();
        let op_1 = import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            operation_conn,
        )
        .unwrap();
        let op_2 = update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
            None,
        )
        .unwrap();
        let mut write_stream: Vec<u8> = Vec::new();
        create_patch(operation_conn, &[op_1.hash, op_2.hash], &mut write_stream);
        load_patches(&write_stream[..]);
    }

    #[test]
    fn test_cross_db_patches() {
        setup_gen_dir();
        let vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.vcf");
        let fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.fa");
        let conn = &mut get_connection(None);
        let conn2 = &mut get_connection(None);
        let db_uuid = get_db_uuid(conn);
        let db_uuid2 = get_db_uuid(conn2);
        let operation_conn = &get_operation_connection(None);
        let operation_conn2 = &get_operation_connection(None);
        setup_db(operation_conn, &db_uuid);
        setup_db(operation_conn2, &db_uuid2);
        let collection = "test".to_string();
        let op_1 = import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            operation_conn,
        )
        .unwrap();
        let op_2 = update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
            None,
        )
        .unwrap();
        let mut write_stream: Vec<u8> = Vec::new();
        create_patch(operation_conn, &[op_1.hash, op_2.hash], &mut write_stream);
        let patches = load_patches(&write_stream[..]);
        apply_patches(conn2, operation_conn2, &patches);
        apply_patches(conn, operation_conn, &patches);
    }

    #[test]
    fn test_cross_branch_patches() {
        setup_gen_dir();
        let vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.vcf");
        let fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.fa");
        let conn = &mut get_connection(None);
        let db_uuid = &get_db_uuid(conn);
        let operation_conn = &get_operation_connection(None);
        setup_db(operation_conn, db_uuid);
        let collection = "test".to_string();
        let op_1 = import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            operation_conn,
        )
        .unwrap();
        let main_branch = Branch::get_by_name(operation_conn, db_uuid, "main").unwrap();
        let branch = Branch::create(operation_conn, db_uuid, "new-branch");
        OperationState::set_branch(operation_conn, db_uuid, "new-branch");
        let op_2 = update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
            None,
        )
        .unwrap();
        let mut write_stream: Vec<u8> = Vec::new();
        create_patch(operation_conn, &[op_2.hash], &mut write_stream);

        operation_management::checkout(
            conn,
            operation_conn,
            db_uuid,
            &Some("main".to_string()),
            None,
        );
        let patches = load_patches(&write_stream[..]);
        apply_patches(conn, operation_conn, &patches);
        let branch_ops = Branch::get_operations(operation_conn, main_branch.id);
        assert_eq!(branch_ops.len(), 2);
        // ensure if we apply the operation again it'll be a no-op
        apply_patches(conn, operation_conn, &patches);
        let branch_ops = Branch::get_operations(operation_conn, main_branch.id);
        assert_eq!(branch_ops.len(), 2);
    }
}
