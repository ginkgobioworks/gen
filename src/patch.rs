use crate::config::get_changeset_path;
use crate::models::operations::Operation;
use crate::operation_management;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::{Compression, Decompress};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::{fs, path};

#[derive(Serialize, Deserialize, Debug)]
struct OperationPatch {
    operation_id: i64,
    dependencies: operation_management::DependencyModels,
    changeset: Vec<u8>,
}

pub fn create_patch<W>(op_conn: &Connection, operations: &[i64], write_stream: &mut W)
where
    W: Write,
{
    let mut patches = vec![];
    for operation in operations.iter() {
        let operation = Operation::get_by_id(op_conn, *operation);
        let dependency_path =
            get_changeset_path(&operation).join(format!("{op_id}.dep", op_id = operation.id));
        let dependencies: operation_management::DependencyModels =
            serde_json::from_reader(fs::File::open(dependency_path).unwrap()).unwrap();
        let change_path =
            get_changeset_path(&operation).join(format!("{op_id}.cs", op_id = operation.id));
        let mut file = fs::File::open(change_path).unwrap();
        let mut contents = vec![];
        file.read_to_end(&mut contents).unwrap();
        patches.push(OperationPatch {
            operation_id: operation.id,
            dependencies,
            changeset: contents,
        })
    }
    let to_compress = serde_json::to_vec(&patches).unwrap();
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    let _ = e.write(&to_compress).unwrap();
    let compressed = e.finish().unwrap();
    write_stream.write_all(&compressed).unwrap();
}

pub fn load_patches<R>(reader: R) -> Vec<OperationPatch>
where
    R: Read,
{
    let mut d = GzDecoder::new(reader);
    let mut s = String::new();
    d.read_to_string(&mut s).unwrap();
    let patches: Vec<OperationPatch> = serde_json::from_str(&s).unwrap();
    patches
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::models::metadata;
    use crate::models::operations::setup_db;
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use crate::updates::vcf::update_with_vcf;
    use std::path::PathBuf;

    #[test]
    fn test_creates_patch() {
        setup_gen_dir();
        let vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.vcf");
        let fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.fa");
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
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
            None,
        );
        let mut write_stream: Vec<u8> = Vec::new();
        create_patch(operation_conn, &[1, 2], &mut write_stream);
        load_patches(&write_stream[..]);
    }
}
