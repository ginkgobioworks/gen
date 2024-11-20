use crate::config::get_changeset_path;
use crate::models::operations::Operation;
use crate::operation_management;
use flate2::read;
use flate2::write::ZlibEncoder;
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

pub fn create_patch(op_conn: &Connection, operations: &[i64]) {
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
        let mut stream = fs::File::create(format!("op_{id}.zz", id = operation.id)).unwrap();
        let to_compress = serde_json::to_vec(&OperationPatch {
            operation_id: operation.id,
            dependencies,
            changeset: contents,
        })
        .unwrap();
        let mut e = ZlibEncoder::new(Vec::new(), Compression::default());
        let _ = e.write(&to_compress).unwrap();
        let compressed = e.finish().unwrap();
        stream.write_all(&compressed).unwrap();
    }
}

pub fn load_patch<P>(conn: &Connection, op_conn: &Connection, patch_file: P)
where
    P: AsRef<Path>,
{
    let mut file = File::open(patch_file).unwrap();
    let mut contents: Vec<u8> = vec![];
    file.read_to_end(&mut contents);
    let mut d = read::ZlibDecoder::new(&contents[..]);
    let mut s = String::new();
    d.read_to_string(&mut s).unwrap();
    let patch: OperationPatch = serde_json::from_str(&s).unwrap();
    println!("p is {patch:?}");
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
        create_patch(operation_conn, &[1, 2]);
        load_patch(conn, operation_conn, "op_1.zz")
    }
}
