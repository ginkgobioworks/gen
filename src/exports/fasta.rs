use noodles::fasta;
use rusqlite::Connection;
use std::fs::File;
use std::path::PathBuf;

use crate::models::{block_group::BlockGroup, operation_path::OperationPath, path::Path};

pub fn export_fasta(conn: &Connection, operation_id: i64, filename: &PathBuf) {
    let operation_paths = OperationPath::paths_for_operation(conn, operation_id);

    let file = File::create(filename).unwrap();
    let mut writer = fasta::io::Writer::new(file);

    for operation_path in operation_paths {
        let path = Path::get(conn, operation_path.path_id);
        let block_group = BlockGroup::get_by_id(conn, path.block_group_id);

        let definition = fasta::record::Definition::new(block_group.name, None);
        let sequence = fasta::record::Sequence::from(path.sequence(conn).into_bytes());
        let record = fasta::Record::new(definition, sequence);

        let _ = writer.write_record(&record);
    }

    println!("Exported to file {}", filename.display());
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::models::{
        metadata,
        operations::{setup_db, OperationState},
    };
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use crate::updates::fasta::update_with_fasta;
    use noodles::fasta;
    use std::path::PathBuf;
    use std::{io, str};
    use tempdir::TempDir;

    #[test]
    fn test_import_then_export() {
        setup_gen_dir();
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        let collection = "test".to_string();

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );
        let current_op_id = OperationState::get_operation(op_conn, &db_uuid).unwrap();
        let tmp_dir = TempDir::new("export_fasta_files").unwrap().into_path();
        let filename = tmp_dir.join("out.fa");
        export_fasta(conn, current_op_id, &filename);

        let mut fasta_reader = fasta::io::reader::Builder
            .build_from_path(filename)
            .unwrap();
        let record = fasta_reader
            .records()
            .next()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "No records found in fasta file")
            })
            .unwrap()
            .unwrap();

        let sequence = str::from_utf8(record.sequence().as_ref())
            .unwrap()
            .to_string();
        assert_eq!(sequence, "ATCGATCGATCGATCGATCGGGAACACACAGAGA");
    }

    #[test]
    fn test_import_fasta_update_with_fasta_export() {
        /*
        Graph after fasta update:
        AT ----> CGA ------> TCGATCGATCGATCGGGAACACACAGAGA
           \-> AAAAAAAA --/
        */
        setup_gen_dir();
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let mut fasta_update_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_update_path.push("fixtures/aaaaaaaa.fa");
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        let collection = "test".to_string();

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );
        let _ = update_with_fasta(
            conn,
            op_conn,
            &collection,
            "m123",
            2,
            5,
            fasta_update_path.to_str().unwrap(),
        );

        let current_op_id = OperationState::get_operation(op_conn, &db_uuid).unwrap();
        let tmp_dir = TempDir::new("export_fasta_files").unwrap().into_path();
        let filename = tmp_dir.join("out.fa");
        export_fasta(conn, current_op_id, &filename);

        let mut fasta_reader = fasta::io::reader::Builder
            .build_from_path(filename)
            .unwrap();
        let record = fasta_reader
            .records()
            .next()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "No records found in fasta file")
            })
            .unwrap()
            .unwrap();

        let sequence = str::from_utf8(record.sequence().as_ref())
            .unwrap()
            .to_string();
        assert_eq!(sequence, "ATAAAAAAAATCGATCGATCGATCGGGAACACACAGAGA");
    }
}
