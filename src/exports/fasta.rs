use noodles::fasta;
use rusqlite;
use rusqlite::Connection;
use std::fs::File;
use std::path::PathBuf;

use crate::models::block_group::BlockGroup;
use crate::models::sample::Sample;

pub fn export_fasta(
    conn: &Connection,
    collection_name: &str,
    sample_name: Option<&str>,
    filename: &PathBuf,
    number_of_paths: Option<i64>,
) {
    let block_groups = Sample::get_block_groups(conn, collection_name, sample_name);

    let file = File::create(filename).unwrap();
    let mut writer = fasta::io::Writer::new(file);

    for block_group in block_groups {
        let sequences = if let Some(number_of_paths) = number_of_paths {
            let all_sequences = BlockGroup::get_all_sequences(conn, block_group.id, false);
            all_sequences
                .iter()
                .take(number_of_paths as usize)
                .cloned()
                .collect()
        } else {
            let path = BlockGroup::get_current_path(conn, block_group.id);
            vec![path.sequence(conn)]
        };

        for (i, sequence) in sequences.iter().enumerate() {
            let definition =
                fasta::record::Definition::new(format!("{0}.{i}", block_group.name), None);
            let fasta_sequence = fasta::record::Sequence::from(sequence.clone().into_bytes());
            let record = fasta::Record::new(definition.clone(), fasta_sequence);

            let _ = writer.write_record(&record);
        }
    }

    println!("Exported to file {}", filename.display());
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::models::{metadata, operations::setup_db};
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use crate::updates::fasta::update_with_fasta;
    use noodles::fasta;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::{io, str};
    use tempfile;

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
            None,
            false,
            conn,
            op_conn,
        )
        .unwrap();
        let tmp_dir = tempfile::tempdir().unwrap().into_path();
        let filename = tmp_dir.join("out.fa");
        export_fasta(conn, &collection, None, &filename, None);

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
            None,
            false,
            conn,
            op_conn,
        )
        .unwrap();
        let _ = update_with_fasta(
            conn,
            op_conn,
            &collection,
            None,
            "child sample",
            "m123",
            2,
            5,
            fasta_update_path.to_str().unwrap(),
        );

        let tmp_dir = tempfile::tempdir().unwrap().into_path();
        let filename = tmp_dir.join("out.fa");
        export_fasta(conn, &collection, Some("child sample"), &filename, None);

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

    #[test]
    fn test_import_fasta_update_with_fasta_export_both_paths() {
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
            None,
            false,
            conn,
            op_conn,
        )
        .unwrap();
        let _ = update_with_fasta(
            conn,
            op_conn,
            &collection,
            None,
            "child sample",
            "m123",
            2,
            5,
            fasta_update_path.to_str().unwrap(),
        );

        let tmp_dir = tempfile::tempdir().unwrap().into_path();
        let filename = tmp_dir.join("out.fa");
        export_fasta(
            conn,
            &collection,
            Some("child sample"),
            &filename,
            Some(1000),
        );

        let mut fasta_reader = fasta::io::reader::Builder
            .build_from_path(filename)
            .unwrap();
        let record1 = fasta_reader
            .records()
            .next()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "No records found in fasta file")
            })
            .unwrap()
            .unwrap();
        let record2 = fasta_reader
            .records()
            .next()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "No records found in fasta file")
            })
            .unwrap()
            .unwrap();

        let sequence1 = str::from_utf8(record1.sequence().as_ref())
            .unwrap()
            .to_string();
        let sequence2 = str::from_utf8(record2.sequence().as_ref())
            .unwrap()
            .to_string();
        let expected_sequences = vec![
            "ATAAAAAAAATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
        ]
        .into_iter()
        .collect::<HashSet<String>>();
        let actual_sequences = vec![sequence1, sequence2]
            .into_iter()
            .collect::<HashSet<String>>();
        assert_eq!(actual_sequences, expected_sequences);
    }
}
