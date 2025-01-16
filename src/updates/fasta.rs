use noodles::fasta;
use rusqlite;
use rusqlite::{types::Value as SQLValue, Connection};
use std::{io, str};

use crate::models::operations::{OperationFile, OperationInfo};
use crate::models::{
    block_group::{BlockGroup, PathChange},
    edge::Edge,
    file_types::FileTypes,
    node::Node,
    path::PathBlock,
    sample::Sample,
    sequence::Sequence,
    strand::Strand,
    traits::*,
};
use crate::{calculate_hash, operation_management};

#[allow(clippy::too_many_arguments)]
pub fn update_with_fasta(
    conn: &Connection,
    operation_conn: &Connection,
    collection_name: &str,
    parent_sample_name: Option<&str>,
    new_sample_name: &str,
    region_name: &str,
    start_coordinate: i64,
    end_coordinate: i64,
    fasta_file_path: &str,
) -> io::Result<()> {
    let mut session = operation_management::start_operation(conn);

    let mut fasta_reader = fasta::io::reader::Builder.build_from_path(fasta_file_path)?;

    let _new_sample = Sample::get_or_create(conn, new_sample_name);
    let block_groups = Sample::get_block_groups(conn, collection_name, parent_sample_name);

    let mut new_block_group_id = 0;
    for block_group in block_groups {
        let new_bg_id = BlockGroup::get_or_create_sample_block_group(
            conn,
            collection_name,
            new_sample_name,
            &block_group.name,
            parent_sample_name,
        )
        .unwrap();
        if block_group.name == region_name {
            new_block_group_id = new_bg_id;
        }
    }

    if new_block_group_id == 0 {
        panic!("No region found with name: {}", region_name);
    }

    let path = BlockGroup::get_current_path(conn, new_block_group_id);

    // Assuming just one entry in the fasta file
    let record = fasta_reader.records().next().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "No records found in fasta file")
    })??;

    let sequence = str::from_utf8(record.sequence().as_ref())
        .unwrap()
        .to_string();
    let seq = Sequence::new()
        .sequence_type("DNA")
        .sequence(&sequence)
        .save(conn);
    let node_id = Node::create(
        conn,
        &seq.hash,
        calculate_hash(&format!(
            "{path_id}:{ref_start}-{ref_end}->{sequence_hash}",
            path_id = path.id,
            ref_start = 0,
            ref_end = seq.length,
            sequence_hash = seq.hash
        )),
    );

    let path_block = PathBlock {
        id: -1,
        node_id,
        block_sequence: sequence,
        sequence_start: 0,
        sequence_end: seq.length,
        path_start: start_coordinate,
        path_end: end_coordinate,
        strand: Strand::Forward,
        phase_layer_id: 0,
    };

    let path_change = PathChange {
        block_group_id: new_block_group_id,
        path: path.clone(),
        path_accession: None,
        start: start_coordinate,
        end: end_coordinate,
        block: path_block,
        chromosome_index: 0,
        phased: 0,
    };

    let interval_tree = path.intervaltree(conn);
    BlockGroup::insert_change(conn, &path_change, &interval_tree);

    let edge_to_new_node = Edge::query(
        conn,
        "select * from edges where target_node_id = ?1",
        rusqlite::params!(SQLValue::from(node_id)),
    )[0]
    .clone();
    let edge_from_new_node = Edge::query(
        conn,
        "select * from edges where source_node_id = ?1",
        rusqlite::params!(SQLValue::from(node_id)),
    )[0]
    .clone();
    // TODO: Uncomment
    // let new_path = path.new_path_with(
    //     conn,
    //     start_coordinate,
    //     end_coordinate,
    //     &edge_to_new_node,
    //     &edge_from_new_node,
    // );

    //    let summary_str = format!(" {}: 1 change", new_path.name);
    let summary_str = format!(" {}: 1 change", "foo");
    operation_management::end_operation(
        conn,
        operation_conn,
        &mut session,
        &OperationInfo {
            files: vec![OperationFile {
                file_path: fasta_file_path.to_string(),
                file_type: FileTypes::Fasta,
            }],
            description: "fasta_update".to_string(),
        },
        &summary_str,
        None,
    )
    .unwrap();

    println!("Updated with fasta file: {}", fasta_file_path);

    Ok(())
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::models::{metadata, operations::setup_db};
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use std::collections::HashSet;
    use std::path::PathBuf;

    #[test]
    fn test_update_with_fasta() {
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

        let expected_sequences = vec![
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATAAAAAAAATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
        ];
        let block_groups = BlockGroup::query(
            conn,
            "select * from block_groups where collection_name = ?1 AND sample_name = ?2;",
            rusqlite::params!(
                SQLValue::from(collection),
                SQLValue::from("child sample".to_string()),
            ),
        );
        assert_eq!(block_groups.len(), 1);
        assert_eq!(
            BlockGroup::get_all_sequences(conn, block_groups[0].id, false),
            HashSet::from_iter(expected_sequences),
        );
    }

    #[test]
    fn test_update_within_update() {
        /*
        Graph after fasta updates:
        AT --------------> CGA ----------------> TCGATCGATCGATCGGGAACACACAGAGA
            \-> AA -----> AA -------> AAAA --/
                   \--> TTTTTTTT --/
        */
        setup_gen_dir();
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let mut fasta_update1_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_update1_path.push("fixtures/aaaaaaaa.fa");
        let mut fasta_update2_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_update2_path.push("fixtures/tttttttt.fa");
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        let collection = "test".to_string();

        let _ = import_fasta(
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
            fasta_update1_path.to_str().unwrap(),
        );
        // Second fasta update replacing part of the first update sequence
        let _ = update_with_fasta(
            conn,
            op_conn,
            &collection,
            Some("child sample"),
            "grandchild sample",
            "m123",
            4,
            6,
            fasta_update2_path.to_str().unwrap(),
        );
        let expected_sequences = vec![
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATAAAAAAAATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATAATTTTTTTTAAAATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
        ];
        let block_groups = BlockGroup::query(
            conn,
            "select * from block_groups where collection_name = ?1 AND sample_name = ?2;",
            rusqlite::params!(
                SQLValue::from(collection),
                SQLValue::from("grandchild sample".to_string()),
            ),
        );
        assert_eq!(block_groups.len(), 1);
        assert_eq!(
            BlockGroup::get_all_sequences(conn, block_groups[0].id, false),
            HashSet::from_iter(expected_sequences),
        );
    }

    #[test]
    fn test_update_with_two_fastas_partial_leading_overlap() {
        /*
        Graph after fasta updates:
        A --> T --------------> CGA ----------------> TCGATCGATCGATCGGGAACACACAGAGA
         \       \-> AAAA -------> AAAA --/
          \--> TTTTTTTT --/
        */
        setup_gen_dir();
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let mut fasta_update1_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_update1_path.push("fixtures/aaaaaaaa.fa");
        let mut fasta_update2_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_update2_path.push("fixtures/tttttttt.fa");
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
            fasta_update1_path.to_str().unwrap(),
        );
        // Second fasta update replacing parts of both the original and first update sequences
        let _ = update_with_fasta(
            conn,
            op_conn,
            &collection,
            Some("child sample"),
            "grandchild sample",
            "m123",
            1,
            6,
            fasta_update2_path.to_str().unwrap(),
        );
        let expected_sequences = vec![
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATAAAAAAAATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATTTTTTTTAAAATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
        ];
        let block_groups = BlockGroup::query(
            conn,
            "select * from block_groups where collection_name = ?1 AND sample_name = ?2;",
            rusqlite::params!(
                SQLValue::from(collection),
                SQLValue::from("grandchild sample".to_string()),
            ),
        );
        assert_eq!(block_groups.len(), 1);
        assert_eq!(
            BlockGroup::get_all_sequences(conn, block_groups[0].id, false),
            HashSet::from_iter(expected_sequences),
        );
    }

    #[test]
    fn test_update_with_two_fastas_partial_trailing_overlap() {
        /*
        Graph after fasta updates:
        A --> T --------------> CGA ----------------> TC --> GATCGATCGATCGGGAACACACAGAGA
         \       \-----> AAAAAAAA ---------/             /
          \-------------> TTTTTTTT ---------------------/
        */
        /*
        Graph after fasta updates:
        AT --------------> CGA ------------> TC --> GATCGATCGATCGGGAACACACAGAGA
              \-> AAAA -------> AAAA ----/        /
                           \--> TTTTTTTT --------/
        */
        setup_gen_dir();
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let mut fasta_update1_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_update1_path.push("fixtures/aaaaaaaa.fa");
        let mut fasta_update2_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_update2_path.push("fixtures/tttttttt.fa");
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
            fasta_update1_path.to_str().unwrap(),
        );
        // Second fasta update replacing parts of both the original and first update sequences
        let _ = update_with_fasta(
            conn,
            op_conn,
            &collection,
            Some("child sample"),
            "grandchild sample",
            "m123",
            1,
            12,
            fasta_update2_path.to_str().unwrap(),
        );
        let expected_sequences = vec![
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATAAAAAAAATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATTTTTTTTGATCGATCGATCGGGAACACACAGAGA".to_string(),
        ];
        let block_groups = BlockGroup::query(
            conn,
            "select * from block_groups where collection_name = ?1 AND sample_name = ?2;",
            rusqlite::params!(
                SQLValue::from(collection),
                SQLValue::from("grandchild sample".to_string()),
            ),
        );
        assert_eq!(block_groups.len(), 1);
        assert_eq!(
            BlockGroup::get_all_sequences(conn, block_groups[0].id, false),
            HashSet::from_iter(expected_sequences),
        );
    }

    #[test]
    fn test_update_with_two_fastas_second_over_first() {
        /*
        Graph after fasta updates:
        AT --------------> CGA ------------> TC --> GATCGATCGATCGGGAACACACAGAGA
              \-> AAAA -------> AAAA ----/        /
                           \--> TTTTTTTT --------/
        */
        setup_gen_dir();
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let mut fasta_update1_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_update1_path.push("fixtures/aaaaaaaa.fa");
        let mut fasta_update2_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_update2_path.push("fixtures/tttttttt.fa");
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
            fasta_update1_path.to_str().unwrap(),
        );
        // Second fasta update replacing parts of both the original and first update sequences
        let _ = update_with_fasta(
            conn,
            op_conn,
            &collection,
            Some("child sample"),
            "grandchild sample",
            "m123",
            6,
            12,
            fasta_update2_path.to_str().unwrap(),
        );
        let expected_sequences = vec![
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATAAAAAAAATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATAAAATTTTTTTTGATCGATCGATCGGGAACACACAGAGA".to_string(),
        ];
        let block_groups = BlockGroup::query(
            conn,
            "select * from block_groups where collection_name = ?1 AND sample_name = ?2;",
            rusqlite::params!(
                SQLValue::from(collection),
                SQLValue::from("grandchild sample".to_string()),
            ),
        );
        assert_eq!(block_groups.len(), 1);
        assert_eq!(
            BlockGroup::get_all_sequences(conn, block_groups[0].id, false),
            HashSet::from_iter(expected_sequences),
        );
    }

    #[test]
    fn test_update_with_same_fasta_twice() {
        /*
        Graph after fasta updates:
        AT --------------> CGA ----------------> TCGATCGATCGATCGGGAACACACAGAGA
            \-> AA -----> AA -------> AAAA --/
                   \--> AAAAAAAA --/
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
        // Same fasta second time
        let _ = update_with_fasta(
            conn,
            op_conn,
            &collection,
            Some("child sample"),
            "grandchild sample",
            "m123",
            4,
            6,
            fasta_update_path.to_str().unwrap(),
        );
        let expected_sequences = vec![
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATAAAAAAAATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATAAAAAAAAAAAAAATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
        ];
        let block_groups = BlockGroup::query(
            conn,
            "select * from block_groups where collection_name = ?1 AND sample_name = ?2;",
            rusqlite::params!(
                SQLValue::from(collection),
                SQLValue::from("grandchild sample".to_string()),
            ),
        );
        assert_eq!(block_groups.len(), 1);
        assert_eq!(
            BlockGroup::get_all_sequences(conn, block_groups[0].id, false),
            HashSet::from_iter(expected_sequences),
        );
    }
}
