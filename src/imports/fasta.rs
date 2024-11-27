use std::collections::HashMap;
use std::str;

use crate::calculate_hash;
use crate::models::file_types::FileTypes;
use crate::models::{
    block_group::BlockGroup,
    block_group_edge::{BlockGroupEdge, BlockGroupEdgeData},
    collection::Collection,
    edge::Edge,
    node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID},
    operations::Operation,
    path::Path,
    sequence::Sequence,
    strand::Strand,
};
use crate::operation_management::{end_operation, start_operation};
use noodles::fasta;
use rusqlite;
use rusqlite::Connection;

pub fn import_fasta(
    fasta: &String,
    name: &str,
    shallow: bool,
    conn: &Connection,
    operation_conn: &Connection,
) -> Result<Operation, &'static str> {
    let mut session = start_operation(conn);

    let mut reader = fasta::io::reader::Builder.build_from_path(fasta).unwrap();

    let collection = if !Collection::exists(conn, name) {
        Collection::create(conn, name)
    } else {
        Collection {
            name: name.to_string(),
        }
    };
    let mut summary: HashMap<String, i64> = HashMap::new();

    for result in reader.records() {
        let record = result.expect("Error during fasta record parsing");
        let sequence = str::from_utf8(record.sequence().as_ref())
            .unwrap()
            .to_string();
        let name = String::from_utf8(record.name().to_vec()).unwrap();
        let sequence_length = record.sequence().len() as i64;
        let seq = if shallow {
            Sequence::new()
                .sequence_type("DNA")
                .name(&name)
                .file_path(fasta)
                .length(sequence_length)
                .save(conn)
        } else {
            Sequence::new()
                .sequence_type("DNA")
                .sequence(&sequence)
                .save(conn)
        };
        let node_id = Node::create(
            conn,
            &seq.hash,
            calculate_hash(&format!(
                "{collection}.{name}:{hash}",
                collection = collection.name,
                hash = seq.hash
            )),
        );
        let block_group = BlockGroup::create(conn, &collection.name, None, &name);
        let edge_into = Edge::create(
            conn,
            PATH_START_NODE_ID,
            0,
            Strand::Forward,
            node_id,
            0,
            Strand::Forward,
        );
        let edge_out_of = Edge::create(
            conn,
            node_id,
            sequence_length,
            Strand::Forward,
            PATH_END_NODE_ID,
            0,
            Strand::Forward,
        );

        let new_block_group_edges = vec![
            BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: edge_into.id,
                chromosome_index: 0,
            },
            BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: edge_out_of.id,
                chromosome_index: 0,
            },
        ];

        BlockGroupEdge::bulk_create(conn, &new_block_group_edges);
        let path = Path::create(conn, &name, block_group.id, &[edge_into.id, edge_out_of.id]);
        summary.entry(path.name).or_insert(sequence_length);
    }
    let mut summary_str = "".to_string();
    for (path_name, change_count) in summary.iter() {
        summary_str.push_str(&format!(" {path_name}: {change_count} changes.\n"));
    }

    end_operation(
        conn,
        operation_conn,
        &mut session,
        fasta,
        FileTypes::Fasta,
        "fasta_addition",
        &summary_str,
        None,
    )
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::models::metadata;
    use crate::models::operations::setup_db;
    use crate::models::traits::*;
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use std::collections::HashSet;
    use std::path::PathBuf;

    #[test]
    fn test_add_fasta() {
        setup_gen_dir();
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = get_connection(None);
        let db_uuid = metadata::get_db_uuid(&conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            "test",
            false,
            &conn,
            op_conn,
        )
        .unwrap();
        assert_eq!(
            BlockGroup::get_all_sequences(&conn, 1, false),
            HashSet::from_iter(vec!["ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string()])
        );

        let path = Path::get(&conn, 1);
        assert_eq!(
            path.sequence(&conn),
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string()
        );
    }

    #[test]
    fn test_add_fasta_shallow() {
        setup_gen_dir();
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = get_connection(None);
        let db_uuid = metadata::get_db_uuid(&conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            "test",
            true,
            &conn,
            op_conn,
        )
        .unwrap();
        assert_eq!(
            BlockGroup::get_all_sequences(&conn, 1, false),
            HashSet::from_iter(vec!["ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string()])
        );

        let path = Path::get(&conn, 1);
        assert_eq!(
            path.sequence(&conn),
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string()
        );
    }

    #[test]
    fn test_deduplicates_nodes() {
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
        )
        .unwrap();
        assert_eq!(
            Node::query(conn, "select * from nodes;", rusqlite::params!()).len(),
            3
        );
        assert_eq!(
            import_fasta(
                &fasta_path.to_str().unwrap().to_string(),
                &collection,
                false,
                conn,
                op_conn,
            ),
            Err("No changes.")
        );
    }
}
