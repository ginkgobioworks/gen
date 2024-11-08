use csv;
use itertools::Itertools;
use noodles::fasta;
use rusqlite::{session, types::Value as SQLValue, Connection};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::str;

use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::edge::{Edge, EdgeData};
use crate::models::file_types::FileTypes;
use crate::models::metadata;
use crate::models::node::Node;
use crate::models::operations::{FileAddition, Operation, OperationSummary};
use crate::models::path::Path;
use crate::models::sequence::Sequence;
use crate::models::strand::Strand;
use crate::models::traits::*;
use crate::{calculate_hash, operation_management};

#[allow(clippy::too_many_arguments)]
pub fn update_with_library(
    conn: &Connection,
    operation_conn: &Connection,
    name: &str,
    path_name: &str,
    start_coordinate: i64,
    end_coordinate: i64,
    parts_file_path: &str,
    library_file_path: &str,
) -> std::io::Result<()> {
    let mut session = session::Session::new(conn).unwrap();
    operation_management::attach_session(&mut session);
    let change = FileAddition::create(operation_conn, library_file_path, FileTypes::CSV);

    let db_uuid = metadata::get_db_uuid(conn);

    let operation = Operation::create(
        operation_conn,
        &db_uuid,
        name.to_string(),
        "library_csv_update",
        change.id,
    );

    let mut parts_reader = fasta::io::reader::Builder.build_from_path(parts_file_path)?;

    let path = Path::query(
        conn,
        "select * from paths where name = ?1",
        rusqlite::params!(SQLValue::from(path_name.to_string())),
    )[0]
    .clone();

    let mut node_ids_by_name = HashMap::new();
    let mut sequence_lengths_by_node_id = HashMap::new();
    for result in parts_reader.records() {
        let record = result?;
        let sequence = str::from_utf8(record.sequence().as_ref())
            .unwrap()
            .to_string();
        let name = String::from_utf8(record.name().to_vec()).unwrap();
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

        node_ids_by_name.insert(name, node_id);
        sequence_lengths_by_node_id.insert(node_id, seq.length);
    }

    let library_file = File::open(library_file_path)?;
    let library_reader = BufReader::new(library_file);

    let mut parts_by_index = HashMap::new();
    let mut library_csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(library_reader);
    let mut max_index = 0;
    for result in library_csv_reader.records() {
        let record = result?;
        for (index, part) in record.iter().enumerate() {
            if !part.is_empty() {
                let part_id = node_ids_by_name.get(part).unwrap();
                parts_by_index.entry(index).or_insert(vec![]).push(part_id);
                if index >= max_index {
                    max_index = index + 1;
                }
            }
        }
    }

    let mut parts_list = vec![];
    for index in 0..max_index {
        parts_list.push(parts_by_index.get(&index).unwrap());
    }

    let path_intervaltree = path.intervaltree(conn);
    let start_blocks: Vec<_> = path_intervaltree
        .query_point(start_coordinate)
        .map(|x| &x.value)
        .collect();
    assert_eq!(start_blocks.len(), 1);
    let start_block = start_blocks[0];
    let node_start_coordinate = start_coordinate - start_block.start + start_block.sequence_start;

    let end_blocks: Vec<_> = path_intervaltree
        .query_point(end_coordinate)
        .map(|x| &x.value)
        .collect();
    assert_eq!(end_blocks.len(), 1);
    let end_block = end_blocks[0];
    let node_end_coordinate = end_coordinate - end_block.start + end_block.sequence_start;

    let mut new_edges = HashSet::new();
    let start_parts = parts_list.first().unwrap();
    for start_part in *start_parts {
        let edge = EdgeData {
            source_node_id: start_block.node_id,
            source_coordinate: node_start_coordinate,
            source_strand: Strand::Forward,
            target_node_id: **start_part,
            target_coordinate: 0,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };
        new_edges.insert(edge);
    }

    let end_parts = parts_list.last().unwrap();
    for end_part in *end_parts {
        let end_part_source_coordinate = sequence_lengths_by_node_id.get(end_part).unwrap();
        let edge = EdgeData {
            source_node_id: **end_part,
            source_coordinate: *end_part_source_coordinate,
            source_strand: Strand::Forward,
            target_node_id: end_block.node_id,
            target_coordinate: node_end_coordinate,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };
        new_edges.insert(edge);
    }

    let mut path_changes_count = 1;
    for (parts1, parts2) in parts_list.iter().tuple_windows() {
        path_changes_count *= parts1.len();
        for part1 in *parts1 {
            for part2 in *parts2 {
                let part1_source_coordinate = sequence_lengths_by_node_id.get(part1).unwrap();
                let edge = EdgeData {
                    source_node_id: **part1,
                    source_coordinate: *part1_source_coordinate,
                    source_strand: Strand::Forward,
                    target_node_id: **part2,
                    target_coordinate: 0,
                    target_strand: Strand::Forward,
                    chromosome_index: 0,
                    phased: 0,
                };
                new_edges.insert(edge);
            }
        }
    }

    path_changes_count *= end_parts.len();

    let new_edge_ids = Edge::bulk_create(conn, &new_edges.iter().cloned().collect());
    BlockGroupEdge::bulk_create(conn, path.block_group_id, &new_edge_ids);

    let summary_str = format!("{path_name}: {path_changes_count} changes.\n");
    OperationSummary::create(operation_conn, operation.id, &summary_str);

    println!("Updated with library file: {}", library_file_path);
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    operation_management::write_changeset(conn, &operation, &output);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::models::{block_group::BlockGroup, collection::Collection, operations::setup_db};
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use std::path::PathBuf;

    #[test]
    fn makes_a_pool() {
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

        let mut parts_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        parts_path.push("fixtures/parts.fa");
        let mut library_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        library_path.push("fixtures/combinatorial_design.csv");

        let _ = update_with_library(
            conn,
            op_conn,
            "test",
            "m123",
            7,
            20,
            parts_path.to_str().unwrap(),
            library_path.to_str().unwrap(),
        );

        let block_groups = Collection::get_block_groups(conn, "test");
        let block_group = &block_groups[0];

        let all_sequences = BlockGroup::get_all_sequences(conn, block_group.id, false);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
                "ATCGATCAAAAATGATAAGGAACACACAGAGA".to_string(),
                "ATCGATCAAAAATGTTAAGGAACACACAGAGA".to_string(),
                "ATCGATCAAAAATGCTAAGGAACACACAGAGA".to_string(),
                "ATCGATCTAATATGATAAGGAACACACAGAGA".to_string(),
                "ATCGATCTAATATGTTAAGGAACACACAGAGA".to_string(),
                "ATCGATCTAATATGCTAAGGAACACACAGAGA".to_string(),
                "ATCGATCCAACATGATAAGGAACACACAGAGA".to_string(),
                "ATCGATCCAACATGTTAAGGAACACACAGAGA".to_string(),
                "ATCGATCCAACATGCTAAGGAACACACAGAGA".to_string(),
            ])
        );
    }
}
