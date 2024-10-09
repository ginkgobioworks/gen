use csv;
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
use crate::operation_management;

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
        "csv_update",
        change.id,
    );

    let mut parts_reader = fasta::io::reader::Builder.build_from_path(parts_file_path)?;

    let mut node_ids_by_name = HashMap::new();
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
        let node_id = Node::create(conn, &seq.hash);
        node_ids_by_name.insert(name, node_id);
    }

    let library_file = File::open(library_file_path)?;
    let library_reader = BufReader::new(library_file);

    let mut parts1 = vec![];
    let mut parts2 = vec![];
    let mut library_csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(library_reader);
    for result in library_csv_reader.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here..
        let record = result?;
        println!("{:?}", record);
        let part1_name = record.get(0).unwrap();
        let part2_name = record.get(1).unwrap();
        let part1_id = node_ids_by_name.get(part1_name).unwrap();
        let part2_id = node_ids_by_name.get(part2_name).unwrap();
        parts1.push(part1_id);
        parts2.push(part2_id);
    }

    let path = Path::get_paths(
        conn,
        "select * from path where name = ?1",
        vec![SQLValue::from(path_name.to_string())],
    )[0]
    .clone();

    let path_intervaltree = Path::intervaltree_for(conn, &path);
    let start_blocks: Vec<_> = path_intervaltree
        .query_point(start_coordinate)
        .map(|x| &x.value)
        .collect();
    assert_eq!(start_blocks.len(), 1);
    let start_block = start_blocks[0];
    // TODO: Get this right
    let node_start_coordinate = start_coordinate - start_block.path_start;
    let end_blocks: Vec<_> = path_intervaltree
        .query_point(end_coordinate)
        .map(|x| &x.value)
        .collect();
    assert_eq!(end_blocks.len(), 1);
    let end_block = end_blocks[0];
    // TODO: Get this right
    let node_end_coordinate = end_coordinate - end_block.path_start;

    let mut new_edges = HashSet::new();
    for part1 in &parts1 {
        let edge = EdgeData {
            source_node_id: start_block.node_id,
            source_coordinate: node_start_coordinate,
            source_strand: Strand::Forward,
            target_node_id: **part1,
            target_coordinate: 0,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };
        new_edges.insert(edge);
    }

    for part2 in &parts2 {
        let edge = EdgeData {
            source_node_id: **part2,
            // TODO: Fix this
            source_coordinate: 0,
            source_strand: Strand::Forward,
            target_node_id: end_block.node_id,
            target_coordinate: node_end_coordinate,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };
        new_edges.insert(edge);
    }

    for part1 in parts1 {
        for part2 in &parts2 {
            let edge = EdgeData {
                source_node_id: *part1,
                // TODO: Fix this
                source_coordinate: 0,
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

    let new_edge_ids = Edge::bulk_create(conn, new_edges.iter().cloned().collect());
    BlockGroupEdge::bulk_create(conn, path.block_group_id, &new_edge_ids);

    //    OperationSummary::create(operation_conn, operation.id, &summary_str);
    println!("Updated with library file: {}", library_file_path);
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    operation_management::write_changeset(conn, &operation, &output);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        block_group::{BlockGroup, PathChange},
        node::Node,
        path::PathBlock,
        sequence::Sequence,
    };
    use crate::test_helpers::{get_connection, setup_block_group};

    // TODO: Get this right
    //    #[test]
    fn makes_a_pool() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let promoter1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("pro1")
            .save(&conn);
        let promoter1_node_id = Node::create(&conn, promoter1.hash.as_str());
        let promoter2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("pro2")
            .save(&conn);
        let promoter2_node_id = Node::create(&conn, promoter2.hash.as_str());
        let gene1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("gen1")
            .save(&conn);
        let gene1_node_id = Node::create(&conn, gene1.hash.as_str());
        let gene2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("gen2")
            .save(&conn);
        let gene2_node_id = Node::create(&conn, gene2.hash.as_str());
        let promoter1_block = PathBlock {
            id: 0,
            node_id: promoter1_node_id,
            block_sequence: promoter1.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: Strand::Forward,
        };
        let promoter2_block = PathBlock {
            id: 0,
            node_id: promoter2_node_id,
            block_sequence: promoter2.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: Strand::Forward,
        };
        let gene1_block = PathBlock {
            id: 0,
            node_id: gene1_node_id,
            block_sequence: gene1.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 15,
            path_end: 20,
            strand: Strand::Forward,
        };
        let gene2_block = PathBlock {
            id: 0,
            node_id: gene2_node_id,
            block_sequence: gene2.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 15,
            path_end: 20,
            strand: Strand::Forward,
        };
        let changes = vec![
            PathChange {
                block_group_id,
                path: path.clone(),
                start: 7,
                end: 15,
                block: promoter1_block,
                chromosome_index: 1,
                phased: 0,
            },
            PathChange {
                block_group_id,
                path: path.clone(),
                start: 7,
                end: 15,
                block: promoter2_block,
                chromosome_index: 1,
                phased: 0,
            },
            PathChange {
                block_group_id,
                path: path.clone(),
                start: 15,
                end: 20,
                block: gene1_block,
                chromosome_index: 1,
                phased: 0,
            },
            PathChange {
                block_group_id,
                path: path.clone(),
                start: 15,
                end: 20,
                block: gene2_block,
                chromosome_index: 1,
                phased: 0,
            },
        ];
        let tree = Path::intervaltree_for(&conn, &path);
        for change in changes.iter() {
            BlockGroup::insert_change(&conn, change, &tree);
        }

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAApro1TTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTgen1CCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAApro1gen1CCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAApro2TTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTgen2CCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAApro1gen2CCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAApro2gen1CCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAApro2gen2CCCCCCCCCCGGGGGGGGGG".to_string(),
            ])
        );
    }
}
