use crate::models::operations::OperationFile;
use crate::models::{
    block_group::BlockGroup,
    block_group_edge::BlockGroupEdge,
    file_types::FileTypes,
    node::{PATH_END_NODE_ID, PATH_START_NODE_ID},
    operations::OperationInfo,
    path::Path,
    path_edge::PathEdge,
    sample::Sample,
};
use crate::operation_management;
use core::ops::Range;
use rusqlite::Connection;
use std::collections::HashMap;
use std::io;

#[allow(clippy::too_many_arguments)]
pub fn derive_subgraph(
    conn: &Connection,
    operation_conn: &Connection,
    collection_name: &str,
    parent_sample_name: Option<&str>,
    new_sample_name: &str,
    region_name: &str,
    start_coordinate: i64,
    end_coordinate: i64,
) -> io::Result<()> {
    let mut session = operation_management::start_operation(conn);
    let _new_sample = Sample::get_or_create(conn, new_sample_name);
    let block_groups = Sample::get_block_groups(conn, collection_name, parent_sample_name);

    let mut parent_block_group_id = 0;
    let mut new_block_group_id = 0;
    for block_group in block_groups {
        if block_group.name == region_name {
            parent_block_group_id = block_group.id;
            let new_block_group = BlockGroup::create(
                conn,
                collection_name,
                Some(new_sample_name),
                &block_group.name,
            );
            new_block_group_id = new_block_group.id;
        }
    }

    if new_block_group_id == 0 {
        panic!("No region found with name: {}", region_name);
    }

    let current_path = BlockGroup::get_current_path(conn, parent_block_group_id);
    let current_path_length = current_path.sequence(conn).len() as i64;
    if (start_coordinate < 0 || start_coordinate > current_path_length)
        || (end_coordinate < 0 || end_coordinate > current_path_length)
    {
        panic!("Start and/or end coordinates are out of range for the current path.");
    }
    let current_intervaltree = current_path.intervaltree(conn);
    let mut blocks = current_intervaltree
        .query(Range {
            start: start_coordinate,
            end: end_coordinate,
        })
        .map(|x| x.value)
        .collect::<Vec<_>>();
    blocks.sort_by(|a, b| a.start.cmp(&b.start));
    let start_block = blocks[0];
    let start_node_coordinate = start_coordinate - start_block.start + start_block.sequence_start;
    let end_block = blocks[blocks.len() - 1];
    let end_node_coordinate = end_coordinate - end_block.start + end_block.sequence_start;

    let child_block_group_id = BlockGroup::derive_subgraph(
        conn,
        parent_block_group_id,
        &start_block,
        &end_block,
        start_node_coordinate,
        end_node_coordinate,
        new_block_group_id,
    );

    let current_edges = PathEdge::block_group_edges_for_path(conn, current_path.id);
    let child_block_group_edges = BlockGroupEdge::edges_for_block_group(conn, child_block_group_id);
    let new_start_edge = child_block_group_edges
        .iter()
        .find(|x| x.edge.source_node_id == PATH_START_NODE_ID)
        .unwrap();
    let new_end_edge = child_block_group_edges
        .iter()
        .find(|x| x.edge.target_node_id == PATH_END_NODE_ID)
        .unwrap();
    let child_edges_by_id = child_block_group_edges
        .iter()
        .map(|x| (x.edge.id, x))
        .collect::<HashMap<_, _>>();

    let mut new_path_block_group_edge_ids = vec![];
    new_path_block_group_edge_ids.push(new_start_edge.block_group_edge_id);
    for current_edge in current_edges {
        let child_edge = child_edges_by_id.get(&current_edge.edge_id);
        if let Some(child_edge) = child_edge {
            new_path_block_group_edge_ids.push(child_edge.block_group_edge_id);
        }
    }
    new_path_block_group_edge_ids.push(new_end_edge.block_group_edge_id);
    Path::create(
        conn,
        &current_path.name,
        child_block_group_id,
        &new_path_block_group_edge_ids,
    );

    let summary_str = format!(" {}: 1 new derived block group", new_sample_name);
    operation_management::end_operation(
        conn,
        operation_conn,
        &mut session,
        &OperationInfo {
            files: vec![OperationFile {
                file_path: "".to_string(),
                file_type: FileTypes::None,
            }],
            description: "derive subgraph".to_string(),
        },
        &summary_str,
        None,
    )
    .unwrap();

    println!("Derived subgraph successfully.");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        block_group_edge::BlockGroupEdgeData, collection::Collection, edge::Edge, metadata,
        node::Node, operations::setup_db, sequence::Sequence, strand::Strand,
    };
    use crate::test_helpers::{
        get_connection, get_operation_connection, setup_block_group, setup_gen_dir,
    };
    use std::collections::HashSet;

    #[test]
    fn test_derive_subgraph_one_insertion() {
        /*
        AAAAAAAAAA -> TTTTTTTTTT -> CCCCCCCCCC -> GGGGGGGGGG
                          \-> AAAAAAAA ->/
        Subgraph range:  |-----------------|
        Sequences of the subgraph are TAAAAAAAAC, TTTTTCCCCC
         */
        setup_gen_dir();
        let conn = &get_connection(None);
        let op_conn = &get_operation_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        setup_db(op_conn, &db_uuid);
        Collection::create(conn, "test");
        let (block_group1_id, original_path) = setup_block_group(conn);

        let intervaltree = original_path.intervaltree(conn);
        let insert_start_node_id = intervaltree.query_point(16).next().unwrap().value.node_id;
        let insert_end_node_id = intervaltree.query_point(24).next().unwrap().value.node_id;

        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let insert_node_id = Node::create(conn, insert_sequence.hash.as_str(), None);
        let edge_into_insert = Edge::create(
            conn,
            insert_start_node_id,
            6,
            Strand::Forward,
            insert_node_id,
            0,
            Strand::Forward,
        );
        let edge_out_of_insert = Edge::create(
            conn,
            insert_node_id,
            8,
            Strand::Forward,
            insert_end_node_id,
            4,
            Strand::Forward,
        );

        let edge_ids = [edge_into_insert.id, edge_out_of_insert.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group1_id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let insert_path = original_path.new_path_with(
            conn,
            16,
            24,
            block_group_edge_ids[0],
            block_group_edge_ids[1],
            insert_node_id,
        );
        assert_eq!(
            insert_path.sequence(conn),
            "AAAAAAAAAATTTTTTAAAAAAAACCCCCCGGGGGGGGGG"
        );

        let all_sequences = BlockGroup::get_all_sequences(conn, block_group1_id, false);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTAAAAAAAACCCCCCGGGGGGGGGG".to_string(),
            ])
        );

        derive_subgraph(conn, op_conn, "test", None, "test", "chr1", 15, 25).unwrap();

        let block_groups = Sample::get_block_groups(conn, "test", Some("test"));
        let block_group2 = block_groups.iter().find(|x| x.name == "chr1").unwrap();

        let all_sequences2 = BlockGroup::get_all_sequences(conn, block_group2.id, false);
        assert_eq!(
            all_sequences2,
            HashSet::from_iter(vec!["TTTTTCCCCC".to_string(), "TAAAAAAAAC".to_string(),])
        );

        let new_path = BlockGroup::get_current_path(conn, block_group2.id);
        assert_eq!(new_path.sequence(conn), "TAAAAAAAAC");
    }
}
