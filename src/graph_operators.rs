use crate::models::operations::OperationFile;
use crate::models::{
    block_group::BlockGroup, block_group_edge::BlockGroupEdge, file_types::FileTypes,
    operations::OperationInfo, path::Path, path_edge::PathEdge, sample::Sample,
};
use crate::operation_management;
use core::ops::Range;
use rusqlite::Connection;
use std::collections::HashSet;
use std::io;

fn get_path(conn: &Connection, block_group_id: i64, backbone: Option<&str>) -> Path {
    if let Some(backbone) = backbone {
        let path = BlockGroup::get_path_by_name(conn, block_group_id, backbone);
        if path.is_none() {
            panic!("No path found with name {}", backbone);
        }
        path.unwrap()
    } else {
        BlockGroup::get_current_path(conn, block_group_id)
    }
}

pub fn get_sized_ranges(
    conn: &Connection,
    collection_name: &str,
    parent_sample_name: Option<&str>,
    new_sample_name: &str,
    region_name: &str,
    backbone: Option<&str>,
    chunk_size: i64,
) -> Vec<Range<i64>> {
    let _new_sample = Sample::get_or_create(conn, new_sample_name);
    let (parent_block_group_id, _new_block_group_id) = get_parent_and_new_block_groups(
        conn,
        collection_name,
        parent_sample_name,
        new_sample_name,
        region_name,
    );

    let current_path = get_path(conn, parent_block_group_id, backbone);

    let path_length = current_path.sequence(conn).len() as i64;

    let mut range_start = 0;
    let chunk_count = path_length / chunk_size;
    let chunk_remainder = path_length % chunk_size;

    let mut chunk_ranges = vec![];
    for _ in 0..chunk_count {
        chunk_ranges.push(Range {
            start: range_start,
            end: range_start + chunk_size,
        });
        range_start += chunk_size;
    }
    if chunk_remainder > 0 {
        chunk_ranges.push(Range {
            start: range_start,
            end: path_length,
        });
    }

    chunk_ranges
}

pub fn get_breakpoint_ranges(
    conn: &Connection,
    collection_name: &str,
    parent_sample_name: Option<&str>,
    new_sample_name: &str,
    region_name: &str,
    backbone: Option<&str>,
    breakpoints: &str,
) -> Vec<Range<i64>> {
    let _new_sample = Sample::get_or_create(conn, new_sample_name);
    let (parent_block_group_id, _new_block_group_id) = get_parent_and_new_block_groups(
        conn,
        collection_name,
        parent_sample_name,
        new_sample_name,
        region_name,
    );

    let current_path = get_path(conn, parent_block_group_id, backbone);

    let path_length = current_path.sequence(conn).len() as i64;

    let parsed_breakpoints = breakpoints
        .split(",")
        .map(|x| x.parse::<i64>().unwrap())
        .collect::<Vec<i64>>();

    let mut range_start = 0;
    let mut chunk_ranges = vec![];
    for breakpoint in parsed_breakpoints {
        chunk_ranges.push(Range {
            start: range_start,
            end: breakpoint,
        });
        range_start = breakpoint;
    }
    chunk_ranges.push(Range {
        start: range_start,
        end: path_length,
    });

    chunk_ranges
}

#[allow(clippy::too_many_arguments)]
pub fn derive_chunks(
    conn: &Connection,
    operation_conn: &Connection,
    collection_name: &str,
    parent_sample_name: Option<&str>,
    new_sample_name: &str,
    region_name: &str,
    backbone: Option<&str>,
    chunk_ranges: Vec<Range<i64>>,
) -> io::Result<()> {
    let mut session = operation_management::start_operation(conn);
    let _new_sample = Sample::get_or_create(conn, new_sample_name);
    let (parent_block_group_id, new_block_group_id) = get_parent_and_new_block_groups(
        conn,
        collection_name,
        parent_sample_name,
        new_sample_name,
        region_name,
    );

    let current_path = get_path(conn, parent_block_group_id, backbone);

    let current_path_length = current_path.sequence(conn).len() as i64;
    let current_intervaltree = current_path.intervaltree(conn);
    let current_edges = PathEdge::edges_for_path(conn, current_path.id);

    for chunk_range in &chunk_ranges {
        let start_coordinate = chunk_range.start;
        let end_coordinate = chunk_range.end;
        if (start_coordinate < 0 || start_coordinate > current_path_length)
            || (end_coordinate < 0 || end_coordinate > current_path_length)
        {
            panic!(
                "Start and/or end coordinates ({}, {}) are out of range for the current path.",
                start_coordinate, end_coordinate
            );
        }

        let mut blocks = current_intervaltree
            .query(Range {
                start: start_coordinate,
                end: end_coordinate,
            })
            .map(|x| x.value)
            .collect::<Vec<_>>();
        blocks.sort_by(|a, b| a.start.cmp(&b.start));
        let start_block = blocks[0];
        let start_node_coordinate =
            start_coordinate - start_block.start + start_block.sequence_start;
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

        let child_block_group_edges =
            BlockGroupEdge::edges_for_block_group(conn, child_block_group_id);
        let new_edge_id_set = child_block_group_edges
            .iter()
            .map(|x| x.edge.id)
            .collect::<HashSet<i64>>();

        let mut new_path_edge_ids = vec![];
        for current_edge in &current_edges {
            if new_edge_id_set.contains(&current_edge.id) {
                new_path_edge_ids.push(current_edge.id);
            }
        }
        Path::create(
            conn,
            &current_path.name,
            child_block_group_id,
            &new_path_edge_ids,
        );
    }

    let summary_str = format!(
        " {}: {} new derived block group(s)",
        new_sample_name,
        chunk_ranges.len()
    );
    operation_management::end_operation(
        conn,
        operation_conn,
        &mut session,
        &OperationInfo {
            files: vec![OperationFile {
                file_path: "".to_string(),
                file_type: FileTypes::None,
            }],
            description: "derive chunks".to_string(),
        },
        &summary_str,
        None,
    )
    .unwrap();

    println!("Derived chunks successfully.");

    Ok(())
}

fn get_parent_and_new_block_groups(
    conn: &Connection,
    collection_name: &str,
    parent_sample_name: Option<&str>,
    new_sample_name: &str,
    region_name: &str,
) -> (i64, i64) {
    let block_groups = Sample::get_block_groups(conn, collection_name, parent_sample_name);

    let mut parent_block_group_id = 0;
    let mut new_block_group_id = 0;
    for block_group in &block_groups {
        if block_group.name == region_name {
            parent_block_group_id = block_group.id;
            println!("here10");
            println!("collection_name: {}", collection_name);
            println!("new_sample_name: {}", new_sample_name);
            println!("block_group.name: {}", block_group.name);
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

    (parent_block_group_id, new_block_group_id)
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

    #[test]
    fn test_derive_chunks_one_insertion() {
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
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let insert_path =
            original_path.new_path_with(conn, 16, 24, &edge_into_insert, &edge_out_of_insert);
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

        derive_chunks(
            conn,
            op_conn,
            "test",
            None,
            "test",
            "chr1",
            None,
            vec![Range { start: 15, end: 25 }],
        )
        .unwrap();

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
