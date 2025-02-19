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

// Given a specific chunk size, returns a list of ranges of that chunk size that cover the entire
// path
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
    let parent_block_group_id =
        get_parent_block_group_id(conn, collection_name, parent_sample_name, region_name);

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

// Given specific points on a path, returns a list of ranges that cover the entire path, broken up
// by the input points
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
    let parent_block_group_id =
        get_parent_block_group_id(conn, collection_name, parent_sample_name, region_name);

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
    let parent_block_group_id =
        get_parent_block_group_id(conn, collection_name, parent_sample_name, region_name);

    let current_path = get_path(conn, parent_block_group_id, backbone);

    let current_path_length = current_path.sequence(conn).len() as i64;
    let current_intervaltree = current_path.intervaltree(conn);
    let current_edges = PathEdge::edges_for_path(conn, current_path.id);

    for (i, chunk_range) in chunk_ranges.clone().into_iter().enumerate() {
        let new_block_group_name = format!("{}.{}", region_name, i + 1);
        let new_block_group = BlockGroup::create(
            conn,
            collection_name,
            Some(new_sample_name),
            new_block_group_name.as_str(),
        );
        let new_block_group_id = new_block_group.id;

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
        let start_edge = &current_edges[0];
        if !new_edge_id_set.contains(&start_edge.id) {
            let new_start_edge = child_block_group_edges
                .iter()
                .find(|e| {
                    e.edge.source_node_id == PATH_START_NODE_ID
                        && e.edge.target_node_id == start_block.node_id
                        && e.edge.target_coordinate == start_node_coordinate
                })
                .unwrap();
            new_path_edge_ids.push(new_start_edge.edge.id);
        }
        for current_edge in &current_edges {
            if new_edge_id_set.contains(&current_edge.id) {
                new_path_edge_ids.push(current_edge.id);
            }
        }
        let end_edge = &current_edges[current_edges.len() - 1];
        if !new_edge_id_set.contains(&end_edge.id) {
            let new_end_edge = child_block_group_edges
                .iter()
                .find(|e| {
                    e.edge.target_node_id == PATH_END_NODE_ID
                        && e.edge.source_node_id == end_block.node_id
                        && e.edge.source_coordinate == end_node_coordinate
                })
                .unwrap();
            new_path_edge_ids.push(new_end_edge.edge.id);
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

fn get_parent_block_group_id(
    conn: &Connection,
    collection_name: &str,
    parent_sample_name: Option<&str>,
    region_name: &str,
) -> i64 {
    let block_groups = Sample::get_block_groups(conn, collection_name, parent_sample_name);

    for block_group in &block_groups {
        if block_group.name == region_name {
            return block_group.id;
        }
    }

    panic!("No region found with name: {}", region_name);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::models::{
        block_group_edge::BlockGroupEdgeData, collection::Collection, edge::Edge, metadata,
        node::Node, operations::setup_db, sequence::Sequence, strand::Strand,
    };
    use crate::test_helpers::{
        get_connection, get_operation_connection, setup_block_group, setup_gen_dir,
    };
    use crate::updates::fasta::update_with_fasta;
    use std::path::PathBuf;

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
        let block_group2 = block_groups.iter().find(|x| x.name == "chr1.1").unwrap();

        let all_sequences2 = BlockGroup::get_all_sequences(conn, block_group2.id, false);
        assert_eq!(
            all_sequences2,
            HashSet::from_iter(vec!["TTTTTCCCCC".to_string(), "TAAAAAAAAC".to_string(),])
        );

        let new_path = BlockGroup::get_current_path(conn, block_group2.id);
        assert_eq!(new_path.sequence(conn), "TAAAAAAAAC");
    }

    #[test]
    fn derive_chunks_two_inserts() {
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let mut fasta_update_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_update_path.push("fixtures/aa.fa");

        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        let collection = "test";

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            collection,
            None,
            false,
            conn,
            op_conn,
        )
        .unwrap();

        let _ = update_with_fasta(
            conn,
            op_conn,
            collection,
            None,
            "test1",
            "m123",
            3,
            5,
            fasta_update_path.to_str().unwrap(),
        );

        let _ = update_with_fasta(
            conn,
            op_conn,
            collection,
            Some("test1"),
            "test2",
            "m123",
            15,
            20,
            fasta_update_path.to_str().unwrap(),
        );

        let original_block_groups = Sample::get_block_groups(conn, collection, None);
        let original_block_group_id = original_block_groups[0].id;
        let all_original_sequences =
            BlockGroup::get_all_sequences(conn, original_block_group_id, false);
        assert_eq!(
            all_original_sequences,
            HashSet::from_iter(vec!["ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),])
        );

        let grandchild_block_groups = Sample::get_block_groups(conn, collection, Some("test2"));
        let grandchild_block_group_id = grandchild_block_groups[0].id;
        let all_grandchild_sequences =
            BlockGroup::get_all_sequences(conn, grandchild_block_group_id, false);
        assert_eq!(
            all_grandchild_sequences,
            HashSet::from_iter(vec![
                "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
                "ATCAATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
                "ATCGATCGATCGATCAAGGAACACACAGAGA".to_string(),
                "ATCAATCGATCGATCAAGGAACACACAGAGA".to_string(),
            ])
        );

        derive_chunks(
            conn,
            op_conn,
            collection,
            Some("test2"),
            "test3",
            "m123",
            None,
            vec![
                Range { start: 0, end: 1 },
                Range { start: 1, end: 8 },
                Range { start: 8, end: 25 },
                Range { start: 25, end: 31 },
            ],
        )
        .unwrap();

        let block_groups = Sample::get_block_groups(conn, collection, Some("test3"));
        let block_group2 = block_groups.iter().find(|x| x.name == "m123.2").unwrap();

        let all_sequences2 = BlockGroup::get_all_sequences(conn, block_group2.id, false);
        assert_eq!(
            all_sequences2,
            HashSet::from_iter(vec!["TCAATCG".to_string(), "TCGATCG".to_string(),])
        );

        let path2 = BlockGroup::get_current_path(conn, block_group2.id);
        assert_eq!(path2.sequence(conn), "TCAATCG");

        let block_group3 = block_groups.iter().find(|x| x.name == "m123.3").unwrap();
        let all_sequences3 = BlockGroup::get_all_sequences(conn, block_group3.id, false);
        assert_eq!(
            all_sequences3,
            HashSet::from_iter(vec![
                "ATCGATCAAGGAACACA".to_string(),
                "ATCGATCGATCGGGAACACA".to_string(),
            ])
        );

        let path3 = BlockGroup::get_current_path(conn, block_group3.id);
        assert_eq!(path3.sequence(conn), "ATCGATCAAGGAACACA");
    }
}
