use crate::models::operations::OperationFile;
use crate::models::{
    block_group::BlockGroup,
    block_group_edge::{BlockGroupEdge, BlockGroupEdgeData},
    edge::{Edge, EdgeData},
    file_types::FileTypes,
    node::{PATH_END_NODE_ID, PATH_START_NODE_ID},
    operations::{Operation, OperationInfo},
    path::Path,
    path_edge::PathEdge,
    sample::Sample,
    strand::Strand,
};
use crate::operation_management::{end_operation, start_operation, OperationError};
use core::ops::Range;
use itertools::Itertools;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum DeriveGraphError {
    #[error("Operation Error: {0}")]
    OperationError(#[from] OperationError),
    #[error("Invalid coordinate(s): {0}")]
    InvalidCoordinate(String),
    #[error("Region not found: {0}")]
    RegionNotFound(String),
    #[error("Path not found: {0}")]
    PathNotFound(String),
}

pub fn get_path(
    conn: &Connection,
    collection_name: &str,
    sample_name: Option<&str>,
    region_name: &str,
    backbone: Option<&str>,
) -> Result<Path, DeriveGraphError> {
    let block_group_id = get_block_group_id(conn, collection_name, sample_name, region_name)?;

    if let Some(backbone) = backbone {
        let path = BlockGroup::get_path_by_name(conn, block_group_id, backbone);
        if path.is_none() {
            return Err(DeriveGraphError::PathNotFound(format!(
                "No path found with name {}",
                backbone
            )));
        }
        Ok(path.unwrap())
    } else {
        Ok(BlockGroup::get_current_path(conn, block_group_id))
    }
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
) -> Result<Operation, DeriveGraphError> {
    let mut session = start_operation(conn);
    let _new_sample = Sample::get_or_create(conn, new_sample_name);

    let parent_block_group_id =
        get_block_group_id(conn, collection_name, parent_sample_name, region_name)?;
    let current_path = get_path(
        conn,
        collection_name,
        parent_sample_name,
        region_name,
        backbone,
    )?;

    let current_path_length = current_path.length(conn);
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
            return Err(DeriveGraphError::InvalidCoordinate(format!(
                "Start and/or end coordinates ({}, {}) are out of range for the current path.",
                start_coordinate, end_coordinate
            )));
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
    let op = end_operation(
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
    .map_err(DeriveGraphError::OperationError);

    println!("Derived chunks successfully.");

    op
}

fn get_block_group_id(
    conn: &Connection,
    collection_name: &str,
    parent_sample_name: Option<&str>,
    region_name: &str,
) -> Result<i64, DeriveGraphError> {
    let block_groups = Sample::get_block_groups(conn, collection_name, parent_sample_name);

    for block_group in &block_groups {
        if block_group.name == region_name {
            return Ok(block_group.id);
        }
    }

    Err(DeriveGraphError::RegionNotFound(format!(
        "No region found with name: {}",
        region_name
    )))
}

pub fn make_stitch(
    conn: &Connection,
    operation_conn: &Connection,
    collection_name: &str,
    parent_sample_name: Option<&str>,
    new_sample_name: &str,
    region_names: Vec<&str>,
    new_name: &str,
) -> Result<Operation, DeriveGraphError> {
    let mut session = start_operation(conn);

    let _new_sample = Sample::get_or_create(conn, new_sample_name);
    let block_groups = Sample::get_block_groups(conn, collection_name, parent_sample_name);

    let mut block_groups_by_name = HashMap::new();
    for block_group in &block_groups {
        let block_group_name = block_group.name.as_str();
        if region_names.contains(&block_group_name) {
            block_groups_by_name.insert(block_group_name, block_group);
        }
    }

    let mut source_node_coordinates = vec![(PATH_START_NODE_ID, 0, Strand::Forward)];
    let mut edges_to_reuse = vec![];
    let mut edges_to_create = vec![];
    let mut concatenated_path_edges = vec![];

    for region_name in &region_names {
        if let Some(block_group) = block_groups_by_name.get(region_name) {
            let edges = BlockGroupEdge::edges_for_block_group(conn, block_group.id);

            let nonterminal_edges = edges
                .iter()
                .filter(|edge| !edge.edge.is_start_edge() && !edge.edge.is_end_edge())
                .cloned();
            edges_to_reuse.extend(nonterminal_edges);

            let start_edges = edges
                .iter()
                .filter(|edge| edge.edge.is_start_edge())
                .collect::<Vec<_>>();
            for source_node_coordinate in &source_node_coordinates {
                for start_edge in &start_edges {
                    edges_to_create.push(EdgeData {
                        source_node_id: source_node_coordinate.0,
                        source_coordinate: source_node_coordinate.1,
                        source_strand: source_node_coordinate.2,
                        target_node_id: start_edge.edge.target_node_id,
                        target_coordinate: start_edge.edge.target_coordinate,
                        target_strand: start_edge.edge.target_strand,
                    });
                }
            }

            let end_edges = edges.iter().filter(|edge| edge.edge.is_end_edge());
            source_node_coordinates = end_edges
                .map(|edge| {
                    (
                        edge.edge.source_node_id,
                        edge.edge.source_coordinate,
                        edge.edge.source_strand,
                    )
                })
                .collect();

            let current_path = BlockGroup::get_current_path(conn, block_group.id);
            concatenated_path_edges.extend(PathEdge::edges_for_path(conn, current_path.id));
        } else {
	    return Err(DeriveGraphError::RegionNotFound(format!(
		"No region found with name: {}",
		region_name
	    )));
        }
    }

    let created_edge_ids = Edge::bulk_create(conn, &edges_to_create);
    let created_edges = Edge::bulk_load(conn, &created_edge_ids);

    let created_edges_by_node_info = created_edges
        .iter()
        .map(|edge| {
            (
                (
                    edge.source_node_id,
                    edge.source_coordinate,
                    edge.source_strand,
                    edge.target_node_id,
                    edge.target_coordinate,
                    edge.target_strand,
                ),
                edge.clone(),
            )
        })
        .collect::<HashMap<_, _>>();

    let mut stitch_path_edge_ids = vec![];
    for (path_edge1, path_edge2) in concatenated_path_edges.iter().tuple_windows() {
        if path_edge1.target_node_id == PATH_END_NODE_ID
            && path_edge2.source_node_id == PATH_START_NODE_ID
        {
            stitch_path_edge_ids.push(
                created_edges_by_node_info[&(
                    path_edge1.source_node_id,
                    path_edge1.source_coordinate,
                    path_edge1.source_strand,
                    path_edge2.target_node_id,
                    path_edge2.target_coordinate,
                    path_edge2.target_strand,
                )]
                    .id,
            );
        }
    }

    let child_block_group =
        BlockGroup::create(conn, collection_name, Some(new_sample_name), new_name);
    let child_block_group_id = child_block_group.id;

    let mut new_path_edge_ids = vec![concatenated_path_edges[0].id];
    let mut stitch_count = 0;
    for path_edge in &concatenated_path_edges {
        if path_edge.is_end_edge() {
            new_path_edge_ids.push(stitch_path_edge_ids[stitch_count]);
            stitch_count += 1;
        } else if !path_edge.is_start_edge() {
            new_path_edge_ids.push(path_edge.id);
        }
    }

    new_path_edge_ids.push(concatenated_path_edges[concatenated_path_edges.len() - 1].id);

    Path::create(conn, new_name, child_block_group_id, &new_path_edge_ids);

    let bg_edges_for_existing_edges = edges_to_reuse
        .iter()
        .map(|edge| BlockGroupEdgeData {
            block_group_id: child_block_group_id,
            edge_id: edge.edge.id,
            chromosome_index: edge.chromosome_index,
            phased: edge.phased,
        })
        .collect::<Vec<_>>();

    let mut chromosome_index_counter = edges_to_reuse
        .iter()
        .min_by(|x, y| x.chromosome_index.cmp(&y.chromosome_index))
        .unwrap()
        .chromosome_index
        + 1;

    let path_edge_id_set = new_path_edge_ids.iter().collect::<HashSet<_>>();
    let mut bg_edges = vec![];
    for created_edge in created_edges {
        if path_edge_id_set.contains(&created_edge.id) {
            bg_edges.push(BlockGroupEdgeData {
                block_group_id: child_block_group_id,
                edge_id: created_edge.id,
                chromosome_index: 0,
                phased: 0,
            });
        } else {
            bg_edges.push(BlockGroupEdgeData {
                block_group_id: child_block_group_id,
                edge_id: created_edge.id,
                chromosome_index: chromosome_index_counter,
                phased: 0,
            });
            chromosome_index_counter += 1;
        }
    }

    bg_edges.extend(bg_edges_for_existing_edges);
    BlockGroupEdge::bulk_create(conn, &bg_edges);

    let summary_str = format!(
        " {}: stitched {} chunks into new graph",
        new_sample_name,
        region_names.len()
    );
    let op = end_operation(
        conn,
        operation_conn,
        &mut session,
        &OperationInfo {
            files: vec![OperationFile {
                file_path: "".to_string(),
                file_type: FileTypes::None,
            }],
            description: "make stitch".to_string(),
        },
        &summary_str,
        None,
    )
    .unwrap();

    println!("Stitched chunks successfully.");

    Ok(op)
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
        let insert_node_id = Node::create(
            conn,
            insert_sequence.hash.as_str(),
            format!("test-insert-a.{}", insert_sequence.hash),
        );
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
