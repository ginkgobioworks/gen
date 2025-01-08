use crate::gfa_reader::{Gfa, Segment};
use crate::models::operations::OperationInfo;
use crate::models::{
    block_group::BlockGroup,
    block_group_edge::{BlockGroupEdge, BlockGroupEdgeData},
    edge::{Edge, EdgeData},
    file_types::FileTypes,
    node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID},
    path::Path,
    sample::Sample,
    sequence::Sequence,
    strand::Strand,
};
use crate::operation_management;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::io;

pub fn update_with_gfa(
    conn: &Connection,
    operation_conn: &Connection,
    collection_name: &str,
    parent_sample_name: Option<&str>,
    new_sample_name: &str,
    gfa_path: &str,
) -> io::Result<()> {
    let mut session = operation_management::start_operation(conn);

    let _new_sample =
        Sample::get_or_create_child(conn, collection_name, new_sample_name, parent_sample_name);
    let block_groups = Sample::get_block_groups(conn, collection_name, Some(new_sample_name));

    let existing_paths = block_groups
        .iter()
        .map(|block_group| BlockGroup::get_current_path(conn, block_group.id))
        .collect::<Vec<Path>>();

    let gfa: Gfa<String, (), ()> = Gfa::parse_gfa_file(gfa_path);

    let segments_by_id: HashMap<String, _> = gfa
        .segments
        .iter()
        .map(|segment| (segment.id.clone(), segment))
        .collect();

    // Find which incoming paths match existing paths, and store information about them and their
    // segments
    let mut existing_path_ids_by_new_path_name = HashMap::new();
    let mut path_segments_by_name = HashMap::new();
    for path in &gfa.paths {
        let path_name = path.name.clone();
        path_segments_by_name.insert(path_name.clone(), path.nodes.clone());
        let mut path_segments = vec![];
        for segment_id in path.nodes.iter() {
            let sequence = segments_by_id
                .get(segment_id)
                .unwrap()
                .sequence
                .get_string(&gfa.sequence);
            path_segments.push(sequence);
        }
        let path_sequence = path_segments
            .iter()
            .map(|segment| segment.to_string())
            .collect::<Vec<String>>()
            .join("");
        for existing_path in existing_paths.iter() {
            if existing_path.sequence(conn) == path_sequence {
                existing_path_ids_by_new_path_name.insert(path_name.clone(), existing_path.id);
            }
        }
    }

    // Same thing as with paths, but for walks
    for walk in &gfa.walk {
        let walk_name = walk.sample_id.clone();
        path_segments_by_name.insert(walk_name.clone(), walk.walk_id.clone());
        let mut walk_segments = vec![];
        for segment_id in walk.walk_id.iter() {
            let sequence = segments_by_id
                .get(segment_id)
                .unwrap()
                .sequence
                .get_string(&gfa.sequence);
            walk_segments.push(sequence);
        }
        let walk_sequence = walk_segments
            .iter()
            .map(|segment| segment.to_string())
            .collect::<Vec<String>>()
            .join("");
        for existing_path in existing_paths.iter() {
            if existing_path.sequence(conn) == walk_sequence {
                existing_path_ids_by_new_path_name.insert(walk_name.clone(), existing_path.id);
            }
        }
    }

    let matched_path_name_list = existing_path_ids_by_new_path_name
        .keys()
        .map(|path_name| path_name.as_str())
        .collect::<Vec<&str>>();
    let matched_path_names = matched_path_name_list
        .iter()
        .cloned()
        .collect::<HashSet<&str>>();

    // Record unmatched paths and walks, update existing matched ones
    let mut unmatched_path_names = vec![];
    let mut matched_path_name_by_segment_id = HashMap::new();
    for path in &gfa.paths {
        let path_name = &path.name;
        if matched_path_names.contains(path_name.as_str()) {
            for segment_id in path.nodes.iter() {
                matched_path_name_by_segment_id.insert(segment_id, path_name);
            }
        } else {
            unmatched_path_names.push(path.name.clone());
        }
    }

    for walk in &gfa.walk {
        let walk_name = &walk.sample_id;
        if matched_path_names.contains(walk_name.as_str()) {
            for segment_id in walk.walk_id.iter() {
                matched_path_name_by_segment_id.insert(segment_id, walk_name);
            }
        } else {
            unmatched_path_names.push(walk_name.clone());
        }
    }

    let mut new_paths_added = 0;

    // For any apparently new path, check if it shares segments with another path in the GFA that
    // matches a path in the database.  If so, create the new path and appropriate nodes/edges.
    for unmatched_path_name in unmatched_path_names.iter() {
        let mut matched_new_paths = HashSet::new();
        let segment_ids = path_segments_by_name.get(unmatched_path_name).unwrap();
        for segment_id in segment_ids.iter() {
            let path_name_result = matched_path_name_by_segment_id.get(segment_id);
            if let Some(path_name) = path_name_result {
                matched_new_paths.insert(path_name);
            }
        }
        if matched_new_paths.len() == 1 {
            let matched_new_path_name = *matched_new_paths.iter().next().unwrap();
            let existing_path_id = existing_path_ids_by_new_path_name
                .get(*matched_new_path_name)
                .unwrap();
            let existing_path = Path::get(conn, *existing_path_id);
            let matched_path_segments = path_segments_by_name.get(*matched_new_path_name).unwrap();
            let unmatched_path_segments = path_segments_by_name.get(unmatched_path_name).unwrap();
            create_new_path_from_existing(
                conn,
                &existing_path,
                matched_path_segments,
                unmatched_path_name,
                unmatched_path_segments,
                &gfa,
                &segments_by_id,
            );
            new_paths_added += 1;
        }
    }

    let summary_str = format!("{} new paths added", new_paths_added);

    operation_management::end_operation(
        conn,
        operation_conn,
        &mut session,
        OperationInfo {
            file_path: gfa_path.to_string(),
            file_type: FileTypes::GFA,
            description: "gfa_update".to_string(),
        },
        &summary_str,
        None,
    )
    .unwrap();

    println!("Updated with GFA file: {}", gfa_path);

    Ok(())
}

fn create_new_path_from_existing(
    conn: &Connection,
    existing_path: &Path,
    matched_path_segment_ids: &[String],
    unmatched_path_name: &str,
    unmatched_path_segment_ids: &[String],
    gfa: &Gfa<String, (), ()>,
    segments_by_id: &HashMap<String, &Segment<String, ()>>,
) {
    let interval_tree = existing_path.intervaltree(conn);
    let mut existing_path_ranges_by_segment_id = HashMap::new();
    let mut existing_path_position = 0;
    for segment_id in matched_path_segment_ids.iter() {
        let segment_sequence = segments_by_id
            .get(segment_id)
            .unwrap()
            .sequence
            .get_string(&gfa.sequence);
        let segment_length = segment_sequence.len();
        existing_path_ranges_by_segment_id.insert(
            segment_id,
            (
                existing_path_position,
                existing_path_position + segment_length,
            ),
        );
        existing_path_position += segment_length;
    }

    // Build up a new path by merging the shared nodes from the existing path with newly
    // created nodes
    let mut existing_path_position = 0;
    let mut previous_node_id = PATH_START_NODE_ID;
    let mut previous_node_coordinate = -1;
    let mut previous_node_strand = Strand::Forward;
    let mut new_path_edges = vec![];
    for segment_id in unmatched_path_segment_ids.iter() {
        if existing_path_ranges_by_segment_id.contains_key(segment_id) {
            // Current segment matches something in the existing path, add an edge from the previous
            // node to the next one, which already exists
            let (start, end) = existing_path_ranges_by_segment_id.get(segment_id).unwrap();
            let block_with_start = interval_tree
                .query_point(*start as i64)
                .next()
                .unwrap()
                .value;
            let block_with_end = interval_tree.query_point(*end as i64).next().unwrap().value;

            let target_coordinate =
                block_with_start.sequence_start + *start as i64 - block_with_start.start;
            if previous_node_id != block_with_start.node_id {
                new_path_edges.push(EdgeData {
                    source_node_id: previous_node_id,
                    source_coordinate: previous_node_coordinate,
                    source_strand: previous_node_strand,
                    target_node_id: block_with_start.node_id,
                    target_coordinate,
                    target_strand: block_with_start.strand,
                });
            }

            existing_path_position += (end - start) as i64;
            previous_node_id = block_with_end.node_id;
            previous_node_coordinate =
                block_with_end.sequence_start + existing_path_position - block_with_end.start;
            previous_node_strand = block_with_end.strand;
        } else {
            // Current segment is new, create a sequence and node for it, then add an edge to the
            // new node
            let segment = segments_by_id.get(segment_id).unwrap();
            let segment_sequence = segment.sequence.get_string(&gfa.sequence);
            let sequence = Sequence::new()
                .sequence_type("DNA")
                .sequence(segment_sequence)
                .save(conn);
            let node_id = Node::create(conn, &sequence.hash, None);
            // TODO: Set strand correctly
            let next_node_strand = Strand::Forward;
            new_path_edges.push(EdgeData {
                source_node_id: previous_node_id,
                source_coordinate: previous_node_coordinate,
                source_strand: previous_node_strand,
                target_node_id: node_id,
                target_coordinate: 0,
                target_strand: next_node_strand,
            });
            previous_node_id = node_id;
            previous_node_coordinate = segment_sequence.len() as i64;
            previous_node_strand = next_node_strand;
        }
    }

    let last_segment_id = unmatched_path_segment_ids.last().unwrap();
    if existing_path_ranges_by_segment_id.contains_key(last_segment_id) {
        let (start, _end) = existing_path_ranges_by_segment_id
            .get(last_segment_id)
            .unwrap();
        let block_with_start = interval_tree
            .query_point(*start as i64)
            .next()
            .unwrap()
            .value;
        new_path_edges.push(EdgeData {
            source_node_id: block_with_start.node_id,
            source_coordinate: block_with_start.end,
            source_strand: block_with_start.strand,
            target_node_id: PATH_END_NODE_ID,
            target_coordinate: 0,
            target_strand: Strand::Forward,
        });
    } else {
        new_path_edges.push(EdgeData {
            source_node_id: previous_node_id,
            source_coordinate: previous_node_coordinate,
            source_strand: previous_node_strand,
            target_node_id: PATH_END_NODE_ID,
            target_coordinate: 0,
            target_strand: Strand::Forward,
        });
    }

    let block_group_id = existing_path.block_group_id;
    let new_edge_ids = Edge::bulk_create(conn, &new_path_edges);
    let block_group_edges = new_edge_ids
        .iter()
        .map(|edge_id| BlockGroupEdgeData {
            block_group_id,
            edge_id: *edge_id,
            chromosome_index: 0,
            phased: 0,
        })
        .collect::<Vec<BlockGroupEdgeData>>();
    BlockGroupEdge::bulk_create(conn, &block_group_edges);
    Path::create(conn, unmatched_path_name, block_group_id, &new_edge_ids);
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::diffs::gfa::gfa_sample_diff;
    use crate::imports::fasta::import_fasta;
    use crate::models::operations::setup_db;
    use crate::models::{metadata, traits::Query};
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use crate::updates::fasta::update_with_fasta;
    use rusqlite::types::Value as SQLValue;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn test_basic_update() {
        // Does the following things to confirm update works:
        // 1. Import from fasta
        // 2. Update with fasta
        // 3. Generate a GFA diff between the original import and the update
        // 4. Update the original with the diff in a new sample
        // 5. Confirm the fasta update and the GFA update match
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

        let mut fasta_update_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_update_path.push("fixtures/aaaaaaaa.fa");

        let _ = update_with_fasta(
            conn,
            op_conn,
            &collection,
            None,
            "child",
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
                SQLValue::from(collection.clone()),
                SQLValue::from("child".to_string()),
            ),
        );
        assert_eq!(block_groups.len(), 1);
        assert_eq!(
            BlockGroup::get_all_sequences(conn, block_groups[0].id, false),
            HashSet::from_iter(expected_sequences),
        );

        let temp_dir = tempdir().unwrap();
        let gfa_diff_path = temp_dir.path().join("parent-child-diff.gfa");
        gfa_sample_diff(conn, &collection, &gfa_diff_path, None, Some("child"));

        let _ = update_with_gfa(
            conn,
            op_conn,
            &collection,
            None,
            "applied diff",
            gfa_diff_path.to_str().unwrap(),
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
                SQLValue::from("applied diff".to_string()),
            ),
        );
        assert_eq!(block_groups.len(), 1);
        assert_eq!(
            BlockGroup::get_all_sequences(conn, block_groups[0].id, false),
            HashSet::from_iter(expected_sequences),
        );
    }
}
