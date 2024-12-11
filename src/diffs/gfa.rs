use itertools::Itertools;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::gfa::{path_line, write_links, write_segments, Link, Path as GFAPath, Segment};
use crate::models::{
    block_group::{BlockGroup, NodeIntervalBlock},
    path::Path,
    sample::Sample,
};
use crate::range::Range;

pub fn gfa_sample_diff(
    conn: &Connection,
    collection_name: &str,
    filename: &PathBuf,
    from_sample_name: Option<&str>,
    to_sample_name: &str,
) {
    // General note about how we encode segment IDs.  The node ID and the start coordinate in the
    // sequence are all that's needed, because the end coordinate can be inferred from the length of
    // the segment's sequence.  So the segment ID is of the form <node ID>.<start coordinate>

    let source_block_groups = Sample::get_block_groups(conn, collection_name, from_sample_name);
    let target_block_groups = Sample::get_block_groups(conn, collection_name, Some(to_sample_name));

    let source_paths_by_name = source_block_groups
        .iter()
        .map(|bg| (bg.name.clone(), BlockGroup::get_current_path(conn, bg.id)))
        .collect::<HashMap<String, Path>>();
    let target_paths_by_name = target_block_groups
        .iter()
        .map(|bg| (bg.name.clone(), BlockGroup::get_current_path(conn, bg.id)))
        .collect::<HashMap<String, Path>>();

    let mut segments = HashSet::new();
    let mut links = HashSet::new();
    let mut paths = vec![];

    let target_path_names = target_paths_by_name
        .keys()
        .cloned()
        .collect::<HashSet<String>>();
    let source_path_names = source_paths_by_name
        .keys()
        .cloned()
        .collect::<HashSet<String>>();
    let path_names = source_path_names
        .union(&target_path_names)
        .cloned()
        .collect::<Vec<String>>();

    for path_name in &path_names {
        let source_path_result = source_paths_by_name.get(path_name);
        let target_path_result = target_paths_by_name.get(path_name);

        let has_source_path = source_path_result.is_some();
        let has_target_path = target_path_result.is_some();

        let mappings = if has_source_path && has_target_path {
            source_path_result
                .unwrap()
                .find_block_mappings(conn, target_path_result.unwrap())
        } else {
            vec![]
        };

        let mut source_ranges = vec![];
        let mut target_ranges = vec![];

        let mut source_mapping_start = 0;
        let mut target_mapping_start = 0;
        for mapping in &mappings {
            if mapping.source_range.start > source_mapping_start {
                source_ranges.push(Range {
                    start: source_mapping_start,
                    end: mapping.source_range.start,
                });
            }
            source_ranges.push(mapping.source_range.clone());
            source_mapping_start = mapping.source_range.end;
            if mapping.target_range.start > target_mapping_start {
                target_ranges.push(Range {
                    start: target_mapping_start,
                    end: mapping.target_range.start,
                });
            }
            target_ranges.push(mapping.target_range.clone());
            target_mapping_start = mapping.target_range.end;
        }

        if has_source_path {
            let source_path = source_path_result.unwrap();
            let source_sequence = source_path.sequence(conn);

            let source_len = source_sequence.len().try_into().unwrap();
            if source_mapping_start < source_len {
                source_ranges.push(Range {
                    start: source_mapping_start,
                    end: source_len,
                });
            }

            let source_node_blocks = source_path.node_block_partition(conn, source_ranges);
            let source_segments = segments_from_blocks(&source_node_blocks, &source_sequence);
            segments.extend(source_segments.iter().cloned());

            let source_links = links_from_blocks(&source_node_blocks);
            links.extend(source_links.iter().cloned());

            let source_gfa_path =
                path_from_segments(from_sample_name, source_path, &source_segments);
            paths.push(source_gfa_path);
        }

        if has_target_path {
            let target_path = target_path_result.unwrap();
            let target_sequence = target_path.sequence(conn);

            let target_len = target_sequence.len().try_into().unwrap();
            if target_mapping_start < target_len {
                target_ranges.push(Range {
                    start: target_mapping_start,
                    end: target_len,
                });
            }

            let target_node_blocks = target_path.node_block_partition(conn, target_ranges);
            let target_segments = segments_from_blocks(&target_node_blocks, &target_sequence);
            segments.extend(target_segments.iter().cloned());

            let target_links = links_from_blocks(&target_node_blocks);
            links.extend(target_links.iter().cloned());

            let target_gfa_path =
                path_from_segments(Some(to_sample_name), target_path, &target_segments);
            paths.push(target_gfa_path);
        }
    }

    let file = File::create(filename).unwrap();
    let mut writer = BufWriter::new(file);
    write_segments(&mut writer, &segments.iter().cloned().collect());
    write_links(&mut writer, &links.iter().cloned().collect());

    for path in paths {
        writer
            .write_all(&path_line(&path).into_bytes())
            .unwrap_or_else(|_| panic!("Error writing path {} to GFA stream", path.name));
    }
}

fn segments_from_blocks(node_blocks: &Vec<NodeIntervalBlock>, sequence: &str) -> Vec<Segment> {
    let mut segments = vec![];
    for block in node_blocks {
        let start = usize::try_from(block.start).unwrap();
        let end = usize::try_from(block.end).unwrap();
        let segment = Segment {
            sequence: sequence[start..end].to_string(),
            node_id: block.node_id,
            sequence_start: block.sequence_start,
            strand: block.strand,
        };
        segments.push(segment.clone());
    }
    segments
}

fn links_from_blocks(node_blocks: &[NodeIntervalBlock]) -> Vec<Link> {
    let mut links = vec![];

    for (block1, block2) in node_blocks.iter().tuple_windows() {
        let source_segment = Segment {
            sequence: "".to_string(),
            node_id: block1.node_id,
            sequence_start: block1.sequence_start,
            strand: block1.strand,
        };
        let target_segment = Segment {
            sequence: "".to_string(),
            node_id: block2.node_id,
            sequence_start: block2.sequence_start,
            strand: block2.strand,
        };

        let link = Link {
            source_segment_id: source_segment.segment_id(),
            source_strand: block1.strand,
            target_segment_id: target_segment.segment_id(),
            target_strand: block2.strand,
        };
        links.push(link);
    }

    links
}

fn path_from_segments(sample_name: Option<&str>, path: &Path, segments: &[Segment]) -> GFAPath {
    let path_name = if sample_name.is_some() && sample_name.unwrap() != "" {
        format!("{}.{}", sample_name.unwrap(), path.name)
    } else {
        path.name.clone()
    };
    GFAPath {
        name: path_name.clone(),
        segment_ids: segments.iter().map(|s| s.segment_id()).collect(),
        node_strands: segments.iter().map(|s| s.strand).collect(),
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::imports::gfa::import_gfa;
    use crate::models::{
        block_group::BlockGroup,
        block_group_edge::{BlockGroupEdge, BlockGroupEdgeData},
        collection::Collection,
        edge::Edge,
        node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID},
        sequence::Sequence,
        strand::Strand,
    };

    use crate::test_helpers::get_connection;
    use tempfile::tempdir;

    #[test]
    fn test_gfa_diff() {
        // Sets up a basic graph and then exports it to a GFA file
        let conn = get_connection(None);

        let collection_name = "test collection";
        Collection::create(&conn, collection_name);
        let block_group = BlockGroup::create(&conn, collection_name, None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(&conn);
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(&conn);
        let node1_id = Node::create(&conn, &sequence1.hash, None);
        let node2_id = Node::create(&conn, &sequence2.hash, None);

        let edge1 = Edge::create(
            &conn,
            PATH_START_NODE_ID,
            0,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            &conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge3 = Edge::create(
            &conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            0,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id, edge3.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|&edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id,
                chromosome_index: 0,
                phased: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path1 = Path::create(&conn, "parent", block_group.id, &edge_ids);

        // Set up child
        let _child_sample = Sample::get_or_create_child(&conn, collection_name, "child", None);
        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCC")
            .save(&conn);
        let node3_id = Node::create(&conn, &sequence3.hash, None);
        let edge4 = Edge::create(
            &conn,
            node1_id,
            2,
            Strand::Forward,
            node3_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            &conn,
            node3_id,
            4,
            Strand::Forward,
            node1_id,
            6,
            Strand::Forward,
        );

        let child_block_groups = Sample::get_block_groups(&conn, collection_name, Some("child"));
        let child_block_group = child_block_groups.first().unwrap();
        let child_edge_ids = [edge4.id, edge5.id];
        let child_block_group_edges = child_edge_ids
            .iter()
            .map(|&edge_id| BlockGroupEdgeData {
                block_group_id: child_block_group.id,
                edge_id,
                chromosome_index: 0,
                phased: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(&conn, &child_block_group_edges);
        let original_child_path = BlockGroup::get_current_path(&conn, child_block_group.id);
        let _child_path = original_child_path.new_path_with(&conn, 2, 6, &edge4, &edge5);

        let temp_dir = tempdir().unwrap();
        let gfa_path = temp_dir.path().join("parent-child-diff.gfa");
        gfa_sample_diff(&conn, collection_name, &gfa_path, None, "child");

        import_gfa(&gfa_path, "test collection 2", None, &conn);

        let new_child_block_group = Collection::get_block_groups(&conn, "test collection 2")
            .pop()
            .unwrap();
        let all_child_sequences =
            BlockGroup::get_all_sequences(&conn, new_child_block_group.id, false);

        // We've replaced the middle AAAA with CCCC, so expect that as the child sequence
        assert_eq!(
            all_child_sequences,
            ["AAAAAAAATTTTTTTT", "AACCCCAATTTTTTTT"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<String>>()
        );

        // Set up grandchild
        let _grandchild_sample =
            Sample::get_or_create_child(&conn, collection_name, "grandchild", Some("child"));
        let sequence4 = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGG")
            .save(&conn);
        let node4_id = Node::create(&conn, &sequence4.hash, None);
        let edge6 = Edge::create(
            &conn,
            node2_id,
            2,
            Strand::Forward,
            node4_id,
            0,
            Strand::Forward,
        );
        let edge7 = Edge::create(
            &conn,
            node4_id,
            4,
            Strand::Forward,
            node2_id,
            6,
            Strand::Forward,
        );

        let grandchild_block_groups =
            Sample::get_block_groups(&conn, collection_name, Some("grandchild"));
        let grandchild_block_group = grandchild_block_groups.first().unwrap();
        let grandchild_edge_ids = [edge6.id, edge7.id];
        let grandchild_block_group_edges = grandchild_edge_ids
            .iter()
            .map(|&edge_id| BlockGroupEdgeData {
                block_group_id: grandchild_block_group.id,
                edge_id,
                chromosome_index: 0,
                phased: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(&conn, &grandchild_block_group_edges);
        let original_grandchild_path =
            BlockGroup::get_current_path(&conn, grandchild_block_group.id);
        let _grandchild_path =
            original_grandchild_path.new_path_with(&conn, 10, 14, &edge6, &edge7);

        let gfa_path = temp_dir.path().join("parent-grandchild-diff.gfa");
        gfa_sample_diff(&conn, collection_name, &gfa_path, None, "grandchild");

        import_gfa(&gfa_path, "test collection 3", None, &conn);

        let new_grandchild_block_group = Collection::get_block_groups(&conn, "test collection 3")
            .pop()
            .unwrap();
        let all_grandchild_sequences =
            BlockGroup::get_all_sequences(&conn, new_grandchild_block_group.id, false);

        // We've replaced the middle AAAA with CCCC and the middle TTTT with GGGG, so four possible sequences
        assert_eq!(
            all_grandchild_sequences,
            [
                "AAAAAAAATTTTTTTT",
                "AACCCCAATTTTTTTT",
                "AACCCCAATTGGGGTT",
                "AAAAAAAATTGGGGTT"
            ]
            .iter()
            .map(|s| s.to_string())
            .collect::<HashSet<String>>()
        );

        let gfa_path = temp_dir.path().join("child-grandchild-diff.gfa");
        gfa_sample_diff(
            &conn,
            collection_name,
            &gfa_path,
            Some("child"),
            "grandchild",
        );

        import_gfa(&gfa_path, "test collection 4", None, &conn);

        let new_grandchild_block_group = Collection::get_block_groups(&conn, "test collection 4")
            .pop()
            .unwrap();
        let all_grandchild_sequences =
            BlockGroup::get_all_sequences(&conn, new_grandchild_block_group.id, false);

        assert_eq!(
            all_grandchild_sequences,
            ["AACCCCAATTTTTTTT", "AACCCCAATTGGGGTT"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<String>>()
        );
    }

    #[test]
    fn test_gfa_self_diff() {
        // Confirm diff of a sample to itself is just a single segment
        let conn = get_connection(None);

        let collection_name = "test collection";
        Collection::create(&conn, collection_name);
        let block_group = BlockGroup::create(&conn, collection_name, None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(&conn);
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(&conn);
        let node1_id = Node::create(&conn, &sequence1.hash, None);
        let node2_id = Node::create(&conn, &sequence2.hash, None);

        let edge1 = Edge::create(
            &conn,
            PATH_START_NODE_ID,
            0,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            &conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge3 = Edge::create(
            &conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            0,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id, edge3.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|&edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id,
                chromosome_index: 0,
                phased: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path1 = Path::create(&conn, "parent", block_group.id, &edge_ids);

        let temp_dir = tempdir().unwrap();
        let gfa_path = temp_dir.path().join("self-diff.gfa");
        gfa_sample_diff(&conn, collection_name, &gfa_path, None, "child");

        import_gfa(&gfa_path, "test collection 2", None, &conn);

        let new_block_group = Collection::get_block_groups(&conn, "test collection 2")
            .pop()
            .unwrap();
        let all_sequences = BlockGroup::get_all_sequences(&conn, new_block_group.id, false);

        assert_eq!(
            all_sequences,
            ["AAAAAAAATTTTTTTT"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<String>>()
        );
    }

    #[test]
    fn test_gfa_diff_unrelated_paths() {
        // Confirm diff of a sample to itself is just a single segment
        let conn = get_connection(None);

        let collection_name = "test collection";
        Collection::create(&conn, collection_name);
        let _sample1 = Sample::get_or_create(&conn, "sample1");
        let block_group =
            BlockGroup::create(&conn, collection_name, Some("sample1"), "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(&conn);
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(&conn);
        let node1_id = Node::create(&conn, &sequence1.hash, None);
        let node2_id = Node::create(&conn, &sequence2.hash, None);

        let edge1 = Edge::create(
            &conn,
            PATH_START_NODE_ID,
            0,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            &conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge3 = Edge::create(
            &conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            0,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id, edge3.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|&edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id,
                chromosome_index: 0,
                phased: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path1 = Path::create(&conn, "parent", block_group.id, &edge_ids);

        let _sample2 = Sample::get_or_create(&conn, "sample2");
        let block_group2 = BlockGroup::create(
            &conn,
            collection_name,
            Some("sample2"),
            "test block group 2",
        );
        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGG")
            .save(&conn);
        let sequence4 = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCC")
            .save(&conn);
        let node3_id = Node::create(&conn, &sequence3.hash, None);
        let node4_id = Node::create(&conn, &sequence4.hash, None);

        let edge4 = Edge::create(
            &conn,
            PATH_START_NODE_ID,
            0,
            Strand::Forward,
            node3_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            &conn,
            node3_id,
            8,
            Strand::Forward,
            node4_id,
            0,
            Strand::Forward,
        );
        let edge6 = Edge::create(
            &conn,
            node4_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            0,
            Strand::Forward,
        );

        let edge_ids = [edge4.id, edge5.id, edge6.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|&edge_id| BlockGroupEdgeData {
                block_group_id: block_group2.id,
                edge_id,
                chromosome_index: 0,
                phased: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path2 = Path::create(&conn, "parent", block_group2.id, &edge_ids);

        let temp_dir = tempdir().unwrap();
        let gfa_path = temp_dir.path().join("unrelated-diff.gfa");
        gfa_sample_diff(
            &conn,
            collection_name,
            &gfa_path,
            Some("sample1"),
            "sample2",
        );

        import_gfa(&gfa_path, "test collection 3", None, &conn);

        let new_block_group = Collection::get_block_groups(&conn, "test collection 3")
            .pop()
            .unwrap();
        let all_sequences = BlockGroup::get_all_sequences(&conn, new_block_group.id, false);

        assert_eq!(
            all_sequences,
            ["AAAAAAAATTTTTTTT", "GGGGGGGGCCCCCCCC"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<String>>()
        );
    }

    #[test]
    fn test_gfa_diff_unrelated_paths_matching_block_group_names() {
        // Confirm diff of a sample to itself is just a single segment
        let conn = get_connection(None);

        let collection_name = "test collection";
        Collection::create(&conn, collection_name);
        let _sample1 = Sample::get_or_create(&conn, "sample1");
        let block_group =
            BlockGroup::create(&conn, collection_name, Some("sample1"), "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(&conn);
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(&conn);
        let node1_id = Node::create(&conn, &sequence1.hash, None);
        let node2_id = Node::create(&conn, &sequence2.hash, None);

        let edge1 = Edge::create(
            &conn,
            PATH_START_NODE_ID,
            0,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            &conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge3 = Edge::create(
            &conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            0,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id, edge3.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|&edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id,
                chromosome_index: 0,
                phased: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path1 = Path::create(&conn, "parent", block_group.id, &edge_ids);

        let _sample2 = Sample::get_or_create(&conn, "sample2");
        let block_group2 =
            BlockGroup::create(&conn, collection_name, Some("sample2"), "test block group");
        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGG")
            .save(&conn);
        let sequence4 = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCC")
            .save(&conn);
        let node3_id = Node::create(&conn, &sequence3.hash, None);
        let node4_id = Node::create(&conn, &sequence4.hash, None);

        let edge4 = Edge::create(
            &conn,
            PATH_START_NODE_ID,
            0,
            Strand::Forward,
            node3_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            &conn,
            node3_id,
            8,
            Strand::Forward,
            node4_id,
            0,
            Strand::Forward,
        );
        let edge6 = Edge::create(
            &conn,
            node4_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            0,
            Strand::Forward,
        );

        let edge_ids = [edge4.id, edge5.id, edge6.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|&edge_id| BlockGroupEdgeData {
                block_group_id: block_group2.id,
                edge_id,
                chromosome_index: 0,
                phased: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path2 = Path::create(&conn, "parent", block_group2.id, &edge_ids);

        let temp_dir = tempdir().unwrap();
        let gfa_path = temp_dir.path().join("unrelated-diff.gfa");
        gfa_sample_diff(
            &conn,
            collection_name,
            &gfa_path,
            Some("sample1"),
            "sample2",
        );

        import_gfa(&gfa_path, "test collection 3", None, &conn);

        let new_block_group = Collection::get_block_groups(&conn, "test collection 3")
            .pop()
            .unwrap();
        let all_sequences = BlockGroup::get_all_sequences(&conn, new_block_group.id, false);

        assert_eq!(
            all_sequences,
            ["AAAAAAAATTTTTTTT", "GGGGGGGGCCCCCCCC"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<String>>()
        );
    }

    #[test]
    fn test_gfa_diff_overlapping_inserts() {
        // Sets up a basic graph and then exports it to a GFA file
        let conn = get_connection(None);

        let collection_name = "test collection";
        Collection::create(&conn, collection_name);
        let block_group = BlockGroup::create(&conn, collection_name, None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAAAAAAAAAA")
            .save(&conn);
        let node1_id = Node::create(&conn, &sequence1.hash, None);

        let edge1 = Edge::create(
            &conn,
            PATH_START_NODE_ID,
            0,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            &conn,
            node1_id,
            16,
            Strand::Forward,
            PATH_END_NODE_ID,
            0,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|&edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id,
                chromosome_index: 0,
                phased: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path1 = Path::create(&conn, "parent", block_group.id, &[edge1.id, edge2.id]);

        // Set up child
        let _child_sample = Sample::get_or_create_child(&conn, collection_name, "child", None);
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCC")
            .save(&conn);
        let node2_id = Node::create(&conn, &sequence2.hash, None);
        let edge3 = Edge::create(
            &conn,
            node1_id,
            2,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge4 = Edge::create(
            &conn,
            node2_id,
            4,
            Strand::Forward,
            node1_id,
            6,
            Strand::Forward,
        );

        let child_block_groups = Sample::get_block_groups(&conn, collection_name, Some("child"));
        let child_block_group = child_block_groups.first().unwrap();
        let child_edge_ids = [edge3.id, edge4.id];
        let child_block_group_edges = child_edge_ids
            .iter()
            .map(|&edge_id| BlockGroupEdgeData {
                block_group_id: child_block_group.id,
                edge_id,
                chromosome_index: 0,
                phased: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(&conn, &child_block_group_edges);
        let original_child_path = BlockGroup::get_current_path(&conn, child_block_group.id);
        let _child_path = original_child_path.new_path_with(&conn, 2, 6, &edge3, &edge4);

        let temp_dir = tempdir().unwrap();
        let gfa_path = temp_dir.path().join("parent-child-diff.gfa");
        gfa_sample_diff(&conn, collection_name, &gfa_path, None, "child");

        import_gfa(&gfa_path, "test collection 2", None, &conn);

        let new_child_block_group = Collection::get_block_groups(&conn, "test collection 2")
            .pop()
            .unwrap();
        let all_child_sequences =
            BlockGroup::get_all_sequences(&conn, new_child_block_group.id, false);

        // We've replaced [2, 6) of AAAA with CCCC
        assert_eq!(
            all_child_sequences,
            ["AAAAAAAAAAAAAAAA", "AACCCCAAAAAAAAAA"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<String>>()
        );

        // Set up grandchild
        let _grandchild_sample =
            Sample::get_or_create_child(&conn, collection_name, "grandchild", Some("child"));
        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGG")
            .save(&conn);
        let node3_id = Node::create(&conn, &sequence3.hash, None);
        let edge5 = Edge::create(
            &conn,
            node2_id,
            2,
            Strand::Forward,
            node3_id,
            0,
            Strand::Forward,
        );
        let edge6 = Edge::create(
            &conn,
            node3_id,
            4,
            Strand::Forward,
            node1_id,
            10,
            Strand::Forward,
        );

        let grandchild_block_groups =
            Sample::get_block_groups(&conn, collection_name, Some("grandchild"));
        let grandchild_block_group = grandchild_block_groups.first().unwrap();
        let grandchild_edge_ids = [edge5.id, edge6.id];
        let grandchild_block_group_edges = grandchild_edge_ids
            .iter()
            .map(|&edge_id| BlockGroupEdgeData {
                block_group_id: grandchild_block_group.id,
                edge_id,
                chromosome_index: 0,
                phased: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(&conn, &grandchild_block_group_edges);
        let original_grandchild_path =
            BlockGroup::get_current_path(&conn, grandchild_block_group.id);
        let _grandchild_path = original_grandchild_path.new_path_with(&conn, 4, 10, &edge5, &edge6);

        let gfa_path = temp_dir.path().join("parent-grandchild-diff.gfa");
        gfa_sample_diff(&conn, collection_name, &gfa_path, None, "grandchild");

        import_gfa(&gfa_path, "test collection 3", None, &conn);

        let new_grandchild_block_group = Collection::get_block_groups(&conn, "test collection 3")
            .pop()
            .unwrap();
        let all_grandchild_sequences =
            BlockGroup::get_all_sequences(&conn, new_grandchild_block_group.id, false);

        // Original is AAAAAAAAAAAAAAAA
        // Grandchild is AACCGGGGAAAAAA
        // Because the grandchild change overlaps with the child change, there are no other possibiiities
        assert_eq!(
            all_grandchild_sequences,
            ["AAAAAAAAAAAAAAAA", "AACCGGGGAAAAAA"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<String>>()
        );

        let gfa_path = temp_dir.path().join("child-grandchild-diff.gfa");
        gfa_sample_diff(
            &conn,
            collection_name,
            &gfa_path,
            Some("child"),
            "grandchild",
        );

        import_gfa(&gfa_path, "test collection 4", None, &conn);

        let new_grandchild_block_group = Collection::get_block_groups(&conn, "test collection 4")
            .pop()
            .unwrap();
        let all_grandchild_sequences =
            BlockGroup::get_all_sequences(&conn, new_grandchild_block_group.id, false);

        // Child is      AACCCCAAAAAAAAAA
        // Grandchild is AACCGGGGAAAAAA
        assert_eq!(
            all_grandchild_sequences,
            ["AACCCCAAAAAAAAAA", "AACCGGGGAAAAAA"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<String>>()
        );
    }
}
