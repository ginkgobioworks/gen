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
    to_sample_name: Option<&str>,
) {
    /*
    Generate a GFA file that represents the differences between two samples in a collection.

    General approach: For each pair of shared block groups between the samples, get the current path
    for each and call find_block_mappings on the pair of paths to get mappings between shared
    regions on the paths.  Each shared region may cover multiple nodes.  We assume the mappings will
    be in order from upstream to downstream on the sequences.  We iterate over them to produce a
    list of ranges on each path's sequence.  For each mapping, and for each path, if there is an
    unshared region before the common region, we append the range for that unshared region to the
    path's list of ranges, and then the range for the common region.  Obviously there may be an
    unshared region on each path at the very end, and if so, we append the range for that region to
    the appropriate path's list.

    We then convert the list of ranges for a path to a list of segments, with each range being
    converted to a segment with the range's start coordinate, and the subsequence of the path for
    that range.  We also create links, with one link per pair of adjacent segments in the path.
    Each shared segment will have two links going in (one for each path) and two links going out.
    Each unshared segment will have one link in and one link out.

    We also create a GFA path for each path, which is just a list of the segments generated for that
    path.
    */
    let source_block_groups = Sample::get_block_groups(conn, collection_name, from_sample_name);
    let target_block_groups = Sample::get_block_groups(conn, collection_name, to_sample_name);

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

        let mut last_source_position = 0;
        let mut last_target_position = 0;
        for mapping in &mappings {
            // Iterate over the shared regions between the source and target path.  If there is an
            // unshared region before the shared region, append the range for the unshared region.
            // Then append the range for the shared region.
            if mapping.source_range.start > last_source_position {
                source_ranges.push(Range {
                    start: last_source_position,
                    end: mapping.source_range.start,
                });
            }
            source_ranges.push(mapping.source_range.clone());
            last_source_position = mapping.source_range.end;
            if mapping.target_range.start > last_target_position {
                target_ranges.push(Range {
                    start: last_target_position,
                    end: mapping.target_range.start,
                });
            }
            target_ranges.push(mapping.target_range.clone());
            last_target_position = mapping.target_range.end;
        }

        if has_source_path {
            let source_path = source_path_result.unwrap();
            let source_sequence = source_path.sequence(conn);

            let source_len = source_sequence.len() as i64;
            if last_source_position < source_len {
                source_ranges.push(Range {
                    start: last_source_position,
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

            let target_len = target_sequence.len() as i64;
            if last_target_position < target_len {
                target_ranges.push(Range {
                    start: last_target_position,
                    end: target_len,
                });
            }

            let target_node_blocks = target_path.node_block_partition(conn, target_ranges);
            let target_segments = segments_from_blocks(&target_node_blocks, &target_sequence);
            segments.extend(target_segments.iter().cloned());

            let target_links = links_from_blocks(&target_node_blocks);
            links.extend(target_links.iter().cloned());

            let target_gfa_path = path_from_segments(to_sample_name, target_path, &target_segments);
            paths.push(target_gfa_path);
        }
    }

    let file = File::create(filename).unwrap();
    let mut writer = BufWriter::new(file);
    write_segments(&mut writer, &segments.iter().cloned().collect());
    write_links(&mut writer, &links.iter().collect::<Vec<&Link>>());

    for path in paths {
        writer
            .write_all(&path_line(&path).into_bytes())
            .unwrap_or_else(|_| panic!("Error writing path {} to GFA stream", path.name));
    }
}

fn segments_from_blocks(node_blocks: &Vec<NodeIntervalBlock>, sequence: &str) -> Vec<Segment> {
    let mut segments = vec![];
    for block in node_blocks {
        let start = block.start as usize;
        let end = block.end as usize;
        let segment = Segment {
            sequence: sequence[start..end].to_string(),
            node_id: block.node_id,
            sequence_start: block.sequence_start,
            sequence_end: block.sequence_end,
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
            sequence_end: block1.sequence_end,
            strand: block1.strand,
        };
        let target_segment = Segment {
            sequence: "".to_string(),
            node_id: block2.node_id,
            sequence_start: block2.sequence_start,
            sequence_end: block2.sequence_end,
            strand: block2.strand,
        };

        // General note about how we encode segment IDs.  The node ID and the start coordinate in
        // the sequence are all that's needed, because the end coordinate can be inferred from the
        // length of the segment's sequence.  So the segment ID is of the form <node ID>.<start
        // coordinate>
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
    let path_name = if !sample_name.unwrap_or("").is_empty() {
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

        println!("here1");
        println!("block group id: {}", block_group.id);
        println!("edge1 id: {}", edge1.id);
        println!("edge2 id: {}", edge2.id);
        println!("edge3 id: {}", edge3.id);
        let edge_ids = [edge1.id, edge2.id, edge3.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|&edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        println!("here2");
        for bgei in block_group_edge_ids.iter() {
            println!("block group edge id: {}", bgei);
        }
        println!("block group id: {}", block_group.id);
        let _path1 = Path::create(&conn, "parent", block_group.id, &block_group_edge_ids);

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
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(&conn, &child_block_group_edges);
        let original_child_path = BlockGroup::get_current_path(&conn, child_block_group.id);
        let _child_path = original_child_path.new_path_with(
            &conn,
            2,
            6,
            block_group_edge_ids[0],
            block_group_edge_ids[1],
            node3_id,
        );

        let temp_dir = tempdir().unwrap();
        let gfa_path = temp_dir.path().join("parent-child-diff.gfa");
        gfa_sample_diff(&conn, collection_name, &gfa_path, None, Some("child"));

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
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids =
            BlockGroupEdge::bulk_create(&conn, &grandchild_block_group_edges);
        let original_grandchild_path =
            BlockGroup::get_current_path(&conn, grandchild_block_group.id);
        let _grandchild_path = original_grandchild_path.new_path_with(
            &conn,
            10,
            14,
            block_group_edge_ids[0],
            block_group_edge_ids[1],
            node4_id,
        );

        let gfa_path = temp_dir.path().join("parent-grandchild-diff.gfa");
        gfa_sample_diff(&conn, collection_name, &gfa_path, None, Some("grandchild"));

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
            Some("grandchild"),
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
    fn test_gfa_diff_against_nothing() {
        // Confirm diff of a sample against nothing is just the sample
        let conn = get_connection(None);

        let collection_name = "test collection";
        Collection::create(&conn, collection_name);
        let _sample = Sample::get_or_create(&conn, "test sample");
        let block_group = BlockGroup::create(
            &conn,
            collection_name,
            Some("test sample"),
            "test block group",
        );
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
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path1 = Path::create(&conn, "test path", block_group.id, &block_group_edge_ids);

        let temp_dir = tempdir().unwrap();
        let gfa_path = temp_dir.path().join("diff-against-nothing.gfa");
        gfa_sample_diff(&conn, collection_name, &gfa_path, None, Some("test sample"));

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
    fn test_self_diff() {
        // Confirm diff of a sample to itself just results in a graph that's a single path
        let conn = get_connection(None);

        let collection_name = "test collection";
        Collection::create(&conn, collection_name);
        let _sample = Sample::get_or_create(&conn, "test sample");
        let block_group = BlockGroup::create(
            &conn,
            collection_name,
            Some("test sample"),
            "test block group",
        );
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
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path1 = Path::create(&conn, "test path", block_group.id, &block_group_edge_ids);

        let temp_dir = tempdir().unwrap();
        let gfa_path = temp_dir.path().join("self-diff.gfa");
        gfa_sample_diff(
            &conn,
            collection_name,
            &gfa_path,
            Some("test sample"),
            Some("test sample"),
        );

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
        // Confirm diff of a sample to totally unrelated sample produces two separate paths
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
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path1 = Path::create(&conn, "parent", block_group.id, &block_group_edge_ids);

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
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path2 = Path::create(&conn, "parent", block_group2.id, &block_group_edge_ids);

        let temp_dir = tempdir().unwrap();
        let gfa_path = temp_dir.path().join("unrelated-diff.gfa");
        gfa_sample_diff(
            &conn,
            collection_name,
            &gfa_path,
            Some("sample1"),
            Some("sample2"),
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
        // Confirm diff of two paths that are in the same block group but don't share any nodes
        // results in two disjoint sequences
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
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path1 = Path::create(&conn, "parent", block_group.id, &block_group_edge_ids);

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
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(&conn, &block_group_edges);

        let _path2 = Path::create(&conn, "parent", block_group2.id, &block_group_edge_ids);

        let temp_dir = tempdir().unwrap();
        let gfa_path = temp_dir.path().join("unrelated-diff.gfa");
        gfa_sample_diff(
            &conn,
            collection_name,
            &gfa_path,
            Some("sample1"),
            Some("sample2"),
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
    fn test_gfa_diff_overlapping_replacements() {
        // Set up a child with a replacement, then a grandchild with a replacement on the child that
        // partially overlaps the child's replacement, and confirm diffs between all pairs from
        // (original, child, grandchild)
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
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(&conn, &block_group_edges);
        let bge1_id = block_group_edge_ids[0];
        let bge2_id = block_group_edge_ids[1];

        let _path1 = Path::create(&conn, "parent", block_group.id, &[bge1_id, bge2_id]);

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
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(&conn, &child_block_group_edges);
        let original_child_path = BlockGroup::get_current_path(&conn, child_block_group.id);
        let _child_path = original_child_path.new_path_with(
            &conn,
            2,
            6,
            block_group_edge_ids[0],
            block_group_edge_ids[1],
            node2_id,
        );

        let temp_dir = tempdir().unwrap();
        let gfa_path = temp_dir.path().join("parent-child-diff.gfa");
        gfa_sample_diff(&conn, collection_name, &gfa_path, None, Some("child"));

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
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids =
            BlockGroupEdge::bulk_create(&conn, &grandchild_block_group_edges);
        let original_grandchild_path =
            BlockGroup::get_current_path(&conn, grandchild_block_group.id);
        let _grandchild_path = original_grandchild_path.new_path_with(
            &conn,
            4,
            10,
            block_group_edge_ids[0],
            block_group_edge_ids[1],
            node3_id,
        );

        let gfa_path = temp_dir.path().join("parent-grandchild-diff.gfa");
        gfa_sample_diff(&conn, collection_name, &gfa_path, None, Some("grandchild"));

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
            Some("grandchild"),
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
