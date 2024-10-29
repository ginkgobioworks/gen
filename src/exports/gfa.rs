use crate::graph::{GraphEdge, GraphNode};
use crate::models::{
    block_group::BlockGroup,
    block_group_edge::BlockGroupEdge,
    collection::Collection,
    edge::{Edge, GroupBlock},
    node::Node,
    path::Path,
    path_edge::PathEdge,
    strand::Strand,
};
use itertools::Itertools;
use petgraph::prelude::DiGraphMap;
use rusqlite::{types::Value as SQLValue, Connection};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub fn export_gfa(
    conn: &Connection,
    collection_name: &str,
    filename: &PathBuf,
    sample_name: Option<String>,
) {
    // General note about how we encode segment IDs.  The node ID and the start coordinate in the
    // sequence are all that's needed, because the end coordinate can be inferred from the length of
    // the segment's sequence.  So the segment ID is of the form <node ID>.<start coordinate>
    let block_groups = Collection::get_block_groups(conn, collection_name);

    let mut edge_set = HashSet::new();
    if let Some(sample) = sample_name {
        let block_groups = BlockGroup::query(
            conn,
            "select * from block_group where collection_name = ?1 AND sample_name = ?2;",
            vec![
                SQLValue::from(collection_name.to_string()),
                SQLValue::from(sample.clone()),
            ],
        );
        if block_groups.is_empty() {
            panic!(
                "No block groups found for collection {} and sample {}",
                collection_name, sample
            );
        }

        let block_group_id = block_groups[0].id;
        let block_group_edges = BlockGroupEdge::edges_for_block_group(conn, block_group_id);
        edge_set.extend(block_group_edges);
    } else {
        for block_group in block_groups {
            let block_group_edges = BlockGroupEdge::edges_for_block_group(conn, block_group.id);
            edge_set.extend(block_group_edges);
        }
    }

    let mut edges = edge_set.into_iter().collect();

    let blocks = Edge::blocks_from_edges(conn, &edges);
    let boundary_edges = Edge::boundary_edges_from_sequences(&blocks);
    edges.extend(boundary_edges.clone());

    let (graph, edges_by_node_pair) = Edge::build_graph(&edges, &blocks);

    let file = File::create(filename).unwrap();
    let mut writer = BufWriter::new(file);

    let node_sequence_starts_by_end_coordinate = blocks
        .iter()
        .filter(|block| !Node::is_terminal(block.node_id))
        .map(|block| ((block.node_id, block.end), block.start))
        .collect::<HashMap<(i64, i64), i64>>();
    write_segments(&mut writer, &blocks);
    write_links(
        &mut writer,
        &graph,
        &edges_by_node_pair,
        node_sequence_starts_by_end_coordinate,
    );
    write_paths(&mut writer, conn, collection_name, &blocks);
}

fn write_segments(writer: &mut BufWriter<File>, blocks: &Vec<GroupBlock>) {
    for block in blocks {
        if Node::is_terminal(block.node_id) {
            continue;
        }
        writer
            .write_all(&segment_line(&block.sequence(), block.node_id, block.start).into_bytes())
            .unwrap_or_else(|_| {
                panic!(
                    "Error writing segment with sequence {} to GFA stream",
                    block.sequence(),
                )
            });
    }
}

fn segment_line(sequence: &str, node_id: i64, sequence_start: i64) -> String {
    // NOTE: We encode the node ID and start coordinate in the segment ID
    format!("S\t{}.{}\t{}\t*\n", node_id, sequence_start, sequence,)
}

fn write_links(
    writer: &mut BufWriter<File>,
    graph: &DiGraphMap<GraphNode, GraphEdge>,
    edges_by_node_pair: &HashMap<(i64, i64), Edge>,
    node_sequence_starts_by_end_coordinate: HashMap<(i64, i64), i64>,
) {
    for (source, target, _edge_weight) in graph.all_edges() {
        let edge = edges_by_node_pair
            .get(&(source.block_id, target.block_id))
            .unwrap();
        if Node::is_terminal(edge.source_node_id) || Node::is_terminal(edge.target_node_id) {
            continue;
        }
        // Since we're encoding a segment ID as node ID + sequence start coordinate, we need to do
        // one step of translation to get that for an edge's source.  The edge's source is the node
        // ID + sequence end coordinate, so the following line converts that to the sequence start
        // coordinate.
        let sequence_start = node_sequence_starts_by_end_coordinate
            .get(&(edge.source_node_id, edge.source_coordinate))
            .unwrap();
        writer
            .write_all(
                &link_line(
                    edge.source_node_id,
                    *sequence_start,
                    edge.source_strand,
                    edge.target_node_id,
                    edge.target_coordinate,
                    edge.target_strand,
                )
                .into_bytes(),
            )
            .unwrap_or_else(|_| {
                panic!(
                    "Error writing link from segment {:?} to {:?} to GFA stream",
                    source, target,
                )
            });
    }
}

fn link_line(
    source_node_id: i64,
    source_coordinate: i64,
    source_strand: Strand,
    target_node_id: i64,
    target_coordinate: i64,
    target_strand: Strand,
) -> String {
    format!(
        "L\t{}.{}\t{}\t{}.{}\t{}\t0M\n",
        source_node_id,
        source_coordinate,
        source_strand,
        target_node_id,
        target_coordinate,
        target_strand
    )
}

// NOTE: A path is an immutable list of edges, but the sequence between the target of one edge and
// the source of the next may be "split" by later operations that add edges with sources or targets
// on a sequence that are in between those of a consecutive pair of edges in a path.  This function
// handles that case by collecting all the nodes between the target of one edge and the source of
// the next.
fn segments_for_edges(
    edge1: &Edge,
    edge2: &Edge,
    blocks_by_node_and_start: &HashMap<(i64, i64), GroupBlock>,
    blocks_by_node_and_end: &HashMap<(i64, i64), GroupBlock>,
) -> Vec<String> {
    let mut current_block = blocks_by_node_and_start
        .get(&(edge1.target_node_id, edge1.target_coordinate))
        .unwrap();
    let end_block = blocks_by_node_and_end
        .get(&(edge2.source_node_id, edge2.source_coordinate))
        .unwrap();
    let mut node_ids = vec![];
    #[allow(clippy::while_immutable_condition)]
    while current_block.id != end_block.id {
        node_ids.push(format!("{}.{}", current_block.node_id, current_block.start));
        current_block = blocks_by_node_and_start
            .get(&(current_block.node_id, current_block.end))
            .unwrap();
    }
    node_ids.push(format!("{}.{}", end_block.node_id, end_block.start));

    node_ids
}

fn write_paths(
    writer: &mut BufWriter<File>,
    conn: &Connection,
    collection_name: &str,
    blocks: &[GroupBlock],
) {
    let paths = Path::get_paths_for_collection(conn, collection_name);
    let edges_by_path_id =
        PathEdge::edges_for_paths(conn, paths.iter().map(|path| path.id).collect());

    let blocks_by_node_and_start = blocks
        .iter()
        .map(|block| ((block.node_id, block.start), block.clone()))
        .collect::<HashMap<(i64, i64), GroupBlock>>();
    let blocks_by_node_and_end = blocks
        .iter()
        .map(|block| ((block.node_id, block.end), block.clone()))
        .collect::<HashMap<(i64, i64), GroupBlock>>();

    for path in paths {
        let block_group = BlockGroup::get_by_id(conn, path.block_group_id);
        let sample_name = block_group.sample_name;

        let edges_for_path = edges_by_path_id.get(&path.id).unwrap();
        let mut graph_segment_ids = vec![];
        let mut node_strands = vec![];
        for (edge1, edge2) in edges_for_path.iter().tuple_windows() {
            let segment_ids = segments_for_edges(
                edge1,
                edge2,
                &blocks_by_node_and_start,
                &blocks_by_node_and_end,
            );
            for segment_id in &segment_ids {
                graph_segment_ids.push(segment_id.clone());
                node_strands.push(edge1.target_strand);
            }
        }

        let full_path_name = if sample_name.is_some() && sample_name.clone().unwrap() != "" {
            format!("{}.{}", path.name, sample_name.unwrap()).to_string()
        } else {
            path.name
        };
        writer
            .write_all(&path_line(&full_path_name, &graph_segment_ids, &node_strands).into_bytes())
            .unwrap_or_else(|_| panic!("Error writing path {} to GFA stream", full_path_name));
    }
}

fn path_line(path_name: &str, segment_ids: &[String], node_strands: &[Strand]) -> String {
    let segments = segment_ids
        .iter()
        .zip(node_strands.iter())
        .map(|(segment_id, node_strand)| format!("{}{}", segment_id, node_strand))
        .collect::<Vec<String>>()
        .join(",");
    format!("P\t{}\t{}\t*\n", path_name, segments)
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::imports::gfa::import_gfa;
    use crate::models::{
        block_group::{BlockGroup, PathChange},
        collection::Collection,
        node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID},
        path::PathBlock,
        sequence::Sequence,
    };

    use crate::test_helpers::{get_connection, setup_block_group, setup_gen_dir};
    use tempfile::tempdir;

    #[test]
    fn test_simple_export() {
        // Sets up a basic graph and then exports it to a GFA file
        let conn = get_connection(None);

        let collection_name = "test collection";
        Collection::create(&conn, collection_name);
        let block_group = BlockGroup::create(&conn, collection_name, None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAA")
            .save(&conn);
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTT")
            .save(&conn);
        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGG")
            .save(&conn);
        let sequence4 = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCC")
            .save(&conn);
        let node1_id = Node::create(&conn, &sequence1.hash, None);
        let node2_id = Node::create(&conn, &sequence2.hash, None);
        let node3_id = Node::create(&conn, &sequence3.hash, None);
        let node4_id = Node::create(&conn, &sequence4.hash, None);

        let edge1 = Edge::create(
            &conn,
            PATH_START_NODE_ID,
            0,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge2 = Edge::create(
            &conn,
            node1_id,
            4,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge3 = Edge::create(
            &conn,
            node2_id,
            4,
            Strand::Forward,
            node3_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge4 = Edge::create(
            &conn,
            node3_id,
            4,
            Strand::Forward,
            node4_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge5 = Edge::create(
            &conn,
            node4_id,
            4,
            Strand::Forward,
            PATH_END_NODE_ID,
            0,
            Strand::Forward,
            0,
            0,
        );

        BlockGroupEdge::bulk_create(
            &conn,
            block_group.id,
            &[edge1.id, edge2.id, edge3.id, edge4.id, edge5.id],
        );

        Path::create(
            &conn,
            "1234",
            block_group.id,
            &[edge1.id, edge2.id, edge3.id, edge4.id, edge5.id],
        );

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group.id);

        let temp_dir = tempdir().expect("Couldn't get handle to temp directory");
        let mut gfa_path = PathBuf::from(temp_dir.path());
        gfa_path.push("intermediate.gfa");

        export_gfa(&conn, collection_name, &gfa_path, None);
        // NOTE: Not directly checking file contents because segments are written in random order
        import_gfa(&gfa_path, "test collection 2", &conn);

        let block_group2 = Collection::get_block_groups(&conn, "test collection 2")
            .pop()
            .unwrap();
        let all_sequences2 = BlockGroup::get_all_sequences(&conn, block_group2.id);

        assert_eq!(all_sequences, all_sequences2);

        let paths = Path::get_paths_for_collection(&conn, "test collection 2");
        assert_eq!(paths.len(), 1);
        assert_eq!(Path::sequence(&conn, paths[0].clone()), "AAAATTTTGGGGCCCC");
    }

    #[test]
    fn test_simple_round_trip() {
        setup_gen_dir();
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/simple.gfa");
        let collection_name = "test".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let all_sequences = BlockGroup::get_all_sequences(conn, block_group_id);

        let temp_dir = tempdir().expect("Couldn't get handle to temp directory");
        let mut gfa_path = PathBuf::from(temp_dir.path());
        gfa_path.push("intermediate.gfa");

        export_gfa(conn, &collection_name, &gfa_path, None);
        import_gfa(&gfa_path, "test collection 2", conn);

        let block_group2 = Collection::get_block_groups(conn, "test collection 2")
            .pop()
            .unwrap();
        let all_sequences2 = BlockGroup::get_all_sequences(conn, block_group2.id);

        assert_eq!(all_sequences, all_sequences2);
    }

    #[test]
    fn test_anderson_round_trip() {
        setup_gen_dir();
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/anderson_promoters.gfa");
        let collection_name = "anderson promoters".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let all_sequences = BlockGroup::get_all_sequences(conn, block_group_id);

        let temp_dir = tempdir().expect("Couldn't get handle to temp directory");
        let mut gfa_path = PathBuf::from(temp_dir.path());
        gfa_path.push("intermediate.gfa");

        export_gfa(conn, &collection_name, &gfa_path, None);
        import_gfa(&gfa_path, "anderson promoters 2", conn);

        let block_group2 = Collection::get_block_groups(conn, "anderson promoters 2")
            .pop()
            .unwrap();
        let all_sequences2 = BlockGroup::get_all_sequences(conn, block_group2.id);

        assert_eq!(all_sequences, all_sequences2);
    }

    #[test]
    fn test_reverse_strand_round_trip() {
        setup_gen_dir();
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/reverse_strand.gfa");
        let collection_name = "test".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let all_sequences = BlockGroup::get_all_sequences(conn, block_group_id);

        let temp_dir = tempdir().expect("Couldn't get handle to temp directory");
        let mut gfa_path = PathBuf::from(temp_dir.path());
        gfa_path.push("intermediate.gfa");

        export_gfa(conn, &collection_name, &gfa_path, None);
        import_gfa(&gfa_path, "test collection 2", conn);

        let block_group2 = Collection::get_block_groups(conn, "test collection 2")
            .pop()
            .unwrap();
        let all_sequences2 = BlockGroup::get_all_sequences(conn, block_group2.id);

        assert_eq!(all_sequences, all_sequences2);
    }

    #[test]
    fn test_sequence_is_split_into_multiple_segments() {
        // Confirm that if edges are added to or from a sequence, that results in the sequence being
        // split into multiple segments in the exported GFA, and that the multiple segments are
        // re-imported as multiple sequences
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            path_accession: None,
            start: 7,
            end: 15,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let edges = BlockGroupEdge::edges_for_block_group(&conn, block_group_id);
        let mut node_ids = HashSet::new();
        let mut edge_ids = HashSet::new();
        for edge in edges {
            if !Node::is_terminal(edge.source_node_id) {
                node_ids.insert(edge.source_node_id);
            }
            if !Node::is_terminal(edge.target_node_id) {
                node_ids.insert(edge.target_node_id);
            }
            if !Node::is_terminal(edge.source_node_id) && !Node::is_terminal(edge.target_node_id) {
                edge_ids.insert(edge.id);
            }
        }

        // The original 10-length A, T, C, G sequences, plus NNNN
        assert_eq!(node_ids.len(), 5);
        // 3 edges from A sequence -> T sequence, T sequence -> C sequence, C sequence -> G sequence
        // 2 edges to and from NNNN
        // 5 total
        assert_eq!(edge_ids.len(), 5);

        let nodes = Node::get_nodes(&conn, node_ids.into_iter().collect::<Vec<i64>>());
        let mut node_hashes = HashSet::new();
        for node in nodes {
            if !Node::is_terminal(node.id) {
                node_hashes.insert(node.sequence_hash);
            }
        }

        // The original 10-length A, T, C, G sequences, plus NNNN
        assert_eq!(node_hashes.len(), 5);

        let temp_dir = tempdir().expect("Couldn't get handle to temp directory");
        let mut gfa_path = PathBuf::from(temp_dir.path());
        gfa_path.push("intermediate.gfa");
        export_gfa(&conn, "test", &gfa_path, None);
        import_gfa(&gfa_path, "test collection 2", &conn);

        let block_group2 = Collection::get_block_groups(&conn, "test collection 2")
            .pop()
            .unwrap();

        let edges2 = BlockGroupEdge::edges_for_block_group(&conn, block_group2.id);
        let mut node_ids2 = HashSet::new();
        let mut edge_ids2 = HashSet::new();
        for edge in edges2 {
            if !Node::is_terminal(edge.source_node_id) {
                node_ids2.insert(edge.source_node_id);
            }
            if !Node::is_terminal(edge.target_node_id) {
                node_ids2.insert(edge.target_node_id);
            }
            if !Node::is_terminal(edge.source_node_id) && !Node::is_terminal(edge.target_node_id) {
                edge_ids2.insert(edge.id);
            }
        }

        // The 10-length A and T sequences have now been split in two (showing up as different
        // segments in the exported GFA), so expect two more nodes
        assert_eq!(node_ids2.len(), 7);
        // 3 edges from A sequence -> T sequence, T sequence -> C sequence, C sequence -> G sequence
        // 2 boundary edges (exported as real links) in A sequence and T sequence
        // 2 edges to and from NNNN
        // 7 total
        assert_eq!(edge_ids2.len(), 7);

        let nodes2 = Node::get_nodes(&conn, node_ids2.into_iter().collect::<Vec<i64>>());
        let mut node_hashes2 = HashSet::new();
        for node in nodes2 {
            if !Node::is_terminal(node.id) {
                node_hashes2.insert(node.sequence_hash);
            }
        }

        // The 10-length A and T sequences have now been split in two, but since the T sequences was
        // split in half, there's just one new TTTTT sequence shared by 2 nodes
        assert_eq!(node_hashes2.len(), 6);
    }
}
