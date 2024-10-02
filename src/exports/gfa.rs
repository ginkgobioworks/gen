use itertools::Itertools;
use petgraph::prelude::DiGraphMap;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::models::{
    block_group_edge::BlockGroupEdge,
    collection::Collection,
    edge::{Edge, GroupBlock},
    node::{PATH_END_NODE_ID, PATH_START_NODE_ID},
    path::Path,
    path_edge::PathEdge,
    strand::Strand,
};

pub fn export_gfa(conn: &Connection, collection_name: &str, filename: &PathBuf) {
    let block_groups = Collection::get_block_groups(conn, collection_name);

    let mut edge_set = HashSet::new();
    for block_group in block_groups {
        let block_group_edges = BlockGroupEdge::edges_for_block_group(conn, block_group.id);
        edge_set.extend(block_group_edges.into_iter());
    }

    let mut edges = edge_set.into_iter().collect();
    let (blocks, boundary_edges) = Edge::blocks_from_edges(conn, &edges);
    edges.extend(boundary_edges.clone());

    let (graph, edges_by_node_pair) = Edge::build_graph(&edges, &blocks);

    let file = File::create(filename).unwrap();
    let mut writer = BufWriter::new(file);

    let mut terminal_block_ids = HashSet::new();
    for block in &blocks {
        if block.node_id == PATH_START_NODE_ID || block.node_id == PATH_END_NODE_ID {
            terminal_block_ids.insert(block.id);
            continue;
        }
    }

    write_segments(&mut writer, &blocks, &terminal_block_ids);
    write_links(
        &mut writer,
        &graph,
        &edges_by_node_pair,
        &terminal_block_ids,
    );
    write_paths(&mut writer, conn, collection_name, &blocks);
}

fn write_segments(
    writer: &mut BufWriter<File>,
    blocks: &Vec<GroupBlock>,
    terminal_block_ids: &HashSet<i64>,
) {
    for block in blocks {
        if terminal_block_ids.contains(&block.id) {
            continue;
        }
        writer
            .write_all(&segment_line(&block.sequence, block.id as usize).into_bytes())
            .unwrap_or_else(|_| {
                panic!(
                    "Error writing segment with sequence {} to GFA stream",
                    block.sequence,
                )
            });
    }
}

fn segment_line(sequence: &str, index: usize) -> String {
    format!("S\t{}\t{}\t{}\n", index + 1, sequence, "*")
}

fn write_links(
    writer: &mut BufWriter<File>,
    graph: &DiGraphMap<i64, ()>,
    edges_by_node_pair: &HashMap<(i64, i64), Edge>,
    terminal_block_ids: &HashSet<i64>,
) {
    for (source, target, ()) in graph.all_edges() {
        if terminal_block_ids.contains(&source) || terminal_block_ids.contains(&target) {
            continue;
        }
        let edge = edges_by_node_pair.get(&(source, target)).unwrap();
        writer
            .write_all(
                &link_line(source, edge.source_strand, target, edge.target_strand).into_bytes(),
            )
            .unwrap_or_else(|_| {
                panic!(
                    "Error writing link from segment {} to {} to GFA stream",
                    source, target,
                )
            });
    }
}

fn link_line(
    source_index: i64,
    source_strand: Strand,
    target_index: i64,
    target_strand: Strand,
) -> String {
    format!(
        "L\t{}\t{}\t{}\t{}\t0M\n",
        source_index + 1,
        source_strand,
        target_index + 1,
        target_strand
    )
}

// NOTE: A path is an immutable list of edges, but the sequence between the target of one edge and
// the source of the next may be "split" by later operations that add edges with sources or targets
// on a sequence that are in between those of a consecutive pair of edges in a path.  This function
// handles that case by collecting all the nodes between the target of one edge and the source of
// the next.
fn nodes_for_edges(
    edge1: &Edge,
    edge2: &Edge,
    blocks_by_node_and_start: &HashMap<(i64, i64), GroupBlock>,
    blocks_by_node_and_end: &HashMap<(i64, i64), GroupBlock>,
) -> Vec<i64> {
    let mut current_block = blocks_by_node_and_start
        .get(&(edge1.target_node_id, edge1.target_coordinate))
        .unwrap();
    let end_block = blocks_by_node_and_end
        .get(&(edge2.source_node_id, edge2.source_coordinate))
        .unwrap();
    let mut node_ids = vec![];
    #[allow(clippy::while_immutable_condition)]
    while current_block.id != end_block.id {
        node_ids.push(current_block.id);
        current_block = blocks_by_node_and_start
            .get(&(current_block.node_id, current_block.end))
            .unwrap();
    }
    node_ids.push(end_block.id);

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
        let edges_for_path = edges_by_path_id.get(&path.id).unwrap();
        let mut graph_node_ids = vec![];
        let mut node_strands = vec![];
        for (edge1, edge2) in edges_for_path.iter().tuple_windows() {
            let current_node_ids = nodes_for_edges(
                edge1,
                edge2,
                &blocks_by_node_and_start,
                &blocks_by_node_and_end,
            );
            for node_id in &current_node_ids {
                graph_node_ids.push(*node_id);
                node_strands.push(edge1.target_strand);
            }
        }

        writer
            .write_all(&path_line(&path.name, &graph_node_ids, &node_strands).into_bytes())
            .unwrap_or_else(|_| panic!("Error writing path {} to GFA stream", path.name));
    }
}

fn path_line(path_name: &str, node_ids: &[i64], node_strands: &[Strand]) -> String {
    let nodes = node_ids
        .iter()
        .zip(node_strands.iter())
        .map(|(node_id, node_strand)| format!("{}{}", *node_id + 1, node_strand))
        .collect::<Vec<String>>()
        .join(",");
    format!("P\t{}\t{}\n", path_name, nodes)
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::imports::gfa::import_gfa;
    use crate::models::{
        block_group::BlockGroup, collection::Collection, node::Node, sequence::Sequence,
    };

    use crate::test_helpers::{get_connection, setup_gen_dir};
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
        let node1_id = Node::create(&conn, &sequence1.hash);
        let node2_id = Node::create(&conn, &sequence2.hash);
        let node3_id = Node::create(&conn, &sequence3.hash);
        let node4_id = Node::create(&conn, &sequence4.hash);

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

        export_gfa(&conn, collection_name, &gfa_path);
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

        export_gfa(conn, &collection_name, &gfa_path);
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

        export_gfa(conn, &collection_name, &gfa_path);
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

        export_gfa(conn, &collection_name, &gfa_path);
        import_gfa(&gfa_path, "test collection 2", conn);

        let block_group2 = Collection::get_block_groups(conn, "test collection 2")
            .pop()
            .unwrap();
        let all_sequences2 = BlockGroup::get_all_sequences(conn, block_group2.id);

        assert_eq!(all_sequences, all_sequences2);
    }
}
