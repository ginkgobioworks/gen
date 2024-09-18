use petgraph::prelude::DiGraphMap;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::models::{
    self,
    block_group::BlockGroup,
    block_group_edge::BlockGroupEdge,
    collection::Collection,
    edge::{Edge, GroupBlock},
    path::Path,
    path_edge::PathEdge,
    sequence::Sequence,
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

    let (graph, edges_by_node_pair) = Edge::build_graph(conn, &edges, &blocks);

    let file = File::create(filename).unwrap();
    let mut writer = BufWriter::new(file);

    let terminal_block_ids = write_blocks(&mut writer, blocks.clone());

    write_edges(
        &mut writer,
        graph,
        edges_by_node_pair.clone(),
        terminal_block_ids,
    );

    let paths = Path::get_paths_for_collection(conn, collection_name);
    let edges_by_path_id =
        PathEdge::edges_for_paths(conn, paths.iter().map(|path| path.id).collect());
    let node_pairs_by_edge_id = edges_by_node_pair
        .iter()
        .map(|(node_pair, edge)| (edge.id, *node_pair))
        .collect::<HashMap<i32, (i32, i32)>>();

    println!("here1");
    for path in paths {
        println!("here2");
        println!("{}", path.name);
        let edges_for_path = edges_by_path_id.get(&path.id).unwrap();
        let mut node_ids = vec![];
        let mut node_strands = vec![];
        // Edges actually have too much information, the target of one is the same as the source of
        // the next, so just iterate and take the target node to get the path of segments.
        for edge in edges_for_path[0..edges_for_path.len() - 1].iter() {
            let (_, target) = node_pairs_by_edge_id.get(&edge.id).unwrap();
            node_ids.push(*target);
            node_strands.push(edge.target_strand);
        }

        writer
            .write_all(&path_line(&path.name, &node_ids, &node_strands).into_bytes())
            .unwrap_or_else(|_| panic!("Error writing path {} to GFA stream", path.name));
    }
}

fn path_line(path_name: &str, node_ids: &[i32], node_strands: &[Strand]) -> String {
    let nodes = node_ids
        .iter()
        .zip(node_strands.iter())
        .map(|(node_id, node_strand)| format!("{}{}", node_id + 1, node_strand))
        .collect::<Vec<String>>()
        .join(",");
    format!("P\t{}\t{}\n", path_name, nodes)
}

fn write_blocks(writer: &mut BufWriter<File>, blocks: Vec<GroupBlock>) -> HashSet<i32> {
    let mut terminal_block_ids = HashSet::new();
    for block in &blocks {
        if block.sequence_hash == Sequence::PATH_START_HASH
            || block.sequence_hash == Sequence::PATH_END_HASH
        {
            terminal_block_ids.insert(block.id);
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

    terminal_block_ids
}

fn write_edges(
    writer: &mut BufWriter<File>,
    graph: DiGraphMap<i32, ()>,
    edges_by_node_pair: HashMap<(i32, i32), Edge>,
    terminal_block_ids: HashSet<i32>,
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

fn segment_line(sequence: &str, index: usize) -> String {
    format!("S\t{}\t{}\t{}\n", index + 1, sequence, "*")
}

fn link_line(
    source_index: i32,
    source_strand: Strand,
    target_index: i32,
    target_strand: Strand,
) -> String {
    format!(
        "L\t{}\t{}\t{}\t{}\t*\n",
        source_index + 1,
        source_strand,
        target_index + 1,
        target_strand
    )
}

mod tests {
    use rusqlite::Connection;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::imports::gfa::import_gfa;
    use crate::models::{
        block_group::BlockGroup, block_group_edge::BlockGroupEdge, collection::Collection,
        edge::Edge, sequence::Sequence,
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

        let edge1 = Edge::create(
            &conn,
            Sequence::PATH_START_HASH.to_string(),
            0,
            Strand::Forward,
            sequence1.hash.clone(),
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge2 = Edge::create(
            &conn,
            sequence1.hash,
            4,
            Strand::Forward,
            sequence2.hash.clone(),
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge3 = Edge::create(
            &conn,
            sequence2.hash,
            4,
            Strand::Forward,
            sequence3.hash.clone(),
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge4 = Edge::create(
            &conn,
            sequence3.hash,
            4,
            Strand::Forward,
            sequence4.hash.clone(),
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge5 = Edge::create(
            &conn,
            sequence4.hash,
            4,
            Strand::Forward,
            Sequence::PATH_END_HASH.to_string(),
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
}
