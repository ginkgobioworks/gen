use itertools::Itertools;
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

    let mut file = File::create(filename).unwrap();
    let mut writer = BufWriter::new(file);

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

    let blocks_by_id = blocks
        .clone()
        .into_iter()
        .map(|block| (block.id, block))
        .collect::<HashMap<i32, GroupBlock>>();

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
    format!("S\t{}\t{}\t{}\n", index, sequence, "*")
}

fn link_line(
    source_index: i32,
    source_strand: Strand,
    target_index: i32,
    target_strand: Strand,
) -> String {
    format!(
        "L\t{}\t{}\t{}\t{}\t*\n",
        source_index, source_strand, target_index, target_strand
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
    use crate::test_helpers::get_connection;
    use tempfile::tempdir;

    #[test]
    fn test_simple_export() {
        // Sets up a basic graph and then exports it to a GFA file
        let conn = get_connection(None);

        let collection_name = "test collection";
        let collection = Collection::create(&conn, collection_name);
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
    }
}
