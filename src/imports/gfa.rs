use gfa_reader::Gfa;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::path::Path as FilePath;

use crate::models::{
    block_group::BlockGroup,
    block_group_edge::BlockGroupEdge,
    collection::Collection,
    edge::{Edge, EdgeData},
    node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID},
    path::Path,
    sequence::Sequence,
    strand::Strand,
};

fn bool_to_strand(direction: bool) -> Strand {
    if direction {
        Strand::Forward
    } else {
        Strand::Reverse
    }
}

pub fn import_gfa(gfa_path: &FilePath, collection_name: &str, conn: &Connection) {
    Collection::create(conn, collection_name);
    let block_group = BlockGroup::create(conn, collection_name, None, "");
    let gfa: Gfa<u64, (), ()> = Gfa::parse_gfa_file(gfa_path.to_str().unwrap());
    let mut sequences_by_segment_id: HashMap<u64, Sequence> = HashMap::new();
    let mut node_ids_by_segment_id: HashMap<u64, i32> = HashMap::new();

    for segment in &gfa.segments {
        let input_sequence = segment.sequence.get_string(&gfa.sequence);
        let sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence(input_sequence)
            .save(conn);
        sequences_by_segment_id.insert(segment.id, sequence.clone());
        let node = Node::create(conn, &sequence.hash);
        node_ids_by_segment_id.insert(segment.id, node.id);
    }

    let mut edges = HashSet::new();
    for link in &gfa.links {
        let source = sequences_by_segment_id.get(&link.from).unwrap();
        let source_node_id = *node_ids_by_segment_id.get(&link.from).unwrap();
        let target_node_id = *node_ids_by_segment_id.get(&link.to).unwrap();
        edges.insert(edge_data_from_fields(
            source_node_id,
            source.length,
            bool_to_strand(link.from_dir),
            target_node_id,
            bool_to_strand(link.to_dir),
        ));
    }

    for input_path in &gfa.paths {
        let mut source_node_id = PATH_START_NODE_ID;
        let mut source_coordinate = 0;
        let mut source_strand = Strand::Forward;
        for (index, segment_id) in input_path.nodes.iter().enumerate() {
            let target = sequences_by_segment_id.get(segment_id).unwrap();
            let target_node_id = *node_ids_by_segment_id.get(segment_id).unwrap();
            let target_strand = bool_to_strand(input_path.dir[index]);
            edges.insert(edge_data_from_fields(
                source_node_id,
                source_coordinate,
                source_strand,
                target_node_id,
                target_strand,
            ));
            source_node_id = target_node_id;
            source_coordinate = target.length;
            source_strand = target_strand;
        }
        edges.insert(edge_data_from_fields(
            source_node_id,
            source_coordinate,
            source_strand,
            PATH_END_NODE_ID,
            Strand::Forward,
        ));
    }

    for input_walk in &gfa.walk {
        let mut source_node_id = PATH_START_NODE_ID;
        let mut source_coordinate = 0;
        let mut source_strand = Strand::Forward;
        for (index, segment_id) in input_walk.walk_id.iter().enumerate() {
            let target = sequences_by_segment_id.get(segment_id).unwrap();
            let target_node_id = *node_ids_by_segment_id.get(segment_id).unwrap();
            let target_strand = bool_to_strand(input_walk.walk_dir[index]);
            edges.insert(edge_data_from_fields(
                source_node_id,
                source_coordinate,
                source_strand,
                target_node_id,
                target_strand,
            ));
            source_node_id = target_node_id;
            source_coordinate = target.length;
            source_strand = target_strand;
        }
        edges.insert(edge_data_from_fields(
            source_node_id,
            source_coordinate,
            source_strand,
            PATH_END_NODE_ID,
            Strand::Forward,
        ));
    }

    let edge_ids = Edge::bulk_create(conn, edges.into_iter().collect::<Vec<EdgeData>>());
    BlockGroupEdge::bulk_create(conn, block_group.id, &edge_ids);

    let saved_edges = Edge::bulk_load(conn, &edge_ids);
    let mut edge_ids_by_data = HashMap::new();
    for edge in saved_edges {
        let key = edge_data_from_fields(
            edge.source_node_id,
            edge.source_coordinate,
            edge.source_strand,
            edge.target_node_id,
            edge.target_strand,
        );
        edge_ids_by_data.insert(key, edge.id);
    }

    for input_path in &gfa.paths {
        let path_name = &input_path.name;
        let mut source_node_id = PATH_START_NODE_ID;
        let mut source_coordinate = 0;
        let mut source_strand = Strand::Forward;
        let mut path_edge_ids = vec![];
        for (index, segment_id) in input_path.nodes.iter().enumerate() {
            let target = sequences_by_segment_id.get(segment_id).unwrap();
            let target_node_id = *node_ids_by_segment_id.get(segment_id).unwrap();
            let target_strand = bool_to_strand(input_path.dir[index]);
            let key = edge_data_from_fields(
                source_node_id,
                source_coordinate,
                source_strand,
                target_node_id,
                target_strand,
            );
            let edge_id = *edge_ids_by_data.get(&key).unwrap();
            path_edge_ids.push(edge_id);
            source_node_id = target_node_id;
            source_coordinate = target.length;
            source_strand = target_strand;
        }
        let key = edge_data_from_fields(
            source_node_id,
            source_coordinate,
            source_strand,
            PATH_END_NODE_ID,
            Strand::Forward,
        );
        let edge_id = *edge_ids_by_data.get(&key).unwrap();
        path_edge_ids.push(edge_id);
        Path::create(conn, path_name, block_group.id, &path_edge_ids);
    }

    for input_walk in &gfa.walk {
        let path_name = &input_walk.sample_id;
        let mut source_node_id = PATH_START_NODE_ID;
        let mut source_coordinate = 0;
        let mut source_strand = Strand::Forward;
        let mut path_edge_ids = vec![];
        for (index, segment_id) in input_walk.walk_id.iter().enumerate() {
            let target = sequences_by_segment_id.get(segment_id).unwrap();
            let target_node_id = *node_ids_by_segment_id.get(segment_id).unwrap();
            let target_strand = bool_to_strand(input_walk.walk_dir[index]);
            let key = edge_data_from_fields(
                source_node_id,
                source_coordinate,
                source_strand,
                target_node_id,
                target_strand,
            );
            let edge_id = *edge_ids_by_data.get(&key).unwrap();
            path_edge_ids.push(edge_id);
            source_node_id = target_node_id;
            source_coordinate = target.length;
            source_strand = target_strand;
        }
        let key = edge_data_from_fields(
            source_node_id,
            source_coordinate,
            source_strand,
            PATH_END_NODE_ID,
            Strand::Forward,
        );
        let edge_id = *edge_ids_by_data.get(&key).unwrap();
        path_edge_ids.push(edge_id);
        Path::create(conn, path_name, block_group.id, &path_edge_ids);
    }
}

fn edge_data_from_fields(
    source_node_id: i32,
    source_coordinate: i32,
    source_strand: Strand,
    target_node_id: i32,
    target_strand: Strand,
) -> EdgeData {
    EdgeData {
        source_node_id,
        source_coordinate,
        source_strand,
        target_node_id,
        target_coordinate: 0,
        target_strand,
        chromosome_index: 0,
        phased: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{get_connection, setup_gen_dir};
    use rusqlite::types::Value as SQLValue;
    use std::path::PathBuf;

    #[test]
    fn test_import_simple_gfa() {
        setup_gen_dir();
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/simple.gfa");
        let collection_name = "test".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let path = Path::get_paths(
            conn,
            "select * from path where block_group_id = ?1 AND name = ?2",
            vec![
                SQLValue::from(block_group_id),
                SQLValue::from("124".to_string()),
            ],
        )[0]
        .clone();

        let result = Path::sequence(conn, path);
        assert_eq!(result, "ATGGCATATTCGCAGCT");
    }

    #[test]
    fn test_import_no_path_gfa() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/no_path.gfa");
        let collection_name = "no path".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let all_sequences = BlockGroup::get_all_sequences(conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec!["AAAATTTTGGGGCCCC".to_string()])
        );
    }

    #[test]
    fn test_import_gfa_with_walk() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/walk.gfa");
        let collection_name = "walk".to_string();
        let conn = &mut get_connection(None);
        import_gfa(&gfa_path, &collection_name, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let path = Path::get_paths(
            conn,
            "select * from path where block_group_id = ?1 AND name = ?2",
            vec![
                SQLValue::from(block_group_id),
                SQLValue::from("291344".to_string()),
            ],
        )[0]
        .clone();

        let result = Path::sequence(conn, path);
        assert_eq!(result, "ACCTACAAATTCAAAC");
    }

    #[test]
    fn test_import_gfa_with_reverse_strand_edges() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/reverse_strand.gfa");
        let collection_name = "test".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let path = Path::get_paths(
            conn,
            "select * from path where block_group_id = ?1 AND name = ?2",
            vec![
                SQLValue::from(block_group_id),
                SQLValue::from("124".to_string()),
            ],
        )[0]
        .clone();

        let result = Path::sequence(conn, path);
        assert_eq!(result, "TATGCCAGCTGCGAATA");
    }

    #[test]
    fn test_import_anderson_promoters() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/anderson_promoters.gfa");
        let collection_name = "anderson promoters".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, conn);

        let paths = Path::get_paths_for_collection(conn, &collection_name);
        assert_eq!(paths.len(), 20);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let path = Path::get_paths(
            conn,
            "select * from path where block_group_id = ?1 AND name = ?2",
            vec![
                SQLValue::from(block_group_id),
                SQLValue::from("BBa_J23100".to_string()),
            ],
        )[0]
        .clone();

        let result = Path::sequence(conn, path);
        let expected_sequence_parts = vec!["T", "T", "G", "A", "C", "G", "GCTAGCTCAG", "T", "CCT", "A", "GG", "T", "A", "C", "A", "G",
					   "TGCTAGCTACTAGTGAAAGAGGAGAAATACTAGATGGCTTCCTCCGAAGACGTTATCAAAGAGTTCATGCGTTTCAAAGTTCGTATGGAAGGTTCCGTTAACGGTCACGAGTTCGAAATCGAAGGTGAAGGTGAAGGTCGTCCGTACGAAGGTACCCAGACCGCTAAACTGAAAGTTACCAAAGGTGGTCCGCTGCCGTTCGCTTGGGACATCCTGTCCCCGCAGTTCCAGTACGGTTCCAAAGCTTACGTTAAACACCCGGCTGACATCCCGGACTACCTGAAACTGTCCTTCCCGGAAGGTTTCAAATGGGAACGTGTTATGAACTTCGAAGACGGTGGTGTTGTTACCGTTACCCAGGACTCCTCCCTGCAAGACGGTGAGTTCATCTACAAAGTTAAACTGCGTGGTACCAACTTCCCGTCCGACGGTCCGGTTATGCAGAAAAAAACCATGGGTTGGGAAGCTTCCACCGAACGTATGTACCCGGAAGACGGTGCTCTGAAAGGTGAAATCAAAATGCGTCTGAAACTGAAAGACGGTGGTCACTACGACGCTGAAGTTAAAACCACCTACATGGCTAAAAAACCGGTTCAGCTGCCGGGTGCTTACAAAACCGACATCAAACTGGACATCACCTCCCACAACGAAGACTACACCATCGTTGAACAGTACGAACGTGCTGAAGGTCGTCACTCCACCGGTGCTTAATAACGCTGATAGTGCTAGTGTAGATCGCTACTAGAGCCAGGCATCAAATAAAACGAAAGGCTCAGTCGAAAGACTGGGCCTTTCGTTTTATCTGTTGTTTGTCGGTGAACGCTCTCTACTAGAGTCACACTGGCTCACCTTCGGGTGGGCCTTTCTGCGTTTATATACTAGAAGCGGCCGCTGCAGGCTTCCTCGCTCACTGACTCGCTGCGCTCGGTCGTTCGGCTGCGGCGAGCGGTATCAGCTCACTCAAAGGCGGTAATACGGTTATCCACAGAATCAGGGGATAACGCAGGAAAGAACATGTGAGCAAAAGGCCAGCAAAAGGCCAGGAACCGTAAAAAGGCCGCGTTGCTGGCGTTTTTCCATAGGCTCCGCCCCCCTGACGAGCATCACAAAAATCGACGCTCAAGTCAGAGGTGGCGAAACCCGACAGGACTATAAAGATACCAGGCGTTTCCCCCTGGAAGCTCCCTCGTGCGCTCTCCTGTTCCGACCCTGCCGCTTACCGGATACCTGTCCGCCTTTCTCCCTTCGGGAAGCGTGGCGCTTTCTCATAGCTCACGCTGTAGGTATCTCAGTTCGGTGTAGGTCGTTCGCTCCAAGCTGGGCTGTGTGCACGAACCCCCCGTTCAGCCCGACCGCTGCGCCTTATCCGGTAACTATCGTCTTGAGTCCAACCCGGTAAGACACGACTTATCGCCACTGGCAGCAGCCACTGGTAACAGGATTAGCAGAGCGAGGTATGTAGGCGGTGCTACAGAGTTCTTGAAGTGGTGGCCTAACTACGGCTACACTAGAAGGACAGTATTTGGTATCTGCGCTCTGCTGAAGCCAGTTACCTTCGGAAAAAGAGTTGGTAGCTCTTGATCCGGCAAACAAACCACCGCTGGTAGCGGTGGTTTTTTTGTTTGCAAGCAGCAGATTACGCGCAGAAAAAAAGGATCTCAAGAAGATCCTTTGATCTTTTCTACGGGGTCTGACGCTCAGTGGAACGAAAACTCACGTTAAGGGATTTTGGTCATGAGATTATCAAAAAGGATCTTCACCTAGATCCTTTTAAATTAAAAATGAAGTTTTAAATCAATCTAAAGTATATATGAGTAAACTTGGTCTGACAGTTACCAATGCTTAATCAGTGAGGCACCTATCTCAGCGATCTGTCTATTTCGTTCATCCATAGTTGCCTGACTCCCCGTCGTGTAGATAACTACGATACGGGAGGGCTTACCATCTGGCCCCAGTGCTGCAATGATACCGCGAGACCCACGCTCACCGGCTCCAGATTTATCAGCAATAAACCAGCCAGCCGGAAGGGCCGAGCGCAGAAGTGGTCCTGCAACTTTATCCGCCTCCATCCAGTCTATTAATTGTTGCCGGGAAGCTAGAGTAAGTAGTTCGCCAGTTAATAGTTTGCGCAACGTTGTTGCCATTGCTACAGGCATCGTGGTGTCACGCTCGTCGTTTGGTATGGCTTCATTCAGCTCCGGTTCCCAACGATCAAGGCGAGTTACATGATCCCCCATGTTGTGCAAAAAAGCGGTTAGCTCCTTCGGTCCTCCGATCGTTGTCAGAAGTAAGTTGGCCGCAGTGTTATCACTCATGGTTATGGCAGCACTGCATAATTCTCTTACTGTCATGCCATCCGTAAGATGCTTTTCTGTGACTGGTGAGTACTCAACCAAGTCATTCTGAGAATAGTGTATGCGGCGACCGAGTTGCTCTTGCCCGGCGTCAATACGGGATAATACCGCGCCACATAGCAGAACTTTAAAAGTGCTCATCATTGGAAAACGTTCTTCGGGGCGAAAACTCTCAAGGATCTTACCGCTGTTGAGATCCAGTTCGATGTAACCCACTCGTGCACCCAACTGATCTTCAGCATCTTTTACTTTCACCAGCGTTTCTGGGTGAGCAAAAACAGGAAGGCAAAATGCCGCAAAAAAGGGAATAAGGGCGACACGGAAATGTTGAATACTCATACTCTTCCTTTTTCAATATTATTGAAGCATTTATCAGGGTTATTGTCTCATGAGCGGATACATATTTGAATGTATTTAGAAAAATAAACAAATAGGGGTTCCGCGCACATTTCCCCGAAAAGTGCCACCTGACGTCTAAGAAACCATTATTATCATGACATTAACCTATAAAAATAGGCGTATCACGAGGCAGAATTTCAGATAAAAAAAATCCTTAGCTTTCGCTAAGGATGATTTCTGGAATTCGCGGCCGCATCTAGAG"];
        let expected_sequence = expected_sequence_parts.join("");
        assert_eq!(result, expected_sequence);
    }

    #[test]
    fn test_import_aa_gfa() {
        setup_gen_dir();
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/aa.gfa");
        let collection_name = "test".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let path = Path::get_paths(
            conn,
            "select * from path where block_group_id = ?1 AND name = ?2",
            vec![
                SQLValue::from(block_group_id),
                SQLValue::from("124".to_string()),
            ],
        )[0]
        .clone();

        let result = Path::sequence(conn, path);
        assert_eq!(result, "AA");

        let all_sequences = BlockGroup::get_all_sequences(conn, block_group_id);
        assert_eq!(all_sequences, HashSet::from_iter(vec!["AA".to_string()]));
    }
}
