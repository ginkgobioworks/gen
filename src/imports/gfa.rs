use rusqlite;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::path::Path as FilePath;

use crate::gfa::bool_to_strand;
use crate::gfa_reader::Gfa;
use crate::models::sample::Sample;
use crate::models::{
    block_group::BlockGroup,
    block_group_edge::{BlockGroupEdge, BlockGroupEdgeData},
    collection::Collection,
    edge::{Edge, EdgeData},
    node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID},
    path::Path,
    sequence::Sequence,
    strand::Strand,
};
use crate::progress_bar::{get_handler, get_progress_bar, get_time_elapsed_bar};

pub fn import_gfa<'a>(
    gfa_path: &FilePath,
    collection_name: &str,
    sample_name: impl Into<Option<&'a str>>,
    conn: &Connection,
) {
    let progress_bar = get_handler();
    Collection::create(conn, collection_name);
    let sample_name = sample_name.into();
    if let Some(sample_name) = sample_name {
        Sample::get_or_create(conn, sample_name);
    }
    let block_group = BlockGroup::create(conn, collection_name, sample_name, "");
    let bar = progress_bar.add(get_time_elapsed_bar());
    bar.set_message("Parsing GFA");
    let gfa: Gfa<String, (), ()> = Gfa::parse_gfa_file(gfa_path.to_str().unwrap());
    let mut sequences_by_segment_id: HashMap<&String, Sequence> = HashMap::new();
    let mut node_ids_by_segment_id: HashMap<&String, i64> = HashMap::new();
    bar.finish();

    let bar = progress_bar.add(get_progress_bar(gfa.segments.len() as u64));
    bar.set_message("Parsing Segments");
    for segment in &gfa.segments {
        let input_sequence = segment.sequence.get_string(&gfa.sequence);
        let sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence(input_sequence)
            .save(conn);
        sequences_by_segment_id.insert(&segment.id, sequence.clone());
        let node_id = Node::create(conn, &sequence.hash, None);
        node_ids_by_segment_id.insert(&segment.id, node_id);
        bar.inc(1);
    }
    bar.finish();

    let mut edges = HashSet::new();
    let bar = progress_bar.add(get_progress_bar(gfa.links.len() as u64));
    let mut source_refs_in_links = HashSet::new();
    let mut target_refs_in_links = HashSet::new();

    bar.set_message("Parsing Links");
    for link in &gfa.links {
        let source = sequences_by_segment_id.get(&link.from).unwrap();
        let source_node_id = *node_ids_by_segment_id.get(&link.from).unwrap();
        source_refs_in_links.insert(&link.from);
        let target_node_id = *node_ids_by_segment_id.get(&link.to).unwrap();
        target_refs_in_links.insert(&link.to);
        edges.insert(edge_data_from_fields(
            source_node_id,
            source.length,
            bool_to_strand(link.from_dir),
            target_node_id,
            bool_to_strand(link.to_dir),
        ));
        bar.inc(1);
    }
    bar.finish();

    let pure_source_refs = source_refs_in_links
        .difference(&target_refs_in_links)
        .collect::<HashSet<_>>();
    let pure_target_refs = target_refs_in_links
        .difference(&source_refs_in_links)
        .collect::<HashSet<_>>();
    for source_ref in pure_source_refs {
        let source_node_id = *node_ids_by_segment_id.get(source_ref).unwrap();
        edges.insert(edge_data_from_fields(
            PATH_START_NODE_ID,
            0,
            Strand::Forward,
            source_node_id,
            Strand::Forward,
        ));
    }

    for target_ref in pure_target_refs {
        let target_node_id = *node_ids_by_segment_id.get(target_ref).unwrap();
        let target_sequence = sequences_by_segment_id.get(target_ref).unwrap();
        edges.insert(edge_data_from_fields(
            target_node_id,
            target_sequence.length,
            Strand::Forward,
            PATH_END_NODE_ID,
            Strand::Forward,
        ));
    }

    let bar = progress_bar.add(get_progress_bar(gfa.paths.len() as u64));
    bar.set_message("Parsing Paths");
    for input_path in &gfa.paths {
        let mut source_node_id = PATH_START_NODE_ID;
        let mut source_coordinate = 0;
        let mut source_strand = Strand::Forward;
        for (index, segment_id) in input_path.segments.iter().enumerate() {
            let target = sequences_by_segment_id.get(segment_id).unwrap();
            let target_node_id = *node_ids_by_segment_id.get(segment_id).unwrap();
            let target_strand = bool_to_strand(input_path.strands[index]);
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
        bar.inc(1);
    }
    bar.finish();

    let bar = progress_bar.add(get_progress_bar(gfa.paths.len() as u64));
    bar.set_message("Parsing Walks");
    for input_walk in &gfa.walk {
        let mut source_node_id = PATH_START_NODE_ID;
        let mut source_coordinate = 0;
        let mut source_strand = Strand::Forward;
        for (index, segment_id) in input_walk.segments.iter().enumerate() {
            let target = sequences_by_segment_id.get(segment_id).unwrap();
            let target_node_id = *node_ids_by_segment_id.get(segment_id).unwrap();
            let target_strand = bool_to_strand(input_walk.strands[index]);
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
        bar.inc(1);
    }
    bar.finish();

    let bar = progress_bar.add(get_time_elapsed_bar());
    bar.set_message("Creating Gen Objects");
    let edge_ids = Edge::bulk_create(conn, &edges.into_iter().collect::<Vec<EdgeData>>());
    let block_group_edges = edge_ids
        .iter()
        .map(|edge_id| BlockGroupEdgeData {
            block_group_id: block_group.id,
            edge_id: *edge_id,
            chromosome_index: 0,
            phased: 0,
            source_phase_layer_id: 0,
            target_phase_layer_id: 0,
        })
        .collect::<Vec<_>>();

    let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);
    let block_group_edge_ids_by_edge_id = block_group_edge_ids
        .iter()
        .enumerate()
        .map(|(index, block_group_edge_id)| (edge_ids[index], block_group_edge_id))
        .collect::<HashMap<_, _>>();

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
        for (index, segment_id) in input_path.segments.iter().enumerate() {
            let target = sequences_by_segment_id.get(segment_id).unwrap();
            let target_node_id = *node_ids_by_segment_id.get(segment_id).unwrap();
            let target_strand = bool_to_strand(input_path.strands[index]);
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
        let path_block_group_edge_ids = path_edge_ids
            .iter()
            .map(|edge_id| **block_group_edge_ids_by_edge_id.get(edge_id).unwrap())
            .collect::<Vec<_>>();
        Path::create(conn, path_name, block_group.id, &path_block_group_edge_ids);
    }

    for input_walk in &gfa.walk {
        let path_name = &input_walk.sample_id;
        let mut source_node_id = PATH_START_NODE_ID;
        let mut source_coordinate = 0;
        let mut source_strand = Strand::Forward;
        let mut path_edge_ids = vec![];
        for (index, segment_id) in input_walk.segments.iter().enumerate() {
            let target = sequences_by_segment_id.get(segment_id).unwrap();
            let target_node_id = *node_ids_by_segment_id.get(segment_id).unwrap();
            let target_strand = bool_to_strand(input_walk.strands[index]);
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
        let path_block_group_edge_ids = path_edge_ids
            .iter()
            .map(|edge_id| **block_group_edge_ids_by_edge_id.get(edge_id).unwrap())
            .collect::<Vec<_>>();
        Path::create(conn, path_name, block_group.id, &path_block_group_edge_ids);
    }
    bar.finish();
}

fn edge_data_from_fields(
    source_node_id: i64,
    source_coordinate: i64,
    source_strand: Strand,
    target_node_id: i64,
    target_strand: Strand,
) -> EdgeData {
    EdgeData {
        source_node_id,
        source_coordinate,
        source_strand,
        target_node_id,
        target_coordinate: 0,
        target_strand,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::traits::*;
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
        import_gfa(&gfa_path, &collection_name, None, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let path = Path::query(
            conn,
            "select * from paths where block_group_id = ?1 AND name = ?2",
            rusqlite::params!(
                SQLValue::from(block_group_id),
                SQLValue::from("m123".to_string()),
            ),
        )[0]
        .clone();

        let result = path.sequence(conn);
        assert_eq!(result, "ATCGATCGATCGATCGATCGGGAACACACAGAGA");

        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len() as i64;
        assert_eq!(node_count, 6);
    }

    #[test]
    fn test_creates_sample() {
        setup_gen_dir();
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/simple.gfa");
        let collection_name = "test".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, "new-sample", conn);
        assert_eq!(
            Sample::get_by_name(conn, "new-sample").unwrap().name,
            "new-sample"
        );
    }

    #[test]
    fn test_import_no_path_gfa() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/no_path.gfa");
        let collection_name = "no path".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, None, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let all_sequences = BlockGroup::get_all_sequences(conn, block_group_id, false);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec!["AAAATTTTGGGGCCCC".to_string()])
        );

        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len() as i64;
        assert_eq!(node_count, 6);
    }

    #[test]
    fn test_import_gfa_with_walk() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/walk.gfa");
        let collection_name = "walk".to_string();
        let conn = &mut get_connection(None);
        import_gfa(&gfa_path, &collection_name, None, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let path = Path::query(
            conn,
            "select * from paths where block_group_id = ?1 AND name = ?2",
            rusqlite::params!(
                SQLValue::from(block_group_id),
                SQLValue::from("291344".to_string()),
            ),
        )[0]
        .clone();

        let result = path.sequence(conn);
        assert_eq!(result, "ACCTACAAATTCAAAC");

        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len() as i64;
        assert_eq!(node_count, 6);
    }

    #[test]
    fn test_import_gfa_with_reverse_strand_edges() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/reverse_strand.gfa");
        let collection_name = "test".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, None, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let path = Path::query(
            conn,
            "select * from paths where block_group_id = ?1 AND name = ?2",
            rusqlite::params!(
                SQLValue::from(block_group_id),
                SQLValue::from("124".to_string()),
            ),
        )[0]
        .clone();

        let result = path.sequence(conn);
        assert_eq!(result, "TATGCCAGCTGCGAATA");

        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len() as i64;
        assert_eq!(node_count, 6);
    }

    #[test]
    fn test_import_anderson_promoters() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/anderson_promoters.gfa");
        let collection_name = "anderson promoters".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, None, conn);

        let paths = Path::query_for_collection(conn, &collection_name);
        assert_eq!(paths.len(), 20);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let path = Path::query(
            conn,
            "select * from paths where block_group_id = ?1 AND name = ?2",
            rusqlite::params!(
                SQLValue::from(block_group_id),
                SQLValue::from("BBa_J23100".to_string()),
            ),
        )[0]
        .clone();

        let result = path.sequence(conn);
        let big_part = "TGCTAGCTACTAGTGAAAGAGGAGAAATACTAGATGGCTTCCTCCGAAGACGTTATCAAAGAGTTCATGCGTTTCAAAGTTCGTATGGAAGGTTCCGTTAACGGTCACGAGTTCGAAATCGAAGGTGAAGGTGAAGGTCGTCCGTACGAAGGTACCCAGACCGCTAAACTGAAAGTTACCAAAGGTGGTCCGCTGCCGTTCGCTTGGGACATCCTGTCCCCGCAGTTCCAGTACGGTTCCAAAGCTTACGTTAAACACCCGGCTGACATCCCGGACTACCTGAAACTGTCCTTCCCGGAAGGTTTCAAATGGGAACGTGTTATGAACTTCGAAGACGGTGGTGTTGTTACCGTTACCCAGGACTCCTCCCTGCAAGACGGTGAGTTCATCTACAAAGTTAAACTGCGTGGTACCAACTTCCCGTCCGACGGTCCGGTTATGCAGAAAAAAACCATGGGTTGGGAAGCTTCCACCGAACGTATGTACCCGGAAGACGGTGCTCTGAAAGGTGAAATCAAAATGCGTCTGAAACTGAAAGACGGTGGTCACTACGACGCTGAAGTTAAAACCACCTACATGGCTAAAAAACCGGTTCAGCTGCCGGGTGCTTACAAAACCGACATCAAACTGGACATCACCTCCCACAACGAAGACTACACCATCGTTGAACAGTACGAACGTGCTGAAGGTCGTCACTCCACCGGTGCTTAATAACGCTGATAGTGCTAGTGTAGATCGCTACTAGAGCCAGGCATCAAATAAAACGAAAGGCTCAGTCGAAAGACTGGGCCTTTCGTTTTATCTGTTGTTTGTCGGTGAACGCTCTCTACTAGAGTCACACTGGCTCACCTTCGGGTGGGCCTTTCTGCGTTTATATACTAGAAGCGGCCGCTGCAGGCTTCCTCGCTCACTGACTCGCTGCGCTCGGTCGTTCGGCTGCGGCGAGCGGTATCAGCTCACTCAAAGGCGGTAATACGGTTATCCACAGAATCAGGGGATAACGCAGGAAAGAACATGTGAGCAAAAGGCCAGCAAAAGGCCAGGAACCGTAAAAAGGCCGCGTTGCTGGCGTTTTTCCATAGGCTCCGCCCCCCTGACGAGCATCACAAAAATCGACGCTCAAGTCAGAGGTGGCGAAACCCGACAGGACTATAAAGATACCAGGCGTTTCCCCCTGGAAGCTCCCTCGTGCGCTCTCCTGTTCCGACCCTGCCGCTTACCGGATACCTGTCCGCCTTTCTCCCTTCGGGAAGCGTGGCGCTTTCTCATAGCTCACGCTGTAGGTATCTCAGTTCGGTGTAGGTCGTTCGCTCCAAGCTGGGCTGTGTGCACGAACCCCCCGTTCAGCCCGACCGCTGCGCCTTATCCGGTAACTATCGTCTTGAGTCCAACCCGGTAAGACACGACTTATCGCCACTGGCAGCAGCCACTGGTAACAGGATTAGCAGAGCGAGGTATGTAGGCGGTGCTACAGAGTTCTTGAAGTGGTGGCCTAACTACGGCTACACTAGAAGGACAGTATTTGGTATCTGCGCTCTGCTGAAGCCAGTTACCTTCGGAAAAAGAGTTGGTAGCTCTTGATCCGGCAAACAAACCACCGCTGGTAGCGGTGGTTTTTTTGTTTGCAAGCAGCAGATTACGCGCAGAAAAAAAGGATCTCAAGAAGATCCTTTGATCTTTTCTACGGGGTCTGACGCTCAGTGGAACGAAAACTCACGTTAAGGGATTTTGGTCATGAGATTATCAAAAAGGATCTTCACCTAGATCCTTTTAAATTAAAAATGAAGTTTTAAATCAATCTAAAGTATATATGAGTAAACTTGGTCTGACAGTTACCAATGCTTAATCAGTGAGGCACCTATCTCAGCGATCTGTCTATTTCGTTCATCCATAGTTGCCTGACTCCCCGTCGTGTAGATAACTACGATACGGGAGGGCTTACCATCTGGCCCCAGTGCTGCAATGATACCGCGAGACCCACGCTCACCGGCTCCAGATTTATCAGCAATAAACCAGCCAGCCGGAAGGGCCGAGCGCAGAAGTGGTCCTGCAACTTTATCCGCCTCCATCCAGTCTATTAATTGTTGCCGGGAAGCTAGAGTAAGTAGTTCGCCAGTTAATAGTTTGCGCAACGTTGTTGCCATTGCTACAGGCATCGTGGTGTCACGCTCGTCGTTTGGTATGGCTTCATTCAGCTCCGGTTCCCAACGATCAAGGCGAGTTACATGATCCCCCATGTTGTGCAAAAAAGCGGTTAGCTCCTTCGGTCCTCCGATCGTTGTCAGAAGTAAGTTGGCCGCAGTGTTATCACTCATGGTTATGGCAGCACTGCATAATTCTCTTACTGTCATGCCATCCGTAAGATGCTTTTCTGTGACTGGTGAGTACTCAACCAAGTCATTCTGAGAATAGTGTATGCGGCGACCGAGTTGCTCTTGCCCGGCGTCAATACGGGATAATACCGCGCCACATAGCAGAACTTTAAAAGTGCTCATCATTGGAAAACGTTCTTCGGGGCGAAAACTCTCAAGGATCTTACCGCTGTTGAGATCCAGTTCGATGTAACCCACTCGTGCACCCAACTGATCTTCAGCATCTTTTACTTTCACCAGCGTTTCTGGGTGAGCAAAAACAGGAAGGCAAAATGCCGCAAAAAAGGGAATAAGGGCGACACGGAAATGTTGAATACTCATACTCTTCCTTTTTCAATATTATTGAAGCATTTATCAGGGTTATTGTCTCATGAGCGGATACATATTTGAATGTATTTAGAAAAATAAACAAATAGGGGTTCCGCGCACATTTCCCCGAAAAGTGCCACCTGACGTCTAAGAAACCATTATTATCATGACATTAACCTATAAAAATAGGCGTATCACGAGGCAGAATTTCAGATAAAAAAAATCCTTAGCTTTCGCTAAGGATGATTTCTGGAATTCGCGGCCGCATCTAGAG";
        let expected_sequence_parts = vec![
            "T",
            "T",
            "G",
            "A",
            "C",
            "G",
            "GCTAGCTCAG",
            "T",
            "CCT",
            "A",
            "GG",
            "T",
            "A",
            "C",
            "A",
            "G",
            big_part,
        ];

        let expected_sequence = expected_sequence_parts.join("");
        assert_eq!(result, expected_sequence);

        let part1 = "T";
        let part3 = "T";
        let part4_5 = vec!["G", "T"];
        let part6 = "A";
        let part7_8 = vec!["C", "T"];
        let part9_10 = vec!["A", "G"];
        let part11 = "GCTAGCTCAG";
        let part12_13 = vec!["T", "C"];
        let part14 = "CCT";
        let part15_16 = vec!["A", "T"];
        let part17 = "GG";
        let part18_19 = vec!["T", "G"];
        let part20 = "A";
        let part21_22 = vec!["T", "C"];
        let part23_24 = vec!["A", "T"];
        let part25_26 = vec!["A", "G"];

        let mut expected_sequences = HashSet::new();
        for part_a in &part4_5 {
            for part_b in &part7_8 {
                for part_c in &part9_10 {
                    for part_d in &part12_13 {
                        for part_e in &part15_16 {
                            for part_f in &part18_19 {
                                for part_g in &part21_22 {
                                    for part_h in &part23_24 {
                                        for part_i in &part25_26 {
                                            let expected_sequence_parts1 = vec![
                                                part1, part3, part_a, part6, part_b, part_c,
                                                part11, part_d, part14, part_e, part17, part_f,
                                                part20, part_g, part_h, part_i, big_part,
                                            ];
                                            let temp_sequence1 = expected_sequence_parts1.join("");
                                            let expected_sequence_parts2 = vec![
                                                part3, part_a, part6, part_b, part_c, part11,
                                                part_d, part14, part_e, part17, part_f, part20,
                                                part_g, part_h, part_i, big_part,
                                            ];
                                            let temp_sequence2 = expected_sequence_parts2.join("");
                                            expected_sequences.insert(temp_sequence1);
                                            expected_sequences.insert(temp_sequence2);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        let all_sequences = BlockGroup::get_all_sequences(conn, block_group_id, false);
        assert_eq!(all_sequences.len(), 1024);
        assert_eq!(all_sequences, expected_sequences);

        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len() as i64;
        assert_eq!(node_count, 28);
    }

    #[test]
    fn test_import_aa_gfa() {
        setup_gen_dir();
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/aa.gfa");
        let collection_name = "test".to_string();
        let conn = &get_connection(None);
        import_gfa(&gfa_path, &collection_name, None, conn);

        let block_group_id = BlockGroup::get_id(conn, &collection_name, None, "");
        let path = Path::query(
            conn,
            "select * from paths where block_group_id = ?1 AND name = ?2",
            rusqlite::params!(
                SQLValue::from(block_group_id),
                SQLValue::from("124".to_string()),
            ),
        )[0]
        .clone();

        let result = path.sequence(conn);
        assert_eq!(result, "AA");

        let all_sequences = BlockGroup::get_all_sequences(conn, block_group_id, false);
        assert_eq!(all_sequences, HashSet::from_iter(vec!["AA".to_string()]));

        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len() as i64;
        assert_eq!(node_count, 4);
    }
}
