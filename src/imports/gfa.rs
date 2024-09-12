use gfa_reader::Gfa;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};

use crate::models::{
    self,
    block_group::BlockGroup,
    block_group_edge::BlockGroupEdge,
    edge::{Edge, EdgeData},
    path::Path,
    sequence::Sequence,
    strand::Strand,
};

fn import_gfa(gfa_path: &str, collection_name: &str, conn: &Connection) {
    models::Collection::create(conn, collection_name);
    let block_group = BlockGroup::create(conn, collection_name, None, "");
    let gfa: Gfa<u64, (), ()> = Gfa::parse_gfa_file(gfa_path);
    let mut sequences_by_segment_id: HashMap<u64, Sequence> = HashMap::new();

    for segment in &gfa.segments {
        let input_sequence = segment.sequence.get_string(&gfa.sequence);
        let sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence(input_sequence)
            .save(conn);
        sequences_by_segment_id.insert(segment.id, sequence);
    }

    let mut edges = HashSet::new();
    for link in &gfa.links {
        let source = sequences_by_segment_id.get(&link.from).unwrap();
        let target = sequences_by_segment_id.get(&link.to).unwrap();
        edges.insert(edge_data_from_fields(
            &source.hash,
            source.length,
            &target.hash,
        ));
    }

    for input_path in &gfa.paths {
        let mut source_hash = Sequence::PATH_START_HASH;
        let mut source_coordinate = 0;
        for segment_id in input_path.nodes.iter() {
            let target = sequences_by_segment_id.get(segment_id).unwrap();
            edges.insert(edge_data_from_fields(
                source_hash,
                source_coordinate,
                &target.hash,
            ));
            source_hash = &target.hash;
            source_coordinate = target.length;
        }
        edges.insert(edge_data_from_fields(
            source_hash,
            source_coordinate,
            Sequence::PATH_END_HASH,
        ));
    }

    for input_walk in &gfa.walk {
        let mut source_hash = Sequence::PATH_START_HASH;
        let mut source_coordinate = 0;
        for segment_id in input_walk.walk_id.iter() {
            let target = sequences_by_segment_id.get(segment_id).unwrap();
            edges.insert(edge_data_from_fields(
                source_hash,
                source_coordinate,
                &target.hash,
            ));
            source_hash = &target.hash;
            source_coordinate = target.length;
        }
        edges.insert(edge_data_from_fields(
            source_hash,
            source_coordinate,
            Sequence::PATH_END_HASH,
        ));
    }

    let edge_ids = Edge::bulk_create(conn, edges.into_iter().collect::<Vec<EdgeData>>());
    BlockGroupEdge::bulk_create(conn, block_group.id, &edge_ids);

    let saved_edges = Edge::bulk_load(conn, &edge_ids);
    let mut edge_ids_by_data = HashMap::new();
    for edge in saved_edges {
        let key =
            edge_data_from_fields(&edge.source_hash, edge.source_coordinate, &edge.target_hash);
        edge_ids_by_data.insert(key, edge.id);
    }

    for input_path in &gfa.paths {
        let path_name = &input_path.name;
        let mut source_hash = Sequence::PATH_START_HASH;
        let mut source_coordinate = 0;
        let mut path_edge_ids = vec![];
        for segment_id in input_path.nodes.iter() {
            let target = sequences_by_segment_id.get(segment_id).unwrap();
            let key = edge_data_from_fields(source_hash, source_coordinate, &target.hash);
            let edge_id = *edge_ids_by_data.get(&key).unwrap();
            path_edge_ids.push(edge_id);
            source_hash = &target.hash;
            source_coordinate = target.length;
        }
        let key = edge_data_from_fields(source_hash, source_coordinate, Sequence::PATH_END_HASH);
        let edge_id = *edge_ids_by_data.get(&key).unwrap();
        path_edge_ids.push(edge_id);
        Path::create(conn, path_name, block_group.id, &path_edge_ids);
    }

    for input_walk in &gfa.walk {
        let path_name = &input_walk.sample_id;
        let mut source_hash = Sequence::PATH_START_HASH;
        let mut source_coordinate = 0;
        let mut path_edge_ids = vec![];
        for segment_id in input_walk.walk_id.iter() {
            let target = sequences_by_segment_id.get(segment_id).unwrap();
            let key = edge_data_from_fields(source_hash, source_coordinate, &target.hash);
            let edge_id = *edge_ids_by_data.get(&key).unwrap();
            path_edge_ids.push(edge_id);
            source_hash = &target.hash;
            source_coordinate = target.length;
        }
        let key = edge_data_from_fields(source_hash, source_coordinate, Sequence::PATH_END_HASH);
        let edge_id = *edge_ids_by_data.get(&key).unwrap();
        path_edge_ids.push(edge_id);
        Path::create(conn, path_name, block_group.id, &path_edge_ids);
    }
}

fn edge_data_from_fields(source_hash: &str, source_coordinate: i32, target_hash: &str) -> EdgeData {
    EdgeData {
        source_hash: source_hash.to_string(),
        source_coordinate,
        source_strand: Strand::Forward,
        target_hash: target_hash.to_string(),
        target_coordinate: 0,
        target_strand: Strand::Forward,
        chromosome_index: 0,
        phased: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::get_connection;
    use rusqlite::{types::Value as SQLValue, Connection};
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_import_simple_gfa() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/simple.gfa");
        let collection_name = "test".to_string();
        let conn = &get_connection(None);
        import_gfa(gfa_path.to_str().unwrap(), &collection_name, conn);

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
    fn test_import_gfa_with_walk() {
        let mut gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gfa_path.push("fixtures/walk.gfa");
        let collection_name = "walk".to_string();
        let conn = &mut get_connection(None);
        import_gfa(gfa_path.to_str().unwrap(), &collection_name, conn);

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
}
