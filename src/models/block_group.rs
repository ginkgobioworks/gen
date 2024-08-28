use itertools::Itertools;
use petgraph::graphmap::DiGraphMap;
use petgraph::Direction;
use rusqlite::{types::Value, Connection};
use std::collections::{HashMap, HashSet};

use crate::graph::all_simple_paths;
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::edge::{Edge, EdgeData};
use crate::models::path::{NewBlock, Path};
use crate::models::path_edge::PathEdge;
use crate::models::sequence::{NewSequence, Sequence};

#[derive(Debug)]
pub struct BlockGroup {
    pub id: i32,
    pub collection_name: String,
    pub sample_name: Option<String>,
    pub name: String,
}

#[derive(Clone)]
pub struct GroupBlock {
    pub id: i32,
    pub sequence_hash: String,
    pub sequence: String,
    pub start: i32,
    pub end: i32,
}

#[derive(Eq, Hash, PartialEq)]
pub struct BlockKey {
    pub sequence_hash: String,
    pub coordinate: i32,
}

impl BlockGroup {
    pub fn create(
        conn: &Connection,
        collection_name: &str,
        sample_name: Option<&str>,
        name: &str,
    ) -> BlockGroup {
        let query = "INSERT INTO block_group (collection_name, sample_name, name) VALUES (?1, ?2, ?3) RETURNING *";
        let mut stmt = conn.prepare(query).unwrap();
        match stmt.query_row((collection_name, sample_name, name), |row| {
            Ok(BlockGroup {
                id: row.get(0)?,
                collection_name: row.get(1)?,
                sample_name: row.get(2)?,
                name: row.get(3)?,
            })
        }) {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    BlockGroup {
                        id: conn
                            .query_row(
                                "select id from block_group where collection_name = ?1 and sample_name is null and name = ?2",
                                (collection_name, name),
                                |row| row.get(0),
                            )
                            .unwrap(),
                        collection_name: collection_name.to_string(),
                        sample_name: sample_name.map(|s| s.to_string()),
                        name: name.to_string()
                    }
                } else {
                    panic!("something bad happened querying the database")
                }
            }
            Err(_) => {
                panic!("something bad happened querying the database")
            }
        }
    }

    pub fn clone(conn: &mut Connection, source_block_group_id: i32, target_block_group_id: i32) {
        let existing_paths = Path::get_paths(
            conn,
            "SELECT * from path where block_group_id = ?1",
            vec![Value::from(source_block_group_id)],
        );

        for path in existing_paths {
            let edge_ids = PathEdge::edges_for(conn, path.id)
                .into_iter()
                .map(|edge| edge.id)
                .collect();
            Path::create(conn, &path.name, target_block_group_id, edge_ids);
        }
    }

    pub fn get_or_create_sample_block_group(
        conn: &mut Connection,
        collection_name: &String,
        sample_name: &String,
        group_name: &String,
    ) -> i32 {
        let mut bg_id : i32 = match conn.query_row(
            "select id from block_group where collection_name = ?1 AND sample_name = ?2 AND name = ?3",
            (collection_name, sample_name, group_name),
            |row| row.get(0),
        ) {
            Ok(res) => res,
            Err(rusqlite::Error::QueryReturnedNoRows) => 0,
            Err(_e) => {
                panic!("Error querying the database: {_e}");
            }
        };
        if bg_id != 0 {
            return bg_id;
        } else {
            // use the base reference group if it exists
            bg_id = match conn.query_row(
            "select id from block_group where collection_name = ?1 AND sample_name IS null AND name = ?2",
            (collection_name, group_name),
            |row| row.get(0),
            ) {
                Ok(res) => res,
                Err(rusqlite::Error::QueryReturnedNoRows) => panic!("No base path exists"),
                Err(_e) => {
                    panic!("something bad happened querying the database")
                }
            }
        }
        let new_bg_id = BlockGroup::create(conn, collection_name, Some(sample_name), group_name);

        // clone parent blocks/edges/path
        BlockGroup::clone(conn, bg_id, new_bg_id.id);

        new_bg_id.id
    }

    pub fn blocks_from_edges(conn: &Connection, edges: Vec<Edge>) -> Vec<GroupBlock> {
        let mut sequence_hashes = HashSet::new();
        for edge in &edges {
            if edge.source_hash != Edge::PATH_START_HASH {
                sequence_hashes.insert(edge.source_hash.clone());
            }
            if edge.target_hash != Edge::PATH_END_HASH {
                sequence_hashes.insert(edge.target_hash.clone());
            }
        }

        let mut boundary_edges_by_hash = HashMap::<String, Vec<Edge>>::new();
        for edge in edges {
            if (edge.source_hash == edge.target_hash)
                && (edge.target_coordinate == edge.source_coordinate)
            {
                boundary_edges_by_hash
                    .entry(edge.source_hash.clone())
                    .and_modify(|current_edges| current_edges.push(edge.clone()))
                    .or_insert_with(|| vec![edge.clone()]);
            }
        }

        let sequences_by_hash =
            Sequence::sequences_by_hash(conn, sequence_hashes.into_iter().collect::<Vec<String>>());
        let mut blocks = vec![];

        let mut block_index = 0;
        for (hash, sequence) in sequences_by_hash.into_iter() {
            let sequence_edges = boundary_edges_by_hash.get(&hash);
            if sequence_edges.is_some() {
                let sorted_sequence_edges: Vec<Edge> = sequence_edges
                    .unwrap()
                    .iter()
                    .sorted_by(|edge1, edge2| {
                        Ord::cmp(&edge1.source_coordinate, &edge2.source_coordinate)
                    })
                    .cloned()
                    .collect();
                let first_edge = sorted_sequence_edges[0].clone();
                let start = 0;
                let end = first_edge.source_coordinate;
                let block_sequence = sequence.get_sequence(start, end).to_string();
                let first_block = GroupBlock {
                    id: block_index,
                    sequence_hash: hash.clone(),
                    sequence: block_sequence,
                    start,
                    end,
                };
                blocks.push(first_block);
                block_index += 1;
                for (into, out_of) in sorted_sequence_edges.clone().into_iter().tuple_windows() {
                    let start = into.target_coordinate;
                    let end = out_of.source_coordinate;
                    let block_sequence = sequence.get_sequence(start, end).to_string();
                    let block = GroupBlock {
                        id: block_index,
                        sequence_hash: hash.clone(),
                        sequence: block_sequence,
                        start,
                        end,
                    };
                    blocks.push(block);
                    block_index += 1;
                }
                let last_edge = &sorted_sequence_edges[sorted_sequence_edges.len() - 1];
                let start = last_edge.target_coordinate;
                let end = sequence.length;
                let block_sequence = sequence.get_sequence(start, end).to_string();
                let last_block = GroupBlock {
                    id: block_index,
                    sequence_hash: hash.clone(),
                    sequence: block_sequence,
                    start,
                    end,
                };
                blocks.push(last_block);
                block_index += 1;
            } else {
                blocks.push(GroupBlock {
                    id: block_index,
                    sequence_hash: hash.clone(),
                    sequence: sequence.get_sequence(None, None),
                    start: 0,
                    end: sequence.length,
                });
                block_index += 1;
            }
        }
        blocks
    }

    pub fn get_all_sequences(conn: &Connection, block_group_id: i32) -> HashSet<String> {
        let edges = BlockGroupEdge::edges_for_block_group(conn, block_group_id);
        let blocks = BlockGroup::blocks_from_edges(conn, edges.clone());

        let blocks_by_start = blocks
            .clone()
            .into_iter()
            .map(|block| {
                (
                    BlockKey {
                        sequence_hash: block.sequence_hash,
                        coordinate: block.start,
                    },
                    block.id,
                )
            })
            .collect::<HashMap<BlockKey, i32>>();
        let blocks_by_end = blocks
            .clone()
            .into_iter()
            .map(|block| {
                (
                    BlockKey {
                        sequence_hash: block.sequence_hash,
                        coordinate: block.end,
                    },
                    block.id,
                )
            })
            .collect::<HashMap<BlockKey, i32>>();
        let blocks_by_id = blocks
            .clone()
            .into_iter()
            .map(|block| (block.id, block))
            .collect::<HashMap<i32, GroupBlock>>();

        let mut graph: DiGraphMap<i32, ()> = DiGraphMap::new();
        for block in blocks {
            graph.add_node(block.id);
        }
        for edge in edges {
            let source_key = BlockKey {
                sequence_hash: edge.source_hash,
                coordinate: edge.source_coordinate,
            };
            let source_id = blocks_by_end.get(&source_key);
            let target_key = BlockKey {
                sequence_hash: edge.target_hash,
                coordinate: edge.target_coordinate,
            };
            let target_id = blocks_by_start.get(&target_key);
            if let Some(source_id_value) = source_id {
                if let Some(target_id_value) = target_id {
                    graph.add_edge(*source_id_value, *target_id_value, ());
                }
            }
        }

        let mut start_nodes = vec![];
        let mut end_nodes = vec![];
        for node in graph.nodes() {
            let has_incoming = graph.neighbors_directed(node, Direction::Incoming).next();
            let has_outgoing = graph.neighbors_directed(node, Direction::Outgoing).next();
            if has_incoming.is_none() {
                start_nodes.push(node);
            }
            if has_outgoing.is_none() {
                end_nodes.push(node);
            }
        }
        let mut sequences = HashSet::<String>::new();

        for start_node in start_nodes {
            for end_node in &end_nodes {
                // TODO: maybe make all_simple_paths return a single path id where start == end
                if start_node == *end_node {
                    let block = blocks_by_id.get(&start_node).unwrap();
                    sequences.insert(block.sequence.clone());
                } else {
                    for path in all_simple_paths(&graph, start_node, *end_node) {
                        let mut current_sequence = "".to_string();
                        for node in path {
                            let block = blocks_by_id.get(&node).unwrap();
                            let block_sequence = block.sequence.clone();
                            current_sequence.push_str(&block_sequence);
                        }
                        sequences.insert(current_sequence);
                    }
                }
            }
        }

        sequences
    }

    #[allow(clippy::ptr_arg)]
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::needless_late_init)]
    pub fn insert_change(
        conn: &mut Connection,
        block_group_id: i32,
        path: &Path,
        start: i32,
        end: i32,
        new_block: &NewBlock,
        chromosome_index: i32,
        phased: i32,
    ) {
        let tree = Path::intervaltree_for(conn, path);

        let start_blocks: Vec<NewBlock> =
            tree.query_point(start).map(|x| x.value.clone()).collect();
        assert_eq!(start_blocks.len(), 1);
        // NOTE: This may not be used but needs to be initialized here instead of inside the if
        // statement that uses it, so that the borrow checker is happy
        let previous_start_blocks: Vec<NewBlock> = tree
            .query_point(start - 1)
            .map(|x| x.value.clone())
            .collect();
        assert_eq!(previous_start_blocks.len(), 1);
        let start_block;
        if start_blocks[0].path_start == start {
            // First part of this block will be replaced/deleted, need to get previous block to add
            // edge including it
            start_block = &previous_start_blocks[0];
        } else {
            start_block = &start_blocks[0];
        }

        let end_blocks: Vec<NewBlock> = tree.query_point(end).map(|x| x.value.clone()).collect();
        assert_eq!(end_blocks.len(), 1);
        let end_block = &end_blocks[0];

        let mut new_edges = vec![];

        if new_block.sequence_start == new_block.sequence_end {
            // Deletion
            let new_edge = EdgeData {
                source_hash: start_block.sequence.hash.clone(),
                source_coordinate: start - start_block.path_start + start_block.sequence_start,
                source_strand: "+".to_string(),
                target_hash: end_block.sequence.hash.clone(),
                target_coordinate: end - end_block.path_start + end_block.sequence_start,
                target_strand: "+".to_string(),
                chromosome_index,
                phased,
            };
            new_edges.push(new_edge);
        } else {
            // Insertion/replacement
            let new_start_edge = EdgeData {
                source_hash: start_block.sequence.hash.clone(),
                source_coordinate: start - start_block.path_start + start_block.sequence_start,
                source_strand: "+".to_string(),
                target_hash: new_block.sequence.hash.clone(),
                target_coordinate: new_block.sequence_start,
                target_strand: "+".to_string(),
                chromosome_index,
                phased,
            };
            let new_end_edge = EdgeData {
                source_hash: new_block.sequence.hash.clone(),
                source_coordinate: new_block.sequence_end,
                source_strand: "+".to_string(),
                target_hash: end_block.sequence.hash.clone(),
                target_coordinate: end - end_block.path_start + end_block.sequence_start,
                target_strand: "+".to_string(),
                chromosome_index,
                phased,
            };
            new_edges.push(new_start_edge);
            new_edges.push(new_end_edge);
        }

        // NOTE: Add edges marking the existing part of the sequence that is being substituted out,
        // so we can retrieve it as one node of the overall graph
        if start < start_block.path_end {
            let split_coordinate = start - start_block.path_start + start_block.sequence_start;
            let new_split_start_edge = EdgeData {
                source_hash: start_block.sequence.hash.clone(),
                source_coordinate: split_coordinate,
                source_strand: "+".to_string(),
                target_hash: start_block.sequence.hash.clone(),
                target_coordinate: split_coordinate,
                target_strand: "+".to_string(),
                chromosome_index,
                phased,
            };
            new_edges.push(new_split_start_edge);
        }

        if end > end_block.path_start {
            let split_coordinate = end - end_block.path_start + end_block.sequence_start;
            let new_split_end_edge = EdgeData {
                source_hash: end_block.sequence.hash.clone(),
                source_coordinate: split_coordinate,
                source_strand: "+".to_string(),
                target_hash: end_block.sequence.hash.clone(),
                target_coordinate: split_coordinate,
                target_strand: "+".to_string(),
                chromosome_index,
                phased,
            };

            new_edges.push(new_split_end_edge);
        }

        let edge_ids = Edge::bulk_create(conn, new_edges);
        BlockGroupEdge::bulk_create(conn, block_group_id, edge_ids);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_migrations;
    use crate::models::Collection;

    fn get_connection() -> Connection {
        let mut conn = Connection::open_in_memory()
            .unwrap_or_else(|_| panic!("Error opening in memory test db"));
        run_migrations(&mut conn);
        conn
    }

    fn setup_block_group(conn: &Connection) -> (i32, Path) {
        let a_seq_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAAAA")
            .save(conn);
        let t_seq_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTTTT")
            .save(conn);
        let c_seq_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCCCC")
            .save(conn);
        let g_seq_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGGGG")
            .save(conn);
        let _collection = Collection::create(conn, "test");
        let block_group = BlockGroup::create(conn, "test", None, "hg19");
        let edge0 = Edge::create(
            conn,
            Edge::PATH_START_HASH.to_string(),
            0,
            "+".to_string(),
            a_seq_hash.clone(),
            0,
            "+".to_string(),
            0,
            0,
        );
        let edge1 = Edge::create(
            conn,
            a_seq_hash,
            10,
            "+".to_string(),
            t_seq_hash.clone(),
            0,
            "+".to_string(),
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            t_seq_hash,
            10,
            "+".to_string(),
            c_seq_hash.clone(),
            0,
            "+".to_string(),
            0,
            0,
        );
        let edge3 = Edge::create(
            conn,
            c_seq_hash,
            10,
            "+".to_string(),
            g_seq_hash.clone(),
            0,
            "+".to_string(),
            0,
            0,
        );
        let edge4 = Edge::create(
            conn,
            g_seq_hash,
            10,
            "+".to_string(),
            Edge::PATH_END_HASH.to_string(),
            0,
            "+".to_string(),
            0,
            0,
        );
        BlockGroupEdge::bulk_create(
            conn,
            block_group.id,
            vec![edge0.id, edge1.id, edge2.id, edge3.id, edge4.id],
        );
        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            vec![edge0.id, edge1.id, edge2.id, edge3.id, edge4.id],
        );
        (block_group.id, path)
    }

    #[test]
    fn insert_and_deletion_new_get_all() {
        let mut conn = get_connection();
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_sequence = Sequence::sequence_from_hash(&conn, &insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: "+".to_string(),
        };
        BlockGroup::insert_change(&mut conn, block_group_id, &path, 7, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );

        let deletion_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("")
            .save(&conn);
        let deletion_sequence =
            Sequence::sequence_from_hash(&conn, &deletion_sequence_hash).unwrap();
        let deletion = NewBlock {
            id: 0,
            sequence: deletion_sequence.clone(),
            block_sequence: deletion_sequence.get_sequence(None, None),
            sequence_start: 0,
            sequence_end: 0,
            path_start: 19,
            path_end: 31,
            strand: "+".to_string(),
        };

        // take out an entire block.
        BlockGroup::insert_change(&mut conn, block_group_id, &path, 19, 31, &deletion, 1, 0);
        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTTGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTGGGGGGGGG".to_string(),
            ])
        )
    }

    #[test]
    fn simple_insert_new_get_all() {
        let mut conn = get_connection();
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_sequence = Sequence::sequence_from_hash(&conn, &insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: "+".to_string(),
        };
        BlockGroup::insert_change(&mut conn, block_group_id, &path, 7, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_on_block_boundary_middle_new() {
        let mut conn = get_connection();
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_sequence = Sequence::sequence_from_hash(&conn, &insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 15,
            path_end: 15,
            strand: "+".to_string(),
        };
        BlockGroup::insert_change(&mut conn, block_group_id, &path, 15, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTNNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_within_block_new() {
        let mut conn = get_connection();
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_sequence = Sequence::sequence_from_hash(&conn, &insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 12,
            path_end: 17,
            strand: "+".to_string(),
        };
        BlockGroup::insert_change(&mut conn, block_group_id, &path, 12, 17, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTNNNNTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_on_block_boundary_start_new() {
        let mut conn = get_connection();
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_sequence = Sequence::sequence_from_hash(&conn, &insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 10,
            path_end: 10,
            strand: "+".to_string(),
        };
        BlockGroup::insert_change(&mut conn, block_group_id, &path, 10, 10, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAANNNNTTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_on_block_boundary_end_new() {
        let mut conn = get_connection();
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_sequence = Sequence::sequence_from_hash(&conn, &insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 9,
            path_end: 9,
            strand: "+".to_string(),
        };
        BlockGroup::insert_change(&mut conn, block_group_id, &path, 9, 9, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAANNNNATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_across_entire_block_boundary_new() {
        let mut conn = get_connection();
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_sequence = Sequence::sequence_from_hash(&conn, &insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 10,
            path_end: 20,
            strand: "+".to_string(),
        };
        BlockGroup::insert_change(&mut conn, block_group_id, &path, 10, 20, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAANNNNCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_across_two_blocks_new() {
        let mut conn = get_connection();
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_sequence = Sequence::sequence_from_hash(&conn, &insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 15,
            path_end: 25,
            strand: "+".to_string(),
        };
        BlockGroup::insert_change(&mut conn, block_group_id, &path, 15, 25, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTNNNNCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_spanning_blocks_new() {
        let mut conn = get_connection();
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_sequence = Sequence::sequence_from_hash(&conn, &insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 5,
            path_end: 35,
            strand: "+".to_string(),
        };
        BlockGroup::insert_change(&mut conn, block_group_id, &path, 5, 35, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAANNNNGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn simple_deletion_new() {
        let mut conn = get_connection();
        let (block_group_id, path) = setup_block_group(&conn);
        let deletion_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("")
            .save(&conn);
        let deletion_sequence =
            Sequence::sequence_from_hash(&conn, &deletion_sequence_hash).unwrap();
        let deletion = NewBlock {
            id: 0,
            sequence: deletion_sequence.clone(),
            block_sequence: deletion_sequence.get_sequence(None, None),
            sequence_start: 0,
            sequence_end: 0,
            path_start: 19,
            path_end: 31,
            strand: "+".to_string(),
        };

        // take out an entire block.
        BlockGroup::insert_change(&mut conn, block_group_id, &path, 19, 31, &deletion, 1, 0);
        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTTGGGGGGGGG".to_string(),
            ])
        )
    }

    #[test]
    fn doesnt_apply_same_insert_twice_new() {
        let mut conn = get_connection();
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_sequence = Sequence::sequence_from_hash(&conn, &insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: "+".to_string(),
        };
        BlockGroup::insert_change(&mut conn, block_group_id, &path, 7, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );

        BlockGroup::insert_change(&mut conn, block_group_id, &path, 7, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }
}
