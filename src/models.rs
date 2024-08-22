use itertools::Itertools;
use petgraph::graphmap::DiGraphMap;
use petgraph::Direction;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fmt::*;

pub mod block;
pub mod block_group_edge;
pub mod edge;
pub mod new_edge;
pub mod path;
pub mod path_edge;
pub mod sequence;

use crate::graph::all_simple_paths;
use crate::models::block::Block;
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::edge::Edge;
use crate::models::new_edge::{EdgeData, NewEdge};
use crate::models::path::{NewBlock, Path, PathBlock};
use crate::models::path_edge::PathEdge;
use crate::models::sequence::Sequence;
use crate::{get_overlap, models};

#[derive(Debug)]
pub struct Collection {
    pub name: String,
}

impl Collection {
    pub fn exists(conn: &Connection, name: &str) -> bool {
        let mut stmt = conn
            .prepare("select name from collection where name = ?1")
            .unwrap();
        stmt.exists([name]).unwrap()
    }
    pub fn create(conn: &Connection, name: &str) -> Collection {
        let mut stmt = conn
            .prepare("INSERT INTO collection (name) VALUES (?1) RETURNING *")
            .unwrap();
        let mut rows = stmt
            .query_map((name,), |row| Ok(models::Collection { name: row.get(0)? }))
            .unwrap();
        rows.next().unwrap().unwrap()
    }

    pub fn bulk_create(conn: &Connection, names: &Vec<String>) -> Vec<Collection> {
        let placeholders = names.iter().map(|_| "(?)").collect::<Vec<_>>().join(", ");
        let q = format!(
            "INSERT INTO collection (name) VALUES {} RETURNING *",
            placeholders
        );
        let mut stmt = conn.prepare(&q).unwrap();
        let rows = stmt
            .query_map(params_from_iter(names), |row| {
                Ok(Collection { name: row.get(0)? })
            })
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }
}

#[derive(Debug)]
pub struct Sample {
    pub name: String,
}

impl Sample {
    pub fn create(conn: &Connection, name: &String) -> Sample {
        let mut stmt = conn
            .prepare("INSERT INTO sample (name) VALUES (?1)")
            .unwrap();
        match stmt.execute((name,)) {
            Ok(_) => Sample { name: name.clone() },
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    Sample { name: name.clone() }
                } else {
                    panic!("something bad happened querying the database")
                }
            }
            Err(_) => {
                panic!("something bad happened querying the database")
            }
        }
    }
}

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
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, sequence_hash, start, end, strand from block where block_group_id = ?1",
            )
            .unwrap();
        let mut block_map: HashMap<i32, i32> = HashMap::new();
        let mut it = stmt.query([source_block_group_id]).unwrap();
        let mut row = it.next().unwrap();
        while row.is_some() {
            let block = row.unwrap();
            let block_id: i32 = block.get(0).unwrap();
            let hash: String = block.get(1).unwrap();
            let start = block.get(2).unwrap();
            let end = block.get(3).unwrap();
            let strand: String = block.get(4).unwrap();
            let new_block = Block::create(conn, &hash, target_block_group_id, start, end, &strand);
            block_map.insert(block_id, new_block.id);
            row = it.next().unwrap();
        }

        // todo: figure out rusqlite's rarray
        let mut stmt = conn
            .prepare_cached("SELECT id, source_id, target_id, chromosome_index, phased from edges where source_id IN (?1) OR target_id in (?1)")
            .unwrap();
        let block_keys = block_map
            .keys()
            .map(|k| format!("{k}"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut it = stmt.query([block_keys]).unwrap();
        let mut row = it.next().unwrap();
        while row.is_some() {
            let edge = row.unwrap();
            let source_id: Option<i32> = edge.get(1).unwrap();
            let target_id: Option<i32> = edge.get(2).unwrap();
            let chrom_index = edge.get(3).unwrap();
            let phased = edge.get(4).unwrap();
            if target_id.is_some() && source_id.is_some() {
                let target_id = target_id.unwrap();
                let source_id = source_id.unwrap();
                Edge::create(
                    conn,
                    Some(*block_map.get(&source_id).unwrap_or(&source_id)),
                    Some(*block_map.get(&target_id).unwrap_or(&target_id)),
                    chrom_index,
                    phased,
                );
            } else if target_id.is_some() {
                let target_id = target_id.unwrap();
                Edge::create(
                    conn,
                    None,
                    Some(*block_map.get(&target_id).unwrap_or(&target_id)),
                    chrom_index,
                    phased,
                );
            } else if source_id.is_some() {
                let source_id = source_id.unwrap();
                Edge::create(
                    conn,
                    Some(*block_map.get(&source_id).unwrap_or(&source_id)),
                    None,
                    0,
                    0,
                );
            } else {
                panic!("no source and target specified.");
            }

            row = it.next().unwrap();
        }

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
            Path::new_create(conn, &path.name, target_block_group_id, edge_ids);
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

    pub fn get_all_sequences(conn: &Connection, block_group_id: i32) -> HashSet<String> {
        let mut block_map = HashMap::new();
        for block in Block::get_blocks(
            conn,
            "select * from block where block_group_id = ?1",
            vec![Value::from(block_group_id)],
        ) {
            block_map.insert(block.id, block);
        }
        let sequence_hashes = block_map
            .values()
            .map(|block| format!("\"{id}\"", id = block.sequence_hash))
            .collect::<Vec<_>>();
        let sequence_map = Sequence::sequences_by_hash(conn, sequence_hashes);
        let block_ids = block_map
            .keys()
            .map(|id| format!("{id}"))
            .collect::<Vec<_>>()
            .join(",");
        let edges = Edge::get_edges(conn, &format!("select * from edges where source_id in ({block_ids}) OR target_id in ({block_ids})"), vec![]);
        let mut graph: DiGraphMap<i32, ()> = DiGraphMap::new();
        for block_id in block_map.keys() {
            graph.add_node(*block_id);
        }
        for edge in edges {
            if edge.source_id.is_some() && edge.target_id.is_some() {
                graph.add_edge(edge.source_id.unwrap(), edge.target_id.unwrap(), ());
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
        let mut sequences = HashSet::new();

        for start_node in start_nodes {
            for end_node in &end_nodes {
                // TODO: maybe make all_simple_paths return a single path id where start == end
                if start_node == *end_node {
                    let block = block_map.get(&start_node).unwrap();
                    let block_sequence = sequence_map.get(&block.sequence_hash).unwrap();
                    sequences.insert(
                        block_sequence.sequence[(block.start as usize)..(block.end as usize)]
                            .to_string(),
                    );
                } else {
                    for path in all_simple_paths(&graph, start_node, *end_node) {
                        let mut current_sequence = "".to_string();
                        for node in path {
                            let block = block_map.get(&node).unwrap();
                            let block_sequence = sequence_map.get(&block.sequence_hash).unwrap();
                            current_sequence.push_str(
                                &block_sequence.sequence
                                    [(block.start as usize)..(block.end as usize)],
                            );
                        }
                        sequences.insert(current_sequence);
                    }
                }
            }
        }
        sequences
    }

    pub fn blocks_from_edges(conn: &Connection, edges: Vec<NewEdge>) -> Vec<GroupBlock> {
        let mut sequence_hashes = HashSet::new();
        for edge in &edges {
            if edge.source_hash != NewEdge::PATH_START_HASH {
                sequence_hashes.insert(edge.source_hash.clone());
            }
            if edge.target_hash != NewEdge::PATH_END_HASH {
                sequence_hashes.insert(edge.target_hash.clone());
            }
        }

        let mut boundary_edges_by_hash = HashMap::<String, Vec<NewEdge>>::new();
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

        let sequences_by_hash = Sequence::sequences_by_hash(
            conn,
            sequence_hashes
                .into_iter()
                .map(|hash| format!("\"{hash}\""))
                .collect(),
        );
        let mut blocks = vec![];

        let mut block_index = 0;
        for (hash, sequence) in sequences_by_hash.into_iter() {
            let sequence_edges = boundary_edges_by_hash.get(&hash);
            if sequence_edges.is_some() {
                let sorted_sequence_edges: Vec<NewEdge> = sequence_edges
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
                let block_sequence = sequence.sequence[start as usize..end as usize].to_string();
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
                    let block_sequence =
                        sequence.sequence[start as usize..end as usize].to_string();
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
                let end = sequence.sequence.len() as i32;
                let block_sequence = sequence.sequence[start as usize..end as usize].to_string();
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
                    sequence: sequence.sequence.clone(),
                    start: 0,
                    end: sequence.sequence.len() as i32,
                });
                block_index += 1;
            }
        }
        blocks
    }

    pub fn new_get_all_sequences(conn: &Connection, block_group_id: i32) -> HashSet<String> {
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
    pub fn insert_change(
        conn: &mut Connection,
        path_id: i32,
        start: i32,
        end: i32,
        new_block: &Block,
        chromosome_index: i32,
        phased: i32,
    ) {
        let new_block_id = new_block.id;
        let change = ChangeLog::new(
            path_id,
            start,
            end,
            new_block.sequence_hash.clone(),
            new_block.start,
            new_block.end,
            new_block.strand.clone(),
        );
        if ChangeLog::exists(conn, &change.hash) {
            return;
        }
        // todo:
        // 1. get blocks where start-> end overlap
        // 2. split old blocks at boundary points, make new block for left/right side
        // 3. make new block for sequence we are changing
        // 4. update edges
        // add support for deletion
        // cases to check:
        //  change that is the size of a block
        //  change that goes over multiple blocks
        //  change that hits just start/end boundary, e.g. block is 1,5 and change is 3,5 or 1,3.
        //  change that deletes block boundary
        // https://stackoverflow.com/questions/3269434/whats-the-most-efficient-way-to-test-if-two-ranges-overlap

        // check if we've already inserted this for edges connected
        // that means we have an edge with the chromosome index, that connects our start/end coordinates with the new block id

        let path = Path::get(conn, path_id);
        let graph = PathBlock::blocks_to_graph(conn, path.id);
        let query = format!("SELECT id, sequence_hash, block_group_id, start, end, strand from block where id in ({block_ids})", block_ids = graph.nodes().map(|k| format!("{k}")).collect::<Vec<_>>().join(","));
        let mut stmt = conn.prepare(&query).unwrap();
        let mut blocks: HashMap<i32, Block> = HashMap::new();
        let mut it = stmt.query([]).unwrap();
        let mut row = it.next().unwrap();
        while row.is_some() {
            let entry = row.unwrap();
            let block_id = entry.get(0).unwrap();
            blocks.insert(
                block_id,
                Block {
                    id: block_id,
                    sequence_hash: entry.get(1).unwrap(),
                    block_group_id: entry.get(2).unwrap(),
                    start: entry.get(3).unwrap(),
                    end: entry.get(4).unwrap(),
                    strand: entry.get(5).unwrap(),
                },
            );
            row = it.next().unwrap();
        }
        // TODO: probably don't need the graph, just get vector of source_ids.
        let mut path_start = 0;
        let mut path_end = 0;
        let mut new_edges = vec![];
        let mut previous_block: Option<&Block> = None;
        for block_id in &path.blocks {
            let block = blocks.get(block_id).unwrap();
            let block_length = block.end - block.start;
            path_end += block_length;

            let (contains_start, contains_end, overlap) =
                get_overlap(path_start, path_end, start, end);
            println!(
                "{path_start} {path_end} {start} {end} {contains_start} {contains_end} {overlap}"
            );

            if contains_start && contains_end {
                // our range is fully contained w/in the block
                //      |----block------|
                //        |----range---|
                let start_split_point = block.start + start - path_start;
                let end_split_point = block.start + end - path_start;
                let next_block = if start_split_point == block.start {
                    if let Some(pb) = previous_block {
                        new_edges.push((Some(pb.id), Some(new_block_id)));
                    }
                    block.clone()
                } else {
                    let (left_block, right_block) =
                        Block::split(conn, block, start_split_point, chromosome_index, phased)
                            .unwrap();
                    Block::delete(conn, block.id);
                    new_edges.push((Some(left_block.id), Some(new_block_id)));
                    right_block.clone()
                };

                if end_split_point == next_block.start {
                    new_edges.push((Some(new_block_id), Some(next_block.id)));
                } else {
                    let (_left_block, right_block) =
                        Block::split(conn, &next_block, end_split_point, chromosome_index, phased)
                            .unwrap();
                    Block::delete(conn, next_block.id);
                    new_edges.push((Some(new_block_id), Some(right_block.id)));
                }
            } else if contains_start {
                // our range is overlapping the end of the block
                // |----block---|
                //        |----range---|
                let split_point = block.start + start - path_start;
                if split_point == block.start {
                    // the split happens before this block begins, so it's an insert operation
                    if let Some(pb) = previous_block {
                        new_edges.push((Some(pb.id), Some(new_block_id)));
                    }
                } else {
                    let (left_block, _right_block) =
                        Block::split(conn, block, split_point, chromosome_index, phased).unwrap();
                    Block::delete(conn, block.id);
                    new_edges.push((Some(left_block.id), Some(new_block_id)));
                }
            } else if contains_end {
                // our range is overlapping the beginning of the block
                //              |----block---|
                //        |----range---|
                let split_point = block.start + end - path_start;
                if split_point == block.start {
                    // the previous change ends right before this block starts, so it's an insert
                    new_edges.push((Some(new_block_id), Some(block.id)));
                } else {
                    let (_left_block, right_block) =
                        Block::split(conn, block, split_point, chromosome_index, phased).unwrap();
                    Block::delete(conn, block.id);
                    new_edges.push((Some(new_block_id), Some(right_block.id)));
                }
                break;
            } else if overlap {
                // our range is the whole block, ignore it
                //          |--block---|
                //        |-----range------|
            } else {
                // not yet at the range
            }

            path_start += block_length;
            if path_start > end {
                break;
            }
            // TODO: will we ever have a scenario where previous_block should not be set?
            // for example, if overlap is true, we shouldn't be making the previous block
            // an intermediary. Tests make this appear to not be a problem, but worth
            // exploring fully.
            previous_block = Some(block);
        }

        for new_edge in new_edges {
            Edge::create(conn, new_edge.0, new_edge.1, chromosome_index, phased);
        }

        change.save(conn);
    }

    #[allow(clippy::ptr_arg)]
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::needless_late_init)]
    pub fn new_insert_change(
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
                target_hash: end_block.sequence.hash.clone(),
                target_coordinate: end - end_block.path_start + end_block.sequence_start,
                chromosome_index,
                phased,
            };
            new_edges.push(new_edge);
        } else {
            // Insertion/replacement
            let new_start_edge = EdgeData {
                source_hash: start_block.sequence.hash.clone(),
                source_coordinate: start - start_block.path_start + start_block.sequence_start,
                target_hash: new_block.sequence.hash.clone(),
                target_coordinate: new_block.sequence_start,
                chromosome_index,
                phased,
            };
            let new_end_edge = EdgeData {
                source_hash: new_block.sequence.hash.clone(),
                source_coordinate: new_block.sequence_end,
                target_hash: end_block.sequence.hash.clone(),
                target_coordinate: end - end_block.path_start + end_block.sequence_start,
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
                target_hash: start_block.sequence.hash.clone(),
                target_coordinate: split_coordinate,
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
                target_hash: end_block.sequence.hash.clone(),
                target_coordinate: split_coordinate,
                chromosome_index,
                phased,
            };

            new_edges.push(new_split_end_edge);
        }

        let edge_ids = NewEdge::bulk_create(conn, new_edges);
        BlockGroupEdge::bulk_create(conn, block_group_id, edge_ids);
    }
}

pub struct ChangeLog {
    hash: String,
    path_id: i32,
    path_start: i32,
    path_end: i32,
    seq_hash: String,
    seq_start: i32,
    seq_end: i32,
    strand: String,
}

impl ChangeLog {
    pub fn new(
        path_id: i32,
        path_start: i32,
        path_end: i32,
        seq_hash: String,
        seq_start: i32,
        seq_end: i32,
        seq_strand: String,
    ) -> ChangeLog {
        let mut hasher = Sha256::new();
        hasher.update(path_id.to_string());
        hasher.update(path_start.to_string());
        hasher.update(path_end.to_string());
        hasher.update(&seq_hash);
        hasher.update(seq_start.to_string());
        hasher.update(seq_end.to_string());
        hasher.update(&seq_strand);
        let result = hasher.finalize();
        let hash = format!("{:x}", result);
        ChangeLog {
            hash,
            path_id,
            path_start,
            path_end,
            seq_hash,
            seq_start,
            seq_end,
            strand: seq_strand,
        }
    }

    pub fn save(&self, conn: &Connection) {
        ChangeLog::create(conn, self);
    }

    pub fn create(conn: &Connection, change_log: &ChangeLog) {
        let mut stmt = conn
            .prepare("INSERT INTO change_log (hash, path_id, path_start, path_end, sequence_hash, sequence_start, sequence_end, sequence_strand) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);")
            .unwrap();
        let placeholders = vec![
            Value::from(change_log.hash.clone()),
            Value::from(change_log.path_id),
            Value::from(change_log.path_start),
            Value::from(change_log.path_end),
            Value::from(change_log.seq_hash.clone()),
            Value::from(change_log.seq_start),
            Value::from(change_log.seq_end),
            Value::from(change_log.strand.clone()),
        ];
        stmt.execute(params_from_iter(placeholders)).unwrap();
    }

    pub fn exists(conn: &mut Connection, hash: &String) -> bool {
        let query = "SELECT hash from change_log where hash = ?1;";
        let mut stmt = conn.prepare(query).unwrap();
        stmt.exists((hash,)).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_migrations;

    fn get_connection() -> Connection {
        let mut conn = Connection::open_in_memory()
            .unwrap_or_else(|_| panic!("Error opening in memory test db"));
        run_migrations(&mut conn);
        conn
    }

    fn setup_block_group(conn: &Connection) -> (i32, i32) {
        let a_seq_hash = Sequence::create(conn, "DNA", "AAAAAAAAAA", true);
        let t_seq_hash = Sequence::create(conn, "DNA", "TTTTTTTTTT", true);
        let c_seq_hash = Sequence::create(conn, "DNA", "CCCCCCCCCC", true);
        let g_seq_hash = Sequence::create(conn, "DNA", "GGGGGGGGGG", true);
        let _collection = Collection::create(conn, "test");
        let block_group = BlockGroup::create(conn, "test", None, "hg19");
        let a_block = Block::create(conn, &a_seq_hash, block_group.id, 0, 10, "+");
        let t_block = Block::create(conn, &t_seq_hash, block_group.id, 0, 10, "+");
        let c_block = Block::create(conn, &c_seq_hash, block_group.id, 0, 10, "+");
        let g_block = Block::create(conn, &g_seq_hash, block_group.id, 0, 10, "+");
        let _edge_0 = Edge::create(conn, None, Some(a_block.id), 0, 0);
        let _edge_1 = Edge::create(conn, Some(a_block.id), Some(t_block.id), 0, 0);
        let _edge_2 = Edge::create(conn, Some(t_block.id), Some(c_block.id), 0, 0);
        let _edge_3 = Edge::create(conn, Some(c_block.id), Some(g_block.id), 0, 0);
        let _edge_4 = Edge::create(conn, Some(g_block.id), None, 0, 0);
        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            vec![a_block.id, t_block.id, c_block.id, g_block.id],
        );
        (block_group.id, path.id)
    }

    #[test]
    fn insert_and_deletion() {
        let mut conn = get_connection();
        let (block_group_id, path_id) = setup_block_group(&conn);
        let insert_sequence = Sequence::create(&conn, "DNA", "NNNN", true);
        let insert = Block::create(&conn, &insert_sequence, block_group_id, 0, 4, "+");
        BlockGroup::insert_change(&mut conn, path_id, 7, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );

        // TODO: should handle this w/ edges instead of a block, maybe this is ok though.
        let deletion_sequence = Sequence::create(&conn, "DNA", "", true);
        let deletion = Block::create(&conn, &deletion_sequence, block_group_id, 0, 0, "+");

        // take out an entire block.
        BlockGroup::insert_change(&mut conn, path_id, 19, 31, &deletion, 1, 0);
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
    fn simple_insert() {
        let mut conn = get_connection();
        let (block_group_id, path_id) = setup_block_group(&conn);
        let insert_sequence = Sequence::create(&conn, "DNA", "NNNN", true);
        let insert = Block::create(&conn, &insert_sequence, block_group_id, 0, 4, "+");
        BlockGroup::insert_change(&mut conn, path_id, 7, 15, &insert, 1, 0);

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
    fn insert_on_block_boundary_middle() {
        let mut conn = get_connection();
        let (block_group_id, path_id) = setup_block_group(&conn);
        let insert_sequence = Sequence::create(&conn, "DNA", "NNNN", true);
        let insert = Block::create(&conn, &insert_sequence, block_group_id, 0, 4, "+");
        BlockGroup::insert_change(&mut conn, path_id, 15, 15, &insert, 1, 0);

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
    fn insert_within_block() {
        let mut conn = get_connection();
        let (block_group_id, path_id) = setup_block_group(&conn);
        let insert_sequence = Sequence::create(&conn, "DNA", "NNNN", true);
        let insert = Block::create(&conn, &insert_sequence, block_group_id, 0, 4, "+");
        BlockGroup::insert_change(&mut conn, path_id, 12, 17, &insert, 1, 0);

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
    fn insert_on_block_boundary_start() {
        let mut conn = get_connection();
        let (block_group_id, path_id) = setup_block_group(&conn);
        let insert_sequence = Sequence::create(&conn, "DNA", "NNNN", true);
        let insert = Block::create(&conn, &insert_sequence, block_group_id, 0, 4, "+");
        BlockGroup::insert_change(&mut conn, path_id, 10, 10, &insert, 1, 0);

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
    fn insert_on_block_boundary_end() {
        let mut conn = get_connection();
        let (block_group_id, path_id) = setup_block_group(&conn);
        let insert_sequence = Sequence::create(&conn, "DNA", "NNNN", true);
        let insert = Block::create(&conn, &insert_sequence, block_group_id, 0, 4, "+");
        BlockGroup::insert_change(&mut conn, path_id, 9, 9, &insert, 1, 0);

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
    fn insert_across_entire_block_boundary() {
        let mut conn = get_connection();
        let (block_group_id, path_id) = setup_block_group(&conn);
        let insert_sequence = Sequence::create(&conn, "DNA", "NNNN", true);
        let insert = Block::create(&conn, &insert_sequence, block_group_id, 0, 4, "+");
        BlockGroup::insert_change(&mut conn, path_id, 10, 20, &insert, 1, 0);

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
    fn insert_across_two_blocks() {
        let mut conn = get_connection();
        let (block_group_id, path_id) = setup_block_group(&conn);
        let insert_sequence = Sequence::create(&conn, "DNA", "NNNN", true);
        let insert = Block::create(&conn, &insert_sequence, block_group_id, 0, 4, "+");
        BlockGroup::insert_change(&mut conn, path_id, 15, 25, &insert, 1, 0);

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
    fn insert_spanning_blocks() {
        let mut conn = get_connection();
        let (block_group_id, path_id) = setup_block_group(&conn);
        let insert_sequence = Sequence::create(&conn, "DNA", "NNNN", true);
        let insert = Block::create(&conn, &insert_sequence, block_group_id, 0, 4, "+");
        BlockGroup::insert_change(&mut conn, path_id, 5, 35, &insert, 1, 0);

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
    fn simple_deletion() {
        let mut conn = get_connection();
        let (block_group_id, path_id) = setup_block_group(&conn);
        let deletion_sequence = Sequence::create(&conn, "DNA", "", true);
        let deletion = Block::create(&conn, &deletion_sequence, block_group_id, 0, 0, "+");

        // take out an entire block.
        BlockGroup::insert_change(&mut conn, path_id, 19, 31, &deletion, 1, 0);
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
    fn doesnt_apply_same_insert_twice() {
        let mut conn = get_connection();
        let (block_group_id, path_id) = setup_block_group(&conn);
        let insert_sequence = Sequence::create(&conn, "DNA", "NNNN", true);
        let insert = Block::create(&conn, &insert_sequence, block_group_id, 0, 4, "+");
        BlockGroup::insert_change(&mut conn, path_id, 7, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );

        BlockGroup::insert_change(&mut conn, path_id, 7, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    fn setup_multipath(conn: &Connection) -> (i32, Path) {
        let a_seq_hash = Sequence::create(conn, "DNA", "AAAAAAAAAA", true);
        let t_seq_hash = Sequence::create(conn, "DNA", "TTTTTTTTTT", true);
        let c_seq_hash = Sequence::create(conn, "DNA", "CCCCCCCCCC", true);
        let g_seq_hash = Sequence::create(conn, "DNA", "GGGGGGGGGG", true);
        let _collection = Collection::create(conn, "test");
        let block_group = BlockGroup::create(conn, "test", None, "hg19");
        let edge0 = NewEdge::create(
            conn,
            NewEdge::PATH_START_HASH.to_string(),
            0,
            a_seq_hash.clone(),
            0,
            0,
            0,
        );
        let edge1 = NewEdge::create(conn, a_seq_hash, 10, t_seq_hash.clone(), 0, 0, 0);
        let edge2 = NewEdge::create(conn, t_seq_hash, 10, c_seq_hash.clone(), 0, 0, 0);
        let edge3 = NewEdge::create(conn, c_seq_hash, 10, g_seq_hash.clone(), 0, 0, 0);
        let edge4 = NewEdge::create(
            conn,
            g_seq_hash,
            10,
            NewEdge::PATH_END_HASH.to_string(),
            0,
            0,
            0,
        );
        BlockGroupEdge::bulk_create(
            conn,
            block_group.id,
            vec![edge0.id, edge1.id, edge2.id, edge3.id, edge4.id],
        );
        let path = Path::new_create(
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
        let (block_group_id, path) = setup_multipath(&conn);
        let insert_sequence_hash = Sequence::create(&conn, "DNA", "NNNN", true);
        let sequences_by_hash =
            Sequence::sequences_by_hash(&conn, vec![format!("\"{}\"", insert_sequence_hash)]);
        let insert_sequence = sequences_by_hash.get(&insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.sequence[0..4].to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: "+".to_string(),
        };
        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 7, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );

        let deletion_sequence_hash = Sequence::create(&conn, "DNA", "", true);
        let sequences_by_hash =
            Sequence::sequences_by_hash(&conn, vec![format!("\"{}\"", deletion_sequence_hash)]);
        let deletion_sequence = sequences_by_hash.get(&deletion_sequence_hash).unwrap();
        let deletion = NewBlock {
            id: 0,
            sequence: deletion_sequence.clone(),
            block_sequence: deletion_sequence.sequence.clone(),
            sequence_start: 0,
            sequence_end: 0,
            path_start: 19,
            path_end: 31,
            strand: "+".to_string(),
        };

        // take out an entire block.
        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 19, 31, &deletion, 1, 0);
        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
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
        let (block_group_id, path) = setup_multipath(&conn);
        let insert_sequence_hash = Sequence::create(&conn, "DNA", "NNNN", true);
        let sequences_by_hash =
            Sequence::sequences_by_hash(&conn, vec![format!("\"{}\"", insert_sequence_hash)]);
        let insert_sequence = sequences_by_hash.get(&insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.sequence[0..4].to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: "+".to_string(),
        };
        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 7, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
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
        let (block_group_id, path) = setup_multipath(&conn);
        let insert_sequence_hash = Sequence::create(&conn, "DNA", "NNNN", true);
        let sequences_by_hash =
            Sequence::sequences_by_hash(&conn, vec![format!("\"{}\"", insert_sequence_hash)]);
        let insert_sequence = sequences_by_hash.get(&insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.sequence[0..4].to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 15,
            path_end: 15,
            strand: "+".to_string(),
        };
        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 15, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
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
        let (block_group_id, path) = setup_multipath(&conn);
        let insert_sequence_hash = Sequence::create(&conn, "DNA", "NNNN", true);
        let sequences_by_hash =
            Sequence::sequences_by_hash(&conn, vec![format!("\"{}\"", insert_sequence_hash)]);
        let insert_sequence = sequences_by_hash.get(&insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.sequence[0..4].to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 12,
            path_end: 17,
            strand: "+".to_string(),
        };
        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 12, 17, &insert, 1, 0);

        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
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
        let (block_group_id, path) = setup_multipath(&conn);
        let insert_sequence_hash = Sequence::create(&conn, "DNA", "NNNN", true);
        let sequences_by_hash =
            Sequence::sequences_by_hash(&conn, vec![format!("\"{}\"", insert_sequence_hash)]);
        let insert_sequence = sequences_by_hash.get(&insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.sequence[0..4].to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 10,
            path_end: 10,
            strand: "+".to_string(),
        };
        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 10, 10, &insert, 1, 0);

        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
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
        let (block_group_id, path) = setup_multipath(&conn);
        let insert_sequence_hash = Sequence::create(&conn, "DNA", "NNNN", true);
        let sequences_by_hash =
            Sequence::sequences_by_hash(&conn, vec![format!("\"{}\"", insert_sequence_hash)]);
        let insert_sequence = sequences_by_hash.get(&insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.sequence[0..4].to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 9,
            path_end: 9,
            strand: "+".to_string(),
        };
        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 9, 9, &insert, 1, 0);

        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
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
        let (block_group_id, path) = setup_multipath(&conn);
        let insert_sequence_hash = Sequence::create(&conn, "DNA", "NNNN", true);
        let sequences_by_hash =
            Sequence::sequences_by_hash(&conn, vec![format!("\"{}\"", insert_sequence_hash)]);
        let insert_sequence = sequences_by_hash.get(&insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.sequence[0..4].to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 10,
            path_end: 20,
            strand: "+".to_string(),
        };
        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 10, 20, &insert, 1, 0);

        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
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
        let (block_group_id, path) = setup_multipath(&conn);
        let insert_sequence_hash = Sequence::create(&conn, "DNA", "NNNN", true);
        let sequences_by_hash =
            Sequence::sequences_by_hash(&conn, vec![format!("\"{}\"", insert_sequence_hash)]);
        let insert_sequence = sequences_by_hash.get(&insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.sequence[0..4].to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 15,
            path_end: 25,
            strand: "+".to_string(),
        };
        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 15, 25, &insert, 1, 0);

        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
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
        let (block_group_id, path) = setup_multipath(&conn);
        let insert_sequence_hash = Sequence::create(&conn, "DNA", "NNNN", true);
        let sequences_by_hash =
            Sequence::sequences_by_hash(&conn, vec![format!("\"{}\"", insert_sequence_hash)]);
        let insert_sequence = sequences_by_hash.get(&insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.sequence[0..4].to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 5,
            path_end: 35,
            strand: "+".to_string(),
        };
        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 5, 35, &insert, 1, 0);

        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
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
        let (block_group_id, path) = setup_multipath(&conn);
        let deletion_sequence_hash = Sequence::create(&conn, "DNA", "", true);
        let sequences_by_hash =
            Sequence::sequences_by_hash(&conn, vec![format!("\"{}\"", deletion_sequence_hash)]);
        let deletion_sequence = sequences_by_hash.get(&deletion_sequence_hash).unwrap();
        let deletion = NewBlock {
            id: 0,
            sequence: deletion_sequence.clone(),
            block_sequence: deletion_sequence.sequence.clone(),
            sequence_start: 0,
            sequence_end: 0,
            path_start: 19,
            path_end: 31,
            strand: "+".to_string(),
        };

        // take out an entire block.
        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 19, 31, &deletion, 1, 0);
        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
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
        let (block_group_id, path) = setup_multipath(&conn);
        let insert_sequence_hash = Sequence::create(&conn, "DNA", "NNNN", true);
        let sequences_by_hash =
            Sequence::sequences_by_hash(&conn, vec![format!("\"{}\"", insert_sequence_hash)]);
        let insert_sequence = sequences_by_hash.get(&insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.sequence[0..4].to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: "+".to_string(),
        };
        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 7, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );

        BlockGroup::new_insert_change(&mut conn, block_group_id, &path, 7, 15, &insert, 1, 0);

        let all_sequences = BlockGroup::new_get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }
}
