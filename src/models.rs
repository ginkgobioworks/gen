use noodles::vcf::variant::record::info::field::value::array::Values;
use petgraph::data::Build;
use petgraph::graphmap::DiGraphMap;
use petgraph::visit::{Dfs, IntoNeighborsDirected, NodeCount};
use petgraph::Direction;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fmt::*;
use std::hash::Hash;

pub mod block;
pub mod edge;
pub mod path;
pub mod sequence;

use crate::models;
use crate::models::block::Block;
use crate::models::edge::Edge;
use crate::models::path::{all_simple_paths, Path, PathBlock};
use crate::models::sequence::Sequence;

#[derive(Debug)]
pub struct Collection {
    pub name: String,
}

impl Collection {
    pub fn exists(conn: &mut Connection, name: &String) -> bool {
        let mut stmt = conn
            .prepare("select name from collection where name = ?1")
            .unwrap();
        stmt.exists([name]).unwrap()
    }
    pub fn create(conn: &mut Connection, name: &String) -> Collection {
        let mut stmt = conn
            .prepare("INSERT INTO collection (name) VALUES (?1) RETURNING *")
            .unwrap();
        let mut rows = stmt
            .query_map((name,), |row| Ok(models::Collection { name: row.get(0)? }))
            .unwrap();
        rows.next().unwrap().unwrap()
    }

    pub fn bulk_create(conn: &mut Connection, names: &Vec<String>) -> Vec<Collection> {
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
    pub fn create(conn: &mut Connection, name: &String) -> Sample {
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

impl BlockGroup {
    pub fn create(
        conn: &mut Connection,
        collection_name: &String,
        sample_name: Option<&String>,
        name: &String,
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
                        collection_name: collection_name.clone(),
                        sample_name: sample_name.map(|s| s.to_string()),
                        name: name.clone()
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
            let mut new_edge;
            if target_id.is_some() && source_id.is_some() {
                let target_id = target_id.unwrap();
                let source_id = source_id.unwrap();
                new_edge = Edge::create(
                    conn,
                    Some(*block_map.get(&source_id).unwrap_or(&source_id)),
                    Some(*block_map.get(&target_id).unwrap_or(&target_id)),
                    chrom_index,
                    phased,
                );
            } else if target_id.is_some() {
                let target_id = target_id.unwrap();
                new_edge = Edge::create(
                    conn,
                    None,
                    Some(*block_map.get(&target_id).unwrap_or(&target_id)),
                    chrom_index,
                    phased,
                );
            } else if source_id.is_some() {
                let source_id = source_id.unwrap();
                new_edge = Edge::create(
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
            let mut new_blocks = vec![];
            for block in path.blocks {
                new_blocks.push(*block_map.get(&block).unwrap());
            }
            Path::create(conn, &path.name, target_block_group_id, new_blocks);
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
            .collect::<Vec<_>>()
            .join(",");
        let mut sequence_map = HashMap::new();
        for sequence in Sequence::get_sequences(
            conn,
            &format!("select * from sequence where hash in ({sequence_hashes})"),
            vec![],
        ) {
            sequence_map.insert(sequence.hash, sequence.sequence);
        }
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
                for path in all_simple_paths(&graph, start_node, *end_node) {
                    let mut current_sequence = "".to_string();
                    for node in path {
                        let block = block_map.get(&node).unwrap();
                        let block_sequence = sequence_map.get(&block.sequence_hash).unwrap();
                        current_sequence.push_str(
                            &block_sequence[(block.start as usize)..(block.end as usize)],
                        );
                    }
                    sequences.insert(current_sequence);
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
        println!("change is {path_id} {start} {end} {new_block_id}");
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
        println!("{path:?} {graph:?}");
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
            let block_length = (block.end - block.start);
            path_end += block_length;

            let contains_start = path_start <= start && start < path_end;
            let contains_end = path_start < end && end < path_end;
            let overlap = path_start < end && start < path_end;

            if contains_start && contains_end {
                // our range is fully contained w/in the block
                //      |----block------|
                //        |----range---|
                let (left_block, right_block) = Block::split(
                    conn,
                    block,
                    block.start + start - path_start,
                    chromosome_index,
                    phased,
                )
                .unwrap();
                Block::delete(conn, block.id);
                // let left_block = Block::create(
                //     conn,
                //     &block.sequence_hash,
                //     block_group_id,
                //     block.start,
                //     start - path_start,
                //     &block.strand,
                // );
                // let right_block = Block::create(
                //     conn,
                //     &block.sequence_hash,
                //     block_group_id,
                //     block.start + (end - path_start),
                //     block.end,
                //     &block.strand,
                // );
                // if let Some(value) = previous_block {
                //     new_edges.push((Some(value.id), Some(left_block.id)))
                // }
                new_edges.push((Some(left_block.id), Some(new_block_id)));
                new_edges.push((Some(new_block_id), Some(right_block.id)));
            } else if contains_start {
                // our range is overlapping the end of the block
                // |----block---|
                //        |----range---|
                let (left_block, right_block) = Block::split(
                    conn,
                    block,
                    block.start + start - path_start,
                    chromosome_index,
                    phased,
                )
                .unwrap();
                Block::delete(conn, block.id);
                // let left_block = Block::create(
                //     conn,
                //     &block.sequence_hash,
                //     block_group_id,
                //     block.start,
                //     start - path_start,
                //     &block.strand,
                // );
                // if let Some(value) = previous_block {
                //     new_edges.push((Some(value.id), Some(left_block.id)));
                // } else {
                //     new_edges.push((None, Some(left_block.id)));
                // }
                new_edges.push((Some(left_block.id), Some(new_block_id)));
            } else if contains_end {
                // our range is overlapping the beginning of the block
                //              |----block---|
                //        |----range---|
                let (left_block, right_block) = Block::split(
                    conn,
                    block,
                    block.start + end - path_start,
                    chromosome_index,
                    phased,
                )
                .unwrap();
                Block::delete(conn, block.id);
                // let right_block = Block::create(
                //     conn,
                //     &block.sequence_hash,
                //     block_group_id,
                //     end - path_start,
                //     block.end,
                //     &block.strand,
                // );
                // // what stuff went to this block?
                new_edges.push((Some(new_block_id), Some(right_block.id)));
                // let last_node = dfs.next(&graph);
                // if last_node.is_some() {
                //     let next_block = blocks.get(&(last_node.unwrap() as i32)).unwrap();
                //     new_edges.push((Some(right_block.id), Some(next_block.id)));
                // } else {
                //     new_edges.push((Some(right_block.id), None))
                // }
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
            previous_block = Some(block);
        }

        for new_edge in new_edges {
            Edge::create(conn, new_edge.0, new_edge.1, chromosome_index, phased);
        }

        change.save(conn);
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
    use crate::get_connection as get_db_connection;
    use crate::migrations::run_migrations;
    use std::fs;
    use std::hash::Hash;

    fn get_connection() -> Connection {
        let mut conn = Connection::open_in_memory()
            .unwrap_or_else(|_| panic!("Error opening in memory test db"));
        run_migrations(&mut conn);
        conn
    }

    fn setup_block_group(conn: &mut Connection) -> (i32, i32) {
        let a_seq_hash = Sequence::create(conn, "DNA".to_string(), &"AAAAAAAAAA".to_string(), true);
        let t_seq_hash = Sequence::create(conn, "DNA".to_string(), &"TTTTTTTTTT".to_string(), true);
        let c_seq_hash = Sequence::create(conn, "DNA".to_string(), &"CCCCCCCCCC".to_string(), true);
        let g_seq_hash = Sequence::create(conn, "DNA".to_string(), &"GGGGGGGGGG".to_string(), true);
        let collection = Collection::create(conn, &"test".to_string());
        let block_group = BlockGroup::create(conn, &"test".to_string(), None, &"hg19".to_string());
        let a_block = Block::create(conn, &a_seq_hash, block_group.id, 0, 10, &"1".to_string());
        let t_block = Block::create(conn, &t_seq_hash, block_group.id, 0, 10, &"1".to_string());
        let c_block = Block::create(conn, &c_seq_hash, block_group.id, 0, 10, &"1".to_string());
        let g_block = Block::create(conn, &g_seq_hash, block_group.id, 0, 10, &"1".to_string());
        let edge_0 = Edge::create(conn, None, Some(a_block.id), 0, 0);
        let edge_1 = Edge::create(conn, Some(a_block.id), Some(t_block.id), 0, 0);
        let edge_2 = Edge::create(conn, Some(t_block.id), Some(c_block.id), 0, 0);
        let edge_3 = Edge::create(conn, Some(c_block.id), Some(g_block.id), 0, 0);
        let edge_4 = Edge::create(conn, Some(g_block.id), None, 0, 0);
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
        let (block_group_id, path_id) = setup_block_group(&mut conn);
        let insert_sequence =
            Sequence::create(&mut conn, "DNA".to_string(), &"NNNN".to_string(), true);
        let insert = Block::create(
            &conn,
            &insert_sequence,
            block_group_id,
            0,
            4,
            &"1".to_string(),
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

        // TODO: should handle this w/ edges instead of a block, maybe this is ok though.
        let deletion_sequence =
            Sequence::create(&mut conn, "DNA".to_string(), &"".to_string(), true);
        let deletion = Block::create(
            &conn,
            &deletion_sequence,
            block_group_id,
            0,
            0,
            &"1".to_string(),
        );

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
        let (block_group_id, path_id) = setup_block_group(&mut conn);
        let insert_sequence =
            Sequence::create(&mut conn, "DNA".to_string(), &"NNNN".to_string(), true);
        let insert = Block::create(
            &conn,
            &insert_sequence,
            block_group_id,
            0,
            4,
            &"1".to_string(),
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

    #[test]
    fn simple_deletion() {
        let mut conn = get_connection();
        let (block_group_id, path_id) = setup_block_group(&mut conn);
        let deletion_sequence =
            Sequence::create(&mut conn, "DNA".to_string(), &"".to_string(), true);
        let deletion = Block::create(
            &conn,
            &deletion_sequence,
            block_group_id,
            0,
            0,
            &"1".to_string(),
        );

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
        let (block_group_id, path_id) = setup_block_group(&mut conn);
        let insert_sequence =
            Sequence::create(&mut conn, "DNA".to_string(), &"NNNN".to_string(), true);
        let insert = Block::create(
            &conn,
            &insert_sequence,
            block_group_id,
            0,
            4,
            &"1".to_string(),
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
}
