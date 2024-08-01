use std::collections::hash_map::Entry::Vacant;
use std::collections::HashMap;
use std::fmt::*;

use rusqlite::{params_from_iter, Connection};

pub mod block;
pub mod edge;
pub mod sequence;
use crate::models;
use crate::models::block::Block;
use crate::models::edge::Edge;

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
            .prepare("INSERT INTO sample (name) VALUES (?1) RETURNING *")
            .unwrap();
        let mut rows = stmt
            .query_map((name,), |row| Ok(Sample { name: row.get(0)? }))
            .unwrap();
        rows.next().unwrap().unwrap()
    }
}

#[derive(Debug)]
pub struct Path {
    pub id: i32,
    pub collection_name: String,
    pub sample_name: Option<String>,
    pub name: String,
    pub path_index: i32,
}

impl Path {
    // pub fn create(
    //     conn: &mut Connection,
    //     collection_name: &String,
    //     sample_name: Option<&String>,
    //     path_name: &String,
    //     path_index: Option<i32>,
    // ) -> Path {
    //     let query = "INSERT INTO path (collection_name, sample_name, name, path_index) VALUES (?1, ?2, ?3, ?4) RETURNING *";
    //     let mut stmt = conn.prepare(query).unwrap();
    //     let index = path_index.unwrap_or(0);
    //     match stmt.query_row((collection_name, sample_name, path_name, index), |row| {
    //         Ok(Path {
    //             id: row.get(0)?,
    //             collection_name: row.get(1)?,
    //             sample_name: row.get(2)?,
    //             name: row.get(3)?,
    //             path_index: row.get(4)?,
    //         })
    //     }) {
    //         Ok(path) => path,
    //         Err(rusqlite::Error::SqliteFailure(err, details)) => {
    //             if err.code == rusqlite::ErrorCode::ConstraintViolation {
    //                 println!("{err:?} {details:?}");
    //                 Path {
    //                     id: conn
    //                         .query_row(
    //                             "select id from path where collection_name = ?1 and sample_name is null and name = ?2 and path_index = ?3",
    //                             (collection_name, path_name, index),
    //                             |row| row.get(0),
    //                         )
    //                         .unwrap(),
    //                     collection_name: collection_name.clone(),
    //                     sample_name: sample_name.map(|s| s.to_string()),
    //                     name: path_name.clone(),
    //                     path_index: index,
    //                 }
    //             } else {
    //                 panic!("something bad happened querying the database")
    //             }
    //         }
    //         Err(_) => {
    //             panic!("something bad happened querying the database")
    //         }
    //     }
    // }
    //
    // pub fn clone(conn: &mut Connection, source_path_id: i32, target_path_id: i32) {
    //     let mut stmt = conn
    //         .prepare_cached(
    //             "SELECT id, sequence_hash, start, end, strand from block where path_id = ?1",
    //         )
    //         .unwrap();
    //     let mut block_map: HashMap<i32, i32> = HashMap::new();
    //     let mut it = stmt.query([source_path_id]).unwrap();
    //     let mut row = it.next().unwrap();
    //     while row.is_some() {
    //         let block = row.unwrap();
    //         let block_id: i32 = block.get(0).unwrap();
    //         let hash: String = block.get(1).unwrap();
    //         let start = block.get(2).unwrap();
    //         let end = block.get(3).unwrap();
    //         let strand: String = block.get(4).unwrap();
    //         let new_block = Block::create(conn, &hash, target_path_id, start, end, &strand);
    //         block_map.insert(block_id, new_block.id);
    //         row = it.next().unwrap();
    //     }
    //
    //     // todo: figure out rusqlite's rarray
    //     let mut stmt = conn
    //         .prepare_cached("SELECT source_id, target_id from edges where source_id IN (?1)")
    //         .unwrap();
    //     let block_keys = block_map
    //         .keys()
    //         .map(|k| format!("{k}"))
    //         .collect::<Vec<_>>()
    //         .join(", ");
    //     let mut it = stmt.query([block_keys]).unwrap();
    //     let mut row = it.next().unwrap();
    //     while row.is_some() {
    //         let edge = row.unwrap();
    //         let source_id: i32 = edge.get(0).unwrap();
    //         let target_id: Option<i32> = edge.get(1).unwrap();
    //         Edge::create(
    //             conn,
    //             *block_map.get(&source_id).unwrap_or(&source_id),
    //             target_id,
    //         );
    //         row = it.next().unwrap();
    //     }
    // }
    //
    // pub fn get_or_create_sample_path(
    //     conn: &mut Connection,
    //     collection_name: &String,
    //     sample_name: &String,
    //     path_name: &String,
    //     new_path_index: i32,
    // ) -> i32 {
    //     let mut path_id : i32 = match conn.query_row(
    //         "select id from path where collection_name = ?1 AND sample_name = ?2 AND name = ?3 AND path_index = ?4",
    //         (collection_name, sample_name, path_name, new_path_index),
    //         |row| row.get(0),
    //     ) {
    //         Ok(res) => res,
    //         Err(rusqlite::Error::QueryReturnedNoRows) => 0,
    //         Err(_e) => {
    //             panic!("Error querying the database: {_e}");
    //         }
    //     };
    //     if path_id != 0 {
    //         return path_id;
    //     } else {
    //         // no path exists, so make it first -- check if we have a reference path for this sample first
    //         path_id = match conn.query_row(
    //         "select id from path where collection_name = ?1 AND sample_name = ?2 AND name = ?3 AND path_index = 0",
    //         (collection_name, sample_name, path_name),
    //         |row| row.get(0),
    //         ) {
    //             Ok(res) => res,
    //             Err(rusqlite::Error::QueryReturnedNoRows) => 0,
    //             Err(_e) => {
    //                 panic!("something bad happened querying the database")
    //             }
    //         }
    //     }
    //     if path_id == 0 {
    //         // use the base reference bath if it exists since there is no base sample path
    //         path_id = match conn.query_row(
    //         "select path.id from path where collection_name = ?1 AND sample_name IS null AND name = ?2 AND path_index = 0",
    //         (collection_name, path_name),
    //         |row| row.get(0),
    //         ) {
    //             Ok(res) => res,
    //             Err(rusqlite::Error::QueryReturnedNoRows) => panic!("No base path exists"),
    //             Err(_e) => {
    //                 panic!("something bad happened querying the database")
    //             }
    //         }
    //     }
    //     let new_path_id = Path::create(
    //         conn,
    //         collection_name,
    //         Some(sample_name),
    //         path_name,
    //         Some(new_path_index),
    //     );
    //
    //     // clone parent blocks/edges
    //     Path::clone(conn, path_id, new_path_id.id);
    //
    //     new_path_id.id
    // }
    //
    // #[allow(clippy::ptr_arg)]
    // #[allow(clippy::too_many_arguments)]
    // pub fn insert_change(
    //     conn: &mut Connection,
    //     path_id: i32,
    //     start: i32,
    //     end: i32,
    //     new_block_id: i32,
    // ) {
    //     println!("change is {path_id} {start} {end} {new_block_id}");
    //     // todo:
    //     // 1. get blocks where start-> end overlap
    //     // 2. split old blocks at boundry points, make new block for left/right side
    //     // 3. make new block for sequence we are changing
    //     // 4. update edges
    //     // add support for deletion
    //     // cases to check:
    //     //  change that is the size of a block
    //     //  change that goes over multiple blocks
    //     //  change that hits just start/end boundry, e.g. block is 1,5 and change is 3,5 or 1,3.
    //     //  change that deletes block boundry
    //     // https://stackoverflow.com/questions/3269434/whats-the-most-efficient-way-to-test-if-two-ranges-overlap
    //     let mut stmt = conn.prepare_cached("select b.id, b.sequence_hash, b.path_id, b.start, b.end, b.strand, e.id as edge_id, e.source_id, e.target_id from block b left join edges e on (e.source_id = b.id or e.target_id = b.id) where b.path_id = ?1 AND b.start <= ?3 AND ?2 <= b.end AND b.id != ?4;").unwrap();
    //     let mut block_edges: HashMap<i32, Vec<Edge>> = HashMap::new();
    //     let mut blocks: HashMap<i32, Block> = HashMap::new();
    //     let mut it = stmt.query([path_id, start, end, new_block_id]).unwrap();
    //     let mut row = it.next().unwrap();
    //     while row.is_some() {
    //         let entry = row.unwrap();
    //         let block_id = entry.get(0).unwrap();
    //         let edge_id: Option<i32> = entry.get(6).unwrap();
    //         blocks.insert(
    //             block_id,
    //             Block {
    //                 id: block_id,
    //                 sequence_hash: entry.get(1).unwrap(),
    //                 path_id: entry.get(2).unwrap(),
    //                 start: entry.get(3).unwrap(),
    //                 end: entry.get(4).unwrap(),
    //                 strand: entry.get(5).unwrap(),
    //             },
    //         );
    //         if edge_id.is_some() {
    //             if let Vacant(e) = block_edges.entry(block_id) {
    //                 e.insert(vec![Edge {
    //                     id: edge_id.unwrap(),
    //                     source_id: entry.get(7).unwrap(),
    //                     target_id: entry.get(8).unwrap(),
    //                 }]);
    //             } else {
    //                 block_edges.get_mut(&block_id).unwrap().push(Edge {
    //                     id: entry.get(6).unwrap(),
    //                     source_id: entry.get(7).unwrap(),
    //                     target_id: entry.get(8).unwrap(),
    //                 });
    //             }
    //         } else {
    //             println!("empty eid {row:?}");
    //         }
    //         row = it.next().unwrap();
    //     }
    //
    //     #[derive(Debug)]
    //     struct ReplacementEdge {
    //         id: i32,
    //         new_source_id: Option<i32>,
    //         new_target_id: Option<i32>,
    //     }
    //     let mut replacement_edges: Vec<ReplacementEdge> = vec![];
    //     let mut new_edges: Vec<(i32, i32)> = vec![];
    //
    //     for (block_id, block) in &blocks {
    //         let contains_start = block.start <= start && start < block.end;
    //         let contains_end = block.start <= end && end < block.end;
    //
    //         if contains_start && contains_end {
    //             // our range is fully contained w/in the block
    //             //      |----block------|
    //             //        |----range---|
    //             let left_block = Block::create(
    //                 conn,
    //                 &block.sequence_hash,
    //                 path_id,
    //                 block.start,
    //                 start,
    //                 &block.strand,
    //             );
    //             let right_block = Block::create(
    //                 conn,
    //                 &block.sequence_hash,
    //                 path_id,
    //                 end,
    //                 block.end,
    //                 &block.strand,
    //             );
    //             println!("lb {left_block:?} {right_block:?}");
    //             new_edges.push((left_block.id, new_block_id));
    //             new_edges.push((new_block_id, right_block.id));
    //             // what stuff went to this block?
    //             for edges in block_edges.get(block_id) {
    //                 for edge in edges {
    //                     println!("block {block_id} on edge {edge:?}");
    //                     let mut new_source_id = None;
    //                     let mut new_target_id = None;
    //                     if edge.source_id == *block_id {
    //                         new_source_id = Some(right_block.id);
    //                     }
    //                     if edge.target_id.is_some() && edge.target_id.unwrap() == *block_id {
    //                         new_target_id = Some(left_block.id);
    //                     }
    //                     replacement_edges.push(ReplacementEdge {
    //                         id: edge.id,
    //                         new_source_id,
    //                         new_target_id,
    //                     });
    //                     println!("new res {replacement_edges:?}");
    //                 }
    //             }
    //         } else if contains_start {
    //             // our range is overlapping the end of the block
    //             // |----block---|
    //             //        |----range---|
    //             let left_block = Block::create(
    //                 conn,
    //                 &block.sequence_hash,
    //                 path_id,
    //                 block.start,
    //                 start,
    //                 &block.strand,
    //             );
    //             new_edges.push((left_block.id, new_block_id));
    //             // what stuff went to this block?
    //             for edges in block_edges.get(block_id) {
    //                 for edge in edges {
    //                     let mut new_source_id = None;
    //                     let mut new_target_id = None;
    //                     if edge.source_id == *block_id {
    //                         new_source_id = Some(new_block_id);
    //                     }
    //                     if edge.target_id.is_some() && edge.target_id.unwrap() == *block_id {
    //                         new_target_id = Some(left_block.id);
    //                     }
    //                     replacement_edges.push(ReplacementEdge {
    //                         id: edge.id,
    //                         new_source_id,
    //                         new_target_id,
    //                     });
    //                 }
    //             }
    //         } else if contains_end {
    //             // our range is overlapping the beginning of the block
    //             //              |----block---|
    //             //        |----range---|
    //             let right_block = Block::create(
    //                 conn,
    //                 &block.sequence_hash,
    //                 path_id,
    //                 end,
    //                 block.end,
    //                 &block.strand,
    //             );
    //             // what stuff went to this block?
    //             new_edges.push((new_block_id, right_block.id));
    //             for edges in block_edges.get(block_id) {
    //                 for edge in edges {
    //                     let mut new_source_id = None;
    //                     let mut new_target_id = None;
    //                     if edge.source_id == *block_id {
    //                         new_source_id = Some(right_block.id);
    //                     }
    //                     if edge.target_id.is_some() && edge.target_id.unwrap() == *block_id {
    //                         new_target_id = Some(new_block_id);
    //                     }
    //                     replacement_edges.push(ReplacementEdge {
    //                         id: edge.id,
    //                         new_source_id,
    //                         new_target_id,
    //                     })
    //                 }
    //             }
    //         } else {
    //             // our range is the whole block, get rid of it
    //             //          |--block---|
    //             //        |-----range------|
    //             // what stuff went to this block?
    //             for edges in block_edges.get(block_id) {
    //                 for edge in edges {
    //                     let mut new_source_id = None;
    //                     let mut new_target_id = None;
    //                     if edge.source_id == *block_id {
    //                         new_source_id = Some(new_block_id);
    //                     }
    //                     if edge.target_id.is_some() && edge.target_id.unwrap() == *block_id {
    //                         new_target_id = Some(new_block_id);
    //                     }
    //                     replacement_edges.push(ReplacementEdge {
    //                         id: edge.id,
    //                         new_source_id,
    //                         new_target_id,
    //                     })
    //                 }
    //             }
    //         }
    //     }
    //
    //     for replacement_edge in replacement_edges {
    //         let mut exist_query;
    //         let mut update_query;
    //         let mut placeholders: Vec<i32> = vec![];
    //         if replacement_edge.new_source_id.is_some() && replacement_edge.new_target_id.is_some()
    //         {
    //             exist_query = "select id from edges where source_id = ?1 and target_id = ?2;";
    //             update_query = "update edges set source_id = ?1 AND target_id = ?2 where id = ?3";
    //             placeholders.push(replacement_edge.new_source_id.unwrap());
    //             placeholders.push(replacement_edge.new_target_id.unwrap());
    //         } else if replacement_edge.new_source_id.is_some() {
    //             exist_query = "select id from edges where source_id = ?1 and target_id is null;";
    //             update_query = "update edges set source_id = ?1 where id = ?2";
    //             placeholders.push(replacement_edge.new_source_id.unwrap());
    //         } else if replacement_edge.new_target_id.is_some() {
    //             exist_query = "select id from edges where source_id is null and target_id = ?1;";
    //             update_query = "update edges set target_id = ?1 where id = ?2";
    //             placeholders.push(replacement_edge.new_target_id.unwrap());
    //         } else {
    //             continue;
    //         }
    //         println!("{exist_query:?} {update_query} {placeholders:?}");
    //
    //         let mut stmt = conn.prepare_cached(exist_query).unwrap();
    //         if !stmt.exists(params_from_iter(&placeholders)).unwrap() {
    //             placeholders.push(replacement_edge.id);
    //             println!("updating {exist_query:?} {update_query} {placeholders:?}");
    //             let mut stmt = conn.prepare_cached(update_query).unwrap();
    //             stmt.execute(params_from_iter(&placeholders)).unwrap();
    //         } else {
    //             println!("edge exists");
    //         }
    //     }
    //     for new_edge in new_edges {
    //         Edge::create(conn, new_edge.0, Some(new_edge.1));
    //     }
    //
    //     let block_keys = blocks
    //         .keys()
    //         .map(|k| format!("{k}"))
    //         .collect::<Vec<_>>()
    //         .join(", ");
    //     let mut stmt = conn
    //         .prepare_cached("DELETE from block where id IN (?1)")
    //         .unwrap();
    //     stmt.execute([block_keys]).unwrap();
    // }
    //
    // pub fn sequence(
    //     conn: &mut Connection,
    //     collection_name: &str,
    //     sample_name: Option<&String>,
    //     path_name: &str,
    //     path_index: i32,
    // ) -> String {
    //     struct SequenceBlock {
    //         sequence: String,
    //         strand: String,
    //     }
    //     let mut query;
    //     let mut placeholders: Vec<rusqlite::types::Value> =
    //         vec![collection_name.to_string().into()];
    //
    //     if sample_name.is_some() {
    //         query = "WITH RECURSIVE traverse(block_id, block_sequence, block_start, block_end, block_strand, depth) AS (
    //       SELECT edges.source_id, substr(seq.sequence, block.start + 1, block.end - block.start), block.start, block.end, block.strand, 0 as depth FROM path left join block on (path.id = block.path_id) left join sequence seq on (seq.hash = block.sequence_hash) left join edges on (block.id = edges.source_id or block.id = edges.target_id) WHERE path.collection_name = ?1 AND path.sample_name = ?2 AND path.name = ?3 AND path.path_index = ?4 and edges.target_id is null
    //       UNION
    //       SELECT e2.source_id, substr(seq2.sequence, b2.start + 1, b2.end - b2.start), b2.start, b2.end, b2.strand, depth + 1 FROM edges e2 left join block b2 on (b2.id = e2.source_id) left join sequence seq2 on (seq2.hash = b2.sequence_hash) JOIN traverse t2 ON e2.target_id = t2.block_id
    //     ) SELECT block_sequence as sequence, block_strand as strand FROM traverse order by depth desc;";
    //         placeholders.push(sample_name.unwrap().clone().into());
    //     } else {
    //         query = "WITH RECURSIVE traverse(block_id, block_sequence, block_start, block_end, block_strand, depth) AS (
    //       SELECT edges.source_id, substr(seq.sequence, block.start + 1, block.end - block.start), block.start, block.end, block.strand, 0 as depth FROM path left join block on (path.id = block.path_id) left join sequence seq on (seq.hash = block.sequence_hash) left join edges on (block.id = edges.source_id or block.id = edges.target_id) WHERE path.collection_name = ?1 AND path.sample_name is null AND path.name = ?2 AND path.path_index = ?3 and edges.target_id is null
    //       UNION
    //       SELECT e2.source_id, substr(seq2.sequence, b2.start + 1, b2.end - b2.start), b2.start, b2.end, b2.strand, depth + 1 FROM edges e2 left join block b2 on (b2.id = e2.source_id) left join sequence seq2 on (seq2.hash = b2.sequence_hash) JOIN traverse t2 ON e2.target_id = t2.block_id
    //     ) SELECT block_sequence as sequence, block_strand as strand FROM traverse order by depth desc;"
    //     }
    //     placeholders.push(path_name.to_string().into());
    //     placeholders.push(path_index.into());
    //     let mut stmt = conn.prepare(query).unwrap();
    //     let mut blocks = stmt
    //         .query_map(params_from_iter(placeholders), |row| {
    //             Ok(SequenceBlock {
    //                 sequence: row.get(0)?,
    //                 strand: row.get(1)?,
    //             })
    //         })
    //         .unwrap();
    //     let mut sequence = "".to_string();
    //     for block in blocks {
    //         sequence.push_str(&block.unwrap().sequence);
    //     }
    //     sequence
    // }
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
            Ok(path) => path,
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

    pub fn clone(conn: &mut Connection, source_id: i32, target_id: i32) {
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, sequence_hash, start, end, strand from block where block_group_id = ?1",
            )
            .unwrap();
        let mut block_map: HashMap<i32, i32> = HashMap::new();
        let mut it = stmt.query([source_id]).unwrap();
        let mut row = it.next().unwrap();
        while row.is_some() {
            let block = row.unwrap();
            let block_id: i32 = block.get(0).unwrap();
            let hash: String = block.get(1).unwrap();
            let start = block.get(2).unwrap();
            let end = block.get(3).unwrap();
            let strand: String = block.get(4).unwrap();
            let new_block = Block::create(conn, &hash, target_id, start, end, &strand);
            block_map.insert(block_id, new_block.id);
            row = it.next().unwrap();
        }

        // todo: figure out rusqlite's rarray
        let mut stmt = conn
            .prepare_cached("SELECT source_id, target_id from edges where source_id IN (?1)")
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
            let source_id: i32 = edge.get(0).unwrap();
            let target_id: Option<i32> = edge.get(1).unwrap();
            Edge::create(
                conn,
                *block_map.get(&source_id).unwrap_or(&source_id),
                target_id,
                0,
                0,
            );
            row = it.next().unwrap();
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

        // clone parent blocks/edges
        BlockGroup::clone(conn, bg_id, new_bg_id.id);

        new_bg_id.id
    }

    #[allow(clippy::ptr_arg)]
    #[allow(clippy::too_many_arguments)]
    pub fn insert_change(
        conn: &mut Connection,
        block_group_id: i32,
        start: i32,
        end: i32,
        new_block_id: i32,
        chromosome_index: i32,
        phased: i32,
    ) {
        println!("change is {block_group_id} {start} {end} {new_block_id}");
        // todo:
        // 1. get blocks where start-> end overlap
        // 2. split old blocks at boundry points, make new block for left/right side
        // 3. make new block for sequence we are changing
        // 4. update edges
        // add support for deletion
        // cases to check:
        //  change that is the size of a block
        //  change that goes over multiple blocks
        //  change that hits just start/end boundry, e.g. block is 1,5 and change is 3,5 or 1,3.
        //  change that deletes block boundry
        // https://stackoverflow.com/questions/3269434/whats-the-most-efficient-way-to-test-if-two-ranges-overlap
        let mut stmt = conn.prepare_cached("select b.id, b.sequence_hash, b.block_group_id, b.start, b.end, b.strand, e.id as edge_id, e.source_id, e.target_id, e.chromosome_index, e.phased from block b left join edges e on (e.source_id = b.id or e.target_id = b.id) where b.block_group_id = ?1 AND b.start <= ?3 AND ?2 <= b.end AND b.id != ?4 AND e.chromosome_index = 0;").unwrap();
        let mut block_edges: HashMap<i32, Vec<Edge>> = HashMap::new();
        let mut blocks: HashMap<i32, Block> = HashMap::new();
        let mut it = stmt
            .query([block_group_id, start, end, new_block_id])
            .unwrap();
        let mut row = it.next().unwrap();
        while row.is_some() {
            let entry = row.unwrap();
            let block_id = entry.get(0).unwrap();
            let edge_id: Option<i32> = entry.get(6).unwrap();
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
            if edge_id.is_some() {
                if let Vacant(e) = block_edges.entry(block_id) {
                    e.insert(vec![Edge {
                        id: edge_id.unwrap(),
                        source_id: entry.get(7).unwrap(),
                        target_id: entry.get(8).unwrap(),
                        chromosome_index: entry.get(9).unwrap(),
                        phased: entry.get(10).unwrap(),
                    }]);
                } else {
                    block_edges.get_mut(&block_id).unwrap().push(Edge {
                        id: entry.get(6).unwrap(),
                        source_id: entry.get(7).unwrap(),
                        target_id: entry.get(8).unwrap(),
                        chromosome_index: entry.get(9).unwrap(),
                        phased: entry.get(10).unwrap(),
                    });
                }
            } else {
                println!("empty eid {row:?}");
            }
            row = it.next().unwrap();
        }

        #[derive(Debug)]
        struct ReplacementEdge {
            id: i32,
            new_source_id: Option<i32>,
            new_target_id: Option<i32>,
        }
        let mut replacement_edges: Vec<ReplacementEdge> = vec![];
        let mut new_edges: Vec<(i32, i32)> = vec![];

        for (block_id, block) in &blocks {
            let contains_start = block.start <= start && start < block.end;
            let contains_end = block.start <= end && end < block.end;

            if contains_start && contains_end {
                // our range is fully contained w/in the block
                //      |----block------|
                //        |----range---|
                let left_block = Block::create(
                    conn,
                    &block.sequence_hash,
                    block_group_id,
                    block.start,
                    start,
                    &block.strand,
                );
                let right_block = Block::create(
                    conn,
                    &block.sequence_hash,
                    block_group_id,
                    end,
                    block.end,
                    &block.strand,
                );
                println!("lb {left_block:?} {right_block:?}");
                new_edges.push((left_block.id, new_block_id));
                new_edges.push((new_block_id, right_block.id));
                // what stuff went to this block?
                for edges in block_edges.get(block_id) {
                    for edge in edges {
                        println!("block {block_id} on edge {edge:?}");
                        let mut new_source_id = None;
                        let mut new_target_id = None;
                        if edge.source_id == *block_id {
                            new_source_id = Some(right_block.id);
                        }
                        if edge.target_id.is_some() && edge.target_id.unwrap() == *block_id {
                            new_target_id = Some(left_block.id);
                        }
                        replacement_edges.push(ReplacementEdge {
                            id: edge.id,
                            new_source_id,
                            new_target_id,
                        });
                        println!("new res {replacement_edges:?}");
                    }
                }
            } else if contains_start {
                // our range is overlapping the end of the block
                // |----block---|
                //        |----range---|
                let left_block = Block::create(
                    conn,
                    &block.sequence_hash,
                    block_group_id,
                    block.start,
                    start,
                    &block.strand,
                );
                new_edges.push((left_block.id, new_block_id));
                // what stuff went to this block?
                for edges in block_edges.get(block_id) {
                    for edge in edges {
                        let mut new_source_id = None;
                        let mut new_target_id = None;
                        if edge.source_id == *block_id {
                            new_source_id = Some(new_block_id);
                        }
                        if edge.target_id.is_some() && edge.target_id.unwrap() == *block_id {
                            new_target_id = Some(left_block.id);
                        }
                        replacement_edges.push(ReplacementEdge {
                            id: edge.id,
                            new_source_id,
                            new_target_id,
                        });
                    }
                }
            } else if contains_end {
                // our range is overlapping the beginning of the block
                //              |----block---|
                //        |----range---|
                let right_block = Block::create(
                    conn,
                    &block.sequence_hash,
                    block_group_id,
                    end,
                    block.end,
                    &block.strand,
                );
                // what stuff went to this block?
                new_edges.push((new_block_id, right_block.id));
                for edges in block_edges.get(block_id) {
                    for edge in edges {
                        let mut new_source_id = None;
                        let mut new_target_id = None;
                        if edge.source_id == *block_id {
                            new_source_id = Some(right_block.id);
                        }
                        if edge.target_id.is_some() && edge.target_id.unwrap() == *block_id {
                            new_target_id = Some(new_block_id);
                        }
                        replacement_edges.push(ReplacementEdge {
                            id: edge.id,
                            new_source_id,
                            new_target_id,
                        })
                    }
                }
            } else {
                // our range is the whole block, get rid of it
                //          |--block---|
                //        |-----range------|
                // what stuff went to this block?
                for edges in block_edges.get(block_id) {
                    for edge in edges {
                        let mut new_source_id = None;
                        let mut new_target_id = None;
                        if edge.source_id == *block_id {
                            new_source_id = Some(new_block_id);
                        }
                        if edge.target_id.is_some() && edge.target_id.unwrap() == *block_id {
                            new_target_id = Some(new_block_id);
                        }
                        replacement_edges.push(ReplacementEdge {
                            id: edge.id,
                            new_source_id,
                            new_target_id,
                        })
                    }
                }
            }
        }

        for replacement_edge in replacement_edges {
            let mut exist_query;
            let mut update_query;
            let mut placeholders: Vec<i32> = vec![];
            if replacement_edge.new_source_id.is_some() && replacement_edge.new_target_id.is_some()
            {
                exist_query = "select id from edges where source_id = ?1 and target_id = ?2;";
                update_query = "update edges set source_id = ?1 AND target_id = ?2 where id = ?3";
                placeholders.push(replacement_edge.new_source_id.unwrap());
                placeholders.push(replacement_edge.new_target_id.unwrap());
            } else if replacement_edge.new_source_id.is_some() {
                exist_query = "select id from edges where source_id = ?1 and target_id is null;";
                update_query = "update edges set source_id = ?1 where id = ?2";
                placeholders.push(replacement_edge.new_source_id.unwrap());
            } else if replacement_edge.new_target_id.is_some() {
                exist_query = "select id from edges where source_id is null and target_id = ?1;";
                update_query = "update edges set target_id = ?1 where id = ?2";
                placeholders.push(replacement_edge.new_target_id.unwrap());
            } else {
                continue;
            }
            println!("{exist_query:?} {update_query} {placeholders:?}");

            let mut stmt = conn.prepare_cached(exist_query).unwrap();
            if !stmt.exists(params_from_iter(&placeholders)).unwrap() {
                placeholders.push(replacement_edge.id);
                println!("updating {exist_query:?} {update_query} {placeholders:?}");
                let mut stmt = conn.prepare_cached(update_query).unwrap();
                stmt.execute(params_from_iter(&placeholders)).unwrap();
            } else {
                println!("edge exists");
            }
        }
        for new_edge in new_edges {
            Edge::create(conn, new_edge.0, Some(new_edge.1), chromosome_index, phased);
        }

        let block_keys = blocks
            .keys()
            .map(|k| format!("{k}"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut stmt = conn
            .prepare_cached("DELETE from block where id IN (?1)")
            .unwrap();
        stmt.execute([block_keys]).unwrap();
    }

    // TODO: move this to path, doesn't belong in block group
    pub fn sequence(
        conn: &mut Connection,
        collection_name: &str,
        sample_name: Option<&String>,
        block_group_name: &str,
    ) -> String {
        struct SequenceBlock {
            sequence: String,
            strand: String,
        }
        let mut query;
        let mut placeholders: Vec<rusqlite::types::Value> =
            vec![collection_name.to_string().into()];

        if sample_name.is_some() {
            query = "WITH RECURSIVE traverse(block_id, block_sequence, block_start, block_end, block_strand, depth) AS (
          SELECT edges.source_id, substr(seq.sequence, block.start + 1, block.end - block.start), block.start, block.end, block.strand, 0 as depth FROM block_group left join block on (block_group.id = block.block_group_id) left join sequence seq on (seq.hash = block.sequence_hash) left join edges on (block.id = edges.source_id or block.id = edges.target_id) WHERE block_group.collection_name = ?1 AND block_group.sample_name = ?2 AND block_group.name = ?3 and edges.target_id is null
          UNION
          SELECT e2.source_id, substr(seq2.sequence, b2.start + 1, b2.end - b2.start), b2.start, b2.end, b2.strand, depth + 1 FROM edges e2 left join block b2 on (b2.id = e2.source_id) left join sequence seq2 on (seq2.hash = b2.sequence_hash) JOIN traverse t2 ON e2.target_id = t2.block_id
        ) SELECT block_sequence as sequence, block_strand as strand FROM traverse order by depth desc;";
            placeholders.push(sample_name.unwrap().clone().into());
        } else {
            query = "WITH RECURSIVE traverse(block_id, block_sequence, block_start, block_end, block_strand, depth) AS (
          SELECT edges.source_id, substr(seq.sequence, block.start + 1, block.end - block.start), block.start, block.end, block.strand, 0 as depth FROM block_group left join block on (block_group.id = block.block_group_id) left join sequence seq on (seq.hash = block.sequence_hash) left join edges on (block.id = edges.source_id or block.id = edges.target_id) WHERE block_group.collection_name = ?1 AND block_group.sample_name is null AND block_group.name = ?2 and edges.target_id is null
          UNION
          SELECT e2.source_id, substr(seq2.sequence, b2.start + 1, b2.end - b2.start), b2.start, b2.end, b2.strand, depth + 1 FROM edges e2 left join block b2 on (b2.id = e2.source_id) left join sequence seq2 on (seq2.hash = b2.sequence_hash) JOIN traverse t2 ON e2.target_id = t2.block_id
        ) SELECT block_sequence as sequence, block_strand as strand FROM traverse order by depth desc;"
        }
        placeholders.push(block_group_name.to_string().into());
        let mut stmt = conn.prepare(query).unwrap();
        let mut blocks = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(SequenceBlock {
                    sequence: row.get(0)?,
                    strand: row.get(1)?,
                })
            })
            .unwrap();
        let mut sequence = "".to_string();
        for block in blocks {
            sequence.push_str(&block.unwrap().sequence);
        }
        sequence
    }
}
