use std::collections::{HashMap, HashSet};
use std::fmt::*;

use noodles::vcf::variant::record::info::field::value::array::Values;
use petgraph::graphmap::DiGraphMap;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use sha2::{Digest, Sha256};

use crate::{calculate_hash, models};

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
pub struct Sequence {
    pub hash: String,
    pub sequence_type: String,
    pub sequence: String,
    pub length: i32,
}

impl Sequence {
    pub fn create(
        conn: &mut Connection,
        sequence_type: String,
        sequence: &String,
        store: bool,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(&sequence_type);
        hasher.update(sequence);
        let hash = format!("{:x}", hasher.finalize());
        let mut obj_hash: String = match conn.query_row(
            "SELECT hash from sequence where hash = ?1;",
            [hash.clone()],
            |row| row.get(0),
        ) {
            Ok(res) => res,
            Err(rusqlite::Error::QueryReturnedNoRows) => "".to_string(),
            Err(_e) => {
                panic!("something bad happened querying the database")
            }
        };
        if obj_hash.is_empty() {
            let mut stmt = conn.prepare("INSERT INTO sequence (hash, sequence_type, sequence, length) VALUES (?1, ?2, ?3, ?4) RETURNING (hash);").unwrap();
            let mut rows = stmt
                .query_map(
                    (
                        hash,
                        sequence_type,
                        if store { sequence } else { "" },
                        sequence.len(),
                    ),
                    |row| row.get(0),
                )
                .unwrap();
            obj_hash = rows.next().unwrap().unwrap();
        }
        obj_hash
    }
}

#[derive(Debug)]
pub struct Block {
    pub id: i32,
    pub sequence_hash: String,
    pub block_group_id: i32,
    pub start: i32,
    pub end: i32,
    pub strand: String,
}

impl Block {
    pub fn create(
        conn: &Connection,
        hash: &String,
        block_group_id: i32,
        start: i32,
        end: i32,
        strand: &String,
    ) -> Block {
        let mut stmt = conn
            .prepare_cached("INSERT INTO block (sequence_hash, block_group_id, start, end, strand) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING *")
            .unwrap();
        match stmt.query_row((hash, block_group_id, start, end, strand), |row| {
            Ok(Block {
                id: row.get(0)?,
                sequence_hash: row.get(1)?,
                block_group_id: row.get(2)?,
                start: row.get(3)?,
                end: row.get(4)?,
                strand: row.get(5)?,
            })
        }) {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    // println!("{err:?} {details:?}");
                    Block {
                        id: conn
                            .query_row(
                                "select id from block where sequence_hash = ?1 AND block_group_id = ?2 AND start = ?3 AND end = ?4 AND strand = ?5;",
                                (hash, block_group_id, start, end, strand),
                                |row| row.get(0),
                            )
                            .unwrap(),
                        sequence_hash: hash.clone(),
                        block_group_id,
                        start,
                        end,
                        strand: strand.clone(),
                    }
                } else {
                    panic!("something bad happened querying the database")
                }
            }
            Err(_e) => {
                panic!("failure in making block {_e}")
            }
        }
    }
}

#[derive(Debug)]
pub struct Edge {
    pub id: i32,
    pub source_id: i32,
    pub target_id: Option<i32>,
    pub origin: i32,
    pub chromosome_index: i32,
    pub phased: i32,
}

impl Edge {
    pub fn create(
        conn: &Connection,
        source_id: i32,
        target_id: Option<i32>,
        origin: i32,
        chromosome_index: i32,
        phased: i32,
    ) -> Edge {
        let mut query;
        let mut id_query;
        let mut placeholders: Vec<Value> = vec![];
        if target_id.is_some() {
            query = "INSERT INTO edges (source_id, target_id, origin, chromosome_index, phased) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING *";
            id_query = "select id from edges where source_id = ?1 and target_id = ?2 and chromosome_index = ?3 and phased = ?4";
            placeholders.push(Value::from(source_id));
            placeholders.push(target_id.unwrap().into());
            placeholders.push(origin.into());
            placeholders.push(chromosome_index.into());
            placeholders.push(phased.into());
        } else {
            id_query = "select id from edges where source_id = ?1 and target_id is null and chromosome_index = ?2 and phased = ?3";
            query = "INSERT INTO edges (source_id, origin, chromosome_index, phased) VALUES (?1, ?2, ?3, ?4) RETURNING *";
            placeholders.push(source_id.into());
            placeholders.push(origin.into());
            placeholders.push(chromosome_index.into());
            placeholders.push(phased.into());
        }
        let mut stmt = conn.prepare(query).unwrap();
        match stmt.query_row(params_from_iter(&placeholders), |row| {
            Ok(Edge {
                id: row.get(0)?,
                source_id: row.get(1)?,
                target_id: row.get(2)?,
                origin: row.get(3)?,
                chromosome_index: row.get(4)?,
                phased: row.get(5)?,
            })
        }) {
            Ok(edge) => edge,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    Edge {
                        id: conn
                            .query_row(id_query, params_from_iter(&placeholders), |row| row.get(0))
                            .unwrap(),
                        source_id,
                        target_id,
                        origin,
                        chromosome_index,
                        phased,
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
}

#[derive(Debug)]
pub struct Path {
    pub id: i32,
    pub name: String,
    pub block_group_id: i32,
    pub edges: Vec<i32>,
}

impl Path {
    pub fn create(conn: &mut Connection, name: &str, block_group_id: i32, edges: Vec<i32>) -> Path {
        let query =
            "INSERT INTO path (name, block_group_id, edges) VALUES (?1, ?2, ?3) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let edge_str = edges
            .iter()
            .map(|k| format!("{k}"))
            .collect::<Vec<_>>()
            .join(",");
        let mut rows = stmt
            .query_map((name, block_group_id, &edge_str), |row| {
                Ok(Path {
                    id: row.get(0)?,
                    name: name.to_string(),
                    block_group_id,
                    edges: edges.clone(),
                })
            })
            .unwrap();
        rows.next().unwrap().unwrap()
    }

    pub fn get(conn: &mut Connection, path_id: i32) -> Path {
        let query = "SELECT id, block_group_id, name, edges from path where id = ?1;";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map((path_id,), |row| {
                let mut edge_str: String = row.get(3).unwrap();
                Ok(Path {
                    id: row.get(0)?,
                    block_group_id: row.get(1)?,
                    name: row.get(2)?,
                    edges: edge_str
                        .split(',')
                        .map(|v| v.parse::<i32>().unwrap())
                        .collect::<Vec<i32>>(),
                })
            })
            .unwrap();
        rows.next().unwrap().unwrap()
    }

    pub fn edges_to_graph(conn: &mut Connection, edges: &Vec<i32>) -> DiGraphMap<(u32), ()> {
        let edge_str = (*edges)
            .iter()
            .map(|v| format!("{v}"))
            .collect::<Vec<_>>()
            .join(",");
        let query = format!("SELECT source_id, target_id from edges where id IN ({edge_str});");
        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt
            .query_map([], |row| {
                let source_id: u32 = row.get(0).unwrap();
                let target_id: u32 = row.get(1).unwrap();
                Ok((source_id, target_id))
            })
            .unwrap();
        let mut graph = DiGraphMap::new();
        for edge in rows {
            let (source, target) = edge.unwrap();
            graph.add_edge(source, target, ());
        }
        graph
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
            .prepare_cached(
                "SELECT source_id, target_id, origin from edges where source_id IN (?1)",
            )
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
            let origin: i32 = edge.get(2).unwrap();
            if (target_id.is_some()) {
                let target_id = target_id.unwrap();
                Edge::create(
                    conn,
                    *block_map.get(&source_id).unwrap_or(&source_id),
                    Some(*block_map.get(&target_id).unwrap_or(&target_id)),
                    origin,
                    0,
                    0,
                );
            } else {
                Edge::create(
                    conn,
                    *block_map.get(&source_id).unwrap_or(&source_id),
                    None,
                    origin,
                    0,
                    0,
                );
            }

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
        path_id: i32,
        start: i32,
        end: i32,
        new_block_id: i32,
        chromosome_index: i32,
        phased: i32,
    ) {
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
        let graph = Path::edges_to_graph(conn, &path.edges);
        let query = format!("SELECT id, sequence_hash, block_group_id, start, end, strand from block where id in ({block_ids})", block_ids = graph.nodes().map(|k| format!("{k}")).collect::<Vec<_>>().join(","));
        let mut stmt = conn.prepare(&query).unwrap();
        let mut blocks: Vec<Block> = vec![];
        let mut it = stmt.query([]).unwrap();
        let mut row = it.next().unwrap();
        while row.is_some() {
            let entry = row.unwrap();
            blocks.push(Block {
                id: entry.get(0).unwrap(),
                sequence_hash: entry.get(1).unwrap(),
                block_group_id: entry.get(2).unwrap(),
                start: entry.get(3).unwrap(),
                end: entry.get(4).unwrap(),
                strand: entry.get(5).unwrap(),
            });
            row = it.next().unwrap();
        }
        println!("change is {path:?} {graph:?} {blocks:?}");

        // This identifies blocks that have overlap with the change.
        // TODO: we need to be able to specify a change on a specific chromosome, so using chromosome_index
        // let overlapping_blocks_query = "WITH RECURSIVE traverse(block_id, linked_edge_id, direction, block_start, block_end, depth, global_start, global_end) AS (
        //   SELECT edges.source_id, edges.id, (CASE WHEN edges.source_id = block.id THEN 'out' ELSE 'in' END) as direction, block.start, block.end, 0 as depth, 0 as global_start, block.end - block.start as global_end FROM block_group left join block on (block_group.id = block.block_group_id) left join edges on (block.id = edges.source_id) WHERE block.block_group_id = ?1 AND block.id != ?4 AND edges.origin = 1 AND edges.chromosome_index = ?5
        //   UNION ALL
        //   SELECT e2.target_id, e2.id, (CASE WHEN e2.source_id = b2.id THEN 'out' ELSE 'in' END) as direction, b2.start, b2.end, t2.depth + 1, t2.global_end, t2.global_end + b2.end - b2.start FROM edges e2 left join block b2 on (b2.id = e2.target_id) JOIN traverse t2 ON e2.source_id = t2.block_id where e2.target_id is not null and global_start <= ?3 order by depth desc
        // ) SELECT block_id, linked_edge_id, direction, global_start, global_end FROM traverse where global_start <= ?3 AND ?2 < global_end;";
        // let mut stmt = conn.prepare_cached(overlapping_blocks_query).unwrap();
        // let mut it = stmt
        //     .query((block_group_id, start, end, new_block_id, ref_chromosome_index))
        //     .unwrap();
        // let mut row = it.next().unwrap();
        // let mut impacted_blocks : HashMap<i32, HashMap<i32, (i32, i32, String)>> = HashMap::new();
        // while row.is_some() {
        //     let entry = row.unwrap();
        //     let block_id : i32 = entry.get(0).unwrap();
        //     impacted_blocks.entry(block_id).or_insert_with(|| HashMap::new()).insert(entry.get(1).unwrap(), (entry.get(3).unwrap(), entry.get(4).unwrap(), entry.get(2).unwrap()));
        //     row = it.next().unwrap();
        // }
        // println!("ib {impacted_blocks:?}");
        //
        //
        // // TODO: this wasn't working with placeholders
        // let impacted_block_ids = impacted_blocks
        //     .keys()
        //     .map(|k| format!("{k}"))
        //     .collect::<Vec<_>>()
        //     .join(", ");
        // let mut stmt = conn.prepare(&format!("select b.id, b.sequence_hash, b.block_group_id, b.start, b.end, b.strand, e.id as edge_id, e.source_id, e.target_id, e.chromosome_index, e.phased from block b left join edges e on (e.source_id = b.id or e.target_id = b.id) where b.id IN ({impacted_block_ids})")).unwrap();
        // let mut it = stmt.query([]).unwrap();
        //
        // let mut block_edges: HashMap<i32, Vec<Edge>> = HashMap::new();
        // let mut blocks: HashMap<i32, Block> = HashMap::new();
        //
        // let mut row = it.next().unwrap();
        // while row.is_some() {
        //     let entry = row.unwrap();
        //     let block_id = entry.get(0).unwrap();
        //     let edge_id: i32 = entry.get(6).unwrap();
        //
        //     let edge_chromosome_index = entry.get(9).unwrap();
        //     let known_edges = impacted_blocks.get(&block_id).unwrap().get(&edge_id);
        //     println!("ke {start} {end} {chromosome_index} {edge_chromosome_index} {block_id} {new_block_id} {known_edges:?}");
        //     // check if any of the edges match our current operation
        //     if known_edges.is_some() {
        //         let (global_start, global_end, direction) = known_edges.unwrap();
        //         let source_id : i32 = entry.get(7).unwrap();
        //         let target_id : i32 = entry.get(8).unwrap();
        //         if (direction == "out" && source_id == block_id) {
        //
        //         }
        //         else if (direction == "in" && target_id == block_id) {
        //
        //         }
        //         else {
        //             row = it.next().unwrap();
        //             continue;
        //         }
        //
        //         // we're going to have to update this connection
        //
        //         if let Vacant(e) = block_edges.entry(block_id) {
        //             e.insert(vec![Edge {
        //                 id: edge_id,
        //                 source_id: entry.get(7).unwrap(),
        //                 target_id: entry.get(8).unwrap(),
        //                 origin: 0,
        //                 chromosome_index: edge_chromosome_index,
        //                 phased: entry.get(10).unwrap(),
        //             }]);
        //         } else {
        //             block_edges.get_mut(&block_id).unwrap().push(Edge {
        //                 id: entry.get(6).unwrap(),
        //                 source_id: entry.get(7).unwrap(),
        //                 target_id: entry.get(8).unwrap(),
        //                 origin: 0,
        //                 chromosome_index: edge_chromosome_index,
        //                 phased: entry.get(10).unwrap(),
        //             });
        //         }
        //
        //         blocks.insert(
        //             block_id,
        //             Block {
        //                 id: block_id,
        //                 sequence_hash: entry.get(1).unwrap(),
        //                 block_group_id: entry.get(2).unwrap(),
        //                 start: entry.get(3).unwrap(),
        //                 end: entry.get(4).unwrap(),
        //                 strand: entry.get(5).unwrap(),
        //             },
        //         );
        //     }
        //     row = it.next().unwrap();
        // }
        //
        // #[derive(Debug)]
        // struct ReplacementEdge {
        //     id: i32,
        //     new_source_id: Option<i32>,
        //     new_target_id: Option<i32>,
        //     origin: i32,
        // }
        // let mut replacement_edges: Vec<ReplacementEdge> = vec![];
        // let mut new_edges: Vec<(i32, i32)> = vec![];
        //
        // for (block_id, block) in &blocks {
        //     let contains_start = block.start <= start && start < block.end;
        //     let contains_end = block.start <= end && end < block.end;
        //
        //     if contains_start && contains_end {
        //         // our range is fully contained w/in the block
        //         //      |----block------|
        //         //        |----range---|
        //         let left_block = Block::create(
        //             conn,
        //             &block.sequence_hash,
        //             block_group_id,
        //             block.start,
        //             start,
        //             &block.strand,
        //         );
        //         let right_block = Block::create(
        //             conn,
        //             &block.sequence_hash,
        //             block_group_id,
        //             end,
        //             block.end,
        //             &block.strand,
        //         );
        //         println!("lb {left_block:?} {right_block:?}");
        //         new_edges.push((left_block.id, new_block_id));
        //         new_edges.push((new_block_id, right_block.id));
        //         // what stuff went to this block?
        //         for edges in block_edges.get(block_id) {
        //             for edge in edges {
        //                 println!("block {block_id} on edge {edge:?}");
        //                 let mut new_source_id = None;
        //                 let mut new_target_id = None;
        //                 if edge.source_id == *block_id {
        //                     new_source_id = Some(right_block.id);
        //                 }
        //                 if edge.target_id.is_some() && edge.target_id.unwrap() == *block_id {
        //                     new_target_id = Some(left_block.id);
        //                 }
        //                 replacement_edges.push(ReplacementEdge {
        //                     id: edge.id,
        //                     new_source_id,
        //                     new_target_id,
        //                     origin: 0,
        //                 });
        //                 println!("new res {replacement_edges:?}");
        //             }
        //         }
        //     } else if contains_start {
        //         // our range is overlapping the end of the block
        //         // |----block---|
        //         //        |----range---|
        //         let left_block = Block::create(
        //             conn,
        //             &block.sequence_hash,
        //             block_group_id,
        //             block.start,
        //             start,
        //             &block.strand,
        //         );
        //         new_edges.push((left_block.id, new_block_id));
        //         // what stuff went to this block?
        //         for edges in block_edges.get(block_id) {
        //             for edge in edges {
        //                 let mut new_source_id = None;
        //                 let mut new_target_id = None;
        //                 if edge.source_id == *block_id {
        //                     new_source_id = Some(new_block_id);
        //                 }
        //                 if edge.target_id.is_some() && edge.target_id.unwrap() == *block_id {
        //                     new_target_id = Some(left_block.id);
        //                 }
        //                 replacement_edges.push(ReplacementEdge {
        //                     id: edge.id,
        //                     new_source_id,
        //                     new_target_id,
        //                     origin: 0,
        //                 });
        //             }
        //         }
        //     } else if contains_end {
        //         // our range is overlapping the beginning of the block
        //         //              |----block---|
        //         //        |----range---|
        //         let right_block = Block::create(
        //             conn,
        //             &block.sequence_hash,
        //             block_group_id,
        //             end,
        //             block.end,
        //             &block.strand,
        //         );
        //         // what stuff went to this block?
        //         new_edges.push((new_block_id, right_block.id));
        //         for edges in block_edges.get(block_id) {
        //             for edge in edges {
        //                 let mut new_source_id = None;
        //                 let mut new_target_id = None;
        //                 if edge.source_id == *block_id {
        //                     new_source_id = Some(right_block.id);
        //                 }
        //                 if edge.target_id.is_some() && edge.target_id.unwrap() == *block_id {
        //                     new_target_id = Some(new_block_id);
        //                 }
        //                 replacement_edges.push(ReplacementEdge {
        //                     id: edge.id,
        //                     new_source_id,
        //                     new_target_id,
        //                     origin: 0,
        //                 })
        //             }
        //         }
        //     } else {
        //         // our range is the whole block, get rid of it
        //         //          |--block---|
        //         //        |-----range------|
        //         // what stuff went to this block?
        //         for edges in block_edges.get(block_id) {
        //             for edge in edges {
        //                 let mut new_source_id = None;
        //                 let mut new_target_id = None;
        //                 if edge.source_id == *block_id {
        //                     new_source_id = Some(new_block_id);
        //                 }
        //                 if edge.target_id.is_some() && edge.target_id.unwrap() == *block_id {
        //                     new_target_id = Some(new_block_id);
        //                 }
        //                 replacement_edges.push(ReplacementEdge {
        //                     id: edge.id,
        //                     new_source_id,
        //                     new_target_id,
        //                     origin: 0,
        //                 })
        //             }
        //         }
        //     }
        // }
        //
        // for replacement_edge in replacement_edges {
        //     let mut update_query;
        //     let mut placeholders: Vec<i32> = vec![];
        //     if replacement_edge.new_source_id.is_some() && replacement_edge.new_target_id.is_some()
        //     {
        //         update_query = "update edges set source_id = ?1 AND target_id = ?2 where id = ?3";
        //         placeholders.push(replacement_edge.new_source_id.unwrap());
        //         placeholders.push(replacement_edge.new_target_id.unwrap());
        //     } else if replacement_edge.new_source_id.is_some() {
        //         update_query = "update edges set source_id = ?1 where id = ?2";
        //         placeholders.push(replacement_edge.new_source_id.unwrap());
        //     } else if replacement_edge.new_target_id.is_some() {
        //         update_query = "update edges set target_id = ?1 where id = ?2";
        //         placeholders.push(replacement_edge.new_target_id.unwrap());
        //     } else {
        //         continue;
        //     }
        //     placeholders.push(replacement_edge.id);
        //     println!("{update_query} {placeholders:?}");
        //
        //     let mut stmt = conn.prepare_cached(update_query).unwrap();
        //     stmt.execute(params_from_iter(&placeholders)).unwrap_or_else(|_| 0);
        // }
        // for new_edge in new_edges {
        //     // TODO: fix the origin thing
        //     Edge::create(conn, new_edge.0, Some(new_edge.1), 0, chromosome_index, phased);
        // }
        //
        // let block_keys = blocks
        //     .keys()
        //     .map(|k| format!("{k}"))
        //     .collect::<Vec<_>>()
        //     .join(", ");
        // let mut stmt = conn
        //     .prepare_cached("DELETE from block where block.id IN (select b.id from block b left join edges e on (e.source_id = b.id or e.target_id = b.id) where b.id in (?1) and e.id is null)")
        //     .unwrap();
        // stmt.execute([block_keys]).unwrap();
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
          SELECT e2.source_id, substr(seq2.sequence, b2.start + 1, b2.end - b2.start), b2.start, b2.end, b2.strand, depth + 1 FROM edges e2 left join block b2 on (b2.id = e2.source_id) left join sequence seq2 on (seq2.hash = b2.sequence_hash) JOIN traverse t2 ON e2.target_id = t2.block_id order by depth desc
        ) SELECT block_sequence as sequence, block_strand as strand FROM traverse;";
            placeholders.push(sample_name.unwrap().clone().into());
        } else {
            query = "WITH RECURSIVE traverse(block_id, block_sequence, block_start, block_end, block_strand, depth) AS (
          SELECT edges.source_id, substr(seq.sequence, block.start + 1, block.end - block.start), block.start, block.end, block.strand, 0 as depth FROM block_group left join block on (block_group.id = block.block_group_id) left join sequence seq on (seq.hash = block.sequence_hash) left join edges on (block.id = edges.source_id or block.id = edges.target_id) WHERE block_group.collection_name = ?1 AND block_group.sample_name is null AND block_group.name = ?2 and edges.target_id is null
          UNION
          SELECT e2.source_id, substr(seq2.sequence, b2.start + 1, b2.end - b2.start), b2.start, b2.end, b2.strand, depth + 1 FROM edges e2 left join block b2 on (b2.id = e2.source_id) left join sequence seq2 on (seq2.hash = b2.sequence_hash) JOIN traverse t2 ON e2.target_id = t2.block_id  order by depth desc
        ) SELECT block_sequence as sequence, block_strand as strand FROM traverse;"
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::get_connection as get_db_connection;
    use crate::migrations::run_migrations;
    use noodles::fasta::record::Sequence;
    use std::fs;
    use std::hash::Hash;

    fn get_connection() -> Connection {
        let mut conn = Connection::open_in_memory()
            .unwrap_or_else(|_| panic!("Error opening in memory test db"));
        run_migrations(&mut conn);
        conn
    }

    fn setup_block_group(conn: &mut Connection) -> (i32, i32) {
        let a_seq_hash =
            models::Sequence::create(conn, "DNA".to_string(), &"AAAAAAAAAA".to_string(), true);
        let t_seq_hash =
            models::Sequence::create(conn, "DNA".to_string(), &"TTTTTTTTTT".to_string(), true);
        let c_seq_hash =
            models::Sequence::create(conn, "DNA".to_string(), &"CCCCCCCCCC".to_string(), true);
        let g_seq_hash =
            models::Sequence::create(conn, "DNA".to_string(), &"GGGGGGGGGG".to_string(), true);
        let collection = Collection::create(conn, &"test".to_string());
        let block_group = BlockGroup::create(conn, &"test".to_string(), None, &"hg19".to_string());
        let a_block = Block::create(conn, &a_seq_hash, block_group.id, 0, 10, &"1".to_string());
        let t_block = Block::create(conn, &t_seq_hash, block_group.id, 0, 10, &"1".to_string());
        let c_block = Block::create(conn, &c_seq_hash, block_group.id, 0, 10, &"1".to_string());
        let g_block = Block::create(conn, &g_seq_hash, block_group.id, 0, 10, &"1".to_string());
        let edge_1 = Edge::create(conn, a_block.id, Some(t_block.id), 1, 0, 0);
        let edge_2 = Edge::create(conn, t_block.id, Some(c_block.id), 0, 0, 0);
        let edge_3 = Edge::create(conn, c_block.id, Some(g_block.id), 0, 0, 0);
        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            vec![edge_1.id, edge_2.id, edge_3.id],
        );
        (block_group.id, path.id)
    }

    #[test]
    fn simple_insert() {
        fs::remove_file("test.db");
        let mut conn = get_db_connection("test.db");
        let (block_group_id, path_id) = setup_block_group(&mut conn);
        let insert_sequence =
            models::Sequence::create(&mut conn, "DNA".to_string(), &"NNNN".to_string(), true);
        let insert = Block::create(
            &conn,
            &insert_sequence,
            block_group_id,
            0,
            4,
            &"1".to_string(),
        );
        BlockGroup::insert_change(&mut conn, path_id, 7, 15, insert.id, 1, 0);

        let blocks_query = "WITH RECURSIVE traverse(block_id, sequence, block_start, block_end, depth, global_start, global_end) AS (
          SELECT edges.source_id, seq.sequence, block.start, block.end, 0 as depth, 0 as global_start, block.end - block.start as global_end FROM block_group left join block on (block_group.id = block.block_group_id) left join sequence seq on (seq.hash = block.sequence_hash) left join edges on (block.id = edges.source_id) WHERE block.block_group_id = ?1 AND edges.origin = 1
          UNION ALL
          SELECT e2.target_id, seq2.sequence, b2.start, b2.end, t2.depth + 1, t2.global_end, t2.global_end + b2.end - b2.start FROM edges e2 left join block b2 on (b2.id = e2.target_id) left join sequence seq2 on (seq2.hash = b2.sequence_hash) JOIN traverse t2 ON e2.source_id = t2.block_id where e2.target_id is not null order by depth desc
        ) SELECT block_id, sequence, block_start, block_end, depth, global_start, global_end FROM traverse;";
        let mut stmt = conn.prepare_cached(blocks_query).unwrap();

        #[derive(Debug)]
        struct BlockInfo {
            id: i32,
            sequence: String,
            block_start: i32,
            block_end: i32,
            depth: i32,
            global_start: i32,
            global_end: i32,
        }
        let rows = stmt
            .query_map([block_group_id], |row| {
                Ok(BlockInfo {
                    id: row.get(0).unwrap(),
                    sequence: row.get(1).unwrap(),
                    block_start: row.get(2).unwrap(),
                    block_end: row.get(3).unwrap(),
                    depth: row.get(4).unwrap(),
                    global_start: row.get(5).unwrap(),
                    global_end: row.get(6).unwrap(),
                })
            })
            .unwrap();
        for block in rows {
            println!("{block:?}");
        }
    }
}
