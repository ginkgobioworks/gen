use std::collections::HashMap;
use std::fmt::*;

use rusqlite::{params, params_from_iter, Connection};
use sha2::{Digest, Sha256};

use crate::models;

#[derive(Debug)]
pub struct Collection {
    pub name: String,
}

pub enum QueryCollection {
    name(String),
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

pub struct OptionSequence {
    pub hash: Option<String>,
    pub sequence_type: Option<String>,
    pub sequence: Option<String>,
    pub length: Option<i32>,
}

pub enum QuerySequence {
    hash(String),
    sequence_type(String),
    sequence(String),
    length(i32),
}

impl Sequence {
    pub fn create(conn: &mut Connection, sequence_type: String, sequence: &String) -> String {
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
                .query_map((hash, sequence_type, sequence, sequence.len()), |row| {
                    row.get(0)
                })
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
    pub path_id: i32,
    pub start: i32,
    pub end: i32,
    pub strand: String,
}

impl Block {
    pub fn create(
        conn: &Connection,
        hash: &String,
        path_id: i32,
        start: i32,
        end: i32,
        strand: String,
    ) -> Block {
        let mut stmt = conn
            .prepare_cached("INSERT INTO block (sequence_hash, path_id, start, end, strand) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING *")
            .unwrap();
        stmt.query_row((hash, path_id, start, end, strand), |row| {
            Ok(models::Block {
                id: row.get(0)?,
                sequence_hash: row.get(1)?,
                path_id: row.get(2)?,
                start: row.get(3)?,
                end: row.get(4)?,
                strand: row.get(5)?,
            })
        })
        .unwrap()
    }
}

#[derive(Debug)]
pub struct Edge {
    pub id: i32,
    pub source_id: i32,
    pub target_id: Option<i32>,
}

impl Edge {
    pub fn create(conn: &Connection, source_id: i32, target_id: Option<i32>) -> Edge {
        let mut query;
        let mut placeholders = vec![];
        if target_id.is_some() {
            query = "INSERT INTO edges (source_id, target_id) VALUES (?1, ?2) RETURNING *";
            placeholders.push(source_id);
            placeholders.push(target_id.unwrap());
        } else {
            query = "INSERT INTO edges (source_id) VALUES (?1) RETURNING *";
            placeholders.push(source_id);
        }
        let mut stmt = conn.prepare(query).unwrap();
        stmt.query_row(params_from_iter(placeholders), |row| {
            Ok(models::Edge {
                id: row.get(0)?,
                source_id: row.get(1)?,
                target_id: row.get(2)?,
            })
        })
        .unwrap()
    }

    // pub fn bulk_create(conn: &mut Connection, edges: &Vec<Edge>) -> Vec<Edge> {
    //     let tx = conn.transaction().unwrap();
    //     let mut results = vec![];
    //     for edge in edges {
    //         let mut stmt = tx
    //             .prepare("INSERT INTO edges (source_id, target_id) VALUES (?1, ?2) RETURNING *")
    //             .unwrap();
    //         let result = stmt
    //             .query_row([edge.source_id, edge.target_id], |row| {
    //                 Ok(models::Edge {
    //                     id: row.get(0)?,
    //                     source_id: row.get(1)?,
    //                     target_id: row.get(2)?,
    //                 })
    //             })
    //             .unwrap();
    //         results.push(result);
    //     }
    //     tx.commit()
    //         .unwrap_or_else(|_| panic!("failed to commit changes."));
    //     results
    // }
}

#[derive(Debug)]
pub struct Path {
    pub id: i32,
    pub name: String,
    pub path_index: i32,
}

impl Path {
    pub fn create(conn: &mut Connection, name: &String, path_index: Option<i32>) -> Path {
        let query = "INSERT INTO path (name, path_index) VALUES (?1, ?2) RETURNING *";
        let mut stmt = conn.prepare(query).unwrap();
        let index = path_index.unwrap_or(0);
        match stmt.query_row((name, index), |row| {
            Ok(Path {
                id: row.get(0)?,
                name: row.get(1)?,
                path_index: row.get(2)?,
            })
        }) {
            Ok(path) => path,
            Err(rusqlite::Error::SqliteFailure(err, _)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    Path {
                        id: conn
                            .query_row(
                                "select id from path where name = ?1 and path_index = ?2",
                                (name, index),
                                |row| row.get(0),
                            )
                            .unwrap(),
                        name: name.clone(),
                        path_index: index,
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

    pub fn clone(conn: &mut Connection, source_path_id: i32, target_path_id: i32) {
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, sequence_hash, start, end, strand from block where path_id = ?1",
            )
            .unwrap();
        let mut block_map: HashMap<i32, i32> = HashMap::new();
        let mut it = stmt.query([source_path_id]).unwrap();
        let mut row = it.next().unwrap();
        while row.is_some() {
            let block = row.unwrap();
            let block_id: i32 = block.get(0).unwrap();
            let hash: String = block.get(1).unwrap();
            let start = block.get(2).unwrap();
            let end = block.get(3).unwrap();
            let strand = block.get(4).unwrap();
            let new_block = Block::create(conn, &hash, target_path_id, start, end, strand);
            block_map.insert(block_id, new_block.id);
            row = it.next().unwrap();
        }

        let mut stmt = conn.prepare_cached("SELECT source_id, target_id from edges where source_id IN rarray(?1) OR target_id IN rarray(?1)").unwrap();
        let mut it = stmt
            .query(params_from_iter(Vec::from_iter(block_map.keys())))
            .unwrap();
        let mut row = it.next().unwrap();
        while row.is_some() {
            let edge = row.unwrap();
            let source_id: i32 = edge.get(0).unwrap();
            let target_id: Option<i32> = edge.get(1).unwrap();
            Edge::create(conn, *block_map.get(&source_id).unwrap_or(&source_id), None);
        }

        // for b in blocks {
        //
        // }
        // println!("{block_map:?}");
        // let create_blocks = "INSERT into block (sequence_hash, path_id, start, end, strand) SELECT sb.sequence_hash, ?1, sb.start, sb.end, sb.strand from block sb where sb.path_id = ?2";
        // let mut stmt = conn.prepare(create_blocks).unwrap();
        // let mut it = stmt.query([target_path_id, source_path_id]).unwrap();
        // println!("{v:?}", v=it.next());
        // println!("{v:?}", v=it.next());
        // println!("{v:?}", v=it.next());
        // println!("{v:?}", v=it.next());
        // tx.commit().unwrap();
    }

    pub fn get_or_create_sample_path(
        conn: &mut Connection,
        collection_name: &String,
        sample_name: &String,
        path_name: &String,
        new_path_index: i32,
    ) -> (i32, i32) {
        println!("lookup {collection_name} {path_name} {new_path_index}");
        let mut path_id : i32 = match conn.query_row(
            "select path.id from path left join path_collection pc on (pc.path_id = path.id) where pc.collection_name = ?1 AND pc.sample_name = ?2 AND path.name = ?3 AND path_index = ?4",
            (collection_name, sample_name, path_name, new_path_index),
            |row| row.get(0),
        ) {
            Ok(res) => res,
            Err(rusqlite::Error::QueryReturnedNoRows) => 0,
            Err(_e) => {
                panic!("Error querying the database: {_e}");
            }
        };
        println!("here {path_id}");
        if path_id != 0 {
            return (path_id, path_id);
        } else {
            // no path exists, so make it first -- check if we have a reference path for this sample first
            path_id = match conn.query_row(
            "select path.id from path left join path_collection pc on (pc.path_id = path.id) where pc.collection_name = ?1 AND pc.sample_name = ?2 AND path.name = ?3 AND path_index = 0",
            (collection_name, sample_name, path_name),
            |row| row.get(0),
            ) {
                Ok(res) => res,
                Err(rusqlite::Error::QueryReturnedNoRows) => 0,
                Err(_e) => {
                    panic!("something bad happened querying the database")
                }
            }
        }
        if path_id == 0 {
            // use the base reference bath if it exists since there is no base sample path
            path_id = match conn.query_row(
            "select path.id from path left join path_collection pc on (pc.path_id = path.id) where pc.collection_name = ?1 AND pc.sample_name IS null AND path.name = ?2 AND path_index = 0",
            (collection_name, path_name),
            |row| row.get(0),
            ) {
                Ok(res) => res,
                Err(rusqlite::Error::QueryReturnedNoRows) => panic!("No base path exists"),
                Err(_e) => {
                    panic!("something bad happened querying the database")
                }
            }
        }
        let new_path_id = Path::create(conn, path_name, Some(new_path_index));
        PathCollection::create(conn, collection_name, new_path_id.id, Some(sample_name));

        // clone parent blocks/edges
        Path::clone(conn, path_id, new_path_id.id);

        println!("made new one {path_id} {new_path_id:?}");
        (path_id, new_path_id.id)
    }

    #[allow(clippy::ptr_arg)]
    #[allow(clippy::too_many_arguments)]
    pub fn insert_change(
        conn: &mut Connection,
        path_id: i32,
        start: i32,
        end: i32,
        new_sequence_hash: &String,
    ) {
        // let mut blocks : Vec<Block> = vec![];
        // let mut block_edges : HashMap<i32, Vec<Edge>> = HashMap::new();
        // let mut stmt = conn.prepare("select b.id, b.path_id, b.sequence_hash, b.start, b.end, b.strand, e.id as edge_id, e.source_id, e.target_id from block b left join edges e on (e.target_id = b.id or e.source_id = b.id) where b.path_id = ?1 order by b.start;").unwrap();
        // let mut it = stmt.query([path_id]).unwrap();
        // println!("{v:?}", v=it.next());
        // println!("{v:?}", v=it.next());
        // println!("{v:?}", v=it.next());
        // #[derive(Debug)]
        // struct BlockEdge {
        //     id: i32,
        //     hash: String,
        //     start: i32,
        //     end: i32,
        //     strand: String,
        //     edge_id: i32,
        //     source_id: i32,
        //     target_id: Option<i32>
        // }
        //
        // // is this not visible atm?
        // let mut stmt = conn.prepare("select b.id, b.sequence_hash, b.start, b.end, b.strand, e.id as edge_id, e.source_id, e.target_id from block b left join edges e on (e.target_id = b.id or e.source_id = b.id) where b.path_id = ?1 order by b.start;").unwrap();
        // let rows = stmt
        //     .query_map([parent_path_id], |row| Ok(BlockEdge {
        //         id: row.get(0)?,
        //         hash: row.get(1)?,
        //         start: row.get(2)?,
        //         end: row.get(3)?,
        //         strand: row.get(4)?,
        //         edge_id: row.get(5)?,
        //         source_id: row.get(6)?,
        //         target_id: row.get(7)?,
        //     })).unwrap();
        // for block_edge_result in rows {
        //     println!("{block_edge_result:?}");
        //     let block_edge = block_edge_result.unwrap();
        //         let block_id = block_edge.id;
        //         blocks.push(Block {
        //             id: block_id,
        //             path_id: 0, // this is empty
        //             sequence_hash: block_edge.hash,
        //             start: block_edge.start,
        //             end: block_edge.end,
        //             strand: block_edge.strand,
        //         });
        //         let edge = Edge {
        //             id: block_edge.edge_id,
        //             source_id: block_edge.source_id,
        //             target_id: block_edge.target_id,
        //         };
        //         if block_edges.contains_key(&block_id) {
        //             block_edges.get_mut(&block_id).unwrap().push(edge);
        //         } else {
        //             block_edges.insert(block_id, vec![edge]);
        //
        //         };
        //     }
        //
        // println!("blocks are {blocks:?} {block_edges:?}");
        //
        // #[derive(Debug)]
        // struct BlockPath {
        //     id: Option<i32>,
        //     left: Option<i32>,
        //     right: Option<i32>,
        // }
        // let mut new_block_id_map : HashMap<i32, BlockPath> = HashMap::new();
        //
        // // let tx = conn.transaction().unwrap();
        //
        // for block in blocks {
        //     // TODO: do the whole check for 0 indices and correct edge behavior
        //     let contains_start = block.start <= start && start < block.end;
        //     let contains_end = block.end <= end - 1 && end - 1 < block.end;
        //     if contains_start || contains_end {
        //         let mut left_block : Option<i32> = None;
        //         let mut right_block : Option<i32> = None;
        //         if contains_start {
        //             let block_start = block.start;
        //             let block_end = start;
        //             let mut stmt = conn
        //             .prepare("INSERT INTO block (sequence_hash, path_id, start, end, strand) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING (id)")
        //                 .unwrap();
        //
        //             left_block = stmt
        //                 .query_row((&block.sequence_hash, haplotype_path_id, block_start, block_end, &block.strand), |row| {
        //                     Ok(row.get(0)?)
        //                 })
        //                 .unwrap();
        //         }
        //         if contains_end {
        //             let block_start = block.end;
        //             let block_end = end;
        //             let mut stmt = conn
        //             .prepare("INSERT INTO block (sequence_hash, path_id, start, end, strand) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING (id)")
        //                 .unwrap();
        //
        //             right_block = stmt
        //                 .query_row((&block.sequence_hash, haplotype_path_id, block_start, block_end, &block.strand), |row| {
        //                     Ok(row.get(0)?)
        //                 })
        //                 .unwrap();
        //         }
        //         new_block_id_map.insert(block.id, BlockPath{id: None, left: left_block, right: right_block});
        //
        //     } else {
        //         let mut stmt = conn
        //             .prepare("INSERT INTO block (sequence_hash, path_id, start, end, strand) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING (id)")
        //             .unwrap();
        //
        //         let new_block_id : i32 = stmt
        //             .query_row((&block.sequence_hash, haplotype_path_id, block.start, block.end, &block.strand), |row| {
        //                 Ok(row.get(0)?)
        //             })
        //             .unwrap();
        //         new_block_id_map.insert(block.id, BlockPath{id: Some(new_block_id), left: None, right: None});
        //     }
        // }
        //
        // // make our edges
        // println!("{new_block_id_map:?} {block_edges:?}");
        // for (block_id, block_edges) in block_edges.iter() {
        //     for edge in block_edges.iter() {
        //         let new_source_block = &new_block_id_map[&edge.source_id];
        //         let mut new_source_id;
        //         let mut new_target_id = None;
        //         if new_source_block.id.is_some() {
        //             new_source_id = new_source_block.id.unwrap();
        //         } else {
        //             new_source_id = new_source_block.left.unwrap();
        //         }
        //         if edge.target_id.is_some() {
        //             let new_target_block = &new_block_id_map[&edge.target_id.unwrap()];
        //             if new_target_block.id.is_some() {
        //                 new_target_id = Some(new_target_block.id.unwrap());
        //             } else {
        //                 new_target_id = Some(new_target_block.right.unwrap());
        //             }
        //         }
        //         if new_target_id.is_some() {
        //             let mut stmt = conn
        //             .prepare("INSERT INTO edges (source_id, target_id) VALUES (?1, ?2)")
        //             .unwrap();
        //         stmt.query([new_source_id, new_target_id.unwrap()]).unwrap();
        //         } else {
        //             let mut stmt = conn
        //             .prepare("INSERT INTO edges (source_id) VALUES (?1)")
        //             .unwrap();
        //         stmt.query([new_source_id]).unwrap();
        //         }
        //
        //     }
        // }
        // tx.commit()
        //     .unwrap_or_else(|_| panic!("failed to commit changes."));
    }

    //
    // struct Block {
    //     id : i32,
    //     start: i32,
    //     end: i32,
    // }
    //
    // // now we have a path to insert our change into
    // // find the block we need to split
    // let mut stmt = conn.prepare("select e.id, start_block.id, start_block.start, start_block.end, end_block.id, end_block.start, end_block.end from edges e left join blocks start_block on (e.source_id = start_block.id) left join blocks end_block on (e.target_id = end_block.id) where (e.start < ?2 AND e.end > ?2) OR (e.start < ?3 AND e.end > ?3) AND e.path_id = ?1 ORDER by e.start").unwrap();
    // stmt.query_row((path_id, start, end), |row| {
    //     Ok(models::PathCollection {
    //         id: row.get(0)?,
    //         collection_name: row.get(1)?,
    //         path_id: row.get(2)?,
    //         sample_name: row.get(3)?,
    //     })
    // })
    // .unwrap();
}

#[derive(Debug)]
pub struct PathCollection {
    pub id: i32,
    pub collection_name: String,
    pub path_id: i32,
    pub sample_name: Option<String>,
}

impl PathCollection {
    pub fn create(
        conn: &mut Connection,
        collection_name: &String,
        path_id: i32,
        sample_name: Option<&String>,
    ) -> PathCollection {
        let mut query;
        let mut placeholders: Vec<rusqlite::types::Value> = vec![];
        if sample_name.is_some() {
            query = "INSERT INTO path_collection (collection_name, path_id, sample_name) VALUES (?1, ?2, ?3) RETURNING *";
            placeholders.push((*collection_name).clone().into());
            placeholders.push(path_id.into());
            placeholders.push((*sample_name.unwrap()).clone().into());
        } else {
            query = "INSERT INTO path_collection (collection_name, path_id) VALUES (?1, ?2) RETURNING *";
            placeholders.push((*collection_name).clone().into());
            placeholders.push(path_id.into());
        }
        let mut stmt = conn.prepare(query).unwrap();
        stmt.query_row(params_from_iter(placeholders), |row| {
            Ok(models::PathCollection {
                id: row.get(0)?,
                collection_name: row.get(1)?,
                path_id: row.get(2)?,
                sample_name: row.get(3)?,
            })
        })
        .unwrap()
    }
}

#[derive(Debug)]
pub struct OptionPath {
    pub id: Option<i32>,
    pub name: Option<String>,
    pub collection_id: Option<i32>,
}

pub enum QueryPath {
    id(i32),
    name(String),
    CollectionId(i32),
    StartEdgeId(i32),
}

impl Path {
    // pub fn get(conn: &mut Connection, filters: Vec<QueryPath>, to_fetch: Vec<&str>) -> OptionPath {
    //     let mut clauses = vec![];
    //     let mut placeholders: Vec<rusqlite::types::Value> = vec![];
    //     for (index, filter) in filters.iter().enumerate() {
    //         let prefix = match filter {
    //             QueryPath::id(value) => {
    //                 placeholders.push((*value).into());
    //                 "id"
    //             }
    //             QueryPath::name(value) => {
    //                 placeholders.push((*value).clone().into());
    //                 "name"
    //             }
    //             QueryPath::CollectionId(value) => {
    //                 placeholders.push((*value).into());
    //                 "collection_id"
    //             }
    //             QueryPath::StartEdgeId(value) => {
    //                 placeholders.push((*value).into());
    //                 "start_edge_id"
    //             }
    //         };
    //         clauses.push(format!("{prefix} = ?{index}", index = index + 1));
    //     }
    //     let mut return_columns: Vec<&str> = vec![];
    //     let mut column_map: HashMap<&str, i32> = HashMap::new();
    //     let mut index = 0;
    //     for entry in ["id", "name", "collection_id"] {
    //         if to_fetch.contains(&entry) {
    //             column_map.insert(entry, index);
    //             return_columns.push(entry);
    //             index += 1;
    //         }
    //     }
    //     let mut stmt = conn
    //         .prepare(&format!(
    //             "select {columns} from sequence where {clauses}",
    //             columns = return_columns.join(","),
    //             clauses = clauses.join(" AND ")
    //         ))
    //         .expect("Unable to prepare given SQL statement");
    //     stmt.query_row(params_from_iter(placeholders), |row| {
    //         Ok(OptionPath {
    //             id: row.get(0)?,
    //             collection_id: row.get(0)?,
    //             name: row.get(0)?,
    //         })
    //     })
    //     .expect("Unable to find collection with id.")
    // }

    // pub fn sequence(conn: &mut Connection, collection_id: i32, path_name: String) -> String {
    //     struct SequenceBlock {
    //         sequence: String,
    //         strand: String,
    //     }
    //
    //     let mut stmt = conn.prepare("WITH RECURSIVE traverse(x) AS (
    //       SELECT edges.source_id FROM path left join edges on (edges.id = path.start_edge_id) WHERE path.collection_id = ?1 AND path.name = ?2
    //       UNION
    //       SELECT id FROM block JOIN traverse ON id = x
    //       UNION
    //       SELECT target_id FROM edges JOIN traverse ON source_id = x
    //     ) select substr(seq.sequence, b.start, b.end), b.strand from block b left join sequence_collection sc on (b.sequence_collection_id = sc.id) left join sequence seq on (sc.sequence_id = seq.id) where b.id IN (SELECT x FROM traverse);").unwrap();
    //     let mut blocks = stmt
    //         .query_map((collection_id, path_name), |row| {
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

    // pub fn create(
    //     conn: &mut Connection,
    //     collection_id: i32,
    //     sample_id: Option<i32>,
    //     name: String,
    // ) -> i32 {
    //     let mut stmt   = conn.prepare("INSERT INTO sequence (type, name, sequence, length, circular) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING (id)").unwrap();
    //     let mut rows = stmt
    //         .query_map((r#type, name, sequence, sequence.len(), circular), |row| {
    //             row.get(0)
    //         })
    //         .unwrap();
    //     rows.next().unwrap().unwrap()
    // }
}
