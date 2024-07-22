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
    pub collection_name: String,
    pub sample_name: Option<String>,
    pub name: String,
    pub path_index: i32,
}

impl Path {
    pub fn create(
        conn: &mut Connection,
        collection_name: &String,
        sample_name: Option<&String>,
        path_name: &String,
        path_index: Option<i32>,
    ) -> Path {
        let query = "INSERT INTO path (collection_name, sample_name, name, path_index) VALUES (?1, ?2, ?3, ?4) RETURNING *";
        let mut stmt = conn.prepare(query).unwrap();
        let index = path_index.unwrap_or(0);
        match stmt.query_row((collection_name, sample_name, path_name, index), |row| {
            Ok(Path {
                id: row.get(0)?,
                collection_name: row.get(1)?,
                sample_name: row.get(2)?,
                name: row.get(3)?,
                path_index: row.get(4)?,
            })
        }) {
            Ok(path) => path,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    Path {
                        id: conn
                            .query_row(
                                "select id from path where collection_name = ?1 and sample_name is null and name = ?2 and path_index = ?3",
                                (collection_name, path_name, index),
                                |row| row.get(0),
                            )
                            .unwrap(),
                        collection_name: collection_name.clone(),
                        sample_name: sample_name.map(|s| s.to_string()),
                        name: path_name.clone(),
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
            "select id from path where collection_name = ?1 AND sample_name = ?2 AND name = ?3 AND path_index = ?4",
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
            "select id from path where collection_name = ?1 AND sample_name = ?2 AND name = ?3 AND path_index = 0",
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
            "select path.id from path where collection_name = ?1 AND sample_name IS null AND name = ?2 AND path_index = 0",
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
        let new_path_id = Path::create(
            conn,
            collection_name,
            Some(sample_name),
            path_name,
            Some(new_path_index),
        );

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
