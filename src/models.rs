use std::collections::HashMap;
use std::fmt::*;

use rusqlite::{params_from_iter, Connection};
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
    pub start: i32,
    pub end: i32,
    pub strand: String,
}

impl Block {
    pub fn create(
        conn: &mut Connection,
        hash: &String,
        start: i32,
        end: i32,
        strand: String,
    ) -> Block {
        let mut stmt = conn
            .prepare("INSERT INTO block (sequence_hash, start, end, strand) VALUES (?1, ?2, ?3, ?4) RETURNING *")
            .unwrap();
        stmt.query_row((hash, start, end, strand), |row| {
            Ok(models::Block {
                id: row.get(0)?,
                sequence_hash: row.get(1)?,
                start: row.get(2)?,
                end: row.get(3)?,
                strand: row.get(4)?,
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
    pub fn create(conn: &mut Connection, source_id: i32, target_id: Option<i32>) -> Edge {
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
    pub start_edge_id: i32,
    pub path_index: i32,
}

impl Path {
    pub fn create(
        conn: &mut Connection,
        name: &String,
        start_edge_id: i32,
        path_index: Option<i32>,
    ) -> Path {
        let query =
            "INSERT INTO path (name, start_edge_id, path_index) VALUES (?1, ?2, ?3) RETURNING *";
        let mut stmt = conn.prepare(query).unwrap();
        let index = path_index.unwrap_or(0);
        stmt.query_row((name, start_edge_id, index), |row| {
            Ok(models::Path {
                id: row.get(0)?,
                name: row.get(1)?,
                start_edge_id: row.get(2)?,
                path_index: row.get(3)?,
            })
        })
        .unwrap()
    }
    #[allow(clippy::ptr_arg)]
    #[allow(clippy::too_many_arguments)]
    pub fn insert_change(
        conn: &mut Connection,
        collection_name: &String,
        sample_name: &String,
        path_name: &String,
        new_path_index: i32,
        start: i32,
        end: i32,
        new_sequence_hash: &String,
    ) {
        let mut stmt = conn
            .prepare("select path.id from path left join path_collection pc on (pc.path_id = path.id) where pc.collection_name = ?1 AND pc.sample_name = ?2 AND path.name = ?2 AND path_index = ?3")
            .unwrap();

        let mut path_id : i32 = match conn.query_row(
            "select path.id from path left join path_collection pc on (pc.path_id = path.id) where pc.collection_name = ?1 AND pc.sample_name = ?2 AND path.name = ?2 AND path_index = ?3",
            (collection_name, sample_name, path_name, new_path_index),
            |row| row.get(0),
        ) {
            Ok(res) => res,
            Err(rusqlite::Error::QueryReturnedNoRows) => 0,
            Err(_e) => {
                panic!("something bad happened querying the database")
            }
        };
        if path_id == 0 {}
    }
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
        sample_id: Option<&String>,
    ) -> PathCollection {
        let mut query;
        let mut placeholders: Vec<rusqlite::types::Value> = vec![];
        if sample_id.is_some() {
            query = "INSERT INTO path_collection (collection_name, path_id, sample_id) VALUES (?1, ?2, ?3) RETURNING *";
            placeholders.push((*collection_name).clone().into());
            placeholders.push(path_id.into());
            placeholders.push((*sample_id.unwrap()).clone().into());
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
    pub start_edge_id: Option<i32>,
}

pub enum QueryPath {
    id(i32),
    name(String),
    CollectionId(i32),
    StartEdgeId(i32),
}

impl Path {
    pub fn get(conn: &mut Connection, filters: Vec<QueryPath>, to_fetch: Vec<&str>) -> OptionPath {
        let mut clauses = vec![];
        let mut placeholders: Vec<rusqlite::types::Value> = vec![];
        for (index, filter) in filters.iter().enumerate() {
            let prefix = match filter {
                QueryPath::id(value) => {
                    placeholders.push((*value).into());
                    "id"
                }
                QueryPath::name(value) => {
                    placeholders.push((*value).clone().into());
                    "name"
                }
                QueryPath::CollectionId(value) => {
                    placeholders.push((*value).into());
                    "collection_id"
                }
                QueryPath::StartEdgeId(value) => {
                    placeholders.push((*value).into());
                    "start_edge_id"
                }
            };
            clauses.push(format!("{prefix} = ?{index}", index = index + 1));
        }
        let mut return_columns: Vec<&str> = vec![];
        let mut column_map: HashMap<&str, i32> = HashMap::new();
        let mut index = 0;
        for entry in ["id", "name", "collection_id", "start_edge_id"] {
            if to_fetch.contains(&entry) {
                column_map.insert(entry, index);
                return_columns.push(entry);
                index += 1;
            }
        }
        let mut stmt = conn
            .prepare(&format!(
                "select {columns} from sequence where {clauses}",
                columns = return_columns.join(","),
                clauses = clauses.join(" AND ")
            ))
            .expect("Unable to prepare given SQL statement");
        stmt.query_row(params_from_iter(placeholders), |row| {
            Ok(OptionPath {
                id: row.get(0)?,
                collection_id: row.get(0)?,
                name: row.get(0)?,
                start_edge_id: row.get(0)?,
            })
        })
        .expect("Unable to find collection with id.")
    }

    pub fn sequence(conn: &mut Connection, collection_id: i32, path_name: String) -> String {
        struct SequenceBlock {
            sequence: String,
            strand: String,
        }

        let mut stmt = conn.prepare("WITH RECURSIVE traverse(x) AS (
          SELECT edges.source_id FROM path left join edges on (edges.id = path.start_edge_id) WHERE path.collection_id = ?1 AND path.name = ?2
          UNION
          SELECT id FROM block JOIN traverse ON id = x
          UNION
          SELECT target_id FROM edges JOIN traverse ON source_id = x
        ) select substr(seq.sequence, b.start, b.end), b.strand from block b left join sequence_collection sc on (b.sequence_collection_id = sc.id) left join sequence seq on (sc.sequence_id = seq.id) where b.id IN (SELECT x FROM traverse);").unwrap();
        let mut blocks = stmt
            .query_map((collection_id, path_name), |row| {
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
