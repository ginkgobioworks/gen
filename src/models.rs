use std::fmt::*;

use crate::calculate_hash;
use crate::models;
use rusqlite::{params_from_iter, Connection};
use sha2::{Digest, Sha256};

#[derive(Debug)]
pub struct Collection {
    pub id: i32,
    pub name: String,
}

impl Collection {
    pub fn exists(conn: &mut Connection, name: &String) -> bool {
        let mut stmt = conn
            .prepare("select id from collection where name = ?1")
            .unwrap();
        stmt.exists([name]).unwrap()
    }
    pub fn create(conn: &mut Connection, name: &String) -> crate::models::Collection {
        let mut stmt = conn
            .prepare("INSERT INTO collection (name) VALUES (?1) RETURNING *")
            .unwrap();
        let mut rows = stmt
            .query_map((name,), |row| {
                Ok(models::Collection {
                    id: row.get(0)?,
                    name: row.get(1)?,
                })
            })
            .unwrap();
        let node = rows.next().unwrap().unwrap();
        return node;
    }

    pub fn bulk_create(
        conn: &mut Connection,
        names: &Vec<String>,
    ) -> Vec<crate::models::Collection> {
        let placeholders = names.iter().map(|_| "(?)").collect::<Vec<_>>().join(", ");
        let q = format!(
            "INSERT INTO collection (name) VALUES {} RETURNING *",
            placeholders
        );
        let mut stmt = conn.prepare(&q).unwrap();
        let rows = stmt
            .query_map(params_from_iter(names), |row| {
                Ok(models::Collection {
                    id: row.get(0)?,
                    name: row.get(1)?,
                })
            })
            .unwrap();
        return rows.map(|row| row.unwrap()).collect();
    }
}

#[derive(Debug)]
pub struct Sequence {
    pub id: i32,
    pub r#type: String,
    pub name: String,
    pub sequence: String,
    pub length: i32,
    pub circular: bool,
}

#[derive(Debug)]
pub struct SequenceCollection {
    pub id: i32,
    pub collection_id: i32,
    pub sequence_id: i32,
}

impl SequenceCollection {
    pub fn create(conn: &mut Connection, collection_id: i32, sequence_id: i32) -> i32 {
        let mut stmt   = conn.prepare("INSERT INTO sequence_collection (sequence_id, collection_id) VALUES (?1, ?2) RETURNING (id)").unwrap();
        let mut rows = stmt
            .query_map((sequence_id, collection_id), |row| Ok(row.get(0)?))
            .unwrap();
        let obj_id = rows.next().unwrap().unwrap();
        return obj_id;
    }
}

impl Sequence {
    pub fn get(conn: &mut Connection, id: i32) -> Sequence {
        conn.query_row("SELECT * from sequence where id = ?1", (id,), |row| {
            Ok(Sequence {
                id: row.get(0)?,
                r#type: row.get(1)?,
                name: row.get(2)?,
                sequence: row.get(3)?,
                length: row.get(4)?,
                circular: row.get(5)?,
            })
        })
        .unwrap()
    }

    pub fn create(
        conn: &mut Connection,
        collection_id: i32,
        r#type: String,
        name: String,
        sequence: &String,
        circular: bool,
    ) -> i32 {
        let mut hasher = Sha256::new();
        hasher.update(&name);
        hasher.update(&r#type);
        hasher.update(&sequence);
        let hash = format!("{:x}", hasher.finalize());
        let mut obj_id = match conn.query_row(
            "SELECT id from sequence where hash = ?1",
            [hash.clone()],
            |row| row.get(0),
        ) {
            Ok(res) => res,
            Err(rusqlite::Error::QueryReturnedNoRows) => 0,
            Err(e) => {
                panic!("something bad happened querying the database")
            }
        };
        if (obj_id == 0) {
            let mut stmt = conn.prepare("INSERT INTO sequence (hash, type, name, sequence, length, circular) VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING (id)").unwrap();
            let mut rows = stmt
                .query_map(
                    (hash, r#type, name, sequence, sequence.len(), circular),
                    |row| Ok(row.get(0)?),
                )
                .unwrap();
            obj_id = rows.next().unwrap().unwrap();
        }
        return obj_id;
    }
}

#[derive(Debug)]
pub struct Path {
    pub id: i32,
    pub r#type: String,
    pub name: String,
    pub sequence: String,
    pub length: i32,
    pub circular: bool,
}

impl Path {
    pub fn get(conn: &mut Connection, collection_id: i32, path_name: String) -> String {
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
        return sequence;
    }

    pub fn create(
        conn: &mut Connection,
        collection_id: i32,
        r#type: String,
        name: String,
        sequence: &String,
        circular: bool,
    ) -> i32 {
        let mut stmt   = conn.prepare("INSERT INTO sequence (type, name, sequence, length, circular) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING (id)").unwrap();
        let mut rows = stmt
            .query_map((r#type, name, sequence, sequence.len(), circular), |row| {
                Ok(row.get(0)?)
            })
            .unwrap();
        let obj_id = rows.next().unwrap().unwrap();
        return obj_id;
    }
}

#[derive(Debug)]
pub struct Node {
    pub id: i32,
    pub base: String,
}

impl Node {
    pub fn create(conn: &mut Connection, base: String) -> Node {
        let mut stmt = conn
            .prepare("INSERT INTO nodes (base) VALUES (?1) RETURNING *")
            .unwrap();
        let mut rows = stmt
            .query_map((base,), |row| {
                Ok(models::Node {
                    id: row.get(0)?,
                    base: row.get(1)?,
                })
            })
            .unwrap();
        let node = rows.next().unwrap().unwrap();
        Node {
            id: node.id,
            base: node.base,
        }
    }

    pub fn bulk_create(conn: &mut Connection, bases: &Vec<String>) -> Vec<Node> {
        let placeholders = bases.iter().map(|_| "(?)").collect::<Vec<_>>().join(", ");
        let q = format!(
            "INSERT INTO nodes (base) VALUES {} RETURNING *",
            placeholders
        );
        let mut stmt = conn.prepare(&q).unwrap();
        let rows = stmt
            .query_map(params_from_iter(bases), |row| {
                Ok(models::Node {
                    id: row.get(0)?,
                    base: row.get(1)?,
                })
            })
            .unwrap();
        let mut nodes: Vec<Node> = vec![];
        for row in rows {
            let node = row.unwrap();
            nodes.push(Node {
                id: node.id,
                base: node.base,
            });
        }
        return nodes;
    }
}

#[derive(Debug)]
pub struct Edge {
    pub id: i32,
    pub source_id: i32,
    pub target_id: i32,
}

impl Edge {
    pub fn create(conn: &mut Connection, source_id: i32, target_id: i32) -> Edge {
        let mut stmt = conn
            .prepare("INSERT INTO edges (source_id, target_id) VALUES (?1, ?2) RETURNING *")
            .unwrap();
        stmt.query_row((source_id, target_id), |row| {
            Ok(models::Edge {
                id: row.get(0)?,
                source_id: row.get(1)?,
                target_id: row.get(2)?,
            })
        })
        .unwrap()
    }

    pub fn bulk_create(conn: &mut Connection, edges: &Vec<Edge>) -> Vec<Edge> {
        let tx = conn.transaction().unwrap();
        let mut results = vec![];
        for edge in edges {
            let mut stmt = tx
                .prepare("INSERT INTO edges (source_id, target_id) VALUES (?1, ?2) RETURNING *")
                .unwrap();
            let result = stmt
                .query_row([edge.source_id, edge.target_id], |row| {
                    Ok(models::Edge {
                        id: row.get(0)?,
                        source_id: row.get(1)?,
                        target_id: row.get(2)?,
                    })
                })
                .unwrap();
            results.push(result);
        }
        tx.commit();
        return results;
    }
}
