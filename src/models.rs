use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use sha2::{Digest, Sha256};
use std::fmt::*;

pub mod block_group;
pub mod block_group_edge;
mod diff_block;
pub mod edge;
pub mod path;
pub mod path_edge;
pub mod sequence;

use crate::models;

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
    pub fn create(conn: &Connection, name: &str) -> Sample {
        let mut stmt = conn
            .prepare("INSERT INTO sample (name) VALUES (?1)")
            .unwrap();
        match stmt.execute((name,)) {
            Ok(_) => Sample {
                name: name.to_string(),
            },
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    Sample {
                        name: name.to_string(),
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
