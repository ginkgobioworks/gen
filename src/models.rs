use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use sha2::{Digest, Sha256};
use std::fmt::*;

pub mod block_group;
pub mod block_group_edge;
pub mod change_log;
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
