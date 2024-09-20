use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use std::fmt::*;

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

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<Sample> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(Sample { name: row.get(0)? })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }
}
