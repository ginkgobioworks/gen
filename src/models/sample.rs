use crate::models::traits::*;
use rusqlite::{Connection, Row};
use std::fmt::*;

#[derive(Debug)]
pub struct Sample {
    pub name: String,
}

impl Query for Sample {
    type Model = Sample;
    fn process_row(row: &Row) -> Self::Model {
        Sample {
            name: row.get(0).unwrap(),
        }
    }
}

impl Sample {
    pub fn create(conn: &Connection, name: &str) -> Sample {
        let mut stmt = conn
            .prepare("INSERT INTO samples (name) VALUES (?1)")
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
