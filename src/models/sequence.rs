use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Sequence {
    pub hash: String,
    pub sequence_type: String,
    pub sequence: String,
    // these 2 fields are only relevant when the sequence is stored externally
    pub name: String,
    pub file_path: String,
    pub length: i32,
    // indicates whether the sequence is stored externally, a quick flag instead of having to
    // check sequence or file_path and do the logic in function calls.
    pub external_sequence: bool,
}

#[derive(Default)]
pub struct NewSequence<'a> {
    sequence_type: Option<&'a str>,
    sequence: Option<&'a str>,
    name: Option<&'a str>,
    file_path: Option<&'a str>,
    length: Option<i32>,
    shallow: bool,
}

impl<'a> NewSequence<'a> {
    pub fn new() -> NewSequence<'static> {
        NewSequence {
            shallow: false,
            ..NewSequence::default()
        }
    }

    pub fn shallow(mut self, setting: bool) -> Self {
        self.shallow = setting;
        self
    }

    pub fn sequence_type(mut self, seq_type: &'a str) -> Self {
        self.sequence_type = Some(seq_type);
        self
    }

    pub fn sequence(mut self, sequence: &'a str) -> Self {
        self.sequence = Some(sequence);
        self
    }

    pub fn name(mut self, name: &'a str) -> Self {
        self.name = Some(name);
        self
    }

    pub fn file_path(mut self, path: &'a str) -> Self {
        self.file_path = Some(path);
        self.shallow = true;
        self
    }

    pub fn length(mut self, length: i32) -> Self {
        self.length = Some(length);
        self
    }

    pub fn save(mut self, conn: &Connection) -> String {
        let mut length = 0;
        if self.sequence.is_none() && self.file_path.is_none() {
            panic!("Sequence or file_path must be set.");
        }
        if self.file_path.is_some() && self.name.is_none() {
            panic!("A filepath must have an accompanying sequence name");
        }
        if self.length.is_none() {
            if let Some(v) = self.sequence {
                length = v.len() as i32;
            } else {
                panic!("Sequence length must be specified.");
            }
        }
        let mut hasher = Sha256::new();
        hasher.update(self.sequence_type.expect("Sequence type must be defined."));
        hasher.update(";");
        if let Some(v) = self.sequence {
            hasher.update(v);
            hasher.update(";");
        }
        if let Some(v) = self.name {
            hasher.update(v);
            hasher.update(";");
        }
        if let Some(v) = self.file_path {
            hasher.update(v);
            hasher.update(";");
        }
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
            let mut stmt = conn.prepare("INSERT INTO sequence (hash, sequence_type, sequence, name, file_path, length) VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING (hash);").unwrap();
            let mut rows = stmt
                .query_map(
                    (
                        Value::from(hash.to_string()),
                        Value::from(self.sequence_type.unwrap().to_string()),
                        Value::from(
                            (if self.shallow {
                                ""
                            } else {
                                self.sequence.unwrap()
                            })
                            .to_string(),
                        ),
                        Value::from(self.name.unwrap_or("").to_string()),
                        Value::from(self.file_path.unwrap_or("").to_string()),
                        Value::from(self.length.unwrap_or(length)),
                    ),
                    |row| row.get(0),
                )
                .unwrap();
            obj_hash = rows.next().unwrap().unwrap();
        }
        obj_hash
    }
}

impl Sequence {
    pub fn sequences(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<Sequence> {
        let mut stmt = conn.prepare_cached(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                let file_path: String = row.get(4).unwrap();
                let mut external_sequence = false;
                if !file_path.is_empty() {
                    external_sequence = true;
                }
                Ok(Sequence {
                    hash: row.get(0).unwrap(),
                    sequence_type: row.get(1).unwrap(),
                    sequence: row.get(2).unwrap(),
                    name: row.get(3).unwrap(),
                    file_path,
                    length: row.get(5).unwrap(),
                    external_sequence,
                })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }

    pub fn sequences_by_hash(conn: &Connection, hashes: Vec<String>) -> HashMap<String, Sequence> {
        let joined_hashes = &hashes
            .into_iter()
            .map(|hash| format!("\"{}\"", hash))
            .collect::<Vec<_>>()
            .join(",");
        let sequences = Sequence::sequences(
            conn,
            &format!("select * from sequence where hash in ({0})", joined_hashes),
            vec![],
        );
        sequences
            .into_iter()
            .map(|sequence| (sequence.hash.clone(), sequence))
            .collect::<HashMap<String, Sequence>>()
    }

    pub fn sequence_from_hash(conn: &Connection, hash: &str) -> Option<Sequence> {
        let sequences_by_hash = Sequence::sequences_by_hash(conn, vec![hash.to_string()]);
        sequences_by_hash.get(hash).cloned()
    }
}

mod tests {
    use rusqlite::Connection;
    use std::ops::Deref;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::migrations::run_migrations;

    fn get_connection() -> Connection {
        let mut conn = Connection::open_in_memory()
            .unwrap_or_else(|_| panic!("Error opening in memory test db"));
        rusqlite::vtab::array::load_module(&conn).unwrap();
        run_migrations(&mut conn);
        conn
    }

    #[test]
    fn test_create_sequence_in_db() {
        let conn = &mut get_connection();
        let seq_hash = NewSequence::new()
            .sequence_type("DNA")
            .sequence("AACCTT")
            .save(conn);
        let sequences = Sequence::sequences(
            conn,
            "select * from sequence where hash = ?1",
            vec![Value::from(seq_hash)],
        );
        let sequence = sequences.first().unwrap();
        assert_eq!(&sequence.sequence, "AACCTT");
        assert_eq!(sequence.sequence_type, "DNA");
        assert!(!sequence.external_sequence);
    }

    #[test]
    fn test_create_sequence_on_disk() {
        let conn = &mut get_connection();
        let seq_hash = NewSequence::new()
            .sequence_type("DNA")
            .name("chr1")
            .file_path("/some/path.fa")
            .length(10)
            .save(conn);
        let sequences = Sequence::sequences(
            conn,
            "select * from sequence where hash = ?1",
            vec![Value::from(seq_hash)],
        );
        let sequence = sequences.first().unwrap();
        assert_eq!(sequence.sequence_type, "DNA");
        assert_eq!(&sequence.sequence, "");
        assert_eq!(sequence.name, "chr1");
        assert_eq!(sequence.file_path, "/some/path.fa");
        assert_eq!(sequence.length, 10);
        assert!(sequence.external_sequence);
    }
}
