use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use sha2::{Digest, Sha256};

#[derive(Debug, Default)]
pub struct Sequence {
    pub hash: String,
    pub sequence_type: String,
    pub sequence: String,
    // these 2 fields are only relevant when the sequence is stored externally
    pub name: String,
    pub file_path: String,
    pub length: i32,
    // by default we want to store the sequence in the db, bools default to false, so our
    // flag is whether the sequence is stored externally
    pub external_sequence: bool,
}

impl Sequence {
    pub fn create(conn: &Connection, sequence: &Sequence) -> String {
        let mut hasher = Sha256::new();
        let sequence_length = if sequence.length == 0 {
            sequence.sequence.len() as u32
        } else {
            sequence.length as u32
        };
        hasher.update(&sequence.sequence_type);
        hasher.update(";");
        hasher.update(&sequence.sequence);
        hasher.update(";");
        hasher.update(&sequence.name);
        hasher.update(";");
        hasher.update(&sequence.file_path);
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
                        Value::from(sequence.sequence_type.clone()),
                        Value::from(sequence.sequence.clone()),
                        Value::from(sequence.name.clone()),
                        Value::from(sequence.file_path.clone()),
                        Value::from(sequence_length),
                    ),
                    |row| row.get(0),
                )
                .unwrap();
            obj_hash = rows.next().unwrap().unwrap();
        }
        obj_hash
    }

    pub fn get_sequences(
        conn: &Connection,
        query: &str,
        placeholders: Vec<Value>,
    ) -> Vec<Sequence> {
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
                    file_path: row.get(4).unwrap(),
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
        let seq_hash = Sequence::create(
            conn,
            &Sequence {
                sequence_type: "DNA".to_string(),
                sequence: "AACCTT".to_string(),
                ..Default::default()
            },
        );
        let sequences = Sequence::get_sequences(
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
        let seq_hash = Sequence::create(
            conn,
            &Sequence {
                sequence_type: "DNA".to_string(),
                name: "chr1".to_string(),
                file_path: "/some/path.fa".to_string(),
                ..Default::default()
            },
        );
        let sequences = Sequence::get_sequences(
            conn,
            "select * from sequence where hash = ?1",
            vec![Value::from(seq_hash)],
        );
        let sequence = sequences.first().unwrap();
        assert_eq!(sequence.sequence_type, "DNA");
        assert_eq!(&sequence.sequence, "");
        assert_eq!(sequence.name, "chr1");
        assert_eq!(sequence.file_path, "/some/path.fa");
        assert!(sequence.external_sequence);
    }
}
