use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use sha2::{Digest, Sha256};

#[derive(Debug)]
pub struct Sequence {
    pub hash: String,
    pub sequence_type: String,
    pub sequence: String,
    pub length: i32,
}

impl Sequence {
    pub fn create(conn: &Connection, sequence_type: &str, sequence: &str, store: bool) -> String {
        let mut hasher = Sha256::new();
        hasher.update(sequence_type);
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
                .query_map(
                    (
                        Value::from(hash.to_string()),
                        Value::from(sequence_type.to_string()),
                        Value::from((if store { sequence } else { "" }).to_string()),
                        Value::from(sequence.len() as i32),
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
        let mut rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(Sequence {
                    hash: row.get(0).unwrap(),
                    sequence_type: row.get(1).unwrap(),
                    sequence: row.get(2).unwrap(),
                    length: row.get(3).unwrap(),
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
