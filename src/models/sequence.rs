use rusqlite::Connection;
use sha2::{Digest, Sha256};

#[derive(Debug)]
pub struct Sequence {
    pub hash: String,
    pub sequence_type: String,
    pub sequence: String,
    pub length: i32,
}

impl Sequence {
    pub fn create(
        conn: &mut Connection,
        sequence_type: String,
        sequence: &String,
        store: bool,
    ) -> String {
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
                .query_map(
                    (
                        hash,
                        sequence_type,
                        if store { sequence } else { "" },
                        sequence.len(),
                    ),
                    |row| row.get(0),
                )
                .unwrap();
            obj_hash = rows.next().unwrap().unwrap();
        }
        obj_hash
    }
}
