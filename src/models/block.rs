use rusqlite::Connection;

#[derive(Debug)]
pub struct Block {
    pub id: i32,
    pub sequence_hash: String,
    pub block_group_id: i32,
    pub start: i32,
    pub end: i32,
    pub strand: String,
}

impl Block {
    pub fn create(
        conn: &Connection,
        hash: &String,
        block_group_id: i32,
        start: i32,
        end: i32,
        strand: &String,
    ) -> Block {
        let mut stmt = conn
            .prepare_cached("INSERT INTO block (sequence_hash, block_group_id, start, end, strand) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING *")
            .unwrap();
        match stmt.query_row((hash, block_group_id, start, end, strand), |row| {
            Ok(Block {
                id: row.get(0)?,
                sequence_hash: row.get(1)?,
                block_group_id: row.get(2)?,
                start: row.get(3)?,
                end: row.get(4)?,
                strand: row.get(5)?,
            })
        }) {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, _details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    Block {
                        id: conn
                            .query_row(
                                "select id from block where sequence_hash = ?1 AND block_group_id = ?2 AND start = ?3 AND end = ?4 AND strand = ?5;",
                                (hash, block_group_id, start, end, strand),
                                |row| row.get(0),
                            )
                            .unwrap(),
                        sequence_hash: hash.clone(),
                        block_group_id,
                        start,
                        end,
                        strand: strand.clone(),
                    }
                } else {
                    panic!("something bad happened querying the database")
                }
            }
            Err(_e) => {
                panic!("failure in making block {_e}")
            }
        }
    }
}
