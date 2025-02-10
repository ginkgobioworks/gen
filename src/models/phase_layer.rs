use rusqlite::Connection;

#[derive(Clone, Debug)]
pub struct PhaseLayer {
    pub id: i64,
    pub chromosome_index: i64,
    pub is_reference: i64,
}

pub const UNPHASED_CHROMOSOME_INDEX: i64 = -1;

impl PhaseLayer {
    pub fn get_or_create(
        conn: &Connection,
        chromosome_index: i64,
        is_reference: i64,
    ) -> Result<i64, &'static str> {
        let phase_layer_id: i64 = match conn.query_row(
            "select id from phase_layers where chromosome_index = ?1 AND is_reference = ?2",
            (chromosome_index, is_reference),
            |row| row.get(0),
        ) {
            Ok(res) => res,
            Err(rusqlite::Error::QueryReturnedNoRows) => 0,
            Err(_e) => {
                panic!("Error querying the database: {_e}");
            }
        };
        if phase_layer_id != 0 {
            return Ok(phase_layer_id);
        }

        let new_phase_layer_id = PhaseLayer::create(conn, chromosome_index, is_reference);

        Ok(new_phase_layer_id)
    }

    pub fn create(conn: &Connection, chromosome_index: i64, is_reference: i64) -> i64 {
        let query = "INSERT INTO phase_layers (chromosome_index, is_reference) VALUES (?1, ?2) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        match stmt.query_row((chromosome_index, is_reference), |row| row.get(0)) {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    conn
                        .query_row(
                            "select id from phase_layers where chromosome_index = ?1 and is_reference = ?2",
                            (chromosome_index, is_reference),
                            |row| row.get(0),
                        )
                        .unwrap()
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
