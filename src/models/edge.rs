use rusqlite::{params_from_iter, Connection};

#[derive(Debug)]
pub struct Edge {
    pub id: i32,
    pub source_id: i32,
    pub target_id: Option<i32>,
    pub chromosome_index: i32,
    pub phased: i32,
}

impl Edge {
    pub fn create(
        conn: &Connection,
        source_id: i32,
        target_id: Option<i32>,
        chromosome_index: i32,
        phased: i32,
    ) -> Edge {
        let query;
        let id_query;
        let mut placeholders = vec![];
        if target_id.is_some() {
            query = "INSERT INTO edges (source_id, target_id, chromosome_index, phased) VALUES (?1, ?2, ?3, ?4) RETURNING *";
            id_query = "select id from edges where source_id = ?1 and target_id = ?2";
            placeholders.push(source_id);
            placeholders.push(target_id.unwrap());
            placeholders.push(chromosome_index);
            placeholders.push(phased);
        } else {
            id_query = "select id from edges where source_id = ?1 and target_id is null and chromosome_index = ?2 and phased = ?3";
            query = "INSERT INTO edges (source_id, chromosome_index, phased) VALUES (?1, ?2, ?3) RETURNING *";
            placeholders.push(source_id);
            placeholders.push(chromosome_index);
            placeholders.push(phased);
        }
        let mut stmt = conn.prepare(query).unwrap();
        match stmt.query_row(params_from_iter(&placeholders), |row| {
            Ok(Edge {
                id: row.get(0)?,
                source_id: row.get(1)?,
                target_id: row.get(2)?,
                chromosome_index: row.get(3)?,
                phased: row.get(4)?,
            })
        }) {
            Ok(edge) => edge,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    Edge {
                        id: conn
                            .query_row(id_query, params_from_iter(&placeholders), |row| row.get(0))
                            .unwrap(),
                        source_id,
                        target_id,
                        chromosome_index,
                        phased,
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
