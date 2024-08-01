use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};

#[derive(Debug)]
pub struct Edge {
    pub id: i32,
    pub source_id: Option<i32>,
    pub target_id: Option<i32>,
    pub chromosome_index: i32,
    pub phased: i32,
}

impl Edge {
    pub fn create(
        conn: &Connection,
        source_id: Option<i32>,
        target_id: Option<i32>,
        chromosome_index: i32,
        phased: i32,
    ) -> Edge {
        let mut query;
        let mut id_query;
        let mut placeholders: Vec<Value> = vec![];
        if target_id.is_some() && source_id.is_some() {
            query = "INSERT INTO edges (source_id, target_id, chromosome_index, phased) VALUES (?1, ?2, ?3, ?4) RETURNING *";
            id_query = "select id from edges where source_id = ?1 and target_id = ?2 and chromosome_index = ?3 and phased = ?4";
            placeholders.push(Value::from(source_id));
            placeholders.push(target_id.unwrap().into());
            placeholders.push(chromosome_index.into());
            placeholders.push(phased.into());
        } else if target_id.is_some() {
            id_query = "select id from edges where target_id = ?1 and source_id is null and chromosome_index = ?2 and phased = ?3";
            query = "INSERT INTO edges (target_id, chromosome_index, phased) VALUES (?1, ?2, ?3) RETURNING *";
            placeholders.push(target_id.into());
            placeholders.push(chromosome_index.into());
            placeholders.push(phased.into());
        } else {
            id_query = "select id from edges where source_id = ?1 and target_id is null and chromosome_index = ?2 and phased = ?3";
            query = "INSERT INTO edges (source_id, chromosome_index, phased) VALUES (?1, ?2, ?3) RETURNING *";
            placeholders.push(source_id.into());
            placeholders.push(chromosome_index.into());
            placeholders.push(phased.into());
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
