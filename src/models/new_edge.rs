use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};

#[derive(Clone, Debug)]
pub struct NewEdge {
    pub id: i32,
    pub source_hash: Option<String>,
    pub source_coordinate: Option<i32>,
    pub target_hash: Option<String>,
    pub target_coordinate: Option<i32>,
    pub chromosome_index: i32,
    pub phased: i32,
}

impl NewEdge {
    pub fn create(
        conn: &Connection,
        source_hash: Option<String>,
        source_coordinate: Option<i32>,
        target_hash: Option<String>,
        target_coordinate: Option<i32>,
        chromosome_index: i32,
        phased: i32,
    ) -> NewEdge {
        let query;
        let id_query;
        let mut placeholders: Vec<Value> = vec![];
        if target_hash.is_some() && source_hash.is_some() {
            query = "INSERT INTO new_edges (source_hash, source_coordinate, target_hash, target_coordinate, chromosome_index, phased) VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING *";
            id_query = "select id from new_edges where source_hash = ?1 and source_coordinate = ?2 and target_hash = ?3 and target_coordinate = ?4 and chromosome_index = ?5 and phased = ?6";
            placeholders.push(source_hash.clone().unwrap().into());
            placeholders.push(source_coordinate.unwrap().into());
            placeholders.push(target_hash.clone().unwrap().into());
            placeholders.push(target_coordinate.unwrap().into());
            placeholders.push(chromosome_index.into());
            placeholders.push(phased.into());
        } else if target_hash.is_some() {
            id_query = "select id from new_edges where target_hash = ?1 and target_coordinate = ?2 and source_hash is null and chromosome_index = ?3 and phased = ?4";
            query = "INSERT INTO new_edges (target_hash, target_coordinate, chromosome_index, phased) VALUES (?1, ?2, ?3, ?4) RETURNING *";
            placeholders.push(target_hash.clone().unwrap().into());
            placeholders.push(target_coordinate.unwrap().into());
            placeholders.push(chromosome_index.into());
            placeholders.push(phased.into());
        } else {
            id_query = "select id from new_edges where source_hash = ?1 and source_coordinate = ?2 and target_id is null and chromosome_index = ?3 and phased = ?4";
            query = "INSERT INTO new_edges (source_hash, source_coordinate, chromosome_index, phased) VALUES (?1, ?2, ?3, ?4) RETURNING *";
            placeholders.push(source_hash.clone().unwrap().into());
            placeholders.push(source_coordinate.unwrap().into());
            placeholders.push(chromosome_index.into());
            placeholders.push(phased.into());
        }
        let mut stmt = conn.prepare(query).unwrap();
        match stmt.query_row(params_from_iter(&placeholders), |row| {
            Ok(NewEdge {
                id: row.get(0)?,
                source_hash: row.get(1)?,
                source_coordinate: row.get(2)?,
                target_hash: row.get(3)?,
                target_coordinate: row.get(4)?,
                chromosome_index: row.get(5)?,
                phased: row.get(6)?,
            })
        }) {
            Ok(edge) => edge,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    NewEdge {
                        id: conn
                            .query_row(id_query, params_from_iter(&placeholders), |row| row.get(0))
                            .unwrap(),
                        source_hash,
                        source_coordinate,
                        target_hash,
                        target_coordinate,
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

    pub fn load(conn: &Connection, edge_ids: Vec<i32>) -> Vec<NewEdge> {
        let formatted_edge_ids = edge_ids
            .into_iter()
            .map(|edge_id| edge_id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let query = format!("select id, source_hash, source_coordinate, target_hash, target_coordinate, chromosome_index, phased from new_edges where id in ({});", formatted_edge_ids);
        let mut stmt = conn.prepare_cached(&query).unwrap();
        let rows = stmt
            .query_map([], |row| {
                Ok(NewEdge {
                    id: row.get(0)?,
                    source_hash: row.get(1)?,
                    source_coordinate: row.get(2)?,
                    target_hash: row.get(3)?,
                    target_coordinate: row.get(4)?,
                    chromosome_index: row.get(5)?,
                    phased: row.get(6)?,
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
