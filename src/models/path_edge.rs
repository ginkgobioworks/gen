use crate::models::new_edge::NewEdge;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};

#[derive(Debug)]
pub struct PathEdge {
    pub id: i32,
    pub path_id: i32,
    pub source_edge_id: Option<i32>,
    pub target_edge_id: Option<i32>,
}

impl PathEdge {
    pub fn create(
        conn: &Connection,
        path_id: i32,
        source_edge_id: Option<i32>,
        target_edge_id: Option<i32>,
    ) -> PathEdge {
        let query =
            "INSERT INTO path_edges (path_id, source_edge_id, target_edge_id) VALUES (?1, ?2, ?3) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map((path_id, source_edge_id, target_edge_id), |row| {
                Ok(PathEdge {
                    id: row.get(0)?,
                    path_id,
                    source_edge_id,
                    target_edge_id,
                })
            })
            .unwrap();
        match rows.next().unwrap() {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    let query;
                    let mut placeholders = vec![path_id];
                    if let Some(s) = source_edge_id {
                        if let Some(t) = target_edge_id {
                            query = "SELECT id from path_edges where path_id = ?1 AND source_edge_id = ?2 AND target_edge_id = ?3;";
                            placeholders.push(s);
                            placeholders.push(t);
                        } else {
                            query = "SELECT id from path_edges where path_id = ?1 AND source_edge_id = ?2 AND target_edge_id is null;";
                            placeholders.push(s);
                        }
                    } else if let Some(t) = target_edge_id {
                        query = "SELECT id from path_edges where path_id = ?1 AND source_edge_id is null AND target_edge_id = ?2;";
                        placeholders.push(t);
                    } else {
                        panic!("No edge ids passed");
                    }
                    println!("{query} {placeholders:?}");
                    PathEdge {
                        id: conn
                            .query_row(query, params_from_iter(&placeholders), |row| row.get(0))
                            .unwrap(),
                        path_id,
                        source_edge_id,
                        target_edge_id,
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

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<PathEdge> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(PathEdge {
                    id: row.get(0)?,
                    path_id: row.get(1)?,
                    source_edge_id: row.get(2)?,
                    target_edge_id: row.get(3)?,
                })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }

    pub fn edges_for(conn: &Connection, path_id: i32) -> Vec<NewEdge> {
        let edges = vec![];
        let path_edges = PathEdge::query(
            conn,
            "select * from path_edges where path_id = ?1",
            vec![Value::from(path_id)],
        );
        edges
    }
}
