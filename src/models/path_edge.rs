use crate::models::{edge::Edge, path::Path};
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct PathEdge {
    pub id: i32,
    pub path_id: i32,
    pub index_in_path: i32,
    pub edge_id: i32,
}

impl PathEdge {
    pub fn create(conn: &Connection, path_id: i32, index_in_path: i32, edge_id: i32) -> PathEdge {
        let query =
            "INSERT INTO path_edges (path_id, index_in_path, edge_id) VALUES (?1, ?2, ?3) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map((path_id, index_in_path, edge_id), |row| {
                Ok(PathEdge {
                    id: row.get(0)?,
                    path_id,
                    index_in_path,
                    edge_id,
                })
            })
            .unwrap();
        match rows.next().unwrap() {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    let mut placeholders = vec![path_id];
                    let query = "SELECT id from path_edges where path_id = ?1 AND edge_id = ?2;";
                    placeholders.push(edge_id);
                    println!("{query} {placeholders:?}");
                    PathEdge {
                        id: conn
                            .query_row(query, params_from_iter(&placeholders), |row| row.get(0))
                            .unwrap(),
                        path_id,
                        index_in_path,
                        edge_id,
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
                    index_in_path: row.get(2)?,
                    edge_id: row.get(3)?,
                })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }

    pub fn edges_for(conn: &Connection, path_id: i32) -> Vec<Edge> {
        let path_edges = PathEdge::query(
            conn,
            "select * from path_edges where path_id = ?1 order by index_in_path ASC",
            vec![Value::from(path_id)],
        );
        let edge_ids = path_edges.into_iter().map(|path_edge| path_edge.edge_id);
        let edges = Edge::bulk_load(conn, edge_ids.clone().collect());
        let edges_by_id = edges
            .into_iter()
            .map(|edge| (edge.id, edge))
            .collect::<HashMap<i32, Edge>>();
        edge_ids
            .into_iter()
            .map(|edge_id| edges_by_id[&edge_id].clone())
            .collect::<Vec<Edge>>()
    }
}

mod tests {}
