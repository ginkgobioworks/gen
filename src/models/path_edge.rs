use itertools::Itertools;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection, Row};
use std::collections::HashMap;

use crate::models::edge::Edge;
use crate::models::traits::*;

#[derive(Clone, Debug)]
pub struct PathEdge {
    pub id: i64,
    pub path_id: i64,
    pub index_in_path: i64,
    pub edge_id: i64,
}

impl Query for PathEdge {
    type Model = PathEdge;
    fn process_row(row: &Row) -> Self::Model {
        PathEdge {
            id: row.get(0).unwrap(),
            path_id: row.get(1).unwrap(),
            index_in_path: row.get(2).unwrap(),
            edge_id: row.get(3).unwrap(),
        }
    }
}

impl PathEdge {
    pub fn create(conn: &Connection, path_id: i64, index_in_path: i64, edge_id: i64) -> PathEdge {
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

    pub fn bulk_create(conn: &Connection, path_id: i64, edge_ids: &[i64]) {
        for (index1, chunk) in edge_ids.chunks(100000).enumerate() {
            let mut rows_to_insert = vec![];
            for (index2, edge_id) in chunk.iter().enumerate() {
                let row = format!(
                    "({0}, {1}, {2})",
                    path_id,
                    edge_id,
                    index1 * 100000 + index2
                );
                rows_to_insert.push(row);
            }

            let formatted_rows_to_insert = rows_to_insert.join(", ");

            let insert_statement = format!(
                "INSERT OR IGNORE INTO path_edges (path_id, edge_id, index_in_path) VALUES {0};",
                formatted_rows_to_insert
            );
            let _ = conn.execute(&insert_statement, ());
        }
    }

    pub fn edges_for_path(conn: &Connection, path_id: i64) -> Vec<Edge> {
        let path_edges = PathEdge::query(
            conn,
            "select * from path_edges where path_id = ?1 order by index_in_path ASC",
            vec![Value::from(path_id)],
        );
        let edge_ids = path_edges
            .into_iter()
            .map(|path_edge| path_edge.edge_id)
            .collect::<Vec<i64>>();
        let edges = Edge::bulk_load(conn, &edge_ids);
        let edges_by_id = edges
            .into_iter()
            .map(|edge| (edge.id, edge))
            .collect::<HashMap<i64, Edge>>();
        edge_ids
            .into_iter()
            .map(|edge_id| edges_by_id[&edge_id].clone())
            .collect::<Vec<Edge>>()
    }

    pub fn edges_for_paths(conn: &Connection, path_ids: Vec<i64>) -> HashMap<i64, Vec<Edge>> {
        let placeholder_string = path_ids.iter().map(|_| "?").join(",");
        let path_edges = PathEdge::query(
            conn,
            format!(
                "select * from path_edges where path_id in ({}) ORDER BY path_id, index_in_path",
                placeholder_string
            )
            .as_str(),
            path_ids
                .into_iter()
                .map(Value::from)
                .collect::<Vec<Value>>(),
        );
        let edge_ids = path_edges
            .clone()
            .into_iter()
            .map(|path_edge| path_edge.edge_id)
            .collect::<Vec<i64>>();
        let edges = Edge::bulk_load(conn, &edge_ids);
        let edges_by_id = edges
            .into_iter()
            .map(|edge| (edge.id, edge))
            .collect::<HashMap<i64, Edge>>();
        let path_edges_by_path_id = path_edges
            .into_iter()
            .map(|path_edge| (path_edge.path_id, path_edge.edge_id))
            .into_group_map();
        path_edges_by_path_id
            .into_iter()
            .map(|(path_id, edge_ids)| {
                (
                    path_id,
                    edge_ids
                        .into_iter()
                        .map(|edge_id| edges_by_id[&edge_id].clone())
                        .collect::<Vec<Edge>>(),
                )
            })
            .collect::<HashMap<i64, Vec<Edge>>>()
    }
}

mod tests {}
