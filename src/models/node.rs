use rusqlite::{params_from_iter, types::Value as SQLValue, Connection, Row};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::rc::Rc;

use crate::models::sequence::Sequence;
use crate::models::traits::*;

pub const PATH_START_NODE_ID: i64 = 1;
pub const PATH_END_NODE_ID: i64 = 2;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Node {
    pub id: i64,
    pub sequence_hash: String,
}

impl Query for Node {
    type Model = Node;
    fn process_row(row: &Row) -> Self::Model {
        Node {
            id: row.get(0).unwrap(),
            sequence_hash: row.get(1).unwrap(),
        }
    }
}

impl Node {
    pub fn create(
        conn: &Connection,
        sequence_hash: &str,
        node_hash: impl Into<Option<String>>,
    ) -> i64 {
        let node_hash = node_hash.into();
        let insert_statement =
            "INSERT INTO nodes (sequence_hash, hash) VALUES (?1, ?2) RETURNING (id);";
        let mut stmt = conn.prepare_cached(insert_statement).unwrap();
        let mut rows = stmt
            .query_map(
                params_from_iter(vec![
                    SQLValue::from(sequence_hash.to_string()),
                    SQLValue::from(node_hash.clone()),
                ]),
                |row| row.get(0),
            )
            .unwrap();
        match rows.next().unwrap() {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    let placeholders = vec![node_hash.unwrap()];
                    let query = "SELECT id from nodes where hash = ?1;";
                    conn.query_row(query, params_from_iter(&placeholders), |row| row.get(0))
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

    pub fn get_nodes(conn: &Connection, node_ids: Vec<i64>) -> Vec<Node> {
        let mut nodes: Vec<Node> = vec![];
        for chunk in node_ids.chunks(1000) {
            let query_node_ids: Vec<SQLValue> = chunk
                .iter()
                .map(|node_id| SQLValue::from(*node_id))
                .collect();
            nodes.extend(Node::query(
                conn,
                "SELECT * FROM nodes WHERE id IN rarray(?1);",
                rusqlite::params!(Rc::new(query_node_ids)),
            ))
        }
        nodes
    }

    pub fn get_sequences_by_node_ids(
        conn: &Connection,
        node_ids: Vec<i64>,
    ) -> HashMap<i64, Sequence> {
        let nodes = Node::get_nodes(conn, node_ids.into_iter().collect::<Vec<i64>>());
        let sequence_hashes_by_node_id = nodes
            .iter()
            .map(|node| (node.id, node.sequence_hash.clone()))
            .collect::<HashMap<i64, String>>();
        let sequences_by_hash = Sequence::sequences_by_hash(
            conn,
            sequence_hashes_by_node_id
                .values()
                .map(|hash| hash.as_str())
                .collect::<Vec<&str>>(),
        );
        sequence_hashes_by_node_id
            .clone()
            .into_iter()
            .map(|(node_id, sequence_hash)| {
                (
                    node_id,
                    sequences_by_hash.get(&sequence_hash).unwrap().clone(),
                )
            })
            .collect::<HashMap<i64, Sequence>>()
    }

    pub fn is_terminal(node_id: i64) -> bool {
        node_id == PATH_START_NODE_ID || node_id == PATH_END_NODE_ID
    }
}
