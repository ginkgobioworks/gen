use rusqlite::{params_from_iter, types::Value as SQLValue, Connection};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::models::sequence::Sequence;

pub const PATH_START_NODE_ID: i32 = 1;
pub const PATH_END_NODE_ID: i32 = 2;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Node {
    pub id: i32,
    pub sequence_hash: String,
}

impl Node {
    pub fn create(conn: &Connection, sequence_hash: &str) -> i32 {
        let insert_statement = format!(
            "INSERT INTO nodes (sequence_hash) VALUES ('{}');",
            sequence_hash
        );
        let _ = conn.execute(&insert_statement, ());
        conn.last_insert_rowid() as i32
    }

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<SQLValue>) -> Vec<Node> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(Node {
                    id: row.get(0)?,
                    sequence_hash: row.get(1)?,
                })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }

    pub fn get_nodes(conn: &Connection, node_ids: Vec<i32>) -> Vec<Node> {
        Node::query(
            conn,
            &format!(
                "SELECT * FROM nodes WHERE id IN ({})",
                node_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ")
            ),
            node_ids
                .iter()
                .map(|id| SQLValue::Integer(*id as i64))
                .collect(),
        )
    }

    pub fn get_sequences_by_node_ids(
        conn: &Connection,
        node_ids: Vec<i32>,
    ) -> HashMap<i32, Sequence> {
        let nodes = Node::get_nodes(conn, node_ids.into_iter().collect::<Vec<i32>>());
        let sequence_hashes_by_node_id = nodes
            .iter()
            .map(|node| (node.id, node.sequence_hash.clone()))
            .collect::<HashMap<i32, String>>();
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
            .collect::<HashMap<i32, Sequence>>()
    }
}
