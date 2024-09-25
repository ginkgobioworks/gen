use rusqlite::{params_from_iter, types::Value as SQLValue, Connection};

use crate::models::sequence::Sequence;

pub const BOGUS_SOURCE_NODE_ID: i32 = -1;
pub const BOGUS_TARGET_NODE_ID: i32 = -2;

pub const PATH_START_NODE_ID: i32 = 1;
pub const PATH_END_NODE_ID: i32 = 2;

#[derive(Clone, Debug)]
pub struct Node {
    pub id: i32,
    pub sequence_hash: String,
}

impl Node {
    pub fn create(conn: &Connection, sequence_hash: &str) -> Node {
        let insert_statement = format!(
            "INSERT INTO nodes (sequence_hash) VALUES ('{}');",
            sequence_hash
        );
        let _ = conn.execute(&insert_statement, ());
        Node {
            id: conn.last_insert_rowid() as i32,
            sequence_hash: sequence_hash.to_string(),
        }
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

    pub fn sequences_from_node_ids(conn: &Connection, node_ids: Vec<i32>) -> Vec<Sequence> {
        let nodes = Node::get_nodes(conn, node_ids);
        let sequence_hashes = nodes
            .iter()
            .map(|node| node.sequence_hash.as_str())
            .collect::<Vec<&str>>();
        Sequence::sequences_by_hash(conn, sequence_hashes)
            .values()
            .cloned()
            .collect()
    }
}
