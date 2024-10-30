use crate::models::edge::Edge;
use crate::models::traits::*;
use rusqlite::{Connection, Row};

#[derive(Clone, Debug)]
pub struct BlockGroupEdge {
    pub id: i64,
    pub block_group_id: i64,
    pub edge_id: i64,
}

impl Query for BlockGroupEdge {
    type Model = BlockGroupEdge;
    fn process_row(row: &Row) -> Self::Model {
        BlockGroupEdge {
            id: row.get(0).unwrap(),
            block_group_id: row.get(1).unwrap(),
            edge_id: row.get(2).unwrap(),
        }
    }
}

impl BlockGroupEdge {
    pub fn bulk_create(conn: &Connection, block_group_id: i64, edge_ids: &[i64]) {
        for chunk in edge_ids.chunks(100000) {
            let mut rows_to_insert = vec![];
            for edge_id in chunk {
                let row = format!("({0}, {1})", block_group_id, edge_id);
                rows_to_insert.push(row);
            }

            let formatted_rows_to_insert = rows_to_insert.join(", ");

            let insert_statement = format!(
                "INSERT OR IGNORE INTO block_group_edges (block_group_id, edge_id) VALUES {0};",
                formatted_rows_to_insert
            );
            let _ = conn.execute(&insert_statement, ());
        }
    }

    pub fn edges_for_block_group(conn: &Connection, block_group_id: i64) -> Vec<Edge> {
        let query = format!(
            "select * from block_group_edges where block_group_id = {};",
            block_group_id
        );
        let block_group_edges = BlockGroupEdge::query(conn, &query, vec![]);
        let edge_ids = block_group_edges
            .into_iter()
            .map(|block_group_edge| block_group_edge.edge_id)
            .collect::<Vec<i64>>();
        Edge::bulk_load(conn, &edge_ids)
    }
}
