use crate::models::edge::Edge;
use crate::models::operations::Operation;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};

#[derive(Clone, Debug)]
pub struct BlockGroupEdge {
    pub id: i32,
    pub block_group_id: i32,
    pub edge_id: i32,
}

impl BlockGroupEdge {
    pub fn bulk_create(
        conn: &Connection,
        block_group_id: i32,
        edge_ids: Vec<i32>,
    ) -> Vec<BlockGroupEdge> {
        let mut results = vec![];
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
            let query_stmt = conn.prepare_cached(&format!("select * from block_group_edges where (block_group_id, edge_id) IN ({formatted_rows_to_insert})"));
            for res in query_stmt
                .unwrap()
                .query_map((), |row| {
                    Ok(BlockGroupEdge {
                        id: row.get(0)?,
                        block_group_id: row.get(1)?,
                        edge_id: row.get(2)?,
                    })
                })
                .unwrap()
            {
                results.push(res.unwrap());
            }
        }
        results
    }

    pub fn edges_for_block_group(conn: &Connection, block_group_id: i32) -> Vec<Edge> {
        let valid_bg_edge_ids = Operation::get_valid_blockgroup_edge_ids(conn);
        let query = match valid_bg_edge_ids {
            Some(v) => format!(
            "select * from block_group_edges where block_group_id = {block_group_id} AND id IN ({ids});", ids=v.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ")
                ),
            None => format!(
                "select * from block_group_edges where block_group_id = {block_group_id};",
            )
        };

        println!("vg {query:?}");
        let block_group_edges = BlockGroupEdge::query(conn, &query, vec![]);
        let edge_ids = block_group_edges
            .into_iter()
            .map(|block_group_edge| block_group_edge.edge_id)
            .collect();
        println!("found edge ids {edge_ids:?}");
        Edge::bulk_load(conn, edge_ids)
    }

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<BlockGroupEdge> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(BlockGroupEdge {
                    id: row.get(0)?,
                    block_group_id: row.get(1)?,
                    edge_id: row.get(2)?,
                })
            })
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }
}
