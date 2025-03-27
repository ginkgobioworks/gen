use crate::models::edge::{Edge, EdgeData};
use crate::models::traits::*;
use rusqlite;
use rusqlite::types::Value;
use rusqlite::{Connection, Row};
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct BlockGroupEdge {
    pub id: i64,
    pub block_group_id: i64,
    pub edge_id: i64,
    pub chromosome_index: i64,
    pub phased: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct BlockGroupEdgeData {
    pub block_group_id: i64,
    pub edge_id: i64,
    pub chromosome_index: i64,
    pub phased: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct AugmentedEdge {
    pub edge: Edge,
    pub chromosome_index: i64,
    pub phased: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct AugmentedEdgeData {
    pub edge_data: EdgeData,
    pub chromosome_index: i64,
    pub phased: i64,
}

impl Query for BlockGroupEdge {
    type Model = BlockGroupEdge;
    fn process_row(row: &Row) -> Self::Model {
        BlockGroupEdge {
            id: row.get(0).unwrap(),
            block_group_id: row.get(1).unwrap(),
            edge_id: row.get(2).unwrap(),
            chromosome_index: row.get(3).unwrap(),
            phased: row.get(4).unwrap(),
        }
    }
}

impl BlockGroupEdge {
    pub fn bulk_create(conn: &Connection, block_group_edges: &[BlockGroupEdgeData]) {
        for chunk in block_group_edges.chunks(100000) {
            let mut rows_to_insert = vec![];
            for block_group_edge in chunk {
                let row = format!(
                    "({0}, {1}, {2}, {3})",
                    block_group_edge.block_group_id,
                    block_group_edge.edge_id,
                    block_group_edge.chromosome_index,
                    block_group_edge.phased,
                );
                rows_to_insert.push(row);
            }

            let formatted_rows_to_insert = rows_to_insert.join(", ");

            let insert_statement = format!(
                "INSERT OR IGNORE INTO block_group_edges (block_group_id, edge_id, chromosome_index, phased) VALUES {0};",
                formatted_rows_to_insert
            );
            let _ = conn.execute(&insert_statement, ());
        }
    }

    pub fn edges_for_block_group(conn: &Connection, block_group_id: i64) -> Vec<AugmentedEdge> {
        let block_group_edges = BlockGroupEdge::query(
            conn,
            "select * from block_group_edges where block_group_id = ?1;",
            rusqlite::params!(Value::from(block_group_id)),
        );
        let edge_ids = block_group_edges
            .clone()
            .into_iter()
            .map(|block_group_edge| block_group_edge.edge_id)
            .collect::<Vec<i64>>();
        let edges = Edge::bulk_load(conn, &edge_ids);
        let edge_map = edges
            .iter()
            .map(|edge| (edge.id, edge))
            .collect::<HashMap<i64, &Edge>>();
        block_group_edges
            .into_iter()
            .map(|bge| {
                let edge_info = *edge_map.get(&bge.edge_id).unwrap();
                AugmentedEdge {
                    edge: edge_info.clone(),
                    chromosome_index: bge.chromosome_index,
                    phased: bge.phased,
                }
            })
            .collect()
    }

    pub fn specific_edges_for_block_group(
        conn: &Connection,
        block_group_id: i64,
        edge_ids: &[i64],
    ) -> Vec<AugmentedEdge> {
        let block_group_edges = BlockGroupEdge::query(
            conn,
            "SELECT * FROM block_group_edges WHERE block_group_id = ?1 AND edge_id in rarray(?2);",
            rusqlite::params!(
                Value::from(block_group_id),
                Rc::new(
                    edge_ids
                        .iter()
                        .map(|x| Value::from(*x))
                        .collect::<Vec<Value>>()
                )
            ),
        );
        let edge_ids = block_group_edges
            .clone()
            .into_iter()
            .map(|block_group_edge| block_group_edge.edge_id)
            .collect::<Vec<i64>>();
        let chromosome_index_by_edge_id = block_group_edges
            .clone()
            .into_iter()
            .map(|block_group_edge| (block_group_edge.edge_id, block_group_edge.chromosome_index))
            .collect::<HashMap<i64, i64>>();
        let phased_by_edge_id = block_group_edges
            .into_iter()
            .map(|block_group_edge| (block_group_edge.edge_id, block_group_edge.phased))
            .collect::<HashMap<i64, i64>>();
        let edges = Edge::bulk_load(conn, &edge_ids);
        edges
            .into_iter()
            .map(|edge| AugmentedEdge {
                edge: edge.clone(),
                chromosome_index: *chromosome_index_by_edge_id.get(&edge.id).unwrap(),
                phased: *phased_by_edge_id.get(&edge.id).unwrap(),
            })
            .collect()
    }
}
