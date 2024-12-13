use crate::models::edge::{Edge, EdgeData};
use crate::models::traits::*;
use rusqlite;
use rusqlite::types::Value;
use rusqlite::{Connection, Row};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct NewBlockGroupEdge {
    pub id: i64,
    pub block_group_id: i64,
    pub edge_id: i64,
    pub chromosome_index: i64,
    pub phased: i64,
    pub source_phase_layer_id: i64,
    pub target_phase_layer_id: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NewBlockGroupEdgeData {
    pub block_group_id: i64,
    pub edge_id: i64,
    pub chromosome_index: i64,
    pub phased: i64,
    pub source_phase_layer_id: i64,
    pub target_phase_layer_id: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NewAugmentedEdge {
    pub edge: Edge,
    pub chromosome_index: i64,
    pub phased: i64,
    pub source_phase_layer_id: i64,
    pub target_phase_layer_id: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NewAugmentedEdgeData {
    pub edge_data: EdgeData,
    pub chromosome_index: i64,
    pub phased: i64,
    pub source_phase_layer_id: i64,
    pub target_phase_layer_id: i64,
}

impl Query for NewBlockGroupEdge {
    type Model = NewBlockGroupEdge;
    fn process_row(row: &Row) -> Self::Model {
        NewBlockGroupEdge {
            id: row.get(0).unwrap(),
            block_group_id: row.get(1).unwrap(),
            edge_id: row.get(2).unwrap(),
            chromosome_index: row.get(3).unwrap(),
            phased: row.get(4).unwrap(),
            source_phase_layer_id: row.get(5).unwrap(),
            target_phase_layer_id: row.get(6).unwrap(),
        }
    }
}

impl NewBlockGroupEdge {
    pub fn bulk_create(conn: &Connection, block_group_edges: &[NewBlockGroupEdgeData]) {
        for chunk in block_group_edges.chunks(100000) {
            let mut rows_to_insert = vec![];
            for block_group_edge in chunk {
                let row = format!(
                    "({0}, {1}, {2}, {3}, {4}, {5})",
                    block_group_edge.block_group_id,
                    block_group_edge.edge_id,
                    block_group_edge.chromosome_index,
                    block_group_edge.phased,
                    block_group_edge.source_phase_layer_id,
                    block_group_edge.target_phase_layer_id,
                );
                rows_to_insert.push(row);
            }

            let formatted_rows_to_insert = rows_to_insert.join(", ");

            let insert_statement = format!(
                "INSERT OR IGNORE INTO block_group_edges (block_group_id, edge_id, chromosome_index, phased, source_phase_layer_id, target_phase_layer_id) VALUES {0};",
                formatted_rows_to_insert
            );
            let _ = conn.execute(&insert_statement, ());
        }
    }

    pub fn edges_for_block_group(conn: &Connection, block_group_id: i64) -> Vec<NewAugmentedEdge> {
        let block_group_edges = NewBlockGroupEdge::query(
            conn,
            "select * from block_group_edges where block_group_id = ?1;",
            rusqlite::params!(Value::from(block_group_id)),
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
            .clone()
            .into_iter()
            .map(|block_group_edge| (block_group_edge.edge_id, block_group_edge.phased))
            .collect::<HashMap<i64, i64>>();
        let source_phase_layer_id_by_edge_id = block_group_edges
            .clone()
            .into_iter()
            .map(|block_group_edge| {
                (
                    block_group_edge.edge_id,
                    block_group_edge.source_phase_layer_id,
                )
            })
            .collect::<HashMap<i64, i64>>();
        let target_phase_layer_id_by_edge_id = block_group_edges
            .clone()
            .into_iter()
            .map(|block_group_edge| {
                (
                    block_group_edge.edge_id,
                    block_group_edge.target_phase_layer_id,
                )
            })
            .collect::<HashMap<i64, i64>>();
        let edges = Edge::bulk_load(conn, &edge_ids);
        edges
            .into_iter()
            .map(|edge| NewAugmentedEdge {
                edge: edge.clone(),
                chromosome_index: *chromosome_index_by_edge_id.get(&edge.id).unwrap(),
                phased: *phased_by_edge_id.get(&edge.id).unwrap(),
                source_phase_layer_id: *source_phase_layer_id_by_edge_id.get(&edge.id).unwrap(),
                target_phase_layer_id: *target_phase_layer_id_by_edge_id.get(&edge.id).unwrap(),
            })
            .collect()
    }
}
