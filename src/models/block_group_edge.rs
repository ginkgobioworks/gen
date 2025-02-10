use crate::models::edge::{Edge, EdgeData};
use crate::models::traits::*;
use rusqlite;
use rusqlite::types::Value;
use rusqlite::{Connection, Result as SQLResult, Row};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::RandomState;
use std::rc::Rc;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct BlockGroupEdge {
    pub id: i64,
    pub block_group_id: i64,
    pub edge_id: i64,
    pub chromosome_index: i64,
    pub phased: i64,
    pub source_phase_layer_id: i64,
    pub target_phase_layer_id: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BlockGroupEdgeData {
    pub block_group_id: i64,
    pub edge_id: i64,
    pub chromosome_index: i64,
    pub phased: i64,
    pub source_phase_layer_id: i64,
    pub target_phase_layer_id: i64,
}

impl From<&BlockGroupEdge> for BlockGroupEdgeData {
    fn from(item: &BlockGroupEdge) -> Self {
        BlockGroupEdgeData {
            block_group_id: item.block_group_id,
            edge_id: item.edge_id,
            chromosome_index: item.chromosome_index,
            phased: item.phased,
            source_phase_layer_id: item.source_phase_layer_id,
            target_phase_layer_id: item.target_phase_layer_id,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NewAugmentedEdge {
    pub edge: Edge,
    pub block_group_edge_id: i64,
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

impl Query for BlockGroupEdge {
    type Model = BlockGroupEdge;
    fn process_row(row: &Row) -> Self::Model {
        BlockGroupEdge {
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

impl BlockGroupEdge {
    pub fn bulk_create(conn: &Connection, block_group_edges: &[BlockGroupEdgeData]) -> Vec<i64> {
        let mut block_group_edge_rows = vec![];
        let mut block_group_edge_map: HashMap<BlockGroupEdgeData, i64> = HashMap::new();
        for block_group_edge in block_group_edges {
            let block_group_edge_row = format!(
                "({0}, {1}, {2}, {3}, {4}, {5})",
                block_group_edge.block_group_id,
                block_group_edge.edge_id,
                block_group_edge.chromosome_index,
                block_group_edge.phased,
                block_group_edge.source_phase_layer_id,
                block_group_edge.target_phase_layer_id,
            );
            block_group_edge_rows.push(block_group_edge_row);
        }
        let formatted_block_group_edge_rows = block_group_edge_rows.join(", ");

        let select_statement = format!("SELECT * FROM block_group_edges WHERE (block_group_id, edge_id, chromosome_index, phased, source_phase_layer_id, target_phase_layer_id) in ({0});", formatted_block_group_edge_rows);
        let existing_block_group_edges =
            BlockGroupEdge::query(conn, &select_statement, rusqlite::params!());
        for block_group_edge in existing_block_group_edges.iter() {
            block_group_edge_map.insert(
                BlockGroupEdgeData::from(block_group_edge),
                block_group_edge.id,
            );
        }

        let existing_block_group_edge_set = HashSet::<BlockGroupEdgeData, RandomState>::from_iter(
            existing_block_group_edges
                .into_iter()
                .map(BlockGroupEdge::to_data),
        );
        let mut block_group_edges_to_insert = HashSet::new();
        for block_group_edge in block_group_edges {
            if !existing_block_group_edge_set.contains(block_group_edge) {
                block_group_edges_to_insert.insert(block_group_edge);
            }
        }

        let mut block_group_edge_rows_to_insert = vec![];
        for block_group_edge in block_group_edges_to_insert {
            let block_group_edge_row = format!(
                "({0}, {1}, {2}, {3}, {4}, {5})",
                block_group_edge.block_group_id,
                block_group_edge.edge_id,
                block_group_edge.chromosome_index,
                block_group_edge.phased,
                block_group_edge.source_phase_layer_id,
                block_group_edge.target_phase_layer_id,
            );
            block_group_edge_rows_to_insert.push(block_group_edge_row);
        }

        if !block_group_edge_rows_to_insert.is_empty() {
            for chunk in block_group_edge_rows_to_insert.chunks(100000) {
                let formatted_block_group_edge_rows_to_insert = chunk.join(", ");

                let insert_statement = format!("INSERT INTO block_group_edges (block_group_id, edge_id, chromosome_index, phased, source_phase_layer_id, target_phase_layer_id) VALUES {0} RETURNING *;", formatted_block_group_edge_rows_to_insert);
                let mut stmt = conn.prepare(&insert_statement).unwrap();
                let rows = stmt
                    .query_map([], BlockGroupEdge::block_group_edge_from_row)
                    .unwrap();
                for row in rows {
                    let block_group_edge = row.unwrap();
                    block_group_edge_map.insert(
                        BlockGroupEdgeData::from(&block_group_edge),
                        block_group_edge.id,
                    );
                }
            }
        }
        block_group_edges
            .iter()
            .map(|block_group_edge| *block_group_edge_map.get(block_group_edge).unwrap())
            .collect::<Vec<i64>>()
    }

    fn block_group_edge_from_row(row: &Row) -> SQLResult<BlockGroupEdge> {
        Ok(BlockGroupEdge {
            id: row.get(0)?,
            block_group_id: row.get(1)?,
            edge_id: row.get(2)?,
            chromosome_index: row.get(3)?,
            phased: row.get(4)?,
            source_phase_layer_id: row.get(5)?,
            target_phase_layer_id: row.get(6)?,
        })
    }

    pub fn to_data(block_group_edge: BlockGroupEdge) -> BlockGroupEdgeData {
        BlockGroupEdgeData {
            block_group_id: block_group_edge.block_group_id,
            edge_id: block_group_edge.edge_id,
            chromosome_index: block_group_edge.chromosome_index,
            phased: block_group_edge.phased,
            source_phase_layer_id: block_group_edge.source_phase_layer_id,
            target_phase_layer_id: block_group_edge.target_phase_layer_id,
        }
    }

    pub fn edges_for_block_group(conn: &Connection, block_group_id: i64) -> Vec<NewAugmentedEdge> {
        let block_group_edges = BlockGroupEdge::query(
            conn,
            "select * from block_group_edges where block_group_id = ?1;",
            rusqlite::params!(Value::from(block_group_id)),
        );
        let block_group_edge_ids = block_group_edges
            .clone()
            .into_iter()
            .map(|block_group_edge| block_group_edge.id)
            .collect::<Vec<i64>>();
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
            .enumerate()
            .map(|(i, edge)| NewAugmentedEdge {
                edge: edge.clone(),
                block_group_edge_id: block_group_edge_ids[i],
                chromosome_index: *chromosome_index_by_edge_id.get(&edge.id).unwrap(),
                phased: *phased_by_edge_id.get(&edge.id).unwrap(),
                source_phase_layer_id: *source_phase_layer_id_by_edge_id.get(&edge.id).unwrap(),
                target_phase_layer_id: *target_phase_layer_id_by_edge_id.get(&edge.id).unwrap(),
            })
            .collect()
    }

    pub fn load_block_group_edges(
        conn: &Connection,
        block_group_edge_ids: &[i64],
    ) -> Vec<BlockGroupEdge> {
        let query_block_group_edge_ids: Vec<Value> = block_group_edge_ids
            .iter()
            .map(|block_group_edge_id| Value::from(*block_group_edge_id))
            .collect();
        let query = "select id, block_group_id, edge_id, chromosome_index, phased, source_phase_layer_id, target_phase_layer_id from block_group_edges where id in rarray(?1);";
        let block_group_edges = BlockGroupEdge::query(
            conn,
            query,
            rusqlite::params!(Rc::new(query_block_group_edge_ids)),
        );
        let block_group_edges_by_id = block_group_edges
            .clone()
            .into_iter()
            .map(|block_group_edge| (block_group_edge.id, block_group_edge))
            .collect::<HashMap<i64, BlockGroupEdge>>();
        block_group_edge_ids
            .iter()
            .map(|block_group_edge_id| {
                block_group_edges_by_id
                    .get(block_group_edge_id)
                    .unwrap()
                    .clone()
            })
            .collect()
    }

    pub fn specific_edges_for_block_group(
        conn: &Connection,
        block_group_id: i64,
        edge_ids: &[i64],
    ) -> Vec<NewAugmentedEdge> {
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
        let block_group_edge_ids = block_group_edges
            .clone()
            .into_iter()
            .map(|block_group_edge| block_group_edge.id)
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
            .enumerate()
            .map(|(i, edge)| NewAugmentedEdge {
                edge: edge.clone(),
                block_group_edge_id: block_group_edge_ids[i],
                chromosome_index: *chromosome_index_by_edge_id.get(&edge.id).unwrap(),
                phased: *phased_by_edge_id.get(&edge.id).unwrap(),
                source_phase_layer_id: *source_phase_layer_id_by_edge_id.get(&edge.id).unwrap(),
                target_phase_layer_id: *target_phase_layer_id_by_edge_id.get(&edge.id).unwrap(),
            })
            .collect()
    }
}
