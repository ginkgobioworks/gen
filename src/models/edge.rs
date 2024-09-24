use std::collections::{HashMap, HashSet};
use std::hash::{Hash, RandomState};

use itertools::Itertools;
use petgraph::graphmap::DiGraphMap;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection, Result as SQLResult, Row};
use serde::{Deserialize, Serialize};

use crate::models::node::{BOGUS_SOURCE_NODE_ID, BOGUS_TARGET_NODE_ID};
use crate::models::{sequence::Sequence, strand::Strand};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct Edge {
    pub id: i32,
    pub source_hash: String,
    pub source_node_id: i32,
    pub source_coordinate: i32,
    pub source_strand: Strand,
    pub target_hash: String,
    pub target_node_id: i32,
    pub target_coordinate: i32,
    pub target_strand: Strand,
    pub chromosome_index: i32,
    pub phased: i32,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EdgeData {
    pub source_hash: String,
    pub source_node_id: i32,
    pub source_coordinate: i32,
    pub source_strand: Strand,
    pub target_hash: String,
    pub target_node_id: i32,
    pub target_coordinate: i32,
    pub target_strand: Strand,
    pub chromosome_index: i32,
    pub phased: i32,
}

impl From<&Edge> for EdgeData {
    fn from(item: &Edge) -> Self {
        EdgeData {
            source_hash: item.source_hash.clone(),
            source_node_id: item.source_node_id,
            source_coordinate: item.source_coordinate,
            source_strand: item.source_strand,
            target_hash: item.target_hash.clone(),
            target_node_id: item.target_node_id,
            target_coordinate: item.target_coordinate,
            target_strand: item.target_strand,
            chromosome_index: item.chromosome_index,
            phased: item.phased,
        }
    }
}

#[derive(Eq, Hash, PartialEq)]
pub struct BlockKey {
    pub sequence_hash: String,
    pub coordinate: i32,
}

#[derive(Clone, Debug)]
pub struct GroupBlock {
    pub id: i32,
    pub sequence_hash: String,
    pub sequence: String,
    pub start: i32,
    pub end: i32,
}

impl Edge {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        conn: &Connection,
        source_hash: String,
        source_coordinate: i32,
        source_strand: Strand,
        target_hash: String,
        target_coordinate: i32,
        target_strand: Strand,
        chromosome_index: i32,
        phased: i32,
    ) -> Edge {
        let query = "INSERT INTO edges (source_hash, source_node_id, source_coordinate, source_strand, target_hash, target_node_id, target_coordinate, target_strand, chromosome_index, phased) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) RETURNING *";
        let id_query = "select id from edges where source_hash = ?1 and source_coordinate = ?2 and source_strand = ?3 and target_hash = ?4 and target_coordinate = ?5 and target_strand = ?6 and chromosome_index = ?7 and phased = ?8";
        let placeholders: Vec<Value> = vec![
            source_hash.clone().into(),
            BOGUS_SOURCE_NODE_ID.into(),
            source_coordinate.into(),
            source_strand.into(),
            target_hash.clone().into(),
            BOGUS_TARGET_NODE_ID.into(),
            target_coordinate.into(),
            target_strand.into(),
            chromosome_index.into(),
            phased.into(),
        ];

        let mut stmt = conn.prepare(query).unwrap();
        match stmt.query_row(params_from_iter(&placeholders), |row| {
            Ok(Edge {
                id: row.get(0)?,
                source_hash: row.get(1)?,
                source_node_id: row.get(2)?,
                source_coordinate: row.get(3)?,
                source_strand: row.get(4)?,
                target_hash: row.get(5)?,
                target_node_id: row.get(6)?,
                target_coordinate: row.get(7)?,
                target_strand: row.get(8)?,
                chromosome_index: row.get(9)?,
                phased: row.get(10)?,
            })
        }) {
            Ok(edge) => edge,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    Edge {
                        id: conn
                            .query_row(id_query, params_from_iter(&placeholders), |row| row.get(0))
                            .unwrap(),
                        source_hash,
                        source_node_id: BOGUS_SOURCE_NODE_ID,
                        source_coordinate,
                        source_strand,
                        target_hash,
                        target_node_id: BOGUS_TARGET_NODE_ID,
                        target_coordinate,
                        target_strand,
                        chromosome_index,
                        phased,
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

    fn edge_from_row(row: &Row) -> SQLResult<Edge> {
        Ok(Edge {
            id: row.get(0)?,
            source_hash: row.get(1)?,
            source_node_id: BOGUS_SOURCE_NODE_ID,
            source_coordinate: row.get(3)?,
            source_strand: row.get(4)?,
            target_hash: row.get(5)?,
            target_node_id: BOGUS_TARGET_NODE_ID,
            target_coordinate: row.get(7)?,
            target_strand: row.get(8)?,
            chromosome_index: row.get(9)?,
            phased: row.get(10)?,
        })
    }

    pub fn bulk_load(conn: &Connection, edge_ids: &[i32]) -> Vec<Edge> {
        let formatted_edge_ids = edge_ids
            .iter()
            .map(|edge_id| edge_id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let query = format!("select id, source_hash, source_node_id, source_coordinate, source_strand, target_hash, target_node_id, target_coordinate, target_strand, chromosome_index, phased from edges where id in ({});", formatted_edge_ids);
        Edge::query(conn, &query, vec![])
    }

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<Edge> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), Edge::edge_from_row)
            .unwrap();
        let mut edges = vec![];
        for row in rows {
            edges.push(row.unwrap());
        }
        edges
    }

    pub fn bulk_create(conn: &Connection, edges: Vec<EdgeData>) -> Vec<i32> {
        let mut edge_rows = vec![];
        let mut edge_map: HashMap<EdgeData, i32> = HashMap::new();
        for edge in &edges {
            let source_hash = format!("\"{0}\"", edge.source_hash);
            let source_strand = format!("\"{0}\"", edge.source_strand);
            let target_hash = format!("\"{0}\"", edge.target_hash);
            let target_strand = format!("\"{0}\"", edge.target_strand);
            let edge_row = format!(
                "({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7})",
                source_hash,
                edge.source_coordinate,
                source_strand,
                target_hash,
                edge.target_coordinate,
                target_strand,
                edge.chromosome_index,
                edge.phased
            );
            edge_rows.push(edge_row);
        }
        let formatted_edge_rows = edge_rows.join(", ");

        let select_statement = format!("SELECT * FROM edges WHERE (source_hash, source_coordinate, source_strand, target_hash, target_coordinate, target_strand, chromosome_index, phased) in ({0});", formatted_edge_rows);
        let existing_edges = Edge::query(conn, &select_statement, vec![]);
        for edge in existing_edges.iter() {
            edge_map.insert(EdgeData::from(edge), edge.id);
        }

        let existing_edge_set = HashSet::<EdgeData, RandomState>::from_iter(
            existing_edges.into_iter().map(Edge::to_data),
        );
        let mut edges_to_insert = HashSet::new();
        for edge in &edges {
            if !existing_edge_set.contains(edge) {
                edges_to_insert.insert(edge);
            }
        }

        let mut edge_rows_to_insert = vec![];
        for edge in edges_to_insert {
            let source_hash = format!("\"{0}\"", edge.source_hash);
            let target_hash = format!("\"{0}\"", edge.target_hash);
            let source_strand = format!("\"{0}\"", edge.source_strand);
            let target_strand = format!("\"{0}\"", edge.target_strand);
            let edge_row = format!(
                "({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})",
                source_hash,
                BOGUS_SOURCE_NODE_ID,
                edge.source_coordinate,
                source_strand,
                target_hash,
                BOGUS_TARGET_NODE_ID,
                edge.target_coordinate,
                target_strand,
                edge.chromosome_index,
                edge.phased
            );
            edge_rows_to_insert.push(edge_row);
        }

        if !edge_rows_to_insert.is_empty() {
            for chunk in edge_rows_to_insert.chunks(100000) {
                let formatted_edge_rows_to_insert = chunk.join(", ");

                let insert_statement = format!("INSERT INTO edges (source_hash, source_node_id, source_coordinate, source_strand, target_hash, target_node_id, target_coordinate, target_strand, chromosome_index, phased) VALUES {0} RETURNING *;", formatted_edge_rows_to_insert);
                let mut stmt = conn.prepare(&insert_statement).unwrap();
                let rows = stmt.query_map([], Edge::edge_from_row).unwrap();
                for row in rows {
                    let edge = row.unwrap();
                    edge_map.insert(EdgeData::from(&edge), edge.id);
                }
            }
        }
        edges
            .iter()
            .map(|edge| *edge_map.get(edge).unwrap())
            .collect::<Vec<i32>>()
    }

    pub fn to_data(edge: Edge) -> EdgeData {
        EdgeData {
            source_hash: edge.source_hash,
            source_node_id: BOGUS_SOURCE_NODE_ID,
            source_coordinate: edge.source_coordinate,
            source_strand: edge.source_strand,
            target_hash: edge.target_hash,
            target_node_id: BOGUS_TARGET_NODE_ID,
            target_coordinate: edge.target_coordinate,
            target_strand: edge.target_strand,
            chromosome_index: edge.chromosome_index,
            phased: edge.phased,
        }
    }

    fn get_block_boundaries(
        source_edges: Option<&Vec<&Edge>>,
        target_edges: Option<&Vec<&Edge>>,
        sequence_length: i32,
    ) -> Vec<i32> {
        let mut block_boundary_coordinates = HashSet::new();
        if let Some(actual_source_edges) = source_edges {
            for source_edge in actual_source_edges {
                if source_edge.source_coordinate > 0
                    && source_edge.source_coordinate < sequence_length
                {
                    block_boundary_coordinates.insert(source_edge.source_coordinate);
                }
            }
        }
        if let Some(actual_target_edges) = target_edges {
            for target_edge in actual_target_edges {
                if target_edge.target_coordinate > 0
                    && target_edge.target_coordinate < sequence_length
                {
                    block_boundary_coordinates.insert(target_edge.target_coordinate);
                }
            }
        }

        block_boundary_coordinates
            .into_iter()
            .sorted_by(|c1, c2| Ord::cmp(&c1, &c2))
            .collect::<Vec<i32>>()
    }

    pub fn blocks_from_edges(conn: &Connection, edges: &Vec<Edge>) -> (Vec<GroupBlock>, Vec<Edge>) {
        let mut sequence_hashes = HashSet::new();
        let mut edges_by_source_hash: HashMap<&str, Vec<&Edge>> = HashMap::new();
        let mut edges_by_target_hash: HashMap<&str, Vec<&Edge>> = HashMap::new();
        for edge in edges {
            if edge.source_hash != Sequence::PATH_START_HASH {
                sequence_hashes.insert(edge.source_hash.as_str());
                edges_by_source_hash
                    .entry(&edge.source_hash)
                    .and_modify(|edges| edges.push(edge))
                    .or_default();
            }
            if edge.target_hash != Sequence::PATH_END_HASH {
                sequence_hashes.insert(edge.target_hash.as_str());
                edges_by_target_hash
                    .entry(&edge.target_hash)
                    .and_modify(|edges| edges.push(edge))
                    .or_default();
            }
        }

        let sequences_by_hash =
            Sequence::sequences_by_hash(conn, sequence_hashes.into_iter().collect::<Vec<&str>>());
        let mut blocks = vec![];
        let mut block_index = 0;
        let mut boundary_edges = vec![];
        for (hash, sequence) in sequences_by_hash.into_iter() {
            let block_boundaries = Edge::get_block_boundaries(
                edges_by_source_hash.get(hash.as_str()),
                edges_by_target_hash.get(hash.as_str()),
                sequence.length,
            );
            for block_boundary in &block_boundaries {
                // NOTE: Most of this data is bogus, the Edge struct is just a convenient wrapper
                // for the data we need to set up boundary edges in the block group graph
                boundary_edges.push(Edge {
                    id: -1,
                    source_hash: hash.clone(),
                    source_node_id: BOGUS_SOURCE_NODE_ID,
                    source_coordinate: *block_boundary,
                    source_strand: Strand::Unknown,
                    target_hash: hash.clone(),
                    target_node_id: BOGUS_TARGET_NODE_ID,
                    target_coordinate: *block_boundary,
                    target_strand: Strand::Unknown,
                    chromosome_index: 0,
                    phased: 0,
                });
            }

            if !block_boundaries.is_empty() {
                let start = 0;
                let end = block_boundaries[0];
                let block_sequence = sequence.get_sequence(start, end).to_string();
                let first_block = GroupBlock {
                    id: block_index,
                    sequence_hash: hash.clone(),
                    sequence: block_sequence,
                    start,
                    end,
                };
                blocks.push(first_block);
                block_index += 1;
                for (start, end) in block_boundaries.clone().into_iter().tuple_windows() {
                    let block_sequence = sequence.get_sequence(start, end).to_string();
                    let block = GroupBlock {
                        id: block_index,
                        sequence_hash: hash.clone(),
                        sequence: block_sequence,
                        start,
                        end,
                    };
                    blocks.push(block);
                    block_index += 1;
                }
                let start = block_boundaries[block_boundaries.len() - 1];
                let end = sequence.length;
                let block_sequence = sequence.get_sequence(start, end).to_string();
                let last_block = GroupBlock {
                    id: block_index,
                    sequence_hash: hash.clone(),
                    sequence: block_sequence,
                    start,
                    end,
                };
                blocks.push(last_block);
                block_index += 1;
            } else {
                blocks.push(GroupBlock {
                    id: block_index,
                    sequence_hash: hash.clone(),
                    sequence: sequence.get_sequence(None, None),
                    start: 0,
                    end: sequence.length,
                });
                block_index += 1;
            }
        }

        // NOTE: We need a dedicated start node and a dedicated end node for the graph formed by the
        // block group, since different paths in the block group may start or end at different
        // places on sequences.  These two "start sequence" and "end sequence" blocks will serve
        // that role.
        let start_sequence = Sequence::sequence_from_hash(conn, Sequence::PATH_START_HASH).unwrap();
        let start_block = GroupBlock {
            id: block_index + 1,
            sequence_hash: start_sequence.hash.clone(),
            sequence: "".to_string(),
            start: 0,
            end: 0,
        };
        blocks.push(start_block);
        let end_sequence = Sequence::sequence_from_hash(conn, Sequence::PATH_END_HASH).unwrap();
        let end_block = GroupBlock {
            id: block_index + 2,
            sequence_hash: end_sequence.hash.clone(),
            sequence: "".to_string(),
            start: 0,
            end: 0,
        };
        blocks.push(end_block);
        (blocks, boundary_edges)
    }

    pub fn build_graph(
        edges: &Vec<Edge>,
        blocks: &Vec<GroupBlock>,
    ) -> (DiGraphMap<i32, ()>, HashMap<(i32, i32), Edge>) {
        let blocks_by_start = blocks
            .clone()
            .into_iter()
            .map(|block| {
                (
                    BlockKey {
                        sequence_hash: block.sequence_hash,
                        coordinate: block.start,
                    },
                    block.id,
                )
            })
            .collect::<HashMap<BlockKey, i32>>();
        let blocks_by_end = blocks
            .clone()
            .into_iter()
            .map(|block| {
                (
                    BlockKey {
                        sequence_hash: block.sequence_hash,
                        coordinate: block.end,
                    },
                    block.id,
                )
            })
            .collect::<HashMap<BlockKey, i32>>();

        let mut graph: DiGraphMap<i32, ()> = DiGraphMap::new();
        let mut edges_by_node_pair = HashMap::new();
        for block in blocks {
            graph.add_node(block.id);
        }
        for edge in edges {
            let source_key = BlockKey {
                sequence_hash: edge.source_hash.clone(),
                coordinate: edge.source_coordinate,
            };
            let source_id = blocks_by_end.get(&source_key);
            let target_key = BlockKey {
                sequence_hash: edge.target_hash.clone(),
                coordinate: edge.target_coordinate,
            };
            let target_id = blocks_by_start.get(&target_key);

            if let Some(source_id_value) = source_id {
                if let Some(target_id_value) = target_id {
                    graph.add_edge(*source_id_value, *target_id_value, ());
                    edges_by_node_pair.insert((*source_id_value, *target_id_value), edge.clone());
                }
            }
        }

        (graph, edges_by_node_pair)
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::models::collection::Collection;
    use crate::test_helpers::get_connection;

    #[test]
    fn test_bulk_create() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let edge1 = EdgeData {
            source_hash: Sequence::PATH_START_HASH.to_string(),
            source_node_id: BOGUS_SOURCE_NODE_ID,
            source_coordinate: -1,
            source_strand: Strand::Forward,
            target_hash: sequence1.hash.clone(),
            target_node_id: BOGUS_TARGET_NODE_ID,
            target_coordinate: 1,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let edge2 = EdgeData {
            source_hash: sequence1.hash.clone(),
            source_node_id: BOGUS_SOURCE_NODE_ID,
            source_coordinate: 2,
            source_strand: Strand::Forward,
            target_hash: sequence2.hash.clone(),
            target_node_id: BOGUS_TARGET_NODE_ID,
            target_coordinate: 3,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };
        let edge3 = EdgeData {
            source_hash: sequence2.hash.clone(),
            source_node_id: BOGUS_SOURCE_NODE_ID,
            source_coordinate: 4,
            source_strand: Strand::Forward,
            target_hash: Sequence::PATH_END_HASH.to_string(),
            target_node_id: BOGUS_TARGET_NODE_ID,
            target_coordinate: -1,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };

        let edge_ids = Edge::bulk_create(conn, vec![edge1, edge2, edge3]);
        assert_eq!(edge_ids.len(), 3);
        let edges = Edge::bulk_load(conn, &edge_ids);
        assert_eq!(edges.len(), 3);

        let edges_by_source_hash = edges
            .into_iter()
            .map(|edge| (edge.source_hash.clone(), edge))
            .collect::<HashMap<String, Edge>>();

        let edge_result1 = edges_by_source_hash.get(Sequence::PATH_START_HASH).unwrap();
        assert_eq!(edge_result1.source_coordinate, -1);
        assert_eq!(edge_result1.target_hash, sequence1.hash);
        assert_eq!(edge_result1.target_coordinate, 1);
        let edge_result2 = edges_by_source_hash.get(&sequence1.hash).unwrap();
        assert_eq!(edge_result2.source_coordinate, 2);
        assert_eq!(edge_result2.target_hash, sequence2.hash);
        assert_eq!(edge_result2.target_coordinate, 3);
        let edge_result3 = edges_by_source_hash.get(&sequence2.hash).unwrap();
        assert_eq!(edge_result3.source_coordinate, 4);
        assert_eq!(edge_result3.target_hash, Sequence::PATH_END_HASH);
        assert_eq!(edge_result3.target_coordinate, -1);
    }

    #[test]
    fn test_bulk_create_returns_edges_in_order() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let edge1 = EdgeData {
            source_hash: Sequence::PATH_START_HASH.to_string(),
            source_node_id: BOGUS_SOURCE_NODE_ID,
            source_coordinate: -1,
            source_strand: Strand::Forward,
            target_hash: sequence1.hash.clone(),
            target_node_id: BOGUS_TARGET_NODE_ID,
            target_coordinate: 1,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let edge2 = EdgeData {
            source_hash: sequence1.hash.clone(),
            source_node_id: BOGUS_SOURCE_NODE_ID,
            source_coordinate: 2,
            source_strand: Strand::Forward,
            target_hash: sequence2.hash.clone(),
            target_node_id: BOGUS_TARGET_NODE_ID,
            target_coordinate: 3,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };
        let edge3 = EdgeData {
            source_hash: sequence2.hash.clone(),
            source_node_id: BOGUS_SOURCE_NODE_ID,
            source_coordinate: 4,
            source_strand: Strand::Forward,
            target_hash: Sequence::PATH_END_HASH.to_string(),
            target_node_id: BOGUS_TARGET_NODE_ID,
            target_coordinate: -1,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };

        let edges = vec![edge2.clone(), edge3.clone()];
        let edge_ids1 = Edge::bulk_create(conn, edges.clone());
        assert_eq!(edge_ids1.len(), 2);
        for (index, id) in edge_ids1.iter().enumerate() {
            let binding = Edge::query(
                conn,
                "select * from edges where id = ?1;",
                vec![Value::from(*id)],
            );
            let edge = binding.first().unwrap();
            assert_eq!(EdgeData::from(edge), edges[index]);
        }

        let edges = vec![edge1.clone(), edge2.clone(), edge3.clone()];
        let edge_ids2 = Edge::bulk_create(conn, edges.clone());
        assert_eq!(edge_ids2[1], edge_ids1[0]);
        assert_eq!(edge_ids2[2], edge_ids1[1]);
        assert_eq!(edge_ids2.len(), 3);
        for (index, id) in edge_ids2.iter().enumerate() {
            // this sort by makes it so the order will not match the input order of the function call
            let binding = Edge::query(
                conn,
                "select * from edges where id = ?1;",
                vec![Value::from(*id)],
            );
            let edge = binding.first().unwrap();
            assert_eq!(EdgeData::from(edge), edges[index]);
        }
    }

    #[test]
    fn test_bulk_create_with_existing_edge() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        // NOTE: Create one edge ahead of time to confirm an existing row ID gets returned in the bulk create
        let existing_edge = Edge::create(
            conn,
            Sequence::PATH_START_HASH.to_string(),
            -1,
            Strand::Forward,
            sequence1.hash.clone(),
            1,
            Strand::Forward,
            0,
            0,
        );
        assert_eq!(existing_edge.source_hash, Sequence::PATH_START_HASH);
        assert_eq!(existing_edge.source_coordinate, -1);
        assert_eq!(existing_edge.target_hash, sequence1.hash);
        assert_eq!(existing_edge.target_coordinate, 1);

        let edge1 = EdgeData {
            source_hash: Sequence::PATH_START_HASH.to_string(),
            source_coordinate: -1,
            source_node_id: BOGUS_SOURCE_NODE_ID,
            source_strand: Strand::Forward,
            target_hash: sequence1.hash.clone(),
            target_node_id: BOGUS_TARGET_NODE_ID,
            target_coordinate: 1,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let edge2 = EdgeData {
            source_hash: sequence1.hash.clone(),
            source_node_id: BOGUS_SOURCE_NODE_ID,
            source_coordinate: 2,
            source_strand: Strand::Forward,
            target_hash: sequence2.hash.clone(),
            target_node_id: BOGUS_TARGET_NODE_ID,
            target_coordinate: 3,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };
        let edge3 = EdgeData {
            source_hash: sequence2.hash.clone(),
            source_node_id: BOGUS_SOURCE_NODE_ID,
            source_coordinate: 4,
            source_strand: Strand::Forward,
            target_hash: Sequence::PATH_END_HASH.to_string(),
            target_node_id: BOGUS_TARGET_NODE_ID,
            target_coordinate: -1,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };

        let edge_ids = Edge::bulk_create(conn, vec![edge1, edge2, edge3]);
        assert_eq!(edge_ids.len(), 3);
        let edges = Edge::bulk_load(conn, &edge_ids);
        assert_eq!(edges.len(), 3);

        let edges_by_source_hash = edges
            .into_iter()
            .map(|edge| (edge.source_hash.clone(), edge))
            .collect::<HashMap<String, Edge>>();

        let edge_result1 = edges_by_source_hash.get(Sequence::PATH_START_HASH).unwrap();

        assert_eq!(edge_result1.id, existing_edge.id);

        assert_eq!(edge_result1.source_coordinate, -1);
        assert_eq!(edge_result1.target_hash, sequence1.hash);
        assert_eq!(edge_result1.target_coordinate, 1);
        let edge_result2 = edges_by_source_hash.get(&sequence1.hash).unwrap();
        assert_eq!(edge_result2.source_coordinate, 2);
        assert_eq!(edge_result2.target_hash, sequence2.hash);
        assert_eq!(edge_result2.target_coordinate, 3);
        let edge_result3 = edges_by_source_hash.get(&sequence2.hash).unwrap();
        assert_eq!(edge_result3.source_coordinate, 4);
        assert_eq!(edge_result3.target_hash, Sequence::PATH_END_HASH);
        assert_eq!(edge_result3.target_coordinate, -1);
    }
}
