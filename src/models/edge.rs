use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use std::collections::HashSet;
use std::hash::RandomState;

use crate::models::strand::Strand;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Edge {
    pub id: i32,
    pub source_hash: String,
    pub source_coordinate: i32,
    pub source_strand: Strand,
    pub target_hash: String,
    pub target_coordinate: i32,
    pub target_strand: Strand,
    pub chromosome_index: i32,
    pub phased: i32,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EdgeData {
    pub source_hash: String,
    pub source_coordinate: i32,
    pub source_strand: Strand,
    pub target_hash: String,
    pub target_coordinate: i32,
    pub target_strand: Strand,
    pub chromosome_index: i32,
    pub phased: i32,
}

impl Edge {
    pub const PATH_START_HASH: &'static str =
        "start-node-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    pub const PATH_END_HASH: &'static str =
        "end-node-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";

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
        let query = "INSERT INTO edges (source_hash, source_coordinate, source_strand, target_hash, target_coordinate, target_strand, chromosome_index, phased) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) RETURNING *";
        let id_query = "select id from edges where source_hash = ?1 and source_coordinate = ?2 and source_strand = ?3 and target_hash = ?4 and target_coordinate = ?5 and target_strand = ?6 and chromosome_index = ?7 and phased = ?8";
        let placeholders: Vec<Value> = vec![
            source_hash.clone().into(),
            source_coordinate.into(),
            source_strand.into(),
            target_hash.clone().into(),
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
                source_coordinate: row.get(2)?,
                source_strand: row.get(3)?,
                target_hash: row.get(4)?,
                target_coordinate: row.get(5)?,
                target_strand: row.get(6)?,
                chromosome_index: row.get(7)?,
                phased: row.get(8)?,
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
                        source_coordinate,
                        source_strand,
                        target_hash,
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

    pub fn bulk_load(conn: &Connection, edge_ids: Vec<i32>) -> Vec<Edge> {
        let formatted_edge_ids = edge_ids
            .into_iter()
            .map(|edge_id| edge_id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let query = format!("select id, source_hash, source_coordinate, source_strand, target_hash, target_coordinate, target_strand, chromosome_index, phased from edges where id in ({});", formatted_edge_ids);
        Edge::query(conn, &query, vec![])
    }

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<Edge> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(Edge {
                    id: row.get(0)?,
                    source_hash: row.get(1)?,
                    source_coordinate: row.get(2)?,
                    source_strand: row.get(3)?,
                    target_hash: row.get(4)?,
                    target_coordinate: row.get(5)?,
                    target_strand: row.get(6)?,
                    chromosome_index: row.get(7)?,
                    phased: row.get(8)?,
                })
            })
            .unwrap();
        let mut edges = vec![];
        for row in rows {
            edges.push(row.unwrap());
        }
        edges
    }

    pub fn bulk_create(conn: &Connection, edges: Vec<EdgeData>) -> Vec<i32> {
        let mut edge_rows = vec![];
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
        let mut existing_edge_ids: Vec<i32> = existing_edges
            .clone()
            .into_iter()
            .map(|edge| edge.id)
            .collect();

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
            edge_rows_to_insert.push(edge_row);
        }

        if edge_rows_to_insert.is_empty() {
            return existing_edge_ids;
        }

        for chunk in edge_rows_to_insert.chunks(100000) {
            let formatted_edge_rows_to_insert = chunk.join(", ");

            let insert_statement = format!("INSERT INTO edges (source_hash, source_coordinate, source_strand, target_hash, target_coordinate, target_strand, chromosome_index, phased) VALUES {0} RETURNING (id);", formatted_edge_rows_to_insert);
            let mut stmt = conn.prepare(&insert_statement).unwrap();
            let rows = stmt.query_map([], |row| row.get(0)).unwrap();
            let mut edge_ids: Vec<i32> = vec![];
            for row in rows {
                edge_ids.push(row.unwrap());
            }

            existing_edge_ids.extend(edge_ids);
        }

        existing_edge_ids
    }

    pub fn to_data(edge: Edge) -> EdgeData {
        EdgeData {
            source_hash: edge.source_hash,
            source_coordinate: edge.source_coordinate,
            source_strand: edge.source_strand,
            target_hash: edge.target_hash,
            target_coordinate: edge.target_coordinate,
            target_strand: edge.target_strand,
            chromosome_index: edge.chromosome_index,
            phased: edge.phased,
        }
    }
}

mod tests {
    use rusqlite::Connection;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use std::collections::HashMap;

    use crate::models::{sequence::Sequence, Collection};
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
            source_hash: Edge::PATH_START_HASH.to_string(),
            source_coordinate: -1,
            source_strand: Strand::Forward,
            target_hash: sequence1.hash.clone(),
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
            source_coordinate: 2,
            source_strand: Strand::Forward,
            target_hash: sequence2.hash.clone(),
            target_coordinate: 3,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };
        let edge3 = EdgeData {
            source_hash: sequence2.hash.clone(),
            source_coordinate: 4,
            source_strand: Strand::Forward,
            target_hash: Edge::PATH_END_HASH.to_string(),
            target_coordinate: -1,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };

        let edge_ids = Edge::bulk_create(conn, vec![edge1, edge2, edge3]);
        assert_eq!(edge_ids.len(), 3);
        let edges = Edge::bulk_load(conn, edge_ids);
        assert_eq!(edges.len(), 3);

        let edges_by_source_hash = edges
            .into_iter()
            .map(|edge| (edge.source_hash.clone(), edge))
            .collect::<HashMap<String, Edge>>();

        let edge_result1 = edges_by_source_hash.get(Edge::PATH_START_HASH).unwrap();
        assert_eq!(edge_result1.source_coordinate, -1);
        assert_eq!(edge_result1.target_hash, sequence1.hash);
        assert_eq!(edge_result1.target_coordinate, 1);
        let edge_result2 = edges_by_source_hash.get(&sequence1.hash).unwrap();
        assert_eq!(edge_result2.source_coordinate, 2);
        assert_eq!(edge_result2.target_hash, sequence2.hash);
        assert_eq!(edge_result2.target_coordinate, 3);
        let edge_result3 = edges_by_source_hash.get(&sequence2.hash).unwrap();
        assert_eq!(edge_result3.source_coordinate, 4);
        assert_eq!(edge_result3.target_hash, Edge::PATH_END_HASH);
        assert_eq!(edge_result3.target_coordinate, -1);
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
            Edge::PATH_START_HASH.to_string(),
            -1,
            Strand::Forward,
            sequence1.hash.clone(),
            1,
            Strand::Forward,
            0,
            0,
        );
        assert_eq!(existing_edge.source_hash, Edge::PATH_START_HASH);
        assert_eq!(existing_edge.source_coordinate, -1);
        assert_eq!(existing_edge.target_hash, sequence1.hash);
        assert_eq!(existing_edge.target_coordinate, 1);

        let edge1 = EdgeData {
            source_hash: Edge::PATH_START_HASH.to_string(),
            source_coordinate: -1,
            source_strand: Strand::Forward,
            target_hash: sequence1.hash.clone(),
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
            source_coordinate: 2,
            source_strand: Strand::Forward,
            target_hash: sequence2.hash.clone(),
            target_coordinate: 3,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };
        let edge3 = EdgeData {
            source_hash: sequence2.hash.clone(),
            source_coordinate: 4,
            source_strand: Strand::Forward,
            target_hash: Edge::PATH_END_HASH.to_string(),
            target_coordinate: -1,
            target_strand: Strand::Forward,
            chromosome_index: 0,
            phased: 0,
        };

        let edge_ids = Edge::bulk_create(conn, vec![edge1, edge2, edge3]);
        assert_eq!(edge_ids.len(), 3);
        let edges = Edge::bulk_load(conn, edge_ids);
        assert_eq!(edges.len(), 3);

        let edges_by_source_hash = edges
            .into_iter()
            .map(|edge| (edge.source_hash.clone(), edge))
            .collect::<HashMap<String, Edge>>();

        let edge_result1 = edges_by_source_hash.get(Edge::PATH_START_HASH).unwrap();

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
        assert_eq!(edge_result3.target_hash, Edge::PATH_END_HASH);
        assert_eq!(edge_result3.target_coordinate, -1);
    }
}
