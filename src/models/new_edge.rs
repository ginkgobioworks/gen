use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use std::collections::HashSet;
use std::hash::RandomState;

#[derive(Clone, Debug)]
pub struct NewEdge {
    pub id: i32,
    pub source_hash: String,
    pub source_coordinate: i32,
    pub target_hash: String,
    pub target_coordinate: i32,
    pub chromosome_index: i32,
    pub phased: i32,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EdgeData {
    pub source_hash: String,
    pub source_coordinate: i32,
    pub target_hash: String,
    pub target_coordinate: i32,
    pub chromosome_index: i32,
    pub phased: i32,
}

impl NewEdge {
    pub const PATH_START_HASH: &'static str =
        "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    pub const PATH_END_HASH: &'static str =
        "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";

    pub fn create(
        conn: &Connection,
        source_hash: String,
        source_coordinate: i32,
        target_hash: String,
        target_coordinate: i32,
        chromosome_index: i32,
        phased: i32,
    ) -> NewEdge {
        let query = "INSERT INTO new_edges (source_hash, source_coordinate, target_hash, target_coordinate, chromosome_index, phased) VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING *";
        let id_query = "select id from new_edges where source_hash = ?1 and source_coordinate = ?2 and target_hash = ?3 and target_coordinate = ?4 and chromosome_index = ?5 and phased = ?6";
        let mut placeholders: Vec<Value> = vec![
            source_hash.clone().into(),
            source_coordinate.into(),
            target_hash.clone().into(),
            target_coordinate.into(),
            chromosome_index.into(),
            phased.into(),
        ];

        let mut stmt = conn.prepare(query).unwrap();
        match stmt.query_row(params_from_iter(&placeholders), |row| {
            Ok(NewEdge {
                id: row.get(0)?,
                source_hash: row.get(1)?,
                source_coordinate: row.get(2)?,
                target_hash: row.get(3)?,
                target_coordinate: row.get(4)?,
                chromosome_index: row.get(5)?,
                phased: row.get(6)?,
            })
        }) {
            Ok(edge) => edge,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    NewEdge {
                        id: conn
                            .query_row(id_query, params_from_iter(&placeholders), |row| row.get(0))
                            .unwrap(),
                        source_hash,
                        source_coordinate,
                        target_hash,
                        target_coordinate,
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

    pub fn bulk_load(conn: &Connection, edge_ids: Vec<i32>) -> Vec<NewEdge> {
        let formatted_edge_ids = edge_ids
            .into_iter()
            .map(|edge_id| edge_id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let query = format!("select id, source_hash, source_coordinate, target_hash, target_coordinate, chromosome_index, phased from new_edges where id in ({});", formatted_edge_ids);
        NewEdge::query(conn, &query, vec![])
    }

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<NewEdge> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(NewEdge {
                    id: row.get(0)?,
                    source_hash: row.get(1)?,
                    source_coordinate: row.get(2)?,
                    target_hash: row.get(3)?,
                    target_coordinate: row.get(4)?,
                    chromosome_index: row.get(5)?,
                    phased: row.get(6)?,
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
            let target_hash = format!("\"{0}\"", edge.target_hash);
            let edge_row = format!(
                "({0}, {1}, {2}, {3}, {4}, {5})",
                source_hash,
                edge.source_coordinate,
                target_hash,
                edge.target_coordinate,
                edge.chromosome_index,
                edge.phased
            );
            edge_rows.push(edge_row);
        }
        let formatted_edge_rows = edge_rows.join(", ");

        let select_statement = format!("SELECT * FROM new_edges WHERE (source_hash, source_coordinate, target_hash, target_coordinate, chromosome_index, phased) in ({0});", formatted_edge_rows);
        let existing_edges = NewEdge::query(conn, &select_statement, vec![]);
        let mut existing_edge_ids: Vec<i32> = existing_edges
            .clone()
            .into_iter()
            .map(|edge| edge.id)
            .collect();

        let existing_edge_set = HashSet::<EdgeData, RandomState>::from_iter(
            existing_edges.into_iter().map(NewEdge::to_data),
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
            let edge_row = format!(
                "({0}, {1}, {2}, {3}, {4}, {5})",
                source_hash,
                edge.source_coordinate,
                target_hash,
                edge.target_coordinate,
                edge.chromosome_index,
                edge.phased
            );
            edge_rows_to_insert.push(edge_row);
        }
        let formatted_edge_rows_to_insert = edge_rows_to_insert.join(", ");

        let insert_statement = format!("INSERT OR IGNORE INTO new_edges (source_hash, source_coordinate, target_hash, target_coordinate, chromosome_index, phased) VALUES {0} RETURNING (id);", formatted_edge_rows_to_insert);
        let mut stmt = conn.prepare(&insert_statement).unwrap();
        let rows = stmt.query_map([], |row| row.get(0)).unwrap();
        let mut edge_ids: Vec<i32> = vec![];
        for row in rows {
            edge_ids.push(row.unwrap());
        }

        existing_edge_ids.extend(edge_ids);
        existing_edge_ids
    }

    pub fn to_data(edge: NewEdge) -> EdgeData {
        EdgeData {
            source_hash: edge.source_hash,
            source_coordinate: edge.source_coordinate,
            target_hash: edge.target_hash,
            target_coordinate: edge.target_coordinate,
            chromosome_index: edge.chromosome_index,
            phased: edge.phased,
        }
    }
}

mod tests {
    use rusqlite::Connection;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::migrations::run_migrations;
    use crate::models::{sequence::Sequence, Collection};

    fn get_connection() -> Connection {
        let mut conn = Connection::open_in_memory()
            .unwrap_or_else(|_| panic!("Error opening in memory test db"));
        rusqlite::vtab::array::load_module(&conn).unwrap();
        run_migrations(&mut conn);
        conn
    }

    #[test]
    fn test_bulk_create() {
        let conn = &mut get_connection();
        Collection::create(conn, "test collection");
        let sequence1_hash = Sequence::create(conn, "DNA", "ATCGATCG", true);
        let edge1 = EdgeData {
            source_hash: NewEdge::PATH_START_HASH.to_string(),
            source_coordinate: -1,
            target_hash: sequence1_hash.clone(),
            target_coordinate: 1,
            chromosome_index: 0,
            phased: 0,
        };
        let sequence2_hash = Sequence::create(conn, "DNA", "AAAAAAAA", true);
        let edge2 = EdgeData {
            source_hash: sequence1_hash.clone(),
            source_coordinate: 2,
            target_hash: sequence2_hash.clone(),
            target_coordinate: 3,
            chromosome_index: 0,
            phased: 0,
        };
        let edge3 = EdgeData {
            source_hash: sequence2_hash.clone(),
            source_coordinate: 4,
            target_hash: NewEdge::PATH_END_HASH.to_string(),
            target_coordinate: -1,
            chromosome_index: 0,
            phased: 0,
        };

        let edge_ids = NewEdge::bulk_create(conn, vec![edge1, edge2, edge3]);
        assert_eq!(edge_ids.len(), 3);
        let edges = NewEdge::bulk_load(conn, edge_ids);
        assert_eq!(edges.len(), 3);

        let edge_result1 = &edges[0];
        assert_eq!(edge_result1.source_hash, NewEdge::PATH_START_HASH);
        assert_eq!(edge_result1.source_coordinate, -1);
        assert_eq!(edge_result1.target_hash, sequence1_hash);
        assert_eq!(edge_result1.target_coordinate, 1);
        let edge_result2 = &edges[1];
        assert_eq!(edge_result2.source_hash, sequence1_hash);
        assert_eq!(edge_result2.source_coordinate, 2);
        assert_eq!(edge_result2.target_hash, sequence2_hash);
        assert_eq!(edge_result2.target_coordinate, 3);
        let edge_result3 = &edges[2];
        assert_eq!(edge_result3.source_hash, sequence2_hash);
        assert_eq!(edge_result3.source_coordinate, 4);
        assert_eq!(edge_result3.target_hash, NewEdge::PATH_END_HASH);
        assert_eq!(edge_result3.target_coordinate, -1);
    }

    #[test]
    fn test_bulk_create_with_existing_edge() {
        let conn = &mut get_connection();
        Collection::create(conn, "test collection");
        let sequence1_hash = Sequence::create(conn, "DNA", "ATCGATCG", true);
        // NOTE: Create one edge ahead of time to confirm an existing row ID gets returned in the bulk create
        let existing_edge = NewEdge::create(
            conn,
            NewEdge::PATH_START_HASH.to_string(),
            -1,
            sequence1_hash.clone(),
            1,
            0,
            0,
        );
        assert_eq!(existing_edge.source_hash, NewEdge::PATH_START_HASH);
        assert_eq!(existing_edge.source_coordinate, -1);
        assert_eq!(existing_edge.target_hash, sequence1_hash);
        assert_eq!(existing_edge.target_coordinate, 1);

        let edge1 = EdgeData {
            source_hash: NewEdge::PATH_START_HASH.to_string(),
            source_coordinate: -1,
            target_hash: sequence1_hash.clone(),
            target_coordinate: 1,
            chromosome_index: 0,
            phased: 0,
        };
        let sequence2_hash = Sequence::create(conn, "DNA", "AAAAAAAA", true);
        let edge2 = EdgeData {
            source_hash: sequence1_hash.clone(),
            source_coordinate: 2,
            target_hash: sequence2_hash.clone(),
            target_coordinate: 3,
            chromosome_index: 0,
            phased: 0,
        };
        let edge3 = EdgeData {
            source_hash: sequence2_hash.clone(),
            source_coordinate: 4,
            target_hash: NewEdge::PATH_END_HASH.to_string(),
            target_coordinate: -1,
            chromosome_index: 0,
            phased: 0,
        };

        let edge_ids = NewEdge::bulk_create(conn, vec![edge1, edge2, edge3]);
        assert_eq!(edge_ids.len(), 3);
        let edges = NewEdge::bulk_load(conn, edge_ids);
        assert_eq!(edges.len(), 3);

        let edge_result1 = &edges[0];

        assert_eq!(edge_result1.id, existing_edge.id);

        assert_eq!(edge_result1.source_hash, NewEdge::PATH_START_HASH);
        assert_eq!(edge_result1.source_coordinate, -1);
        assert_eq!(edge_result1.target_hash, sequence1_hash);
        assert_eq!(edge_result1.target_coordinate, 1);
        let edge_result2 = &edges[2];
        assert_eq!(edge_result2.source_hash, sequence1_hash);
        assert_eq!(edge_result2.source_coordinate, 2);
        assert_eq!(edge_result2.target_hash, sequence2_hash);
        assert_eq!(edge_result2.target_coordinate, 3);
        let edge_result3 = &edges[1];
        assert_eq!(edge_result3.source_hash, sequence2_hash);
        assert_eq!(edge_result3.source_coordinate, 4);
        assert_eq!(edge_result3.target_hash, NewEdge::PATH_END_HASH);
        assert_eq!(edge_result3.target_coordinate, -1);
    }
}
