use crate::models::block_group::BlockGroup;
use crate::models::edge::EdgeData;
use crate::models::strand::Strand;
use crate::models::traits::Query;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection, Result as SQLResult, Row};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::RandomState;

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq)]
pub struct Accession {
    pub id: i64,
    pub name: String,
    pub path_id: i64,
    pub accession_id: Option<i64>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct AccessionEdge {
    pub id: i64,
    pub source_node_id: i64,
    pub source_coordinate: i64,
    pub source_strand: Strand,
    pub target_node_id: i64,
    pub target_coordinate: i64,
    pub target_strand: Strand,
    pub chromosome_index: i64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct AccessionPath {
    pub id: i64,
    pub accession_id: i64,
    pub index_in_path: i64,
    pub edge_id: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AccessionEdgeData {
    pub source_node_id: i64,
    pub source_coordinate: i64,
    pub source_strand: Strand,
    pub target_node_id: i64,
    pub target_coordinate: i64,
    pub target_strand: Strand,
    pub chromosome_index: i64,
}

impl From<&AccessionEdge> for AccessionEdgeData {
    fn from(item: &AccessionEdge) -> Self {
        AccessionEdgeData {
            source_node_id: item.source_node_id,
            source_coordinate: item.source_coordinate,
            source_strand: item.source_strand,
            target_node_id: item.target_node_id,
            target_coordinate: item.target_coordinate,
            target_strand: item.target_strand,
            chromosome_index: item.chromosome_index,
        }
    }
}

impl From<&EdgeData> for AccessionEdgeData {
    fn from(item: &EdgeData) -> Self {
        AccessionEdgeData {
            source_node_id: item.source_node_id,
            source_coordinate: item.source_coordinate,
            source_strand: item.source_strand,
            target_node_id: item.target_node_id,
            target_coordinate: item.target_coordinate,
            target_strand: item.target_strand,
            chromosome_index: item.chromosome_index,
        }
    }
}

impl Accession {
    pub fn create(
        conn: &Connection,
        name: &str,
        path_id: i64,
        accession_id: Option<i64>,
    ) -> SQLResult<Accession> {
        let query = "INSERT INTO accession (name, path_id, accession_id) VALUES (?1, ?2, ?3) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();

        stmt.query_row((name, path_id, accession_id), |row| {
            Ok(Accession {
                id: row.get(0)?,
                name: name.to_string(),
                path_id,
                accession_id,
            })
        })
    }

    pub fn get_or_create(
        conn: &Connection,
        name: &str,
        path_id: i64,
        accession_id: Option<i64>,
    ) -> Accession {
        match Accession::create(conn, name, path_id, accession_id) {
            Ok(accession) => accession,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    let mut existing_id: i64;
                    if let Some(id) = accession_id {
                        existing_id = conn.query_row("select id from accession where name = ?1 and path_id = ?2 and accession_id = ?3;", params_from_iter(vec![Value::from(name.to_string()), Value::from(path_id), Value::from(id)]), |row| row.get(0)).unwrap();
                    } else {
                        existing_id = conn.query_row("select id from accession where name = ?1 and path_id = ?2 and accession_id is null;", params_from_iter(vec![Value::from(name.to_string()), Value::from(path_id)]), |row| row.get(0)).unwrap();
                    }
                    Accession {
                        id: existing_id,
                        name: name.to_string(),
                        path_id,
                        accession_id,
                    }
                } else {
                    panic!("something bad happened querying the database")
                }
            }
            Err(_) => {
                panic!("something bad happened.")
            }
        }
    }
}

impl Query for Accession {
    type Model = Accession;
    fn process_row(row: &Row) -> Self::Model {
        Accession {
            id: row.get(0).unwrap(),
            name: row.get(1).unwrap(),
            path_id: row.get(2).unwrap(),
            accession_id: row.get(3).unwrap(),
        }
    }
}

impl AccessionEdge {
    pub fn create(conn: &Connection, edge: AccessionEdgeData) -> AccessionEdge {
        // TODO: handle get-or-create
        let insert_statement = "INSERT INTO accession_edge (source_node_id, source_coordinate, source_strand, target_node_id, target_coordinate, target_strand, chromosome_index) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) RETURNING (id);";
        let placeholders: Vec<Value> = vec![
            edge.source_node_id.into(),
            edge.source_coordinate.into(),
            edge.source_strand.into(),
            edge.target_node_id.into(),
            edge.target_coordinate.into(),
            edge.target_strand.into(),
            edge.chromosome_index.into(),
        ];
        let mut stmt = conn.prepare(insert_statement).unwrap();
        let id : i64 = match stmt.query_row(params_from_iter(&placeholders), |row| {
            row.get(0)
        }) {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                conn.query_row("select id from accession_edge where source_node_id = ?1 source_coordinate = ?2 source_strand = ?3 target_node_id = ?4 target_coordinate = ?5 target_strand = ?6 chromosome_index = ?7", params_from_iter(&placeholders), |row| {
                    row.get(0)
                }).unwrap()
            }
            Err(_) => {
                panic!("something bad happened querying the database")
            }
        };
        AccessionEdge {
            id,
            source_node_id: edge.source_node_id,
            source_coordinate: edge.source_coordinate,
            source_strand: edge.source_strand,
            target_node_id: edge.target_node_id,
            target_coordinate: edge.target_coordinate,
            target_strand: edge.target_strand,
            chromosome_index: edge.chromosome_index,
        }
    }

    pub fn bulk_create(conn: &Connection, edges: &Vec<AccessionEdgeData>) -> Vec<i64> {
        let mut edge_rows = vec![];
        let mut edge_map: HashMap<AccessionEdgeData, i64> = HashMap::new();
        for edge in edges {
            let source_strand = format!("\"{0}\"", edge.source_strand);
            let target_strand = format!("\"{0}\"", edge.target_strand);
            let edge_row = format!(
                "({0}, {1}, {2}, {3}, {4}, {5}, {6})",
                edge.source_node_id,
                edge.source_coordinate,
                source_strand,
                edge.target_node_id,
                edge.target_coordinate,
                target_strand,
                edge.chromosome_index,
            );
            edge_rows.push(edge_row);
        }
        let formatted_edge_rows = edge_rows.join(", ");

        let select_statement = format!("SELECT * FROM accession_edge WHERE (source_node_id, source_coordinate, source_strand, target_node_id, target_coordinate, target_strand, chromosome_index) in ({0});", formatted_edge_rows);
        let existing_edges = AccessionEdge::query(conn, &select_statement, vec![]);
        for edge in existing_edges.iter() {
            edge_map.insert(AccessionEdgeData::from(edge), edge.id);
        }

        let existing_edge_set = HashSet::<AccessionEdgeData, RandomState>::from_iter(
            existing_edges.into_iter().map(AccessionEdge::to_data),
        );
        let mut edges_to_insert = HashSet::new();
        for edge in edges {
            if !existing_edge_set.contains(edge) {
                edges_to_insert.insert(edge);
            }
        }

        let mut edge_rows_to_insert = vec![];
        for edge in edges_to_insert {
            let source_strand = format!("\"{0}\"", edge.source_strand);
            let target_strand = format!("\"{0}\"", edge.target_strand);
            let edge_row = format!(
                "({0}, {1}, {2}, {3}, {4}, {5}, {6})",
                edge.source_node_id,
                edge.source_coordinate,
                source_strand,
                edge.target_node_id,
                edge.target_coordinate,
                target_strand,
                edge.chromosome_index,
            );
            edge_rows_to_insert.push(edge_row);
        }

        if !edge_rows_to_insert.is_empty() {
            for chunk in edge_rows_to_insert.chunks(100000) {
                let formatted_edge_rows_to_insert = chunk.join(", ");

                let insert_statement = format!("INSERT INTO accession_edge (source_node_id, source_coordinate, source_strand, target_node_id, target_coordinate, target_strand, chromosome_index) VALUES {0} RETURNING *;", formatted_edge_rows_to_insert);
                let mut stmt = conn.prepare(&insert_statement).unwrap();
                let rows = stmt
                    .query_map([], |row| Ok(AccessionEdge::process_row(row)))
                    .unwrap();
                for row in rows {
                    let edge = row.unwrap();
                    edge_map.insert(AccessionEdgeData::from(&edge), edge.id);
                }
            }
        }
        edges
            .iter()
            .map(|edge| *edge_map.get(edge).unwrap())
            .collect::<Vec<i64>>()
    }

    pub fn to_data(edge: AccessionEdge) -> AccessionEdgeData {
        AccessionEdgeData {
            source_node_id: edge.source_node_id,
            source_coordinate: edge.source_coordinate,
            source_strand: edge.source_strand,
            target_node_id: edge.target_node_id,
            target_coordinate: edge.target_coordinate,
            target_strand: edge.target_strand,
            chromosome_index: edge.chromosome_index,
        }
    }
}

impl Query for AccessionEdge {
    type Model = AccessionEdge;
    fn process_row(row: &Row) -> Self::Model {
        AccessionEdge {
            id: row.get(0).unwrap(),
            source_node_id: row.get(1).unwrap(),
            source_coordinate: row.get(2).unwrap(),
            source_strand: row.get(3).unwrap(),
            target_node_id: row.get(4).unwrap(),
            target_coordinate: row.get(5).unwrap(),
            target_strand: row.get(6).unwrap(),
            chromosome_index: row.get(7).unwrap(),
        }
    }
}

impl AccessionPath {
    pub fn create(conn: &Connection, accession_id: i64, edge_ids: &[i64]) {
        for (index1, chunk) in edge_ids.chunks(100000).enumerate() {
            let mut rows_to_insert = vec![];
            for (index2, edge_id) in chunk.iter().enumerate() {
                let row = format!(
                    "({0}, {1}, {2})",
                    accession_id,
                    edge_id,
                    index1 * 100000 + index2
                );
                rows_to_insert.push(row);
            }

            let formatted_rows_to_insert = rows_to_insert.join(", ");

            let insert_statement = format!(
                "INSERT OR IGNORE INTO accession_path (accession_id, edge_id, index_in_path) VALUES {0};",
                formatted_rows_to_insert
            );
            let _ = conn.execute(&insert_statement, ());
        }
    }
}

impl Query for AccessionPath {
    type Model = AccessionPath;
    fn process_row(row: &Row) -> AccessionPath {
        AccessionPath {
            id: row.get(0).unwrap(),
            accession_id: row.get(1).unwrap(),
            index_in_path: row.get(2).unwrap(),
            edge_id: row.get(3).unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{get_connection, setup_block_group};

    #[test]
    fn test_accession_create_query() {
        let conn = &get_connection(None);
        let (bg, path) = setup_block_group(conn);
        let accession = Accession::create(conn, "test", path.id, None).unwrap();
        let accession_2 = Accession::create(conn, "test2", path.id, None).unwrap();
        assert_eq!(
            Accession::query(
                conn,
                "select * from accession where name = ?1",
                vec![Value::from("test".to_string())]
            ),
            vec![Accession {
                id: accession.id,
                name: "test".to_string(),
                path_id: path.id,
                accession_id: None,
            }]
        )
    }
}
