use crate::models::edge::{Edge, EdgeData};
use crate::models::strand::Strand;
use crate::models::traits::Query;
use rusqlite::types::Value;
use rusqlite::{Connection, Row};
use std::collections::{HashMap, HashSet};
use std::hash::RandomState;

#[derive(Debug, Eq, PartialEq)]
pub struct Accession {
    pub id: i64,
    pub name: String,
    pub path_id: i64,
    pub accession_id: Option<i64>,
    pub start: i64,
    pub end: i64,
}

pub struct AccessionEdge {
    pub id: i64,
    pub source_node_id: i64,
    pub source_coordinate: i64,
    pub source_strand: Strand,
    pub target_node_id: i64,
    pub target_coordinate: i64,
    pub target_strand: Strand,
    pub chromosome_index: i64,
    pub phased: i64,
}

pub struct AccessionPath {
    pub id: i64,
    pub accession_id: i64,
    pub index_in_path: i64,
    pub edge_id: i64,
}

impl Accession {
    pub fn create(
        conn: &Connection,
        name: &str,
        path_id: i64,
        accession_id: Option<i64>,
        start: i64,
        end: i64,
    ) -> Accession {
        let query = "INSERT INTO accession (name, path_id, accession_id, start, end) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();

        let mut rows = stmt
            .query_map((name, path_id, accession_id, start, end), |row| {
                Ok(Accession {
                    id: row.get(0)?,
                    name: name.to_string(),
                    path_id,
                    accession_id,
                    start,
                    end,
                })
            })
            .unwrap();
        match rows.next().unwrap() {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, _details)) => {
                panic!("handle it");
            }
            Err(_) => {
                panic!("something bad happened querying the database")
            }
        }
    }
}

impl Query for Accession {
    fn process_row(row: &Row) -> Box<Self> {
        Box::new(Accession {
            id: row.get(0).unwrap(),
            name: row.get(1).unwrap(),
            path_id: row.get(2).unwrap(),
            accession_id: row.get(3).unwrap(),
            start: row.get(4).unwrap(),
            end: row.get(5).unwrap(),
        })
    }
}

impl AccessionEdge {
    pub fn bulk_create(conn: &Connection, edges: &Vec<EdgeData>) -> Vec<i64> {
        let mut edge_rows = vec![];
        let mut edge_map: HashMap<EdgeData, i64> = HashMap::new();
        for edge in edges {
            let source_strand = format!("\"{0}\"", edge.source_strand);
            let target_strand = format!("\"{0}\"", edge.target_strand);
            let edge_row = format!(
                "({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7})",
                edge.source_node_id,
                edge.source_coordinate,
                source_strand,
                edge.target_node_id,
                edge.target_coordinate,
                target_strand,
                edge.chromosome_index,
                edge.phased
            );
            edge_rows.push(edge_row);
        }
        let formatted_edge_rows = edge_rows.join(", ");

        let select_statement = format!("SELECT * FROM accession_edge WHERE (source_node_id, source_coordinate, source_strand, target_node_id, target_coordinate, target_strand, chromosome_index, phased) in ({0});", formatted_edge_rows);
        let existing_edges = AccessionEdge::query(conn, &select_statement, vec![]);
        for edge in existing_edges.iter() {
            edge_map.insert(EdgeData::from(edge), edge.id);
        }

        let existing_edge_set =
            HashSet::<EdgeData, RandomState>::from_iter(existing_edges.into_iter().map(
                |edge: Box<AccessionEdge>| EdgeData {
                    source_node_id: edge.source_node_id,
                    source_coordinate: edge.source_coordinate,
                    source_strand: edge.source_strand,
                    target_node_id: edge.target_node_id,
                    target_coordinate: edge.target_coordinate,
                    target_strand: edge.target_strand,
                    chromosome_index: edge.chromosome_index,
                    phased: edge.phased,
                },
            ));
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
                "({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7})",
                edge.source_node_id,
                edge.source_coordinate,
                source_strand,
                edge.target_node_id,
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

                let insert_statement = format!("INSERT INTO accession_edge (source_node_id, source_coordinate, source_strand, target_node_id, target_coordinate, target_strand, chromosome_index, phased) VALUES {0} RETURNING *;", formatted_edge_rows_to_insert);
                let mut stmt = conn.prepare(&insert_statement).unwrap();
                let rows = stmt
                    .query_map([], |row| Ok(AccessionEdge::process_row(row)))
                    .unwrap();
                for row in rows {
                    let edge = row.unwrap();
                    edge_map.insert(EdgeData::from(&edge), edge.id);
                }
            }
        }
        edges
            .iter()
            .map(|edge| *edge_map.get(edge).unwrap())
            .collect::<Vec<i64>>()
    }
}

impl Query for AccessionEdge {
    fn process_row(row: &Row) -> Box<Self> {
        Box::new(AccessionEdge {
            id: row.get(0).unwrap(),
            source_node_id: row.get(1).unwrap(),
            source_coordinate: row.get(2).unwrap(),
            source_strand: row.get(3).unwrap(),
            target_node_id: row.get(4).unwrap(),
            target_coordinate: row.get(5).unwrap(),
            target_strand: row.get(6).unwrap(),
            chromosome_index: row.get(7).unwrap(),
            phased: row.get(8).unwrap(),
        })
    }
}

impl From<&Box<AccessionEdge>> for EdgeData {
    fn from(item: &Box<AccessionEdge>) -> Self {
        EdgeData {
            source_node_id: item.source_node_id,
            source_coordinate: item.source_coordinate,
            source_strand: item.source_strand,
            target_node_id: item.target_node_id,
            target_coordinate: item.target_coordinate,
            target_strand: item.target_strand,
            chromosome_index: item.chromosome_index,
            phased: item.phased,
        }
    }
}

impl AccessionPath {
    pub fn bulk_create(conn: &Connection, accession_id: i64, edge_ids: Vec<i64>) {
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
            println!("is {insert_statement}");
            let _ = conn.execute(&insert_statement, ());
        }
    }
}

impl Query for AccessionPath {
    fn process_row(row: &Row) -> Box<Self> {
        Box::new(AccessionPath {
            id: row.get(0).unwrap(),
            accession_id: row.get(1).unwrap(),
            index_in_path: row.get(2).unwrap(),
            edge_id: row.get(3).unwrap(),
        })
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
        let accession = Accession::create(conn, "test", path.id, None, 3, 5);
        let accession_2 = Accession::create(conn, "test2", path.id, None, 3, 7);
        assert_eq!(
            Accession::query(
                conn,
                "select * from accession where name = ?1",
                vec![Value::from("test".to_string())]
            ),
            vec![Box::new(Accession {
                id: accession.id,
                name: "test".to_string(),
                path_id: path.id,
                accession_id: None,
                start: 3,
                end: 5
            })]
        )
    }
}
