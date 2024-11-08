use crate::graph::{GraphEdge, GraphNode};
use crate::models::block_group::BlockGroup;
use crate::models::traits::*;
use petgraph::prelude::DiGraphMap;
use rusqlite::types::Value;
use rusqlite::{Connection, Row};
use std::fmt::*;

#[derive(Debug)]
pub struct Sample {
    pub name: String,
}

impl Query for Sample {
    type Model = Sample;
    fn process_row(row: &Row) -> Self::Model {
        Sample {
            name: row.get(0).unwrap(),
        }
    }
}

impl Sample {
    pub fn create(conn: &Connection, name: &str) -> Sample {
        let mut stmt = conn
            .prepare("INSERT INTO samples (name) VALUES (?1)")
            .unwrap();
        match stmt.execute((name,)) {
            Ok(_) => Sample {
                name: name.to_string(),
            },
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    Sample {
                        name: name.to_string(),
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

    pub fn get_graph(
        conn: &Connection,
        collection: &str,
        name: Option<&str>,
    ) -> DiGraphMap<GraphNode, GraphEdge> {
        let block_groups = if let Some(sample) = name {
            BlockGroup::query(
                conn,
                "select * from block_groups where collection_name = ?1 AND sample_name = ?2;",
                vec![
                    Value::from(collection.to_string()),
                    Value::from(sample.to_string()),
                ],
            )
        } else {
            BlockGroup::query(
                conn,
                "select * from block_groups where collection_name = ?1 AND sample_name is null;",
                vec![Value::from(collection.to_string())],
            )
        };
        let mut sample_graph: DiGraphMap<GraphNode, GraphEdge> = DiGraphMap::new();
        for bg in block_groups {
            let graph = BlockGroup::get_graph(conn, bg.id);
            for node in graph.nodes() {
                sample_graph.add_node(node);
            }
            for (source, dest, weight) in graph.all_edges() {
                sample_graph.add_edge(source, dest, *weight);
            }
        }
        sample_graph
    }
}
