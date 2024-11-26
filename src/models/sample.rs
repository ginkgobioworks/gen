use crate::graph::{GraphEdge, GraphNode};
use crate::models::block_group::BlockGroup;
use crate::models::traits::*;
use petgraph::prelude::DiGraphMap;
use rusqlite::{params, Connection, Result as SQLResult, Row};
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
    pub fn create(conn: &Connection, name: &str) -> SQLResult<Sample> {
        let mut stmt = conn
            .prepare("INSERT INTO samples (name) VALUES (?1) returning (name);")
            .unwrap();
        stmt.query_row((name,), |row| {
            Ok(Sample {
                name: name.to_string(),
            })
        })
    }

    pub fn get_or_create(conn: &Connection, name: &str) -> Sample {
        match Sample::create(conn, name) {
            Ok(sample) => sample,
            Err(rusqlite::Error::SqliteFailure(err, _details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    Sample {
                        name: name.to_string(),
                    }
                } else {
                    panic!("something bad happened querying the database")
                }
            }
            Err(err) => {
                panic!("something bad happened.")
            }
        }
    }

    pub fn get_graph<'a>(
        conn: &Connection,
        collection: &str,
        name: impl Into<Option<&'a str>>,
    ) -> DiGraphMap<GraphNode, GraphEdge> {
        let name = name.into();
        let block_groups = if let Some(sample) = name {
            BlockGroup::query(
                conn,
                "select * from block_groups where collection_name = ?1 AND sample_name = ?2;",
                params!(collection.to_string(), sample.to_string()),
            )
        } else {
            BlockGroup::query(
                conn,
                "select * from block_groups where collection_name = ?1 AND sample_name is null;",
                params!(collection.to_string()),
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

    pub fn get_or_create_child(
        conn: &Connection,
        collection_name: &str,
        sample_name: &str,
        parent_sample: Option<&str>,
    ) -> Sample {
        if let Ok(new_sample) = Sample::create(conn, sample_name) {
            let bgs = if let Some(parent) = parent_sample {
                BlockGroup::query(
                    conn,
                    "select * from block_groups where collection_name = ?1 AND sample_name = ?2",
                    params!(collection_name, parent),
                )
            } else {
                BlockGroup::query(conn, "select * from block_groups where collection_name = ?1 AND sample_name is null;", params!(collection_name))
            };
            for bg in bgs.iter() {
                BlockGroup::get_or_create_sample_block_group(
                    conn,
                    collection_name,
                    &new_sample.name,
                    &bg.name,
                    parent_sample,
                );
            }
            new_sample
        } else {
            Sample {
                name: sample_name.to_string(),
            }
        }
    }
}
