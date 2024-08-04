use crate::models::edge::Edge;
use petgraph::graphmap::DiGraphMap;
use petgraph::prelude::Dfs;
use petgraph::visit::{IntoNeighborsDirected, NodeCount};
use petgraph::{Direction, Outgoing};
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use std::hash::Hash;
use std::iter::from_fn;

#[derive(Debug)]
pub struct Path {
    pub id: i32,
    pub name: String,
    pub block_group_id: i32,
    pub blocks: Vec<i32>,
}

impl Path {
    pub fn create(conn: &Connection, name: &str, block_group_id: i32, blocks: Vec<i32>) -> Path {
        let query = "INSERT INTO path (name, block_group_id) VALUES (?1, ?2) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map((name, block_group_id), |row| {
                Ok(Path {
                    id: row.get(0)?,
                    name: name.to_string(),
                    block_group_id,
                    blocks: blocks.clone(),
                })
            })
            .unwrap();
        let path = rows.next().unwrap().unwrap();

        for (index, block) in blocks.iter().enumerate() {
            let next_block = blocks.get(index + 1);
            if let Some(v) = next_block {
                PathBlock::create(conn, path.id, Some(*block), Some(*v));
            } else {
                PathBlock::create(conn, path.id, Some(*block), None);
            }
        }

        path
    }

    pub fn get(conn: &mut Connection, path_id: i32) -> Path {
        let query = "SELECT id, block_group_id, name from path where id = ?1;";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map((path_id,), |row| {
                Ok(Path {
                    id: row.get(0)?,
                    block_group_id: row.get(1)?,
                    name: row.get(2)?,
                    blocks: PathBlock::get_blocks(conn, path_id),
                })
            })
            .unwrap();
        rows.next().unwrap().unwrap()
    }

    pub fn get_paths(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<Path> {
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                let path_id = row.get(0).unwrap();
                Ok(Path {
                    id: path_id,
                    block_group_id: row.get(1)?,
                    name: row.get(2)?,
                    blocks: PathBlock::get_blocks(conn, path_id),
                })
            })
            .unwrap();
        let mut paths = vec![];
        for row in rows {
            paths.push(row.unwrap());
        }
        paths
    }
}

#[derive(Debug)]
pub struct PathBlock {
    pub id: i32,
    pub path_id: i32,
    pub source_block_id: Option<i32>,
    pub target_block_id: Option<i32>,
}

impl PathBlock {
    pub fn create(
        conn: &Connection,
        path_id: i32,
        source_block_id: Option<i32>,
        target_block_id: Option<i32>,
    ) -> PathBlock {
        let query =
            "INSERT INTO path_blocks (path_id, source_block_id, target_block_id) VALUES (?1, ?2, ?3) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map((path_id, source_block_id, target_block_id), |row| {
                Ok(PathBlock {
                    id: row.get(0)?,
                    path_id,
                    source_block_id,
                    target_block_id,
                })
            })
            .unwrap();
        match rows.next().unwrap() {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    let mut query;
                    let mut placeholders = vec![path_id];
                    if let Some(s) = source_block_id {
                        if let Some(t) = target_block_id {
                            query = "SELECT id from path_blocks where path_id = ?1 AND source_block_id = ?2 AND target_block_id = ?3;";
                            placeholders.push(s);
                            placeholders.push(t);
                        } else {
                            query = "SELECT id from path_blocks where path_id = ?1 AND source_block_id = ?2 AND target_block_id is null;";
                            placeholders.push(s);
                        }
                    } else if let Some(t) = target_block_id {
                        query = "SELECT id from path_blocks where path_id = ?1 AND source_block_id is null AND target_block_id = ?2;";
                        placeholders.push(t);
                    } else {
                        panic!("No block ids passed");
                    }
                    println!("{query} {placeholders:?}");
                    PathBlock {
                        id: conn
                            .query_row(query, params_from_iter(&placeholders), |row| row.get(0))
                            .unwrap(),
                        path_id,
                        source_block_id,
                        target_block_id,
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

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<PathBlock> {
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(PathBlock {
                    id: row.get(0)?,
                    path_id: row.get(1)?,
                    source_block_id: row.get(2)?,
                    target_block_id: row.get(3)?,
                })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }

    pub fn update(conn: &Connection, query: &str, placeholders: Vec<Value>) {
        let mut stmt = conn.prepare(query).unwrap();
        stmt.execute(params_from_iter(placeholders)).unwrap();
    }

    pub fn get_blocks(conn: &Connection, path_id: i32) -> Vec<i32> {
        let mut blocks = vec![];
        let graph = PathBlock::blocks_to_graph(conn, path_id);
        let mut start_node = None;
        for node in graph.nodes() {
            let has_incoming = graph.neighbors_directed(node, Direction::Incoming).next();
            if has_incoming.is_none() {
                start_node = Some(node);
                break;
            }
        }
        if start_node.is_none() {
            panic!("No starting block found in path {path_id}");
        }
        let mut dfs = Dfs::new(&graph, start_node.unwrap());
        while let Some(nx) = dfs.next(&graph) {
            blocks.push(nx as i32);
        }
        blocks
    }

    pub fn blocks_to_graph(conn: &Connection, path_id: i32) -> DiGraphMap<(u32), ()> {
        let query = "SELECT source_block_id, target_block_id from path_blocks where path_id = ?1;";
        let mut stmt = conn.prepare_cached(query).unwrap();
        let mut rows = stmt
            .query_map((path_id,), |row| {
                let source_id: Option<u32> = row.get(0).unwrap();
                let target_id: Option<u32> = row.get(1).unwrap();
                Ok((source_id, target_id))
            })
            .unwrap();
        let mut graph = DiGraphMap::new();
        for row in rows {
            let (source, target) = row.unwrap();
            if let Some(v) = source {
                graph.add_node(v);
            }
            if let Some(v) = target {
                graph.add_node(v);
            }
            if let Some(source_v) = source {
                if let Some(target_v) = target {
                    graph.add_edge(source_v, target_v, ());
                }
            }
        }
        graph
    }
}

// hacked from https://docs.rs/petgraph/latest/src/petgraph/algo/simple_paths.rs.html#36-102 to support digraphmap
pub fn all_simple_paths<G>(
    graph: G,
    from: G::NodeId,
    to: G::NodeId,
) -> impl Iterator<Item = Vec<G::NodeId>>
where
    G: NodeCount,
    G: IntoNeighborsDirected,
    G::NodeId: Eq + Hash,
{
    // list of visited nodes
    let mut visited = vec![from];
    // list of childs of currently exploring path nodes,
    // last elem is list of childs of last visited node
    let mut stack = vec![graph.neighbors_directed(from, Outgoing)];

    from_fn(move || {
        while let Some(children) = stack.last_mut() {
            if let Some(child) = children.next() {
                if child == to {
                    let path = visited.iter().cloned().chain(Some(to)).collect::<_>();
                    return Some(path);
                } else if !visited.contains(&child) {
                    visited.push(child);
                    stack.push(graph.neighbors_directed(child, Outgoing));
                }
            } else {
                stack.pop();
                visited.pop();
            }
        }
        None
    })
}
