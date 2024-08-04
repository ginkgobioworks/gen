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
    pub edges: Vec<i32>,
}

impl Path {
    pub fn create(conn: &Connection, name: &str, block_group_id: i32, edges: Vec<i32>) -> Path {
        let query = "INSERT INTO path (name, block_group_id) VALUES (?1, ?2) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map((name, block_group_id), |row| {
                Ok(Path {
                    id: row.get(0)?,
                    name: name.to_string(),
                    block_group_id,
                    edges: edges.clone(),
                })
            })
            .unwrap();
        let path = rows.next().unwrap().unwrap();

        for (index, edge) in edges.iter().enumerate() {
            let next_edge = edges.get(index + 1);
            if let Some(v) = next_edge {
                PathEdge::create(conn, path.id, Some(*edge), Some(*v));
            } else {
                PathEdge::create(conn, path.id, Some(*edge), None);
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
                    edges: PathEdge::get_edges(conn, path_id),
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
                    edges: PathEdge::get_edges(conn, path_id),
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
pub struct PathEdge {
    pub id: i32,
    pub path_id: i32,
    pub source_edge_id: Option<i32>,
    pub target_edge_id: Option<i32>,
}

impl PathEdge {
    pub fn create(
        conn: &Connection,
        path_id: i32,
        source_edge_id: Option<i32>,
        target_edge_id: Option<i32>,
    ) -> PathEdge {
        let query =
            "INSERT INTO path_edges (path_id, source_edge_id, target_edge_id) VALUES (?1, ?2, ?3) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map((path_id, source_edge_id, target_edge_id), |row| {
                Ok(PathEdge {
                    id: row.get(0)?,
                    path_id,
                    source_edge_id,
                    target_edge_id,
                })
            })
            .unwrap();
        rows.next().unwrap().unwrap()
    }

    pub fn get_edges(conn: &Connection, path_id: i32) -> Vec<i32> {
        let mut edges = vec![];
        let query = "SELECT source_edge_id, target_edge_id from path_edges where path_id = ?1;";
        let mut stmt = conn.prepare_cached(query).unwrap();
        let mut rows = stmt
            .query_map((path_id,), |row| {
                let source_id: Option<u32> = row.get(0).unwrap();
                let target_id: Option<u32> = row.get(1).unwrap();
                Ok((source_id, target_id))
            })
            .unwrap();
        let mut edge_graph = DiGraphMap::new();
        for row in rows {
            let (source, target) = row.unwrap();
            if let Some(v) = source {
                edge_graph.add_node(v);
            }
            if let Some(v) = target {
                edge_graph.add_node(v);
            }
            if let Some(source_v) = source {
                if let Some(target_v) = target {
                    edge_graph.add_edge(source_v, target_v, ());
                }
            }
        }
        let mut start_edge = None;
        for node in edge_graph.nodes() {
            let has_incoming = edge_graph
                .neighbors_directed(node, Direction::Incoming)
                .next();
            if has_incoming.is_none() {
                start_edge = Some(node);
                break;
            }
        }
        if start_edge.is_none() {
            panic!("No starting edge found in path {path_id}");
        }
        let mut dfs = Dfs::new(&edge_graph, start_edge.unwrap());
        while let Some(nx) = dfs.next(&edge_graph) {
            edges.push(nx as i32);
        }
        edges
    }

    pub fn edges_to_graph(conn: &Connection, path_id: i32) -> DiGraphMap<(u32), ()> {
        let edges = PathEdge::get_edges(conn, path_id);
        let edge_str = (*edges)
            .iter()
            .map(|v| format!("{v}"))
            .collect::<Vec<_>>()
            .join(",");
        let query = format!("SELECT source_id, target_id from edges where id IN ({edge_str});");
        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt
            .query_map([], |row| {
                let source_id: Option<u32> = row.get(0).unwrap();
                let target_id: Option<u32> = row.get(1).unwrap();
                Ok((source_id, target_id))
            })
            .unwrap();
        let mut graph = DiGraphMap::new();
        for edge in rows {
            let (source, target) = edge.unwrap();
            if let Some(source_value) = source {
                graph.add_node(source_value);
                if let Some(target_value) = target {
                    graph.add_edge(source_value, target_value, ());
                }
            }
            if let Some(target_value) = target {
                graph.add_node(target_value);
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
