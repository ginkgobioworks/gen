use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::from_fn;

use crate::models::strand::Strand;
use petgraph::prelude::EdgeRef;
use petgraph::visit::{GraphRef, IntoEdges, IntoNeighbors, IntoNeighborsDirected, NodeCount};
use petgraph::Direction;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct GraphNode {
    pub block_id: i64,
    pub node_id: i64,
    pub sequence_start: i64,
    pub sequence_end: i64,
}

impl GraphNode {
    pub fn length(&self) -> i64 {
        self.sequence_end - self.sequence_start
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct GraphEdge {
    pub edge_id: i64,
    pub chromosome_index: i64,
    pub phased: i64,
    pub source_strand: Strand,
    pub target_strand: Strand,
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
    let mut stack = vec![graph.neighbors_directed(from, Direction::Outgoing)];

    from_fn(move || {
        while let Some(children) = stack.last_mut() {
            if let Some(child) = children.next() {
                if child == to {
                    let path = visited.iter().cloned().chain(Some(to)).collect::<_>();
                    return Some(path);
                } else if !visited.contains(&child) {
                    visited.push(child);
                    stack.push(graph.neighbors_directed(child, Direction::Outgoing));
                }
            } else {
                stack.pop();
                visited.pop();
            }
        }
        None
    })
}

pub fn all_simple_paths_by_edge<G>(
    graph: G,
    from: G::NodeId,
    to: G::NodeId,
) -> impl Iterator<Item = Vec<G::EdgeRef>>
where
    G: NodeCount + IntoEdges,
    G: IntoNeighborsDirected,
    G::NodeId: Eq + Hash,
{
    // list of visited nodes
    let mut visited = vec![from];
    // list of childs of currently exploring path nodes,
    // last elem is list of childs of last visited node
    let mut path: Vec<G::EdgeRef> = vec![];
    let mut stack = vec![graph.edges(from)];

    from_fn(move || {
        while let Some(edges) = stack.last_mut() {
            if let Some(edge) = edges.next() {
                let target = edge.target();
                if target == to {
                    let a_path = path.iter().cloned().chain(Some(edge)).collect::<_>();
                    return Some(a_path);
                } else if !visited.contains(&target) {
                    path.push(edge);
                    visited.push(target);
                    stack.push(graph.edges(target));
                }
            } else {
                stack.pop();
                path.pop();
                visited.pop();
            }
        }
        None
    })
}

pub fn all_reachable_nodes<G>(graph: G, nodes: &[G::NodeId]) -> HashSet<G::NodeId>
where
    G: GraphRef + IntoNeighbors,
    G::NodeId: Eq + Hash + Debug,
{
    let mut stack = VecDeque::new();
    let mut reachable = HashSet::new();
    for node in nodes.iter() {
        stack.push_front(*node);
        reachable.insert(*node);
        while let Some(nx) = stack.pop_front() {
            for succ in graph.neighbors(nx) {
                if !reachable.contains(&succ) {
                    reachable.insert(succ);
                    stack.push_back(succ);
                }
            }
        }
    }
    reachable
}

#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::graphmap::DiGraphMap;
    use std::collections::HashSet;

    #[test]
    fn test_path_graph() {
        let mut graph: DiGraphMap<i64, ()> = DiGraphMap::new();
        graph.add_node(1);
        graph.add_node(2);
        graph.add_node(3);

        graph.add_edge(1, 2, ());
        graph.add_edge(2, 3, ());

        let paths = all_simple_paths(&graph, 1, 3).collect::<Vec<Vec<i64>>>();
        assert_eq!(paths.len(), 1);
        let path = paths.first().unwrap().clone();
        assert_eq!(path, vec![1, 2, 3]);
    }

    #[test]
    fn test_get_simple_paths_by_edge() {
        let mut graph: DiGraphMap<i64, ()> = DiGraphMap::new();
        graph.add_node(1);
        graph.add_node(2);
        graph.add_node(3);
        graph.add_node(4);
        graph.add_node(5);
        graph.add_node(6);
        graph.add_node(7);
        graph.add_node(8);
        graph.add_node(9);

        graph.add_edge(1, 2, ());
        graph.add_edge(2, 3, ());
        graph.add_edge(3, 4, ());
        graph.add_edge(4, 5, ());
        graph.add_edge(2, 6, ());
        graph.add_edge(6, 7, ());
        graph.add_edge(7, 4, ());
        graph.add_edge(6, 8, ());
        graph.add_edge(8, 7, ());

        let edge_path =
            all_simple_paths_by_edge(&graph, 1, 5).collect::<Vec<Vec<(i64, i64, &())>>>();
        assert_eq!(
            edge_path,
            vec![
                vec![(1, 2, &()), (2, 3, &()), (3, 4, &()), (4, 5, &())],
                vec![
                    (1, 2, &()),
                    (2, 6, &()),
                    (6, 7, &()),
                    (7, 4, &()),
                    (4, 5, &())
                ],
                vec![
                    (1, 2, &()),
                    (2, 6, &()),
                    (6, 8, &()),
                    (8, 7, &()),
                    (7, 4, &()),
                    (4, 5, &())
                ]
            ]
        );
    }

    #[test]
    fn test_two_path_graph() {
        let mut graph: DiGraphMap<i64, ()> = DiGraphMap::new();
        graph.add_node(1);
        graph.add_node(2);
        graph.add_node(3);
        graph.add_node(4);

        graph.add_edge(1, 2, ());
        graph.add_edge(1, 3, ());
        graph.add_edge(2, 4, ());
        graph.add_edge(3, 4, ());

        let paths = all_simple_paths(&graph, 1, 4).collect::<Vec<Vec<i64>>>();
        assert_eq!(paths.len(), 2);
        assert_eq!(
            HashSet::<Vec<i64>>::from_iter::<Vec<Vec<i64>>>(paths),
            HashSet::from_iter(vec![vec![1, 2, 4], vec![1, 3, 4]])
        );
    }

    #[test]
    fn test_two_by_two_combinatorial_graph() {
        let mut graph: DiGraphMap<i64, ()> = DiGraphMap::new();
        graph.add_node(1);
        graph.add_node(2);
        graph.add_node(3);
        graph.add_node(4);
        graph.add_node(5);
        graph.add_node(6);
        graph.add_node(7);

        graph.add_edge(1, 2, ());
        graph.add_edge(1, 3, ());
        graph.add_edge(2, 4, ());
        graph.add_edge(3, 4, ());
        graph.add_edge(4, 5, ());
        graph.add_edge(4, 6, ());
        graph.add_edge(5, 7, ());
        graph.add_edge(6, 7, ());

        let paths = all_simple_paths(&graph, 1, 7).collect::<Vec<Vec<i64>>>();
        assert_eq!(paths.len(), 4);
        assert_eq!(
            HashSet::<Vec<i64>>::from_iter::<Vec<Vec<i64>>>(paths),
            HashSet::from_iter(vec![
                vec![1, 2, 4, 5, 7],
                vec![1, 3, 4, 5, 7],
                vec![1, 2, 4, 6, 7],
                vec![1, 3, 4, 6, 7]
            ])
        );
    }

    #[test]
    fn test_three_by_three_combinatorial_graph() {
        let mut graph: DiGraphMap<i64, ()> = DiGraphMap::new();
        graph.add_node(1);
        graph.add_node(2);
        graph.add_node(3);
        graph.add_node(4);
        graph.add_node(5);
        graph.add_node(6);
        graph.add_node(7);
        graph.add_node(8);
        graph.add_node(9);

        graph.add_edge(1, 2, ());
        graph.add_edge(1, 3, ());
        graph.add_edge(1, 4, ());
        graph.add_edge(2, 5, ());
        graph.add_edge(3, 5, ());
        graph.add_edge(4, 5, ());
        graph.add_edge(5, 6, ());
        graph.add_edge(5, 7, ());
        graph.add_edge(5, 8, ());
        graph.add_edge(6, 9, ());
        graph.add_edge(7, 9, ());
        graph.add_edge(8, 9, ());

        let paths = all_simple_paths(&graph, 1, 9).collect::<Vec<Vec<i64>>>();
        assert_eq!(paths.len(), 9);
        let expected_paths = vec![
            vec![1, 2, 5, 6, 9],
            vec![1, 3, 5, 6, 9],
            vec![1, 4, 5, 6, 9],
            vec![1, 2, 5, 7, 9],
            vec![1, 3, 5, 7, 9],
            vec![1, 4, 5, 7, 9],
            vec![1, 2, 5, 8, 9],
            vec![1, 3, 5, 8, 9],
            vec![1, 4, 5, 8, 9],
        ];
        assert_eq!(
            HashSet::<Vec<i64>>::from_iter::<Vec<Vec<i64>>>(paths),
            HashSet::from_iter(expected_paths)
        );
    }

    #[test]
    fn test_super_bubble_path() {
        // This graph looks like this:
        //              8
        //            /  \
        //          6  -> 7
        //         /        \
        //    1 -> 2 -> 3 -> 4 -> 5
        //
        //  We ensure that we capture all 3 paths from 1 -> 5
        let mut graph: DiGraphMap<i64, ()> = DiGraphMap::new();
        graph.add_node(1);
        graph.add_node(2);
        graph.add_node(3);
        graph.add_node(4);
        graph.add_node(5);
        graph.add_node(6);
        graph.add_node(7);
        graph.add_node(8);
        graph.add_node(9);

        graph.add_edge(1, 2, ());
        graph.add_edge(2, 3, ());
        graph.add_edge(3, 4, ());
        graph.add_edge(4, 5, ());
        graph.add_edge(2, 6, ());
        graph.add_edge(6, 7, ());
        graph.add_edge(7, 4, ());
        graph.add_edge(6, 8, ());
        graph.add_edge(8, 7, ());

        let paths = all_simple_paths(&graph, 1, 5).collect::<Vec<Vec<i64>>>();
        assert_eq!(
            HashSet::<Vec<i64>>::from_iter::<Vec<Vec<i64>>>(paths),
            HashSet::from_iter(vec![
                vec![1, 2, 3, 4, 5],
                vec![1, 2, 6, 7, 4, 5],
                vec![1, 2, 6, 8, 7, 4, 5]
            ])
        );
    }

    #[test]
    fn test_finds_all_reachable_nodes() {
        //
        //   1 -> 2 -> 3 -> 4 -> 5
        //           /
        //   6 -> 7
        //
        let mut graph: DiGraphMap<i64, ()> = DiGraphMap::new();
        graph.add_node(1);
        graph.add_node(2);
        graph.add_node(3);
        graph.add_node(4);
        graph.add_node(5);
        graph.add_node(6);
        graph.add_node(7);

        graph.add_edge(1, 2, ());
        graph.add_edge(2, 3, ());
        graph.add_edge(3, 4, ());
        graph.add_edge(4, 5, ());
        graph.add_edge(6, 7, ());
        graph.add_edge(7, 3, ());

        assert_eq!(
            all_reachable_nodes(&graph, &[1]),
            HashSet::from_iter(vec![1, 2, 3, 4, 5])
        );

        assert_eq!(
            all_reachable_nodes(&graph, &[1, 6]),
            HashSet::from_iter(vec![1, 2, 3, 4, 5, 6, 7])
        );

        assert_eq!(
            all_reachable_nodes(&graph, &[3]),
            HashSet::from_iter(vec![3, 4, 5])
        );

        assert_eq!(
            all_reachable_nodes(&graph, &[5]),
            HashSet::from_iter(vec![5])
        );
    }
}
