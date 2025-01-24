use crate::models::block_group::NodeIntervalBlock;
use crate::models::strand::Strand;
use interavl::IntervalTree as IT2;
use intervaltree::IntervalTree;
use petgraph::graphmap::DiGraphMap;
use petgraph::prelude::EdgeRef;
use petgraph::visit::{
    Dfs, GraphRef, IntoEdgeReferences, IntoEdges, IntoNeighbors, IntoNeighborsDirected, NodeCount,
    Reversed,
};
use petgraph::Direction;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::iter::from_fn;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct GraphNode {
    pub block_id: i64,
    pub node_id: i64,
    pub sequence_start: i64,
    pub sequence_end: i64,
}

impl fmt::Display for GraphNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}[{}-{}]",
            self.node_id, self.sequence_start, self.sequence_end
        )
    }
}

impl GraphNode {
    pub fn length(&self) -> i64 {
        self.sequence_end - self.sequence_start
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct GraphEdge {
    pub edge_id: i64,
    pub source_strand: Strand,
    pub target_strand: Strand,
    pub chromosome_index: i64,
    pub phased: i64,
}

#[derive(Debug)]
pub struct OperationGraph {
    pub graph: DiGraphMap<usize, ()>,
    max_node_id: usize,
    pub node_ids: HashMap<String, usize>,
    reverse_map: HashMap<usize, String>,
}

impl Default for OperationGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationGraph {
    pub fn new() -> Self {
        OperationGraph {
            graph: DiGraphMap::new(),
            max_node_id: 0,
            node_ids: HashMap::new(),
            reverse_map: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, hash_id: &str) -> usize {
        let node_id = *self.node_ids.entry(hash_id.to_string()).or_insert_with(|| {
            let node_id = self.max_node_id;
            self.reverse_map.insert(node_id, hash_id.to_string());
            self.graph.add_node(node_id);
            self.max_node_id += 1;
            node_id
        });
        node_id
    }

    pub fn remove_node(&mut self, node_id: usize) {
        self.graph.remove_node(node_id);
        if let Some(key) = self.reverse_map.remove(&node_id) {
            self.node_ids.remove(&key).unwrap();
        }
    }

    pub fn remove_key(&mut self, hash_id: &str) {
        if let Some(node_index) = self.node_ids.remove(hash_id) {
            self.graph.remove_node(node_index);
            self.reverse_map.remove(&node_index).unwrap();
        }
    }

    pub fn get_node(&self, node_id: &str) -> usize {
        self.node_ids[node_id]
    }

    pub fn get_key(&self, index: usize) -> String {
        self.reverse_map[&index].clone()
    }

    pub fn add_edge(&mut self, src: &str, target: &str) {
        let src_node_id = self.add_node(src);
        let target_node_id = self.add_node(target);
        self.graph.add_edge(src_node_id, target_node_id, ());
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

pub fn all_intermediate_edges<G>(
    graph: G,
    from: G::NodeId,
    to: G::NodeId,
) -> Vec<<G as IntoEdgeReferences>::EdgeRef>
where
    G: GraphRef + IntoEdges + petgraph::visit::IntoNeighborsDirected + petgraph::visit::Visitable,
    G::NodeId: Eq + Hash + std::fmt::Display,
{
    let mut outgoing_nodes = HashSet::new();
    outgoing_nodes.insert(from);
    let mut dfs_outbound = Dfs::new(graph, from);

    while let Some(outgoing_node) = dfs_outbound.next(graph) {
        outgoing_nodes.insert(outgoing_node);
    }

    let reversed_graph = Reversed(&graph);
    let mut incoming_nodes = HashSet::new();
    incoming_nodes.insert(to);
    let mut dfs_inbound = Dfs::new(reversed_graph, to);
    while let Some(incoming_node) = dfs_inbound.next(reversed_graph) {
        incoming_nodes.insert(incoming_node);
    }

    let common_nodes: HashSet<&<G as petgraph::visit::GraphBase>::NodeId> =
        outgoing_nodes.intersection(&incoming_nodes).collect();
    let mut common_edgerefs = vec![];
    for edge in graph.edge_references() {
        let (source, target) = (edge.source(), edge.target());

        if common_nodes.contains(&source) && common_nodes.contains(&target) {
            common_edgerefs.push(edge);
        }
    }

    common_edgerefs
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

pub fn flatten_to_interval_tree(
    graph: &DiGraphMap<GraphNode, GraphEdge>,
    remove_ambiguous_positions: bool,
) -> IntervalTree<i64, NodeIntervalBlock> {
    #[derive(Clone, Debug, Ord, PartialOrd, Eq, Hash, PartialEq)]
    struct NodeP {
        x: i64,
        y: i64,
    }
    let mut excluded_nodes = HashSet::new();
    let mut node_tree: HashMap<i64, IT2<NodeP, i64>> = HashMap::new();

    let mut start_nodes = vec![];
    let mut end_nodes = vec![];
    for node in graph.nodes() {
        let has_incoming = graph.neighbors_directed(node, Direction::Incoming).next();
        let has_outgoing = graph.neighbors_directed(node, Direction::Outgoing).next();
        if has_incoming.is_none() {
            start_nodes.push(node);
        }
        if has_outgoing.is_none() {
            end_nodes.push(node);
        }
    }

    let mut spans: HashSet<NodeIntervalBlock> = HashSet::new();

    for start in start_nodes.iter() {
        for end_node in end_nodes.iter() {
            for path in all_simple_paths_by_edge(&graph, *start, *end_node) {
                let mut offset = 0;
                for (source_node, target_node, edge) in path.iter() {
                    let block_len = source_node.length();
                    let node_start = offset;
                    let node_end = offset + block_len;
                    spans.insert(NodeIntervalBlock {
                        block_id: source_node.block_id,
                        node_id: source_node.node_id,
                        start: node_start,
                        end: node_end,
                        sequence_start: source_node.sequence_start,
                        sequence_end: source_node.sequence_end,
                        strand: edge.source_strand,
                    });
                    spans.insert(NodeIntervalBlock {
                        block_id: target_node.block_id,
                        node_id: target_node.node_id,
                        start: node_end,
                        end: node_end + target_node.length(),
                        sequence_start: target_node.sequence_start,
                        sequence_end: target_node.sequence_end,
                        strand: edge.target_strand,
                    });
                    if remove_ambiguous_positions {
                        for (node_id, node_range) in [
                            (
                                source_node.node_id,
                                NodeP {
                                    x: node_start,
                                    y: source_node.sequence_start,
                                }..NodeP {
                                    x: node_end,
                                    y: source_node.sequence_end,
                                },
                            ),
                            (
                                target_node.node_id,
                                NodeP {
                                    x: node_end,
                                    y: target_node.sequence_start,
                                }..NodeP {
                                    x: node_end + target_node.length(),
                                    y: target_node.sequence_end,
                                },
                            ),
                        ] {
                            // TODO; This could be a bit better by trying to conserve subregions
                            // within a node that are not ambiguous instead of kicking the entire
                            // node out.
                            node_tree
                                .entry(node_id)
                                .and_modify(|tree| {
                                    for (stored_range, _stored_node_id) in
                                        tree.iter_overlaps(&node_range)
                                    {
                                        if *stored_range != node_range {
                                            excluded_nodes.insert(node_id);
                                            break;
                                        }
                                    }
                                    tree.insert(node_range.clone(), node_id);
                                })
                                .or_insert_with(|| {
                                    let mut t = IT2::default();
                                    t.insert(node_range.clone(), node_id);
                                    t
                                });
                        }
                    }
                    offset += block_len;
                }
            }
        }
    }

    let tree: IntervalTree<i64, NodeIntervalBlock> = spans
        .iter()
        .filter(|block| !remove_ambiguous_positions || !excluded_nodes.contains(&block.node_id))
        .map(|block| (block.start..block.end, *block))
        .collect();
    tree
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

    mod test_all_intermediate_edges {
        use super::*;
        #[test]
        fn test_one_part_group() {
            //
            //   1 -> 2 -> 3 -> 4 -> 5
            //         \-> 6 /
            //
            let mut graph: DiGraphMap<i64, ()> = DiGraphMap::new();
            graph.add_node(1);
            graph.add_node(2);
            graph.add_node(3);
            graph.add_node(4);
            graph.add_node(5);
            graph.add_node(6);

            graph.add_edge(1, 2, ());
            graph.add_edge(2, 3, ());
            graph.add_edge(3, 4, ());
            graph.add_edge(4, 5, ());
            graph.add_edge(2, 6, ());
            graph.add_edge(6, 4, ());

            let result_edges = all_intermediate_edges(&graph, 2, 4);
            let intermediate_edges = result_edges
                .iter()
                .map(|(source, target, _weight)| (*source, *target))
                .collect::<Vec<(i64, i64)>>();
            assert_eq!(intermediate_edges, vec![(2, 3), (3, 4), (2, 6), (6, 4)]);
        }

        #[test]
        fn test_two_part_groups() {
            //
            //   1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7
            //         \-> 8 /    \-> 9 /
            //
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
            graph.add_edge(5, 6, ());
            graph.add_edge(6, 7, ());
            graph.add_edge(2, 8, ());
            graph.add_edge(8, 4, ());
            graph.add_edge(4, 9, ());
            graph.add_edge(9, 6, ());

            let result_edges = all_intermediate_edges(&graph, 2, 6);
            let intermediate_edges = result_edges
                .iter()
                .map(|(source, target, _weight)| (*source, *target))
                .collect::<Vec<(i64, i64)>>();
            assert_eq!(
                intermediate_edges,
                vec![
                    (2, 3),
                    (3, 4),
                    (4, 5),
                    (5, 6),
                    (2, 8),
                    (8, 4),
                    (4, 9),
                    (9, 6)
                ]
            );
        }

        #[test]
        fn test_one_part_group_with_unrelated_edges() {
            //
            //        / ------- 7 \
            //   1 -> 2 -> 3 -> 4 -> 5
            //         \-> 6 /
            //
            // Because 7 has 5 as a target, it is excluded from the subgraph from 2 to 4
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
            graph.add_edge(2, 6, ());
            graph.add_edge(6, 4, ());
            graph.add_edge(2, 7, ());
            graph.add_edge(7, 5, ());

            let result_edges = all_intermediate_edges(&graph, 2, 4);
            let intermediate_edges = result_edges
                .iter()
                .map(|(source, target, _weight)| (*source, *target))
                .collect::<Vec<(i64, i64)>>();
            assert_eq!(intermediate_edges, vec![(2, 3), (3, 4), (2, 6), (6, 4)]);
        }
    }
}
