use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::visit::{
    EdgeRef, GraphBase, IntoEdgeReferences, IntoEdges, IntoNeighbors, NodeIndexable,
};
use petgraph::{Directed, Direction};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

#[derive(Debug)]
pub struct GenGraph<N, E>
where
    N: Eq + Hash + Clone,
{
    nodes: HashMap<N, NodeIndex>,
    node_weights: Vec<Option<N>>,
    edges: Vec<Option<(NodeIndex, NodeIndex, E)>>,
}

impl<N, E> GraphBase for GenGraph<N, E>
where
    N: Eq + Hash + Clone,
{
    type NodeId = NodeIndex;
    type EdgeId = EdgeIndex;
}

impl<N, E> NodeIndexable for GenGraph<N, E>
where
    N: Eq + Hash + Clone,
{
    fn node_bound(&self) -> usize {
        self.node_weights.len()
    }

    fn to_index(&self, ix: Self::NodeId) -> usize {
        ix.index()
    }

    fn from_index(&self, ix: usize) -> Self::NodeId {
        NodeIndex::new(ix)
    }
}

impl<'a, N, E> IntoNeighbors for &'a GenGraph<N, E>
where
    N: Eq + Hash + Clone,
{
    type Neighbors = Box<dyn Iterator<Item = Self::NodeId> + 'a>;

    fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
        Box::new(self.edges.iter().filter_map(move |e| {
            e.as_ref()
                .and_then(|(src, dst, _)| if *src == a { Some(*dst) } else { None })
        }))
    }
}

impl<'a, N, E: Copy> IntoEdgeReferences for &'a GenGraph<N, E>
where
    N: Eq + Hash + Clone,
{
    type EdgeRef = EdgeReference<'a, Self>;
    type EdgeReferences = Box<dyn Iterator<Item = Self::EdgeRef> + 'a>;

    fn edge_references(self) -> Self::EdgeReferences {
        Box::new(self.edges.iter().enumerate().filter_map(move |(i, e)| {
            e.as_ref()
                .map(|(src, dst, weight)| EdgeReference::new(EdgeIndex::new(i), *src, *dst, weight))
        }))
    }
}

impl<'a, N, E: Copy> IntoEdges for &'a GenGraph<N, E>
where
    N: Eq + Hash + Clone,
{
    type Edges = Box<dyn Iterator<Item = EdgeReference<'a, Self>> + 'a>;

    fn edges(self, a: Self::NodeId) -> Self::Edges {
        Box::new(self.edges.iter().enumerate().filter_map(move |(i, e)| {
            e.as_ref().and_then(|(src, dst, weight)| {
                if *src == a {
                    Some(EdgeReference::new(EdgeIndex::new(i), *src, *dst, weight))
                } else {
                    None
                }
            })
        }))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct EdgeReference<'a, G: GraphBase + Copy + petgraph::visit::Data<EdgeWeight: Copy>> {
    edge_id: G::EdgeId,
    src: G::NodeId,
    dst: G::NodeId,
    weight: &'a G::EdgeWeight,
}

impl<'a, G: GraphBase + Copy + petgraph::visit::Data<EdgeWeight: Copy>> EdgeReference<'a, G> {
    fn new(edge_id: G::EdgeId, src: G::NodeId, dst: G::NodeId, weight: &'a G::EdgeWeight) -> Self {
        Self {
            edge_id,
            src,
            dst,
            weight,
        }
    }
}

impl<G: GraphBase + Copy + petgraph::visit::Data<EdgeWeight: Copy>> EdgeRef
    for EdgeReference<'_, G>
{
    type NodeId = G::NodeId;
    type EdgeId = G::EdgeId;
    type Weight = G::EdgeWeight;

    fn source(&self) -> Self::NodeId {
        self.src
    }

    fn target(&self) -> Self::NodeId {
        self.dst
    }

    fn weight(&self) -> &Self::Weight {
        self.weight
    }

    fn id(&self) -> Self::EdgeId {
        self.edge_id
    }
}

impl<N, E> Default for GenGraph<N, E>
where
    N: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<N, E> GenGraph<N, E>
where
    N: Eq + Hash + Clone,
{
    pub fn new() -> Self {
        GenGraph {
            nodes: HashMap::new(),
            node_weights: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_node(&mut self, n: N) -> N {
        if self.nodes.contains_key(&n) {
            return n;
        }

        let node_idx = NodeIndex::new(self.node_weights.len());
        self.nodes.insert(n.clone(), node_idx);
        self.node_weights.push(Some(n.clone()));
        n
    }

    pub fn add_edge(&mut self, a: N, b: N, weight: E) -> EdgeIndex {
        let a_idx = self.add_node(a);
        let b_idx = self.add_node(b);

        let a_idx = self.nodes[&a_idx];
        let b_idx = self.nodes[&b_idx];

        let edge_idx = EdgeIndex::new(self.edges.len());
        self.edges.push(Some((a_idx, b_idx, weight)));
        edge_idx
    }

    pub fn remove_node(&mut self, n: &N) -> bool {
        if let Some(node_idx) = self.nodes.remove(n) {
            self.node_weights[node_idx.index()] = None;

            for edge in self.edges.iter_mut() {
                if let Some((src, dst, _)) = edge {
                    if *src == node_idx || *dst == node_idx {
                        *edge = None;
                    }
                }
            }
            true
        } else {
            false
        }
    }

    pub fn remove_edge(&mut self, edge: EdgeIndex) -> Option<E> {
        if edge.index() >= self.edges.len() {
            return None;
        }
        self.edges[edge.index()].take().map(|(_, _, weight)| weight)
    }

    pub fn neighbors(&self, n: &N) -> impl Iterator<Item = &N> {
        let node_idx = self.nodes.get(n);
        self.edges.iter().filter_map(move |edge| {
            edge.as_ref().and_then(|(src, dst, _)| {
                if Some(src) == node_idx {
                    self.node_weights[dst.index()].as_ref()
                } else if Some(dst) == node_idx {
                    self.node_weights[src.index()].as_ref()
                } else {
                    None
                }
            })
        })
    }

    pub fn edge_weight(&self, edge: EdgeIndex) -> Option<&E> {
        self.edges
            .get(edge.index())
            .and_then(|e| e.as_ref())
            .map(|(_, _, weight)| weight)
    }

    pub fn edge_weight_mut(&mut self, edge: EdgeIndex) -> Option<&mut E> {
        self.edges
            .get_mut(edge.index())
            .and_then(|e| e.as_mut())
            .map(|(_, _, weight)| weight)
    }

    pub fn contains_node(&self, n: &N) -> bool {
        self.nodes.contains_key(n)
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.iter().filter(|e| e.is_some()).count()
    }

    pub fn neighbors_directed(&self, n: &N, dir: Direction) -> impl Iterator<Item = &N> {
        let node_idx = self.nodes.get(n);
        self.edges.iter().filter_map(move |edge| {
            edge.as_ref().and_then(|(src, dst, _)| match dir {
                Direction::Outgoing => {
                    if Some(src) == node_idx {
                        self.node_weights[dst.index()].as_ref()
                    } else {
                        None
                    }
                }
                Direction::Incoming => {
                    if Some(dst) == node_idx {
                        self.node_weights[src.index()].as_ref()
                    } else {
                        None
                    }
                }
            })
        })
    }

    pub fn successors(&self, n: &N) -> impl Iterator<Item = &N> {
        self.neighbors_directed(n, Direction::Outgoing)
    }

    pub fn predecessors(&self, n: &N) -> impl Iterator<Item = &N> {
        self.neighbors_directed(n, Direction::Incoming)
    }
}

impl<N, E: Copy> petgraph::data::DataMap for GenGraph<N, E>
where
    N: Eq + Hash + Clone,
{
    fn node_weight(&self, id: Self::NodeId) -> Option<&Self::NodeWeight> {
        self.node_weights.get(id.index()).and_then(|n| n.as_ref())
    }

    fn edge_weight(&self, id: Self::EdgeId) -> Option<&Self::EdgeWeight> {
        self.edges
            .get(id.index())
            .and_then(|e| e.as_ref())
            .map(|(_, _, weight)| weight)
    }
}

impl<N, E: Copy> petgraph::visit::Data for GenGraph<N, E>
where
    N: Eq + Hash + Clone,
{
    type NodeWeight = N;
    type EdgeWeight = E;
}

impl<N, E> petgraph::visit::GraphProp for GenGraph<N, E>
where
    N: Eq + Hash + Clone,
{
    type EdgeType = Directed;
}

impl<N, E> petgraph::visit::Visitable for GenGraph<N, E>
where
    N: Eq + Hash + Clone,
{
    type Map = std::collections::HashSet<Self::NodeId>;

    fn visit_map(&self) -> Self::Map {
        HashSet::with_capacity(self.node_count())
    }

    fn reset_map(&self, map: &mut Self::Map) {
        map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::algo::has_path_connecting;
    use std::collections::VecDeque;

    #[test]
    fn test_basic_operations() {
        let mut graph = GenGraph::new();

        // Add nodes
        graph.add_node("A");
        graph.add_node("B");
        graph.add_node("C");

        assert_eq!(graph.node_count(), 3);
        assert!(graph.contains_node(&"A"));

        // Add edges
        let e1 = graph.add_edge("A", "B", 1);
        let e2 = graph.add_edge("B", "C", 2);

        assert_eq!(graph.edge_count(), 2);
        assert_eq!(graph.edge_weight(e1), Some(&1));
        assert_eq!(graph.edge_weight(e2), Some(&2));
    }

    #[test]
    fn test_parallel_edges() {
        let mut graph = GenGraph::new();

        // Add multiple edges between same nodes
        let e1 = graph.add_edge("A", "B", 1);
        let e2 = graph.add_edge("A", "B", 2);
        let e3 = graph.add_edge("A", "B", 3);

        assert_eq!(graph.edge_count(), 3);
        assert_eq!(graph.edge_weight(e1), Some(&1));
        assert_eq!(graph.edge_weight(e2), Some(&2));
        assert_eq!(graph.edge_weight(e3), Some(&3));
    }

    #[test]
    fn test_remove_operations() {
        let mut graph = GenGraph::new();

        graph.add_node("A");
        graph.add_node("B");
        let e1 = graph.add_edge("A", "B", 1);

        assert!(graph.remove_node(&"A"));
        assert_eq!(graph.node_count(), 1);
        assert_eq!(graph.edge_count(), 0);

        // Test edge removal
        let e2 = graph.add_edge("B", "C", 2);
        assert_eq!(graph.remove_edge(e2), Some(2));
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_neighbors() {
        let mut graph = GenGraph::new();

        graph.add_edge("A", "B", 1);
        graph.add_edge("A", "C", 2);
        graph.add_edge("B", "C", 3);

        let neighbors: HashSet<_> = graph.neighbors(&"A").cloned().collect();
        assert_eq!(neighbors.len(), 2);
        assert!(neighbors.contains("B"));
        assert!(neighbors.contains("C"));
    }

    #[test]
    fn test_path_finding() {
        let mut graph = GenGraph::new();

        // Create a simple path: A -1-> B -2-> C -3-> D
        graph.add_edge("A", "B", 1);
        graph.add_edge("B", "C", 2);
        graph.add_edge("C", "D", 3);

        // Add an alternative path: A -10-> D
        graph.add_edge("A", "D", 10);

        // Verify we can reach all nodes from A
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back("A");

        while let Some(node) = queue.pop_front() {
            if !visited.contains(node) {
                visited.insert(node);
                for neighbor in graph.neighbors(&node) {
                    queue.push_back(neighbor);
                }
            }
        }

        assert_eq!(visited.len(), 4); // Should visit all nodes
        assert!(visited.contains("A"));
        assert!(visited.contains("B"));
        assert!(visited.contains("C"));
        assert!(visited.contains("D"));
    }

    #[test]
    fn test_directed_graph_traits() {
        let mut graph = GenGraph::new();

        graph.add_edge("A", "B", 1);
        graph.add_edge("B", "C", 2);

        // Test Direction trait
        let b_successors: Vec<_> = graph
            .neighbors_directed(&"B", Direction::Outgoing)
            .collect();
        assert_eq!(b_successors.len(), 1);
        assert_eq!(b_successors[0], &"C");

        let b_predecessors: Vec<_> = graph
            .neighbors_directed(&"B", Direction::Incoming)
            .collect();
        assert_eq!(b_predecessors.len(), 1);
        assert_eq!(b_predecessors[0], &"A");
    }

    #[test]
    fn test_path_algorithms() {
        let mut graph = GenGraph::new();

        graph.add_edge("A", "B", 1);
        graph.add_edge("B", "C", 2);
        graph.add_edge("A", "C", 5);

        let a_idx = graph.nodes[&"A"];
        let c_idx = graph.nodes[&"C"];

        // Test path existence in directed graph
        assert!(has_path_connecting(&graph, a_idx, c_idx, None));
        // Verify no path exists in reverse direction
        assert!(!has_path_connecting(&graph, c_idx, a_idx, None));
    }

    #[test]
    fn test_parallel_edges_directed() {
        let mut graph = GenGraph::new();

        // Add multiple directed edges between same nodes
        let e1 = graph.add_edge("A", "B", 1);
        let e2 = graph.add_edge("A", "B", 2);
        let e3 = graph.add_edge("B", "A", 3); // Reverse direction

        let a_successors: HashSet<_> = graph.successors(&"A").collect();
        assert_eq!(a_successors.len(), 1); // Only B

        let b_successors: HashSet<_> = graph.successors(&"B").collect();
        assert_eq!(b_successors.len(), 1); // Only A

        // Verify we have 3 distinct edges
        assert_eq!(graph.edge_count(), 3);
        assert_eq!(graph.edge_weight(e1), Some(&1));
        assert_eq!(graph.edge_weight(e2), Some(&2));
        assert_eq!(graph.edge_weight(e3), Some(&3));
    }
}
