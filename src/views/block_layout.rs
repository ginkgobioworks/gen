use crate::models::node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID};
use crate::graph::{GraphEdge, GraphNode};
use crate::views::block_group_viewer::PlotParameters;
use log::{info, warn};
use petgraph::graph::{Graph, NodeIndex};
use petgraph::graphmap::DiGraphMap;
use petgraph::graphmap::GraphMap;
use petgraph::stable_graph::StableGraph;
use petgraph::Directed;
use petgraph::Direction;
use petgraph::visit::Bfs;
use petgraph::stable_graph::StableDiGraph;
use rust_sugiyama::{configure::Config, from_graph};
use std::collections::HashMap;
use std::hash::DefaultHasher;

/// A graph that is compatible with the sugiyama crate.
pub type SugiyamaGraph = StableDiGraph<GraphNode, GraphEdge, u32>;

/// Raw layout data in the format returned by the rust_sugiyama crate
pub type RawLayout = Vec<(NodeIndex, (f64, f64))>;

/// Find the articulation points of a directed graph using a non-recursive approach
/// This is a modified version of the algorithm found here:
/// https://en.wikipedia.org/wiki/Biconnected_component#Articulation_points
fn find_articulation_points(graph: &DiGraphMap<GraphNode, GraphEdge>) -> Vec<GraphNode> {
    let mut articulation_points: Vec<GraphNode> = Vec::new();
    let mut discovery_time: HashMap<GraphNode, usize> = HashMap::new();
    let mut low: HashMap<GraphNode, usize> = HashMap::new();
    let mut parent: HashMap<GraphNode, Option<GraphNode>> = HashMap::new();
    let mut time = 0;

    for node in graph.nodes() {
        if !discovery_time.contains_key(&node) {
            let mut stack = vec![(node, None, true)];
            while let Some((u, p, is_first_time)) = stack.pop() {
                if is_first_time {
                    // Initialize discovery time and low value
                    discovery_time.insert(u, time);
                    low.insert(u, time);
                    time += 1;
                    parent.insert(u, p);

                    // Push the node back with is_first_time = false to process after its neighbors
                    stack.push((u, p, false));

                    // Consider both incoming and outgoing edges as undirected
                    let neighbors: Vec<_> = graph
                        .neighbors_directed(u, Direction::Outgoing)
                        .chain(graph.neighbors_directed(u, Direction::Incoming))
                        .collect();

                    for v in neighbors {
                        if !discovery_time.contains_key(&v) {
                            stack.push((v, Some(u), true));
                        } else if Some(v) != p {
                            // Update low[u] if v is not parent
                            let current_low = low.get(&u).cloned().unwrap_or(usize::MAX);
                            let v_disc = discovery_time.get(&v).cloned().unwrap_or(usize::MAX);
                            low.insert(u, current_low.min(v_disc));
                        }
                    }
                } else {
                    // Post-processing after visiting all neighbors
                    let mut is_articulation = false;
                    let mut child_count = 0;

                    let neighbors: Vec<_> = graph
                        .neighbors_directed(u, Direction::Outgoing)
                        .chain(graph.neighbors_directed(u, Direction::Incoming))
                        .collect();

                    for v in neighbors {
                        if parent.get(&v).cloned() == Some(Some(u)) {
                            child_count += 1;
                            let v_low = low.get(&v).cloned().unwrap_or(usize::MAX);
                            let u_disc = discovery_time.get(&u).cloned().unwrap_or(usize::MAX);
                            if v_low >= u_disc {
                                is_articulation = true;
                            }
                            let current_low = low.get(&u).cloned().unwrap_or(usize::MAX);
                            let v_low = low.get(&v).cloned().unwrap_or(usize::MAX);
                            low.insert(u, current_low.min(v_low));
                        } else if Some(v) != parent.get(&u).cloned().unwrap_or(None) {
                            let v_disc = discovery_time.get(&v).cloned().unwrap_or(usize::MAX);
                            let current_low = low.get(&u).cloned().unwrap_or(usize::MAX);
                            low.insert(u, current_low.min(v_disc));
                        }
                    }

                    let u_parent = parent.get(&u).cloned().unwrap_or(None);
                    if (u_parent.is_some() && is_articulation)
                        || (u_parent.is_none() && child_count > 1)
                    {
                        articulation_points.push(u);
                    }
                }
            }
        }
    }

    articulation_points.sort();
    articulation_points.dedup();
    articulation_points
}

/// The result of partitioning a graph into mutually exclusive subgraphs, intended for use in layout algorithms.
/// - `parts` is vector of StableDiGraphs that make up the partition.
///     - Node weights are GraphNode, edge weights are GraphEdge.
///     - Indices are u32 for compatibility with the sugiyama crate.
///     - The edges are defined by the index of the nodes within the partition.
/// - `inter_part_edges` is a hashmap of edges that cross the partition boundaries
///     - Keyed to a tuple of the source and target subgraph indices.
///     - Edges are defined by the NodeIndex of the nodes within their respective subgraphs.
#[derive(Debug)]
pub struct Partition {
    pub parts: Vec<SugiyamaGraph>,
    pub inter_part_edges: HashMap<(usize, usize), Vec<(GraphNode, GraphNode, GraphEdge)>>,
}

/// Partition a DiGraphMap<GraphNode, GraphEdge> into subgraphs, preferably at articulation points
/// - Subgraph sizes are controlled by min_size and max_size
/// - Algorithm:
///     - Perform a breadth-first search of the graph, starting at the root node and accumulating nodes in a subgraph.
///     - Keep track of the number of nodes seen so far.
///     - If an articulation point is encountered, and the minimum part size has been reached,
///        - add the current subgraph to the list of parts
///        - start a new subgraph
///     - If the maximum part size is reached, forcibly close out the current subgraph.
impl Partition {
    pub fn new(graph: &DiGraphMap<GraphNode, GraphEdge>, min_size: usize, max_size: usize) -> Self {
        let mut parts: Vec<SugiyamaGraph> = Vec::new();
        let mut current_part: SugiyamaGraph = SugiyamaGraph::new();
        let mut current_part_index = 0;

        // Mapping from GraphNode to (part index, node index)
        let mut node_to_nx: HashMap<GraphNode, (usize, NodeIndex<u32>)> = HashMap::new();

        // Find the nodes
        let mut root_nodes: Vec<GraphNode> = Vec::new();
        for node in graph.nodes() {
            if graph.edges_directed(node, Direction::Incoming).count() == 0 {
                root_nodes.push(node);
            }
        }
        // Only handle the case where there is exactly one root node for now
        // TODO: handle multiple root nodes (if it's a disconnected graph)
        if root_nodes.len() > 1 {
            panic!("Found {} root nodes: {:#?}", root_nodes.len(), root_nodes);
        }
        let root_node = root_nodes[0];

        // Find articulation points
        let articulation_points = find_articulation_points(graph);

        // Perform a breadth-first search of the graph, starting at the root node.
        let mut bfs = Bfs::new(&graph, root_node);

        while let Some(node) = bfs.next(&graph) {
            // Once we have enough nodes, try to close out at an articulation point.
            // Forcibly close out if we don't find an articulation point in time.
            if (current_part.node_count() >= min_size && articulation_points.contains(&node))
                || (current_part.node_count() >= max_size)
            {
                parts.push(current_part);
                current_part = StableDiGraph::new();
                current_part_index += 1;
            }

            // Add the node to the current subgraph
            let node_index: NodeIndex<u32> = current_part.add_node(node);
            node_to_nx.insert(node, (current_part_index, node_index));
        }

        // Add the last subgraph to the list of parts
        if current_part.node_count() > 0 {
            parts.push(current_part);
        }

        // Now that every node ended up in a StableDiGraph, we can add edges by looking up the part index and node index for each node.
        // We keep track of edges that cross part boundaries in a separate hashmap keyed to a tuple of the source and target GraphNodes.
        #[allow(clippy::type_complexity)]
        let mut inter_part_edges: HashMap<
            (usize, usize),
            Vec<(GraphNode, GraphNode, GraphEdge)>,
        > = HashMap::new();

        for (source, target, edge) in graph.all_edges() {
            let (source_part_index, source_node_index) = node_to_nx.get(&source).unwrap();
            let (target_part_index, target_node_index) = node_to_nx.get(&target).unwrap();
            if source_part_index == target_part_index {
                parts[*source_part_index].add_edge(*source_node_index, *target_node_index, *edge);
            } else {
                inter_part_edges
                    .entry((*source_part_index, *target_part_index))
                    .or_default()
                    .push((source, target, *edge));
            }
        }

        Self {
            parts,
            inter_part_edges,
        }
    }
}

/// Partitions a graph into subgraphs, computes layouts for each subgraph, and grows the layout as needed.
/// The sugiyama crate uses recursion internally, and is susceptible to stack overflows if we don't constrain the size.
/// Hence we render the layout as subgraphs from a partition, based on which area the user is looking at.
/// To account for nodes close to the boundaries, we compute layouts for (at least) 3 partition subgraphs:
/// - The one currently scrolled into view, the one after, and the one before.
/// - The base_layout holds the boundaries as left_idx and right_idx.
/// - The functions expand_left and expand_right make the graph grow.
/// 
/// Public fields:
/// - `subgraph` = a subgraph in DiGraphMap format that grows as we traverse the partition
/// - `node_positions` = growing map of nodes to their computed positions HashMap<GraphNode, (f64, f64)>
/// - `size` = layout size in number of nodes (number of layers, maximum number of nodes in any layer)
/// - `partition` = the partition we are working with (graph terminology: set of non-overlapping subgraphs)
/// - `left_idx` = the partition index of the leftmost subgraph in the current layout
/// - `right_idx` = the partition index of the rightmost subgraph in the current layout
///
/// Private fields:
/// - `_vertex_size` = closure that specifies the size of each node for the layout algorithm
/// - `_sugiyama_config` = configuration for the layout algorithm
/// - `_partial_layouts` = hashmap of partition index to individiual subgraph layouts
#[derive(Debug)]
pub struct BaseLayout {
    pub subgraph: DiGraphMap<GraphNode, GraphEdge>,
    pub node_positions: HashMap<GraphNode, (f64, f64)>,
    pub size: (f64, f64),
    pub partition: Partition,
    pub left_idx: usize,
    pub right_idx: usize,
    _vertex_size: fn(_id: NodeIndex<u32>, _v: &GraphNode) -> (f64, f64),
    _sugiyama_config: rust_sugiyama::configure::Config,
    _partial_layouts: HashMap<usize, (Vec<(GraphNode, (f64, f64))>, f64, f64)>, // partition index -> (layout, width, height)
}

impl BaseLayout {
    /// Create a new BaseLayout with the default origin, and default chunk sizes.
    /// - `block_graph`: the graph to layout
    /// - Returns a new BaseLayout
    pub fn new(block_graph: &DiGraphMap<GraphNode, GraphEdge>) -> Self {
        Self::with_origin(block_graph, (Node::get_start_node(), 0))
    }

    /// Create a new BaseLayout with a specified origin (Node object, sequence position).
    /// - `block_graph`: the graph to layout
    /// - `origin`: the origin as (node, sequence position)
    /// - Returns a new BaseLayout
    pub fn with_origin(block_graph: &DiGraphMap<GraphNode, GraphEdge>, 
            origin: (Node, i64)) -> Self {

        const MIN_CHUNK_SIZE: usize = 1000;
        const MAX_CHUNK_SIZE: usize = 10000;

        Self::with_origin_and_chunksize(block_graph, origin, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE)
    }

    /// Create a new BaseLayout with a specified chunk size.
    /// - `block_graph`: the graph to layout
    /// - `min_chunk_size`: the minimum size of a partition subgraph
    /// - `max_chunk_size`: the maximum size of a partition subgraph
    /// - Returns a new BaseLayout
    pub fn with_chunksize(block_graph: &DiGraphMap<GraphNode, GraphEdge>,
            min_chunk_size: usize,
            max_chunk_size: usize) -> Self {
        Self::with_origin_and_chunksize(block_graph, (Node::get_start_node(), 0), 
            min_chunk_size, max_chunk_size)
    }

    /// Create a new BaseLayout with a specified origin block and chunk size.
    /// - `block_graph`: the graph to layout
    /// - `origin`: the origin block coordinates (node, sequence position)
    /// - `min_chunk_size`: the minimum size of a partition subgraph
    /// - `max_chunk_size`: the maximum size of a partition subgraph
    /// - Returns a new BaseLayout
    pub fn with_origin_and_chunksize(block_graph: &DiGraphMap<GraphNode, GraphEdge>, 
            origin: (Node, i64),
            min_chunk_size: usize,
            max_chunk_size: usize) -> Self {

        // Partition the graph at articulation points
        let partition = Partition::new(block_graph, min_chunk_size, max_chunk_size);
        let max_part_idx = partition.parts.len() - 1; // Recording here because we'll lose ownership of partition

        // Find where in the partition the origin node is
        let origin_idx = partition
            .parts
            .iter()
            .position(|part| part.node_indices().any(|idx| 
                    part.node_weight(idx)
                        .map(|gn| gn.node_id == origin.0.id
                            && gn.sequence_start <= origin.1 as i64
                            && gn.sequence_end >= origin.1 as i64)
                        .unwrap_or(false)
                ))
            .unwrap();

        // Make a GraphMap based on the first partition subgraph (StableGraph) we're asked to process
        // - we can't convert a StableGraph to a DiGraphMap directly, so we convert to a Graph first
        // - this will grow (and shrink) as we traverse the partition
        let mut subgraph = GraphMap::from_graph(partition.parts[origin_idx].clone().into());

        // Set up the config for the layout algorithm
        // We set the vertex size to 1.0, 1.0 so that the layout algorithm does not take individual node size into account
        // since we do our own stretching/scaling later.
        let _vertex_size = |_id: NodeIndex<u32>, _v: &GraphNode| (1.0, 1.0);
        let _sugiyama_config = Config {
            vertex_spacing: 1.0,
            ..Default::default()
        };

        let mut base_layout = Self {
            subgraph,
            node_positions: HashMap::new(),
            size: (0.0, 0.0),
            partition,
            left_idx: origin_idx,
            right_idx: origin_idx,
            _vertex_size,
            _sugiyama_config,
            _partial_layouts: HashMap::new(),
        };

        // Compute the local layout
        // - In new(), you want it to block, but for later updates we should look into async
        // - Places the resulting layout in the _partial_layouts hashmap (for caching and future async updates)
        base_layout.run_sugiyama(origin_idx);

        // Make sure it succeeded
        assert!(
            base_layout._partial_layouts.contains_key(&origin_idx),
            "Failed to compute layout for partition index {}",
            origin_idx
        );

        // Because this is the first run, we can just move it over to the main layout, without modifications.
        let (sub_layout, width, height) =
            base_layout._partial_layouts.get(&origin_idx).unwrap();
        for (node, pos) in sub_layout {
            base_layout.node_positions.insert(*node, *pos);
        }
        // Update the size of the layout
        base_layout.size = (*width, *height);

        // Expand one unit left and right if within bounds
        if origin_idx > 0 {
            base_layout.expand_left();
        }
        if origin_idx < max_part_idx {
            base_layout.expand_right();
        }

        base_layout
    }

    /// Run the layout algorithm on a partition subgraph.
    /// Writes to the _sublayouts hashmap with future async rendering in mind.
    /// - `partition_idx`: which subgraph we want to compute a layout for
    /// - this is a separate function that writes to a `_partial_layouts` hashmap,
    ///   with future async updates in mind
    fn run_sugiyama(&mut self, partition_index: usize) {
        // We partitioned at articulation points so that it's easier to stitch layouts
        // together. However, partitioning does not duplicate the articulation nodes,
        // they end up on the right side of the boundary. We add dummy nodes to solve this.

        assert!(
            partition_index < self.partition.parts.len(),
            "Invalid partition index"
        );

        // Make a mutable clone of the subgraph we want to layout
        let mut subgraph = self.partition.parts[partition_index].clone();

        // Check if we need to add dummy nodes to tie into the next partition
        if partition_index < self.partition.parts.len() - 1 {
            // Adds edges and nodes for each partition edge that departs here
            for (source, target, edge) in self
                .partition
                .inter_part_edges
                .get(&(partition_index, partition_index + 1))
                .unwrap()
            {
                // We have to use the stablegraph format, which means we need to
                // add the edges by node index, not by node value.
                let source_idx = subgraph
                    .node_indices()
                    .find(|&idx| subgraph.node_weight(idx).unwrap() == source)
                    .unwrap();
                let target_idx = subgraph.add_node(target.clone());
                subgraph.add_edge(source_idx, target_idx, *edge);
            }
        }

        // Run the layout algorithm
        let layouts =
            rust_sugiyama::from_graph(&subgraph, &self._vertex_size, &self._sugiyama_config);

        // Confirm that there is only one layout, which means that the graph is connected
        assert!(
            layouts.len() <= 1,
            "Disconnected graphs are not supported in the viewer currently."
        );
        assert_eq!(
            layouts.len(),
            1,
            "Could not compute layout for the selected partition."
        );

        let (idx_positions, width, height) = &layouts[0];

        // Transpose x and y (converts top-to-bottom to left-to-right) and remap to GraphNodes
        let node_positions: Vec<(GraphNode, (f64, f64))> = idx_positions
            .iter()
            .map(|(idx, (x, y))| (*subgraph.node_weight(*idx).unwrap(), (*y, *x)))
            .collect();

        // Store the layout along with its width and height (format = (node_idx, (x, y)), width, height)
        self._partial_layouts
            .insert(partition_index, (node_positions.clone(), *width, *height));
    }

    /// Expand the layout to the right by adding a new partition subgraph
    pub fn expand_right(&mut self) {
        self.expand(true);
    }

    /// Expand the layout to the left by adding a new partition subgraph
    pub fn expand_left(&mut self) {
        self.expand(false);
    }

    /// Generalized expand function that can expand to the left or right
    fn expand(&mut self, rightwards: bool) {
        // Get the next partition index
        let next_idx = if rightwards && self.right_idx < (self.partition.parts.len() - 1) {
            self.right_idx + 1
        } else if !rightwards && self.left_idx > 0 {
            self.left_idx - 1
        } else {
            return;
        };

        // Compute the layout for the new partition subgraph (with dummy nodes)
        // - this will update the _partial_layouts hashmap
        // - currently this blocks the main thread, we should make this async
        self.run_sugiyama(next_idx);

        assert!(
            self._partial_layouts.contains_key(&next_idx),
            "Failed to compute layout for partition index {}",
            next_idx
        );

        // Add the new layout to the main layout
        let (sub_layout, width, height) = self._partial_layouts.get(&next_idx).unwrap();

            let dummy_nodes = if rightwards {
            // Find the dummy nodes of the partition subgraph to the left
            self.partition
                .inter_part_edges
                .get(&(self.right_idx, next_idx))
                .unwrap()
                .iter()
                .map(|(_, target, _)| *target)
                .collect::<Vec<_>>()
        } else {
            // Find the dummy nodes of the partition subgraph to the right
            self.partition
                .inter_part_edges
                .get(&(next_idx, self.left_idx))
                .unwrap()
                .iter()
                .map(|(_, target, _)| *target)
                .collect::<Vec<_>>()
        };

        // Ideally we have just one, the articulation point, but we'll have more if we had to split early
        if dummy_nodes.len() > 1 {
            warn!(
                "Partition index {} and {} could not be joined together cleanly.",
                if rightwards { self.right_idx } else { next_idx },
                if rightwards { next_idx } else { self.left_idx }
            );
        }

        // The first dummy node is used as the point of reference around which we'll move the new layout
        // We can get it from the already computed layout since it's a hashmap keyed to GraphNode,
        // and we cloned GraphNodes to make the dummy nodes
        let new_origin = self.node_positions.get(&dummy_nodes[0]).unwrap().clone();

        // Shift the new layout so that its origin is the same as the reference node,
        // and add it to the main layout.
        if rightwards {
            for (node, pos) in sub_layout {
                let new_pos = (pos.0 + new_origin.0, pos.1 + new_origin.1);
                self.node_positions.insert(*node, new_pos);
            }
        } else {
            for (node, pos) in sub_layout {
                let new_pos = (pos.0 - new_origin.0, pos.1 - new_origin.1);
                self.node_positions.insert(*node, new_pos);
            }
        }

        // Add all the edges from the new partition subgraph to the main subgraph
        // - the relevant cross-partition edges are already there
        let new_subgraph: GraphMap<GraphNode, GraphEdge, Directed> =
            GraphMap::from_graph(self.partition.parts[next_idx].clone().into());

        for (source, target, edge) in new_subgraph.all_edges() {
            self.subgraph.add_edge(source, target, *edge);
        }

        // Update the size of the main layout (note that these are the number of nodes, not the width and height in cell units)
        self.size.0 += *width - 1.0; // -1.0 because we added a dummy node to stitch the layouts together
        self.size.1 = f64::max(self.size.1, *height);

        // Update the partition indices
        if rightwards {
            self.right_idx = next_idx;
        } else {
            self.left_idx = next_idx;
        }
    }
}

/// Holds processed and scaled layout data, but not the actual sequences.
/// - `lines` = pairs of coordinates for each edge.
/// - `labels` = starting and ending coordinates for each label.
/// - `highlight_[a|b]` = block ID or (block ID, coordinate) to highlight in color A or B.
///
/// The raw layout from the Sugiyama algorithm is processed as follow:
/// - The coordinates are rounded to the nearest integer and transposed to go from top-to-bottom to left-to-right.
/// - Each block is assigned a layer (or rank) based on its y-coordinate.
/// - The width of each layer is determined by the widest label in that layer.
/// - The distance between layers is scaled horizontally and vertically
#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub struct ScaledLayout{
    pub lines: HashMap<GraphEdge, ((f64, f64), (f64, f64))>, // Edge -> (start_coord, end_coord)
    pub labels: HashMap<GraphNode, ((f64, f64), (f64, f64))>, // Node -> (start_coord, end_coord)
    pub highlight_a: Option<(GraphNode, Option<(GraphNode, u32)>)>, // Block or (Block, position) to highlight in color A
    pub highlight_b: Option<(GraphNode, Option<(GraphNode, u32)>)>, // Block or (Block, position) to highlight in color B

}

impl ScaledLayout {
    pub fn from_base_layout(base_layout: &BaseLayout, parameters: &PlotParameters) -> Self {
        let mut layout = ScaledLayout {
            lines: HashMap::new(),
            labels: HashMap::new(),
            highlight_a: None,
            highlight_b: None,
        };
        layout.refresh(&base_layout, &parameters);
        layout
    }

    pub fn refresh(&mut self, base_layout: &BaseLayout, parameters: &PlotParameters) {
        // Scale the overall layout and round to integer coordinates
        let scale_x = parameters.scale as f64;
        let scale_y = parameters.scale as f64 * parameters.aspect_ratio as f64;

        let mut working_layout: Vec<(GraphNode, (i64, i64))> = base_layout
            .node_positions
            .iter()
            .map(|(node, (x, y))| {
                (
                    *node,
                    ((x * scale_x).round() as i64, (y * scale_y).round() as i64),
                )
            })
            .collect();
            
        // We don't need to stretch the layout if the requested label width is too small
        if parameters.label_width < 5 {
            warn!("Requested label width is too small to shrink the labels, falling back to view without sequences.");

            let working_layout_hashmap: HashMap<GraphNode, (f64, f64)> = working_layout
                .iter()
                .map(|(node, (x, y))| (*node, (*x as f64, *y as f64)))
                .collect();

            self.lines = base_layout
                .subgraph
                .all_edges()
                .filter(|(source, target, _)| source.node_id != PATH_START_NODE_ID && target.node_id != PATH_END_NODE_ID)
                .map(|(source, target, edge)| {
                    let source_coord = working_layout_hashmap
                        .get(&source)
                        .map(|&(x, y)| (x + 0.5, y + 0.25))
                        .unwrap();
                    let target_coord = working_layout_hashmap
                        .get(&target)
                        .map(|&(x, y)| (x - 1.0, y + 0.25))
                        .unwrap();
                    (*edge, (source_coord, target_coord))
                })
                .collect();

            self.labels = working_layout_hashmap
                .iter()
                .map(|(node, (x, y))| (*node, ((*x, *y), (*x, *y))))
                .collect();
            return;
        }

        // To stretch, we first sort the layout by x-coordinate so that we can group the blocks by rank
        working_layout.sort_by(|a, b| a.1 .0.cmp(&b.1 .0));

        // Loop over the sorted layout and group the blocks by rank by keeping track of the x-coordinate
        let mut current_x = working_layout[0].1 .0;
        let mut current_layer: Vec<(GraphNode, i64, i64)> = Vec::new(); // node, label_width, y-coordinate

        // Initial values:
        let mut layer_width = 1;
        let mut cumulative_offset = 0;

        for (node, (x, y)) in working_layout.iter() {
            let label_width = std::cmp::min(
                node.sequence_end - node.sequence_start,
                parameters.label_width as i64,
            );

            if *x == current_x {
                // This means we are still in the same layer
                // Keep a tally of the maximum label width
                layer_width = std::cmp::max(layer_width, label_width);

                // Add the block to the current layer vector
                current_layer.push((*node, label_width, *y));
            } else {
                // We switched to a new layer
                // Loop over the current layer (now previous) and:
                // - increment the x-coordinate by the cumulative offset so far
                // - horizontally center the block in its layer
                for (node, label_width, y) in current_layer {
                    let centering_offset =
                        ((layer_width - label_width) as f64 / 2.0).round() as i64;
                    let x = current_x + centering_offset + cumulative_offset;
                    self.labels.insert(
                        node,
                        ((x as f64, y as f64), ((x + label_width) as f64, y as f64)),
                    );
                }
                // Increment the cumulative offset for the next layer by the width of the current layer
                cumulative_offset += layer_width;

                // Reset the layer width and the current layer
                layer_width = label_width;
                current_layer = vec![(*node, label_width, *y)];
                current_x = *x;
            }
        }
        // Loop over the last layer (wasn't processed yet)
        for (node, label_width, y) in current_layer {
            let centering_offset = ((layer_width - label_width) as f64 / 2.0).round() as i64;
            let x = current_x + centering_offset + cumulative_offset;
            self.labels.insert(
                node,
                ((x as f64, y as f64), ((x + label_width) as f64, y as f64)),
            );
        }

        // Recalculate all the edges so they meet labels on the sides instead of the center
        self.lines = base_layout
            .subgraph
            .all_edges()
            .filter(|(source, target, _)| source.node_id != PATH_START_NODE_ID && target.node_id != PATH_END_NODE_ID)
            .map(|(source, target, edge)| {
                let source_coord = self
                    .labels
                    .get(&source)
                    .map(|(_, (x2, y2))| (*x2, *y2 + 0.5))
                    .unwrap();
                let target_coord = self
                    .labels
                    .get(&target)
                    .map(|((x1, y1), _)| (*x1 - 1.5, *y1 + 0.5))
                    .unwrap();
                (*edge, (source_coord, target_coord))
            })
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{GraphEdge, GraphNode};
    use crate::models::strand::Strand;
    use itertools::Itertools;
    use petgraph::graphmap::DiGraphMap;

    fn make_test_graph(
        edges: Vec<(i32, i32)>,
        nodes: Option<Vec<GraphNode>>
    ) -> DiGraphMap<GraphNode, GraphEdge> {
        let mut graph: DiGraphMap<GraphNode, GraphEdge> = DiGraphMap::new();

        // Create default nodes if none provided
        let nodes = nodes.unwrap_or_else(|| {
            edges
                .iter()
                .flat_map(|(s, t)| vec![*s, *t])
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .map(|id| GraphNode {
                    block_id: id as i64,
                    node_id: id as i64,
                    sequence_start: 0,
                    sequence_end: 10,
                })
                .collect()
        });

        DiGraphMap::from_edges(edges.iter().map(|(s, t)| {
            (
                nodes.iter().find(|gn| gn.block_id == *s as i64).unwrap().clone(),
                nodes.iter().find(|gn| gn.block_id == *t as i64).unwrap().clone(),
                GraphEdge {
                    edge_id: 0,
                    source_strand: Strand::Forward,
                    target_strand: Strand::Forward,
                    chromosome_index: 0,
                    phased: 0,
                },
            )
        }))
    }

    #[test]
    fn test_make_test_graph() {
        let edges = vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)];
        let graph = make_test_graph(edges, None);
        println!("Graph: {:#?}", graph);
        assert_eq!(graph.node_count(), 6);
        assert_eq!(graph.edge_count(), 5);
    }

    #[test]
    fn test_find_articulation_points_simple() {
        // 1 -> 2 -> 3
        //      |
        //      v
        //      4 -> 5
        let edges = vec![(1, 2), (2, 3), (2, 4), (4, 5)];
        let graph = make_test_graph(edges, None);
        let ap = find_articulation_points(&graph)
            .iter()
            .map(|node| node.block_id)
            .collect::<Vec<_>>();
        let expected = vec![2, 4];
        assert_eq!(ap, expected);
    }

    #[test]
    fn test_find_articulation_points_multiple_ap() {
        // 1 -> 2 -> 3
        //      |    |
        //      v    v
        //      4 -> 5 -> 6
        let edges = vec![(1, 2), (2, 3), (2, 4), (3, 5), (4, 5), (5, 6)];
        let graph = make_test_graph(edges, None);
        let ap = find_articulation_points(&graph)
            .iter()
            .map(|node| node.block_id)
            .collect::<Vec<_>>();
        let expected = vec![2, 5];
        assert_eq!(ap, expected);
    }

    #[test]
    fn test_find_articulation_points_single_node() {
        let graph = make_test_graph(vec![(1, 1)], None);

        let ap = find_articulation_points(&graph);
        let expected = vec![];
        assert_eq!(ap, expected);
    }

    #[test]
    fn test_find_articulation_points_disconnected_graph() {
        // 1 -> 2
        // 3 -> 4
        let graph = make_test_graph(vec![(1, 2), (3, 4)], None);

        let ap = find_articulation_points(&graph);
        let expected = vec![];
        assert_eq!(ap, expected);
    }

    #[test]
    fn test_partition_new() {
        // 1 -> 2 -> 3
        //      |    |
        //      v    v
        //      4 -> 5 -> 6
        let edges = vec![(1, 2), (2, 3), (2, 4), (3, 5), (4, 5), (5, 6)];
        let graph = make_test_graph(edges, None);
        let partition = Partition::new(&graph, 3, 10);
        assert_eq!(partition.parts.len(), 2);
        assert_eq!(partition.inter_part_edges.len(), 1);
    }

    #[test]
    fn test_partition_new_small() {
        // 1 -> 2 -> 3
        //      |    |
        //      v    v
        //      4 -> 5 -> 6
        let edges = vec![(1, 2), (2, 3), (2, 4), (3, 5), (4, 5), (5, 6)];
        let graph = make_test_graph(edges, None);
        let partition = Partition::new(&graph, 1, 10);
        assert_eq!(partition.parts.len(), 3);
        assert_eq!(partition.inter_part_edges.len(), 2);
    }

    #[test]
    fn test_partition_new_force_close() {
        // 1 -> 2 -> 3
        //      |    |
        //      v    v
        //      4 -> 5 -> 6
        let edges = vec![(1, 2), (2, 3), (2, 4), (3, 5), (4, 5), (5, 6)];
        let graph = make_test_graph(edges, None);
        let partition = Partition::new(&graph, 1, 2);
        assert_eq!(partition.parts.len(), 4);
        assert_eq!(partition.inter_part_edges.len(), 4);
    }

    #[test]
    fn test_base_layout_new() {
        // 0 -> 1 -> 2
        //      |    |
        //      v    v
        //      3 -> 4 -> 5
        let edges = vec![(0, 1), (1, 2), (2, 4), (1, 3), (3, 4), (4, 5)];
        let graph = make_test_graph(edges, None);
        let base_layout = BaseLayout::new(&graph);
        assert_eq!(base_layout.node_positions.len(), 6);
        assert_eq!(base_layout.size, (2.0, 5.0));
        println!("base_layout: {:#?}", base_layout);
  
        
    }

    fn test_base_layout_with_chunksize() {
        // 0 -> 1 -> 2
        //      |    |
        //      v    v
        //      3 -> 4 -> 5
        let edges = vec![(0, 1), (1, 2), (2, 4), (1, 3), (3, 4), (4, 5)];
        let graph = make_test_graph(edges, None);
        let base_layout = BaseLayout::with_chunksize(&graph, 2, 10);
        assert_eq!(base_layout.partition.parts.len(), 2);
        assert_eq!(base_layout.node_positions.len(), 6);
        assert_eq!(base_layout.size, (3.0, 4.0));

    }

    #[test]
    fn test_scaled_layout_new_no_zoom() {
        // Create a test subgraph manually
        let n0 = GraphNode {
            block_id: 0,
            node_id: 0,
            sequence_start: 0,
            sequence_end: 10,
        };
        let n1 = GraphNode {
            block_id: 1,
            node_id: 10,
            sequence_start: 0,
            sequence_end: 20,
        };
        let n2 = GraphNode {
            block_id: 2,
            node_id: 20,
            sequence_start: 0,
            sequence_end: 40,
        };

        let graph = make_test_graph(vec![(0, 1), (1, 2)], Some(vec![n0, n1, n2]));
        let base_layout = BaseLayout::new(&graph);
        let parameters = PlotParameters {
            label_width: 5,
            scale: 1,
            aspect_ratio: 1.0,
        };
        let scaled_layout = ScaledLayout::from_base_layout(&base_layout, &parameters);

        let sorted_layout = scaled_layout
            .labels
            .iter()
            .sorted_by_key(|(node, _)| node.block_id)
            .map(|(node, (start, end))| (node.block_id, (*start, *end)))
            .collect::<Vec<_>>();

        let expected_layout = vec![
            (0, ((0.0, 0.0), (5.0, 0.0))),
            (1, ((13.0, 0.0), (18.0, 0.0))),
            (2, ((26.0, 0.0), (31.0, 0.0))),
        ];
        assert_eq!(sorted_layout, expected_layout);
    }

    #[test]
    fn test_scaled_layout_new_unlabeled() {
        // 0 -> 1 -> 2
        //      |    |
        //      v    v
        //      3 -> 4 -> 5
        let edges = vec![(0, 1), (1, 2), (2, 4), (1, 3), (3, 4), (4, 5)];
        // Generate 6 nodes with sequence length of 10
        let nodes = (0..6)
            .map(|i| GraphNode {
                block_id: i,
                node_id: i,
                sequence_start: 0,
                sequence_end: 10,
            })
            .collect::<Vec<_>>();

        let graph = make_test_graph(edges, Some(nodes));
        let base_layout = BaseLayout::with_chunksize(&graph, 2, 10);
        let parameters = PlotParameters {
            label_width: 1,
            scale: 10,
            aspect_ratio: 1.0,
        };
        let scaled_layout = ScaledLayout::from_base_layout(&base_layout, &parameters);

        let sorted_layout = scaled_layout
            .labels
            .iter()
            .sorted_by_key(|(node, _)| node.block_id)
            .map(|(node, (start, end))| (node.block_id, (*start, *end)))
            .collect::<Vec<_>>();

        let expected_layout = vec![
            (0, ((0.0, 40.0), (0.0, 40.0))),
            (1, ((80.0, 40.0), (80.0, 40.0))),
            (2, ((160.0, 80.0), (160.0, 80.0))),
            (3, ((160.0, 0.0), (160.0, 0.0))),
            (4, ((240.0, 0.0), (240.0, 0.0))),
        ];
    }

    #[test]
    fn test_scaled_layout_new_truncations() {
        // 0 -> 1 -> 2
        //      |    |
        //      v    v
        //      3 -> 4 -> 5
        let edges = vec![(0, 1), (1, 2), (2, 4), (1, 3), (3, 4), (4, 5)];
        // Generate 6 nodes with sequence length of 10, 20, 30, ...
        let nodes = (0..6)
            .map(|i| GraphNode {
                block_id: i,
                node_id: i,
                sequence_start: 0,
                sequence_end: (i + 1) * 10,
            })
            .collect::<Vec<_>>();

        let graph = make_test_graph(edges, Some(nodes));
        let base_layout = BaseLayout::with_chunksize(&graph, 2, 10);
        let parameters = PlotParameters {
            label_width: 15,
            scale: 1,
            aspect_ratio: 1.0,
        };
        let scaled_layout = ScaledLayout::from_base_layout(&base_layout, &parameters);

        let sorted_layout = scaled_layout
            .labels
            .iter()
            .sorted_by_key(|(node, _)| node.block_id)
            .map(|(node, (start, end))| (node.block_id, (*start, *end)))
            .collect::<Vec<_>>();

        let expected_layout = vec![
            (0, ((0.0, 4.0), (10.0, 4.0))),
            (1, ((18.0, 4.0), (33.0, 4.0))),
            (2, ((41.0, 8.0), (56.0, 8.0))),
            (3, ((41.0, 0.0), (56.0, 0.0))),
            (4, ((64.0, 0.0), (79.0, 0.0))),
        ];
        assert_eq!(sorted_layout, expected_layout);
    }

    #[test]
    fn test_base_layout_expand_right() {
        // 0 -> 1 -> 2 -> 3 -> ... -> 100
        let edges = (0..100).map(|i| (i, i + 1)).collect::<Vec<_>>();
        let graph = make_test_graph(edges, None);

        // The comparison case: partition with only 1 large subgraph
        let base_layout_a = BaseLayout::with_chunksize(&graph, 1000, usize::MAX);
        assert_eq!(base_layout_a.partition.parts.len(), 1);

        // The test case: partition with 2 subgraphs
        let mut base_layout_b = BaseLayout::with_chunksize(&graph, 50, usize::MAX);
        assert_eq!(base_layout_b.partition.parts.len(), 2); 

        // Expand the layout to the right
        base_layout_b.expand_right();

        // Check that the layout is the same
        println!("base_layout_a.size: {:?}", base_layout_a.size);
        println!("base_layout_b.size: {:?}", base_layout_b.size);
        println!(
            "base_layout_a block 100: {:?}",
            base_layout_a
                .node_positions
                .iter()
                .find(|(node, _)| node.block_id == 100)
                .map(|(_, pos)| pos)
        );
        println!(
            "base_layout_b block 100: {:?}",
            base_layout_b
                .node_positions
                .iter()
                .find(|(node, _)| node.block_id == 100)
                .map(|(_, pos)| pos)
        );

        assert_eq!(base_layout_a.size, base_layout_b.size);

        let sorted_layout_a = base_layout_a
            .node_positions
            .iter()
            .sorted_by_key(|(node, _)| node.block_id)
            .map(|(node, (x, y))| (node.block_id, (*x, *y)))
            .collect::<Vec<_>>();
        let sorted_layout_b = base_layout_b
            .node_positions
            .iter()
            .sorted_by_key(|(node, _)| node.block_id)
            .map(|(node, (x, y))| (node.block_id, (*x, *y)))
            .collect::<Vec<_>>();
        assert_eq!(sorted_layout_a, sorted_layout_b);
    }
}
