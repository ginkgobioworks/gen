//! The implementation roughly follows sugiyamas algorithm for creating
//! a layered graph layout.
//!
//! Usually Sugiyamas algorithm consists of 4 Phases:
//! 1. Remove Cycles
//! 2. Assign each vertex to a rank/layer
//! 3. Reorder vertices in each rank to reduce crossings
//! 4. Calculate the final coordinates.
//!
//! Currently, phase 2 to 4 are implemented, Cycle removal might be added at
//! a later time.
//!
//! The whole algorithm roughly follows the 1993 paper "A technique for drawing
//! directed graphs" by Gansner et al. It can be found
//! [here](https://ieeexplore.ieee.org/document/221135).
//!
//! See the submodules for each phase for more details on the implementation
//! and references used.

// Suppress clippy::type_complexity warnings, as this is a graph layout algorithm
// that inherently deals with complex types and data structures.
#![allow(clippy::type_complexity)]

use std::collections::{BTreeMap, HashMap};

use log::{debug, info};
use petgraph::stable_graph::{EdgeIndex, NodeIndex, StableDiGraph};

use super::configure::{Config, CrossingMinimization, RankingType};
use super::util::weakly_connected_components;

pub mod p0_cycle_removal;
pub mod p1_layering;
pub mod p2_reduce_crossings;
pub mod p3_calculate_coordinates;

// Re-export the functions we need
pub use p0_cycle_removal::remove_cycles;
pub use p1_layering::rank;
pub use p2_reduce_crossings::{insert_dummy_vertices, ordering, remove_dummy_vertices};
pub use p3_calculate_coordinates::{
    align_to_smallest_width_layout, calculate_relative_coords, create_layouts, VDir,
};



#[derive(Debug)]
pub struct LayoutResult {
    /// Coordinates for each node (node ID and position)
    pub node_coordinates: Vec<(NodeIndex, (f64, f64))>,
    
    /// Coordinates for edges with dummy points (source-target nodes and path of positions)
    pub edge_coordinates: Vec<(NodeIndex, NodeIndex), Vec<(f64,f64)>>,
    
    /// Width of the layout
    pub width: f64,
    
    /// Height of the layout
    pub height: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Vertex {
    pub id: usize,
    pub(crate) size: (f64, f64),
    pub(crate) rank: i32,
    pub(crate) pos: usize,
    pub(crate) low: u32,
    pub(crate) lim: u32,
    pub(crate) parent: Option<NodeIndex>,
    pub(crate) is_tree_vertex: bool,
    pub(crate) is_dummy: bool,
    pub(crate) root: NodeIndex,
    pub(crate) align: NodeIndex,
    pub(crate) shift: f64,
    pub(crate) sink: NodeIndex,
    pub(crate) block_max_vertex_width: f64,
    pub(crate) x: i32,
    pub(crate) y: i32,
}

impl Vertex {
    pub fn new(id: usize, size: (f64, f64)) -> Self {
        Self {
            id,
            size,
            ..Default::default()
        }
    }
}

impl Default for Vertex {
    fn default() -> Self {
        Self {
            id: 0,
            size: (0.0, 0.0),
            rank: 0,
            x: 0,
            y: 0,
            pos: 0,
            low: 0,
            lim: 0,
            parent: None,
            is_tree_vertex: false,
            is_dummy: false,
            root: 0.into(),
            align: 0.into(),
            shift: f64::INFINITY,
            sink: 0.into(),
            block_max_vertex_width: 0.0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Edge {
    pub(crate) weight: i32,
    pub(crate) cut_value: Option<i32>,
    pub(crate) is_tree_edge: bool,
    pub(crate) has_type_1_conflict: bool,
}

impl Default for Edge {
    fn default() -> Self {
        Self {
            weight: 1,
            cut_value: None,
            is_tree_edge: false,
            has_type_1_conflict: false,
        }
    }
}

pub fn start(
    mut graph: StableDiGraph<Vertex, Edge>,
    config: &Config,
) -> Vec<(Vec<(usize, (f64, f64))>, f64, f64)> {
    init_graph(&mut graph);
    weakly_connected_components(graph)
        .into_iter()
        .map(|g| build_layout(g, config))
        .collect()
}

fn init_graph(graph: &mut StableDiGraph<Vertex, Edge>) {
    info!("Initializing graphs vertex weights");
    for id in graph.node_indices().collect::<Vec<_>>() {
        graph[id].id = id.index();
        graph[id].root = id;
        graph[id].align = id;
        graph[id].sink = id;
    }
}

fn build_layout(
    mut graph: StableDiGraph<Vertex, Edge>,
    config: &Config,
) -> (Vec<(usize, (f64, f64))>, f64, f64) {
    info!(target: "layouting", "Start building layout");
    info!(target: "layouting", "Configuration is: {:?}", config);

    // Treat the vertex spacing as just additional padding in each node. Each node will then take
    // 50% of the "responsibility" of the vertex spacing. This does however mean that dummy vertices
    // will have a gap of 50% of the vertex spacing between them and the next and previous vertex.
    for vertex in graph.node_weights_mut() {
        vertex.size.0 += config.vertex_spacing;
        vertex.size.1 += config.vertex_spacing;
    }

    // we don't remember the edges that where reversed for now, since they are
    // currently not needed
    let _ = execute_phase_0(&mut graph);

    execute_phase_1(
        &mut graph,
        config.minimum_length as i32,
        config.ranking_type,
    );

    let layers = execute_phase_2(
        &mut graph,
        config.minimum_length as i32,
        config.dummy_vertices.then_some(config.dummy_size),
        config.c_minimization,
        config.transpose,
    );

    let layout = execute_phase_3(&mut graph, layers);
    debug!(target: "layouting", "Node coordinates: {:?}\nEdge coordinates: {:?}\nwidth: {}, height:{}",
        layout.node_coordinates,
        layout.edge_coordinates,
        layout.width,
        layout.height
    );
    layout
}

fn execute_phase_0(graph: &mut StableDiGraph<Vertex, Edge>) -> Vec<EdgeIndex> {
    info!(target: "layouting", "Executing phase 0: Cycle Removal");
    remove_cycles(graph)
}

/// Assign each vertex a rank
fn execute_phase_1(
    graph: &mut StableDiGraph<Vertex, Edge>,
    minimum_length: i32,
    ranking_type: RankingType,
) {
    info!(target: "layouting", "Executing phase 1: Ranking");
    rank(graph, minimum_length, ranking_type);
}

/// Reorder vertices in ranks to reduce crossings. If `dummy_size` is [Some],
/// dummies will be passed along to the next phase.
fn execute_phase_2(
    graph: &mut StableDiGraph<Vertex, Edge>,
    minimum_length: i32,
    dummy_size: Option<f64>,
    crossing_minimization: CrossingMinimization,
    transpose: bool,
) -> Vec<Vec<NodeIndex>> {
    info!(target: "layouting", "Executing phase 2: Crossing Reduction");
    info!(target: "layouting",
        "dummy vertex size: {:?}, heuristic for crossing minimization: {:?}, using transpose: {}",
        dummy_size,
        crossing_minimization,
        transpose
    );

    insert_dummy_vertices(graph, minimum_length, dummy_size.unwrap_or(0.0));
    let mut order = ordering(graph, crossing_minimization, transpose);
    if dummy_size.is_none() {
        remove_dummy_vertices(graph, &mut order);
    }
    order
}

/// calculate the final coordinates for each vertex, after the graph was layered and crossings where minimized.
fn execute_phase_3(
    graph: &mut StableDiGraph<Vertex, Edge>,
    mut layers: Vec<Vec<NodeIndex>>,
) -> LayoutResult {
    info!(target: "layouting", "Executing phase 3: Coordinate Calculation");
    for n in graph.node_indices().collect::<Vec<_>>() {
        if graph[n].is_dummy {
            graph[n].id = n.index();
        }
    }
    let width = layers.iter().map(|l| l.len()).max().unwrap_or(0) as f64;
    let height = layers.len() as f64;
    let mut layouts = create_layouts(graph, &mut layers);

    align_to_smallest_width_layout(&mut layouts);
    let mut x_coordinates = calculate_relative_coords(layouts);
    // determine the smallest x-coordinate
    let min = x_coordinates
        .iter()
        .min_by(|a, b| a.1.total_cmp(&b.1))
        .unwrap()
        .1;

    // shift all coordinates so the minimum coordinate is 0
    for (_, c) in &mut x_coordinates {
        *c -= min;
    }

    // Find max y size in each rank. Use a BTreeMap so iteration through the map
    // is ordered.
    let mut rank_to_max_height = BTreeMap::<i32, f64>::new();
    for vertex in graph.node_weights() {
        let max = rank_to_max_height.entry(vertex.rank).or_default();
        *max = max.max(vertex.size.1);
    }

    // Stack up each rank to assign it an offset. The gap between each rank and the next is half the
    // height of the current rank, plus half the height of the next rank.
    let mut rank_to_y_offset = HashMap::new();
    let mut current_rank_top_offset = *rank_to_max_height.iter().next().unwrap().1 * -0.5;
    for (rank, max_height) in rank_to_max_height {
        // The center of the rank is the middle of the max height plus the top of the rank.
        rank_to_y_offset.insert(rank, current_rank_top_offset + max_height * 0.5);
        // Shift by the height of the rank. The height of a rank already includes the vertex
        // spacing.
        current_rank_top_offset += max_height;
    }

    let coordinates = x_coordinates
        .iter()
        .filter(|&(v, _)| !graph[*v].is_dummy)
        // calculate y coordinate
        .map(|&(v, x)| {
            (
                graph[v].id,
                (x, *rank_to_y_offset.get(&graph[v].rank).unwrap()),
            )
        })
        .collect::<Vec<_>>();

    // Collect edge to dummy path coordinate mappings
    let mut edge_coordinates = Vec::new();

    let dummy_coordinates: HashMap<_, _> = x_coordinates
        .iter()
        .filter(|&(v, _)| graph[*v].is_dummy)
        .map(|&(v, x)| (v, (x, *rank_to_y_offset.get(&graph[v].rank).unwrap())))
        .collect();

    let regular_nodes = graph.node_indices()
        .filter(|&n| !graph[n].is_dummy)
        .collect::<Vec<_>>();

    for &source in &regular_nodes {
        // DFS to find paths to other regular nodes through dummy nodes
        for mut neighbor in graph.neighbors_directed(source, petgraph::Direction::Outgoing) {
            let mut dummy_path = Vec::new();
            
            // Follow path through dummy nodes
            while graph[neighbor].is_dummy {
                dummy_path.push(*dummy_coordinates.get(&neighbor).unwrap());
                let next_nodes = graph.neighbors_directed(neighbor, petgraph::Direction::Outgoing)
                    .collect::<Vec<_>>();
                neighbor = next_nodes[0];
            }
            
            // Since we exited the loop, we reached a regular node
            if !dummy_path.is_empty() {
                edge_coordinates.push(
                    ((graph[source].id, graph[neighbor].id), dummy_path)
                );
            }
        }
    }

    LayoutResult {
        node_coordinates: coordinates,
        edge_coordinates,
        width,
        height,
    }
}

pub fn slack(graph: &StableDiGraph<Vertex, Edge>, edge: EdgeIndex, minimum_length: i32) -> i32 {
    let (tail, head) = graph.edge_endpoints(edge).unwrap();
    graph[head].rank - graph[tail].rank - minimum_length
}

#[allow(dead_code)]
fn print_to_console(
    dir: VDir,
    graph: &StableDiGraph<Vertex, Edge>,
    layers: &[Vec<NodeIndex>],
    mut coordinates: HashMap<NodeIndex, isize>,
    vertex_spacing: usize,
) {
    let min = *coordinates.values().min().unwrap();
    let str_width = 4;
    coordinates
        .values_mut()
        .for_each(|v| *v = str_width * (*v - min) / vertex_spacing as isize);
    let width = *coordinates.values().max().unwrap() as usize;

    for line in layers {
        let mut v_line = vec!['-'; width + str_width as usize];
        let mut a_line = vec![' '; width + str_width as usize];
        for v in line {
            let pos = *coordinates.get(v).unwrap() as usize;
            if graph[*v].root != *v {
                a_line[pos] = if dir == VDir::Up { 'v' } else { '^' };
            }
            for (i, c) in v.index().to_string().chars().enumerate() {
                v_line[pos + i] = c;
            }
        }
        match dir {
            VDir::Up => {
                println!("{}", v_line.into_iter().collect::<String>());
                println!("{}", a_line.into_iter().collect::<String>());
            }
            VDir::Down => {
                println!("{}", a_line.into_iter().collect::<String>());
                println!("{}", v_line.into_iter().collect::<String>());
            }
        }
    }
    println!();
}

/// Build a layout that preserves dummy nodes and uses integer coordinates.
/// Modifies the graph in place and returns it, adding dummy nodes and setting the coordinates.
///
pub fn build_integer_layout_with_dummies(
    mut graph: StableDiGraph<Vertex, Edge>,
    config: &Config,
) -> StableDiGraph<Vertex, Edge> {
    info!(target: "layouting", "Start building layout (with dummies)");
    info!(target: "layouting", "Configuration is: {:?}", config);

    // Initialize the graph
    init_graph(&mut graph);

    // Phase 1: Ranking
    execute_phase_1(
        &mut graph,
        config.minimum_length as i32,
        config.ranking_type,
    );

    // Phase 2: Crossing reduction with dummy nodes
    let mut order = execute_phase_2(
        &mut graph,
        config.minimum_length as i32,
        Some(1.0), // Use 1x1 dummy nodes
        config.c_minimization,
        config.transpose,
    );

    // Phase 3: Calculate coordinates
    let mut layouts = create_layouts(&mut graph, &mut order);
    align_to_smallest_width_layout(&mut layouts);
    let x_coords = calculate_relative_coords(layouts);

    // Convert to integers and double the x-coordinates to create spacing of 1
    let x_coords: HashMap<NodeIndex, i32> = x_coords
        .into_iter()
        .map(|(node, x)| (node, (x * 2.0).round() as i32))
        .collect();

    // Calculate y-coordinates based on rank
    let mut rank_to_y_offset = HashMap::new();
    for rank in 0..order.len() {
        rank_to_y_offset.insert(rank as i32, rank as i32);
    }

    // Store coordinates in the graph vertices
    for (node, x) in x_coords {
        let y = rank_to_y_offset[&graph[node].rank];
        graph[node].x = x;
        graph[node].y = y;
    }

    graph
}
