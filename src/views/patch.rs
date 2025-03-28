use crate::graph::{GenGraph, GraphEdge, GraphNode};
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::edge::Edge;
use crate::models::node::Node;
use crate::models::sequence::Sequence;
use crate::models::strand::Strand;
use crate::models::strand::Strand::Forward;
use crate::operation_management::{
    load_changeset, load_changeset_dependencies, load_changeset_models, ChangesetModels,
    DependencyModels,
};
use crate::patch::OperationPatch;
use html_escape;
use itertools::Itertools;
use petgraph::graphmap::DiGraphMap;
use petgraph::Direction;
use rusqlite::session::ChangesetIter;
use std::collections::{HashMap, HashSet};
use std::io::Read;

pub fn get_change_graph(
    changes: &ChangesetModels,
    dependencies: &DependencyModels,
) -> HashMap<i64, GenGraph> {
    let start_node = Node::get_start_node();
    let end_node = Node::get_end_node();
    let mut bges_by_bg: HashMap<i64, Vec<&BlockGroupEdge>> = HashMap::new();
    let mut edges_by_id: HashMap<i64, &Edge> = HashMap::new();
    let mut nodes_by_id: HashMap<i64, &Node> = HashMap::new();
    nodes_by_id.insert(start_node.id, &start_node);
    nodes_by_id.insert(end_node.id, &end_node);
    let mut sequences_by_hash: HashMap<&String, &Sequence> = HashMap::new();
    let mut block_graphs: HashMap<i64, GenGraph> = HashMap::new();

    for bge in changes.block_group_edges.iter() {
        bges_by_bg
            .entry(bge.block_group_id)
            .and_modify(|l| l.push(bge))
            .or_insert_with(|| vec![bge]);
    }
    for edge in changes.edges.iter().chain(dependencies.edges.iter()) {
        edges_by_id.insert(edge.id, edge);
    }
    for node in changes.nodes.iter().chain(dependencies.nodes.iter()) {
        nodes_by_id.insert(node.id, node);
    }
    for seq in changes
        .sequences
        .iter()
        .chain(dependencies.sequences.iter())
    {
        sequences_by_hash.insert(&seq.hash, seq);
    }

    for (bg_id, bg_edges) in bges_by_bg.iter() {
        // There are 2 graphs created here. The first graph is our normal graph of nodes
        // and edges. This graph is then used to make our second graph representing the spans
        // of each node (blocks).
        let mut graph: DiGraphMap<i64, (i64, i64)> = DiGraphMap::new();
        let mut block_graph: GenGraph = DiGraphMap::new();
        block_graph.add_node(GraphNode {
            block_id: -1,
            node_id: start_node.id,
            sequence_start: 0,
            sequence_end: 0,
        });
        block_graph.add_node(GraphNode {
            block_id: -1,
            node_id: end_node.id,
            sequence_start: 0,
            sequence_end: 0,
        });
        for bg_edge in bg_edges {
            let edge = *edges_by_id.get(&bg_edge.edge_id).unwrap();
            graph.add_edge(
                edge.source_node_id,
                edge.target_node_id,
                (edge.source_coordinate, edge.target_coordinate),
            );
        }

        for node in graph.nodes() {
            // This is where we make the block graph. For this, we figure out the positions of
            // all incoming and outgoing edges from the node. Then we make blocks between those
            // positions.
            if Node::is_terminal(node) {
                continue;
            }
            let in_ports = graph
                .edges_directed(node, Direction::Incoming)
                .map(|(_src, _dest, (_fp, tp))| *tp)
                .collect::<Vec<_>>();
            let out_ports = graph
                .edges_directed(node, Direction::Outgoing)
                .map(|(_src, _dest, (fp, _tp))| *fp)
                .collect::<Vec<_>>();

            let node_obj = *nodes_by_id.get(&node).unwrap();
            let sequence = *sequences_by_hash.get(&node_obj.sequence_hash).unwrap();
            let s_len = sequence.length;
            let mut block_starts: HashSet<i64> = HashSet::from_iter(in_ports.iter().copied());
            block_starts.insert(0);
            for x in out_ports.iter() {
                if *x < s_len - 1 {
                    block_starts.insert(*x);
                }
            }
            let mut block_ends: HashSet<i64> = HashSet::from_iter(out_ports.iter().copied());
            block_ends.insert(s_len);
            for x in in_ports.iter() {
                if *x > 0 {
                    block_ends.insert(*x);
                }
            }

            let block_starts = block_starts.into_iter().sorted().collect::<Vec<_>>();
            let block_ends = block_ends.into_iter().sorted().collect::<Vec<_>>();

            let mut blocks = vec![];
            for (i, j) in block_starts.iter().zip(block_ends.iter()) {
                let node = GraphNode {
                    block_id: -1,
                    node_id: node,
                    sequence_start: *i,
                    sequence_end: *j,
                };
                block_graph.add_node(node);
                blocks.push(node);
            }

            for (i, j) in blocks.iter().tuple_windows() {
                block_graph.add_edge(
                    *i,
                    *j,
                    vec![GraphEdge {
                        edge_id: -1,
                        source_strand: Strand::Forward,
                        target_strand: Forward,
                        chromosome_index: 0,
                        phased: 0,
                    }],
                );
            }
        }

        for (src, dest, (fp, tp)) in graph.all_edges() {
            if !(Node::is_end_node(src) && Node::is_start_node(dest)) {
                let source_block = block_graph
                    .nodes()
                    .find(|node| node.node_id == src && node.sequence_end == *fp)
                    .unwrap();
                let dest_block = block_graph
                    .nodes()
                    .find(|node| node.node_id == dest && node.sequence_start == *tp)
                    .unwrap();
                block_graph.add_edge(
                    source_block,
                    dest_block,
                    vec![GraphEdge {
                        edge_id: -1,
                        source_strand: Strand::Forward,
                        target_strand: Forward,
                        chromosome_index: 0,
                        phased: 0,
                    }],
                );
            }
        }
        block_graphs.insert(*bg_id, block_graph);
    }
    block_graphs
}

pub fn view_patches(patches: &[OperationPatch]) -> HashMap<String, HashMap<i64, String>> {
    // For each blockgroup in a patch, a .dot file is generated showing how the base sequence
    // has been updated.
    let mut diagrams: HashMap<String, HashMap<i64, String>> = HashMap::new();

    for patch in patches {
        // The beginning work is loading the models from the patch as well as dependencies. Once
        // loaded, a graph is created of the added nodes and returned as a dot string
        let mut bg_dots: HashMap<i64, String> = HashMap::new();

        let op_info = &patch.operation;
        let changeset = load_changeset(op_info);
        let dependencies = load_changeset_dependencies(op_info);

        let input: &mut dyn Read = &mut changeset.as_slice();
        let mut iter = ChangesetIter::start_strm(&input).unwrap();

        let new_models = load_changeset_models(&mut iter);

        let block_graphs = get_change_graph(&new_models, &dependencies);

        let mut sequences_by_hash: HashMap<&String, &Sequence> = HashMap::new();
        for seq in new_models
            .sequences
            .iter()
            .chain(dependencies.sequences.iter())
        {
            sequences_by_hash.insert(&seq.hash, seq);
        }
        let mut node_sequence_hashes: HashMap<i64, &String> = HashMap::new();
        for node in new_models.nodes.iter().chain(dependencies.nodes.iter()) {
            node_sequence_hashes.insert(node.id, &node.sequence_hash);
        }

        for (bg_id, block_graph) in block_graphs.iter() {
            let mut dot = "digraph {\n    rankdir=LR\n    node [shape=none]\n".to_string();
            for node in block_graph.nodes() {
                let node_id = node.node_id;
                let start = node.sequence_start;
                let end = node.sequence_end;
                let block_id = format!("{node_id}.{start}.{end}");
                if Node::is_terminal(node.node_id) {
                    let label = if Node::is_start_node(node.node_id) {
                        "start"
                    } else {
                        "end"
                    };
                    dot.push_str(&format!(
                        "\"{block_id}\" [label=\"{label}\", shape=ellipse]\n",
                    ));
                    continue;
                }

                let seq_hash = *node_sequence_hashes.get(&node.node_id).unwrap();
                let seq = *sequences_by_hash.get(seq_hash).unwrap();
                let len = end - start;

                let formatted_seq = if len > 7 {
                    format!(
                        "{s}...{e}",
                        s = seq.get_sequence(start, start + 3),
                        e = seq.get_sequence(end - 2, end + 1)
                    )
                } else {
                    seq.get_sequence(start, end + 1)
                };

                let coordinates = format!("{node_id}:{start}-{end}");

                let label = format!(
                    "<\
                <TABLE BORDER='0'>\
                    <TR>\
                        <TD BORDER='1' ALIGN='CENTER' PORT='seq'>\
                            <FONT POINT-SIZE='12' FACE='Monospace'>{escaped_seq}</FONT>\
                        </TD>\
                    </TR>\
                    <TR>\
                        <TD ALIGN='CENTER'>\
                            <FONT POINT-SIZE='10'>{coordinates}</FONT>\
                        </TD>\
                    </TR>\
                </TABLE>\
                >",
                    escaped_seq = html_escape::encode_safe(&formatted_seq)
                );

                dot.push_str(&format!("\"{block_id}\" [label={label}]\n",));
            }

            for (src_node, dst_node, _) in block_graph.all_edges() {
                let src = src_node.node_id;
                let s_fp = src_node.sequence_start;
                let s_tp = src_node.sequence_end;
                let dest = dst_node.node_id;
                let d_fp = dst_node.sequence_start;
                let d_tp = dst_node.sequence_end;
                // Edges between adjacent blocks from the same node don't have an arrowhead
                // and are dashed because they represent the reference and can't be traversed.
                // TODO: In a heterozygous genome this isn't true. Check needs to be expanded.
                let style = if src == dest && d_fp == s_tp + 1 {
                    "dashed"
                } else {
                    "solid"
                };
                let arrow = if src == dest && d_fp == s_tp + 1 {
                    "none"
                } else {
                    "normal"
                };
                let headport = if Node::is_end_node(dest) {
                    "w"
                } else {
                    "seq:w"
                };
                let tailport = if Node::is_start_node(src) {
                    "e"
                } else {
                    "seq:e"
                };
                dot.push_str(&format!(
                    "\"{src}.{s_fp}.{s_tp}\" -> \"{dest}.{d_fp}.{d_tp}\" [arrowhead={arrow}, headport=\"{headport}\", tailport=\"{tailport}\", style=\"{style}\"]\n"
                ));
            }

            dot.push('}');
            bg_dots.insert(*bg_id, dot);
        }
        diagrams.insert(patch.operation.hash.clone(), bg_dots);
    }
    diagrams
}
