use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::edge::Edge;
use crate::models::node::Node;
use crate::models::sequence::Sequence;
use crate::operation_management::{
    load_changeset, load_changeset_dependencies, load_changeset_models,
};
use crate::patch::OperationPatch;
use html_escape;
use itertools::Itertools;
use petgraph::graphmap::DiGraphMap;
use petgraph::Direction;
use rusqlite::session::ChangesetIter;
use std::collections::{HashMap, HashSet};
use std::io::Read;

pub fn view_patches(patches: &[OperationPatch]) -> HashMap<String, HashMap<i64, String>> {
    // For each blockgroup in a patch, a
    let start_node = Node::get_start_node();
    let end_node = Node::get_end_node();
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
        let mut bges_by_bg: HashMap<i64, Vec<&BlockGroupEdge>> = HashMap::new();
        let mut edges_by_id: HashMap<i64, &Edge> = HashMap::new();
        let mut nodes_by_id: HashMap<i64, &Node> = HashMap::new();
        nodes_by_id.insert(start_node.id, &start_node);
        nodes_by_id.insert(end_node.id, &end_node);
        let mut sequences_by_hash: HashMap<&String, &Sequence> = HashMap::new();

        for bge in new_models.block_group_edges.iter() {
            bges_by_bg
                .entry(bge.block_group_id)
                .and_modify(|l| l.push(bge))
                .or_insert_with(|| vec![bge]);
        }
        for edge in new_models.edges.iter().chain(dependencies.edges.iter()) {
            edges_by_id.insert(edge.id, edge);
        }
        for node in new_models.nodes.iter().chain(dependencies.nodes.iter()) {
            nodes_by_id.insert(node.id, node);
        }
        for seq in new_models
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
            let mut block_graph: DiGraphMap<(i64, i64, i64), ()> = DiGraphMap::new();
            block_graph.add_node((start_node.id, 0, 0));
            block_graph.add_node((end_node.id, 0, 0));
            for bg_edge in bg_edges {
                let edge = *edges_by_id.get(&bg_edge.edge_id).unwrap();
                // Because our model is an edge graph, the coordinate where an edge occurs is
                // actually offset from the block. So we need to adjust coordinates when going
                // to blocks. This isn't true for our start node though, which has a source
                // coordinate of 0.
                if Node::is_start_node(edge.source_node_id) {
                    graph.add_edge(
                        edge.source_node_id,
                        edge.target_node_id,
                        (edge.source_coordinate, edge.target_coordinate),
                    );
                } else {
                    graph.add_edge(
                        edge.source_node_id,
                        edge.target_node_id,
                        (edge.source_coordinate - 1, edge.target_coordinate),
                    );
                }
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
                        block_starts.insert(x + 1);
                    }
                }
                let mut block_ends: HashSet<i64> = HashSet::from_iter(out_ports.iter().copied());
                block_ends.insert(s_len);
                for x in in_ports.iter() {
                    if *x > 0 {
                        block_ends.insert(x - 1);
                    }
                }

                let block_starts = block_starts.into_iter().sorted().collect::<Vec<_>>();
                let block_ends = block_ends.into_iter().sorted().collect::<Vec<_>>();

                let mut blocks = vec![];
                for (i, j) in block_starts.iter().zip(block_ends.iter()) {
                    block_graph.add_node((node, *i, *j));
                    blocks.push((node, *i, *j));
                }

                for (i, j) in blocks.iter().tuple_windows() {
                    block_graph.add_edge(*i, *j, ());
                }
            }

            for (src, dest, (fp, tp)) in graph.all_edges() {
                let source_block = block_graph
                    .nodes()
                    .find(|(node, _start, end)| *node == src && end == fp)
                    .unwrap();
                let dest_block = block_graph
                    .nodes()
                    .find(|(node, start, _end)| *node == dest && start == tp)
                    .unwrap();
                block_graph.add_edge(source_block, dest_block, ());
            }

            let mut dot = "digraph {\n    rankdir=LR\n    node [shape=none]\n".to_string();
            for (node_id, start, end) in block_graph.nodes() {
                let block_id = format!("{node_id}.{start}.{end}");
                if Node::is_terminal(node_id) {
                    let label = if Node::is_start_node(node_id) {
                        "start"
                    } else {
                        "end"
                    };
                    dot.push_str(&format!(
                        "\"{block_id}\" [label=\"{label}\", shape=ellipse]\n",
                    ));
                    continue;
                }

                let node = *nodes_by_id.get(&node_id).unwrap();
                let seq = *sequences_by_hash.get(&node.sequence_hash).unwrap();
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

            for ((src, s_fp, s_tp), (dest, d_fp, d_tp), ()) in block_graph.all_edges() {
                // Edges between adjacent blocks from the same node don't have an arrowhead
                // and are dashed because they represent the reference and can't be traversed.
                // TODO: In a heterozygous genome this isn't true. Check needs to be expanded.
                let style = if src == dest && d_fp == s_tp + 1 { "dashed" } else { "solid" };
                let arrow = if src == dest && d_fp == s_tp + 1 { "none" } else { "normal" };
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
