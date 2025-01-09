use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::edge::Edge;
use crate::models::node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID};
use crate::models::sequence::Sequence;
use crate::operation_management::{
    load_changeset, load_changeset_dependencies, load_changeset_models,
};
use crate::patch::OperationPatch;
use itertools::Itertools;
use petgraph::graphmap::DiGraphMap;
use petgraph::Direction;
use rusqlite::session::ChangesetIter;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};

fn get_contiguous_regions(v: &[i64]) -> Vec<(i64, i64)> {
    let mut regions = vec![];
    let mut last = v[0];
    let mut start = v[0];
    for i in v.iter().skip(1) {
        if i - 1 != last {
            regions.push((start, last));
            start = *i;
        }
        last = *i;
    }
    regions.push((start, last));
    regions
}

pub fn view_patches(patches: &[OperationPatch]) {
    let START_NODE = Node {
        id: PATH_START_NODE_ID,
        sequence_hash: "start-node-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"
            .to_string(),
        hash: None,
    };

    let END_NODE = Node {
        id: PATH_END_NODE_ID,
        sequence_hash: "end-node-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
            .to_string(),
        hash: None,
    };
    for patch in patches {
        let op_info = &patch.operation;
        let changeset = load_changeset(op_info);
        let dependencies = load_changeset_dependencies(op_info);

        let input: &mut dyn Read = &mut changeset.as_slice();
        let mut iter = ChangesetIter::start_strm(&input).unwrap();

        let new_models = load_changeset_models(&mut iter);
        let mut bges_by_bg: HashMap<i64, Vec<&BlockGroupEdge>> = HashMap::new();
        let mut edges_by_id: HashMap<i64, &Edge> = HashMap::new();
        let mut nodes_by_id: HashMap<i64, &Node> = HashMap::new();
        nodes_by_id.insert(START_NODE.id, &START_NODE);
        nodes_by_id.insert(END_NODE.id, &END_NODE);
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
            let mut graph: DiGraphMap<i64, (i64, i64)> = DiGraphMap::new();
            let mut block_graph: DiGraphMap<(i64, i64, i64), ()> = DiGraphMap::new();
            block_graph.add_node((PATH_START_NODE_ID, 0, 0));
            block_graph.add_node((PATH_END_NODE_ID, 0, 0));
            for bg_edge in bg_edges {
                let edge = *edges_by_id.get(&bg_edge.edge_id).unwrap();
                if edge.target_node_id == PATH_END_NODE_ID {
                    graph.add_edge(
                        edge.source_node_id,
                        edge.target_node_id,
                        (edge.source_coordinate - 1, edge.target_coordinate),
                    );
                } else if edge.source_node_id == PATH_START_NODE_ID {
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
                if node == PATH_START_NODE_ID || node == PATH_END_NODE_ID {
                    continue;
                }
                let in_ports = graph
                    .edges_directed(node, Direction::Incoming)
                    .map(|(src, dest, (fp, tp))| *tp)
                    .collect::<Vec<_>>();
                let out_ports = graph
                    .edges_directed(node, Direction::Outgoing)
                    .map(|(src, dest, (fp, tp))| *fp)
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
                    .find(|(node, start, end)| *node == src && end == fp)
                    .unwrap();
                let dest_block = block_graph
                    .nodes()
                    .find(|(node, start, end)| *node == dest && start == tp)
                    .unwrap();
                block_graph.add_edge(source_block, dest_block, ());
            }

            let path = format!("test_{bg_id}.dot");
            use std::fs::File;
            let mut file = File::create(path).unwrap();
            let mut dot = "digraph {\n    rankdir=LR\n    node [shape=none]\n".to_string();
            for (node_id, start, end) in block_graph.nodes() {
                if Node::is_terminal(node_id) {
                    let label = if node_id == PATH_START_NODE_ID {
                        "start"
                    } else {
                        "end"
                    };
                    dot.push_str(&format!(
                        "\"{node_id}.{start}.{end}\" [label=\"{label}\", shape=ellipse]\n",
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
                } else if node_id == PATH_START_NODE_ID {
                    "start".to_string()
                } else if node_id == PATH_END_NODE_ID {
                    "end".to_string()
                } else {
                    seq.get_sequence(start, end + 1)
                };

                let coordinates = if end - start > 0 {
                    format!("{node_id}:{start}-{end}")
                } else if start > 0 {
                    format!("{node_id}:{start}")
                } else {
                    format!("{node_id}")
                };

                let label = format!(
                    "<\
                <TABLE BORDER='0'>\
                    <TR>\
                        <TD BORDER='1' ALIGN='CENTER' PORT='seq'>\
                            <FONT POINT-SIZE='12' FACE='Monospace'>{formatted_seq}</FONT>\
                        </TD>\
                    </TR>\
                    <TR>\
                        <TD ALIGN='CENTER'>\
                            <FONT POINT-SIZE='10'>{coordinates}</FONT>\
                        </TD>\
                    </TR>\
                </TABLE>\
                >"
                );

                dot.push_str(&format!("\"{node_id}.{start}.{end}\" [label={label}]\n",));
            }

            for ((src, s_fp, s_tp), (dest, d_fp, d_tp), ()) in block_graph.all_edges() {
                let style = if src == dest { "dashed" } else { "solid" };
                let arrow = if src == dest { "none" } else { "normal" };
                let headport = if dest == PATH_END_NODE_ID {
                    "w"
                } else {
                    "seq:w"
                };
                let tailport = if src == PATH_START_NODE_ID {
                    "e"
                } else {
                    "seq:e"
                };
                dot.push_str(&format!(
                    "\"{src}.{s_fp}.{s_tp}\" -> \"{dest}.{d_fp}.{d_tp}\" [arrowhead={arrow}, headport=\"{headport}\", tailport=\"{tailport}\", style=\"{style}\"]\n"
                ));
            }

            dot.push('}');
            let _ = file.write_all(dot.as_bytes());
        }
    }
}
