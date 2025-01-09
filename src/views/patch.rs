use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::edge::Edge;
use crate::models::node::Node;
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
            for bg_edge in bg_edges {
                let edge = *edges_by_id.get(&bg_edge.edge_id).unwrap();
                if Node::is_terminal(edge.source_node_id) || Node::is_terminal(edge.target_node_id)
                {
                    continue;
                }
                graph.add_edge(
                    edge.source_node_id,
                    edge.target_node_id,
                    (edge.source_coordinate, edge.target_coordinate),
                );
            }

            let path = format!("test_{bg_id}.dot");
            use std::fs::File;
            let mut file = File::create(path).unwrap();
            let mut dot = "digraph struct {\n    rankdir=TB\n    node [shape=record]\n".to_string();
            for node in graph.nodes() {
                let mut labels = vec![];
                let node_obj = *nodes_by_id.get(&node).unwrap();
                let seq = *sequences_by_hash.get(&node_obj.sequence_hash).unwrap();
                let mut regions: HashSet<i64> = HashSet::new();
                for (_, _, (_, to)) in graph.edges_directed(node, Direction::Incoming) {
                    regions.insert(*to);
                }
                for (_, _, (from, _)) in graph.edges_directed(node, Direction::Outgoing) {
                    regions.insert(*from);
                }
                let regions = regions.into_iter().sorted().collect::<Vec<_>>();
                let mut added = HashSet::new();
                for (start, end) in get_contiguous_regions(&regions) {
                    let mut label = vec![];
                    if start != 0 {
                        label.push("...".to_string());
                    }
                    for i in (start - 1..end + 2) {
                        if !added.contains(&i) && i >= 0 && i < seq.length {
                            label.push(format!(
                                "{{<{i}>{i}|{bp}}}",
                                bp = seq.get_sequence(i, i + 1)
                            ));
                        } else if i == seq.length {
                            label.push(format!("{{<{i}>{i}|END}}"));
                        }
                        added.insert(i);
                    }
                    if end != seq.length {
                        label.push("...".to_string());
                    }
                    labels.push(label.join("|"));
                }
                dot.push_str(&format!(
                    "{node} [label=\"{label}\"]\n",
                    label = labels.join("|")
                ));
            }

            for (src, dest, (from, to)) in graph.all_edges() {
                dot.push_str(&format!("{src}:{from} -> {dest}:{to}\n"));
            }

            dot.push('}');
            let _ = file.write_all(dot.as_bytes());
        }
    }
}
