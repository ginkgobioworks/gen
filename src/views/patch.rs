use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::edge::Edge;
use crate::models::node::Node;
use crate::models::sequence::Sequence;
use crate::operation_management::{
    load_changeset, load_changeset_dependencies, load_changeset_models,
};
use crate::patch::OperationPatch;
use petgraph::graphmap::DiGraphMap;
use rusqlite::session::ChangesetIter;
use std::collections::HashMap;
use std::io::{Read, Write};

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
            let mut graph: DiGraphMap<i64, ()> = DiGraphMap::new();
            let mut node_map = HashMap::new();
            let mut i = 1;
            let mut edge_labels = HashMap::new();
            let mut last_node: Option<i64> = None;
            for bg_edge in bg_edges {
                let edge = *edges_by_id.get(&bg_edge.edge_id).unwrap();
                if Node::is_terminal(edge.source_node_id) || Node::is_terminal(edge.target_node_id)
                {
                    continue;
                }
                println!("edge is {edge:?}");
                let source_node = *nodes_by_id.get(&edge.source_node_id).unwrap();
                let source_s = *sequences_by_hash.get(&source_node.sequence_hash).unwrap();
                let target_node = *nodes_by_id.get(&edge.target_node_id).unwrap();
                let target_s = *sequences_by_hash.get(&target_node.sequence_hash).unwrap();
                let key = format!(
                    "{name}[{start}-{end}]",
                    name = &source_s.name,
                    start = 0,
                    end = edge.source_coordinate
                );
                let sn = *node_map.entry(key).or_insert_with(|| {
                    i += 1;
                    i
                });
                let key = format!(
                    "{name}[{start}-{end}]",
                    name = &target_s.name,
                    start = edge.target_coordinate,
                    end = edge.target_coordinate + target_s.length
                );
                let tn = *node_map.entry(key).or_insert_with(|| {
                    i += 1;
                    i
                });
                graph.add_edge(sn, tn, ());
                let hdir;
                let tdir;
                if edge.source_node_id != edge.target_node_id {
                    if let Some(ln) = last_node {
                        if sn == ln {
                            hdir = "n";
                            tdir = "e";
                        } else {
                            hdir = "w";
                            tdir = "n";
                        }
                    } else if sn == tn {
                        hdir = "w";
                        tdir = "e";
                    } else {
                        hdir = "w";
                        tdir = "n";
                    }
                } else {
                    hdir = "w";
                    tdir = "e";
                }
                println!("{last_node:?} {sn} {tn}");
                edge_labels.insert((sn, tn), format!("tailport = {tdir}, headport = {hdir}"));
                last_node = Some(tn);
            }

            let path = format!("test_{bg_id}.dot");
            use petgraph::dot::{Config, Dot};
            use std::fs::File;
            let mut file = File::create(path).unwrap();
            let nodemap_inv: HashMap<i64, String> =
                HashMap::from_iter(node_map.iter().map(|(k, v)| (*v, k.clone())));
            let dot = format!(
                "{dot:?}",
                dot = Dot::with_attr_getters(
                    &graph,
                    &[Config::NodeNoLabel, Config::EdgeNoLabel],
                    &|_, (src, dst, _)| {
                        return edge_labels
                            .get(&(src, dst))
                            .unwrap_or(&"".to_string())
                            .to_string();
                        // if src != dst {
                        //     return format!("headport = e, tailport = n");
                        // }
                        // return format!("");
                    },
                    &|_, (node, _weight)| format!(
                        "label = \"{label}\"",
                        label = nodemap_inv.get(&node).unwrap()
                    ),
                )
            );
            let dot = dot.replace("digraph {", "digraph {\nrankdir=LR");
            let _ = file.write_all(dot.as_bytes());
        }
    }
}
