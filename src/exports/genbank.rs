#![allow(warnings)]
use crate::graph::{all_simple_paths, GraphEdge, GraphNode};
use crate::models::block_group::BlockGroup;
use crate::models::node::Node;
use crate::models::path::PathBlock;
use crate::models::sample::Sample;
use gb_io;
use gb_io::QualifierKey;
use itertools::Itertools;
use petgraph::prelude::DiGraphMap;
use petgraph::visit::Dfs;
use rusqlite;
use rusqlite::Connection;
use std::collections::HashSet;
use std::fs::File;
use std::hash::Hash;
use std::iter::zip;
use std::path::PathBuf;

fn merge_nodes(nodes: &[GraphNode]) -> Vec<GraphNode> {
    let mut merged = vec![*nodes.first().unwrap()];
    if nodes.len() > 1 {
        for node in nodes[1..].iter() {
            let last_node = merged.last().unwrap();
            if node.node_id == last_node.node_id && node.sequence_start == last_node.sequence_end {
                merged.last_mut().unwrap().sequence_end = node.sequence_end;
            } else {
                merged.push(*node);
            }
        }
    }
    merged
}

fn get_path_nodes(
    graph: &DiGraphMap<GraphNode, GraphEdge>,
    path_blocks: &[PathBlock],
) -> Vec<GraphNode> {
    // From a graph, return graph nodes that traverse a given path.

    // first, reduce the graph down to nodes and connections that are possible
    let mut path_nodes = HashSet::new();
    let mut path_edges = HashSet::new();
    let mut path_it = path_blocks.iter().peekable();
    while let Some(block) = path_it.next() {
        let terminal_a = Node::is_terminal(block.node_id);
        if !terminal_a {
            path_nodes.insert(block.node_id);
        }
        if let Some(next_block) = path_it.peek() {
            let terminal_b = Node::is_terminal(next_block.node_id);
            if !terminal_b {
                path_nodes.insert(next_block.node_id);
            }
            if !terminal_a && !terminal_b {
                path_edges.insert((block.node_id, next_block.node_id));
            }
        }
    }

    let mut path_graph: DiGraphMap<GraphNode, GraphEdge> = DiGraphMap::new();
    for node in graph.nodes() {
        if path_nodes.contains(&node.node_id) {
            path_graph.add_node(node);
            for (_src_node, target_node, weight) in graph.edges(node) {
                // we include self-node connections because nodes are often split in the graph creation, so this ensures
                // connections like node_1 -> node_1 are kept since they will not be path edges.
                if (node.node_id == target_node.node_id
                    && node.sequence_end == target_node.sequence_start)
                    || (path_edges.contains(&(node.node_id, target_node.node_id)))
                {
                    path_graph.add_edge(node, target_node, *weight);
                } else {
                    println!("invalid edge {_src_node:?} {target_node:?}");
                }
            }
        }
    }

    let blocks = path_blocks
        .iter()
        .filter(|block| !Node::is_terminal(block.node_id))
        .sorted_by(|a, b| Ord::cmp(&a.path_start, &b.path_start))
        .collect::<Vec<&PathBlock>>();
    let (_, first_block) = path_blocks
        .iter()
        .find_position(|block| block.path_start == 0)
        .unwrap();
    let last_block = blocks[blocks.len() - 1];
    let start_nodes = path_graph
        .nodes()
        .filter(|node| {
            if node.node_id == first_block.node_id {
                return node.sequence_start <= first_block.sequence_start
                    && node.sequence_end <= first_block.sequence_end;
            }
            false
        })
        .collect::<Vec<GraphNode>>();
    let end_nodes = path_graph
        .nodes()
        .filter(|node| {
            if node.node_id == last_block.node_id {
                return node.sequence_end >= last_block.sequence_end
                    && node.sequence_start >= last_block.sequence_start
                    && node.sequence_start <= last_block.sequence_end;
            }
            false
        })
        .collect::<Vec<GraphNode>>();

    let mut nodes_for_path = vec![];

    for start_node in start_nodes.iter() {
        for end_node in end_nodes.iter() {
            for node_path in all_simple_paths(&path_graph, *start_node, *end_node) {
                let merged_path = merge_nodes(&node_path);
                let mut invalid = false;
                for (putative_path_node, path_block) in zip(merged_path, &blocks) {
                    if !(putative_path_node.sequence_start <= path_block.sequence_start
                        && putative_path_node.sequence_end == path_block.sequence_end)
                    {
                        invalid = true;
                        break;
                    }
                }
                if !invalid {
                    nodes_for_path = node_path
                }
            }
        }
    }

    nodes_for_path
}

pub fn export_genbank(
    conn: &Connection,
    collection_name: &str,
    sample_name: Option<&str>,
    filename: &PathBuf,
) {
    let block_groups = Sample::get_block_groups(conn, collection_name, sample_name);

    let file = File::create(filename).unwrap();
    let mut writer = gb_io::writer::SeqWriter::new(file);

    for block_group in block_groups.iter() {
        let path = BlockGroup::get_current_path(conn, block_group.id);
        let path_blocks = path.blocks(conn);
        let mut seq = gb_io::seq::Seq::empty();
        seq.name = Some(block_group.name.clone());
        seq.seq = path.sequence(conn).into_bytes();

        // for marking engineering, we take our path and annotate regions that exit it
        let graph = BlockGroup::get_graph(conn, block_group.id);
        let path_nodes = get_path_nodes(&graph, &path_blocks);
        let path_node_set: HashSet<&GraphNode> = HashSet::from_iter(&path_nodes);
        let mut node_it = path_nodes.iter().peekable();

        let mut position = 0;

        while let Some(current_node) = node_it.next() {
            position += current_node.length();
            for (source_node, target_node, edge_weight) in graph.edges(*current_node) {
                if let Some(next_node) = node_it.peek() {
                    if &&target_node != next_node {
                        let mut sub_path = vec![target_node];
                        // if we fork out from a path, write it as a change until we get back into the path. We do a simple
                        // DFS until we are back in, as genbank can't support real combinatorials so we assume there is simple
                        // engineering here with only 1 alternative path
                        let mut dfs = Dfs::new(&graph, target_node);
                        while let Some(nx) = dfs.next(&graph) {
                            if path_node_set.contains(&nx) {
                                break;
                            }
                            sub_path.push(nx)
                        }

                        let mut sequence = String::new();
                        for sub_node in sub_path.iter() {
                            let seqs = Node::get_sequences_by_node_ids(conn, &[sub_node.node_id]);
                            let seq = &seqs[&sub_node.node_id];
                            sequence.push_str(
                                &seq.get_sequence(sub_node.sequence_start, sub_node.sequence_end),
                            );
                        }
                        let mut qualifiers = vec![];
                        // if the next node is not contiguous, the user did an insertion as it appears
                        // like a deletion in the graph when importing a genbank
                        if target_node.node_id == current_node.node_id
                            && target_node.sequence_start != current_node.sequence_end
                        {
                            qualifiers.push((
                                QualifierKey::from("note"),
                                Some("Geneious type: Editing History Insertion".to_string()),
                            ));
                            qualifiers.push((QualifierKey::from("Original_Bases"), None));
                        } else if target_node.node_id != current_node.node_id {
                            qualifiers.push((
                                QualifierKey::from("note"),
                                Some("Geneious type: Editing History Replacement".to_string()),
                            ));
                            qualifiers.push((
                                QualifierKey::from("Original_Bases"),
                                Some(sequence.to_string()),
                            ));
                        }
                        // if the next node is a different node_id, the user did a replacement
                        seq.features.push(gb_io::seq::Feature {
                            kind: gb_io::seq::FeatureKind::from("misc_feature"),
                            location: seq
                                .range_to_location(position, position + sequence.len() as i64),
                            qualifiers,
                        });
                    }
                }
            }
        }
        writer.write(&seq).unwrap();
    }

    println!("Exported to file {}", filename.display());
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::imports::genbank::import_genbank;
    use crate::models::file_types::FileTypes;
    use crate::models::operations::OperationInfo;
    use crate::models::strand::Strand::Forward;
    use crate::models::{metadata, operations::setup_db};
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use std::io::BufReader;
    use std::path::PathBuf;
    use std::{io, str};
    use tempfile;

    #[test]
    fn test_import_then_export_insertion() {
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/geneious_genbank/insertion.gb");
        let file = File::open(&path).unwrap();
        let operation = import_genbank(
            conn,
            op_conn,
            BufReader::new(file),
            None,
            OperationInfo {
                file_path: path.to_str().unwrap().to_string(),
                file_type: FileTypes::GenBank,
                description: "test".to_string(),
            },
        )
        .unwrap();
        let ig = BlockGroup::get_graph(conn, 1);
        let tmp_dir = tempfile::tempdir().unwrap().into_path();
        let filename = tmp_dir.join("out.gb");
        export_genbank(conn, "", None, &filename);
    }

    #[test]
    fn test_import_then_export_replacement() {
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/geneious_genbank/deletion_and_insertion.gb");
        let file = File::open(&path).unwrap();
        let operation = import_genbank(
            conn,
            op_conn,
            BufReader::new(file),
            None,
            OperationInfo {
                file_path: path.to_str().unwrap().to_string(),
                file_type: FileTypes::GenBank,
                description: "test".to_string(),
            },
        )
        .unwrap();
        let ig = BlockGroup::get_graph(conn, 1);
        let tmp_dir = tempfile::tempdir().unwrap().into_path();
        let filename = tmp_dir.join("out.gb");
        export_genbank(conn, "", None, &filename);
    }

    #[test]
    fn test_get_path_graph() {
        let mut graph = DiGraphMap::new();
        graph.add_edge(
            GraphNode {
                block_id: -1,
                node_id: 10,
                sequence_start: 0,
                sequence_end: 10,
            },
            GraphNode {
                block_id: -1,
                node_id: 10,
                sequence_start: 10,
                sequence_end: 20,
            },
            GraphEdge {
                edge_id: 1,
                chromosome_index: 0,
                phased: 0,
                source_strand: Forward,
                target_strand: Forward,
            },
        );
        // second starting point for the graph, this also represents a node that is part of the path, but part of the sequence we don't want to use in our path
        graph.add_edge(
            GraphNode {
                block_id: -1,
                node_id: 20,
                sequence_start: 0,
                sequence_end: 10,
            },
            GraphNode {
                block_id: -1,
                node_id: 10,
                sequence_start: 10,
                sequence_end: 20,
            },
            GraphEdge {
                edge_id: 1,
                chromosome_index: 0,
                phased: 0,
                source_strand: Forward,
                target_strand: Forward,
            },
        );
        // represent node_id being split into 3 pieces
        graph.add_edge(
            GraphNode {
                block_id: -1,
                node_id: 10,
                sequence_start: 10,
                sequence_end: 20,
            },
            GraphNode {
                block_id: -1,
                node_id: 10,
                sequence_start: 20,
                sequence_end: 30,
            },
            GraphEdge {
                edge_id: 1,
                chromosome_index: 0,
                phased: 0,
                source_strand: Forward,
                target_strand: Forward,
            },
        );
        // put the same node_id 1 somewhere random in the graph on an edge we don't want to follow
        graph.add_edge(
            GraphNode {
                block_id: -1,
                node_id: 10,
                sequence_start: 0,
                sequence_end: 10,
            },
            GraphNode {
                block_id: -1,
                node_id: 30,
                sequence_start: 0,
                sequence_end: 10,
            },
            GraphEdge {
                edge_id: 1,
                chromosome_index: 0,
                phased: 0,
                source_strand: Forward,
                target_strand: Forward,
            },
        );
        graph.add_edge(
            GraphNode {
                block_id: -1,
                node_id: 30,
                sequence_start: 0,
                sequence_end: 10,
            },
            GraphNode {
                block_id: -1,
                node_id: 10,
                sequence_start: 10,
                sequence_end: 20,
            },
            GraphEdge {
                edge_id: 1,
                chromosome_index: 0,
                phased: 0,
                source_strand: Forward,
                target_strand: Forward,
            },
        );
        graph.add_edge(
            GraphNode {
                block_id: -1,
                node_id: 10,
                sequence_start: 10,
                sequence_end: 20,
            },
            GraphNode {
                block_id: -1,
                node_id: 10,
                sequence_start: 20,
                sequence_end: 30,
            },
            GraphEdge {
                edge_id: 1,
                chromosome_index: 0,
                phased: 0,
                source_strand: Forward,
                target_strand: Forward,
            },
        );
        // final part of path block
        graph.add_edge(
            GraphNode {
                block_id: -1,
                node_id: 10,
                sequence_start: 20,
                sequence_end: 30,
            },
            GraphNode {
                block_id: -1,
                node_id: 20,
                sequence_start: 30,
                sequence_end: 40,
            },
            GraphEdge {
                edge_id: 1,
                chromosome_index: 0,
                phased: 0,
                source_strand: Forward,
                target_strand: Forward,
            },
        );
        graph.add_edge(
            GraphNode {
                block_id: -1,
                node_id: 20,
                sequence_start: 30,
                sequence_end: 40,
            },
            GraphNode {
                block_id: -1,
                node_id: 20,
                sequence_start: 40,
                sequence_end: 60,
            },
            GraphEdge {
                edge_id: 1,
                chromosome_index: 0,
                phased: 0,
                source_strand: Forward,
                target_strand: Forward,
            },
        );
        graph.add_edge(
            GraphNode {
                block_id: -1,
                node_id: 20,
                sequence_start: 30,
                sequence_end: 40,
            },
            GraphNode {
                block_id: -1,
                node_id: 40,
                sequence_start: 40,
                sequence_end: 60,
            },
            GraphEdge {
                edge_id: 1,
                chromosome_index: 0,
                phased: 0,
                source_strand: Forward,
                target_strand: Forward,
            },
        );
        let path_blocks = vec![
            PathBlock {
                id: 0,
                node_id: 10,
                block_sequence: String::new(),
                sequence_start: 0,
                sequence_end: 30,
                path_start: 0,
                path_end: 30,
                strand: Forward,
            },
            PathBlock {
                id: 0,
                node_id: 20,
                block_sequence: String::new(),
                sequence_start: 30,
                sequence_end: 60,
                path_start: 30,
                path_end: 60,
                strand: Forward,
            },
        ];
        assert_eq!(
            get_path_nodes(&graph, &path_blocks),
            vec![
                GraphNode {
                    block_id: -1,
                    node_id: 10,
                    sequence_start: 0,
                    sequence_end: 10
                },
                GraphNode {
                    block_id: -1,
                    node_id: 10,
                    sequence_start: 10,
                    sequence_end: 20
                },
                GraphNode {
                    block_id: -1,
                    node_id: 10,
                    sequence_start: 20,
                    sequence_end: 30
                },
                GraphNode {
                    block_id: -1,
                    node_id: 20,
                    sequence_start: 30,
                    sequence_end: 40
                },
                GraphNode {
                    block_id: -1,
                    node_id: 20,
                    sequence_start: 40,
                    sequence_end: 60
                }
            ]
        )
    }

    #[test]
    fn test_get_path_graph_single_path_block() {
        let mut graph = DiGraphMap::new();
        graph.add_edge(
            GraphNode {
                block_id: 0,
                node_id: 3,
                sequence_start: 0,
                sequence_end: 1425,
            },
            GraphNode {
                block_id: 0,
                node_id: 3,
                sequence_start: 2220,
                sequence_end: 8302,
            },
            GraphEdge {
                edge_id: 1,
                chromosome_index: 0,
                phased: 0,
                source_strand: Forward,
                target_strand: Forward,
            },
        );
        graph.add_edge(
            GraphNode {
                block_id: 0,
                node_id: 3,
                sequence_start: 0,
                sequence_end: 1425,
            },
            GraphNode {
                block_id: 0,
                node_id: 3,
                sequence_start: 1425,
                sequence_end: 2220,
            },
            GraphEdge {
                edge_id: 1,
                chromosome_index: 0,
                phased: 0,
                source_strand: Forward,
                target_strand: Forward,
            },
        );
        graph.add_edge(
            GraphNode {
                block_id: 0,
                node_id: 3,
                sequence_start: 1425,
                sequence_end: 2220,
            },
            GraphNode {
                block_id: 0,
                node_id: 3,
                sequence_start: 2220,
                sequence_end: 8302,
            },
            GraphEdge {
                edge_id: 1,
                chromosome_index: 0,
                phased: 0,
                source_strand: Forward,
                target_strand: Forward,
            },
        );
        let path_blocks = vec![PathBlock {
            id: 0,
            node_id: 3,
            block_sequence: String::new(),
            sequence_start: 0,
            sequence_end: 8302,
            path_start: 0,
            path_end: 8302,
            strand: Forward,
        }];
        assert_eq!(
            get_path_nodes(&graph, &path_blocks),
            vec![
                GraphNode {
                    block_id: 0,
                    node_id: 3,
                    sequence_start: 0,
                    sequence_end: 1425
                },
                GraphNode {
                    block_id: 0,
                    node_id: 3,
                    sequence_start: 1425,
                    sequence_end: 2220
                },
                GraphNode {
                    block_id: 0,
                    node_id: 3,
                    sequence_start: 2220,
                    sequence_end: 8302
                }
            ]
        )
    }
}
