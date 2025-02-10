use core::ops::Range as RustRange;
use std::collections::{HashMap, HashSet};

use intervaltree::IntervalTree;
use itertools::Itertools;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection, Row};
use serde::{Deserialize, Serialize};

use crate::models::block_group::NodeIntervalBlock;
use crate::models::{
    block_group_edge::BlockGroupEdge,
    edge::Edge,
    node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID},
    path_edge::PathEdge,
    phase_layer::UNPHASED_CHROMOSOME_INDEX,
    sequence::Sequence,
    strand::Strand,
    traits::*,
};
use crate::range::{Range, RangeMapping};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct Path {
    pub id: i64,
    pub block_group_id: i64,
    pub name: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PathData {
    pub name: String,
    pub block_group_id: i64,
}

// interesting gist here: https://gist.github.com/mbhall88/cd900add6335c96127efea0e0f6a9f48, see if we
// can expand this to ambiguous bases/keep case
pub fn revcomp(seq: &str) -> String {
    String::from_utf8(
        seq.chars()
            .rev()
            .map(|c| -> u8 {
                let is_upper = c.is_ascii_uppercase();
                let rc = c as u8;
                let v = if rc == 78 {
                    // N
                    rc
                } else if rc == 110 {
                    // n
                    rc
                } else if rc & 2 != 0 {
                    // CG
                    rc ^ 4
                } else {
                    // AT
                    rc ^ 21
                };
                if is_upper {
                    v
                } else {
                    v.to_ascii_lowercase()
                }
            })
            .collect(),
    )
    .unwrap()
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PathBlock {
    pub id: i64,
    pub node_id: i64,
    pub block_sequence: String,
    pub sequence_start: i64,
    pub sequence_end: i64,
    pub path_start: i64,
    pub path_end: i64,
    pub strand: Strand,
    pub phase_layer_id: i64,
}

#[derive(Clone, Debug)]
pub struct Annotation {
    pub name: String,
    pub start: i64,
    pub end: i64,
}

impl Path {
    pub fn validate_block_group_edges(block_group_edges: &[BlockGroupEdge], block_group_id: i64) {
        for block_group_edge in block_group_edges.iter() {
            assert!(
                block_group_edge.block_group_id == block_group_id,
                "Block group edge {} doesn't belong to block group {}",
                block_group_edge.id,
                block_group_id
            );
        }

        // Two consecutive block group edges must go into and out of a node on the same phase layer
        for (block_group_edge1, block_group_edge2) in block_group_edges.iter().tuple_windows() {
            assert!(
                block_group_edge1.target_phase_layer_id == block_group_edge2.source_phase_layer_id,
                "Block group edges {} and {} don't share the same phase layer ({} vs. {})",
                block_group_edge1.id,
                block_group_edge2.id,
                block_group_edge1.target_phase_layer_id,
                block_group_edge2.source_phase_layer_id
            );
        }
    }

    pub fn validate_edges(conn: &Connection, edge_ids: &[i64]) {
        let edge_id_set = edge_ids.iter().collect::<HashSet<_>>();

        // No duplicate edges allowed
        if edge_id_set.len() != edge_ids.len() {
            println!("Duplicate edge IDs detected in path creation");
        }

        let edges = Edge::bulk_load(conn, edge_ids);
        let edges_by_id = edges
            .iter()
            .map(|edge| (edge.id, edge.clone()))
            .collect::<HashMap<_, _>>();

        // Two consecutive edges must share a node
        // Two consecutive edges must not go into and out of a node at the same coordinate
        for (edge1_id, edge2_id) in edge_ids.iter().tuple_windows() {
            let edge1 = edges_by_id.get(edge1_id).unwrap();
            let edge2 = edges_by_id.get(edge2_id).unwrap();
            assert!(
                edge1.target_node_id == edge2.source_node_id,
                "Edges {} and {} don't share the same node ({} vs. {})",
                edge1.id,
                edge2.id,
                edge1.target_node_id,
                edge2.source_node_id
            );

            assert!(
                edge1.target_coordinate < edge2.source_coordinate,
                "Source coordinate {} for edge {} is before target coordinate {} for edge {}",
                edge2.source_coordinate,
                edge2.id,
                edge1.target_coordinate,
                edge1.id
            );

            assert!(
                edge1.target_strand == edge2.source_strand,
                "Strand mismatch between consecutive edges {} and {}",
                edge1.id,
                edge2.id,
            );
        }

        // An edge shouldn't start and end at the same coordinate on the same node
        for edge_id in edge_ids {
            let edge = edges_by_id.get(edge_id).unwrap();
            assert!(
                edge.source_node_id != edge.target_node_id
                    || edge.source_coordinate != edge.target_coordinate,
                "Edge {} goes into and out of the same node at the same coordinate",
                edge.id
            );
        }
    }

    pub fn create(
        conn: &Connection,
        name: &str,
        block_group_id: i64,
        block_group_edge_ids: &[i64],
    ) -> Path {
        let block_group_edges = BlockGroupEdge::load_block_group_edges(conn, block_group_edge_ids);
        Path::validate_block_group_edges(&block_group_edges, block_group_id);
        let edge_ids = block_group_edges
            .iter()
            .map(|block_group_edge| block_group_edge.edge_id)
            .collect::<Vec<i64>>();
        Path::validate_edges(conn, &edge_ids);

        // TODO: Should we do something if edge_ids don't match here? Suppose we have a path
        // for a block group with edges 1,2,3. And then the same path is added again with edges
        // 5,6,7, should this be an error? Should we just keep adding edges?
        let query = "INSERT INTO paths (name, block_group_id) VALUES (?1, ?2) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();

        let mut rows = stmt
            .query_map((name, block_group_id), |row| {
                Ok(Path {
                    id: row.get(0)?,
                    name: name.to_string(),
                    block_group_id,
                })
            })
            .unwrap();
        let path = match rows.next().unwrap() {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, _details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    let query = "SELECT id from paths where name = ?1 AND block_group_id = ?2;";
                    Path {
                        id: conn
                            .query_row(
                                query,
                                params_from_iter(vec![
                                    Value::from(name.to_string()),
                                    Value::from(block_group_id),
                                ]),
                                |row| row.get(0),
                            )
                            .unwrap(),
                        name: name.to_string(),
                        block_group_id,
                    }
                } else {
                    panic!("something bad happened querying the database")
                }
            }
            Err(_) => {
                panic!("something bad happened querying the database")
            }
        };

        PathEdge::bulk_create(conn, path.id, block_group_edge_ids);

        path
    }

    pub fn get(conn: &Connection, path_id: i64) -> Path {
        let query = "SELECT id, block_group_id, name from paths where id = ?1;";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map((path_id,), |row| {
                Ok(Path {
                    id: row.get(0)?,
                    block_group_id: row.get(1)?,
                    name: row.get(2)?,
                })
            })
            .unwrap();
        rows.next().unwrap().unwrap()
    }

    pub fn query_for_collection(conn: &Connection, collection_name: &str) -> Vec<Path> {
        let query = "SELECT * FROM paths JOIN block_groups ON paths.block_group_id = block_groups.id WHERE block_groups.collection_name = ?1";
        Path::query(
            conn,
            query,
            rusqlite::params!(Value::from(collection_name.to_string())),
        )
    }

    pub fn query_for_collection_and_sample(
        conn: &Connection,
        collection_name: &str,
        sample_name: Option<String>,
    ) -> Vec<Path> {
        if let Some(actual_sample_name) = sample_name {
            let query = "SELECT * FROM paths JOIN block_groups ON paths.block_group_id = block_groups.id WHERE block_groups.collection_name = ?1 AND block_groups.sample_name = ?2";
            Path::query(
                conn,
                query,
                rusqlite::params!(
                    Value::from(collection_name.to_string()),
                    Value::from(actual_sample_name)
                ),
            )
        } else {
            let query = "SELECT * FROM paths JOIN block_groups ON paths.block_group_id = block_groups.id WHERE block_groups.collection_name = ?1 AND block_groups.sample_name IS NULL";
            Path::query(
                conn,
                query,
                rusqlite::params!(Value::from(collection_name.to_string())),
            )
        }
    }

    pub fn sequence(&self, conn: &Connection) -> String {
        let blocks = self.blocks(conn);
        blocks
            .into_iter()
            .map(|block| block.block_sequence)
            .collect::<Vec<_>>()
            .join("")
    }

    pub fn edge_pairs_to_block(
        &self,
        block_id: i64,
        into: Edge,
        out_of: Edge,
        phase_layer_id: i64,
        sequences_by_node_id: &HashMap<i64, Sequence>,
        current_path_length: i64,
    ) -> PathBlock {
        let sequence = sequences_by_node_id.get(&into.target_node_id).unwrap();
        let start = into.target_coordinate;
        let end = out_of.source_coordinate;

        let strand = into.target_strand;
        let block_sequence_length = end - start;

        let block_sequence = if strand == Strand::Reverse {
            revcomp(&sequence.get_sequence(start, end))
        } else {
            sequence.get_sequence(start, end)
        };

        PathBlock {
            id: block_id,
            node_id: into.target_node_id,
            block_sequence,
            sequence_start: start,
            sequence_end: end,
            path_start: current_path_length,
            path_end: current_path_length + block_sequence_length,
            strand,
            phase_layer_id,
        }
    }

    pub fn blocks(&self, conn: &Connection) -> Vec<PathBlock> {
        let block_group_edges = PathEdge::block_group_edges_for_path(conn, self.id);
        let edge_ids = block_group_edges
            .iter()
            .map(|block_group_edge| block_group_edge.edge_id)
            .collect::<Vec<i64>>();
        let edges = Edge::bulk_load(conn, &edge_ids);
        let edges_by_id = edges
            .iter()
            .map(|edge| (edge.id, edge.clone()))
            .collect::<HashMap<i64, Edge>>();
        let edges_by_block_group_edge_id = block_group_edges
            .iter()
            .map(|block_group_edge| {
                (
                    block_group_edge.id,
                    edges_by_id[&block_group_edge.edge_id].clone(),
                )
            })
            .collect::<HashMap<i64, Edge>>();

        let mut sequence_node_ids = HashSet::new();
        for edge in &edges {
            if edge.source_node_id != PATH_START_NODE_ID {
                sequence_node_ids.insert(edge.source_node_id);
            }
            if edge.target_node_id != PATH_END_NODE_ID {
                sequence_node_ids.insert(edge.target_node_id);
            }
        }
        let sequences_by_node_id = Node::get_sequences_by_node_ids(
            conn,
            &sequence_node_ids.into_iter().collect::<Vec<i64>>(),
        );

        let mut blocks = vec![];
        let mut path_length = 0;

        // NOTE: Adding a "start block" for the dedicated start sequence with a range from i64::MIN
        // to 0 makes interval tree lookups work better.  If the point being looked up is -1 (or
        // below), it will return this block.
        blocks.push(PathBlock {
            id: -1,
            node_id: PATH_START_NODE_ID,
            block_sequence: "".to_string(),
            sequence_start: 0,
            sequence_end: 0,
            path_start: i64::MIN + 1,
            path_end: 0,
            strand: Strand::Forward,
            phase_layer_id: UNPHASED_CHROMOSOME_INDEX,
        });

        for (index, (into, out_of)) in block_group_edges.into_iter().tuple_windows().enumerate() {
            let edge_in = edges_by_block_group_edge_id[&into.id].clone();
            let edge_out = edges_by_block_group_edge_id[&out_of.id].clone();
            let block = self.edge_pairs_to_block(
                index as i64,
                edge_in,
                edge_out,
                into.target_phase_layer_id,
                &sequences_by_node_id,
                path_length,
            );
            path_length += block.block_sequence.len() as i64;
            blocks.push(block);
        }

        // NOTE: Adding an "end block" for the dedicated end sequence with a range from the path
        // length to i64::MAX makes interval tree lookups work better.  If the point being looked up
        // is the path length (or higher), it will return this block.
        blocks.push(PathBlock {
            id: -2,
            node_id: PATH_END_NODE_ID,
            block_sequence: "".to_string(),
            sequence_start: 0,
            sequence_end: 0,
            path_start: path_length,
            path_end: i64::MAX - 1,
            strand: Strand::Forward,
            phase_layer_id: UNPHASED_CHROMOSOME_INDEX,
        });

        blocks
    }

    pub fn intervaltree(&self, conn: &Connection) -> IntervalTree<i64, NodeIntervalBlock> {
        let blocks = self.blocks(conn);
        let tree: IntervalTree<i64, NodeIntervalBlock> = blocks
            .into_iter()
            .map(|block| {
                (
                    block.path_start..block.path_end,
                    NodeIntervalBlock {
                        block_id: block.id,
                        node_id: block.node_id,
                        start: block.path_start,
                        end: block.path_end,
                        sequence_start: block.sequence_start,
                        sequence_end: block.sequence_end,
                        strand: block.strand,
                        phase_layer_id: 0,
                    },
                )
            })
            .collect();
        tree
    }

    pub fn find_block_mappings(&self, conn: &Connection, other_path: &Path) -> Vec<RangeMapping> {
        // Given two paths, find the overlapping parts of common nodes/blocks and return a list af
        // mappings from subranges of one path to corresponding shared subranges of the other path
        let our_blocks = self.blocks(conn);
        let their_blocks = other_path.blocks(conn);

        let our_node_ids = our_blocks
            .iter()
            .map(|block| block.node_id)
            .collect::<HashSet<i64>>();
        let their_node_ids = their_blocks
            .iter()
            .map(|block| block.node_id)
            .collect::<HashSet<i64>>();
        let common_node_ids = our_node_ids
            .intersection(&their_node_ids)
            .copied()
            .collect::<HashSet<i64>>();

        let mut our_blocks_by_node_id = HashMap::new();
        for block in our_blocks
            .iter()
            .filter(|block| common_node_ids.contains(&block.node_id))
        {
            our_blocks_by_node_id
                .entry(block.node_id)
                .or_insert(vec![])
                .push(block);
        }

        let mut their_blocks_by_node_id = HashMap::new();
        for block in their_blocks
            .iter()
            .filter(|block| common_node_ids.contains(&block.node_id))
        {
            their_blocks_by_node_id
                .entry(block.node_id)
                .or_insert(vec![])
                .push(block);
        }

        let mut mappings = vec![];
        for node_id in common_node_ids {
            let our_blocks = our_blocks_by_node_id.get(&node_id).unwrap();
            let our_sorted_blocks = our_blocks
                .clone()
                .into_iter()
                .sorted_by(|a, b| a.sequence_start.cmp(&b.sequence_start))
                .collect::<Vec<&PathBlock>>();
            let their_blocks = their_blocks_by_node_id.get(&node_id).unwrap();
            let their_sorted_blocks = their_blocks
                .clone()
                .into_iter()
                .sorted_by(|a, b| a.sequence_start.cmp(&b.sequence_start))
                .collect::<Vec<&PathBlock>>();

            for our_block in our_sorted_blocks {
                let mut their_block_index = 0;

                while their_block_index < their_sorted_blocks.len() {
                    let their_block = their_sorted_blocks[their_block_index];
                    if their_block.sequence_end <= our_block.sequence_start {
                        // If their block is before ours, move along to the next one
                        their_block_index += 1;
                    } else {
                        let our_range = Range {
                            start: our_block.sequence_start,
                            end: our_block.sequence_end,
                        };
                        let their_range = Range {
                            start: their_block.sequence_start,
                            end: their_block.sequence_end,
                        };

                        let common_ranges = our_range.overlap(&their_range);
                        if !common_ranges.is_empty() {
                            if common_ranges.len() > 1 {
                                panic!(
                                    "Found more than one common range for blocks with node {}",
                                    node_id
                                );
                            }

                            let common_range = &common_ranges[0];
                            let our_start = our_block.path_start
                                + (common_range.start - our_block.sequence_start);
                            let our_end = our_block.path_start
                                + (common_range.end - our_block.sequence_start);
                            let their_start = their_block.path_start
                                + (common_range.start - their_block.sequence_start);
                            let their_end = their_block.path_start
                                + (common_range.end - their_block.sequence_start);

                            let mapping = RangeMapping {
                                source_range: Range {
                                    start: our_start,
                                    end: our_end,
                                },
                                target_range: Range {
                                    start: their_start,
                                    end: their_end,
                                },
                            };
                            mappings.push(mapping);
                        }

                        if their_block.sequence_end < our_block.sequence_end {
                            // If their block ends before ours, move along to the next one
                            their_block_index += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        mappings
            .into_iter()
            .sorted_by(|a, b| a.source_range.start.cmp(&b.source_range.start))
            .collect::<Vec<RangeMapping>>()
    }

    pub fn propagate_annotation(
        annotation: Annotation,
        mapping_tree: &IntervalTree<i64, RangeMapping>,
        sequence_length: i64,
    ) -> Option<Annotation> {
        /*
        This method contains the core logic for propagating an annotation from one path to another.
        The core rules are:

        1. If the annotation can be fully propagated to a matching subregion of the other path,
            we propagate it

        2. If only part of the annotation can be propagated to a partial subregion of the other
            path, we propagate just that part and truncate the rest

        3. If the first and last parts of the annotation can be propagated to subregions of the
            other path (but not one or more parts of the middle of the annotation), we propagate the
            entire annotation, including across the parts that don't match those of this path
         */

        // TODO: Add support for different propagation strategies
        // TODO: Handle circular contigs
        let start = annotation.start;
        let end = annotation.end;
        let mappings: Vec<RangeMapping> = mapping_tree
            .query(RustRange { start, end })
            .map(|x| x.value.clone())
            .collect();
        if mappings.is_empty() {
            return None;
        }

        let sorted_mappings: Vec<RangeMapping> = mappings
            .into_iter()
            .sorted_by(|a, b| a.source_range.start.cmp(&b.source_range.start))
            .collect();
        let first_mapping = sorted_mappings.first().unwrap();
        let last_mapping = sorted_mappings.last().unwrap();
        let translated_start = if first_mapping.source_range.contains(start) {
            first_mapping.source_range.translate_index(
                start,
                &first_mapping.target_range,
                sequence_length,
                false,
            )
        } else {
            Ok(first_mapping.target_range.start)
        };

        let translated_end = if last_mapping.source_range.contains(end) {
            last_mapping.source_range.translate_index(
                end,
                &last_mapping.target_range,
                sequence_length,
                false,
            )
        } else {
            Ok(last_mapping.target_range.end)
        };

        if translated_start.is_err() || translated_end.is_err() {
            return None;
        }

        Some(Annotation {
            name: annotation.name,
            start: translated_start.expect("Failed to translate start"),
            end: translated_end.expect("Failed to translate end"),
        })
    }

    pub fn get_mapping_tree(
        &self,
        conn: &Connection,
        path: &Path,
    ) -> IntervalTree<i64, RangeMapping> {
        let mappings = self.find_block_mappings(conn, path);
        mappings
            .into_iter()
            .map(|mapping| {
                (
                    mapping.source_range.start..mapping.source_range.end,
                    mapping,
                )
            })
            .collect()
    }

    pub fn propagate_annotations(
        &self,
        conn: &Connection,
        path: &Path,
        annotations: Vec<Annotation>,
    ) -> Vec<Annotation> {
        let mapping_tree = self.get_mapping_tree(conn, path);
        let sequence_length = path.sequence(conn).len();
        annotations
            .into_iter()
            .filter_map(|annotation| {
                Path::propagate_annotation(annotation, &mapping_tree, sequence_length as i64)
            })
            .clone()
            .collect()
    }

    pub fn new_path_with(
        &self,
        conn: &Connection,
        path_start: i64,
        path_end: i64,
        block_group_edge_to_new_node_id: i64,
        block_group_edge_from_new_node_id: i64,
        new_node_id: i64,
    ) -> Path {
        // Creates a new path from the current one by replacing all edges between path_start and
        // path_end with the input edges that are to and from a new node
        let tree = self.intervaltree(conn);
        let block_with_start = tree.query_point(path_start).next().unwrap().value;
        let block_with_end = tree.query_point(path_end).next().unwrap().value;

        let block_group_edges = PathEdge::block_group_edges_for_path(conn, self.id);
        let edge_ids = block_group_edges
            .iter()
            .map(|block_group_edge| block_group_edge.edge_id)
            .collect::<Vec<i64>>();
        let edges = Edge::bulk_load(conn, &edge_ids);
        let edges_by_id = edges
            .iter()
            .map(|edge| (edge.id, edge.clone()))
            .collect::<HashMap<i64, Edge>>();
        let edges_by_block_group_edge_id = block_group_edges
            .iter()
            .map(|block_group_edge| {
                (
                    block_group_edge.id,
                    edges_by_id[&block_group_edge.edge_id].clone(),
                )
            })
            .collect::<HashMap<i64, Edge>>();

        let block_group_edges_by_source = block_group_edges
            .iter()
            .map(|block_group_edge| {
                let edge = &edges_by_block_group_edge_id[&block_group_edge.id];
                (
                    (edge.source_node_id, edge.source_coordinate),
                    block_group_edge,
                )
            })
            .collect::<HashMap<(i64, i64), &BlockGroupEdge>>();
        let block_group_edges_by_target = block_group_edges
            .iter()
            .map(|block_group_edge| {
                let edge = &edges_by_block_group_edge_id[&block_group_edge.id];
                (
                    (edge.target_node_id, edge.target_coordinate),
                    block_group_edge,
                )
            })
            .collect::<HashMap<(i64, i64), &BlockGroupEdge>>();
        let block_group_edge_before_new_node = block_group_edges_by_target
            .get(&(block_with_start.node_id, block_with_start.sequence_start))
            .unwrap();
        let block_group_edge_after_new_node = block_group_edges_by_source
            .get(&(block_with_end.node_id, block_with_end.sequence_end))
            .unwrap();

        let mut block_group_edge_ids = vec![];
        let mut before_new_node = true;
        let mut after_new_node = false;
        for block_group_edge in &block_group_edges {
            if before_new_node {
                block_group_edge_ids.push(block_group_edge.id);
                if block_group_edge.id == block_group_edge_before_new_node.id {
                    before_new_node = false;
                    block_group_edge_ids.push(block_group_edge_to_new_node_id);
                    block_group_edge_ids.push(block_group_edge_from_new_node_id);
                }
            } else if after_new_node {
                block_group_edge_ids.push(block_group_edge.id);
            } else if block_group_edge.id == block_group_edge_after_new_node.id {
                after_new_node = true;
                block_group_edge_ids.push(block_group_edge.id);
            }
        }

        let new_name = format!(
            "{}-start-{}-end-{}-node-{}",
            self.name, path_start, path_end, new_node_id,
        );
        Path::create(conn, &new_name, self.block_group_id, &block_group_edge_ids)
    }

    fn node_blocks_for_range(
        &self,
        intervaltree: &IntervalTree<i64, NodeIntervalBlock>,
        start: i64,
        end: i64,
    ) -> Vec<NodeIntervalBlock> {
        // TODO: Handle start/end values that are in the middle of blocks
        let node_blocks: Vec<NodeIntervalBlock> = intervaltree
            .query(RustRange { start, end })
            .map(|x| x.value)
            .sorted_by(|a, b| a.start.cmp(&b.start))
            .collect();

        if node_blocks.is_empty() {
            return vec![];
        }

        let mut result_node_blocks = vec![];
        let start_offset = if node_blocks[0].start < start {
            start - node_blocks[0].start
        } else {
            0
        };

        let mut consolidated_block = NodeIntervalBlock {
            block_id: 0,
            node_id: node_blocks[0].node_id,
            start: node_blocks[0].start + start_offset,
            end: node_blocks[0].end,
            sequence_start: node_blocks[0].sequence_start + start_offset,
            sequence_end: node_blocks[0].sequence_end,
            strand: node_blocks[0].strand,
            phase_layer_id: 0,
        };

        for block in &node_blocks[1..] {
            if consolidated_block.node_id == block.node_id && consolidated_block.end == block.start
            {
                // If the current block is immediately adjacent to the previous one (as recorded
                // in the consolidated block), extend the consolidated block
                consolidated_block = NodeIntervalBlock {
                    block_id: consolidated_block.block_id,
                    node_id: consolidated_block.node_id,
                    start: consolidated_block.start,
                    end: block.end,
                    sequence_start: consolidated_block.sequence_start,
                    sequence_end: block.sequence_end,
                    strand: consolidated_block.strand,
                    phase_layer_id: 0,
                };
            } else {
                result_node_blocks.push(consolidated_block);
                consolidated_block = *block;
            }
        }

        let end_offset = if consolidated_block.end > end {
            consolidated_block.end - end
        } else {
            0
        };

        result_node_blocks.push(NodeIntervalBlock {
            block_id: consolidated_block.block_id,
            node_id: consolidated_block.node_id,
            start: consolidated_block.start,
            end: consolidated_block.end - end_offset,
            sequence_start: consolidated_block.sequence_start,
            sequence_end: consolidated_block.sequence_end - end_offset,
            strand: consolidated_block.strand,
            phase_layer_id: 0,
        });

        result_node_blocks
    }

    pub fn node_block_partition(
        &self,
        conn: &Connection,
        ranges: Vec<Range>,
    ) -> Vec<NodeIntervalBlock> {
        let intervaltree = self.intervaltree(conn);
        let mut partitioned_nodes = vec![];
        for range in ranges {
            let node_blocks = self.node_blocks_for_range(&intervaltree, range.start, range.end);
            for node_block in &node_blocks {
                partitioned_nodes.push(*node_block);
            }
        }
        partitioned_nodes
    }
}

impl Query for Path {
    type Model = Path;
    fn process_row(row: &Row) -> Self::Model {
        Path {
            id: row.get(0).unwrap(),
            block_group_id: row.get(1).unwrap(),
            name: row.get(2).unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::models::{
        block_group::BlockGroup, block_group_edge::BlockGroupEdgeData, collection::Collection,
    };
    use crate::test_helpers::get_connection;

    #[test]
    fn test_gets_sequence() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -123,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            1,
            Strand::Forward,
        );
        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCC")
            .save(conn);
        let node3_id = Node::create(conn, sequence3.hash.as_str(), None);
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            node3_id,
            1,
            Strand::Forward,
        );
        let sequence4 = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGG")
            .save(conn);
        let node4_id = Node::create(conn, sequence4.hash.as_str(), None);
        let edge4 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node4_id,
            1,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node4_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge1.id, edge2.id, edge3.id, edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);
        assert_eq!(path.sequence(conn), "ATCGATCGAAAAAAACCCCCCCGGGGGGG");
    }

    #[test]
    fn test_gets_sequence_with_rc() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge5 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Reverse,
            PATH_END_NODE_ID,
            0,
            Strand::Reverse,
        );
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge4 = Edge::create(
            conn,
            node2_id,
            7,
            Strand::Reverse,
            node1_id,
            0,
            Strand::Reverse,
        );
        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCC")
            .save(conn);
        let node3_id = Node::create(conn, sequence3.hash.as_str(), None);
        let edge3 = Edge::create(
            conn,
            node3_id,
            7,
            Strand::Reverse,
            node2_id,
            0,
            Strand::Reverse,
        );
        let sequence4 = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGG")
            .save(conn);
        let node4_id = Node::create(conn, sequence4.hash.as_str(), None);
        let edge2 = Edge::create(
            conn,
            node4_id,
            7,
            Strand::Reverse,
            node3_id,
            0,
            Strand::Reverse,
        );
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Reverse,
            node4_id,
            0,
            Strand::Reverse,
        );

        let edge_ids = &[edge1.id, edge2.id, edge3.id, edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);
        assert_eq!(path.sequence(conn), "CCCCCCCGGGGGGGTTTTTTTCGATCGAT");
    }

    #[test]
    fn test_reverse_complement() {
        assert_eq!(revcomp("ATCCGG"), "CCGGAT");
        assert_eq!(revcomp("CNNNNA"), "TNNNNG");
        assert_eq!(revcomp("cNNgnAt"), "aTncNNg");
    }

    #[test]
    fn test_intervaltree() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            1,
            Strand::Forward,
        );
        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCC")
            .save(conn);
        let node3_id = Node::create(conn, sequence3.hash.as_str(), None);
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            node3_id,
            1,
            Strand::Forward,
        );
        let sequence4 = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGG")
            .save(conn);
        let node4_id = Node::create(conn, sequence4.hash.as_str(), None);
        let edge4 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node4_id,
            1,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node4_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge1.id, edge2.id, edge3.id, edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);
        let tree = path.intervaltree(conn);
        let blocks1: Vec<NodeIntervalBlock> = tree.query_point(2).map(|x| x.value).collect();
        assert_eq!(blocks1.len(), 1);
        let block1 = &blocks1[0];
        assert_eq!(block1.node_id, node1_id);
        assert_eq!(block1.sequence_start, 0);
        assert_eq!(block1.sequence_end, 8);
        assert_eq!(block1.start, 0);
        assert_eq!(block1.end, 8);
        assert_eq!(block1.strand, Strand::Forward);

        let blocks2: Vec<NodeIntervalBlock> = tree.query_point(12).map(|x| x.value).collect();
        assert_eq!(blocks2.len(), 1);
        let block2 = &blocks2[0];
        assert_eq!(block2.node_id, node2_id);
        assert_eq!(block2.sequence_start, 1);
        assert_eq!(block2.sequence_end, 8);
        assert_eq!(block2.start, 8);
        assert_eq!(block2.end, 15);
        assert_eq!(block2.strand, Strand::Forward);

        let blocks4: Vec<NodeIntervalBlock> = tree.query_point(25).map(|x| x.value).collect();
        assert_eq!(blocks4.len(), 1);
        let block4 = &blocks4[0];
        assert_eq!(block4.node_id, node4_id);
        assert_eq!(block4.sequence_start, 1);
        assert_eq!(block4.sequence_end, 8);
        assert_eq!(block4.start, 22);
        assert_eq!(block4.end, 29);
        assert_eq!(block4.strand, Strand::Forward);
    }

    #[test]
    fn test_gets_sequence_with_edges_into_node_middles() {
        // Tests that if the edge from the virtual start node goes into the middle of the first
        // node, and the edge to the virtual end node comes from the middle of the last node, the
        // sequence is correctly generated
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            4,
            Strand::Forward,
        );
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCC")
            .save(conn);
        let node3_id = Node::create(conn, sequence3.hash.as_str(), None);
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            node3_id,
            0,
            Strand::Forward,
        );
        let sequence4 = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGG")
            .save(conn);
        let node4_id = Node::create(conn, sequence4.hash.as_str(), None);
        let edge4 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node4_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node4_id,
            4,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = vec![edge1.id, edge2.id, edge3.id, edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path = Path::create(conn, "chr1", block_group.id, &edge_ids);
        let tree = path.intervaltree(conn);
        let blocks1: Vec<NodeIntervalBlock> = tree.query_point(2).map(|x| x.value).collect();
        assert_eq!(blocks1.len(), 1);
        let block1 = &blocks1[0];
        assert_eq!(block1.node_id, node1_id);
        assert_eq!(block1.sequence_start, 4);
        assert_eq!(block1.sequence_end, 8);
        assert_eq!(block1.start, 0);
        assert_eq!(block1.end, 4);
        assert_eq!(block1.strand, Strand::Forward);

        let blocks2: Vec<NodeIntervalBlock> = tree.query_point(10).map(|x| x.value).collect();
        assert_eq!(blocks2.len(), 1);
        let block2 = &blocks2[0];
        assert_eq!(block2.node_id, node2_id);
        assert_eq!(block2.sequence_start, 0);
        assert_eq!(block2.sequence_end, 8);
        assert_eq!(block2.start, 4);
        assert_eq!(block2.end, 12);
        assert_eq!(block2.strand, Strand::Forward);

        let blocks4: Vec<NodeIntervalBlock> = tree.query_point(22).map(|x| x.value).collect();
        assert_eq!(blocks4.len(), 1);
        let block4 = &blocks4[0];
        assert_eq!(block4.node_id, node4_id);
        assert_eq!(block4.sequence_start, 0);
        assert_eq!(block4.sequence_end, 4);
        assert_eq!(block4.start, 20);
        assert_eq!(block4.end, 24);
        assert_eq!(block4.strand, Strand::Forward);

        assert_eq!(path.sequence(conn), "ATCGAAAAAAAACCCCCCCCGGGG");
    }

    #[test]
    fn test_full_block_mapping() {
        /*
            |--------| path: 1 sequence, (0, 8)
            |ATCGATCG|
            |--------| Same path: 1 sequence, (0, 8)

            Mapping: (0, 8) -> (0, 8)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);

        let mappings = path.find_block_mappings(conn, &path);
        assert_eq!(mappings.len(), 1);
        let mapping = &mappings[0];
        assert_eq!(mapping.source_range, mapping.target_range);
        assert_eq!(mapping.source_range.start, 0);
        assert_eq!(mapping.source_range.end, 8);
        assert_eq!(mapping.target_range.start, 0);
        assert_eq!(mapping.target_range.end, 8);
    }

    #[test]
    fn test_no_block_mapping_overlap() {
        /*
            |--------| -> path 1 (one node)
            |ATCGATCG| -> sequence

            |--------| -> path 2 (one node, totally different sequence)
            |TTTTTTTT| -> other sequence

            Mappings: empty
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);

        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge3 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge4 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge3.id, edge4.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path2 = Path::create(conn, "chr2", block_group.id, &block_group_edge_ids);

        let mappings = path1.find_block_mappings(conn, &path2);
        assert_eq!(mappings.len(), 0);
    }

    #[test]
    fn test_partial_overlap_block_mapping() {
        /*
            path 1 (one node/sequence):
            |--------|
            |ATCGATCG| -> sequence (0, 8)

            path 2:
            |----| -> (0, 4)
                |--------| -> (4, 12)
            |ATCG| -> shared with path 1
                |TTTTTTTT| -> unrelated sequence

            Mapping: (0, 4) -> (0, 4)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);

        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge3 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge4 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge3.id, edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path2 = Path::create(conn, "chr2", block_group.id, &block_group_edge_ids);

        assert_eq!(path2.sequence(conn), "ATCGTTTTTTTT");

        let mappings = path1.find_block_mappings(conn, &path2);
        assert_eq!(mappings.len(), 1);
        let mapping = &mappings[0];
        assert_eq!(mapping.source_range, mapping.target_range);
        assert_eq!(mapping.source_range.start, 0);
        assert_eq!(mapping.source_range.end, 4);
        assert_eq!(mapping.target_range.start, 0);
        assert_eq!(mapping.target_range.end, 4);
    }

    #[test]
    fn test_insertion_block_mapping() {
        /*
            path 1 (one node/sequence):
            |ATCGATCG| -> sequence (0, 8)

            path 2:	Mimics a pure insertion
            |ATCG| -> (0, 4) shared with first half of path 1
                |TTTTTTTT| -> (4, 12) unrelated sequence
                        |ATCG| -> (12, 16) shared with second half of path 1

            Mappings:
            (0, 4) -> (0, 4)
            (4, 8) -> (12, 16)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids1 = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids1);

        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge4 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            node1_id,
            4,
            Strand::Forward,
        );

        let edge_ids = [edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids2 = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let bge1_id = block_group_edge_ids1[0];
        let bge4_id = block_group_edge_ids2[0];
        let bge5_id = block_group_edge_ids2[1];
        let bge2_id = block_group_edge_ids1[1];

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[bge1_id, bge4_id, bge5_id, bge2_id],
        );

        assert_eq!(path2.sequence(conn), "ATCGTTTTTTTTATCG");

        let mappings = path1.find_block_mappings(conn, &path2);
        assert_eq!(mappings.len(), 2);
        let mapping1 = &mappings[0];
        assert_eq!(mapping1.source_range, mapping1.target_range);
        assert_eq!(mapping1.source_range.start, 0);
        assert_eq!(mapping1.source_range.end, 4);
        assert_eq!(mapping1.target_range.start, 0);
        assert_eq!(mapping1.target_range.end, 4);

        let mapping2 = &mappings[1];
        assert_eq!(mapping2.source_range.start, 4);
        assert_eq!(mapping2.source_range.end, 8);
        assert_eq!(mapping2.target_range.start, 12);
        assert_eq!(mapping2.target_range.end, 16);
    }

    #[test]
    fn test_replacement_block_mapping() {
        /*
            path 1 (one node/sequence):
            |ATCGATCG| -> sequence (0, 8)

            path 2:	Mimics a replacement
            |AT| -> (0, 2) shared with first two bp of path 1
              |TTTTTTTT| -> (2, 10) unrelated sequence
                      |CG| -> (10, 12) shared with last 2 bp of path 1

            Mappings:
            (0, 2) -> (0, 2)
            (6, 8) -> (10, 12)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids1 = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids1);

        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge4 = Edge::create(
            conn,
            node1_id,
            2,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            node1_id,
            6,
            Strand::Forward,
        );

        let edge_ids = [edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids2 = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let bge1_id = block_group_edge_ids1[0];
        let bge4_id = block_group_edge_ids2[0];
        let bge5_id = block_group_edge_ids2[1];
        let bge2_id = block_group_edge_ids1[1];

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[bge1_id, bge4_id, bge5_id, bge2_id],
        );

        assert_eq!(path2.sequence(conn), "ATTTTTTTTTCG");

        let mappings = path1.find_block_mappings(conn, &path2);
        assert_eq!(mappings.len(), 2);
        let mapping1 = &mappings[0];
        assert_eq!(mapping1.source_range, mapping1.target_range);
        assert_eq!(mapping1.source_range.start, 0);
        assert_eq!(mapping1.source_range.end, 2);
        assert_eq!(mapping1.target_range.start, 0);
        assert_eq!(mapping1.target_range.end, 2);

        let mapping2 = &mappings[1];
        assert_eq!(mapping2.source_range.start, 6);
        assert_eq!(mapping2.source_range.end, 8);
        assert_eq!(mapping2.target_range.start, 10);
        assert_eq!(mapping2.target_range.end, 12);
    }

    #[test]
    fn test_deletion_block_mapping() {
        /*
            path 1 (one node/sequence):
            |ATCGATCG| -> sequence (0, 8)

            path 2: Mimics a pure deletion
            |AT| -> (0, 2) shared with first two bp of path 1
              |CG| -> (2, 4) shared with last 2 bp of path 1

            Mappings:
            (0, 2) -> (0, 2)
            (6, 8) -> (2, 4)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids1 = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids1);

        let edge4 = Edge::create(
            conn,
            node1_id,
            2,
            Strand::Forward,
            node1_id,
            6,
            Strand::Forward,
        );

        let edge_ids = [edge4.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids2 = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let bge1_id = block_group_edge_ids1[0];
        let bge4_id = block_group_edge_ids2[0];
        let bge2_id = block_group_edge_ids1[1];

        let path2 = Path::create(conn, "chr2", block_group.id, &[bge1_id, bge4_id, bge2_id]);

        assert_eq!(path2.sequence(conn), "ATCG");

        let mappings = path1.find_block_mappings(conn, &path2);
        assert_eq!(mappings.len(), 2);
        let mapping1 = &mappings[0];
        assert_eq!(mapping1.source_range, mapping1.target_range);
        assert_eq!(mapping1.source_range.start, 0);
        assert_eq!(mapping1.source_range.end, 2);
        assert_eq!(mapping1.target_range.start, 0);
        assert_eq!(mapping1.target_range.end, 2);

        let mapping2 = &mappings[1];
        assert_eq!(mapping2.source_range.start, 6);
        assert_eq!(mapping2.source_range.end, 8);
        assert_eq!(mapping2.target_range.start, 2);
        assert_eq!(mapping2.target_range.end, 4);
    }

    #[test]
    fn test_two_block_insertion_mapping() {
        /*
            path 1 (two nodes/sequences):
            |ATCGATCG| -> sequence (0, 8)
                    |TTTTTTTT| -> sequence (8, 16)

            path 2: Mimics a pure insertion in the middle of the two blocks
            |ATCGATCG| -> sequence (0, 8)
                    |AAAAAAAA| -> sequence (8, 16)
                            |TTTTTTTT| -> sequence (16, 24)

            Mappings:
            (0, 8) -> (0, 8)
            (8, 16) -> (16, 24)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id, edge3.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids1 = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids1);

        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node3_id = Node::create(conn, sequence3.hash.as_str(), None);
        let edge4 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node3_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );

        let edge_ids = [edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids2 = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let bge1_id = block_group_edge_ids1[0];
        let bge4_id = block_group_edge_ids2[0];
        let bge5_id = block_group_edge_ids2[1];
        let bge3_id = block_group_edge_ids1[2];

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[bge1_id, bge4_id, bge5_id, bge3_id],
        );

        assert_eq!(path2.sequence(conn), "ATCGATCGAAAAAAAATTTTTTTT");

        let mappings = path1.find_block_mappings(conn, &path2);
        assert_eq!(mappings.len(), 2);
        let mapping1 = &mappings[0];
        assert_eq!(mapping1.source_range, mapping1.target_range);
        assert_eq!(mapping1.source_range.start, 0);
        assert_eq!(mapping1.source_range.end, 8);
        assert_eq!(mapping1.target_range.start, 0);
        assert_eq!(mapping1.target_range.end, 8);

        let mapping2 = &mappings[1];
        assert_eq!(mapping2.source_range.start, 8);
        assert_eq!(mapping2.source_range.end, 16);
        assert_eq!(mapping2.target_range.start, 16);
        assert_eq!(mapping2.target_range.end, 24);
    }

    #[test]
    fn test_two_block_replacement_mapping() {
        /*
            path 1 (two nodes/sequences):
            |ATCGATCG| -> sequence (0, 8)
                    |TTTTTTTT| -> sequence (8, 16)

            path 2: Mimics a replacement across the two blocks
            |ATCG| -> sequence (0, 4)
                |AAAAAAAA| -> sequence (4, 12)
                        |TTTT| -> sequence (12, 16)

            Mappings:
            (0, 4) -> (0, 4)
            (12, 16) -> (12, 16)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id, edge3.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids1 = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids1);

        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node3_id = Node::create(conn, sequence3.hash.as_str(), None);
        let edge4 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node3_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node2_id,
            4,
            Strand::Forward,
        );

        let edge_ids = [edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids2 = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let bge1_id = block_group_edge_ids1[0];
        let bge4_id = block_group_edge_ids2[0];
        let bge5_id = block_group_edge_ids2[1];
        let bge3_id = block_group_edge_ids1[2];

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[bge1_id, bge4_id, bge5_id, bge3_id],
        );

        assert_eq!(path2.sequence(conn), "ATCGAAAAAAAATTTT");

        let mappings = path1.find_block_mappings(conn, &path2);
        assert_eq!(mappings.len(), 2);
        let mapping1 = &mappings[0];
        assert_eq!(mapping1.source_range, mapping1.target_range);
        assert_eq!(mapping1.source_range.start, 0);
        assert_eq!(mapping1.source_range.end, 4);
        assert_eq!(mapping1.target_range.start, 0);
        assert_eq!(mapping1.target_range.end, 4);

        let mapping2 = &mappings[1];
        assert_eq!(mapping2.source_range.start, 12);
        assert_eq!(mapping2.source_range.end, 16);
        assert_eq!(mapping2.target_range.start, 12);
        assert_eq!(mapping2.target_range.end, 16);
    }

    #[test]
    fn test_two_block_deletion_mapping() {
        /*
            path 1 (two nodes/sequences):
            |ATCGATCG| -> sequence (0, 8)
                    |TTTTTTTT| -> sequence (8, 16)

            path 2: Mimics a deletion across the two blocks
            |ATCG| -> sequence (0, 4)
                |TTTT| -> sequence (4, 8)

            Mappings:
            (0, 4) -> (0, 4)
            (12, 16) -> (4, 8)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id, edge3.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids1 = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids1);

        let edge4 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node2_id,
            4,
            Strand::Forward,
        );

        let edge_ids = [edge4.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids2 = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let bge1_id = block_group_edge_ids1[0];
        let bge4_id = block_group_edge_ids2[0];
        let bge3_id = block_group_edge_ids1[2];

        let path2 = Path::create(conn, "chr2", block_group.id, &[bge1_id, bge4_id, bge3_id]);

        assert_eq!(path2.sequence(conn), "ATCGTTTT");

        let mappings = path1.find_block_mappings(conn, &path2);
        assert_eq!(mappings.len(), 2);
        let mapping1 = &mappings[0];
        assert_eq!(mapping1.source_range, mapping1.target_range);
        assert_eq!(mapping1.source_range.start, 0);
        assert_eq!(mapping1.source_range.end, 4);
        assert_eq!(mapping1.target_range.start, 0);
        assert_eq!(mapping1.target_range.end, 4);

        let mapping2 = &mappings[1];
        assert_eq!(mapping2.source_range.start, 12);
        assert_eq!(mapping2.source_range.end, 16);
        assert_eq!(mapping2.target_range.start, 4);
        assert_eq!(mapping2.target_range.end, 8);
    }

    #[test]
    fn test_annotation_propagation_full_overlap() {
        /*
            |--------| path: 1 sequence, (0, 8)
            |ATCGATCG|
            |--------| Same path: 1 sequence, (0, 8)

            Mapping: (0, 8) -> (0, 8)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);

        let annotation = Annotation {
            name: "foo".to_string(),
            start: 0,
            end: 8,
        };
        let annotations = path.propagate_annotations(conn, &path, vec![annotation]);
        assert_eq!(annotations.len(), 1);
        let result_annotation = &annotations[0];
        assert_eq!(result_annotation.name, "foo");
        assert_eq!(result_annotation.start, 0);
        assert_eq!(result_annotation.end, 8);
    }

    #[test]
    fn test_propagate_annotations_no_overlap() {
        /*
            |--------| -> path 1 (one node)
            |ATCGATCG| -> sequence

            |--------| -> path 2 (one node, totally different sequence)
            |TTTTTTTT| -> other sequence

            Mappings: empty
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);

        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge3 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge4 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = [edge3.id, edge4.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path2 = Path::create(conn, "chr2", block_group.id, &block_group_edge_ids);

        let annotation = Annotation {
            name: "foo".to_string(),
            start: 0,
            end: 8,
        };
        let annotations = path1.propagate_annotations(conn, &path2, vec![annotation]);
        assert_eq!(annotations.len(), 0);
    }

    #[test]
    fn test_propagate_annotations_partial_overlap() {
        /*
            path 1 (one node/sequence):
            |--------|
            |ATCGATCG| -> sequence (0, 8)

            path 2:
            |----| -> (0, 4)
                |--------| -> (4, 12)
            |ATCG| -> shared with path 1
                |TTTTTTTT| -> unrelated sequence

            Mapping: (0, 4) -> (0, 4)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);

        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge3 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge4 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge3.id, edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path2 = Path::create(conn, "chr2", block_group.id, &block_group_edge_ids);

        assert_eq!(path2.sequence(conn), "ATCGTTTTTTTT");

        let annotation = Annotation {
            name: "foo".to_string(),
            start: 0,
            end: 8,
        };
        let annotations = path1.propagate_annotations(conn, &path2, vec![annotation]);
        assert_eq!(annotations.len(), 1);
        let result_annotation = &annotations[0];
        assert_eq!(result_annotation.name, "foo");
        assert_eq!(result_annotation.start, 0);
        assert_eq!(result_annotation.end, 4);
    }

    #[test]
    fn test_propagate_annotations_with_insertion() {
        /*
            path 1 (one node/sequence):
            |ATCGATCG| -> sequence (0, 8)

            path 2:	Mimics a pure insertion
            |ATCG| -> (0, 4) shared with first half of path 1
                |TTTTTTTT| -> (4, 12) unrelated sequence
                        |ATCG| -> (12, 16) shared with second half of path 1

            Mappings:
            (0, 4) -> (0, 4)
            (4, 8) -> (12, 16)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids1 = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids1);

        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge4 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            node1_id,
            4,
            Strand::Forward,
        );

        let edge_ids = [edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids2 = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let bge1_id = block_group_edge_ids1[0];
        let bge4_id = block_group_edge_ids2[0];
        let bge5_id = block_group_edge_ids2[1];
        let bge2_id = block_group_edge_ids1[1];

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[bge1_id, bge4_id, bge5_id, bge2_id],
        );

        assert_eq!(path2.sequence(conn), "ATCGTTTTTTTTATCG");

        let annotation = Annotation {
            name: "foo".to_string(),
            start: 0,
            end: 8,
        };

        let annotations = path1.propagate_annotations(conn, &path2, vec![annotation]);
        assert_eq!(annotations.len(), 1);

        // Under the default propagation strategy, the annotation is expanded to cover anything in
        // between parts it covers
        let result_annotation = &annotations[0];
        assert_eq!(result_annotation.name, "foo");
        assert_eq!(result_annotation.start, 0);
        assert_eq!(result_annotation.end, 16);
    }

    #[test]
    fn test_propagate_annotations_with_replacement() {
        /*
            path 1 (one node/sequence):
            |ATCGATCG| -> sequence (0, 8)

            path 2:	Mimics a replacement
            |AT| -> (0, 2) shared with first two bp of path 1
              |TTTTTTTT| -> (2, 10) unrelated sequence
                      |CG| -> (10, 12) shared with last 2 bp of path 1

            Mappings:
            (0, 2) -> (0, 2)
            (6, 8) -> (10, 12)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids1 = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids1);

        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge4 = Edge::create(
            conn,
            node1_id,
            2,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            node1_id,
            6,
            Strand::Forward,
        );

        let edge_ids = [edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids2 = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let bge1_id = block_group_edge_ids1[0];
        let bge4_id = block_group_edge_ids2[0];
        let bge5_id = block_group_edge_ids2[1];
        let bge2_id = block_group_edge_ids1[1];

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[bge1_id, bge4_id, bge5_id, bge2_id],
        );

        assert_eq!(path2.sequence(conn), "ATTTTTTTTTCG");

        let annotation = Annotation {
            name: "foo".to_string(),
            start: 0,
            end: 4,
        };

        let annotations = path1.propagate_annotations(conn, &path2, vec![annotation]);
        assert_eq!(annotations.len(), 1);

        // Under the default propagation strategy, the annotation is truncated
        let result_annotation = &annotations[0];
        assert_eq!(result_annotation.name, "foo");
        assert_eq!(result_annotation.start, 0);
        assert_eq!(result_annotation.end, 2);
    }

    #[test]
    fn test_propagate_annotations_with_insertion_across_two_blocks() {
        /*
            path 1 (two nodes/sequences):
            |ATCGATCG| -> sequence (0, 8)
                    |TTTTTTTT| -> sequence (8, 16)

            path 2: Mimics a pure insertion in the middle of the two blocks
            |ATCGATCG| -> sequence (0, 8)
                    |AAAAAAAA| -> sequence (8, 16)
                            |TTTTTTTT| -> sequence (16, 24)

            Mappings:
            (0, 8) -> (0, 8)
            (8, 16) -> (16, 24)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id, edge3.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids1 = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids1);

        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node3_id = Node::create(conn, sequence3.hash.as_str(), None);
        let edge4 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node3_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );

        let edge_ids = [edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids2 = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let bge1_id = block_group_edge_ids1[0];
        let bge4_id = block_group_edge_ids2[0];
        let bge5_id = block_group_edge_ids2[1];
        let bge3_id = block_group_edge_ids1[2];

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[bge1_id, bge4_id, bge5_id, bge3_id],
        );

        assert_eq!(path2.sequence(conn), "ATCGATCGAAAAAAAATTTTTTTT");

        let annotation = Annotation {
            name: "foo".to_string(),
            start: 0,
            end: 16,
        };

        let annotations = path1.propagate_annotations(conn, &path2, vec![annotation]);
        assert_eq!(annotations.len(), 1);

        // Under the default propagation strategy, the annotation is extended across the inserted
        // region
        let result_annotation = &annotations[0];
        assert_eq!(result_annotation.name, "foo");
        assert_eq!(result_annotation.start, 0);
        assert_eq!(result_annotation.end, 24);
    }

    #[test]
    fn test_propagate_annotations_with_deletion_across_two_blocks() {
        /*
            path 1 (two nodes/sequences):
            |ATCGATCG| -> sequence (0, 8)
                    |TTTTTTTT| -> sequence (8, 16)

            path 2: Mimics a deletion across the two blocks
            |ATCG| -> sequence (0, 4)
                |TTTT| -> sequence (4, 8)

            Mappings:
            (0, 4) -> (0, 4)
            (12, 16) -> (4, 8)
        */
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id, edge3.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids1 = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids1);

        let edge4 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node2_id,
            4,
            Strand::Forward,
        );

        let edge_ids = [edge4.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids2 = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let bge1_id = block_group_edge_ids1[0];
        let bge4_id = block_group_edge_ids2[0];
        let bge3_id = block_group_edge_ids1[2];

        let path2 = Path::create(conn, "chr2", block_group.id, &[bge1_id, bge4_id, bge3_id]);

        assert_eq!(path2.sequence(conn), "ATCGTTTT");

        let annotation = Annotation {
            name: "foo".to_string(),
            start: 0,
            end: 12,
        };

        let annotations = path1.propagate_annotations(conn, &path2, vec![annotation]);
        assert_eq!(annotations.len(), 1);

        // Under the default propagation strategy, the annotation is truncated
        let result_annotation = &annotations[0];
        assert_eq!(result_annotation.name, "foo");
        assert_eq!(result_annotation.start, 0);
        assert_eq!(result_annotation.end, 4);
    }

    #[test]
    fn test_new_path_with() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -123,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = [edge1.id, edge2.id, edge3.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path1 = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);
        assert_eq!(path1.sequence(conn), "ATCGATCGAAAAAAAA");

        let sequence3 = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCC")
            .save(conn);
        let node3_id = Node::create(conn, sequence3.hash.as_str(), None);
        let edge4 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node3_id,
            0,
            Strand::Forward,
        );
        let edge5 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node2_id,
            3,
            Strand::Forward,
        );

        let edge_ids = [edge4.id, edge5.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path2 = path1.new_path_with(
            conn,
            4,
            11,
            block_group_edge_ids[0],
            block_group_edge_ids[1],
            node3_id,
        );
        assert_eq!(path2.sequence(conn), "ATCGCCCCCCCCAAAAA");

        let edge6 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node3_id,
            0,
            Strand::Forward,
        );
        let edge7 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node1_id,
            7,
            Strand::Forward,
        );

        let edge_ids = [edge6.id, edge7.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path3 = path1.new_path_with(
            conn,
            4,
            7,
            block_group_edge_ids[0],
            block_group_edge_ids[1],
            node3_id,
        );
        assert_eq!(path3.sequence(conn), "ATCGCCCCCCCCGAAAAAAAA");
    }

    #[test]
    fn test_duplicate_edge_warning() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -123,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        // Should print a warning that there are duplicate edges, but continue
        let _path = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);
    }

    #[test]
    #[should_panic]
    // Panic message is something like "Block group edge 1 doesn't belong to block group 2"
    fn test_block_group_edges_must_be_in_path_block_group() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -123,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        Collection::create(conn, "test collection 2");
        let block_group2 =
            BlockGroup::create(conn, "test collection 2", None, "test block group 2");
        let edge_ids = [edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let _path = Path::create(conn, "chr1", block_group2.id, &block_group_edge_ids);
    }

    #[test]
    #[should_panic]
    // Panic message is something like "Edges 1 and 2 don't share the same node (3 vs. 4)"
    fn test_consecutive_edges_must_share_a_node() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -123,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge2 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let block_group_edges = &[
            BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: edge1.id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            },
            BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: edge2.id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            },
        ];
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, block_group_edges);

        let _path = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);
    }

    #[test]
    #[should_panic]
    // Panic message is something like "Strand mismatch between consecutive edges 1 and 2"
    fn test_consecutive_edges_must_share_the_same_strand() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -123,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge2 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Reverse,
        );

        let edge_ids = &[edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();

        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let _path = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);
    }

    #[test]
    #[should_panic]
    // Panic message is something like "Source coordinate 2 for edge 2 is before target coordinate 4 for edge 1"
    fn test_consecutive_edges_must_have_different_coordinates_on_a_node() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -123,
            Strand::Forward,
            node1_id,
            4,
            Strand::Forward,
        );
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        // Source coordinate on node 1 is before target coordinate on node1 for edge1
        let edge2 = Edge::create(
            conn,
            node1_id,
            2,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );

        let edge_ids = &[edge1.id, edge2.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let _path = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);
    }

    #[test]
    #[should_panic]
    // Panic message is something like: "Edge 1 goes into and out of the same node at the same coordinate"
    fn test_edge_does_not_start_and_end_on_same_bp() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            node1_id,
            2,
            Strand::Forward,
            node1_id,
            2,
            Strand::Forward,
        );

        let block_group_edges = &[BlockGroupEdgeData {
            block_group_id: block_group.id,
            edge_id: edge1.id,
            chromosome_index: 0,
            phased: 0,
            source_phase_layer_id: 0,
            target_phase_layer_id: 0,
        }];
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, block_group_edges);

        let _path = Path::create(conn, "chr1", block_group.id, &[block_group_edge_ids[0]]);
    }

    #[test]
    fn test_node_blocks_for_range() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -123,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge1.id, edge2.id, edge3.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);

        let path = Path::create(conn, "chr1", block_group.id, &block_group_edge_ids);

        let intervaltree = path.intervaltree(conn);

        let node_blocks1 = path.node_blocks_for_range(&intervaltree, 0, 8);
        let expected_node_blocks1 = &[NodeIntervalBlock {
            block_id: 0,
            node_id: node1_id,
            start: 0,
            end: 8,
            sequence_start: 0,
            sequence_end: 8,
            strand: Strand::Forward,
            phase_layer_id: 0,
        }];
        assert_eq!(node_blocks1, expected_node_blocks1);

        let node_blocks2 = path.node_blocks_for_range(&intervaltree, 0, 4);
        let expected_node_blocks2 = &[NodeIntervalBlock {
            block_id: 0,
            node_id: node1_id,
            start: 0,
            end: 4,
            sequence_start: 0,
            sequence_end: 4,
            strand: Strand::Forward,
            phase_layer_id: 0,
        }];
        assert_eq!(node_blocks2, expected_node_blocks2);

        let node_blocks3 = path.node_blocks_for_range(&intervaltree, 2, 6);
        let expected_node_blocks3 = &[NodeIntervalBlock {
            block_id: 0,
            node_id: node1_id,
            start: 2,
            end: 6,
            sequence_start: 2,
            sequence_end: 6,
            strand: Strand::Forward,
            phase_layer_id: 0,
        }];
        assert_eq!(node_blocks3, expected_node_blocks3);

        let node_blocks4 = path.node_blocks_for_range(&intervaltree, 3, 8);
        let expected_node_blocks4 = &[NodeIntervalBlock {
            block_id: 0,
            node_id: node1_id,
            start: 3,
            end: 8,
            sequence_start: 3,
            sequence_end: 8,
            strand: Strand::Forward,
            phase_layer_id: 0,
        }];
        assert_eq!(node_blocks4, expected_node_blocks4);

        let node_blocks5 = path.node_blocks_for_range(&intervaltree, 6, 10);
        let expected_node_blocks5 = &[
            NodeIntervalBlock {
                block_id: 0,
                node_id: node1_id,
                start: 6,
                end: 8,
                sequence_start: 6,
                sequence_end: 8,
                strand: Strand::Forward,
                phase_layer_id: 0,
            },
            NodeIntervalBlock {
                block_id: 1,
                node_id: node2_id,
                start: 8,
                end: 10,
                sequence_start: 0,
                sequence_end: 2,
                strand: Strand::Forward,
                phase_layer_id: 0,
            },
        ];
        assert_eq!(node_blocks5, expected_node_blocks5);

        let node_blocks6 = path.node_blocks_for_range(&intervaltree, 12, 16);
        let expected_node_blocks6 = &[NodeIntervalBlock {
            block_id: 0,
            node_id: node2_id,
            start: 12,
            end: 16,
            sequence_start: 4,
            sequence_end: 8,
            strand: Strand::Forward,
            phase_layer_id: 0,
        }];
        assert_eq!(node_blocks6, expected_node_blocks6);
    }

    #[test]
    fn test_node_blocks_for_range_with_node_parts() {
        let conn = &mut get_connection(None);
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1 = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let node1_id = Node::create(conn, sequence1.hash.as_str(), None);
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -123,
            Strand::Forward,
            node1_id,
            0,
            Strand::Forward,
        );
        let sequence2 = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTT")
            .save(conn);
        let node2_id = Node::create(conn, sequence2.hash.as_str(), None);
        let edge2 = Edge::create(
            conn,
            node1_id,
            5,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            node1_id,
            6,
            Strand::Forward,
        );
        let edge4 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );

        let edge_ids = &[edge1.id, edge2.id, edge3.id, edge4.id];
        let block_group_edges = edge_ids
            .iter()
            .map(|edge_id| BlockGroupEdgeData {
                block_group_id: block_group.id,
                edge_id: *edge_id,
                chromosome_index: 0,
                phased: 0,
                source_phase_layer_id: 0,
                target_phase_layer_id: 0,
            })
            .collect::<Vec<BlockGroupEdgeData>>();
        let block_group_edge_ids = BlockGroupEdge::bulk_create(conn, &block_group_edges);
        let bge1_id = block_group_edge_ids[0];
        let bge4_id = block_group_edge_ids[3];

        let path1 = Path::create(conn, "chr1.1", block_group.id, &[bge1_id, bge4_id]);
        let path2 = Path::create(conn, "chr1.2", block_group.id, &block_group_edge_ids);

        let intervaltree1 = path1.intervaltree(conn);

        let node_blocks1 = path1.node_blocks_for_range(&intervaltree1, 0, 8);
        let expected_node_blocks1 = &[NodeIntervalBlock {
            block_id: 0,
            node_id: node1_id,
            start: 0,
            end: 8,
            sequence_start: 0,
            sequence_end: 8,
            strand: Strand::Forward,
            phase_layer_id: 0,
        }];
        assert_eq!(node_blocks1, expected_node_blocks1);

        let intervaltree2 = path2.intervaltree(conn);

        let node_blocks2 = path2.node_blocks_for_range(&intervaltree2, 0, 8);
        let expected_node_blocks2 = &[
            NodeIntervalBlock {
                block_id: 0,
                node_id: node1_id,
                start: 0,
                end: 5,
                sequence_start: 0,
                sequence_end: 5,
                strand: Strand::Forward,
                phase_layer_id: 0,
            },
            NodeIntervalBlock {
                block_id: 1,
                node_id: node2_id,
                start: 5,
                end: 8,
                sequence_start: 0,
                sequence_end: 3,
                strand: Strand::Forward,
                phase_layer_id: 0,
            },
        ];
        assert_eq!(node_blocks2, expected_node_blocks2);

        let node_blocks3 = path2.node_blocks_for_range(&intervaltree2, 4, 14);
        let expected_node_blocks3 = &[
            NodeIntervalBlock {
                block_id: 0,
                node_id: node1_id,
                start: 4,
                end: 5,
                sequence_start: 4,
                sequence_end: 5,
                strand: Strand::Forward,
                phase_layer_id: 0,
            },
            NodeIntervalBlock {
                block_id: 1,
                node_id: node2_id,
                start: 5,
                end: 13,
                sequence_start: 0,
                sequence_end: 8,
                strand: Strand::Forward,
                phase_layer_id: 0,
            },
            NodeIntervalBlock {
                block_id: 2,
                node_id: node1_id,
                start: 13,
                end: 14,
                sequence_start: 6,
                sequence_end: 7,
                strand: Strand::Forward,
                phase_layer_id: 0,
            },
        ];
        assert_eq!(node_blocks3, expected_node_blocks3);
    }
}
