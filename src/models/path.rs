use core::ops::Range as RustRange;
use std::collections::{HashMap, HashSet};

use intervaltree::IntervalTree;
use itertools::Itertools;
use rusqlite::types::Value;
use rusqlite::Params;
use rusqlite::{params_from_iter, Connection};
use serde::{Deserialize, Serialize};

use crate::models::block_group::NodeIntervalBlock;
use crate::models::{
    edge::Edge,
    node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID},
    path_edge::PathEdge,
    sequence::Sequence,
    strand::Strand,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathBlock {
    pub id: i64,
    pub node_id: i64,
    pub block_sequence: String,
    pub sequence_start: i64,
    pub sequence_end: i64,
    pub path_start: i64,
    pub path_end: i64,
    pub strand: Strand,
}

#[derive(Clone, Debug)]
pub struct Annotation {
    pub name: String,
    pub start: i64,
    pub end: i64,
}

impl Path {
    pub fn create(conn: &Connection, name: &str, block_group_id: i64, edge_ids: &[i64]) -> Path {
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

        PathEdge::bulk_create(conn, path.id, edge_ids);

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

    pub fn full_query(conn: &Connection, query: &str, params: impl Params) -> Vec<Path> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params, |row| {
                let path_id = row.get(0).unwrap();
                Ok(Path {
                    id: path_id,
                    block_group_id: row.get(1)?,
                    name: row.get(2)?,
                })
            })
            .unwrap();
        let mut paths = vec![];
        for row in rows {
            paths.push(row.unwrap());
        }
        paths
    }

    pub fn get_paths(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<Path> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                let path_id = row.get(0).unwrap();
                Ok(Path {
                    id: path_id,
                    block_group_id: row.get(1)?,
                    name: row.get(2)?,
                })
            })
            .unwrap();
        let mut paths = vec![];
        for row in rows {
            paths.push(row.unwrap());
        }
        paths
    }

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<Path> {
        Path::get_paths(conn, query, placeholders)
    }

    pub fn get_paths_for_collection(conn: &Connection, collection_name: &str) -> Vec<Path> {
        let query = "SELECT * FROM paths JOIN block_groups ON paths.block_group_id = block_groups.id WHERE block_groups.collection_name = ?1";
        Path::get_paths(conn, query, vec![Value::from(collection_name.to_string())])
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
        sequences_by_node_id: &HashMap<i64, Sequence>,
        current_path_length: i64,
    ) -> PathBlock {
        if into.target_node_id != out_of.source_node_id {
            panic!(
                "Consecutive edges in path {0} don't share the same sequence",
                self.id
            );
        }

        let sequence = sequences_by_node_id.get(&into.target_node_id).unwrap();
        let start = into.target_coordinate;
        let end = out_of.source_coordinate;

        let strand;
        let block_sequence_length;

        if into.target_strand == out_of.source_strand {
            strand = into.target_strand;
            block_sequence_length = end - start;
        } else {
            panic!(
                "Edge pair with target_strand/source_strand mismatch for path {}",
                self.id
            );
        }

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
        }
    }

    pub fn blocks(&self, conn: &Connection) -> Vec<PathBlock> {
        let edges = PathEdge::edges_for_path(conn, self.id);
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
            sequence_node_ids.into_iter().collect::<Vec<i64>>(),
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
        });

        for (index, (into, out_of)) in edges.into_iter().tuple_windows().enumerate() {
            let block = self.edge_pairs_to_block(
                index as i64,
                into,
                out_of,
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

    fn propagate_annotation(
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

    pub fn propagate_annotations(
        &self,
        conn: &Connection,
        path: &Path,
        annotations: Vec<Annotation>,
    ) -> Vec<Annotation> {
        let mappings = self.find_block_mappings(conn, path);
        let sequence_length = path.sequence(conn).len();
        let mapping_tree: IntervalTree<i64, RangeMapping> = mappings
            .into_iter()
            .map(|mapping| {
                (
                    mapping.source_range.start..mapping.source_range.end,
                    mapping,
                )
            })
            .collect();

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
        edge_to_new_node: &Edge,
        edge_from_new_node: &Edge,
    ) -> Path {
        // Creates a new path from the current one by replacing all edges between path_start and
        // path_end with the input edges that are to and from a new node
        let tree = self.intervaltree(conn);
        let block_with_start = tree.query_point(path_start).next().unwrap().value.clone();
        let block_with_end = tree.query_point(path_end).next().unwrap().value.clone();

        let edges = PathEdge::edges_for_path(conn, self.id);
        let edges_by_source = edges
            .iter()
            .map(|edge| ((edge.source_node_id, edge.source_coordinate), edge))
            .collect::<HashMap<(i64, i64), &Edge>>();
        let edges_by_target = edges
            .iter()
            .map(|edge| ((edge.target_node_id, edge.target_coordinate), edge))
            .collect::<HashMap<(i64, i64), &Edge>>();
        let edge_before_new_node = edges_by_target
            .get(&(block_with_start.node_id, block_with_start.sequence_start))
            .unwrap();
        let edge_after_new_node = edges_by_source
            .get(&(block_with_end.node_id, block_with_end.sequence_end))
            .unwrap();

        let mut new_edge_ids = vec![];
        let mut before_new_node = true;
        let mut after_new_node = false;
        for edge in &edges {
            if before_new_node {
                new_edge_ids.push(edge.id);
                if edge.id == edge_before_new_node.id {
                    before_new_node = false;
                    new_edge_ids.push(edge_to_new_node.id);
                    new_edge_ids.push(edge_from_new_node.id);
                }
            } else if after_new_node {
                new_edge_ids.push(edge.id);
            } else if edge.id == edge_after_new_node.id {
                after_new_node = true;
                new_edge_ids.push(edge.id);
            }
        }

        let new_name = format!(
            "{}-start-{}-end-{}-node-{}",
            self.name, path_start, path_end, edge_to_new_node.target_node_id
        );
        Path::create(conn, &new_name, self.block_group_id, &new_edge_ids)
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::models::{block_group::BlockGroup, collection::Collection};
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
            0,
            0,
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
            0,
            0,
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
            0,
            0,
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
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            node4_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            &[edge1.id, edge2.id, edge3.id, edge4.id, edge5.id],
        );
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
            0,
            0,
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
            0,
            0,
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
            0,
            0,
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
            0,
            0,
        );
        let edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Reverse,
            node4_id,
            0,
            Strand::Reverse,
            0,
            0,
        );

        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            &[edge1.id, edge2.id, edge3.id, edge4.id, edge5.id],
        );
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
            0,
            0,
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
            0,
            0,
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
            0,
            0,
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
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            node4_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            &[edge1.id, edge2.id, edge3.id, edge4.id, edge5.id],
        );
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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path = Path::create(conn, "chr1", block_group.id, &[edge1.id, edge2.id]);

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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(conn, "chr1", block_group.id, &[edge1.id, edge2.id]);

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
            0,
            0,
        );
        let edge4 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(conn, "chr2", block_group.id, &[edge3.id, edge4.id]);

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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(conn, "chr1", block_group.id, &[edge1.id, edge2.id]);

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
            0,
            0,
        );
        let edge4 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[edge3.id, edge4.id, edge5.id],
        );

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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(conn, "chr1", block_group.id, &[edge1.id, edge2.id]);

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
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            node1_id,
            4,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[edge1.id, edge4.id, edge5.id, edge2.id],
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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(conn, "chr1", block_group.id, &[edge1.id, edge2.id]);

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
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            node1_id,
            6,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[edge1.id, edge4.id, edge5.id, edge2.id],
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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(conn, "chr1", block_group.id, &[edge1.id, edge2.id]);

        let edge4 = Edge::create(
            conn,
            node1_id,
            2,
            Strand::Forward,
            node1_id,
            6,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[edge1.id, edge4.id, edge2.id],
        );

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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(
            conn,
            "chr1",
            block_group.id,
            &[edge1.id, edge2.id, edge3.id],
        );

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
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[edge1.id, edge4.id, edge5.id, edge3.id],
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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(
            conn,
            "chr1",
            block_group.id,
            &[edge1.id, edge2.id, edge3.id],
        );

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
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node2_id,
            4,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[edge1.id, edge4.id, edge5.id, edge3.id],
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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(
            conn,
            "chr1",
            block_group.id,
            &[edge1.id, edge2.id, edge3.id],
        );

        let edge4 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node2_id,
            4,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[edge1.id, edge4.id, edge3.id],
        );

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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path = Path::create(conn, "chr1", block_group.id, &[edge1.id, edge2.id]);

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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(conn, "chr1", block_group.id, &[edge1.id, edge2.id]);

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
            0,
            0,
        );
        let edge4 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(conn, "chr2", block_group.id, &[edge3.id, edge4.id]);

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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(conn, "chr1", block_group.id, &[edge1.id, edge2.id]);

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
            0,
            0,
        );
        let edge4 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[edge3.id, edge4.id, edge5.id],
        );

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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(conn, "chr1", block_group.id, &[edge1.id, edge2.id]);

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
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            node1_id,
            4,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[edge1.id, edge4.id, edge5.id, edge2.id],
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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(conn, "chr1", block_group.id, &[edge1.id, edge2.id]);

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
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            node1_id,
            6,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[edge1.id, edge4.id, edge5.id, edge2.id],
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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(
            conn,
            "chr1",
            block_group.id,
            &[edge1.id, edge2.id, edge3.id],
        );

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
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[edge1.id, edge4.id, edge5.id, edge3.id],
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
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            node1_id,
            8,
            Strand::Forward,
            node2_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(
            conn,
            "chr1",
            block_group.id,
            &[edge1.id, edge2.id, edge3.id],
        );

        let edge4 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node2_id,
            4,
            Strand::Forward,
            0,
            0,
        );

        let path2 = Path::create(
            conn,
            "chr2",
            block_group.id,
            &[edge1.id, edge4.id, edge3.id],
        );

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
            0,
            0,
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
            0,
            0,
        );
        let edge3 = Edge::create(
            conn,
            node2_id,
            8,
            Strand::Forward,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
            0,
            0,
        );

        let path1 = Path::create(
            conn,
            "chr1",
            block_group.id,
            &[edge1.id, edge2.id, edge3.id],
        );
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
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node2_id,
            3,
            Strand::Forward,
            0,
            0,
        );

        let path2 = path1.new_path_with(conn, 4, 11, &edge4, &edge5);
        assert_eq!(path2.sequence(conn), "ATCGCCCCCCCCAAAAA");

        let edge6 = Edge::create(
            conn,
            node1_id,
            4,
            Strand::Forward,
            node3_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge7 = Edge::create(
            conn,
            node3_id,
            8,
            Strand::Forward,
            node1_id,
            7,
            Strand::Forward,
            0,
            0,
        );

        let path3 = path1.new_path_with(conn, 4, 7, &edge6, &edge7);
        assert_eq!(path3.sequence(conn), "ATCGCCCCCCCCGAAAAAAAA");
    }
}
