use std::collections::{HashMap, HashSet};

use intervaltree::IntervalTree;
use itertools::Itertools;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use serde::{Deserialize, Serialize};

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

impl Path {
    pub fn create(conn: &Connection, name: &str, block_group_id: i64, edge_ids: &[i64]) -> Path {
        // TODO: Should we do something if edge_ids don't match here? Suppose we have a path
        // for a block group with edges 1,2,3. And then the same path is added again with edges
        // 5,6,7, should this be an error? Should we just keep adding edges?
        let query = "INSERT INTO path (name, block_group_id) VALUES (?1, ?2) RETURNING (id)";
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
                    let query = "SELECT id from path where name = ?1 AND block_group_id = ?2;";
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
        let query = "SELECT id, block_group_id, name from path where id = ?1;";
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
        let query = "SELECT * FROM path JOIN block_group ON path.block_group_id = block_group.id WHERE block_group.collection_name = ?1";
        Path::get_paths(conn, query, vec![Value::from(collection_name.to_string())])
    }

    pub fn sequence(conn: &Connection, path: Path) -> String {
        let blocks = Path::blocks_for(conn, &path);
        blocks
            .into_iter()
            .map(|block| block.block_sequence)
            .collect::<Vec<_>>()
            .join("")
    }

    pub fn edge_pairs_to_block(
        block_id: i64,
        path: &Path,
        into: Edge,
        out_of: Edge,
        sequences_by_node_id: &HashMap<i64, Sequence>,
        current_path_length: i64,
    ) -> PathBlock {
        if into.target_node_id != out_of.source_node_id {
            panic!(
                "Consecutive edges in path {0} don't share the same sequence",
                path.id
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
                path.id
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

    pub fn blocks_for(conn: &Connection, path: &Path) -> Vec<PathBlock> {
        let edges = PathEdge::edges_for_path(conn, path.id);
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
            let block = Path::edge_pairs_to_block(
                index as i64,
                path,
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

    pub fn intervaltree_for(conn: &Connection, path: &Path) -> IntervalTree<i64, PathBlock> {
        let blocks = Path::blocks_for(conn, path);
        let tree: IntervalTree<i64, PathBlock> = blocks
            .into_iter()
            .map(|block| (block.path_start..block.path_end, block))
            .collect();
        tree
    }

    pub fn find_block_mappings(
        conn: &Connection,
        our_path: &Path,
        other_path: &Path,
    ) -> Vec<RangeMapping> {
        // Given two paths, find the overlapping parts of common nodes/blocks and return a list af
        // mappings from subranges of one path to corresponding shared subranges of the other path
        let our_blocks = Path::blocks_for(conn, our_path);
        let their_blocks = Path::blocks_for(conn, other_path);

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
        assert_eq!(Path::sequence(conn, path), "ATCGATCGAAAAAAACCCCCCCGGGGGGG");
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
        assert_eq!(Path::sequence(conn, path), "CCCCCCCGGGGGGGTTTTTTTCGATCGAT");
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
        let tree = Path::intervaltree_for(conn, &path);
        let blocks1: Vec<PathBlock> = tree.query_point(2).map(|x| x.value.clone()).collect();
        assert_eq!(blocks1.len(), 1);
        let block1 = &blocks1[0];
        assert_eq!(block1.node_id, node1_id);
        assert_eq!(block1.sequence_start, 0);
        assert_eq!(block1.sequence_end, 8);
        assert_eq!(block1.path_start, 0);
        assert_eq!(block1.path_end, 8);
        assert_eq!(block1.strand, Strand::Forward);

        let blocks2: Vec<PathBlock> = tree.query_point(12).map(|x| x.value.clone()).collect();
        assert_eq!(blocks2.len(), 1);
        let block2 = &blocks2[0];
        assert_eq!(block2.node_id, node2_id);
        assert_eq!(block2.sequence_start, 1);
        assert_eq!(block2.sequence_end, 8);
        assert_eq!(block2.path_start, 8);
        assert_eq!(block2.path_end, 15);
        assert_eq!(block2.strand, Strand::Forward);

        let blocks4: Vec<PathBlock> = tree.query_point(25).map(|x| x.value.clone()).collect();
        assert_eq!(blocks4.len(), 1);
        let block4 = &blocks4[0];
        assert_eq!(block4.node_id, node4_id);
        assert_eq!(block4.sequence_start, 1);
        assert_eq!(block4.sequence_end, 8);
        assert_eq!(block4.path_start, 22);
        assert_eq!(block4.path_end, 29);
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

        let mappings = Path::find_block_mappings(conn, &path, &path);
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

        let mappings = Path::find_block_mappings(conn, &path1, &path2);
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

        assert_eq!(Path::sequence(conn, path2.clone()), "ATCGTTTTTTTT");

        let mappings = Path::find_block_mappings(conn, &path1, &path2);
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

        assert_eq!(Path::sequence(conn, path2.clone()), "ATCGTTTTTTTTATCG");

        let mappings = Path::find_block_mappings(conn, &path1, &path2);
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

        assert_eq!(Path::sequence(conn, path2.clone()), "ATTTTTTTTTCG");

        let mappings = Path::find_block_mappings(conn, &path1, &path2);
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

        path 2:	Mimics a pure deletion
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

        assert_eq!(Path::sequence(conn, path2.clone()), "ATCG");

        let mappings = Path::find_block_mappings(conn, &path1, &path2);
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

        path 2:	Mimics a pure insertion in the middle of the two blocks
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

        assert_eq!(
            Path::sequence(conn, path2.clone()),
            "ATCGATCGAAAAAAAATTTTTTTT"
        );

        let mappings = Path::find_block_mappings(conn, &path1, &path2);
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

        path 2:	Mimics a replacement across the two blocks
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

        assert_eq!(Path::sequence(conn, path2.clone()), "ATCGAAAAAAAATTTT");

        let mappings = Path::find_block_mappings(conn, &path1, &path2);
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

        path 2:	Mimics a deletion across the two blocks
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

        assert_eq!(Path::sequence(conn, path2.clone()), "ATCGTTTT");

        let mappings = Path::find_block_mappings(conn, &path1, &path2);
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
}
