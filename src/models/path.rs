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

#[derive(Clone, Debug)]
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
        let node1_id = Node::create(conn, sequence1.hash.as_str());
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
        let node2_id = Node::create(conn, sequence2.hash.as_str());
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
        let node3_id = Node::create(conn, sequence3.hash.as_str());
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
        let node4_id = Node::create(conn, sequence4.hash.as_str());
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
        let node1_id = Node::create(conn, sequence1.hash.as_str());
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
        let node2_id = Node::create(conn, sequence2.hash.as_str());
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
        let node3_id = Node::create(conn, sequence3.hash.as_str());
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
        let node4_id = Node::create(conn, sequence4.hash.as_str());
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
        let node1_id = Node::create(conn, sequence1.hash.as_str());
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
        let node2_id = Node::create(conn, sequence2.hash.as_str());
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
        let node3_id = Node::create(conn, sequence3.hash.as_str());
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
        let node4_id = Node::create(conn, sequence4.hash.as_str());
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
}
