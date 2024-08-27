use crate::models::{edge::Edge, path_edge::PathEdge, sequence::Sequence};
use intervaltree::IntervalTree;
use itertools::Itertools;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Path {
    pub id: i32,
    pub name: String,
    pub block_group_id: i32,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PathData {
    pub name: String,
    pub block_group_id: i32,
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
pub struct NewBlock {
    pub id: i32,
    pub sequence: Sequence,
    pub block_sequence: String,
    pub sequence_start: i32,
    pub sequence_end: i32,
    pub path_start: i32,
    pub path_end: i32,
    pub strand: String,
}

impl Path {
    pub fn create(conn: &Connection, name: &str, block_group_id: i32, edge_ids: Vec<i32>) -> Path {
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
        let path = rows.next().unwrap().unwrap();

        for (index, edge_id) in edge_ids.iter().enumerate() {
            PathEdge::create(conn, path.id, index.try_into().unwrap(), *edge_id);
        }

        path
    }

    pub fn get(conn: &Connection, path_id: i32) -> Path {
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

    pub fn sequence(conn: &Connection, path: Path) -> String {
        let blocks = Path::blocks_for(conn, &path);
        blocks
            .into_iter()
            .map(|block| block.block_sequence)
            .collect::<Vec<_>>()
            .join("")
    }

    pub fn edge_pairs_to_block(
        block_id: i32,
        path: &Path,
        into: Edge,
        out_of: Edge,
        sequences_by_hash: &HashMap<String, Sequence>,
        current_path_length: i32,
    ) -> NewBlock {
        if into.target_hash != out_of.source_hash {
            panic!(
                "Consecutive edges in path {0} don't share the same block",
                path.id
            );
        }

        let sequence = sequences_by_hash.get(&into.target_hash).unwrap();
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

        let block_sequence = if strand == "-" {
            revcomp(&sequence.get_sequence(start, end))
        } else {
            sequence.get_sequence(start, end)
        };

        NewBlock {
            id: block_id,
            sequence: sequence.clone(),
            block_sequence,
            sequence_start: start,
            sequence_end: end,
            path_start: current_path_length,
            path_end: current_path_length + block_sequence_length,
            strand: strand.to_string(),
        }
    }

    pub fn blocks_for(conn: &Connection, path: &Path) -> Vec<NewBlock> {
        let edges = PathEdge::edges_for(conn, path.id);
        let mut sequence_hashes = HashSet::new();
        for edge in &edges {
            if edge.source_hash != Edge::PATH_START_HASH {
                sequence_hashes.insert(edge.source_hash.clone());
            }
            if edge.target_hash != Edge::PATH_END_HASH {
                sequence_hashes.insert(edge.target_hash.clone());
            }
        }
        let sequences_by_hash =
            Sequence::sequences_by_hash(conn, sequence_hashes.into_iter().collect());

        let mut blocks = vec![];
        let mut path_length = 0;
        for (index, (into, out_of)) in edges.into_iter().tuple_windows().enumerate() {
            let block = Path::edge_pairs_to_block(
                index as i32,
                path,
                into,
                out_of,
                &sequences_by_hash,
                path_length,
            );
            path_length += block.block_sequence.len() as i32;
            blocks.push(block);
        }
        blocks
    }

    pub fn intervaltree_for(conn: &Connection, path: &Path) -> IntervalTree<i32, NewBlock> {
        let blocks = Path::blocks_for(conn, path);
        let tree: IntervalTree<i32, NewBlock> = blocks
            .into_iter()
            .map(|block| (block.path_start..block.path_end, block))
            .collect();
        tree
    }
}

mod tests {
    use rusqlite::Connection;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::migrations::run_migrations;
    use crate::models::{block_group::BlockGroup, sequence::NewSequence, Collection};

    fn get_connection() -> Connection {
        let mut conn = Connection::open_in_memory()
            .unwrap_or_else(|_| panic!("Error opening in memory test db"));
        rusqlite::vtab::array::load_module(&conn).unwrap();
        run_migrations(&mut conn);
        conn
    }

    #[test]
    fn test_gets_sequence() {
        let conn = &mut get_connection();
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let edge1 = Edge::create(
            conn,
            Edge::PATH_START_HASH.to_string(),
            -123,
            "+".to_string(),
            sequence1_hash.clone(),
            0,
            "+".to_string(),
            0,
            0,
        );
        let sequence2_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let edge2 = Edge::create(
            conn,
            sequence1_hash.clone(),
            8,
            "+".to_string(),
            sequence2_hash.clone(),
            1,
            "+".to_string(),
            0,
            0,
        );
        let sequence3_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCC")
            .save(conn);
        let edge3 = Edge::create(
            conn,
            sequence2_hash.clone(),
            8,
            "+".to_string(),
            sequence3_hash.clone(),
            1,
            "+".to_string(),
            0,
            0,
        );
        let sequence4_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGG")
            .save(conn);
        let edge4 = Edge::create(
            conn,
            sequence3_hash.clone(),
            8,
            "+".to_string(),
            sequence4_hash.clone(),
            1,
            "+".to_string(),
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            sequence4_hash.clone(),
            8,
            "+".to_string(),
            Edge::PATH_END_HASH.to_string(),
            -1,
            "+".to_string(),
            0,
            0,
        );

        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            vec![edge1.id, edge2.id, edge3.id, edge4.id, edge5.id],
        );
        assert_eq!(Path::sequence(conn, path), "ATCGATCGAAAAAAACCCCCCCGGGGGGG");
    }

    #[test]
    fn test_gets_sequence_with_rc() {
        let conn = &mut get_connection();
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let edge5 = Edge::create(
            conn,
            sequence1_hash.clone(),
            8,
            "-".to_string(),
            Edge::PATH_END_HASH.to_string(),
            0,
            "-".to_string(),
            0,
            0,
        );
        let sequence2_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let edge4 = Edge::create(
            conn,
            sequence2_hash.clone(),
            7,
            "-".to_string(),
            sequence1_hash.clone(),
            0,
            "-".to_string(),
            0,
            0,
        );
        let sequence3_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCC")
            .save(conn);
        let edge3 = Edge::create(
            conn,
            sequence3_hash.clone(),
            7,
            "-".to_string(),
            sequence2_hash.clone(),
            0,
            "-".to_string(),
            0,
            0,
        );
        let sequence4_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGG")
            .save(conn);
        let edge2 = Edge::create(
            conn,
            sequence4_hash.clone(),
            7,
            "-".to_string(),
            sequence3_hash.clone(),
            0,
            "-".to_string(),
            0,
            0,
        );
        let edge1 = Edge::create(
            conn,
            Edge::PATH_START_HASH.to_string(),
            -1,
            "-".to_string(),
            sequence4_hash.clone(),
            0,
            "-".to_string(),
            0,
            0,
        );

        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            vec![edge1.id, edge2.id, edge3.id, edge4.id, edge5.id],
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
        let conn = &mut get_connection();
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCG")
            .save(conn);
        let edge1 = Edge::create(
            conn,
            Edge::PATH_START_HASH.to_string(),
            -1,
            "+".to_string(),
            sequence1_hash.clone(),
            0,
            "+".to_string(),
            0,
            0,
        );
        let sequence2_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAA")
            .save(conn);
        let edge2 = Edge::create(
            conn,
            sequence1_hash.clone(),
            8,
            "+".to_string(),
            sequence2_hash.clone(),
            1,
            "+".to_string(),
            0,
            0,
        );
        let sequence3_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCC")
            .save(conn);
        let edge3 = Edge::create(
            conn,
            sequence2_hash.clone(),
            8,
            "+".to_string(),
            sequence3_hash.clone(),
            1,
            "+".to_string(),
            0,
            0,
        );
        let sequence4_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGG")
            .save(conn);
        let edge4 = Edge::create(
            conn,
            sequence3_hash.clone(),
            8,
            "+".to_string(),
            sequence4_hash.clone(),
            1,
            "+".to_string(),
            0,
            0,
        );
        let edge5 = Edge::create(
            conn,
            sequence4_hash.clone(),
            8,
            "+".to_string(),
            Edge::PATH_END_HASH.to_string(),
            -1,
            "+".to_string(),
            0,
            0,
        );

        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            vec![edge1.id, edge2.id, edge3.id, edge4.id, edge5.id],
        );
        let tree = Path::intervaltree_for(conn, &path);
        let blocks1: Vec<_> = tree.query_point(2).map(|x| x.value.clone()).collect();
        assert_eq!(blocks1.len(), 1);
        let block1 = &blocks1[0];
        assert_eq!(block1.sequence.hash, sequence1_hash);
        assert_eq!(block1.sequence_start, 0);
        assert_eq!(block1.sequence_end, 8);
        assert_eq!(block1.path_start, 0);
        assert_eq!(block1.path_end, 8);
        assert_eq!(block1.strand, "+");

        let blocks2: Vec<_> = tree.query_point(12).map(|x| x.value.clone()).collect();
        assert_eq!(blocks2.len(), 1);
        let block2 = &blocks2[0];
        assert_eq!(block2.sequence.hash, sequence2_hash);
        assert_eq!(block2.sequence_start, 1);
        assert_eq!(block2.sequence_end, 8);
        assert_eq!(block2.path_start, 8);
        assert_eq!(block2.path_end, 15);
        assert_eq!(block2.strand, "+");

        let blocks4: Vec<_> = tree.query_point(25).map(|x| x.value.clone()).collect();
        assert_eq!(blocks4.len(), 1);
        let block4 = &blocks4[0];
        assert_eq!(block4.sequence.hash, sequence4_hash);
        assert_eq!(block4.sequence_start, 1);
        assert_eq!(block4.sequence_end, 8);
        assert_eq!(block4.path_start, 22);
        assert_eq!(block4.path_end, 29);
        assert_eq!(block4.strand, "+");
    }
}
