use crate::models::block::Block;
use crate::models::edge::Edge;
use petgraph::graphmap::DiGraphMap;
use petgraph::prelude::Dfs;
use petgraph::Direction;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};

#[derive(Debug)]
pub struct Path {
    pub id: i32,
    pub name: String,
    pub block_group_id: i32,
    pub blocks: Vec<i32>,
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

impl Path {
    pub fn create(conn: &Connection, name: &str, block_group_id: i32, blocks: Vec<i32>) -> Path {
        let query = "INSERT INTO path (name, block_group_id) VALUES (?1, ?2) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map((name, block_group_id), |row| {
                Ok(Path {
                    id: row.get(0)?,
                    name: name.to_string(),
                    block_group_id,
                    blocks: blocks.clone(),
                })
            })
            .unwrap();
        let path = rows.next().unwrap().unwrap();

        for (index, block) in blocks.iter().enumerate() {
            let next_block = blocks.get(index + 1);
            if let Some(v) = next_block {
                PathBlock::create(conn, path.id, Some(*block), Some(*v));
            } else {
                PathBlock::create(conn, path.id, Some(*block), None);
            }
        }

        path
    }

    pub fn get(conn: &mut Connection, path_id: i32) -> Path {
        let query = "SELECT id, block_group_id, name from path where id = ?1;";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map((path_id,), |row| {
                Ok(Path {
                    id: row.get(0)?,
                    block_group_id: row.get(1)?,
                    name: row.get(2)?,
                    blocks: PathBlock::get_blocks(conn, path_id),
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
                    blocks: PathBlock::get_blocks(conn, path_id),
                })
            })
            .unwrap();
        let mut paths = vec![];
        for row in rows {
            paths.push(row.unwrap());
        }
        paths
    }

    pub fn sequence(conn: &Connection, path_id: i32) -> String {
        let block_ids = PathBlock::get_blocks(conn, path_id);
        let mut sequence = "".to_string();
        for block_id in block_ids {
            let (block_sequence, strand) = Block::get_sequence(conn, block_id);
            if strand == "-" {
                sequence.push_str(&revcomp(&block_sequence));
            } else {
                sequence.push_str(&block_sequence);
            }
        }
        sequence
    }
}

#[derive(Debug)]
pub struct PathBlock {
    pub id: i32,
    pub path_id: i32,
    pub source_block_id: Option<i32>,
    pub target_block_id: Option<i32>,
}

impl PathBlock {
    pub fn create(
        conn: &Connection,
        path_id: i32,
        source_block_id: Option<i32>,
        target_block_id: Option<i32>,
    ) -> PathBlock {
        let query =
            "INSERT INTO path_blocks (path_id, source_block_id, target_block_id) VALUES (?1, ?2, ?3) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map((path_id, source_block_id, target_block_id), |row| {
                Ok(PathBlock {
                    id: row.get(0)?,
                    path_id,
                    source_block_id,
                    target_block_id,
                })
            })
            .unwrap();
        match rows.next().unwrap() {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    let query;
                    let mut placeholders = vec![path_id];
                    if let Some(s) = source_block_id {
                        if let Some(t) = target_block_id {
                            query = "SELECT id from path_blocks where path_id = ?1 AND source_block_id = ?2 AND target_block_id = ?3;";
                            placeholders.push(s);
                            placeholders.push(t);
                        } else {
                            query = "SELECT id from path_blocks where path_id = ?1 AND source_block_id = ?2 AND target_block_id is null;";
                            placeholders.push(s);
                        }
                    } else if let Some(t) = target_block_id {
                        query = "SELECT id from path_blocks where path_id = ?1 AND source_block_id is null AND target_block_id = ?2;";
                        placeholders.push(t);
                    } else {
                        panic!("No block ids passed");
                    }
                    println!("{query} {placeholders:?}");
                    PathBlock {
                        id: conn
                            .query_row(query, params_from_iter(&placeholders), |row| row.get(0))
                            .unwrap(),
                        path_id,
                        source_block_id,
                        target_block_id,
                    }
                } else {
                    panic!("something bad happened querying the database")
                }
            }
            Err(_) => {
                panic!("something bad happened querying the database")
            }
        }
    }

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<PathBlock> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(PathBlock {
                    id: row.get(0)?,
                    path_id: row.get(1)?,
                    source_block_id: row.get(2)?,
                    target_block_id: row.get(3)?,
                })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }

    pub fn update(conn: &Connection, query: &str, placeholders: Vec<Value>) {
        let mut stmt = conn.prepare(query).unwrap();
        stmt.execute(params_from_iter(placeholders)).unwrap();
    }

    pub fn get_blocks(conn: &Connection, path_id: i32) -> Vec<i32> {
        let mut blocks = vec![];
        let graph = PathBlock::blocks_to_graph(conn, path_id);
        let mut start_node = None;
        for node in graph.nodes() {
            let has_incoming = graph.neighbors_directed(node, Direction::Incoming).next();
            if has_incoming.is_none() {
                start_node = Some(node);
                break;
            }
        }
        if start_node.is_none() {
            panic!("No starting block found in path {path_id}");
        }
        let mut dfs = Dfs::new(&graph, start_node.unwrap());
        while let Some(nx) = dfs.next(&graph) {
            blocks.push(nx as i32);
        }
        blocks
    }

    pub fn blocks_to_graph(conn: &Connection, path_id: i32) -> DiGraphMap<u32, ()> {
        let query = "SELECT source_block_id, target_block_id from path_blocks where path_id = ?1;";
        let mut stmt = conn.prepare_cached(query).unwrap();
        let rows = stmt
            .query_map((path_id,), |row| {
                let source_id: Option<u32> = row.get(0).unwrap();
                let target_id: Option<u32> = row.get(1).unwrap();
                Ok((source_id, target_id))
            })
            .unwrap();
        let mut graph = DiGraphMap::new();
        for row in rows {
            let (source, target) = row.unwrap();
            if let Some(v) = source {
                graph.add_node(v);
            }
            if let Some(v) = target {
                graph.add_node(v);
            }
            if let Some(source_v) = source {
                if let Some(target_v) = target {
                    graph.add_edge(source_v, target_v, ());
                }
            }
        }
        graph
    }
}

mod tests {
    use rusqlite::Connection;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::migrations::run_migrations;
    use crate::models::{sequence::Sequence, BlockGroup, Collection};

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
        let sequence1_hash = Sequence::create(conn, "DNA", "ATCGATCG", true);
        let block1 = Block::create(conn, &sequence1_hash, block_group.id, 0, 8, "+");
        let sequence2_hash = Sequence::create(conn, "DNA", "AAAAAAAA", true);
        let block2 = Block::create(conn, &sequence2_hash, block_group.id, 1, 8, "+");
        let sequence3_hash = Sequence::create(conn, "DNA", "CCCCCCCC", true);
        let block3 = Block::create(conn, &sequence3_hash, block_group.id, 1, 8, "+");
        let sequence4_hash = Sequence::create(conn, "DNA", "GGGGGGGG", true);
        let block4 = Block::create(conn, &sequence4_hash, block_group.id, 1, 8, "+");
        Edge::create(conn, Some(block1.id), Some(block2.id), 0, 0);
        Edge::create(conn, Some(block2.id), Some(block3.id), 0, 0);
        Edge::create(conn, Some(block2.id), Some(block4.id), 0, 0);

        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            vec![block1.id, block2.id, block3.id],
        );
        assert_eq!(Path::sequence(conn, path.id), "ATCGATCGAAAAAAACCCCCCC");
    }

    #[test]
    fn test_gets_sequence_with_rc() {
        let conn = &mut get_connection();
        Collection::create(conn, "test collection");
        let block_group = BlockGroup::create(conn, "test collection", None, "test block group");
        let sequence1_hash = Sequence::create(conn, "DNA", "ATCGATCG", true);
        let block1 = Block::create(conn, &sequence1_hash, block_group.id, 0, 8, "-");
        let sequence2_hash = Sequence::create(conn, "DNA", "AAAAAAAA", true);
        let block2 = Block::create(conn, &sequence2_hash, block_group.id, 1, 8, "-");
        let sequence3_hash = Sequence::create(conn, "DNA", "CCCCCCCC", true);
        let block3 = Block::create(conn, &sequence3_hash, block_group.id, 1, 8, "-");
        let sequence4_hash = Sequence::create(conn, "DNA", "GGGGGGGG", true);
        let block4 = Block::create(conn, &sequence4_hash, block_group.id, 1, 8, "-");
        Edge::create(conn, Some(block1.id), Some(block2.id), 0, 0);
        Edge::create(conn, Some(block2.id), Some(block3.id), 0, 0);
        Edge::create(conn, Some(block1.id), Some(block4.id), 0, 0);

        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            vec![block3.id, block2.id, block1.id],
        );
        assert_eq!(Path::sequence(conn, path.id), "GGGGGGGTTTTTTTCGATCGAT");
    }

    #[test]
    fn test_reverse_complement() {
        assert_eq!(revcomp("ATCCGG"), "CCGGAT");
        assert_eq!(revcomp("CNNNNA"), "TNNNNG");
        assert_eq!(revcomp("cNNgnAt"), "aTncNNg");
    }
}
