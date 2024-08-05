use crate::models::Path;
use rusqlite::{params_from_iter, types::Value, Connection};

use crate::models::edge::{Edge, UpdatedEdge};
use crate::models::path::PathBlock;

#[derive(Debug)]
pub struct Block {
    pub id: i32,
    pub sequence_hash: String,
    pub block_group_id: i32,
    pub start: i32,
    pub end: i32,
    pub strand: String,
}

impl Block {
    pub fn create(
        conn: &Connection,
        hash: &String,
        block_group_id: i32,
        start: i32,
        end: i32,
        strand: &String,
    ) -> Block {
        let mut stmt = conn
            .prepare_cached("INSERT INTO block (sequence_hash, block_group_id, start, end, strand) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING *")
            .unwrap();
        match stmt.query_row((hash, block_group_id, start, end, strand), |row| {
            Ok(Block {
                id: row.get(0)?,
                sequence_hash: row.get(1)?,
                block_group_id: row.get(2)?,
                start: row.get(3)?,
                end: row.get(4)?,
                strand: row.get(5)?,
            })
        }) {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, _details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    Block {
                        id: conn
                            .query_row(
                                "select id from block where sequence_hash = ?1 AND block_group_id = ?2 AND start = ?3 AND end = ?4 AND strand = ?5;",
                                (hash, block_group_id, start, end, strand),
                                |row| row.get(0),
                            )
                            .unwrap(),
                        sequence_hash: hash.clone(),
                        block_group_id,
                        start,
                        end,
                        strand: strand.clone(),
                    }
                } else {
                    panic!("something bad happened querying the database")
                }
            }
            Err(_e) => {
                panic!("failure in making block {_e}")
            }
        }
    }

    pub fn delete(conn: &Connection, block_id: i32) {
        let mut stmt = conn
            .prepare_cached("DELETE from block where id = ?1")
            .unwrap();
        stmt.execute((block_id,)).unwrap();
    }

    pub fn edges_into(conn: &Connection, block_id: i32) -> Vec<Edge> {
        let edge_query = "select id, source_id, target_id, chromosome_index, phased from edges where target_id = ?1;";
        let mut stmt = conn.prepare_cached(edge_query).unwrap();

        let mut edges: Vec<Edge> = vec![];
        let mut it = stmt.query([block_id]).unwrap();
        let mut row = it.next().unwrap();
        while row.is_some() {
            let edge = row.unwrap();
            let edge_id: i32 = edge.get(0).unwrap();
            let source_block_id: Option<i32> = edge.get(1).unwrap();
            let target_block_id: Option<i32> = edge.get(2).unwrap();
            let chromosome_index: i32 = edge.get(3).unwrap();
            let phased: i32 = edge.get(4).unwrap();
            edges.push(Edge {
                id: edge_id,
                source_id: source_block_id,
                target_id: target_block_id,
                chromosome_index,
                phased,
            });
            row = it.next().unwrap();
        }

        edges
    }

    pub fn edges_out_of(conn: &Connection, block_id: i32) -> Vec<Edge> {
        let edge_query = "select id, source_id, target_id, chromosome_index, phased from edges where source_id = ?1;";
        let mut stmt = conn.prepare_cached(edge_query).unwrap();

        let mut edges: Vec<Edge> = vec![];
        let mut it = stmt.query([block_id]).unwrap();
        let mut row = it.next().unwrap();
        while row.is_some() {
            let edge = row.unwrap();
            let edge_id: i32 = edge.get(0).unwrap();
            let source_block_id: Option<i32> = edge.get(1).unwrap();
            let target_block_id: Option<i32> = edge.get(2).unwrap();
            let chromosome_index: i32 = edge.get(3).unwrap();
            let phased: i32 = edge.get(4).unwrap();
            edges.push(Edge {
                id: edge_id,
                source_id: source_block_id,
                target_id: target_block_id,
                chromosome_index,
                phased,
            });
            row = it.next().unwrap();
        }

        edges
    }

    pub fn split(
        conn: &Connection,
        block: &Block,
        coordinate: i32,
        chromosome_index: i32,
        phased: i32,
    ) -> Option<(Block, Block)> {
        println!("splitting block {id} at {coordinate}", id = block.id);
        if coordinate < block.start || coordinate >= block.end {
            println!(
                "Coordinate {coordinate} is out of block {block_id} bounds ({start}, {end})",
                start = block.start,
                end = block.end,
                block_id = block.id
            );
            return None;
        }
        let new_left_block = Block::create(
            conn,
            &block.sequence_hash,
            block.block_group_id,
            block.start,
            coordinate,
            &block.strand,
        );
        let new_right_block = Block::create(
            conn,
            &block.sequence_hash,
            block.block_group_id,
            coordinate,
            block.end,
            &block.strand,
        );

        let mut replacement_edges: Vec<UpdatedEdge> = vec![];

        let edges_into = Block::edges_into(conn, block.id);

        for edge in edges_into.iter() {
            replacement_edges.push(UpdatedEdge {
                id: edge.id,
                new_source_id: edge.source_id,
                new_target_id: Some(new_left_block.id),
            });
        }

        let edges_out_of = Block::edges_out_of(conn, block.id);

        for edge in edges_out_of.iter() {
            replacement_edges.push(UpdatedEdge {
                id: edge.id,
                new_source_id: Some(new_right_block.id),
                new_target_id: edge.target_id,
            });
        }

        println!("impacted edges {edges_into:?} {edges_out_of:?}");

        Edge::create(
            conn,
            Some(new_left_block.id),
            Some(new_right_block.id),
            chromosome_index,
            phased,
        );

        Edge::bulk_update(conn, replacement_edges);

        // replace paths using this block
        let impacted_path_blocks = PathBlock::query(
            conn,
            "select * from path_blocks where source_block_id = ?1 OR target_block_id = ?1",
            vec![Value::from(block.id)],
        );

        for path_block in impacted_path_blocks {
            let path_id = path_block.path_id;
            PathBlock::create(
                conn,
                path_id,
                Some(new_left_block.id),
                Some(new_right_block.id),
            );
            if let Some(source_block_id) = path_block.source_block_id {
                if source_block_id == block.id {
                    println!("updating {path_block:?} with {new_right_block:?}");
                    PathBlock::update(
                        conn,
                        "update path_blocks set source_block_id = ?2 where id = ?1",
                        vec![Value::from(path_block.id), Value::from(new_right_block.id)],
                    );
                }
            }
            if let Some(target_block_id) = path_block.target_block_id {
                if target_block_id == block.id {
                    PathBlock::update(
                        conn,
                        "update path_blocks set target_block_id = ?2 where id = ?1",
                        vec![Value::from(path_block.id), Value::from(new_left_block.id)],
                    );
                }
            }
        }

        // TODO: Delete existing block? -- leave to caller atm

        Some((new_left_block, new_right_block))
    }

    pub fn get_blocks(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<Block> {
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(Block {
                    id: row.get(0)?,
                    sequence_hash: row.get(1)?,
                    block_group_id: row.get(2)?,
                    start: row.get(3)?,
                    end: row.get(4)?,
                    strand: row.get(5)?,
                })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }

    pub fn get_sequence(conn: &Connection, block_id: i32) -> (String, String) {
        let mut stmt = conn.prepare_cached("select substr(sequence.sequence, block.start + 1, block.end - block.start) as sequence, block.strand from sequence left join block on (block.sequence_hash = sequence.hash) where block.id = ?1").unwrap();
        let mut rows = stmt
            .query_map((block_id,), |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();
        rows.next().unwrap().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use std::collections::HashSet;
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
    fn test_edges_into() {
        let conn = &mut get_connection();
        Collection::create(conn, &"test collection".to_string());
        let block_group = BlockGroup::create(
            conn,
            &"test collection".to_string(),
            None,
            &"test block group".to_string(),
        );
        let sequence1_hash =
            Sequence::create(conn, "DNA".to_string(), &"ATCGATCG".to_string(), true);
        let block1 = Block::create(
            conn,
            &sequence1_hash,
            block_group.id,
            0,
            8,
            &"+".to_string(),
        );
        let sequence2_hash =
            Sequence::create(conn, "DNA".to_string(), &"AAAAAAAA".to_string(), true);
        let block2 = Block::create(
            conn,
            &sequence2_hash,
            block_group.id,
            1,
            8,
            &"+".to_string(),
        );
        let sequence3_hash =
            Sequence::create(conn, "DNA".to_string(), &"CCCCCCCC".to_string(), true);
        let block3 = Block::create(
            conn,
            &sequence3_hash,
            block_group.id,
            1,
            8,
            &"+".to_string(),
        );
        let sequence4_hash =
            Sequence::create(conn, "DNA".to_string(), &"GGGGGGGG".to_string(), true);
        let block4 = Block::create(
            conn,
            &sequence4_hash,
            block_group.id,
            1,
            8,
            &"+".to_string(),
        );
        let edge1 = Edge::create(conn, Some(block1.id), Some(block3.id), 0, 0);
        let edge2 = Edge::create(conn, Some(block2.id), Some(block3.id), 0, 0);
        Edge::create(conn, Some(block3.id), Some(block4.id), 0, 0);

        let edges_into_block3 = Block::edges_into(conn, block3.id);
        assert_eq!(edges_into_block3.len(), 2);

        let mut actual_ids = HashSet::new();
        actual_ids.insert(edges_into_block3[0].id);
        actual_ids.insert(edges_into_block3[1].id);
        let mut expected_ids = HashSet::new();
        expected_ids.insert(edge1.id);
        expected_ids.insert(edge2.id);
        assert_eq!(actual_ids, expected_ids);
    }

    #[test]
    fn test_no_edges_into() {
        let conn = &mut get_connection();
        Collection::create(conn, &"test collection".to_string());
        let block_group = BlockGroup::create(
            conn,
            &"test collection".to_string(),
            None,
            &"test block group".to_string(),
        );
        let sequence1_hash =
            Sequence::create(conn, "DNA".to_string(), &"ATCGATCG".to_string(), true);
        let block1 = Block::create(
            conn,
            &sequence1_hash,
            block_group.id,
            0,
            8,
            &"+".to_string(),
        );
        let sequence2_hash =
            Sequence::create(conn, "DNA".to_string(), &"AAAAAAAA".to_string(), true);
        let block2 = Block::create(
            conn,
            &sequence2_hash,
            block_group.id,
            1,
            8,
            &"+".to_string(),
        );
        Edge::create(conn, Some(block1.id), Some(block2.id), 0, 0);

        let edges_into_block1 = Block::edges_into(conn, block1.id);
        assert_eq!(edges_into_block1.len(), 0);
    }

    #[test]
    fn test_edges_out_of() {
        let conn = &mut get_connection();
        Collection::create(conn, &"test collection".to_string());
        let block_group = BlockGroup::create(
            conn,
            &"test collection".to_string(),
            None,
            &"test block group".to_string(),
        );
        let sequence1_hash =
            Sequence::create(conn, "DNA".to_string(), &"ATCGATCG".to_string(), true);
        let block1 = Block::create(
            conn,
            &sequence1_hash,
            block_group.id,
            0,
            8,
            &"+".to_string(),
        );
        let sequence2_hash =
            Sequence::create(conn, "DNA".to_string(), &"AAAAAAAA".to_string(), true);
        let block2 = Block::create(
            conn,
            &sequence2_hash,
            block_group.id,
            1,
            8,
            &"+".to_string(),
        );
        let sequence3_hash =
            Sequence::create(conn, "DNA".to_string(), &"CCCCCCCC".to_string(), true);
        let block3 = Block::create(
            conn,
            &sequence3_hash,
            block_group.id,
            1,
            8,
            &"+".to_string(),
        );
        let sequence4_hash =
            Sequence::create(conn, "DNA".to_string(), &"GGGGGGGG".to_string(), true);
        let block4 = Block::create(
            conn,
            &sequence4_hash,
            block_group.id,
            1,
            8,
            &"+".to_string(),
        );
        Edge::create(conn, Some(block1.id), Some(block2.id), 0, 0);
        let edge1 = Edge::create(conn, Some(block2.id), Some(block3.id), 0, 0);
        let edge2 = Edge::create(conn, Some(block2.id), Some(block4.id), 0, 0);

        let edges_out_of_block2 = Block::edges_out_of(conn, block2.id);
        assert_eq!(edges_out_of_block2.len(), 2);

        let mut actual_ids = HashSet::new();
        actual_ids.insert(edges_out_of_block2[0].id);
        actual_ids.insert(edges_out_of_block2[1].id);
        let mut expected_ids = HashSet::new();
        expected_ids.insert(edge1.id);
        expected_ids.insert(edge2.id);
        assert_eq!(actual_ids, expected_ids);
    }

    #[test]
    fn test_no_edges_out_of() {
        let conn = &mut get_connection();
        Collection::create(conn, &"test collection".to_string());
        let block_group = BlockGroup::create(
            conn,
            &"test collection".to_string(),
            None,
            &"test block group".to_string(),
        );
        let sequence1_hash =
            Sequence::create(conn, "DNA".to_string(), &"ATCGATCG".to_string(), true);
        let block1 = Block::create(
            conn,
            &sequence1_hash,
            block_group.id,
            0,
            8,
            &"+".to_string(),
        );
        let sequence2_hash =
            Sequence::create(conn, "DNA".to_string(), &"AAAAAAAA".to_string(), true);
        let block2 = Block::create(
            conn,
            &sequence2_hash,
            block_group.id,
            1,
            8,
            &"+".to_string(),
        );
        Edge::create(conn, Some(block1.id), Some(block2.id), 0, 0);

        let edges_out_of_block2 = Block::edges_out_of(conn, block2.id);
        assert_eq!(edges_out_of_block2.len(), 0);
    }

    #[test]
    fn test_split_block() {
        let conn = &mut get_connection();
        Collection::create(conn, &"test collection".to_string());
        let block_group = BlockGroup::create(
            conn,
            &"test collection".to_string(),
            None,
            &"test block group".to_string(),
        );
        let sequence1_hash =
            Sequence::create(conn, "DNA".to_string(), &"ATCGATCG".to_string(), true);
        let block1 = Block::create(
            conn,
            &sequence1_hash,
            block_group.id,
            0,
            8,
            &"+".to_string(),
        );
        let sequence2_hash =
            Sequence::create(conn, "DNA".to_string(), &"AAAAAAAA".to_string(), true);
        let block2 = Block::create(
            conn,
            &sequence2_hash,
            block_group.id,
            1,
            8,
            &"+".to_string(),
        );
        let sequence3_hash =
            Sequence::create(conn, "DNA".to_string(), &"CCCCCCCC".to_string(), true);
        let block3 = Block::create(
            conn,
            &sequence3_hash,
            block_group.id,
            1,
            8,
            &"+".to_string(),
        );
        let sequence4_hash =
            Sequence::create(conn, "DNA".to_string(), &"GGGGGGGG".to_string(), true);
        let block4 = Block::create(
            conn,
            &sequence4_hash,
            block_group.id,
            1,
            8,
            &"+".to_string(),
        );
        let edge1 = Edge::create(conn, Some(block1.id), Some(block3.id), 0, 0);
        let edge2 = Edge::create(conn, Some(block2.id), Some(block3.id), 0, 0);
        let edge3 = Edge::create(conn, Some(block3.id), Some(block4.id), 0, 0);

        let (left_block, right_block) = Block::split(conn, &block3, 4, 0, 0).unwrap();

        let edges_into_left_block = Block::edges_into(conn, left_block.id);
        assert_eq!(edges_into_left_block.len(), 2);

        let mut actual_incoming_ids = HashSet::new();
        actual_incoming_ids.insert(edges_into_left_block[0].id);
        actual_incoming_ids.insert(edges_into_left_block[1].id);
        let mut expected_incoming_ids = HashSet::new();
        expected_incoming_ids.insert(edge1.id);
        expected_incoming_ids.insert(edge2.id);
        assert_eq!(actual_incoming_ids, expected_incoming_ids);

        let edges_out_of_right_block = Block::edges_out_of(conn, right_block.id);
        assert_eq!(edges_out_of_right_block.len(), 1);
        assert_eq!(edges_out_of_right_block[0].id, edge3.id);

        let new_edge = Edge::lookup(conn, Some(left_block.id), Some(right_block.id));
        assert!(new_edge.is_some());
    }

    #[test]
    fn test_split_block_bad_coordinate() {
        let conn = &mut get_connection();
        Collection::create(conn, &"test collection".to_string());
        let block_group = BlockGroup::create(
            conn,
            &"test collection".to_string(),
            None,
            &"test block group".to_string(),
        );
        let sequence1_hash =
            Sequence::create(conn, "DNA".to_string(), &"ATCGATCG".to_string(), true);
        let block1 = Block::create(
            conn,
            &sequence1_hash,
            block_group.id,
            0,
            8,
            &"+".to_string(),
        );
        let result = Block::split(conn, &block1, -1, 0, 0);
        assert!(result.is_none());

        let block2 = Block::create(
            conn,
            &sequence1_hash,
            block_group.id,
            0,
            8,
            &"+".to_string(),
        );
        let result = Block::split(conn, &block2, 100, 0, 0);
        assert!(result.is_none());
    }
}
