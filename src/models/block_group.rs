use std::collections::{HashMap, HashSet};

use intervaltree::IntervalTree;
use petgraph::graphmap::DiGraphMap;
use petgraph::visit::{depth_first_search, Dfs, DfsEvent, IntoEdgesDirected, Reversed};
use petgraph::Direction;
use rusqlite::{params_from_iter, types::Value as SQLValue, Connection};
use serde::{Deserialize, Serialize};

use crate::graph::all_simple_paths;
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::edge::{Edge, EdgeData, GroupBlock};
use crate::models::node::{PATH_END_NODE_ID, PATH_START_NODE_ID};
use crate::models::path::{Path, PathBlock, PathData};
use crate::models::path_edge::PathEdge;
use crate::models::strand::Strand;

#[derive(Debug, Deserialize, Serialize)]
pub struct BlockGroup {
    pub id: i64,
    pub collection_name: String,
    pub sample_name: Option<String>,
    pub name: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BlockGroupData<'a> {
    pub collection_name: &'a str,
    pub sample_name: Option<&'a str>,
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct PathChange {
    pub block_group_id: i64,
    pub path: Path,
    pub start: i64,
    pub end: i64,
    pub block: PathBlock,
    pub chromosome_index: i64,
    pub phased: i64,
}

pub struct PathCache<'a> {
    pub cache: HashMap<PathData, Path>,
    pub intervaltree_cache: HashMap<Path, IntervalTree<i64, PathBlock>>,
    pub conn: &'a Connection,
}

impl PathCache<'_> {
    pub fn new(conn: &Connection) -> PathCache {
        PathCache {
            cache: HashMap::<PathData, Path>::new(),
            intervaltree_cache: HashMap::<Path, IntervalTree<i64, PathBlock>>::new(),
            conn,
        }
    }

    pub fn lookup(path_cache: &mut PathCache, block_group_id: i64, name: String) -> Path {
        let path_key = PathData {
            name: name.clone(),
            block_group_id,
        };
        let path_lookup = path_cache.cache.get(&path_key);
        if let Some(path) = path_lookup {
            path.clone()
        } else {
            let new_path = Path::get_paths(
                path_cache.conn,
                "select * from path where block_group_id = ?1 AND name = ?2",
                vec![SQLValue::from(block_group_id), SQLValue::from(name)],
            )[0]
            .clone();

            path_cache.cache.insert(path_key, new_path.clone());
            let tree = Path::intervaltree_for(path_cache.conn, &new_path);
            path_cache.intervaltree_cache.insert(new_path.clone(), tree);
            new_path
        }
    }

    pub fn get_intervaltree<'a>(
        path_cache: &'a PathCache<'a>,
        path: &'a Path,
    ) -> Option<&'a IntervalTree<i64, PathBlock>> {
        path_cache.intervaltree_cache.get(path)
    }
}

impl BlockGroup {
    pub fn create(
        conn: &Connection,
        collection_name: &str,
        sample_name: Option<&str>,
        name: &str,
    ) -> BlockGroup {
        let query = "INSERT INTO block_group (collection_name, sample_name, name) VALUES (?1, ?2, ?3) RETURNING *";
        let mut stmt = conn.prepare(query).unwrap();
        match stmt.query_row((collection_name, sample_name, name), |row| {
            Ok(BlockGroup {
                id: row.get(0)?,
                collection_name: row.get(1)?,
                sample_name: row.get(2)?,
                name: row.get(3)?,
            })
        }) {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    let bg_id = match sample_name {
                        Some(v) => {conn
                            .query_row(
                                "select id from block_group where collection_name = ?1 and sample_name = ?2 and name = ?3",
                                (collection_name, v, name),
                                |row| row.get(0),
                            )
                            .unwrap()}
                        None => {
                            conn
                            .query_row(
                                "select id from block_group where collection_name = ?1 and sample_name is null and name = ?2",
                                (collection_name, name),
                                |row| row.get(0),
                            )
                            .unwrap()
                        }
                    };
                    BlockGroup {
                        id: bg_id,
                        collection_name: collection_name.to_string(),
                        sample_name: sample_name.map(|s| s.to_string()),
                        name: name.to_string(),
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

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<SQLValue>) -> Vec<BlockGroup> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(BlockGroup {
                    id: row.get(0)?,
                    collection_name: row.get(1)?,
                    sample_name: row.get(2)?,
                    name: row.get(3)?,
                })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }

    pub fn add_relation(conn: &Connection, source_block_group_id: i64, target_block_group_id: i64) {
        let query = "INSERT OR IGNORE INTO block_group_tree (parent_id, child_id) values (?1, ?2)";
        let mut stmt = conn.prepare(query).unwrap();
        stmt.execute((source_block_group_id, target_block_group_id))
            .unwrap();
    }

    pub fn get_children(conn: &Connection, block_group_id: i64) -> Vec<i64> {
        let query = "select child_id from block_group_tree where parent_id = ?1;";
        let mut stmt = conn.prepare(query).unwrap();
        let mut children = vec![];
        for row in stmt.query_map((block_group_id,), |row| row.get(0)).unwrap() {
            children.push(row.unwrap());
        }
        children
    }

    pub fn get_parents(conn: &Connection, block_group_id: i64) -> Vec<i64> {
        let query = "select parent_id from block_group_tree where child_id = ?1;";
        let mut stmt = conn.prepare(query).unwrap();
        let mut ids = vec![];
        for row in stmt.query_map((block_group_id,), |row| row.get(0)).unwrap() {
            ids.push(row.unwrap());
        }
        ids
    }

    pub fn get_ancestors(conn: &Connection, block_group_id: i64) -> Vec<Vec<i64>> {
        let query = "WITH RECURSIVE ancestors(parent_id, child_id, depth) AS ( \
        VALUES(?1, NULL, 0) UNION  \
        select bgt.parent_id, bgt.child_id, ancestors.depth+1 from block_group_tree bgt join ancestors ON bgt.child_id=ancestors.parent_id \
        ) SELECT parent_id, child_id, depth from ancestors where child_id is not null ORDER BY depth, parent_id DESC;";
        let mut stmt = conn.prepare(query).unwrap();

        let mut graph: DiGraphMap<i64, ()> = DiGraphMap::new();

        for row in stmt
            .query_map((block_group_id,), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap()
        {
            let (parent_id, child_id, depth): (i64, i64, i64) = row.unwrap();
            graph.add_node(parent_id);
            graph.add_node(child_id);
            graph.add_edge(parent_id, child_id, ());
        }

        let rev = Reversed(&graph);
        let mut paths = vec![];
        let mut current_path = vec![];
        depth_first_search(&rev, Some(block_group_id), |event| {
            if let DfsEvent::TreeEdge(u, v) = event {
                current_path.push(v);
                if rev.edges_directed(v, Direction::Outgoing).next().is_none() {
                    paths.push(current_path.clone());
                    current_path.clear();
                }
            } else if let DfsEvent::CrossForwardEdge(u, v) = event {
                if u != block_group_id {
                    current_path.push(u);
                }
                if rev.edges_directed(v, Direction::Outgoing).next().is_none() {
                    current_path.push(v);
                    paths.push(current_path.clone());
                    current_path.clear();
                }
            }
        });
        paths
    }

    pub fn get_by_id(conn: &Connection, id: i64) -> BlockGroup {
        let query = "SELECT * FROM block_group WHERE id = ?1";
        let mut stmt = conn.prepare(query).unwrap();
        match stmt.query_row(params_from_iter(vec![SQLValue::from(id)]), |row| {
            Ok(BlockGroup {
                id: row.get(0)?,
                collection_name: row.get(1)?,
                sample_name: row.get(2)?,
                name: row.get(3)?,
            })
        }) {
            Ok(res) => res,
            Err(rusqlite::Error::QueryReturnedNoRows) => panic!("No block group with id {}", id),
            Err(_) => {
                panic!("something bad happened querying the database")
            }
        }
    }

    pub fn clone(conn: &Connection, source_block_group_id: i64, target_block_group_id: i64) {
        let existing_paths = Path::get_paths(
            conn,
            "SELECT * from path where block_group_id = ?1",
            vec![SQLValue::from(source_block_group_id)],
        );

        let edge_ids = BlockGroupEdge::edges_for_block_group(conn, source_block_group_id)
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<i64>>();
        BlockGroupEdge::bulk_create(conn, target_block_group_id, &edge_ids);

        for path in existing_paths {
            let edge_ids = PathEdge::edges_for_path(conn, path.id)
                .into_iter()
                .map(|edge| edge.id)
                .collect::<Vec<i64>>();
            Path::create(conn, &path.name, target_block_group_id, &edge_ids);
        }

        BlockGroup::add_relation(conn, source_block_group_id, target_block_group_id);
    }

    pub fn get_or_create_sample_block_group(
        conn: &Connection,
        collection_name: &str,
        sample_name: &str,
        group_name: &str,
    ) -> i64 {
        let mut bg_id : i64 = match conn.query_row(
            "select id from block_group where collection_name = ?1 AND sample_name = ?2 AND name = ?3",
            (collection_name, sample_name, group_name),
            |row| row.get(0),
        ) {
            Ok(res) => res,
            Err(rusqlite::Error::QueryReturnedNoRows) => 0,
            Err(_e) => {
                panic!("Error querying the database: {_e}");
            }
        };
        if bg_id != 0 {
            return bg_id;
        } else {
            // use the base reference group if it exists
            bg_id = match conn.query_row(
            "select id from block_group where collection_name = ?1 AND sample_name IS null AND name = ?2",
            (collection_name, group_name),
            |row| row.get(0),
            ) {
                Ok(res) => res,
                Err(rusqlite::Error::QueryReturnedNoRows) => panic!("No base path exists"),
                Err(_e) => {
                    panic!("something bad happened querying the database")
                }
            }
        }
        let new_bg_id = BlockGroup::create(conn, collection_name, Some(sample_name), group_name);

        // clone parent blocks/edges/path
        BlockGroup::clone(conn, bg_id, new_bg_id.id);

        new_bg_id.id
    }

    pub fn get_id(
        conn: &Connection,
        collection_name: &str,
        sample_name: Option<&str>,
        group_name: &str,
    ) -> i64 {
        let result = if sample_name.is_some() {
            conn.query_row(
		"select id from block_group where collection_name = ?1 AND sample_name = ?2 AND name = ?3",
		(collection_name, sample_name, group_name),
		|row| row.get(0),
            )
        } else {
            conn.query_row(
		"select id from block_group where collection_name = ?1 AND sample_name IS NULL AND name = ?2",
		(collection_name, group_name),
		|row| row.get(0),
            )
        };

        match result {
            Ok(res) => res,
            Err(rusqlite::Error::QueryReturnedNoRows) => 0,
            Err(_e) => {
                panic!("Error querying the database: {_e}");
            }
        }
    }

    pub fn get_graph(conn: &Connection, block_group_id: i64) -> DiGraphMap<i64, ()> {
        let mut edges = BlockGroupEdge::edges_for_block_group(conn, block_group_id);
        let (blocks, boundary_edges) = Edge::blocks_from_edges(conn, &edges);
        edges.extend(boundary_edges);
        let (graph, _) = Edge::build_graph(&edges, &blocks);
        graph
    }

    pub fn get_all_sequences(conn: &Connection, block_group_id: i64) -> HashSet<String> {
        let mut edges = BlockGroupEdge::edges_for_block_group(conn, block_group_id);
        let (blocks, boundary_edges) = Edge::blocks_from_edges(conn, &edges);
        edges.extend(boundary_edges.clone());
        let (graph, _) = Edge::build_graph(&edges, &blocks);

        let mut start_nodes = vec![];
        let mut end_nodes = vec![];
        for node in graph.nodes() {
            let has_incoming = graph.neighbors_directed(node, Direction::Incoming).next();
            let has_outgoing = graph.neighbors_directed(node, Direction::Outgoing).next();
            if has_incoming.is_none() {
                start_nodes.push(node);
            }
            if has_outgoing.is_none() {
                end_nodes.push(node);
            }
        }

        let blocks_by_id = blocks
            .clone()
            .into_iter()
            .map(|block| (block.id, block))
            .collect::<HashMap<i64, GroupBlock>>();
        let mut sequences = HashSet::<String>::new();

        for start_node in start_nodes {
            for end_node in &end_nodes {
                // TODO: maybe make all_simple_paths return a single path id where start == end
                if start_node == *end_node {
                    let block = blocks_by_id.get(&start_node).unwrap();
                    if block.node_id != PATH_START_NODE_ID && block.node_id != PATH_END_NODE_ID {
                        sequences.insert(block.sequence());
                    }
                } else {
                    for path in all_simple_paths(&graph, start_node, *end_node) {
                        let mut current_sequence = "".to_string();
                        for node in path {
                            let block = blocks_by_id.get(&node).unwrap();
                            let block_sequence = block.sequence();
                            current_sequence.push_str(&block_sequence);
                        }
                        sequences.insert(current_sequence);
                    }
                }
            }
        }

        sequences
    }

    pub fn insert_changes(conn: &Connection, changes: &Vec<PathChange>, cache: &PathCache) {
        let mut new_edges_by_block_group = HashMap::<i64, Vec<EdgeData>>::new();
        for change in changes {
            let tree = PathCache::get_intervaltree(cache, &change.path).unwrap();
            let new_edges = BlockGroup::set_up_new_edges(change, tree);
            new_edges_by_block_group
                .entry(change.block_group_id)
                .and_modify(|new_edge_data| new_edge_data.extend(new_edges.clone()))
                .or_insert_with(|| new_edges.clone());
        }

        for (block_group_id, new_edges) in new_edges_by_block_group {
            let edge_ids = Edge::bulk_create(conn, new_edges);
            BlockGroupEdge::bulk_create(conn, block_group_id, &edge_ids);
        }
    }

    #[allow(clippy::ptr_arg)]
    #[allow(clippy::needless_late_init)]
    pub fn insert_change(
        conn: &Connection,
        change: &PathChange,
        tree: &IntervalTree<i64, PathBlock>,
    ) {
        let new_edges = BlockGroup::set_up_new_edges(change, tree);
        let edge_ids = Edge::bulk_create(conn, new_edges);
        BlockGroupEdge::bulk_create(conn, change.block_group_id, &edge_ids);
    }

    pub fn set_up_new_edges(
        change: &PathChange,
        tree: &IntervalTree<i64, PathBlock>,
    ) -> Vec<EdgeData> {
        let start_blocks: Vec<&PathBlock> =
            tree.query_point(change.start).map(|x| &x.value).collect();
        assert_eq!(start_blocks.len(), 1);
        // NOTE: This may not be used but needs to be initialized here instead of inside the if
        // statement that uses it, so that the borrow checker is happy
        let previous_start_blocks: Vec<&PathBlock> = tree
            .query_point(change.start - 1)
            .map(|x| &x.value)
            .collect();
        assert_eq!(previous_start_blocks.len(), 1);
        let start_block = if start_blocks[0].path_start == change.start {
            // First part of this block will be replaced/deleted, need to get previous block to add
            // edge including it
            previous_start_blocks[0]
        } else {
            start_blocks[0]
        };

        let end_blocks: Vec<&PathBlock> = tree.query_point(change.end).map(|x| &x.value).collect();
        assert_eq!(end_blocks.len(), 1);
        let end_block = end_blocks[0];

        let mut new_edges = vec![];

        if change.block.sequence_start == change.block.sequence_end {
            // Deletion
            let new_edge = EdgeData {
                source_node_id: start_block.node_id,
                source_coordinate: change.start - start_block.path_start
                    + start_block.sequence_start,
                source_strand: Strand::Forward,
                target_node_id: end_block.node_id,
                target_coordinate: change.end - end_block.path_start + end_block.sequence_start,
                target_strand: Strand::Forward,
                chromosome_index: change.chromosome_index,
                phased: change.phased,
            };
            new_edges.push(new_edge);

            // NOTE: If the deletion is happening at the very beginning of a path, we need to add
            // an edge from the dedicated start node to the end of the deletion, to indicate it's
            // another start point in the block group DAG.
            if change.start == 0 {
                let new_beginning_edge = EdgeData {
                    source_node_id: PATH_START_NODE_ID,
                    source_coordinate: 0,
                    source_strand: Strand::Forward,
                    target_node_id: end_block.node_id,
                    target_coordinate: change.end - end_block.path_start + end_block.sequence_start,
                    target_strand: Strand::Forward,
                    chromosome_index: change.chromosome_index,
                    phased: change.phased,
                };
                new_edges.push(new_beginning_edge);
            }
        // NOTE: If the deletion is happening at the very end of a path, we might add an edge
        // from the beginning of the deletion to the dedicated end node, but in practice it
        // doesn't affect sequence readouts, so it may not be worth it.
        } else {
            // Insertion/replacement
            let new_start_edge = EdgeData {
                source_node_id: start_block.node_id,
                source_coordinate: change.start - start_block.path_start
                    + start_block.sequence_start,
                source_strand: Strand::Forward,
                target_node_id: change.block.node_id,
                target_coordinate: change.block.sequence_start,
                target_strand: Strand::Forward,
                chromosome_index: change.chromosome_index,
                phased: change.phased,
            };
            let new_end_edge = EdgeData {
                source_node_id: change.block.node_id,
                source_coordinate: change.block.sequence_end,
                source_strand: Strand::Forward,
                target_node_id: end_block.node_id,
                target_coordinate: change.end - end_block.path_start + end_block.sequence_start,
                target_strand: Strand::Forward,
                chromosome_index: change.chromosome_index,
                phased: change.phased,
            };
            new_edges.push(new_start_edge);
            new_edges.push(new_end_edge);
        }

        new_edges
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{collection::Collection, node::Node, sample::Sample, sequence::Sequence};
    use crate::test_helpers::{get_connection, setup_block_group};

    #[test]
    fn test_blockgroup_create() {
        let conn = &get_connection(None);
        Collection::create(conn, "test");
        let bg1 = BlockGroup::create(conn, "test", None, "hg19");
        assert_eq!(bg1.collection_name, "test");
        assert_eq!(bg1.name, "hg19");
        Sample::create(conn, "sample");
        let bg2 = BlockGroup::create(conn, "test", Some("sample"), "hg19");
        assert_eq!(bg2.collection_name, "test");
        assert_eq!(bg2.name, "hg19");
        assert_eq!(bg2.sample_name, Some("sample".to_string()));
        assert_ne!(bg1.id, bg2.id);
    }

    #[test]
    fn test_blockgroup_clone() {
        let conn = &get_connection(None);
        Collection::create(conn, "test");
        let bg1 = BlockGroup::create(conn, "test", None, "hg19");
        assert_eq!(bg1.collection_name, "test");
        assert_eq!(bg1.name, "hg19");
        Sample::create(conn, "sample");
        let bg2 = BlockGroup::get_or_create_sample_block_group(conn, "test", "sample", "hg19");
        assert_eq!(
            BlockGroupEdge::edges_for_block_group(conn, bg1.id),
            BlockGroupEdge::edges_for_block_group(conn, bg2)
        );
    }

    #[test]
    fn insert_and_deletion_get_all() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 7,
            end: 15,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );

        let deletion_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("")
            .save(&conn);
        let deletion_node_id = Node::create(&conn, deletion_sequence.hash.as_str(), None);
        let deletion = PathBlock {
            id: 0,
            node_id: deletion_node_id,
            block_sequence: deletion_sequence.get_sequence(None, None),
            sequence_start: 0,
            sequence_end: 0,
            path_start: 19,
            path_end: 31,
            strand: Strand::Forward,
        };

        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 19,
            end: 31,
            block: deletion,
            chromosome_index: 1,
            phased: 0,
        };
        // take out an entire block.
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);
        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTTGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTGGGGGGGGG".to_string(),
            ])
        )
    }

    #[test]
    fn simple_insert_get_all() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 7,
            end: 15,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_on_block_boundary_middle() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 15,
            path_end: 15,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 15,
            end: 15,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTNNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_within_block() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 12,
            path_end: 17,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 12,
            end: 17,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTNNNNTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_on_block_boundary_start() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 10,
            path_end: 10,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 10,
            end: 10,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAANNNNTTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_on_block_boundary_end() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 9,
            path_end: 9,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 9,
            end: 9,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAANNNNATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_across_entire_block_boundary() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 10,
            path_end: 20,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 10,
            end: 20,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAANNNNCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_across_two_blocks() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 15,
            path_end: 25,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 15,
            end: 25,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTNNNNCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_spanning_blocks() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 5,
            path_end: 35,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 5,
            end: 35,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAANNNNGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn simple_deletion() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let deletion_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("")
            .save(&conn);
        let deletion_node_id = Node::create(&conn, deletion_sequence.hash.as_str(), None);
        let deletion = PathBlock {
            id: 0,
            node_id: deletion_node_id,
            block_sequence: deletion_sequence.get_sequence(None, None),
            sequence_start: 0,
            sequence_end: 0,
            path_start: 19,
            path_end: 31,
            strand: Strand::Forward,
        };

        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 19,
            end: 31,
            block: deletion,
            chromosome_index: 1,
            phased: 0,
        };

        // take out an entire block.
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);
        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTTGGGGGGGGG".to_string(),
            ])
        )
    }

    #[test]
    fn doesnt_apply_same_insert_twice() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 7,
            end: 15,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );

        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );
    }

    #[test]
    fn insert_at_beginning_of_path() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 0,
            path_end: 0,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 0,
            end: 0,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "NNNNAAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
            ])
        );
    }

    #[test]
    fn insert_at_end_of_path() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);

        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 40,
            path_end: 40,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 40,
            end: 40,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGGNNNN".to_string(),
            ])
        );
    }

    #[test]
    fn insert_at_one_bp_into_block() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 10,
            path_end: 11,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 10,
            end: 11,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAANNNNTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
            ])
        );
    }

    #[test]
    fn insert_at_one_bp_from_end_of_block() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(&conn);
        let insert_node_id = Node::create(&conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 19,
            path_end: 20,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 19,
            end: 20,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTTNNNNCCCCCCCCCCGGGGGGGGGG".to_string(),
            ])
        );
    }

    #[test]
    fn delete_at_beginning_of_path() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let deletion_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("")
            .save(&conn);
        let deletion_node_id = Node::create(&conn, deletion_sequence.hash.as_str(), None);
        let deletion = PathBlock {
            id: 0,
            node_id: deletion_node_id,
            block_sequence: deletion_sequence.get_sequence(None, None),
            sequence_start: 0,
            sequence_end: 0,
            path_start: 0,
            path_end: 1,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 0,
            end: 1,
            block: deletion,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
            ])
        );
    }

    #[test]
    fn delete_at_end_of_path() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let deletion_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("")
            .save(&conn);
        let deletion_node_id = Node::create(&conn, deletion_sequence.hash.as_str(), None);
        let deletion = PathBlock {
            id: 0,
            node_id: deletion_node_id,
            block_sequence: deletion_sequence.get_sequence(None, None),
            sequence_start: 0,
            sequence_end: 0,
            path_start: 35,
            path_end: 40,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 35,
            end: 40,
            block: deletion,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
            ])
        );
    }

    #[test]
    fn deletion_starting_at_block_boundary() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let deletion_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("")
            .save(&conn);
        let deletion_node_id = Node::create(&conn, deletion_sequence.hash.as_str(), None);
        let deletion = PathBlock {
            id: 0,
            node_id: deletion_node_id,
            block_sequence: deletion_sequence.get_sequence(None, None),
            sequence_start: 0,
            sequence_end: 0,
            path_start: 10,
            path_end: 12,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 10,
            end: 12,
            block: deletion,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
            ])
        );
    }

    #[test]
    fn deletion_ending_at_block_boundary() {
        let conn = get_connection(None);
        let (block_group_id, path) = setup_block_group(&conn);
        let deletion_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("")
            .save(&conn);
        let deletion_node_id = Node::create(&conn, deletion_sequence.hash.as_str(), None);
        let deletion = PathBlock {
            id: 0,
            node_id: deletion_node_id,
            block_sequence: deletion_sequence.get_sequence(None, None),
            sequence_start: 0,
            sequence_end: 0,
            path_start: 18,
            path_end: 20,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id,
            path: path.clone(),
            start: 18,
            end: 20,
            block: deletion,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(&conn, &path);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
            ])
        );
    }

    #[test]
    fn test_adds_relation_on_clone() {
        let conn = &get_connection(None);
        let (block_group_id, path) = setup_block_group(conn);
        let new_bg = BlockGroup::create(conn, "test", None, "test2");
        let new_bg_id = new_bg.id;
        BlockGroup::clone(conn, block_group_id, new_bg_id);
        assert_eq!(
            BlockGroup::get_children(conn, block_group_id),
            vec![new_bg_id]
        );
        assert_eq!(
            BlockGroup::get_parents(conn, new_bg_id),
            vec![block_group_id]
        );
    }

    #[test]
    fn test_finds_all_ancestors() {
        let conn = &get_connection(None);
        let collection = Collection::create(conn, "test");
        let bg1 = BlockGroup::create(conn, "test", None, "test1");
        let bg2 = BlockGroup::create(conn, "test", None, "test2");
        let bg3 = BlockGroup::create(conn, "test", None, "test3");
        let bg4 = BlockGroup::create(conn, "test", None, "test4");
        BlockGroup::add_relation(conn, bg1.id, bg2.id);
        BlockGroup::add_relation(conn, bg2.id, bg3.id);
        BlockGroup::add_relation(conn, bg3.id, bg4.id);
        BlockGroup::add_relation(conn, bg1.id, bg4.id);
        BlockGroup::add_relation(conn, bg1.id, bg3.id);
        assert_eq!(
            BlockGroup::get_ancestors(conn, bg4.id),
            vec![
                vec![bg3.id, bg2.id, bg1.id],
                vec![bg3.id, bg1.id],
                vec![bg1.id]
            ]
        );
    }
}
