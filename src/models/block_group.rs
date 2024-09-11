use crate::config::get_operation;
use crate::graph::all_simple_paths;
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::change_log::ChangeLog;
use crate::models::edge::{Edge, EdgeData};
use crate::models::operations::OperationEdge;
use crate::models::path::{NewBlock, Path, PathData};
use crate::models::path_edge::PathEdge;
use crate::models::sequence::Sequence;
use crate::models::strand::Strand;
use intervaltree::IntervalTree;
use itertools::Itertools;
use petgraph::graphmap::DiGraphMap;
use petgraph::Direction;
use rusqlite::{params_from_iter, types::Value as SQLValue, Connection};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct BlockGroup {
    pub id: i32,
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

#[derive(Clone)]
pub struct GroupBlock {
    pub id: i32,
    pub sequence_hash: String,
    pub sequence: String,
    pub start: i32,
    pub end: i32,
}

#[derive(Eq, Hash, PartialEq)]
pub struct BlockKey {
    pub sequence_hash: String,
    pub coordinate: i32,
}

#[derive(Clone, Debug)]
pub struct PathChange {
    pub block_group_id: i32,
    pub path: Path,
    pub start: i32,
    pub end: i32,
    pub block: NewBlock,
    pub chromosome_index: i32,
    pub phased: i32,
}

pub struct PathCache<'a> {
    pub cache: HashMap<PathData, Path>,
    pub intervaltree_cache: HashMap<Path, IntervalTree<i32, NewBlock>>,
    pub conn: &'a Connection,
}

impl PathCache<'_> {
    pub fn new(conn: &Connection) -> PathCache {
        PathCache {
            cache: HashMap::<PathData, Path>::new(),
            intervaltree_cache: HashMap::<Path, IntervalTree<i32, NewBlock>>::new(),
            conn,
        }
    }

    pub fn lookup(path_cache: &mut PathCache, block_group_id: i32, name: String) -> Path {
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
    ) -> Option<&'a IntervalTree<i32, NewBlock>> {
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
                    BlockGroup {
                        id: conn
                            .query_row(
                                "select id from block_group where collection_name = ?1 and sample_name is null and name = ?2",
                                (collection_name, name),
                                |row| row.get(0),
                            )
                            .unwrap(),
                        collection_name: collection_name.to_string(),
                        sample_name: sample_name.map(|s| s.to_string()),
                        name: name.to_string()
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

    pub fn clone(conn: &Connection, source_block_group_id: i32, target_block_group_id: i32) {
        let operation_id = get_operation();
        let binding = BlockGroup::query(
            conn,
            "select * from block_group where id = ?1",
            vec![SQLValue::from(target_block_group_id)],
        );
        let target_block_group = binding.first().unwrap();
        let target_sample = target_block_group.sample_name.clone();
        let existing_paths = Path::get_paths(
            conn,
            "SELECT * from path where block_group_id = ?1",
            vec![SQLValue::from(source_block_group_id)],
        );

        let edge_ids = BlockGroupEdge::edges_for_block_group(conn, source_block_group_id)
            .iter()
            .map(|edge| edge.id)
            .collect();
        println!("cloning edges {edge_ids:?}");
        let bg_edges = BlockGroupEdge::bulk_create(conn, target_block_group_id, edge_ids);
        let mut edge_id_to_bg_edge_id: HashMap<i32, i32> = HashMap::new();
        for bg_edge in bg_edges.iter() {
            edge_id_to_bg_edge_id.insert(bg_edge.edge_id, bg_edge.id);
        }
        let mut added_edges: HashSet<i32> = HashSet::new();

        for path in existing_paths {
            let edge_ids = PathEdge::edges_for(conn, path.id)
                .into_iter()
                .map(|edge| edge.id)
                .collect::<Vec<i32>>();
            let path = Path::create(conn, &path.name, target_block_group_id, &edge_ids);
            let path_bg_edits = edge_ids
                .iter()
                .map(|id| edge_id_to_bg_edge_id[id])
                .collect::<Vec<i32>>();
            OperationEdge::bulk_create(
                conn,
                operation_id.unwrap(),
                Some(path.id),
                target_sample.clone(),
                &path_bg_edits,
            );
            for id in path_bg_edits.iter() {
                added_edges.insert(*id);
            }
        }
        OperationEdge::bulk_create(
            conn,
            operation_id.unwrap(),
            None,
            target_sample,
            &bg_edges
                .iter()
                .filter(|bg_edge| !added_edges.contains(&(bg_edge.id)))
                .map(|bg_edge| bg_edge.id)
                .collect::<Vec<i32>>(),
        );
    }

    pub fn get_or_create_sample_block_group(
        conn: &Connection,
        collection_name: &str,
        sample_name: &str,
        group_name: &str,
    ) -> i32 {
        let mut bg_id : i32 = match conn.query_row(
            "select id from block_group where collection_name = ?1 AND sample_name = ?2 AND name = ?3",
            (collection_name, sample_name, group_name.clone()),
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
            (collection_name, group_name.clone()),
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

    pub fn get_block_boundaries(
        source_edges: Option<&Vec<&Edge>>,
        target_edges: Option<&Vec<&Edge>>,
        sequence_length: i32,
    ) -> Vec<i32> {
        let mut block_boundary_coordinates = HashSet::new();
        if let Some(actual_source_edges) = source_edges {
            for source_edge in actual_source_edges {
                if source_edge.source_coordinate > 0
                    && source_edge.source_coordinate < sequence_length
                {
                    block_boundary_coordinates.insert(source_edge.source_coordinate);
                }
            }
        }
        if let Some(actual_target_edges) = target_edges {
            for target_edge in actual_target_edges {
                if target_edge.target_coordinate > 0
                    && target_edge.target_coordinate < sequence_length
                {
                    block_boundary_coordinates.insert(target_edge.target_coordinate);
                }
            }
        }

        block_boundary_coordinates
            .into_iter()
            .sorted_by(|c1, c2| Ord::cmp(&c1, &c2))
            .collect::<Vec<i32>>()
    }

    pub fn blocks_from_edges(conn: &Connection, edges: &Vec<Edge>) -> (Vec<GroupBlock>, Vec<Edge>) {
        let mut sequence_hashes = HashSet::new();
        let mut edges_by_source_hash: HashMap<&str, Vec<&Edge>> = HashMap::new();
        let mut edges_by_target_hash: HashMap<&str, Vec<&Edge>> = HashMap::new();
        for edge in edges {
            if edge.source_hash != Edge::PATH_START_HASH {
                sequence_hashes.insert(edge.source_hash.as_str());
                edges_by_source_hash
                    .entry(&edge.source_hash)
                    .and_modify(|edges| edges.push(edge))
                    .or_default();
            }
            if edge.target_hash != Edge::PATH_END_HASH {
                sequence_hashes.insert(edge.target_hash.as_str());
                edges_by_target_hash
                    .entry(&edge.target_hash)
                    .and_modify(|edges| edges.push(edge))
                    .or_default();
            }
        }

        let sequences_by_hash =
            Sequence::sequences_by_hash(conn, sequence_hashes.into_iter().collect::<Vec<&str>>());
        let mut blocks = vec![];
        let mut block_index = 0;
        let mut boundary_edges = vec![];
        for (hash, sequence) in sequences_by_hash.into_iter() {
            let block_boundaries = BlockGroup::get_block_boundaries(
                edges_by_source_hash.get(hash.as_str()),
                edges_by_target_hash.get(hash.as_str()),
                sequence.length,
            );
            for block_boundary in &block_boundaries {
                // NOTE: Most of this data is bogus, the Edge struct is just a convenient wrapper
                // for the data we need to set up boundary edges in the block group graph
                boundary_edges.push(Edge {
                    id: -1,
                    source_hash: hash.clone(),
                    source_coordinate: *block_boundary,
                    source_strand: Strand::Unknown,
                    target_hash: hash.clone(),
                    target_coordinate: *block_boundary,
                    target_strand: Strand::Unknown,
                    chromosome_index: 0,
                    phased: 0,
                });
            }

            if !block_boundaries.is_empty() {
                let start = 0;
                let end = block_boundaries[0];
                let block_sequence = sequence.get_sequence(start, end).to_string();
                let first_block = GroupBlock {
                    id: block_index,
                    sequence_hash: hash.clone(),
                    sequence: block_sequence,
                    start,
                    end,
                };
                blocks.push(first_block);
                block_index += 1;
                for (start, end) in block_boundaries.clone().into_iter().tuple_windows() {
                    let block_sequence = sequence.get_sequence(start, end).to_string();
                    let block = GroupBlock {
                        id: block_index,
                        sequence_hash: hash.clone(),
                        sequence: block_sequence,
                        start,
                        end,
                    };
                    blocks.push(block);
                    block_index += 1;
                }
                let start = block_boundaries[block_boundaries.len() - 1];
                let end = sequence.length;
                let block_sequence = sequence.get_sequence(start, end).to_string();
                let last_block = GroupBlock {
                    id: block_index,
                    sequence_hash: hash.clone(),
                    sequence: block_sequence,
                    start,
                    end,
                };
                blocks.push(last_block);
                block_index += 1;
            } else {
                blocks.push(GroupBlock {
                    id: block_index,
                    sequence_hash: hash.clone(),
                    sequence: sequence.get_sequence(None, None),
                    start: 0,
                    end: sequence.length,
                });
                block_index += 1;
            }
        }
        (blocks, boundary_edges)
    }

    pub fn get_all_sequences(conn: &Connection, block_group_id: i32) -> HashSet<String> {
        let mut edges = BlockGroupEdge::edges_for_block_group(conn, block_group_id);
        let (blocks, boundary_edges) = BlockGroup::blocks_from_edges(conn, &edges);
        edges.extend(boundary_edges.clone());

        let blocks_by_start = blocks
            .clone()
            .into_iter()
            .map(|block| {
                (
                    BlockKey {
                        sequence_hash: block.sequence_hash,
                        coordinate: block.start,
                    },
                    block.id,
                )
            })
            .collect::<HashMap<BlockKey, i32>>();
        let blocks_by_end = blocks
            .clone()
            .into_iter()
            .map(|block| {
                (
                    BlockKey {
                        sequence_hash: block.sequence_hash,
                        coordinate: block.end,
                    },
                    block.id,
                )
            })
            .collect::<HashMap<BlockKey, i32>>();
        let blocks_by_id = blocks
            .clone()
            .into_iter()
            .map(|block| (block.id, block))
            .collect::<HashMap<i32, GroupBlock>>();

        let mut graph: DiGraphMap<i32, ()> = DiGraphMap::new();
        for block in blocks {
            graph.add_node(block.id);
        }
        for edge in edges {
            let source_key = BlockKey {
                sequence_hash: edge.source_hash,
                coordinate: edge.source_coordinate,
            };
            let source_id = blocks_by_end.get(&source_key);
            let target_key = BlockKey {
                sequence_hash: edge.target_hash,
                coordinate: edge.target_coordinate,
            };
            let target_id = blocks_by_start.get(&target_key);
            if let Some(source_id_value) = source_id {
                if let Some(target_id_value) = target_id {
                    graph.add_edge(*source_id_value, *target_id_value, ());
                }
            }
        }

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
        let mut sequences = HashSet::<String>::new();

        for start_node in start_nodes {
            for end_node in &end_nodes {
                // TODO: maybe make all_simple_paths return a single path id where start == end
                if start_node == *end_node {
                    let block = blocks_by_id.get(&start_node).unwrap();
                    sequences.insert(block.sequence.clone());
                } else {
                    for path in all_simple_paths(&graph, start_node, *end_node) {
                        let mut current_sequence = "".to_string();
                        for node in path {
                            let block = blocks_by_id.get(&node).unwrap();
                            let block_sequence = block.sequence.clone();
                            current_sequence.push_str(&block_sequence);
                        }
                        sequences.insert(current_sequence);
                    }
                }
            }
        }

        sequences
    }

    pub fn insert_changes(
        conn: &Connection,
        changes: &Vec<PathChange>,
        cache: &PathCache,
    ) -> Vec<BlockGroupEdge> {
        let mut bg_edges = vec![];
        let mut new_edges_by_block_group = HashMap::<i32, Vec<EdgeData>>::new();
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
            let block_group_edges = BlockGroupEdge::bulk_create(conn, block_group_id, edge_ids);
            bg_edges.extend(block_group_edges);
        }
        bg_edges
    }

    #[allow(clippy::ptr_arg)]
    #[allow(clippy::needless_late_init)]
    pub fn insert_change(
        conn: &Connection,
        change: &PathChange,
        tree: &IntervalTree<i32, NewBlock>,
    ) -> Vec<BlockGroupEdge> {
        let new_edges = BlockGroup::set_up_new_edges(change, tree);
        let edge_ids = Edge::bulk_create(conn, new_edges);
        BlockGroupEdge::bulk_create(conn, change.block_group_id, edge_ids)
    }

    pub fn set_up_new_edges(
        change: &PathChange,
        tree: &IntervalTree<i32, NewBlock>,
    ) -> Vec<EdgeData> {
        let start_blocks: Vec<&NewBlock> =
            tree.query_point(change.start).map(|x| &x.value).collect();
        assert_eq!(start_blocks.len(), 1);
        // NOTE: This may not be used but needs to be initialized here instead of inside the if
        // statement that uses it, so that the borrow checker is happy
        let previous_start_blocks: Vec<&NewBlock> = tree
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

        let end_blocks: Vec<&NewBlock> = tree.query_point(change.end).map(|x| &x.value).collect();
        assert_eq!(end_blocks.len(), 1);
        let end_block = end_blocks[0];

        let mut new_edges = vec![];

        if change.block.sequence_start == change.block.sequence_end {
            // Deletion
            let new_edge = EdgeData {
                source_hash: start_block.sequence.hash.clone(),
                source_coordinate: change.start - start_block.path_start
                    + start_block.sequence_start,
                source_strand: Strand::Forward,
                target_hash: end_block.sequence.hash.clone(),
                target_coordinate: change.end - end_block.path_start + end_block.sequence_start,
                target_strand: Strand::Forward,
                chromosome_index: change.chromosome_index,
                phased: change.phased,
            };
            new_edges.push(new_edge);
        } else {
            // Insertion/replacement
            let new_start_edge = EdgeData {
                source_hash: start_block.sequence.hash.clone(),
                source_coordinate: change.start - start_block.path_start
                    + start_block.sequence_start,
                source_strand: Strand::Forward,
                target_hash: change.block.sequence.hash.clone(),
                target_coordinate: change.block.sequence_start,
                target_strand: Strand::Forward,
                chromosome_index: change.chromosome_index,
                phased: change.phased,
            };
            let new_end_edge = EdgeData {
                source_hash: change.block.sequence.hash.clone(),
                source_coordinate: change.block.sequence_end,
                source_strand: Strand::Forward,
                target_hash: end_block.sequence.hash.clone(),
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
    use crate::models::{Collection, Sample};
    use crate::test_helpers::get_connection;

    fn setup_block_group(conn: &Connection) -> (i32, Path) {
        let a_seq = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAAAA")
            .save(conn);
        let t_seq = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTTTT")
            .save(conn);
        let c_seq = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCCCC")
            .save(conn);
        let g_seq = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGGGG")
            .save(conn);
        let _collection = Collection::create(conn, "test");
        let block_group = BlockGroup::create(conn, "test", None, "hg19");
        let edge0 = Edge::create(
            conn,
            Edge::PATH_START_HASH.to_string(),
            0,
            Strand::Forward,
            a_seq.hash.clone(),
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge1 = Edge::create(
            conn,
            a_seq.hash,
            10,
            Strand::Forward,
            t_seq.hash.clone(),
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            t_seq.hash,
            10,
            Strand::Forward,
            c_seq.hash.clone(),
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge3 = Edge::create(
            conn,
            c_seq.hash,
            10,
            Strand::Forward,
            g_seq.hash.clone(),
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge4 = Edge::create(
            conn,
            g_seq.hash,
            10,
            Strand::Forward,
            Edge::PATH_END_HASH.to_string(),
            0,
            Strand::Forward,
            0,
            0,
        );
        BlockGroupEdge::bulk_create(
            conn,
            block_group.id,
            vec![edge0.id, edge1.id, edge2.id, edge3.id, edge4.id],
        );
        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            &[edge0.id, edge1.id, edge2.id, edge3.id, edge4.id],
        );
        (block_group.id, path)
    }

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
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
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
        let deletion = NewBlock {
            id: 0,
            sequence: deletion_sequence.clone(),
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
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
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
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
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
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
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
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
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
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
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
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
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
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
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
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
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
        let deletion = NewBlock {
            id: 0,
            sequence: deletion_sequence.clone(),
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
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
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
}
