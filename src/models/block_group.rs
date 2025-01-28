use core::ops::Range;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use intervaltree::IntervalTree;
use petgraph::graphmap::DiGraphMap;
use petgraph::Direction;
use rusqlite::{params, params_from_iter, types::Value as SQLValue, Connection, Row};
use serde::{Deserialize, Serialize};

use crate::graph::{
    all_intermediate_edges, all_reachable_nodes, all_simple_paths, flatten_to_interval_tree,
    GraphEdge, GraphNode,
};
use crate::models::accession::{Accession, AccessionEdge, AccessionEdgeData, AccessionPath};
use crate::models::block_group_edge::{AugmentedEdgeData, BlockGroupEdge, BlockGroupEdgeData};
use crate::models::edge::{Edge, EdgeData, GroupBlock};
use crate::models::node::{PATH_END_NODE_ID, PATH_START_NODE_ID};
use crate::models::path::{Path, PathBlock, PathData};
use crate::models::path_edge::PathEdge;
use crate::models::strand::Strand;
use crate::models::traits::*;

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
    pub path_accession: Option<String>,
    pub start: i64,
    pub end: i64,
    pub block: PathBlock,
    pub chromosome_index: i64,
    pub phased: i64,
}

pub struct PathCache<'a> {
    pub cache: HashMap<PathData, Path>,
    pub intervaltree_cache: HashMap<Path, IntervalTree<i64, NodeIntervalBlock>>,
    pub conn: &'a Connection,
}

impl PathCache<'_> {
    pub fn new(conn: &Connection) -> PathCache {
        PathCache {
            cache: HashMap::<PathData, Path>::new(),
            intervaltree_cache: HashMap::<Path, IntervalTree<i64, NodeIntervalBlock>>::new(),
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
            let new_path = Path::query(
                path_cache.conn,
                "select * from paths where block_group_id = ?1 AND name = ?2",
                rusqlite::params!(SQLValue::from(block_group_id), SQLValue::from(name)),
            )[0]
            .clone();

            path_cache.cache.insert(path_key, new_path.clone());
            let tree = new_path.intervaltree(path_cache.conn);
            path_cache.intervaltree_cache.insert(new_path.clone(), tree);
            new_path
        }
    }

    pub fn get_intervaltree<'a>(
        path_cache: &'a PathCache<'a>,
        path: &'a Path,
    ) -> Option<&'a IntervalTree<i64, NodeIntervalBlock>> {
        path_cache.intervaltree_cache.get(path)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct NodeIntervalBlock {
    pub block_id: i64,
    pub node_id: i64,
    pub start: i64,
    pub end: i64,
    pub sequence_start: i64,
    pub sequence_end: i64,
    pub strand: Strand,
}
impl BlockGroup {
    pub fn create(
        conn: &Connection,
        collection_name: &str,
        sample_name: Option<&str>,
        name: &str,
    ) -> BlockGroup {
        let query = "INSERT INTO block_groups (collection_name, sample_name, name) VALUES (?1, ?2, ?3) RETURNING *";
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
            Err(rusqlite::Error::SqliteFailure(err, _details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    let bg_id = match sample_name {
                        Some(v) => {conn
                            .query_row(
                                "select id from block_groups where collection_name = ?1 and sample_name = ?2 and name = ?3",
                                (collection_name, v, name),
                                |row| row.get(0),
                            )
                            .unwrap()}
                        None => {
                            conn
                            .query_row(
                                "select id from block_groups where collection_name = ?1 and sample_name is null and name = ?2",
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

    pub fn get_by_id(conn: &Connection, id: i64) -> BlockGroup {
        let query = "SELECT * FROM block_groups WHERE id = ?1";
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
        let existing_paths = Path::query(
            conn,
            "SELECT * from paths where block_group_id = ?1 ORDER BY id ASC;",
            rusqlite::params!(SQLValue::from(source_block_group_id)),
        );

        let augmented_edges = BlockGroupEdge::edges_for_block_group(conn, source_block_group_id);
        let edge_ids = augmented_edges
            .iter()
            .map(|edge| edge.edge.id)
            .collect::<Vec<i64>>();
        let new_block_group_edges = edge_ids
            .iter()
            .enumerate()
            .map(|(i, edge_id)| BlockGroupEdgeData {
                block_group_id: target_block_group_id,
                edge_id: *edge_id,
                chromosome_index: augmented_edges[i].chromosome_index,
                phased: augmented_edges[i].phased,
            })
            .collect::<Vec<_>>();
        BlockGroupEdge::bulk_create(conn, &new_block_group_edges);

        let mut path_map = HashMap::new();

        for path in existing_paths.iter() {
            let edge_ids = PathEdge::edges_for_path(conn, path.id)
                .into_iter()
                .map(|edge| edge.id)
                .collect::<Vec<i64>>();
            let new_path = Path::create(conn, &path.name, target_block_group_id, &edge_ids);
            path_map.insert(path.id, new_path.id);
        }

        for accession in Accession::query(
            conn,
            "select * from accessions where path_id IN rarray(?1);",
            rusqlite::params!(Rc::new(
                existing_paths
                    .iter()
                    .map(|path| SQLValue::from(path.id))
                    .collect::<Vec<SQLValue>>()
            )),
        ) {
            let edges = AccessionPath::query(
                conn,
                "Select * from accession_paths where accession_id = ?1 order by index_in_path ASC;",
                rusqlite::params!(SQLValue::from(accession.id)),
            );
            let new_path_id = path_map[&accession.path_id];
            let obj = Accession::create(
                conn,
                &accession.name,
                new_path_id,
                accession.parent_accession_id,
            )
            .expect("Unable to create accession in clone.");
            AccessionPath::create(
                conn,
                obj.id,
                &edges.iter().map(|ap| ap.edge_id).collect::<Vec<i64>>(),
            );
        }
    }

    pub fn get_or_create_sample_block_group(
        conn: &Connection,
        collection_name: &str,
        sample_name: &str,
        group_name: &str,
        parent_sample: Option<&str>,
    ) -> Result<i64, &'static str> {
        let mut bg_id : i64 = match conn.query_row(
            "select id from block_groups where collection_name = ?1 AND sample_name = ?2 AND name = ?3",
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
            return Ok(bg_id);
        } else {
            // use the base reference group if it exists
            if let Some(parent_sample_name) = parent_sample {
                bg_id = match conn.query_row(
            "select id from block_groups where collection_name = ?1 AND sample_name = ?2 AND name = ?3",
            (collection_name, parent_sample_name, group_name),
            |row| row.get(0),
            ) {
                Ok(res) => res,
                Err(rusqlite::Error::QueryReturnedNoRows) => 0,
                Err(_e) => {
                    panic!("something bad happened querying the database")
                }
            }
            } else {
                bg_id = match conn.query_row(
                    "select id from block_groups where collection_name = ?1 AND sample_name IS null AND name = ?2",
                    (collection_name, group_name),
                    |row| row.get(0),
                ) {
                    Ok(res) => res,
                    Err(rusqlite::Error::QueryReturnedNoRows) => 0,
                    Err(_e) => {
                        panic!("something bad happened querying the database")
                    }
                }
            }
        }
        if bg_id == 0 {
            let error_string = format!(
                "No base path exists for sample {} and graph name {}",
                sample_name, group_name
            );
            return Err(Box::leak(error_string.into_boxed_str()));
        }
        let new_bg_id = BlockGroup::create(conn, collection_name, Some(sample_name), group_name);

        // clone parent blocks/edges/path
        BlockGroup::clone(conn, bg_id, new_bg_id.id);

        Ok(new_bg_id.id)
    }

    pub fn get_id(
        conn: &Connection,
        collection_name: &str,
        sample_name: Option<&str>,
        group_name: &str,
    ) -> i64 {
        let result = if sample_name.is_some() {
            conn.query_row(
		"select id from block_groups where collection_name = ?1 AND sample_name = ?2 AND name = ?3",
		(collection_name, sample_name, group_name),
		|row| row.get(0),
            )
        } else {
            conn.query_row(
		"select id from block_groups where collection_name = ?1 AND sample_name IS NULL AND name = ?2",
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

    pub fn get_graph(conn: &Connection, block_group_id: i64) -> DiGraphMap<GraphNode, GraphEdge> {
        let mut edges = BlockGroupEdge::edges_for_block_group(conn, block_group_id);
        let blocks = Edge::blocks_from_edges(conn, &edges);
        let boundary_edges = Edge::boundary_edges_from_sequences(&blocks);
        edges.extend(boundary_edges.clone());
        let (graph, _) = Edge::build_graph(&edges, &blocks);
        graph
    }

    pub fn prune_graph(graph: &mut DiGraphMap<GraphNode, GraphEdge>) {
        // Prunes a graph by removing edges on the same chromosome_index. This means if 2 edges are
        // both "chromosome index 0", we keep the newer one (newer known by the higher edge id).
        // TODO: This check is not actually right but allows us to test some functionality wrt
        // inherited block groups now. We need to know whether an edge was added to a blockgroup
        // via inheritance or created by it. Because edges can be reused, if an edge created
        // earlier in some other sample is added to a sample, it may be the correct one but have
        // a lower edge id than some edge in the current sample.
        let mut root_nodes = HashSet::new();
        let mut edges_to_remove: Vec<(GraphNode, GraphNode)> = vec![];
        for node in graph.nodes() {
            if node.node_id == PATH_START_NODE_ID {
                root_nodes.insert(node);
            }
            let mut edges_by_ci: HashMap<i64, (i64, GraphNode, GraphNode)> = HashMap::new();
            for (source_node, target_node, edge_weight) in graph.edges(node) {
                edges_by_ci
                    .entry(edge_weight.chromosome_index)
                    .and_modify(|(edge_id, source, target)| {
                        if edge_weight.edge_id > *edge_id {
                            edges_to_remove.push((*source, *target));
                            *edge_id = edge_weight.edge_id;
                            *source = source_node;
                            *target = target_node;
                        } else {
                            edges_to_remove.push((source_node, target_node));
                        }
                    })
                    .or_insert((edge_weight.edge_id, source_node, target_node));
            }
        }

        for (source, target) in edges_to_remove.iter() {
            graph.remove_edge(*source, *target);
        }

        let reachable_nodes = all_reachable_nodes(&*graph, &Vec::from_iter(root_nodes));
        let mut to_remove = vec![];
        for node in graph.nodes() {
            if !reachable_nodes.contains(&node) {
                to_remove.push(node);
            }
        }
        for node in to_remove {
            graph.remove_node(node);
        }
    }

    pub fn get_all_sequences(
        conn: &Connection,
        block_group_id: i64,
        prune: bool,
    ) -> HashSet<String> {
        let mut edges = BlockGroupEdge::edges_for_block_group(conn, block_group_id);
        let blocks = Edge::blocks_from_edges(conn, &edges);
        let boundary_edges = Edge::boundary_edges_from_sequences(&blocks);
        edges.extend(boundary_edges.clone());

        let (mut graph, _) = Edge::build_graph(&edges, &blocks);

        if prune {
            Self::prune_graph(&mut graph);
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
                    let block = blocks_by_id.get(&start_node.block_id).unwrap();
                    if block.node_id != PATH_START_NODE_ID && block.node_id != PATH_END_NODE_ID {
                        sequences.insert(block.sequence());
                    }
                } else {
                    for path in all_simple_paths(&graph, start_node, *end_node) {
                        let mut current_sequence = "".to_string();
                        for node in path {
                            let block = blocks_by_id.get(&node.block_id).unwrap();
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

    pub fn add_accession(
        conn: &Connection,
        path: &Path,
        name: &str,
        start: i64,
        end: i64,
        cache: &mut PathCache,
    ) -> Accession {
        let tree = PathCache::get_intervaltree(cache, path).unwrap();
        let start_blocks: Vec<&NodeIntervalBlock> =
            tree.query_point(start).map(|x| &x.value).collect();
        assert_eq!(start_blocks.len(), 1);
        let start_block = start_blocks[0];
        let end_blocks: Vec<&NodeIntervalBlock> = tree.query_point(end).map(|x| &x.value).collect();
        assert_eq!(end_blocks.len(), 1);
        let end_block = end_blocks[0];
        // we make a start/end edge for the accession start/end, then fill in the middle
        // with any existing edges
        let start_edge = AccessionEdgeData {
            source_node_id: PATH_START_NODE_ID,
            source_coordinate: -1,
            source_strand: Strand::Forward,
            target_node_id: start_block.node_id,
            target_coordinate: start - start_block.start + start_block.sequence_start,
            target_strand: Strand::Forward,
            chromosome_index: 0,
        };
        let end_edge = AccessionEdgeData {
            source_node_id: end_block.node_id,
            source_coordinate: end - end_block.start + end_block.sequence_start,
            source_strand: Strand::Forward,
            target_node_id: PATH_END_NODE_ID,
            target_coordinate: -1,
            target_strand: Strand::Forward,
            chromosome_index: 0,
        };
        let accession =
            Accession::create(conn, name, path.id, None).expect("Unable to create accession.");
        let mut path_edges = vec![start_edge];
        if start_block == end_block {
            path_edges.push(end_edge);
        } else {
            let mut in_range = false;
            let path_blocks: Vec<&NodeIntervalBlock> = tree
                .iter_sorted()
                .map(|x| &x.value)
                .filter(|block| {
                    if block.block_id == start_block.block_id {
                        in_range = true;
                    } else if block.block_id == end_block.block_id {
                        in_range = false;
                        return true;
                    }
                    in_range
                })
                .collect::<Vec<_>>();
            // if start and end block are not the same, we will always have at least 2 elements in path_blocks
            for (block, next_block) in path_blocks.iter().zip(path_blocks[1..].iter()) {
                path_edges.push(AccessionEdgeData {
                    source_node_id: block.node_id,
                    source_coordinate: block.sequence_end,
                    source_strand: block.strand,
                    target_node_id: next_block.node_id,
                    target_coordinate: next_block.sequence_start,
                    target_strand: next_block.strand,
                    chromosome_index: 0,
                })
            }
            path_edges.push(end_edge);
        }
        AccessionPath::create(
            conn,
            accession.id,
            &AccessionEdge::bulk_create(conn, &path_edges),
        );
        accession
    }

    pub fn insert_changes(
        conn: &Connection,
        changes: &Vec<PathChange>,
        cache: &mut PathCache,
        modify_blockgroup: bool,
    ) {
        let mut new_augmented_edges_by_block_group = HashMap::<i64, Vec<AugmentedEdgeData>>::new();
        let mut new_accession_edges = HashMap::new();
        let mut tree_map = HashMap::new();
        for change in changes {
            let tree = if modify_blockgroup {
                tree_map.entry(change.block_group_id).or_insert_with(|| {
                    BlockGroup::intervaltree_for(conn, change.block_group_id, true)
                })
            } else {
                PathCache::get_intervaltree(cache, &change.path).unwrap()
            };
            let new_augmented_edges = BlockGroup::set_up_new_edges(change, tree);
            new_augmented_edges_by_block_group
                .entry(change.block_group_id)
                .and_modify(|new_edge_data| new_edge_data.extend(new_augmented_edges.clone()))
                .or_insert_with(|| new_augmented_edges.clone());
            if let Some(accession) = &change.path_accession {
                new_accession_edges
                    .entry((&change.path, accession))
                    .and_modify(|new_edge_data: &mut Vec<AugmentedEdgeData>| {
                        new_edge_data.extend(new_augmented_edges.clone())
                    })
                    .or_insert_with(|| new_augmented_edges.clone());
            }
        }

        let mut edge_data_map = HashMap::new();

        for (block_group_id, new_augmented_edges) in new_augmented_edges_by_block_group {
            let new_edges = new_augmented_edges
                .iter()
                .map(|augmented_edge| augmented_edge.edge_data.clone())
                .collect::<Vec<_>>();
            let edge_ids = Edge::bulk_create(conn, &new_edges);
            for (i, edge_data) in new_edges.iter().enumerate() {
                edge_data_map.insert(edge_data.clone(), edge_ids[i]);
            }
            let new_block_group_edges = edge_ids
                .iter()
                .enumerate()
                .map(|(i, edge_id)| BlockGroupEdgeData {
                    block_group_id,
                    edge_id: *edge_id,
                    chromosome_index: new_augmented_edges[i].chromosome_index,
                    phased: new_augmented_edges[i].phased,
                })
                .collect::<Vec<_>>();
            BlockGroupEdge::bulk_create(conn, &new_block_group_edges);
        }

        for ((path, accession_name), path_edges) in new_accession_edges {
            match Accession::get(
                conn,
                "select * from accessions where name = ?1 AND path_id = ?2",
                params![
                    SQLValue::from(accession_name.clone()),
                    SQLValue::from(path.id),
                ],
            ) {
                Ok(_) => {
                    println!("accession already exists, consider a better matching algorithm to determine if this is an error.");
                }
                Err(_) => {
                    let acc_edges = AccessionEdge::bulk_create(
                        conn,
                        &path_edges.iter().map(AccessionEdgeData::from).collect(),
                    );
                    let acc = Accession::create(conn, accession_name, path.id, None)
                        .expect("Accession could not be created.");
                    AccessionPath::create(conn, acc.id, &acc_edges);
                }
            }
        }
    }

    #[allow(clippy::ptr_arg)]
    #[allow(clippy::needless_late_init)]
    pub fn insert_change(
        conn: &Connection,
        change: &PathChange,
        tree: &IntervalTree<i64, NodeIntervalBlock>,
    ) {
        let new_augmented_edges = BlockGroup::set_up_new_edges(change, tree);
        let new_edges = new_augmented_edges
            .iter()
            .map(|augmented_edge| augmented_edge.edge_data.clone())
            .collect::<Vec<_>>();
        let edge_ids = Edge::bulk_create(conn, &new_edges);
        let new_block_group_edges = edge_ids
            .iter()
            .enumerate()
            .map(|(i, edge_id)| BlockGroupEdgeData {
                block_group_id: change.block_group_id,
                edge_id: *edge_id,
                chromosome_index: new_augmented_edges[i].chromosome_index,
                phased: new_augmented_edges[i].phased,
            })
            .collect::<Vec<_>>();
        BlockGroupEdge::bulk_create(conn, &new_block_group_edges);
    }

    fn set_up_new_edges(
        change: &PathChange,
        tree: &IntervalTree<i64, NodeIntervalBlock>,
    ) -> Vec<AugmentedEdgeData> {
        let start_blocks: Vec<&NodeIntervalBlock> =
            tree.query_point(change.start).map(|x| &x.value).collect();
        assert_eq!(start_blocks.len(), 1);
        // NOTE: This may not be used but needs to be initialized here instead of inside the if
        // statement that uses it, so that the borrow checker is happy
        let previous_start_blocks: Vec<&NodeIntervalBlock> = tree
            .query_point(change.start - 1)
            .map(|x| &x.value)
            .collect();
        assert_eq!(previous_start_blocks.len(), 1);
        let start_block = if start_blocks[0].start == change.start {
            // First part of this block will be replaced/deleted, need to get previous block to add
            // edge including it
            previous_start_blocks[0]
        } else {
            start_blocks[0]
        };

        let end_blocks: Vec<&NodeIntervalBlock> =
            tree.query_point(change.end).map(|x| &x.value).collect();
        assert_eq!(end_blocks.len(), 1);
        let end_block = end_blocks[0];

        let mut new_edges = vec![];

        if change.block.sequence_start == change.block.sequence_end {
            // Deletion
            let new_edge = EdgeData {
                source_node_id: start_block.node_id,
                source_coordinate: change.start - start_block.start + start_block.sequence_start,
                source_strand: Strand::Forward,
                target_node_id: end_block.node_id,
                target_coordinate: change.end - end_block.start + end_block.sequence_start,
                target_strand: Strand::Forward,
            };
            let new_augmented_edge = AugmentedEdgeData {
                edge_data: new_edge,
                chromosome_index: change.chromosome_index,
                phased: change.phased,
            };
            new_edges.push(new_augmented_edge);

            // NOTE: If the deletion is happening at the very beginning of a path, we need to add
            // an edge from the dedicated start node to the end of the deletion, to indicate it's
            // another start point in the block group DAG.
            if change.start == 0 {
                let new_beginning_edge = EdgeData {
                    source_node_id: PATH_START_NODE_ID,
                    source_coordinate: 0,
                    source_strand: Strand::Forward,
                    target_node_id: end_block.node_id,
                    target_coordinate: change.end - end_block.start + end_block.sequence_start,
                    target_strand: Strand::Forward,
                };
                let new_augmented_edge = AugmentedEdgeData {
                    edge_data: new_beginning_edge,
                    chromosome_index: change.chromosome_index,
                    phased: change.phased,
                };
                new_edges.push(new_augmented_edge);
            }
        // NOTE: If the deletion is happening at the very end of a path, we might add an edge
        // from the beginning of the deletion to the dedicated end node, but in practice it
        // doesn't affect sequence readouts, so it may not be worth it.
        } else {
            // Insertion/replacement
            let new_start_edge = EdgeData {
                source_node_id: start_block.node_id,
                source_coordinate: change.start - start_block.start + start_block.sequence_start,
                source_strand: Strand::Forward,
                target_node_id: change.block.node_id,
                target_coordinate: change.block.sequence_start,
                target_strand: Strand::Forward,
            };
            let new_augmented_start_edge = AugmentedEdgeData {
                edge_data: new_start_edge,
                chromosome_index: change.chromosome_index,
                phased: change.phased,
            };
            let new_end_edge = EdgeData {
                source_node_id: change.block.node_id,
                source_coordinate: change.block.sequence_end,
                source_strand: Strand::Forward,
                target_node_id: end_block.node_id,
                target_coordinate: change.end - end_block.start + end_block.sequence_start,
                target_strand: Strand::Forward,
            };
            let new_augmented_end_edge = AugmentedEdgeData {
                edge_data: new_end_edge,
                chromosome_index: change.chromosome_index,
                phased: change.phased,
            };
            new_edges.push(new_augmented_start_edge);
            new_edges.push(new_augmented_end_edge);
        }

        new_edges
    }

    pub fn intervaltree_for(
        conn: &Connection,
        block_group_id: i64,
        remove_ambiguous_positions: bool,
    ) -> IntervalTree<i64, NodeIntervalBlock> {
        // make a tree where every node has a span in the graph.
        let mut graph = BlockGroup::get_graph(conn, block_group_id);
        BlockGroup::prune_graph(&mut graph);
        flatten_to_interval_tree(&graph, remove_ambiguous_positions)
    }

    pub fn get_current_path(conn: &Connection, block_group_id: i64) -> Path {
        let paths = Path::query(
            conn,
            "SELECT * FROM paths WHERE block_group_id = ?1 ORDER BY id DESC",
            rusqlite::params!(SQLValue::from(block_group_id)),
        );
        paths[0].clone()
    }

    pub fn clone_subgraph(
        conn: &Connection,
        source_block_group_id: i64,
        start: i64,
        end: i64,
        target_block_group_id: i64,
    ) {
        let current_path = BlockGroup::get_current_path(conn, source_block_group_id);
        let current_intervaltree = current_path.intervaltree(conn);
        let mut blocks = current_intervaltree
            .query(Range { start, end })
            .map(|x| x.value)
            .collect::<Vec<_>>();
        blocks.sort_by(|a, b| a.start.cmp(&b.start));
        let start_block = blocks[0];
        let start_node_coordinate = start - start_block.start + start_block.sequence_start;
        let end_block = blocks[blocks.len() - 1];
        let end_node_coordinate = end - end_block.start + end_block.sequence_start;

        let current_graph = BlockGroup::get_graph(conn, source_block_group_id);
        let start_node = current_graph
            .nodes()
            .find(|node| {
                node.node_id == start_block.node_id
                    && node.sequence_start == start_block.sequence_start
            })
            .unwrap();
        let end_node = current_graph
            .nodes()
            .find(|node| {
                node.node_id == end_block.node_id && node.sequence_start == end_block.sequence_start
            })
            .unwrap();
        let subgraph_edges = all_intermediate_edges(&current_graph, start_node, end_node);

        // Filter out internal edges (boundary edges) that don't exist in the database
        let subgraph_edge_ids = subgraph_edges
            .iter()
            .map(|(_to, _from, edge_info)| edge_info.edge_id)
            .collect::<Vec<_>>();
        let source_edges = Edge::bulk_load(conn, &subgraph_edge_ids);

        let source_block_group_edges =
            BlockGroupEdge::edges_for_block_group(conn, source_block_group_id);
        let source_edge_ids = source_edges
            .iter()
            .map(|edge| edge.id)
            .collect::<HashSet<_>>();
        let source_block_group_edges = source_block_group_edges
            .iter()
            .filter(|block_group_edge| source_edge_ids.contains(&block_group_edge.edge.id))
            .collect::<Vec<_>>();
        let source_block_group_edges_by_edge_id = source_block_group_edges
            .iter()
            .map(|block_group_edge| (block_group_edge.edge.id, block_group_edge))
            .collect::<HashMap<_, _>>();

        let subgraph_edge_inputs = source_block_group_edges
            .iter()
            .map(|edge| {
                let block_group_edge = source_block_group_edges_by_edge_id
                    .get(&edge.edge.id)
                    .unwrap();
                BlockGroupEdgeData {
                    block_group_id: target_block_group_id,
                    edge_id: edge.edge.id,
                    chromosome_index: block_group_edge.chromosome_index,
                    phased: block_group_edge.phased,
                }
            })
            .collect::<Vec<_>>();

        let new_start_edge = Edge::create(
            conn,
            PATH_START_NODE_ID,
            -1,
            Strand::Forward,
            start_block.node_id,
            start_node_coordinate,
            start_block.strand,
        );
        let new_start_edge_data = BlockGroupEdgeData {
            block_group_id: target_block_group_id,
            edge_id: new_start_edge.id,
            chromosome_index: 0,
            phased: 0,
        };
        let new_end_edge = Edge::create(
            conn,
            end_block.node_id,
            end_node_coordinate,
            end_block.strand,
            PATH_END_NODE_ID,
            -1,
            Strand::Forward,
        );
        let new_end_edge_data = BlockGroupEdgeData {
            block_group_id: target_block_group_id,
            edge_id: new_end_edge.id,
            chromosome_index: 0,
            phased: 0,
        };
        let mut all_edges = subgraph_edge_inputs.clone();
        all_edges.push(new_start_edge_data);
        all_edges.push(new_end_edge_data);
        BlockGroupEdge::bulk_create(conn, &all_edges);

        let current_edges = PathEdge::edges_for_path(conn, current_path.id);
        let new_edge_id_set = all_edges
            .iter()
            .map(|edge| edge.edge_id)
            .collect::<HashSet<_>>();
        let mut new_path_edge_ids = vec![];
        new_path_edge_ids.push(new_start_edge.id);
        for current_edge in current_edges {
            if new_edge_id_set.contains(&current_edge.id) {
                new_path_edge_ids.push(current_edge.id);
            }
        }
        new_path_edge_ids.push(new_end_edge.id);
        Path::create(
            conn,
            &current_path.name,
            target_block_group_id,
            &new_path_edge_ids,
        );
    }
}

impl Query for BlockGroup {
    type Model = BlockGroup;
    fn process_row(row: &Row) -> Self::Model {
        BlockGroup {
            id: row.get(0).unwrap(),
            collection_name: row.get(1).unwrap(),
            sample_name: row.get(2).unwrap(),
            name: row.get(3).unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{collection::Collection, node::Node, sample::Sample, sequence::Sequence};
    use crate::test_helpers::{get_connection, interval_tree_verify, setup_block_group};

    #[test]
    fn test_blockgroup_create() {
        let conn = &get_connection(None);
        Collection::create(conn, "test");
        let bg1 = BlockGroup::create(conn, "test", None, "hg19");
        assert_eq!(bg1.collection_name, "test");
        assert_eq!(bg1.name, "hg19");
        Sample::get_or_create(conn, "sample");
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
        Sample::get_or_create(conn, "sample");
        let bg2 =
            BlockGroup::get_or_create_sample_block_group(conn, "test", "sample", "hg19", None)
                .unwrap();
        assert_eq!(
            BlockGroupEdge::edges_for_block_group(conn, bg1.id),
            BlockGroupEdge::edges_for_block_group(conn, bg2)
        );
    }

    #[test]
    fn test_blockgroup_clone_passes_accessions() {
        let conn = &get_connection(None);
        let (bg_1, path) = setup_block_group(conn);
        let mut path_cache = PathCache::new(conn);
        PathCache::lookup(&mut path_cache, bg_1, path.name.clone());
        let acc_1 = BlockGroup::add_accession(conn, &path, "test", 3, 7, &mut path_cache);
        assert_eq!(
            Accession::query(
                conn,
                "select * from accessions where name = ?1",
                rusqlite::params!(SQLValue::from("test".to_string())),
            ),
            vec![Accession {
                id: acc_1.id,
                name: "test".to_string(),
                path_id: path.id,
                parent_accession_id: None,
            }]
        );

        Sample::get_or_create(conn, "sample2");
        let _bg2 =
            BlockGroup::get_or_create_sample_block_group(conn, "test", "sample2", "chr1", None)
                .unwrap();
        assert_eq!(
            Accession::query(
                conn,
                "select * from accessions where name = ?1",
                rusqlite::params!(SQLValue::from("test".to_string())),
            ),
            vec![
                Accession {
                    id: acc_1.id,
                    name: "test".to_string(),
                    path_id: path.id,
                    parent_accession_id: None,
                },
                Accession {
                    id: acc_1.id + 1,
                    name: "test".to_string(),
                    path_id: path.id + 1,
                    parent_accession_id: None,
                }
            ]
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
            path_accession: None,
            start: 7,
            end: 15,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 19,
            end: 31,
            block: deletion,
            chromosome_index: 1,
            phased: 0,
        };
        // take out an entire block.
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);
        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 7,
            end: 15,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 15,
            end: 15,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 12,
            end: 17,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 10,
            end: 10,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 9,
            end: 9,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 10,
            end: 20,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 15,
            end: 25,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 5,
            end: 35,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 19,
            end: 31,
            block: deletion,
            chromosome_index: 1,
            phased: 0,
        };

        // take out an entire block.
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);
        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 7,
            end: 15,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string()
            ])
        );

        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 0,
            end: 0,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 40,
            end: 40,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 10,
            end: 11,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 19,
            end: 20,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 0,
            end: 1,
            block: deletion,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 35,
            end: 40,
            block: deletion,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 10,
            end: 12,
            block: deletion,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
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
            path_accession: None,
            start: 18,
            end: 20,
            block: deletion,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(&conn);
        BlockGroup::insert_change(&conn, &change, &tree);

        let all_sequences = BlockGroup::get_all_sequences(&conn, block_group_id, false);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
            ])
        );
    }

    #[test]
    fn test_blockgroup_interval_tree() {
        let conn = &get_connection(None);
        let (block_group_id, path) = setup_block_group(conn);
        let _new_sample = Sample::get_or_create(conn, "child");
        let new_bg_id =
            BlockGroup::get_or_create_sample_block_group(conn, "test", "child", "chr1", None)
                .unwrap();
        let new_path = Path::query(
            conn,
            "select * from paths where block_group_id = ?1",
            rusqlite::params!(SQLValue::from(new_bg_id)),
        );
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(conn);
        let insert_node_id = Node::create(conn, insert_sequence.hash.as_str(), None);
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
            block_group_id: new_bg_id,
            path: new_path[0].clone(),
            path_accession: None,
            start: 7,
            end: 15,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        let tree = path.intervaltree(conn);
        BlockGroup::insert_change(conn, &change, &tree);

        let tree = BlockGroup::intervaltree_for(conn, block_group_id, false);
        let tree2 = BlockGroup::intervaltree_for(conn, block_group_id, true);
        interval_tree_verify(
            &tree,
            3,
            &[NodeIntervalBlock {
                block_id: 3,
                node_id: 3,
                start: 0,
                end: 10,
                sequence_start: 0,
                sequence_end: 10,
                strand: Strand::Forward,
            }],
        );
        interval_tree_verify(
            &tree2,
            3,
            &[NodeIntervalBlock {
                block_id: 3,
                node_id: 3,
                start: 0,
                end: 10,
                sequence_start: 0,
                sequence_end: 10,
                strand: Strand::Forward,
            }],
        );
        interval_tree_verify(
            &tree,
            35,
            &[NodeIntervalBlock {
                block_id: 0,
                node_id: 6,
                start: 30,
                end: 40,
                sequence_start: 0,
                sequence_end: 10,
                strand: Strand::Forward,
            }],
        );
        interval_tree_verify(
            &tree2,
            35,
            &[NodeIntervalBlock {
                block_id: 0,
                node_id: 6,
                start: 30,
                end: 40,
                sequence_start: 0,
                sequence_end: 10,
                strand: Strand::Forward,
            }],
        );

        // This blockgroup has a change from positions 7-15 of 4 base pairs -- so any changes after this will be ambiguous
        let tree = BlockGroup::intervaltree_for(conn, new_bg_id, false);
        let tree2 = BlockGroup::intervaltree_for(conn, new_bg_id, true);
        interval_tree_verify(
            &tree,
            3,
            &[NodeIntervalBlock {
                block_id: 5,
                node_id: 3,
                start: 0,
                end: 7,
                sequence_start: 0,
                sequence_end: 7,
                strand: Strand::Forward,
            }],
        );
        interval_tree_verify(
            &tree2,
            3,
            &[NodeIntervalBlock {
                block_id: 5,
                node_id: 3,
                start: 0,
                end: 7,
                sequence_start: 0,
                sequence_end: 7,
                strand: Strand::Forward,
            }],
        );
        interval_tree_verify(
            &tree,
            30,
            &[
                NodeIntervalBlock {
                    block_id: 0,
                    node_id: 6,
                    start: 26,
                    end: 36,
                    sequence_start: 0,
                    sequence_end: 10,
                    strand: Strand::Forward,
                },
                NodeIntervalBlock {
                    block_id: 0,
                    node_id: 6,
                    start: 30,
                    end: 40,
                    sequence_start: 0,
                    sequence_end: 10,
                    strand: Strand::Forward,
                },
            ],
        );
        interval_tree_verify(&tree2, 30, &[]);
        // TODO: This case should return [] because there are 2 distinct nodes at this position and thus it is ambiguous.
        // currently, the caller needs to filter these out.
        interval_tree_verify(
            &tree2,
            9,
            &[
                NodeIntervalBlock {
                    block_id: 4,
                    node_id: 7,
                    start: 7,
                    end: 11,
                    sequence_start: 0,
                    sequence_end: 4,
                    strand: Strand::Forward,
                },
                NodeIntervalBlock {
                    block_id: 6,
                    node_id: 3,
                    start: 7,
                    end: 10,
                    sequence_start: 7,
                    sequence_end: 10,
                    strand: Strand::Forward,
                },
            ],
        );
    }

    #[test]
    fn test_changes_against_derivative_blockgroups() {
        let conn = &get_connection(None);
        let (_block_group_id, _path) = setup_block_group(conn);
        let _new_sample = Sample::get_or_create(conn, "child");
        let new_bg_id =
            BlockGroup::get_or_create_sample_block_group(conn, "test", "child", "chr1", None)
                .unwrap();
        let new_path = Path::query(
            conn,
            "select * from paths where block_group_id = ?1",
            rusqlite::params!(SQLValue::from(new_bg_id)),
        );
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(conn);
        let insert_node_id = Node::create(conn, insert_sequence.hash.as_str(), None);
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
            block_group_id: new_bg_id,
            path: new_path[0].clone(),
            path_accession: None,
            start: 7,
            end: 15,
            block: insert,
            chromosome_index: 0,
            phased: 0,
        };
        // note we are making our change against the new blockgroup, and not the parent blockgroup
        let tree = BlockGroup::intervaltree_for(conn, new_bg_id, true);
        BlockGroup::insert_change(conn, &change, &tree);
        let all_sequences = BlockGroup::get_all_sequences(conn, new_bg_id, true);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec!["AAAAAAANNNNTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),])
        );

        // Now, we make a change against another descendant
        let _new_sample = Sample::get_or_create(conn, "grandchild");
        let gc_bg_id = BlockGroup::get_or_create_sample_block_group(
            conn,
            "test",
            "grandchild",
            "chr1",
            Some("child"),
        )
        .unwrap();
        let new_path = Path::query(
            conn,
            "select * from paths where block_group_id = ?1",
            rusqlite::params!(SQLValue::from(gc_bg_id)),
        );

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
            block_group_id: gc_bg_id,
            path: new_path[0].clone(),
            path_accession: None,
            start: 7,
            end: 15,
            block: insert,
            chromosome_index: 0,
            phased: 0,
        };
        // take out an entire block.
        let tree = BlockGroup::intervaltree_for(conn, gc_bg_id, true);
        BlockGroup::insert_change(conn, &change, &tree);
        let all_sequences = BlockGroup::get_all_sequences(conn, gc_bg_id, true);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec!["AAAAAAANNNNTCCCCCCCCCCGGGGGGGGGG".to_string(),])
        )
    }

    #[test]
    fn test_changes_against_derivative_diploid_blockgroups() {
        // This test ensures that if we have heterozygous changes that do not introduce frameshifts,
        // we can modify regions downstream of them.
        let conn = &get_connection(None);
        let (_block_group_id, _path) = setup_block_group(conn);
        let _new_sample = Sample::get_or_create(conn, "child");
        let new_bg_id =
            BlockGroup::get_or_create_sample_block_group(conn, "test", "child", "chr1", None)
                .unwrap();
        let new_path = Path::query(
            conn,
            "select * from paths where block_group_id = ?1",
            rusqlite::params!(SQLValue::from(new_bg_id)),
        );
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(conn);
        let insert_node_id = Node::create(conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 11,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id: new_bg_id,
            path: new_path[0].clone(),
            path_accession: None,
            start: 7,
            end: 11,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        // note we are making our change against the new blockgroup, and not the parent blockgroup
        let tree = BlockGroup::intervaltree_for(conn, new_bg_id, true);
        BlockGroup::insert_change(conn, &change, &tree);
        let all_sequences = BlockGroup::get_all_sequences(conn, new_bg_id, true);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAANNNNTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
            ])
        );

        // Now, we make a change against another descendant
        let _new_sample = Sample::get_or_create(conn, "grandchild");
        let gc_bg_id = BlockGroup::get_or_create_sample_block_group(
            conn,
            "test",
            "grandchild",
            "chr1",
            Some("child"),
        )
        .unwrap();
        let new_path = Path::query(
            conn,
            "select * from paths where block_group_id = ?1",
            rusqlite::params!(SQLValue::from(gc_bg_id)),
        );

        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(conn);
        let insert_node_id =
            Node::create(conn, insert_sequence.hash.as_str(), "new-hash".to_string());

        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 20,
            path_end: 24,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id: gc_bg_id,
            path: new_path[0].clone(),
            path_accession: None,
            start: 20,
            end: 24,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        // take out an entire block.
        let tree = BlockGroup::intervaltree_for(conn, gc_bg_id, true);
        BlockGroup::insert_change(conn, &change, &tree);
        let all_sequences = BlockGroup::get_all_sequences(conn, gc_bg_id, true);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAANNNNTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTTTNNNNCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAANNNNTTTTTTTTTNNNNCCCCCCGGGGGGGGGG".to_string()
            ])
        )
    }

    #[test]
    #[should_panic]
    fn test_prohibits_out_of_frame_changes_against_derivative_diploid_blockgroups() {
        // This test ensures that we do not allow ambiguous changes by coordinates
        let conn = &get_connection(None);
        let (_block_group_id, _path) = setup_block_group(conn);
        let _new_sample = Sample::get_or_create(conn, "child");
        let new_bg_id =
            BlockGroup::get_or_create_sample_block_group(conn, "test", "child", "chr1", None)
                .unwrap();
        let new_path = Path::query(
            conn,
            "select * from paths where block_group_id = ?1",
            rusqlite::params!(SQLValue::from(new_bg_id)),
        );
        // This is a heterozygous replacement of 5 bases with 4 bases, so positions
        // downstream of this are not addressable.
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(conn);
        let insert_node_id = Node::create(conn, insert_sequence.hash.as_str(), None);
        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 12,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id: new_bg_id,
            path: new_path[0].clone(),
            path_accession: None,
            start: 7,
            end: 12,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        // note we are making our change against the new blockgroup, and not the parent blockgroup
        let tree = BlockGroup::intervaltree_for(conn, new_bg_id, true);
        BlockGroup::insert_change(conn, &change, &tree);
        let all_sequences = BlockGroup::get_all_sequences(conn, new_bg_id, true);
        assert_eq!(
            all_sequences,
            HashSet::from_iter(vec![
                "AAAAAAANNNNTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
            ])
        );

        // Now, we make a change against another descendant and get an error
        let _new_sample = Sample::get_or_create(conn, "grandchild");
        let gc_bg_id = BlockGroup::get_or_create_sample_block_group(
            conn,
            "test",
            "grandchild",
            "chr1",
            Some("child"),
        )
        .unwrap();
        let new_path = Path::query(
            conn,
            "select * from paths where block_group_id = ?1",
            rusqlite::params!(SQLValue::from(gc_bg_id)),
        );

        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(conn);
        let insert_node_id =
            Node::create(conn, insert_sequence.hash.as_str(), "new-hash".to_string());

        let insert = PathBlock {
            id: 0,
            node_id: insert_node_id,
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 20,
            path_end: 24,
            strand: Strand::Forward,
        };
        let change = PathChange {
            block_group_id: gc_bg_id,
            path: new_path[0].clone(),
            path_accession: None,
            start: 20,
            end: 24,
            block: insert,
            chromosome_index: 1,
            phased: 0,
        };
        // take out an entire block.
        let tree = BlockGroup::intervaltree_for(conn, gc_bg_id, true);
        BlockGroup::insert_change(conn, &change, &tree);
    }

    mod test_clone_subgraph {
        use super::*;
        use crate::models::{collection::Collection, node::Node, sequence::Sequence};
        use crate::test_helpers::{get_connection, setup_block_group};

        #[test]
        fn test_clone_subgraph_one_insertion() {
            /*
            AAAAAAAAAA -> TTTTTTTTTT -> CCCCCCCCCC -> GGGGGGGGGG
                              \-> AAAAAAAA ->/
            Subgraph range:  |-----------------|
            Sequences of the subgraph are TAAAAAAAAC, TTTTTCCCCC
             */
            let conn = &get_connection(None);
            Collection::create(conn, "test");
            let (block_group1_id, original_path) = setup_block_group(conn);

            let intervaltree = original_path.intervaltree(conn);
            let insert_start_node_id = intervaltree.query_point(16).next().unwrap().value.node_id;
            let insert_end_node_id = intervaltree.query_point(24).next().unwrap().value.node_id;

            let insert_sequence = Sequence::new()
                .sequence_type("DNA")
                .sequence("AAAAAAAA")
                .save(conn);
            let insert_node_id = Node::create(conn, insert_sequence.hash.as_str(), None);
            let edge_into_insert = Edge::create(
                conn,
                insert_start_node_id,
                6,
                Strand::Forward,
                insert_node_id,
                0,
                Strand::Forward,
            );
            let edge_out_of_insert = Edge::create(
                conn,
                insert_node_id,
                8,
                Strand::Forward,
                insert_end_node_id,
                4,
                Strand::Forward,
            );

            let edge_ids = [edge_into_insert.id, edge_out_of_insert.id];
            let block_group_edges = edge_ids
                .iter()
                .map(|edge_id| BlockGroupEdgeData {
                    block_group_id: block_group1_id,
                    edge_id: *edge_id,
                    chromosome_index: 0,
                    phased: 0,
                })
                .collect::<Vec<BlockGroupEdgeData>>();
            BlockGroupEdge::bulk_create(conn, &block_group_edges);

            let insert_path =
                original_path.new_path_with(conn, 16, 24, &edge_into_insert, &edge_out_of_insert);
            assert_eq!(
                insert_path.sequence(conn),
                "AAAAAAAAAATTTTTTAAAAAAAACCCCCCGGGGGGGGGG"
            );

            let all_sequences = BlockGroup::get_all_sequences(conn, block_group1_id, false);
            assert_eq!(
                all_sequences,
                HashSet::from_iter(vec![
                    "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                    "AAAAAAAAAATTTTTTAAAAAAAACCCCCCGGGGGGGGGG".to_string(),
                ])
            );

            let block_group2 = BlockGroup::create(conn, "test", None, "chr1.1");
            BlockGroup::clone_subgraph(conn, block_group1_id, 15, 25, block_group2.id);
            let all_sequences2 = BlockGroup::get_all_sequences(conn, block_group2.id, false);
            assert_eq!(
                all_sequences2,
                HashSet::from_iter(vec!["TTTTTCCCCC".to_string(), "TAAAAAAAAC".to_string(),])
            );

            let new_path = BlockGroup::get_current_path(conn, block_group2.id);
            assert_eq!(new_path.sequence(conn), "TAAAAAAAAC");
        }

        #[test]
        fn test_clone_subgraph_two_independent_insertions() {
            /*
            AAAAAAAAAA -> TTTTTTTTTT -> CCCCCCCCCC -----> GGGGGGGGGG
                                  \-> AAAAAAAA ->/  \->TTTTTTTT -/
            Subgraph range:     |----------------------------------|
             */
            let conn = &get_connection(None);
            Collection::create(conn, "test");
            let (block_group1_id, original_path) = setup_block_group(conn);

            let intervaltree = original_path.intervaltree(conn);
            let insert_start_node_id = intervaltree.query_point(16).next().unwrap().value.node_id;
            let insert_end_node_id = intervaltree.query_point(24).next().unwrap().value.node_id;

            let insert_sequence = Sequence::new()
                .sequence_type("DNA")
                .sequence("AAAAAAAA")
                .save(conn);
            let insert_node_id = Node::create(conn, insert_sequence.hash.as_str(), None);
            let edge_into_insert = Edge::create(
                conn,
                insert_start_node_id,
                6,
                Strand::Forward,
                insert_node_id,
                0,
                Strand::Forward,
            );
            let edge_out_of_insert = Edge::create(
                conn,
                insert_node_id,
                8,
                Strand::Forward,
                insert_end_node_id,
                4,
                Strand::Forward,
            );

            let edge_ids = [edge_into_insert.id, edge_out_of_insert.id];
            let block_group_edges = edge_ids
                .iter()
                .map(|edge_id| BlockGroupEdgeData {
                    block_group_id: block_group1_id,
                    edge_id: *edge_id,
                    chromosome_index: 0,
                    phased: 0,
                })
                .collect::<Vec<BlockGroupEdgeData>>();
            BlockGroupEdge::bulk_create(conn, &block_group_edges);

            let insert_path =
                original_path.new_path_with(conn, 16, 24, &edge_into_insert, &edge_out_of_insert);
            assert_eq!(
                insert_path.sequence(conn),
                "AAAAAAAAAATTTTTTAAAAAAAACCCCCCGGGGGGGGGG"
            );

            let insert2_start_node_id = intervaltree.query_point(28).next().unwrap().value.node_id;
            let insert2_end_node_id = intervaltree.query_point(32).next().unwrap().value.node_id;

            let insert2_sequence = Sequence::new()
                .sequence_type("DNA")
                .sequence("TTTTTTTT")
                .save(conn);
            let insert2_node_id = Node::create(conn, insert2_sequence.hash.as_str(), None);
            let edge_into_insert2 = Edge::create(
                conn,
                insert2_start_node_id,
                6,
                Strand::Forward,
                insert2_node_id,
                0,
                Strand::Forward,
            );
            let edge_out_of_insert2 = Edge::create(
                conn,
                insert2_node_id,
                8,
                Strand::Forward,
                insert2_end_node_id,
                4,
                Strand::Forward,
            );

            let edge_ids = [edge_into_insert2.id, edge_out_of_insert2.id];
            let block_group_edges = edge_ids
                .iter()
                .map(|edge_id| BlockGroupEdgeData {
                    block_group_id: block_group1_id,
                    edge_id: *edge_id,
                    chromosome_index: 0,
                    phased: 0,
                })
                .collect::<Vec<BlockGroupEdgeData>>();
            BlockGroupEdge::bulk_create(conn, &block_group_edges);

            let insert2_path =
                insert_path.new_path_with(conn, 28, 32, &edge_into_insert2, &edge_out_of_insert2);
            assert_eq!(
                insert2_path.sequence(conn),
                "AAAAAAAAAATTTTTTAAAAAAAACCTTTTTTTTGGGGGG"
            );

            let all_sequences = BlockGroup::get_all_sequences(conn, block_group1_id, false);
            assert_eq!(
                all_sequences,
                HashSet::from_iter(vec![
                    "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                    "AAAAAAAAAATTTTTTAAAAAAAACCCCCCGGGGGGGGGG".to_string(),
                    "AAAAAAAAAATTTTTTTTTTCCCCCCTTTTTTTTGGGGGG".to_string(),
                    "AAAAAAAAAATTTTTTAAAAAAAACCTTTTTTTTGGGGGG".to_string(),
                ])
            );

            let block_group2 = BlockGroup::create(conn, "test", None, "chr1.1");
            BlockGroup::clone_subgraph(conn, block_group1_id, 15, 36, block_group2.id);
            let all_sequences2 = BlockGroup::get_all_sequences(conn, block_group2.id, false);
            assert_eq!(
                all_sequences2,
                HashSet::from_iter(vec![
                    "TTTTTCCCCCCCCCCGGGGGG".to_string(),
                    "TAAAAAAAACCCCCCGGGGGG".to_string(),
                    "TTTTTCCCCCCTTTTTTTTGG".to_string(),
                    "TAAAAAAAACCTTTTTTTTGG".to_string(),
                ])
            );
        }

        #[test]
        fn test_clone_subgraph_two_independent_insertions_and_one_deletion() {
            /*
                       /--------------------------------------------\  (<-- Deletion edge)
            AAAAAAAAAA -> TTTTTTTTTT -> CCCCCCCCCC -----> GGGGGGGGGG
                                  \-> AAAAAAAA ->/  \->TTTTTTTT -/
            Subgraph range: |----------------------------------|

            Confirms that deletion edge is ignored and not added to subgraph
             */
            let conn = &get_connection(None);
            Collection::create(conn, "test");
            let (block_group1_id, original_path) = setup_block_group(conn);

            let intervaltree = original_path.intervaltree(conn);
            let insert_start_node_id = intervaltree.query_point(16).next().unwrap().value.node_id;
            let insert_end_node_id = intervaltree.query_point(24).next().unwrap().value.node_id;

            let insert_sequence = Sequence::new()
                .sequence_type("DNA")
                .sequence("AAAAAAAA")
                .save(conn);
            let insert_node_id = Node::create(conn, insert_sequence.hash.as_str(), None);
            let edge_into_insert = Edge::create(
                conn,
                insert_start_node_id,
                6,
                Strand::Forward,
                insert_node_id,
                0,
                Strand::Forward,
            );
            let edge_out_of_insert = Edge::create(
                conn,
                insert_node_id,
                8,
                Strand::Forward,
                insert_end_node_id,
                4,
                Strand::Forward,
            );

            let edge_ids = [edge_into_insert.id, edge_out_of_insert.id];
            let block_group_edges = edge_ids
                .iter()
                .map(|edge_id| BlockGroupEdgeData {
                    block_group_id: block_group1_id,
                    edge_id: *edge_id,
                    chromosome_index: 0,
                    phased: 0,
                })
                .collect::<Vec<BlockGroupEdgeData>>();
            BlockGroupEdge::bulk_create(conn, &block_group_edges);

            let insert_path =
                original_path.new_path_with(conn, 16, 24, &edge_into_insert, &edge_out_of_insert);
            assert_eq!(
                insert_path.sequence(conn),
                "AAAAAAAAAATTTTTTAAAAAAAACCCCCCGGGGGGGGGG"
            );

            let insert2_start_node_id = intervaltree.query_point(28).next().unwrap().value.node_id;
            let insert2_end_node_id = intervaltree.query_point(32).next().unwrap().value.node_id;

            let insert2_sequence = Sequence::new()
                .sequence_type("DNA")
                .sequence("TTTTTTTT")
                .save(conn);
            let insert2_node_id = Node::create(conn, insert2_sequence.hash.as_str(), None);
            let edge_into_insert2 = Edge::create(
                conn,
                insert2_start_node_id,
                6,
                Strand::Forward,
                insert2_node_id,
                0,
                Strand::Forward,
            );
            let edge_out_of_insert2 = Edge::create(
                conn,
                insert2_node_id,
                8,
                Strand::Forward,
                insert2_end_node_id,
                4,
                Strand::Forward,
            );

            let edge_ids = [edge_into_insert2.id, edge_out_of_insert2.id];
            let block_group_edges = edge_ids
                .iter()
                .map(|edge_id| BlockGroupEdgeData {
                    block_group_id: block_group1_id,
                    edge_id: *edge_id,
                    chromosome_index: 0,
                    phased: 0,
                })
                .collect::<Vec<BlockGroupEdgeData>>();
            BlockGroupEdge::bulk_create(conn, &block_group_edges);

            let insert2_path =
                insert_path.new_path_with(conn, 28, 32, &edge_into_insert2, &edge_out_of_insert2);
            assert_eq!(
                insert2_path.sequence(conn),
                "AAAAAAAAAATTTTTTAAAAAAAACCTTTTTTTTGGGGGG"
            );

            let deletion_end_node_id = intervaltree.query_point(38).next().unwrap().value.node_id;
            let deletion_edge = Edge::create(
                conn,
                insert_node_id,
                8,
                Strand::Forward,
                deletion_end_node_id,
                8,
                Strand::Forward,
            );
            let block_group_edge = BlockGroupEdgeData {
                block_group_id: block_group1_id,
                edge_id: deletion_edge.id,
                chromosome_index: 0,
                phased: 0,
            };
            BlockGroupEdge::bulk_create(conn, &[block_group_edge]);

            let all_sequences = BlockGroup::get_all_sequences(conn, block_group1_id, false);
            assert_eq!(
                all_sequences,
                HashSet::from_iter(vec![
                    "AAAAAAAAAATTTTTTTTTTCCCCCCCCCCGGGGGGGGGG".to_string(),
                    "AAAAAAAAAATTTTTTAAAAAAAACCCCCCGGGGGGGGGG".to_string(),
                    "AAAAAAAAAATTTTTTTTTTCCCCCCTTTTTTTTGGGGGG".to_string(),
                    "AAAAAAAAAATTTTTTAAAAAAAACCTTTTTTTTGGGGGG".to_string(),
                    "AAAAAAAAAATTTTTTAAAAAAAAGG".to_string(), // Sequence including deletion
                ])
            );

            let block_group2 = BlockGroup::create(conn, "test", None, "chr1.1");
            BlockGroup::clone_subgraph(conn, block_group1_id, 15, 36, block_group2.id);
            let all_sequences2 = BlockGroup::get_all_sequences(conn, block_group2.id, false);
            assert_eq!(
                all_sequences2,
                // The deletion is not included in the cloned subgraph since one end of it is
                // outside the specified range
                HashSet::from_iter(vec![
                    "TTTTTCCCCCCCCCCGGGGGG".to_string(),
                    "TAAAAAAAACCCCCCGGGGGG".to_string(),
                    "TTTTTCCCCCCTTTTTTTTGG".to_string(),
                    "TAAAAAAAACCTTTTTTTTGG".to_string(),
                ])
            );
        }
    }
}
