use crate::config::{get_changeset_path, get_gen_dir, get_operation_connection};
use crate::models::accession::{Accession, AccessionEdge, AccessionEdgeData, AccessionPath};
use crate::models::block_group::BlockGroup;
use crate::models::block_group_edge::{BlockGroupEdge, BlockGroupEdgeData};
use crate::models::collection::Collection;
use crate::models::edge::{Edge, EdgeData};
use crate::models::file_types::FileTypes;
use crate::models::metadata;
use crate::models::node::Node;
use crate::models::operations::{
    Branch, FileAddition, Operation, OperationFile, OperationInfo, OperationState, OperationSummary,
};
use crate::models::path::Path;
use crate::models::sample::Sample;
use crate::models::sequence::{NewSequence, Sequence};
use crate::models::strand::Strand;
use crate::models::traits::*;
use fallible_streaming_iterator::FallibleStreamingIterator;
use itertools::Itertools;
use petgraph::Direction;
use rusqlite;
use rusqlite::session::{ChangesetItem, ChangesetIter};
use rusqlite::types::{FromSql, Value};
use rusqlite::Error as SQLError;
use rusqlite::{session, Connection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::{fs, path::Path as FilePath, path::PathBuf, str};
use tempfile::tempdir;
use thiserror::Error;
use url_parse::core::Parser;

/* General information

Changesets from sqlite will be created in the order that operations are applied in the database,
so given our foreign key setup, we would not expect out of order table/row creation. i.e. block
groups will always appear before block group edges, etc.

 */

#[derive(Debug, PartialEq, Error)]
pub enum OperationError {
    #[error("No Changes")]
    NoChanges,
    #[error("Operation Already Exists")]
    OperationExists,
    #[error("Operation {0} Does Not Exist")]
    NoOperation(String),
    #[error("SQL Error: {0}")]
    SQLError(String),
    #[error("SQLite Error: {0}")]
    SqliteError(#[from] SQLError),
}

#[derive(Debug, PartialEq, Error)]
pub enum MergeError {
    #[error("SQL Error: {0}")]
    SQLError(String),
    #[error("SQLite Error: {0}")]
    SqliteError(#[from] SQLError),
    #[error("Operation Error: {0}")]
    OperationError(#[from] OperationError),
    #[error("Invalid Branch: {0}")]
    InvalidBranch(String),
}

#[derive(Debug, Error)]
pub enum RemoteOperationError {
    #[error("Failed to transfer {0} from {1} to {2}")]
    FileTransferError(String, String, String),
    #[error("Remote url is not valid: {0}")]
    InvalidRemoteUrl(String),
    #[error("Gen does not support the scheme {0} in the remote url {1}")]
    UnsupportedRemoteScheme(String, String),
    #[error("Remote url not set, please set it using set-remote before pushing or pulling")]
    RemoteUrlNotSet,
    #[error(
        "Remote repo has changes that are not in the local repo, please pull them before pushing."
    )]
    RemoteBranchAhead,
    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),
}

pub enum FileMode {
    Read,
    Write,
}

// NOTE: The ordering of these entries is important, because it is the order in which tables are
// processed in get_changeset_dependencies.  If model Foo depends on model Bar, then foo must come
// first in this list.  For instance, an edge depends on a source node and a target node, so the
// edges table must come before the nodes table here.  This is because in
// get_changeset_dependencies, if we process edges first, we correctly find any nodes that were
// created that the edge must be associated with.  If we instead processed nodes first, some nodes
// associated with edges would get counted as new nodes, and if we recrate the edge it might not be
// associated with the correct recreated nodes.
const CHANGE_TABLES: &[&str; 12] = &[
    "accessions",
    "path_edges",
    "paths",
    "block_group_edges",
    "edges",
    "accession_paths",
    "accession_edges",
    "nodes",
    "sequences",
    "block_groups",
    "collections",
    "samples",
];

#[derive(Deserialize, Serialize, Debug)]
pub struct DependencyModels {
    pub sequences: Vec<Sequence>,
    pub block_group: Vec<BlockGroup>,
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub paths: Vec<Path>,
    pub accessions: Vec<Accession>,
    pub accession_edges: Vec<AccessionEdge>,
}

#[derive(Debug)]
pub struct ChangesetModels {
    pub collections: Vec<Collection>,
    pub samples: Vec<Sample>,
    pub sequences: Vec<Sequence>,
    pub block_groups: Vec<BlockGroup>,
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub block_group_edges: Vec<BlockGroupEdge>,
}

pub fn get_file(path: &PathBuf, mode: FileMode) -> fs::File {
    let file;
    match mode {
        FileMode::Read => {
            if fs::metadata(path).is_ok() {
                file = fs::File::open(path);
            } else {
                file = fs::File::create_new(path);
            }
        }
        FileMode::Write => {
            file = fs::File::create(path);
        }
    }

    file.unwrap()
}

pub fn get_changeset_dependencies(conn: &Connection, mut changes: &[u8]) -> Vec<u8> {
    let input: &mut dyn Read = &mut changes;
    let mut iter = ChangesetIter::start_strm(&input).unwrap();
    // the purpose of this function is to capture external changes to the changeset, notably foreign keys
    // that may be made in previous changesets.
    let mut previous_block_groups = HashSet::new();
    let mut previous_edges = HashSet::new();
    let mut previous_paths = HashSet::new();
    let mut previous_accessions = HashSet::new();
    let mut previous_nodes = HashSet::new();
    let mut previous_sequences = HashSet::new();
    let mut previous_accession_edges = HashSet::new();
    let mut created_block_groups = HashSet::new();
    let mut created_paths = HashSet::new();
    let mut created_accessions = HashSet::new();
    let mut created_edges = HashSet::new();
    let mut created_accession_edges = HashSet::new();
    let mut created_nodes = HashSet::new();
    let mut created_sequences: HashSet<String> = HashSet::new();

    while let Some(item) = iter.next().unwrap() {
        let op = item.op().unwrap();
        // info on indirect changes: https://www.sqlite.org/draft/session/sqlite3session_indirect.html
        if !op.indirect() {
            let table = op.table_name();
            let pk_column = item
                .pk()
                .unwrap()
                .iter()
                .find_position(|item| **item == 1)
                .unwrap()
                .0;
            match table {
                "sequences" => {
                    let hash =
                        str::from_utf8(item.new_value(pk_column).unwrap().as_bytes().unwrap())
                            .unwrap();
                    created_sequences.insert(hash.to_string());
                }
                "block_groups" => {
                    let bg_pk = item.new_value(pk_column).unwrap().as_i64().unwrap();
                    created_block_groups.insert(bg_pk);
                }
                "paths" => {
                    created_paths.insert(item.new_value(pk_column).unwrap().as_i64().unwrap());
                    let bg_id = item.new_value(1).unwrap().as_i64().unwrap();
                    if !created_block_groups.contains(&bg_id) {
                        previous_block_groups.insert(bg_id);
                    }
                }
                "nodes" => {
                    created_nodes.insert(item.new_value(pk_column).unwrap().as_i64().unwrap());
                    let sequence_hash =
                        str::from_utf8(item.new_value(1).unwrap().as_bytes().unwrap())
                            .unwrap()
                            .to_string();
                    if !created_sequences.contains(&sequence_hash) {
                        previous_sequences.insert(sequence_hash);
                    }
                }
                "edges" => {
                    let edge_pk = item.new_value(pk_column).unwrap().as_i64().unwrap();
                    let source_node_id = item.new_value(1).unwrap().as_i64().unwrap();
                    let target_node_id = item.new_value(4).unwrap().as_i64().unwrap();
                    created_edges.insert(edge_pk);
                    let nodes = Node::get_nodes(conn, &[source_node_id, target_node_id]);
                    for node in nodes.iter() {
                        if !created_nodes.contains(&node.id) && !Node::is_terminal(node.id) {
                            previous_sequences.insert(node.sequence_hash.clone());
                            previous_nodes.insert(node.id);
                        }
                    }
                }
                "path_edges" => {
                    let path_id = item.new_value(1).unwrap().as_i64().unwrap();
                    let edge_id = item.new_value(3).unwrap().as_i64().unwrap();
                    if !created_paths.contains(&path_id) {
                        previous_paths.insert(path_id);
                    }
                    if !created_edges.contains(&edge_id) {
                        previous_edges.insert(edge_id);
                    }
                }
                "block_group_edges" => {
                    // make sure blockgroup_map has blockgroups for bg ids made in external changes.
                    let bg_id = item.new_value(1).unwrap().as_i64().unwrap();
                    let edge_id = item.new_value(2).unwrap().as_i64().unwrap();
                    if !created_edges.contains(&edge_id) {
                        previous_edges.insert(edge_id);
                    }
                    if !created_block_groups.contains(&bg_id) {
                        previous_block_groups.insert(bg_id);
                    }
                }
                "accessions" => {
                    created_accessions.insert(item.new_value(pk_column).unwrap().as_i64().unwrap());
                    let path_id = item.new_value(2).unwrap().as_i64().unwrap();
                    let parent_accession_id = item.new_value(3).unwrap().as_i64_or_null().unwrap();
                    if !created_paths.contains(&path_id) {
                        previous_paths.insert(path_id);
                    }
                    if let Some(id) = parent_accession_id {
                        if !created_accessions.contains(&id) {
                            previous_accessions.insert(id);
                        }
                    }
                }
                "accession_edges" => {
                    let edge_pk = item.new_value(pk_column).unwrap().as_i64().unwrap();
                    let source_node_id = item.new_value(1).unwrap().as_i64().unwrap();
                    let target_node_id = item.new_value(4).unwrap().as_i64().unwrap();
                    created_accession_edges.insert(edge_pk);
                    let nodes = Node::get_nodes(conn, &[source_node_id, target_node_id]);
                    if !created_nodes.contains(&source_node_id) {
                        previous_sequences.insert(nodes[0].sequence_hash.clone());
                    }
                    if source_node_id != target_node_id && !created_nodes.contains(&target_node_id)
                    {
                        previous_sequences.insert(nodes[1].sequence_hash.clone());
                    }
                }
                "accession_paths" => {
                    let accession_id = item.new_value(1).unwrap().as_i64().unwrap();
                    let edge_id = item.new_value(3).unwrap().as_i64().unwrap();
                    if !created_accessions.contains(&accession_id) {
                        previous_accessions.insert(accession_id);
                    }
                    if !created_accession_edges.contains(&edge_id) {
                        previous_accession_edges.insert(edge_id);
                    }
                }
                _ => {}
            }
        }
    }

    let existing_edges = Edge::bulk_load(
        conn,
        &previous_edges.clone().into_iter().collect::<Vec<_>>(),
    );
    let mut new_nodes = vec![];
    for edge in existing_edges.iter() {
        if !previous_nodes.contains(&edge.source_node_id) && !Node::is_terminal(edge.source_node_id)
        {
            previous_nodes.insert(edge.source_node_id);
            new_nodes.push(edge.source_node_id);
        }
        if !previous_nodes.contains(&edge.target_node_id) && !Node::is_terminal(edge.target_node_id)
        {
            previous_nodes.insert(edge.target_node_id);
            new_nodes.push(edge.target_node_id);
        }
    }
    for node in Node::get_nodes(conn, &new_nodes) {
        previous_sequences.insert(node.sequence_hash.clone());
    }
    let s = DependencyModels {
        sequences: Sequence::sequences_by_hash(
            conn,
            previous_sequences.iter().map(|s| s as &str).collect(),
        )
        .values()
        .cloned()
        .collect(),
        block_group: BlockGroup::query(
            conn,
            &format!(
                "select * from block_groups where id in ({ids})",
                ids = previous_block_groups.iter().join(",")
            ),
            rusqlite::params!(),
        ),
        nodes: Node::get_nodes(
            conn,
            &previous_nodes.into_iter().sorted().collect::<Vec<_>>(),
        ),
        edges: Edge::query(
            conn,
            &format!(
                "select * from edges where id in ({ids})",
                ids = previous_edges.iter().join(",")
            ),
            rusqlite::params!(),
        ),
        paths: Path::query(
            conn,
            &format!(
                "select * from paths where id in ({ids})",
                ids = previous_paths.iter().join(",")
            ),
            rusqlite::params!(),
        ),
        accessions: Accession::query(
            conn,
            &format!(
                "select * from accessions where id in ({ids})",
                ids = previous_accessions.iter().join(",")
            ),
            rusqlite::params!(),
        ),
        accession_edges: AccessionEdge::query(
            conn,
            &format!(
                "select * from accession_edges where id in ({ids})",
                ids = previous_accession_edges.iter().join(",")
            ),
            rusqlite::params!(),
        ),
    };
    serde_json::to_vec(&s).unwrap()
}

pub fn write_changeset(operation: &Operation, changes: &[u8], dependencies: &[u8]) {
    let change_path =
        get_changeset_path(operation).join(format!("{op_id}.cs", op_id = operation.hash));
    let dependency_path =
        get_changeset_path(operation).join(format!("{op_id}.dep", op_id = operation.hash));

    let mut dependency_file = fs::File::create_new(&dependency_path)
        .unwrap_or_else(|_| panic!("Unable to open {dependency_path:?}"));
    dependency_file.write_all(dependencies).unwrap();

    let mut file = fs::File::create_new(&change_path)
        .unwrap_or_else(|_| panic!("Unable to open {change_path:?}"));

    file.write_all(changes).unwrap()
}

pub fn load_changeset_dependencies(operation: &Operation) -> DependencyModels {
    let dependency_path =
        get_changeset_path(operation).join(format!("{op_id}.dep", op_id = operation.hash));
    serde_json::from_reader(fs::File::open(dependency_path).unwrap()).unwrap()
}

pub fn load_changeset(operation: &Operation) -> Vec<u8> {
    let change_path =
        get_changeset_path(operation).join(format!("{op_id}.cs", op_id = operation.hash));
    let mut file = fs::File::open(change_path).unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).unwrap();
    contents
}

fn parse_string(item: &ChangesetItem, col: usize) -> String {
    str::from_utf8(item.new_value(col).unwrap().as_bytes().unwrap())
        .unwrap()
        .to_string()
}

fn parse_maybe_string(item: &ChangesetItem, col: usize) -> Option<String> {
    item.new_value(col)
        .unwrap()
        .as_bytes_or_null()
        .unwrap()
        .map(|v| str::from_utf8(v).unwrap().to_string())
}

fn parse_number(item: &ChangesetItem, col: usize) -> i64 {
    item.new_value(col).unwrap().as_i64().unwrap()
}

fn parse_maybe_number(item: &ChangesetItem, col: usize) -> Option<i64> {
    item.new_value(col).unwrap().as_i64_or_null().unwrap()
}

pub fn load_changeset_models(changeset: &mut ChangesetIter) -> ChangesetModels {
    let mut created_block_groups = vec![];
    let mut created_edges = vec![];
    let mut created_nodes = vec![];
    let mut created_sequences = vec![];
    let mut created_bg_edges = vec![];
    let mut created_samples = vec![];
    let mut created_collections = vec![];

    while let Some(item) = changeset.next().unwrap() {
        let op = item.op().unwrap();
        // info on indirect changes: https://www.sqlite.org/draft/session/sqlite3session_indirect.html
        if !op.indirect() {
            let table = op.table_name();
            let pk_column = item
                .pk()
                .unwrap()
                .iter()
                .find_position(|item| **item == 1)
                .unwrap()
                .0;
            match table {
                "sequences" => {
                    let hash = parse_string(item, pk_column);
                    let sequence = Sequence::new()
                        .sequence_type(&parse_string(item, 1))
                        .sequence(&parse_string(item, 2))
                        .name(&parse_string(item, 3))
                        .file_path(&parse_string(item, 4))
                        .length(parse_number(item, 5))
                        .build();
                    assert_eq!(hash, sequence.hash);
                    created_sequences.push(sequence);
                }
                "block_groups" => created_block_groups.push(BlockGroup {
                    id: parse_number(item, pk_column),
                    collection_name: parse_string(item, 1),
                    sample_name: parse_maybe_string(item, 2),
                    name: parse_string(item, 3),
                }),

                "nodes" => created_nodes.push(Node {
                    id: parse_number(item, pk_column),
                    sequence_hash: parse_string(item, 1),
                    hash: parse_maybe_string(item, 2),
                }),
                "edges" => created_edges.push(Edge {
                    id: parse_number(item, pk_column),
                    source_node_id: parse_number(item, 1),
                    source_coordinate: parse_number(item, 2),
                    source_strand: Strand::column_result(item.new_value(3).unwrap()).unwrap(),
                    target_node_id: parse_number(item, 4),
                    target_coordinate: parse_number(item, 5),
                    target_strand: Strand::column_result(item.new_value(6).unwrap()).unwrap(),
                }),
                "block_group_edges" => created_bg_edges.push(BlockGroupEdge {
                    id: parse_number(item, pk_column),
                    block_group_id: parse_number(item, 1),
                    edge_id: parse_number(item, 2),
                    chromosome_index: parse_number(item, 3),
                    phased: parse_number(item, 4),
                }),
                "samples" => created_samples.push(Sample {
                    name: parse_string(item, pk_column),
                }),
                "collections" => created_collections.push(Collection {
                    name: parse_string(item, pk_column),
                }),
                _ => {}
            }
        }
    }
    ChangesetModels {
        sequences: created_sequences,
        block_groups: created_block_groups,
        nodes: created_nodes,
        edges: created_edges,
        block_group_edges: created_bg_edges,
        samples: created_samples,
        collections: created_collections,
    }
}

pub fn apply_changeset(
    conn: &Connection,
    changeset: &mut ChangesetIter,
    dependencies: &DependencyModels,
) {
    for sequence in dependencies.sequences.iter() {
        NewSequence::from(sequence).save(conn);
    }
    for node in dependencies.nodes.iter() {
        if !Node::is_terminal(node.id) {
            assert!(Sequence::sequence_from_hash(conn, &node.sequence_hash).is_some());
        }
    }

    let mut dep_bg_map = HashMap::new();
    for bg in dependencies.block_group.iter() {
        if !Collection::exists(conn, &bg.collection_name) {
            Collection::create(conn, &bg.collection_name);
        }
        let sample_name = bg.sample_name.as_ref().map(|v| v as &str);
        if let Some(sample_name) = sample_name {
            Sample::get_or_create(conn, sample_name);
        }
        let new_bg = BlockGroup::create(conn, &bg.collection_name, sample_name, &bg.name);
        dep_bg_map.insert(&bg.id, new_bg.id);
    }

    let mut dep_node_map = HashMap::new();
    for node in dependencies.nodes.iter() {
        let new_node_id = Node::create(conn, &node.sequence_hash, node.hash.clone());
        dep_node_map.insert(&node.id, new_node_id);
    }

    let mut dep_edge_map = HashMap::new();
    let updated_edges = dependencies
        .edges
        .iter()
        .map(|edge| EdgeData {
            source_node_id: *dep_node_map
                .get(&edge.source_node_id)
                .unwrap_or(&edge.source_node_id),
            source_coordinate: edge.source_coordinate,
            source_strand: edge.source_strand,
            target_node_id: *dep_node_map
                .get(&edge.target_node_id)
                .unwrap_or(&edge.target_node_id),
            target_coordinate: edge.target_coordinate,
            target_strand: edge.target_strand,
        })
        .collect::<Vec<EdgeData>>();
    let new_edges = Edge::bulk_create(conn, &updated_edges);
    for (index, edge_id) in new_edges.iter().enumerate() {
        dep_edge_map.insert(&dependencies.edges[index].id, *edge_id);
    }

    let mut dep_path_map = HashMap::new();
    for path in dependencies.paths.iter() {
        let new_path = Path::create(
            conn,
            &path.name,
            *dep_bg_map
                .get(&path.block_group_id)
                .unwrap_or(&path.block_group_id),
            &[],
        );
        dep_path_map.insert(path.id, new_path.id);
    }

    let mut dep_accession_edge_map = HashMap::new();
    let updated_accession_edges = dependencies
        .accession_edges
        .iter()
        .map(|edge| AccessionEdgeData {
            source_node_id: *dep_node_map
                .get(&edge.source_node_id)
                .unwrap_or(&edge.source_node_id),
            source_coordinate: edge.source_coordinate,
            source_strand: edge.source_strand,
            target_node_id: *dep_node_map
                .get(&edge.target_node_id)
                .unwrap_or(&edge.target_node_id),
            target_coordinate: edge.target_coordinate,
            target_strand: edge.target_strand,
            chromosome_index: edge.chromosome_index,
        })
        .collect::<Vec<AccessionEdgeData>>();
    let new_accession_edges = AccessionEdge::bulk_create(conn, &updated_accession_edges);
    for (index, edge_id) in new_accession_edges.iter().enumerate() {
        dep_accession_edge_map.insert(&dependencies.accession_edges[index].id, *edge_id);
    }

    let mut dep_accession_map: HashMap<i64, i64> = HashMap::new();
    for accession in dependencies.accessions.iter() {
        let new_accession = if let Some(acc_id) = accession.parent_accession_id {
            Accession::get_or_create(
                conn,
                &accession.name,
                *dep_path_map
                    .get(&accession.path_id)
                    .unwrap_or(&accession.path_id),
                Some(*dep_accession_map.get(&acc_id).unwrap_or(&acc_id)),
            )
        } else {
            Accession::get_or_create(
                conn,
                &accession.name,
                *dep_path_map
                    .get(&accession.path_id)
                    .unwrap_or(&accession.path_id),
                None,
            )
        };
        dep_accession_map.insert(accession.id, new_accession.id);
    }

    conn.pragma_update(None, "foreign_keys", "0").unwrap();

    let mut blockgroup_map: HashMap<i64, i64> = HashMap::new();
    let mut edge_map: HashMap<i64, EdgeData> = HashMap::new();
    let mut node_map: HashMap<i64, (String, Option<String>)> = HashMap::new();
    let mut path_edges: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
    let mut insert_paths = vec![];
    let mut insert_accessions = vec![];
    let mut insert_block_group_edges = vec![];

    let mut accession_edge_map: HashMap<i64, AccessionEdgeData> = HashMap::new();
    let mut accession_path_edges: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();

    while let Some(item) = changeset.next().unwrap() {
        let op = item.op().unwrap();
        // info on indirect changes: https://www.sqlite.org/draft/session/sqlite3session_indirect.html
        if !op.indirect() {
            let table = op.table_name();
            let pk_column = item
                .pk()
                .unwrap()
                .iter()
                .find_position(|item| **item == 1)
                .unwrap()
                .0;
            match table {
                "samples" => {
                    Sample::get_or_create(conn, &parse_string(item, pk_column));
                }
                "sequences" => {
                    Sequence::new()
                        .sequence_type(&parse_string(item, 1))
                        .sequence(&parse_string(item, 2))
                        .name(&parse_string(item, 3))
                        .file_path(&parse_string(item, 4))
                        .length(parse_number(item, 5))
                        .save(conn);
                }
                "block_groups" => {
                    let bg_pk = parse_number(item, pk_column);
                    if let Some(v) = dep_bg_map.get(&bg_pk) {
                        blockgroup_map.insert(bg_pk, *v);
                    } else {
                        let sample_name = parse_maybe_string(item, 2);
                        let new_bg = BlockGroup::create(
                            conn,
                            &parse_string(item, 1),
                            sample_name.as_deref(),
                            &parse_string(item, 3),
                        );
                        blockgroup_map.insert(bg_pk, new_bg.id);
                    };
                }
                "paths" => {
                    // defer path creation until edges are made
                    insert_paths.push(Path {
                        id: parse_number(item, pk_column),
                        block_group_id: parse_number(item, 1),
                        name: parse_string(item, 2),
                    });
                }
                "nodes" => {
                    let node_pk = parse_number(item, pk_column);
                    node_map.insert(
                        node_pk,
                        (
                            parse_string(item, 1),
                            parse_maybe_string(item, 2).map(|s| s.to_string()),
                        ),
                    );
                }
                "edges" => {
                    let edge_pk = parse_number(item, pk_column);
                    edge_map.insert(
                        edge_pk,
                        EdgeData {
                            source_node_id: parse_number(item, 1),
                            source_coordinate: parse_number(item, 2),
                            source_strand: Strand::column_result(item.new_value(3).unwrap())
                                .unwrap(),
                            target_node_id: parse_number(item, 4),
                            target_coordinate: parse_number(item, 5),
                            target_strand: Strand::column_result(item.new_value(6).unwrap())
                                .unwrap(),
                        },
                    );
                }
                "path_edges" => {
                    let path_id = item.new_value(1).unwrap().as_i64().unwrap();
                    let path_index = item.new_value(2).unwrap().as_i64().unwrap();
                    // the edge_id here may not be valid and in this database may have a different pk
                    let edge_id = item.new_value(3).unwrap().as_i64().unwrap();
                    path_edges
                        .entry(path_id)
                        .or_default()
                        .push((path_index, edge_id));
                }
                "block_group_edges" => {
                    // make sure blockgroup_map has blockgroups for bg ids made in external changes.
                    let bg_id = item.new_value(1).unwrap().as_i64().unwrap();
                    let edge_id = item.new_value(2).unwrap().as_i64().unwrap();
                    let chromosome_index = item.new_value(3).unwrap().as_i64().unwrap();
                    let phased = item.new_value(4).unwrap().as_i64().unwrap();
                    insert_block_group_edges.push((bg_id, edge_id, chromosome_index, phased));
                }
                "collections" => {
                    Collection::create(
                        conn,
                        str::from_utf8(item.new_value(pk_column).unwrap().as_bytes().unwrap())
                            .unwrap(),
                    );
                }
                "accessions" => {
                    // we defer accession creation until edges and paths are made
                    insert_accessions.push(Accession {
                        id: parse_number(item, pk_column),
                        name: parse_string(item, 1),
                        path_id: parse_number(item, 2),
                        parent_accession_id: parse_maybe_number(item, 2),
                    });
                }
                "accession_edges" => {
                    let pk = item.new_value(pk_column).unwrap().as_i64().unwrap();
                    accession_edge_map.insert(
                        pk,
                        AccessionEdgeData {
                            source_node_id: item.new_value(1).unwrap().as_i64().unwrap(),
                            source_coordinate: item.new_value(2).unwrap().as_i64().unwrap(),
                            source_strand: Strand::column_result(item.new_value(3).unwrap())
                                .unwrap(),
                            target_node_id: item.new_value(4).unwrap().as_i64().unwrap(),
                            target_coordinate: item.new_value(5).unwrap().as_i64().unwrap(),
                            target_strand: Strand::column_result(item.new_value(6).unwrap())
                                .unwrap(),
                            chromosome_index: item.new_value(7).unwrap().as_i64().unwrap(),
                        },
                    );
                }
                "accession_paths" => {
                    let accession_id = item.new_value(1).unwrap().as_i64().unwrap();
                    let index = item.new_value(2).unwrap().as_i64().unwrap();
                    // the edge_id here may not be valid and in this database may have a different pk
                    let accession_edge_id = item.new_value(3).unwrap().as_i64().unwrap();
                    accession_path_edges
                        .entry(accession_id)
                        .or_default()
                        .push((index, accession_edge_id));
                }
                _ => {
                    panic!("unhandled table is {v}", v = op.table_name());
                }
            }
        }
    }

    let mut node_id_map: HashMap<i64, i64> = HashMap::new();
    for (node_id, (sequence_hash, node_hash)) in node_map {
        let new_node_id = Node::create(conn, &sequence_hash, node_hash);
        node_id_map.insert(node_id, new_node_id);
    }

    let mut updated_edge_map = HashMap::new();
    for (edge_id, edge) in edge_map {
        let updated_source_node_id = dep_node_map.get(&edge.source_node_id).unwrap_or(
            node_id_map
                .get(&edge.source_node_id)
                .unwrap_or(&edge.source_node_id),
        );
        let updated_target_node_id = dep_node_map.get(&edge.target_node_id).unwrap_or(
            node_id_map
                .get(&edge.target_node_id)
                .unwrap_or(&edge.target_node_id),
        );
        updated_edge_map.insert(
            edge_id,
            EdgeData {
                source_node_id: *updated_source_node_id,
                source_coordinate: edge.source_coordinate,
                source_strand: edge.source_strand,
                target_node_id: *updated_target_node_id,
                target_coordinate: edge.target_coordinate,
                target_strand: edge.target_strand,
            },
        );
    }

    let sorted_edge_ids = updated_edge_map
        .keys()
        .copied()
        .sorted()
        .collect::<Vec<i64>>();
    let created_edges = Edge::bulk_create(
        conn,
        &sorted_edge_ids
            .iter()
            .map(|id| updated_edge_map[id])
            .collect::<Vec<EdgeData>>(),
    );
    let mut edge_id_map: HashMap<i64, i64> = HashMap::new();
    for (index, edge_id) in created_edges.iter().enumerate() {
        edge_id_map.insert(sorted_edge_ids[index], *edge_id);
    }

    let mut block_group_edges: HashMap<i64, Vec<(i64, i64, i64)>> = HashMap::new();

    for (bg_id, edge_id, chromosome_index, phased) in insert_block_group_edges {
        let bg_id = *dep_bg_map
            .get(&bg_id)
            .or(blockgroup_map.get(&bg_id).or(Some(&bg_id)))
            .unwrap();
        let edge_id = dep_edge_map
            .get(&edge_id)
            .or(edge_id_map.get(&edge_id).or(Some(&edge_id)))
            .unwrap();
        block_group_edges
            .entry(bg_id)
            .or_default()
            .push((*edge_id, chromosome_index, phased));
    }

    for (bg_id, edges) in block_group_edges.iter() {
        let new_block_group_edges = edges
            .iter()
            .map(|(edge_id, chromosome_index, phased)| BlockGroupEdgeData {
                block_group_id: *bg_id,
                edge_id: *edge_id,
                chromosome_index: *chromosome_index,
                phased: *phased,
            })
            .collect::<Vec<_>>();
        BlockGroupEdge::bulk_create(conn, &new_block_group_edges);
    }

    for path in insert_paths {
        let mut sorted_edges = vec![];
        for (_, edge_id) in path_edges
            .get(&path.id)
            .unwrap()
            .iter()
            .sorted_by(|(c1, _), (c2, _)| Ord::cmp(&c1, &c2))
        {
            let new_edge_id = dep_edge_map
                .get(edge_id)
                .unwrap_or(edge_id_map.get(edge_id).unwrap_or(edge_id));
            sorted_edges.push(*new_edge_id);
        }
        let new_bg_id = *dep_bg_map
            .get(&path.block_group_id)
            .or(blockgroup_map
                .get(&path.block_group_id)
                .or(Some(&path.block_group_id)))
            .unwrap();
        Path::create(conn, &path.name, new_bg_id, &sorted_edges);
    }

    let mut updated_accession_edge_map = HashMap::new();
    for (edge_id, edge) in accession_edge_map {
        let updated_source_node_id = dep_node_map.get(&edge.source_node_id).unwrap_or(
            node_id_map
                .get(&edge.source_node_id)
                .unwrap_or(&edge.source_node_id),
        );
        let updated_target_node_id = dep_node_map.get(&edge.target_node_id).unwrap_or(
            node_id_map
                .get(&edge.target_node_id)
                .unwrap_or(&edge.target_node_id),
        );
        updated_accession_edge_map.insert(
            edge_id,
            AccessionEdgeData {
                source_node_id: *updated_source_node_id,
                source_coordinate: edge.source_coordinate,
                source_strand: edge.source_strand,
                target_node_id: *updated_target_node_id,
                target_coordinate: edge.target_coordinate,
                target_strand: edge.target_strand,
                chromosome_index: edge.chromosome_index,
            },
        );
    }

    let sorted_edge_ids = updated_accession_edge_map
        .keys()
        .copied()
        .sorted()
        .collect::<Vec<i64>>();
    let created_edges = AccessionEdge::bulk_create(
        conn,
        &sorted_edge_ids
            .iter()
            .map(|id| updated_accession_edge_map[id].clone())
            .collect::<Vec<AccessionEdgeData>>(),
    );
    let mut edge_id_map: HashMap<i64, i64> = HashMap::new();
    for (index, edge_id) in created_edges.iter().enumerate() {
        edge_id_map.insert(sorted_edge_ids[index], *edge_id);
    }

    for accession in insert_accessions {
        let mut sorted_edges = vec![];
        for (_, edge_id) in accession_path_edges
            .get(&accession.id)
            .unwrap()
            .iter()
            .sorted_by(|(c1, _), (c2, _)| Ord::cmp(&c1, &c2))
        {
            let new_edge_id = dep_accession_edge_map
                .get(edge_id)
                .unwrap_or(edge_id_map.get(edge_id).unwrap_or(edge_id));
            sorted_edges.push(*new_edge_id);
        }
        let accession_obj = Accession::get_or_create(
            conn,
            &accession.name,
            accession.path_id,
            accession.parent_accession_id,
        );
        AccessionPath::create(conn, accession_obj.id, &sorted_edges);
    }

    conn.pragma_update(None, "foreign_keys", "1").unwrap();
}

pub fn revert_changeset(conn: &Connection, operation: &Operation) {
    let change_path =
        get_changeset_path(operation).join(format!("{op_id}.cs", op_id = operation.hash));
    let mut file = fs::File::open(change_path).unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).unwrap();
    let mut inverted_contents: Vec<u8> = vec![];
    session::invert_strm(&mut &contents[..], &mut inverted_contents).unwrap();

    conn.pragma_update(None, "foreign_keys", "0").unwrap();
    conn.apply_strm(
        &mut &inverted_contents[..],
        None::<fn(&str) -> bool>,
        |_conflict_type, _item| session::ConflictAction::SQLITE_CHANGESET_OMIT,
    )
    .unwrap();
    conn.pragma_update(None, "foreign_keys", "1").unwrap();
}

pub fn reset(conn: &Connection, operation_conn: &Connection, db_uuid: &str, op_hash: &str) {
    let current_op = OperationState::get_operation(operation_conn, db_uuid).unwrap();
    let current_branch_id = OperationState::get_current_branch(operation_conn, db_uuid).unwrap();
    let current_branch = Branch::get_by_id(operation_conn, current_branch_id).unwrap();
    let branch_operations: Vec<String> = Branch::get_operations(operation_conn, current_branch_id)
        .iter()
        .map(|b| b.hash.clone())
        .collect();
    if !branch_operations.contains(&current_op) {
        panic!("{op_hash} is not contained in this branch's operations.");
    }
    let operation = Operation::get_by_hash(operation_conn, op_hash)
        .unwrap_or_else(|_| panic!("Hash {op_hash} does not exist."));
    let full_op_hash = operation.hash.clone();
    move_to(conn, operation_conn, &operation);

    if current_branch.name != "main" {
        match operation_conn.execute(
            "UPDATE branch SET start_operation_hash = ?2 WHERE id = ?1",
            (current_branch_id, full_op_hash.to_string()),
        ) {
            Ok(_) => {}
            Err(e) => {
                panic!("Unable to reset branch: {e}");
            }
        }
    }

    // hide all child operations from this point
    for op in Operation::query(
        operation_conn,
        "select * from operation where parent_hash = ?1",
        rusqlite::params!(Value::from(full_op_hash.to_string())),
    )
    .iter()
    {
        Branch::mask_operation(operation_conn, current_branch_id, &op.hash);
    }
    OperationState::set_operation(operation_conn, db_uuid, &full_op_hash);
}

pub fn apply<'a>(
    conn: &Connection,
    operation_conn: &Connection,
    op_hash: &str,
    force_hash: impl Into<Option<&'a str>>,
) -> Result<Operation, OperationError> {
    let mut session = start_operation(conn);
    let operation = Operation::get_by_hash(operation_conn, op_hash)
        .map_err(|_err| OperationError::NoOperation(op_hash.to_string()))?;
    let changeset = load_changeset(&operation);
    let input: &mut dyn Read = &mut changeset.as_slice();
    let mut iter = ChangesetIter::start_strm(&input).unwrap();
    let dependencies = load_changeset_dependencies(&operation);
    apply_changeset(conn, &mut iter, &dependencies);
    let full_op_hash = operation.hash.clone();
    end_operation(
        conn,
        operation_conn,
        &mut session,
        &OperationInfo {
            files: vec![OperationFile {
                file_path: format!("{full_op_hash}.cs"),
                file_type: FileTypes::Changeset,
            }],
            description: "changeset_application".to_string(),
        },
        &format!("Applied changeset {full_op_hash}."),
        force_hash,
    )
}

pub fn merge<'a>(
    conn: &Connection,
    operation_conn: &Connection,
    db_uuid: &str,
    source_branch: i64,
    other_branch: i64,
    force_hash: impl Into<Option<&'a str>>,
) -> Result<Vec<Operation>, MergeError> {
    let mut new_operations: Vec<Operation> = vec![];
    let hash_prefix = force_hash.into();
    let current_branch =
        OperationState::get_current_branch(operation_conn, db_uuid).expect("No current branch.");
    if source_branch != current_branch {
        return Err(MergeError::InvalidBranch("Source branch and current branch must match. Checkout the branch you wish to merge into.".to_string()));
    }
    if other_branch == current_branch {
        return Err(MergeError::InvalidBranch(
            "Target branch to merge is the currently checked out branch.".to_string(),
        ));
    }
    let current_operations = Branch::get_operations(operation_conn, source_branch);
    let other_operations = Branch::get_operations(operation_conn, other_branch);
    let first_different_op = other_operations
        .iter()
        .position(|op| !current_operations.contains(op))
        .expect("No common operations between two branches.");
    if first_different_op < other_operations.len() {
        for (index, operation) in other_operations[first_different_op..].iter().enumerate() {
            println!("Applying operation {op_id}", op_id = operation.hash);
            let new_op = if let Some(hash) = hash_prefix {
                apply(
                    conn,
                    operation_conn,
                    &operation.hash,
                    format!("{hash}-{index}").as_str(),
                )?
            } else {
                apply(conn, operation_conn, &operation.hash, None)?
            };
            new_operations.push(new_op);
        }
    }
    Ok(new_operations)
}

pub fn move_to(conn: &Connection, operation_conn: &Connection, operation: &Operation) {
    let current_op_hash =
        OperationState::get_operation(operation_conn, &operation.db_uuid).unwrap();
    let op_hash = operation.hash.clone();
    if current_op_hash == op_hash {
        return;
    }
    let path = Operation::get_path_between(operation_conn, &current_op_hash, &op_hash);
    if path.is_empty() {
        println!("No path exists from {current_op_hash} to {op_hash}.");
        return;
    }
    for (operation_hash, direction, next_op) in path.iter() {
        match direction {
            Direction::Incoming => {
                println!("Reverting operation {operation_hash}");
                revert_changeset(
                    conn,
                    &Operation::get_by_hash(operation_conn, operation_hash)
                        .unwrap_or_else(|_| panic!("Hash {operation_hash} does not exist.")),
                );
                OperationState::set_operation(operation_conn, &operation.db_uuid, next_op);
            }
            Direction::Outgoing => {
                println!("Applying operation {next_op}");
                let op_to_apply = Operation::get_by_hash(operation_conn, next_op)
                    .unwrap_or_else(|_| panic!("Hash {next_op} does not exist."));
                let changeset = load_changeset(&op_to_apply);
                let input: &mut dyn Read = &mut changeset.as_slice();
                let mut iter = ChangesetIter::start_strm(&input).unwrap();
                let dependencies = load_changeset_dependencies(&op_to_apply);
                apply_changeset(conn, &mut iter, &dependencies);
                OperationState::set_operation(operation_conn, &operation.db_uuid, next_op);
            }
        }
    }
}

pub fn start_operation(conn: &Connection) -> session::Session {
    let mut session = session::Session::new(conn).unwrap();
    attach_session(&mut session);
    session
}

#[allow(clippy::too_many_arguments)]
pub fn end_operation<'a>(
    conn: &Connection,
    operation_conn: &Connection,
    session: &mut session::Session,
    operation_info: &OperationInfo,
    summary_str: &str,
    force_hash: impl Into<Option<&'a str>>,
) -> Result<Operation, OperationError> {
    let db_uuid = metadata::get_db_uuid(conn);
    // determine if this operation has already happened
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();

    let dependencies = get_changeset_dependencies(conn, &output);

    let hash = if let Some(hash) = force_hash.into() {
        hash.to_string()
    } else {
        if output.is_empty() {
            return Err(OperationError::NoChanges);
        }
        let mut hasher = Sha256::new();
        hasher.update(&db_uuid[..]);
        hasher.update(&output[..]);
        hasher.update(&dependencies[..]);
        format!("{:x}", hasher.finalize())
    };

    operation_conn
        .execute("SAVEPOINT new_operation;", [])
        .unwrap();

    match Operation::create(operation_conn, &db_uuid, &operation_info.description, &hash) {
        Ok(operation) => {
            for op_file in operation_info.files.iter() {
                let fa =
                    FileAddition::create(operation_conn, &op_file.file_path, op_file.file_type);
                Operation::add_file(operation_conn, &operation.hash, fa.id)
                    .map_err(|err| OperationError::SQLError(format!("{err}")))?
            }
            OperationSummary::create(operation_conn, &operation.hash, summary_str);
            write_changeset(&operation, &output, &dependencies);
            operation_conn
                .execute("RELEASE SAVEPOINT new_operation;", [])
                .unwrap();
            Ok(operation)
        }
        Err(rusqlite::Error::SqliteFailure(err, details)) => {
            operation_conn
                .execute("ROLLBACK TRANSACTION TO SAVEPOINT new_operation;", [])
                .unwrap();
            if err.code == rusqlite::ErrorCode::ConstraintViolation {
                Err(OperationError::OperationExists)
            } else {
                panic!("something bad happened querying the database {details:?}");
            }
        }
        Err(e) => {
            operation_conn
                .execute("ROLLBACK TRANSACTION TO SAVEPOINT new_operation;", [])
                .unwrap();
            panic!("something bad happened querying the database {e:?}");
        }
    }
}

pub fn attach_session(session: &mut session::Session) {
    for table in CHANGE_TABLES {
        session.attach(Some(table)).unwrap();
    }
}

pub fn checkout(
    conn: &Connection,
    operation_conn: &Connection,
    db_uuid: &str,
    branch_name: &Option<String>,
    operation_hash: Option<String>,
) {
    let mut dest_op_hash = operation_hash.unwrap_or_default();
    if let Some(name) = branch_name {
        let current_branch = OperationState::get_current_branch(operation_conn, db_uuid)
            .expect("No current branch set");
        let branch = Branch::get_by_name(operation_conn, db_uuid, name)
            .unwrap_or_else(|| panic!("No branch named {name}"));
        if current_branch != branch.id {
            OperationState::set_branch(operation_conn, db_uuid, name);
        }
        if dest_op_hash.is_empty() {
            dest_op_hash = branch.current_operation_hash.unwrap();
        }
    }
    if dest_op_hash.is_empty() {
        panic!("No operation defined.");
    }
    move_to(
        conn,
        operation_conn,
        &Operation::get_by_hash(operation_conn, &dest_op_hash)
            .unwrap_or_else(|_| panic!("Hash {dest_op_hash} does not exist.")),
    );
}

pub fn parse_patch_operations(
    branch_operations: &[Operation],
    head_hash: &str,
    operations: &str,
) -> Vec<String> {
    let mut results = vec![];
    let (head_pos, _) = branch_operations
        .iter()
        .find_position(|op| op.hash == head_hash)
        .expect("Current head position is not in branch.");
    for operation in operations.split(",") {
        if operation.contains("..") {
            let mut it = operation.split("..");
            let start = it.next().unwrap().parse::<String>().unwrap();
            let end = it.next().unwrap().parse::<String>().unwrap();

            let start_hash = if start.starts_with("HEAD") {
                if start.contains("~") {
                    let mut it = start.rsplit("~");
                    let count = it.next().unwrap().parse::<usize>().unwrap();
                    branch_operations[head_pos - count].hash.clone()
                } else {
                    branch_operations[head_pos].hash.clone()
                }
            } else {
                start
            };

            let end_hash = if end.starts_with("HEAD") {
                if end.contains("~") {
                    let mut it = end.rsplit("~");
                    let count = it.next().unwrap().parse::<usize>().unwrap();
                    branch_operations[head_pos - count].hash.clone()
                } else {
                    branch_operations[head_pos].hash.clone()
                }
            } else {
                end
            };
            let mut start_iter = branch_operations
                .iter()
                .positions(|op| op.hash.starts_with(start_hash.as_str()));
            let start_pos = start_iter
                .next()
                .unwrap_or_else(|| panic!("Unable to find starting hash {start_hash:?}"));
            let mut end_iter = branch_operations
                .iter()
                .positions(|op| op.hash.starts_with(end_hash.as_str()));
            let end_pos = end_iter
                .next()
                .unwrap_or_else(|| panic!("Unable to find end hash {end_hash:?}"));
            if start_iter.next().is_some() {
                panic!("Start hash {start_hash} is ambiguous.");
            }
            if end_iter.next().is_some() {
                panic!("Ending hash {end_hash} is ambiguous.");
            }
            results.extend(
                branch_operations[start_pos..end_pos + 1]
                    .iter()
                    .map(|op| op.hash.clone()),
            );
        } else {
            let hash = if operation.starts_with("HEAD") {
                if operation.contains("~") {
                    let mut it = operation.rsplit("~");
                    let count = it.next().unwrap().parse::<usize>().unwrap();
                    branch_operations[head_pos - count].hash.clone()
                } else {
                    branch_operations[head_pos].hash.clone()
                }
            } else {
                let mut iter = branch_operations
                    .iter()
                    .positions(|op| op.hash.starts_with(operation));
                let pos = iter
                    .next()
                    .unwrap_or_else(|| panic!("Unable to find starting hash {operation:?}"));
                if iter.next().is_some() {
                    panic!("Hash {operation:?} is ambiguous.");
                }
                branch_operations[pos].hash.clone()
            };
            results.push(hash);
        }
    }
    results
}

// The url-parse crate doesn't know about file-based urls, so we need to provide it with a
// custom set of port mappings
fn port_mappings() -> HashMap<&'static str, (u32, &'static str)> {
    HashMap::from([
        ("file", (0, "file")),
        ("https", (443, "Hypertext Transfer Protocol Secure")),
        ("s3", (443, "Amazon S3 File Transfer Protocol")),
    ])
}

fn file_exists(file_path: &str, filename: &str) -> Result<bool, RemoteOperationError> {
    match Parser::new(Some(port_mappings())).parse(file_path) {
        Ok(result) => {
            if let Some(scheme) = result.scheme {
                if scheme == "file" {
                    let path_parts = result.path.unwrap();
                    let mut path = PathBuf::from("/");
                    for part in path_parts {
                        path.push(part);
                    }

                    let file_parts = FilePath::new(&filename);
                    for part in file_parts {
                        path.push(part);
                    }
                    Ok(path.exists())
                } else {
                    Err(RemoteOperationError::UnsupportedRemoteScheme(
                        scheme,
                        file_path.to_string(),
                    ))
                }
            } else {
                Err(RemoteOperationError::InvalidRemoteUrl(
                    file_path.to_string(),
                ))
            }
        }
        Err(_) => Err(RemoteOperationError::InvalidRemoteUrl(
            file_path.to_string(),
        )),
    }
}

fn transfer_file(
    local_file_path: &str,
    remote_url: &str,
    filename: &str,
    push: bool,
) -> Result<(), RemoteOperationError> {
    match Parser::new(Some(port_mappings())).parse(remote_url) {
        Ok(result) => {
            if let Some(scheme) = result.scheme {
                if scheme == "file" {
                    // copy
                    if let Some(path_parts) = result.path {
                        let mut remote_path = path_parts
                            .iter()
                            .map(|p| p.to_string())
                            .collect::<PathBuf>();
                        let file_relative_path = FilePath::new(&filename);
                        let source_filename =
                            file_relative_path.file_name().unwrap().to_str().unwrap();

                        if push {
                            // Ensure that the directory we're copying to exists--the filename may
                            // actually be a relative path
                            if let Some(file_prefix) = file_relative_path.parent() {
                                for part in file_prefix {
                                    remote_path.push(part);
                                }
                            }
                            let remote_directory = format!("/{}", &remote_path.to_str().unwrap());
                            fs::create_dir_all(&remote_directory)?;

                            let remote_filename =
                                format!("{}/{}", remote_directory, source_filename);
                            let local_filename = format!("{}/{}", local_file_path, filename);

                            fs::copy(&local_filename, &remote_filename).map_err(|_| {
                                RemoteOperationError::FileTransferError(
                                    filename.to_string(),
                                    local_filename,
                                    remote_filename,
                                )
                            })?;
                        } else {
                            // Ensure that the directory we're copying to exists--the filename may
                            // actually be a relative path
                            let mut local_path = PathBuf::from(local_file_path);
                            if let Some(file_prefix) = file_relative_path.parent() {
                                for part in file_prefix {
                                    local_path.push(part);
                                }
                            }
                            fs::create_dir_all(&local_path)?;

                            let remote_path_filename =
                                format!("/{}/{}", remote_path.to_str().unwrap(), filename);
                            let local_filename =
                                format!("{}/{}", local_path.to_str().unwrap(), source_filename);
                            fs::copy(&remote_path_filename, &local_filename).map_err(|_| {
                                RemoteOperationError::FileTransferError(
                                    filename.to_string(),
                                    remote_path_filename,
                                    local_filename,
                                )
                            })?;
                        }
                        Ok(())
                    } else {
                        Err(RemoteOperationError::InvalidRemoteUrl(
                            remote_url.to_string(),
                        ))
                    }
                } else {
                    Err(RemoteOperationError::UnsupportedRemoteScheme(
                        scheme,
                        remote_url.to_string(),
                    ))
                }
            } else {
                Err(RemoteOperationError::InvalidRemoteUrl(
                    remote_url.to_string(),
                ))
            }
        }
        Err(_) => Err(RemoteOperationError::InvalidRemoteUrl(
            remote_url.to_string(),
        )),
    }
}

fn generate_file_manifest(
    local_op_conn: &Connection,
    remote_op_conn: Option<&Connection>,
    db_name: String,
) -> Result<Vec<String>, RemoteOperationError> {
    let mut remote_filenames = HashSet::new();
    if let Some(remote_op_conn) = remote_op_conn {
        let remote_ops = Operation::query(
            remote_op_conn,
            "select * from operation;",
            rusqlite::params![],
        );
        for remote_op in remote_ops {
            let op_file_additions =
                FileAddition::get_files_for_operation(remote_op_conn, &remote_op.hash);
            for op_file_addition in op_file_additions {
                remote_filenames.insert(op_file_addition.file_path);
            }
        }
    }

    let local_ops = Operation::query(
        local_op_conn,
        "select * from operation;",
        rusqlite::params![],
    );
    let mut local_filenames = HashSet::new();
    for local_op in local_ops {
        let op_file_additions =
            FileAddition::get_files_for_operation(local_op_conn, &local_op.hash);
        for op_file_addition in op_file_additions {
            local_filenames.insert(op_file_addition.file_path);
        }
    }

    let mut filenames_to_push = local_filenames
        .difference(&remote_filenames)
        .collect::<Vec<&String>>();
    let binding = ".gen/gen.db".to_string();
    filenames_to_push.push(&binding);
    let db_filename = format!(".gen/{}.db", db_name);
    filenames_to_push.push(&db_filename);

    Ok(filenames_to_push.iter().map(|f| f.to_string()).collect())
}

// Pushes the current state of the local repo and branch to the corresponding remote repo and branch
pub fn push(operation_conn: &Connection, db_uuid: &str) -> Result<(), RemoteOperationError> {
    let mut stmt = operation_conn
        .prepare("select remote_url from defaults where id = 1;")
        .unwrap();
    let row: Option<String> = stmt.query_row((), |row| row.get(0)).unwrap();
    if let Some(remote_url) = row {
        let mut remote_op_db_path = tempdir().unwrap().into_path();
        let remote_op_db_dir = remote_op_db_path.to_str().unwrap().to_string();
        let remote_op_conn = if file_exists(&remote_url, ".gen/gen.db")? {
            transfer_file(&remote_op_db_dir, &remote_url, ".gen/gen.db", false)?;

            remote_op_db_path.push(".gen");
            remote_op_db_path.push("gen.db");
            Some(get_operation_connection(Some(remote_op_db_path)))
        } else {
            None
        };

        let current_branch_id =
            OperationState::get_current_branch(operation_conn, db_uuid).unwrap();
        let remote_branch_operations = if let Some(ref remote_op_conn) = remote_op_conn {
            let branch = Branch::get_by_id(remote_op_conn, current_branch_id);
            if let Some(_branch) = branch {
                Branch::get_operations(remote_op_conn, current_branch_id)
            } else {
                vec![]
            }
        } else {
            vec![]
        };
        let local_branch_operations = Branch::get_operations(operation_conn, current_branch_id);

        let remote_op_hashes = remote_branch_operations
            .iter()
            .map(|op| op.hash.clone())
            .collect::<HashSet<String>>();
        let local_op_hashes = local_branch_operations
            .iter()
            .map(|op| op.hash.clone())
            .collect::<HashSet<String>>();
        let leading_remote_ops = remote_op_hashes
            .difference(&local_op_hashes)
            .collect::<Vec<&String>>();
        if !leading_remote_ops.is_empty() {
            return Err(RemoteOperationError::RemoteBranchAhead);
        }

        let mut stmt = operation_conn
            .prepare("select db_name from defaults where id = 1;")
            .unwrap();
        let row: Option<String> = stmt.query_row((), |row| row.get(0)).unwrap();
        let db_name = if let Some(row) = row {
            row
        } else {
            "default".to_string()
        };

        let gen_dir = get_gen_dir().unwrap();
        let parent_dir = FilePath::new(&gen_dir).parent().unwrap();
        let parent_dir_str = parent_dir.to_str().unwrap().to_string();

        let filenames = generate_file_manifest(operation_conn, remote_op_conn.as_ref(), db_name)?;
        for filename in filenames {
            if filename.starts_with("/") {
                println!("Skipping since it is an absolute path: {}", filename);
            } else {
                transfer_file(&parent_dir_str, &remote_url, &filename, true)?;
            }
        }
        Ok(())
    } else {
        Err(RemoteOperationError::RemoteUrlNotSet)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::get_connection as get_real_connection;
    use crate::imports::fasta::import_fasta;
    use crate::models::file_types::FileTypes;
    use crate::models::operations::{setup_db, Branch, Operation, OperationState};
    use crate::models::{
        edge::Edge,
        metadata,
        node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID},
        path::Path as PathModel,
        path_edge::PathEdge,
        sample::Sample,
    };
    use crate::test_helpers::{
        create_operation, get_connection, get_operation_connection, setup_block_group,
        setup_gen_dir,
    };
    use crate::updates::vcf::update_with_vcf;
    use rusqlite::types::Value;
    use std::path::{Path, PathBuf};

    #[cfg(test)]
    mod changeset_dependencies {
        use super::*;

        #[test]
        fn test_tracks_nodes_and_sequences_from_previous_block_group_edges() {
            setup_gen_dir();
            let conn = &get_connection(None);
            let db_uuid = &metadata::get_db_uuid(conn);
            let op_conn = &get_operation_connection(None);
            setup_db(op_conn, db_uuid);

            let (bg_id, _path_id) = setup_block_group(conn);
            let old_edges = BlockGroupEdge::edges_for_block_group(conn, bg_id);

            let mut session = start_operation(conn);
            // make a blockgroup with an edge from our parent blockgroup
            let _ = Sample::create(conn, "new").unwrap();
            let new_bg = BlockGroup::create(conn, "test", Some("new"), "new-bg");
            let shared_edge = old_edges[0].edge.clone();
            BlockGroupEdge::bulk_create(
                conn,
                &[BlockGroupEdgeData {
                    block_group_id: new_bg.id,
                    edge_id: shared_edge.id,
                    chromosome_index: 0,
                    phased: 0,
                }],
            );
            let operation = end_operation(
                conn,
                op_conn,
                &mut session,
                &OperationInfo {
                    files: vec![],
                    description: "test".to_string(),
                },
                "test",
                None,
            )
            .unwrap();

            let dependencies = load_changeset_dependencies(&operation);
            assert_eq!(dependencies.nodes[0].id, shared_edge.target_node_id);
            assert_eq!(dependencies.nodes.len(), 1);
            let nodes = Node::get_nodes(conn, &[shared_edge.target_node_id]);
            assert_eq!(dependencies.sequences[0].hash, nodes[0].sequence_hash);
            assert_eq!(dependencies.sequences.len(), 1);
        }
    }

    #[cfg(test)]
    mod merge {
        use super::*;
        use crate::operation_management::checkout;

        #[test]
        fn test_merges() {
            setup_gen_dir();
            let conn = &get_connection(None);
            let db_uuid = &metadata::get_db_uuid(conn);
            let op_conn = &get_operation_connection(None);
            setup_db(op_conn, db_uuid);

            let op_1 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "fasta_addition",
                "op-1",
            );
            let op_2 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "fasta_addition",
                "op-2",
            );

            let branch_1 = Branch::create(op_conn, db_uuid, "branch-1");
            let branch_2 = Branch::create(op_conn, db_uuid, "branch-2");
            OperationState::set_branch(op_conn, db_uuid, "branch-1");
            let op_3 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "vcf_addition",
                "op-3",
            );
            let op_4 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "vcf_addition",
                "op-4",
            );
            checkout(conn, op_conn, db_uuid, &Some("branch-2".to_string()), None);
            let op_5 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "vcf_addition",
                "op-5",
            );
            let op_6 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "vcf_addition",
                "op-6",
            );

            checkout(conn, op_conn, db_uuid, &Some("branch-1".to_string()), None);
            let new_operations = merge(
                conn,
                op_conn,
                db_uuid,
                branch_1.id,
                branch_2.id,
                "merge-test",
            )
            .unwrap()
            .iter()
            .map(|op| op.hash.clone())
            .collect::<Vec<String>>();

            let b1_ops = Branch::get_operations(op_conn, branch_1.id)
                .iter()
                .map(|f| f.hash.clone())
                .collect::<Vec<String>>();

            let b2_ops = Branch::get_operations(op_conn, branch_2.id)
                .iter()
                .map(|f| f.hash.clone())
                .collect::<Vec<String>>();

            assert_eq!(
                b1_ops,
                vec![
                    op_1.hash.clone(),
                    op_2.hash.clone(),
                    op_3.hash.clone(),
                    op_4.hash.clone()
                ]
                .into_iter()
                .chain(new_operations.into_iter())
                .collect::<Vec<String>>()
            );
            assert_eq!(b2_ops, vec![op_1.hash, op_2.hash, op_5.hash, op_6.hash]);
        }
    }

    #[cfg(test)]
    mod parse_patch_operations {
        use super::*;
        use crate::operation_management::parse_patch_operations;

        #[test]
        fn test_head_shorthand() {
            setup_gen_dir();
            let conn = &get_connection(None);
            let db_uuid = &metadata::get_db_uuid(conn);
            let op_conn = &get_operation_connection(None);
            setup_db(op_conn, db_uuid);

            let _op_1 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "fasta_addition",
                "op-1",
            );
            let op_2 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "fasta_addition",
                "op-2",
            );
            let op_3 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "vcf_addition",
                "op-3",
            );

            let branch = Branch::get_by_name(op_conn, db_uuid, "main").unwrap();
            let ops = Branch::get_operations(op_conn, branch.id);
            assert_eq!(
                parse_patch_operations(
                    &ops,
                    &branch.current_operation_hash.unwrap(),
                    "HEAD~1..HEAD"
                ),
                vec![op_2.hash, op_3.hash]
            );
        }

        #[test]
        fn test_hash_shorthand() {
            setup_gen_dir();
            let conn = &get_connection(None);
            let db_uuid = &metadata::get_db_uuid(conn);
            let op_conn = &get_operation_connection(None);
            setup_db(op_conn, db_uuid);

            let _op_1 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "fasta_addition",
                "op-1-abc-123",
            );
            let op_2 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "fasta_addition",
                "op-2-abc-123",
            );
            let op_3 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "vcf_addition",
                "op-3-abc-13",
            );

            let branch = Branch::get_by_name(op_conn, db_uuid, "main").unwrap();
            let ops = Branch::get_operations(op_conn, branch.id);
            let head_hash = branch.current_operation_hash.unwrap();
            assert_eq!(
                parse_patch_operations(
                    &ops,
                    &head_hash,
                    &format!(
                        "{op_2}..{op_3}",
                        op_2 = &op_2.hash[..6],
                        op_3 = &op_3.hash[..6]
                    )
                ),
                vec![op_2.hash.clone(), op_3.hash]
            );

            assert_eq!(
                parse_patch_operations(&ops, &head_hash, &op_2.hash[..6]),
                vec![op_2.hash]
            );
        }

        #[test]
        #[should_panic(expected = "Start hash op- is ambiguous.")]
        fn test_error_on_ambiguous_hash_shorthand() {
            setup_gen_dir();
            let conn = &get_connection(None);
            let db_uuid = &metadata::get_db_uuid(conn);
            let op_conn = &get_operation_connection(None);
            setup_db(op_conn, db_uuid);

            let _op_1 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "fasta_addition",
                "op-1-abc-123",
            );
            let op_2 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "fasta_addition",
                "op-2-abc-123",
            );
            let op_3 = create_operation(
                conn,
                op_conn,
                "foo",
                FileTypes::Fasta,
                "vcf_addition",
                "op-3-abc-13",
            );

            let branch = Branch::get_by_name(op_conn, db_uuid, "main").unwrap();
            let ops = Branch::get_operations(op_conn, branch.id);
            assert_eq!(
                parse_patch_operations(
                    &ops,
                    &branch.current_operation_hash.unwrap(),
                    &format!(
                        "{op_2}..{op_3}",
                        op_2 = &op_2.hash[..3],
                        op_3 = &op_3.hash[..3]
                    )
                ),
                vec![op_2.hash, op_3.hash]
            );
        }
    }

    #[test]
    fn test_writes_operation_hash() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);
        let operation = Operation::create(op_conn, &db_uuid, "test", "some-hash").unwrap();
        OperationState::set_operation(op_conn, &db_uuid, &operation.hash);
        assert_eq!(
            OperationState::get_operation(op_conn, &db_uuid).unwrap(),
            operation.hash
        );
    }

    #[test]
    fn test_records_patch_dependencies() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        // create some stuff before we attach to our main session that will be required as extra information
        let (bg_id, _path_id) = setup_block_group(conn);
        let binding = BlockGroup::query(
            conn,
            "select * from block_groups where id = ?1;",
            rusqlite::params!(Value::from(bg_id)),
        );
        let dep_bg = binding.first().unwrap();

        let existing_seq = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAATTTT")
            .save(conn);
        let existing_node_id = Node::create(conn, existing_seq.hash.as_str(), None);

        let mut session = start_operation(conn);

        let random_seq = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCG")
            .save(conn);
        let random_node_id = Node::create(conn, random_seq.hash.as_str(), None);

        let new_edge1 = Edge::create(
            conn,
            PATH_START_NODE_ID,
            0,
            Strand::Forward,
            existing_node_id,
            0,
            Strand::Forward,
        );
        let block_group_edge1 = BlockGroupEdgeData {
            block_group_id: bg_id,
            edge_id: new_edge1.id,
            chromosome_index: 0,
            phased: 0,
        };

        let new_edge2 = Edge::create(
            conn,
            existing_node_id,
            1,
            Strand::Forward,
            random_node_id,
            0,
            Strand::Forward,
        );
        let block_group_edge2 = BlockGroupEdgeData {
            block_group_id: bg_id,
            edge_id: new_edge2.id,
            chromosome_index: 0,
            phased: 0,
        };

        let new_edge3 = Edge::create(
            conn,
            random_node_id,
            1,
            Strand::Forward,
            PATH_END_NODE_ID,
            0,
            Strand::Forward,
        );
        let block_group_edge3 = BlockGroupEdgeData {
            block_group_id: bg_id,
            edge_id: new_edge3.id,
            chromosome_index: 0,
            phased: 0,
        };

        BlockGroupEdge::bulk_create(
            conn,
            &[block_group_edge1, block_group_edge2, block_group_edge3],
        );

        let _path = PathModel::create(
            conn,
            "test_path",
            bg_id,
            &[new_edge1.id, new_edge2.id, new_edge3.id],
        );

        let operation = end_operation(
            conn,
            op_conn,
            &mut session,
            &OperationInfo {
                files: vec![OperationFile {
                    file_path: "test".to_string(),
                    file_type: FileTypes::Fasta,
                }],
                description: "test".to_string(),
            },
            "test",
            None,
        )
        .unwrap();

        let dependency_path =
            get_changeset_path(&operation).join(format!("{op_id}.dep", op_id = operation.hash));
        let dependencies: DependencyModels =
            serde_json::from_reader(fs::File::open(dependency_path).unwrap()).unwrap();
        assert_eq!(dependencies.sequences.len(), 2);
        assert_eq!(
            dependencies.block_group[0].collection_name,
            dep_bg.collection_name
        );
        assert_eq!(dependencies.block_group[0].name, dep_bg.name);
        assert_eq!(dependencies.block_group[0].sample_name, dep_bg.sample_name);

        let result_path = dependencies.paths.first().unwrap();
        assert_eq!(result_path.block_group_id, bg_id);
        let result_edges = PathEdge::edges_for_path(conn, result_path.id);
        assert_eq!(result_edges.len(), 3);
        assert_eq!(result_edges[0].id, new_edge1.id);
        assert_eq!(result_edges[0].source_node_id, PATH_START_NODE_ID);
        assert_eq!(result_edges[0].target_node_id, existing_node_id);
        assert_eq!(result_edges[1].id, new_edge2.id);
        assert_eq!(result_edges[1].source_node_id, existing_node_id);
        assert_eq!(result_edges[1].target_node_id, random_node_id);
        assert_eq!(result_edges[2].id, new_edge3.id);
        assert_eq!(result_edges[2].source_node_id, random_node_id);
        assert_eq!(result_edges[2].target_node_id, PATH_END_NODE_ID);
    }

    #[test]
    fn test_round_trip() {
        setup_gen_dir();
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/simple.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &mut get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(None);
        setup_db(operation_conn, &db_uuid);
        let collection = "test".to_string();
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            None,
            false,
            conn,
            operation_conn,
        )
        .unwrap();
        let block_group_count =
            BlockGroup::query(conn, "select * from block_groups", rusqlite::params!()).len();
        let edge_count = Edge::query(conn, "select * from edges", rusqlite::params!()).len();
        let block_group_edge_count =
            BlockGroupEdge::query(conn, "select * from block_group_edges", rusqlite::params!())
                .len();
        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len();
        let sample_count = Sample::query(conn, "select * from samples", rusqlite::params!()).len();
        let op_count = Operation::query(
            operation_conn,
            "select * from operation",
            rusqlite::params!(),
        )
        .len();
        assert_eq!(block_group_count, 1);
        assert_eq!(edge_count, 2);
        assert_eq!(block_group_edge_count, 2);
        assert_eq!(node_count, 3);
        assert_eq!(sample_count, 0);
        assert_eq!(op_count, 1);
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
            None,
        )
        .unwrap();
        let block_group_count =
            BlockGroup::query(conn, "select * from block_groups", rusqlite::params!()).len();
        let edge_count = Edge::query(conn, "select * from edges", rusqlite::params!()).len();
        let block_group_edge_count =
            BlockGroupEdge::query(conn, "select * from block_group_edges", rusqlite::params!())
                .len();
        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len();
        let sample_count = Sample::query(conn, "select * from samples", rusqlite::params!()).len();
        let op_count = Operation::query(
            operation_conn,
            "select * from operation",
            rusqlite::params!(),
        )
        .len();
        // NOTE: 3 block groups get created with the update from vcf, corresponding to the unknown, G1, and foo samples
        assert_eq!(block_group_count, 4);
        // NOTE: The edge count is 5 because of the following:
        // * 1 edge from the source node to the node created by the fasta import
        // * 1 edge from the node created by the fasta import to the sink node
        // * 1 edge representing the deletion
        // * 2 edges to and from the node representing the second alt sequence
        assert_eq!(edge_count, 5);
        // NOTE: The block group edge count is 16 because of the following:
        // * 4 edges (one per block group) from the virtual source node
        // * 4 edges (one per block group) to the virtual sink node
        // * 2 block group edges for the G1 sample (1 edge for the first alt sequence, a deletion, with both the 0 and 1 chromosome indices for those edges, 1 * 2 = 2)
        // * 6 block group edges for the foo sample (1 edge to and from the node representing the
        // first alt sequence, with both the 0 and 1 chromosome indices for those edges, 1 * 2 = 2;
        // 2 edges to and from the node representing the second alt sequence, with both the 0 and 1
        // chromosome indices for those edges, 2 * 2 = 4)
        // 4 + 4 + 2 + 6 = 16
        assert_eq!(block_group_edge_count, 16);
        // NOTE: The node count is 5:
        // * 2 source and sink nodes
        // * 1 node created by the initial fasta import
        // * 2 nodes created by the VCF update.  See above explanation of edge count for more details.
        assert_eq!(node_count, 5);
        assert_eq!(sample_count, 3);
        assert_eq!(op_count, 2);

        // revert back to state 1 where vcf samples and blockpaths do not exist
        revert_changeset(
            conn,
            &Operation::get_by_hash(
                operation_conn,
                &OperationState::get_operation(operation_conn, &db_uuid).unwrap(),
            )
            .unwrap_or_else(|_| panic!("Hash does not exist.")),
        );

        let block_group_count =
            BlockGroup::query(conn, "select * from block_groups", rusqlite::params!()).len();
        let edge_count = Edge::query(conn, "select * from edges", rusqlite::params!()).len();
        let block_group_edge_count =
            BlockGroupEdge::query(conn, "select * from block_group_edges", rusqlite::params!())
                .len();
        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len();
        let sample_count = Sample::query(conn, "select * from samples", rusqlite::params!()).len();
        let op_count = Operation::query(
            operation_conn,
            "select * from operation",
            rusqlite::params!(),
        )
        .len();
        assert_eq!(block_group_count, 1);
        assert_eq!(edge_count, 2);
        assert_eq!(block_group_edge_count, 2);
        assert_eq!(node_count, 3);
        assert_eq!(sample_count, 0);
        assert_eq!(op_count, 2);

        let op = Operation::get_by_hash(
            operation_conn,
            &OperationState::get_operation(operation_conn, &db_uuid).unwrap(),
        )
        .unwrap();
        let changeset = load_changeset(&op);
        let input: &mut dyn Read = &mut changeset.as_slice();
        let mut iter = ChangesetIter::start_strm(&input).unwrap();
        let dependencies = load_changeset_dependencies(&op);

        apply_changeset(conn, &mut iter, &dependencies);
        let block_group_count =
            BlockGroup::query(conn, "select * from block_groups", rusqlite::params!()).len();
        let edge_count = Edge::query(conn, "select * from edges", rusqlite::params!()).len();
        let block_group_edge_count =
            BlockGroupEdge::query(conn, "select * from block_group_edges", rusqlite::params!())
                .len();
        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len();
        let sample_count = Sample::query(conn, "select * from samples", rusqlite::params!()).len();
        let op_count = Operation::query(
            operation_conn,
            "select * from operation",
            rusqlite::params!(),
        )
        .len();
        assert_eq!(block_group_count, 4);
        assert_eq!(edge_count, 5);
        assert_eq!(block_group_edge_count, 16);
        assert_eq!(node_count, 5);
        assert_eq!(sample_count, 3);
        assert_eq!(op_count, 2);
    }

    #[test]
    fn test_cross_branch_patch() {
        setup_gen_dir();
        let fasta_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.fa");
        let vcf_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.vcf");
        let vcf2_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple2.vcf");
        let conn = &mut get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(None);
        setup_db(operation_conn, &db_uuid);
        let collection = "test".to_string();

        let _op_1 = import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            None,
            false,
            conn,
            operation_conn,
        )
        .unwrap();

        Branch::create(operation_conn, &db_uuid, "branch-1");
        Branch::create(operation_conn, &db_uuid, "branch-2");
        checkout(
            conn,
            operation_conn,
            &db_uuid,
            &Some("branch-1".to_string()),
            None,
        );

        let op_2 = update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
            None,
        )
        .unwrap();

        let foo_bg_id = BlockGroup::get_id(conn, &collection, Some("foo"), "m123");
        let patch_1_seqs =
            HashSet::from_iter(vec!["ATCATCGATCGATCGATCGGGAACACACAGAGA".to_string()]);

        assert_eq!(
            BlockGroup::get_all_sequences(conn, foo_bg_id, false),
            patch_1_seqs
        );
        assert_eq!(
            BlockGroup::query(conn, "select * from block_groups;", rusqlite::params!())
                .iter()
                .map(|v| v.sample_name.clone().unwrap_or("".to_string()))
                .collect::<Vec<String>>(),
            vec![
                "".to_string(),
                "unknown".to_string(),
                "G1".to_string(),
                "foo".to_string()
            ]
        );

        checkout(
            conn,
            operation_conn,
            &db_uuid,
            &Some("branch-2".to_string()),
            None,
        );
        let _op_3 = update_with_vcf(
            &vcf2_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
            None,
        );

        let foo_bg_id = BlockGroup::get_id(conn, &collection, Some("foo"), "m123");
        let patch_2_seqs = HashSet::from_iter(vec!["ATCGATCGATCGAGATCGGGAACACACAGAGA".to_string()]);
        assert_eq!(
            BlockGroup::get_all_sequences(conn, foo_bg_id, false),
            patch_2_seqs
        );
        assert_ne!(patch_1_seqs, patch_2_seqs);
        assert_eq!(
            BlockGroup::query(conn, "select * from block_groups;", rusqlite::params!())
                .iter()
                .map(|v| v.sample_name.clone().unwrap_or("".to_string()))
                .collect::<Vec<String>>(),
            vec!["".to_string(), "foo".to_string()]
        );

        // apply changes from branch-1, it will be operation id 2
        apply(conn, operation_conn, &op_2.hash, None).unwrap();

        let foo_bg_id = BlockGroup::get_id(conn, &collection, Some("foo"), "m123");
        let patch_2_seqs = HashSet::from_iter(vec!["ATCATCGATCGAGATCGGGAACACACAGAGA".to_string()]);
        assert_eq!(
            BlockGroup::get_all_sequences(conn, foo_bg_id, false),
            patch_2_seqs
        );
        assert_eq!(
            BlockGroup::query(conn, "select * from block_groups;", rusqlite::params!())
                .iter()
                .map(|v| v.sample_name.clone().unwrap_or("".to_string()))
                .collect::<Vec<String>>(),
            vec![
                "".to_string(),
                "foo".to_string(),
                "unknown".to_string(),
                "G1".to_string()
            ]
        );

        let unknown_bg_id = BlockGroup::get_id(conn, &collection, Some("unknown"), "m123");
        let unknown_seqs =
            HashSet::from_iter(vec!["ATCATCGATAGACGATCGATCGGGAACACACAGAGA".to_string()]);
        assert_eq!(
            BlockGroup::get_all_sequences(conn, unknown_bg_id, false),
            unknown_seqs
        );
        assert_ne!(unknown_seqs, patch_2_seqs);
    }

    #[test]
    fn test_branch_movement() {
        setup_gen_dir();
        let fasta_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.fa");
        let vcf_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.vcf");
        let vcf2_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple2.vcf");
        let conn = &mut get_connection("t2.db");
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(None);
        setup_db(operation_conn, &db_uuid);
        let collection = "test".to_string();
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            None,
            false,
            conn,
            operation_conn,
        )
        .unwrap();
        let edge_count = Edge::query(conn, "select * from edges", rusqlite::params!()).len();
        let block_group_edge_count =
            BlockGroupEdge::query(conn, "select * from block_group_edges", rusqlite::params!())
                .len();
        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len();
        let sample_count = Sample::query(conn, "select * from samples", rusqlite::params!()).len();
        let op_count = Operation::query(
            operation_conn,
            "select * from operation",
            rusqlite::params!(),
        )
        .len();
        assert_eq!(edge_count, 2);
        assert_eq!(block_group_edge_count, 2);
        assert_eq!(node_count, 3);
        assert_eq!(sample_count, 0);
        assert_eq!(op_count, 1);

        let branch_1 = Branch::create(operation_conn, &db_uuid, "branch_1");

        let branch_2 = Branch::create(operation_conn, &db_uuid, "branch_2");

        OperationState::set_branch(operation_conn, &db_uuid, "branch_1");
        assert_eq!(
            OperationState::get_current_branch(operation_conn, &db_uuid).unwrap(),
            branch_1.id
        );

        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
            None,
        )
        .unwrap();
        let edge_count = Edge::query(conn, "select * from edges", rusqlite::params!()).len();
        let block_group_edge_count =
            BlockGroupEdge::query(conn, "select * from block_group_edges", rusqlite::params!())
                .len();
        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len();
        let sample_count = Sample::query(conn, "select * from samples", rusqlite::params!()).len();
        let op_count = Operation::query(
            operation_conn,
            "select * from operation",
            rusqlite::params!(),
        )
        .len();
        assert_eq!(edge_count, 5);
        assert_eq!(block_group_edge_count, 16);
        assert_eq!(node_count, 5);
        assert_eq!(sample_count, 3);
        assert_eq!(op_count, 2);

        // checkout branch 2
        checkout(
            conn,
            operation_conn,
            &db_uuid,
            &Some("branch_2".to_string()),
            None,
        );

        assert_eq!(
            OperationState::get_current_branch(operation_conn, &db_uuid).unwrap(),
            branch_2.id
        );

        // ensure branch 1 operations have been undone
        let edge_count = Edge::query(conn, "select * from edges", rusqlite::params!()).len();
        let block_group_edge_count =
            BlockGroupEdge::query(conn, "select * from block_group_edges", rusqlite::params!())
                .len();
        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len();
        let sample_count = Sample::query(conn, "select * from samples", rusqlite::params!()).len();
        let op_count = Operation::query(
            operation_conn,
            "select * from operation",
            rusqlite::params!(),
        )
        .len();
        assert_eq!(edge_count, 2);
        assert_eq!(block_group_edge_count, 2);
        assert_eq!(node_count, 3);
        assert_eq!(sample_count, 0);
        assert_eq!(op_count, 2);

        // apply vcf2
        update_with_vcf(
            &vcf2_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
            None,
        )
        .unwrap();
        let edge_count = Edge::query(conn, "select * from edges", rusqlite::params!()).len();
        let block_group_edge_count =
            BlockGroupEdge::query(conn, "select * from block_group_edges", rusqlite::params!())
                .len();
        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len();
        let sample_count = Sample::query(conn, "select * from samples", rusqlite::params!()).len();
        let op_count = Operation::query(
            operation_conn,
            "select * from operation",
            rusqlite::params!(),
        )
        .len();
        assert_eq!(edge_count, 3);
        assert_eq!(block_group_edge_count, 6);
        assert_eq!(node_count, 4);
        assert_eq!(sample_count, 1);
        assert_eq!(op_count, 3);

        // migrate to branch 1 again
        checkout(
            conn,
            operation_conn,
            &db_uuid,
            &Some("branch_1".to_string()),
            None,
        );
        assert_eq!(
            OperationState::get_current_branch(operation_conn, &db_uuid).unwrap(),
            branch_1.id
        );

        let edge_count = Edge::query(conn, "select * from edges", rusqlite::params!()).len();
        let block_group_edge_count =
            BlockGroupEdge::query(conn, "select * from block_group_edges", rusqlite::params!())
                .len();
        let node_count = Node::query(conn, "select * from nodes", rusqlite::params!()).len();
        let sample_count = Sample::query(conn, "select * from samples", rusqlite::params!()).len();
        let op_count = Operation::query(
            operation_conn,
            "select * from operation",
            rusqlite::params!(),
        )
        .len();
        assert_eq!(edge_count, 5);
        assert_eq!(block_group_edge_count, 16);
        assert_eq!(node_count, 5);
        assert_eq!(sample_count, 3);
        assert_eq!(op_count, 3);
    }

    #[test]
    fn test_reset_hides_operations() {
        setup_gen_dir();
        let conn = &mut get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(None);
        setup_db(operation_conn, &db_uuid);

        let op_1 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-1",
        );
        let op_2 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-2",
        );
        let op_3 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-3",
        );
        let op_4 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-4",
        );
        let op_5 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-5",
        );

        let branch_id = OperationState::get_current_branch(operation_conn, &db_uuid).unwrap();

        assert!(Branch::get_masked_operations(operation_conn, branch_id).is_empty());
        assert_eq!(
            Branch::get_operations(operation_conn, branch_id)
                .iter()
                .map(|op| op.hash.clone())
                .collect::<Vec<String>>(),
            vec![
                op_1.hash.clone(),
                op_2.hash.clone(),
                op_3.hash.clone(),
                op_4.hash,
                op_5.hash
            ]
        );

        reset(conn, operation_conn, &db_uuid, "op-2");
        assert_eq!(
            Branch::get_masked_operations(operation_conn, branch_id),
            vec![op_3.hash]
        );
        assert_eq!(
            Branch::get_operations(operation_conn, branch_id)
                .iter()
                .map(|op| op.hash.clone())
                .collect::<Vec<String>>(),
            vec![op_1.hash.clone(), op_2.hash.clone()]
        );
    }

    #[test]
    fn test_reset_with_branches() {
        // Our setup is like this:
        //          -> 3 -> 4 -> 5 -> 10  branch a
        //        /                \
        //   1-> 2 -> 6 -> 7 -> 8    -> 9 branch b
        //
        // We want to make sure if we reset branch a to 3 that branch b will still show its operations
        setup_gen_dir();
        let conn = &mut get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(None);
        setup_db(operation_conn, &db_uuid);

        let main_branch = Branch::get_by_name(operation_conn, &db_uuid, "main").unwrap();

        let op_1 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-1",
        );
        let op_2 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-2",
        );

        let branch_a = Branch::create(operation_conn, &db_uuid, "branch-a");
        OperationState::set_branch(operation_conn, &db_uuid, "branch-a");
        let op_3 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-3",
        );
        let op_4 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-4",
        );
        let op_5 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-5",
        );
        OperationState::set_branch(operation_conn, &db_uuid, "main");
        OperationState::set_operation(operation_conn, &db_uuid, "op-2");
        let op_6 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-6",
        );
        let op_7 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-7",
        );
        let op_8 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-8",
        );
        OperationState::set_branch(operation_conn, &db_uuid, "branch-a");
        OperationState::set_operation(operation_conn, &db_uuid, "op-5");
        let branch_b = Branch::create(operation_conn, &db_uuid, "branch-b");
        OperationState::set_branch(operation_conn, &db_uuid, "branch-b");
        let op_9 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-9",
        );
        OperationState::set_branch(operation_conn, &db_uuid, "branch-a");
        OperationState::set_operation(operation_conn, &db_uuid, "op-5");
        let op_10 = create_operation(
            conn,
            operation_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-10",
        );

        assert_eq!(
            Branch::get_operations(operation_conn, main_branch.id)
                .iter()
                .map(|op| op.hash.clone())
                .collect::<Vec<String>>(),
            vec![
                op_1.hash.clone(),
                op_2.hash.clone(),
                op_6.hash.clone(),
                op_7.hash.clone(),
                op_8.hash.clone()
            ]
        );
        assert_eq!(
            Branch::get_operations(operation_conn, branch_a.id)
                .iter()
                .map(|op| op.hash.clone())
                .collect::<Vec<String>>(),
            vec![
                op_1.hash.clone(),
                op_2.hash.clone(),
                op_3.hash.clone(),
                op_4.hash.clone(),
                op_5.hash.clone(),
                op_10.hash.clone()
            ]
        );
        assert_eq!(
            Branch::get_operations(operation_conn, branch_b.id)
                .iter()
                .map(|op| op.hash.clone())
                .collect::<Vec<String>>(),
            vec![
                op_1.hash.clone(),
                op_2.hash.clone(),
                op_3.hash.clone(),
                op_4.hash.clone(),
                op_5.hash.clone(),
                op_9.hash.clone()
            ]
        );
        reset(conn, operation_conn, &db_uuid, "op-2");
        assert_eq!(
            Branch::get_masked_operations(operation_conn, branch_a.id),
            vec![op_3.hash.clone(), op_6.hash.clone()]
        );
        assert_eq!(
            Branch::get_operations(operation_conn, main_branch.id)
                .iter()
                .map(|op| op.hash.clone())
                .collect::<Vec<String>>(),
            vec![
                op_1.hash.clone(),
                op_2.hash.clone(),
                op_6.hash.clone(),
                op_7.hash.clone(),
                op_8.hash.clone()
            ]
        );
        assert_eq!(
            Branch::get_operations(operation_conn, branch_a.id)
                .iter()
                .map(|op| op.hash.clone())
                .collect::<Vec<String>>(),
            vec![op_1.hash.clone(), op_2.hash.clone()]
        );
        assert_eq!(
            Branch::get_operations(operation_conn, branch_b.id)
                .iter()
                .map(|op| op.hash.clone())
                .collect::<Vec<String>>(),
            vec![
                op_1.hash.clone(),
                op_2.hash.clone(),
                op_3.hash.clone(),
                op_4.hash.clone(),
                op_5.hash.clone(),
                op_9.hash.clone()
            ]
        );
    }

    #[test]
    fn test_push_to_empty_repo() {
        let gen_path = setup_gen_dir();
        let mut db_path = gen_path.clone();
        db_path.push("default.db");
        let mut op_db_path = gen_path.clone();
        op_db_path.push("gen.db");
        let mut vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vcf_path.push("fixtures/simple.vcf");
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");

        let conn = &mut get_connection(db_path.to_str());
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(op_db_path.to_str());
        setup_db(operation_conn, &db_uuid);
        let collection = "test".to_string();

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            None,
            false,
            conn,
            operation_conn,
        )
        .unwrap();

        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
            None,
        )
        .unwrap();

        let binding = BlockGroup::query(
            conn,
            "SELECT * FROM block_groups ORDER BY id DESC;",
            rusqlite::params!(),
        );
        let block_group = binding.first().unwrap();

        let remote_path = tempdir().unwrap().into_path();
        let remote_dir = remote_path.to_str().unwrap().to_string();
        let formatted_remote_dir = format!("file://{}", remote_dir);
        operation_conn
            .execute(
                "update defaults set remote_url=?1 where id = 1",
                (formatted_remote_dir,),
            )
            .unwrap();

        // Force sync everything in the db to disk before pushing the repo.  TRUNCATE is the most
        // aggressive option (full sync, then truncate the WAL file that had any unsynced changes,
        // to indicate nothing is left to sync).
        operation_conn
            .pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .unwrap();
        conn.pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .unwrap();

        let result = push(operation_conn, &db_uuid);
        assert!(result.is_ok());

        let mut remote_db_path = remote_path.clone();
        remote_db_path.push(".gen");
        remote_db_path.push("default.db");

        // Need to use get_real_connection here because get_connection is a test method and resets
        // the database if it exists
        let remote_conn = &mut get_real_connection(remote_db_path.to_str().unwrap());

        let all_local_sequences = BlockGroup::get_all_sequences(conn, block_group.id, false);
        let binding = BlockGroup::query(
            remote_conn,
            "SELECT * FROM block_groups ORDER BY id DESC;",
            rusqlite::params!(),
        );
        let block_group2 = binding.first().unwrap();
        assert_eq!(block_group2.id, block_group.id);
        let all_remote_sequences =
            BlockGroup::get_all_sequences(remote_conn, block_group.id, false);
        assert_eq!(all_remote_sequences, all_local_sequences);

        let mut remote_op_db_path = remote_path.clone();
        remote_op_db_path.push(".gen");
        remote_op_db_path.push("gen.db");

        // Need to use get_real_connection here because get_operation_connection is a test method
        // and resets the database if it exists
        let remote_operation_conn = &mut get_real_connection(remote_op_db_path.to_str().unwrap());

        let local_operation_hashes: HashSet<String> = HashSet::from_iter(
            Operation::query(
                operation_conn,
                "SELECT * FROM operation;",
                rusqlite::params!(),
            )
            .iter()
            .map(|op| op.hash.clone())
            .collect::<Vec<String>>(),
        );

        let remote_operation_hashes: HashSet<String> = HashSet::from_iter(
            Operation::query(
                remote_operation_conn,
                "SELECT * FROM operation;",
                rusqlite::params!(),
            )
            .iter()
            .map(|op| op.hash.clone())
            .collect::<Vec<String>>(),
        );

        assert_eq!(remote_operation_hashes, local_operation_hashes);
    }

    #[test]
    fn test_push_with_operation_files() {
        let gen_path = setup_gen_dir();
        let mut db_path = gen_path.clone();
        db_path.push("default.db");
        let mut op_db_path = gen_path.clone();
        op_db_path.push("gen.db");

        let main_repo_path = gen_path.parent().unwrap().to_path_buf();
        let fixtures_path = main_repo_path.clone().join("fixtures");
        fs::create_dir(fixtures_path.clone()).unwrap();

        let mut original_vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let vcf_relative_path = "fixtures/simple.vcf";
        original_vcf_path.push(vcf_relative_path);
        let vcf_path = fixtures_path.clone().join("simple.vcf");
        fs::copy(original_vcf_path, &vcf_path).unwrap();

        let mut original_fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let fasta_relative_path = "fixtures/simple.fa";
        original_fasta_path.push(fasta_relative_path);
        let fasta_path = fixtures_path.clone().join("simple.fa");
        fs::copy(original_fasta_path, &fasta_path).unwrap();

        let conn = &mut get_connection(db_path.to_str());
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(op_db_path.to_str());
        setup_db(operation_conn, &db_uuid);
        let collection = "test".to_string();

        import_fasta(
            &fasta_relative_path.to_string(),
            &collection,
            None,
            false,
            conn,
            operation_conn,
        )
        .unwrap();

        update_with_vcf(
            &vcf_relative_path.to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
            None,
        )
        .unwrap();

        let binding = BlockGroup::query(
            conn,
            "SELECT * FROM block_groups ORDER BY id DESC;",
            rusqlite::params!(),
        );
        let block_group = binding.first().unwrap();

        let remote_path = tempdir().unwrap().into_path();
        let remote_dir = remote_path.to_str().unwrap().to_string();
        let formatted_remote_dir = format!("file://{}", remote_dir);
        operation_conn
            .execute(
                "update defaults set remote_url=?1 where id = 1",
                (formatted_remote_dir,),
            )
            .unwrap();

        // Force sync everything in the db to disk before pushing the repo.  TRUNCATE is the most
        // aggressive option (full sync, then truncate the WAL file that had any unsynced changes,
        // to indicate nothing is left to sync).
        operation_conn
            .pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .unwrap();
        conn.pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .unwrap();

        let result = push(operation_conn, &db_uuid);
        assert!(result.is_ok());

        let mut remote_db_path = remote_path.clone();
        remote_db_path.push(".gen");
        remote_db_path.push("default.db");

        // Need to use get_real_connection here because get_connection is a test method and resets
        // the database if it exists
        let remote_conn = &mut get_real_connection(remote_db_path.to_str().unwrap());

        let all_local_sequences = BlockGroup::get_all_sequences(conn, block_group.id, false);
        let binding = BlockGroup::query(
            remote_conn,
            "SELECT * FROM block_groups ORDER BY id DESC;",
            rusqlite::params!(),
        );
        let block_group2 = binding.first().unwrap();
        assert_eq!(block_group2.id, block_group.id);
        let all_remote_sequences =
            BlockGroup::get_all_sequences(remote_conn, block_group.id, false);
        assert_eq!(all_remote_sequences, all_local_sequences);

        let remote_path = remote_path.clone().join("fixtures");
        let remote_fasta_path = remote_path
            .clone()
            .join("simple.fa")
            .to_str()
            .unwrap()
            .to_string();
        let remote_vcf_path = remote_path
            .clone()
            .join("simple.vcf")
            .to_str()
            .unwrap()
            .to_string();
        assert!(fs::exists(remote_fasta_path).unwrap());
        assert!(fs::exists(remote_vcf_path).unwrap());
    }

    #[test]
    fn test_push_with_remote_repo_ahead() {
        let gen_path = setup_gen_dir();
        let mut db_path = gen_path.clone();
        db_path.push("default.db");
        let mut op_db_path = gen_path.clone();
        op_db_path.push("gen.db");

        let main_repo_path = gen_path.parent().unwrap().to_path_buf();
        let fixtures_path = main_repo_path.clone().join("fixtures");
        fs::create_dir(fixtures_path.clone()).unwrap();

        let mut original_fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let fasta_relative_path = "fixtures/simple.fa";
        original_fasta_path.push(fasta_relative_path);
        let fasta_path = fixtures_path.clone().join("simple.fa");
        fs::copy(original_fasta_path, &fasta_path).unwrap();

        let conn = &mut get_connection(db_path.to_str());
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(op_db_path.to_str());
        setup_db(operation_conn, &db_uuid);
        let collection = "test".to_string();

        import_fasta(
            &fasta_relative_path.to_string(),
            &collection,
            None,
            false,
            conn,
            operation_conn,
        )
        .unwrap();

        let remote_path = tempdir().unwrap().into_path();
        let remote_dir = remote_path.to_str().unwrap().to_string();
        let formatted_remote_dir = format!("file://{}", remote_dir);
        operation_conn
            .execute(
                "update defaults set remote_url=?1 where id = 1",
                (formatted_remote_dir,),
            )
            .unwrap();

        // Force sync everything in the db to disk before pushing the repo.  TRUNCATE is the most
        // aggressive option (full sync, then truncate the WAL file that had any unsynced changes,
        // to indicate nothing is left to sync).
        operation_conn
            .pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .unwrap();
        conn.pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .unwrap();

        let result = push(operation_conn, &db_uuid);
        assert!(result.is_ok());

        let mut remote_db_path = remote_path.clone();
        remote_db_path.push(".gen");
        remote_db_path.push("default.db");

        // Need to use get_real_connection here because get_connection is a test method and resets
        // the database if it exists
        let remote_conn = &mut get_real_connection(remote_db_path.to_str().unwrap());

        let mut remote_op_db_path = remote_path.clone();
        remote_op_db_path.push(".gen");
        remote_op_db_path.push("gen.db");

        // Need to use get_real_connection here because get_operation_connection is a test method
        // and resets the database if it exists
        let remote_operation_conn = &mut get_real_connection(remote_op_db_path.to_str().unwrap());

        let mut original_vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let vcf_relative_path = "fixtures/simple.vcf";
        original_vcf_path.push(vcf_relative_path);
        let vcf_path = remote_path.clone().join("fixtures/simple.vcf");
        fs::copy(original_vcf_path, &vcf_path).unwrap();

        update_with_vcf(
            &vcf_relative_path.to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            remote_conn,
            remote_operation_conn,
            None,
        )
        .unwrap();

        // Force sync everything in the db to disk before pushing the repo.  TRUNCATE is the most
        // aggressive option (full sync, then truncate the WAL file that had any unsynced changes,
        // to indicate nothing is left to sync).
        remote_operation_conn
            .pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .unwrap();
        remote_conn
            .pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .unwrap();

        let result = push(operation_conn, &db_uuid).unwrap_err();
        assert!(matches!(result, RemoteOperationError::RemoteBranchAhead));
    }
}
