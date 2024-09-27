use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::{fs, path::PathBuf, str};

use fallible_streaming_iterator::FallibleStreamingIterator;
use itertools::Itertools;
use petgraph::Direction;
use rusqlite::session::ChangesetIter;
use rusqlite::types::{FromSql, Value};
use rusqlite::{session, Connection};
use serde::{Deserialize, Serialize};

use crate::config::get_changeset_path;
use crate::models::block_group::BlockGroup;
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::collection::Collection;
use crate::models::edge::{Edge, EdgeData};
use crate::models::file_types::FileTypes;
use crate::models::node::Node;
use crate::models::operations::{
    Branch, FileAddition, Operation, OperationState, OperationSummary,
};
use crate::models::path::Path;
use crate::models::sample::Sample;
use crate::models::sequence::{NewSequence, Sequence};
use crate::models::strand::Strand;

/* General information

Changesets from sqlite will be created in the order that operations are applied in the database,
so given our foreign key setup, we would not expect out of order table/row creation. i.e. block
groups will always appear before block group edges, etc.

 */

pub enum FileMode {
    Read,
    Write,
}

#[derive(Deserialize, Serialize, Debug)]
struct DependencyModels {
    sequences: Vec<Sequence>,
    block_group: Vec<BlockGroup>,
    nodes: Vec<Node>,
    edges: Vec<Edge>,
    paths: Vec<Path>,
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

pub fn get_changeset_dependencies(conn: &Connection, changes: &[u8]) -> Vec<u8> {
    let input: &mut dyn Read = &mut changes.clone();
    let mut iter = ChangesetIter::start_strm(&input).unwrap();
    // the purpose of this function is to capture external changes to the changeset, notably foreign keys
    // that may be made in previous changesets.
    let mut previous_block_groups = HashSet::new();
    let mut previous_edges = HashSet::new();
    let mut previous_paths = HashSet::new();
    let mut previous_sequences = HashSet::new();
    let mut created_block_groups = HashSet::new();
    let mut created_paths = HashSet::new();
    let mut created_edges = HashSet::new();
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
                "sequence" => {
                    let hash =
                        str::from_utf8(item.new_value(pk_column).unwrap().as_bytes().unwrap())
                            .unwrap();
                    created_sequences.insert(hash.to_string());
                }
                "block_group" => {
                    let bg_pk = item.new_value(pk_column).unwrap().as_i64().unwrap() as i32;
                    created_block_groups.insert(bg_pk);
                }
                "path" => {
                    created_paths
                        .insert(item.new_value(pk_column).unwrap().as_i64().unwrap() as i32);
                    let bg_id = item.new_value(1).unwrap().as_i64().unwrap() as i32;
                    if !created_block_groups.contains(&bg_id) {
                        previous_block_groups.insert(bg_id);
                    }
                }
                "nodes" => {
                    created_nodes
                        .insert(item.new_value(pk_column).unwrap().as_i64().unwrap() as i32);
                    let sequence_hash =
                        str::from_utf8(item.new_value(1).unwrap().as_bytes().unwrap())
                            .unwrap()
                            .to_string();
                    if !created_sequences.contains(&sequence_hash) {
                        previous_sequences.insert(sequence_hash);
                    }
                }
                "edges" => {
                    let edge_pk = item.new_value(pk_column).unwrap().as_i64().unwrap() as i32;
                    let source_node_id = item.new_value(1).unwrap().as_i64().unwrap() as i32;
                    let target_node_id = item.new_value(4).unwrap().as_i64().unwrap() as i32;
                    created_edges.insert(edge_pk);
                    let nodes = Node::get_nodes(conn, vec![source_node_id, target_node_id]);
                    if !created_nodes.contains(&source_node_id) {
                        previous_sequences.insert(nodes[0].sequence_hash.clone());
                    }
                    if !created_nodes.contains(&target_node_id) {
                        previous_sequences.insert(nodes[1].sequence_hash.clone());
                    }
                }
                "path_edges" => {
                    let path_id = item.new_value(1).unwrap().as_i64().unwrap() as i32;
                    let edge_id = item.new_value(3).unwrap().as_i64().unwrap() as i32;
                    if !created_paths.contains(&path_id) {
                        previous_paths.insert(path_id);
                    }
                    if !created_edges.contains(&edge_id) {
                        previous_edges.insert(edge_id);
                    }
                }
                "block_group_edges" => {
                    // make sure blockgroup_map has blockgroups for bg ids made in external changes.
                    let bg_id = item.new_value(1).unwrap().as_i64().unwrap() as i32;
                    let edge_id = item.new_value(2).unwrap().as_i64().unwrap() as i32;
                    if !created_edges.contains(&edge_id) {
                        previous_edges.insert(edge_id);
                    }
                    if !created_block_groups.contains(&bg_id) {
                        previous_block_groups.insert(bg_id);
                    }
                }
                _ => {}
            }
        }
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
                "select * from block_group where id in ({ids})",
                ids = previous_block_groups.iter().join(",")
            ),
            vec![],
        ),
        nodes: vec![],
        edges: Edge::query(
            conn,
            &format!(
                "select * from edges where id in ({ids})",
                ids = previous_edges.iter().join(",")
            ),
            vec![],
        ),
        paths: Path::get_paths(
            conn,
            &format!(
                "select * from path where id in ({ids})",
                ids = previous_paths.iter().join(",")
            ),
            vec![],
        ),
    };
    serde_json::to_vec(&s).unwrap()
}

pub fn write_changeset(conn: &Connection, operation: &Operation, changes: &[u8]) {
    let change_path =
        get_changeset_path(operation).join(format!("{op_id}.cs", op_id = operation.id));
    let dependency_path =
        get_changeset_path(operation).join(format!("{op_id}.dep", op_id = operation.id));

    let mut dependency_file = fs::File::create_new(&dependency_path)
        .unwrap_or_else(|_| panic!("Unable to open {dependency_path:?}"));
    dependency_file
        .write_all(&get_changeset_dependencies(conn, changes))
        .unwrap();

    let mut file = fs::File::create_new(&change_path)
        .unwrap_or_else(|_| panic!("Unable to open {change_path:?}"));

    file.write_all(changes).unwrap()
}

pub fn apply_changeset(conn: &Connection, operation: &Operation) {
    let dependency_path =
        get_changeset_path(operation).join(format!("{op_id}.dep", op_id = operation.id));
    let dependencies: DependencyModels =
        serde_json::from_reader(fs::File::open(dependency_path).unwrap()).unwrap();

    for sequence in dependencies.sequences.iter() {
        if !Sequence::is_delimiter_hash(&sequence.hash) {
            let new_seq = NewSequence::from(sequence).save(conn);
            assert_eq!(new_seq.hash, sequence.hash);
        }
    }

    let mut dep_bg_map = HashMap::new();
    for bg in dependencies.block_group.iter() {
        let sample_name = bg.sample_name.as_ref().map(|v| v as &str);
        let new_bg = BlockGroup::create(conn, &bg.collection_name, sample_name, &bg.name);
        dep_bg_map.insert(&bg.id, new_bg.id);
    }

    let mut dep_node_map = HashMap::new();
    for node in dependencies.nodes.iter() {
        let new_node_id = Node::create(conn, &node.sequence_hash.clone());
        dep_node_map.insert(&node.id, new_node_id);
    }

    let mut dep_edge_map = HashMap::new();
    let new_edges = Edge::bulk_create(
        conn,
        dependencies.edges.iter().map(EdgeData::from).collect(),
    );
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

    let change_path =
        get_changeset_path(operation).join(format!("{op_id}.cs", op_id = operation.id));
    let mut file = fs::File::open(change_path).unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).unwrap();
    conn.pragma_update(None, "foreign_keys", "0").unwrap();

    let input: &mut dyn Read = &mut contents.as_slice();
    let mut iter = ChangesetIter::start_strm(&input).unwrap();

    let mut blockgroup_map: HashMap<i32, i32> = HashMap::new();
    let mut edge_map: HashMap<i32, EdgeData> = HashMap::new();
    let mut node_map: HashMap<i32, String> = HashMap::new();
    let mut path_edges: HashMap<i32, Vec<(i32, i32)>> = HashMap::new();
    let mut insert_paths = vec![];
    let mut insert_block_group_edges = vec![];

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
                "sample" => {
                    Sample::create(
                        conn,
                        str::from_utf8(item.new_value(pk_column).unwrap().as_bytes().unwrap())
                            .unwrap(),
                    );
                }
                "sequence" => {
                    Sequence::new()
                        .sequence_type(
                            str::from_utf8(item.new_value(1).unwrap().as_bytes().unwrap()).unwrap(),
                        )
                        .sequence(
                            str::from_utf8(item.new_value(2).unwrap().as_bytes().unwrap()).unwrap(),
                        )
                        .name(
                            str::from_utf8(item.new_value(3).unwrap().as_bytes().unwrap()).unwrap(),
                        )
                        .file_path(
                            str::from_utf8(item.new_value(4).unwrap().as_bytes().unwrap()).unwrap(),
                        )
                        .length(item.new_value(5).unwrap().as_i64().unwrap() as i32)
                        .save(conn);
                }
                "block_group" => {
                    let bg_pk = item.new_value(pk_column).unwrap().as_i64().unwrap() as i32;
                    if let Some(v) = dep_bg_map.get(&bg_pk) {
                        blockgroup_map.insert(bg_pk, *v);
                    } else {
                        let sample_name: Option<&str> =
                            match item.new_value(2).unwrap().as_bytes_or_null().unwrap() {
                                Some(v) => Some(str::from_utf8(v).unwrap()),
                                None => None,
                            };
                        let new_bg = BlockGroup::create(
                            conn,
                            str::from_utf8(item.new_value(1).unwrap().as_bytes().unwrap()).unwrap(),
                            sample_name,
                            str::from_utf8(item.new_value(3).unwrap().as_bytes().unwrap()).unwrap(),
                        );
                        blockgroup_map.insert(bg_pk, new_bg.id);
                    };
                }
                "path" => {
                    // defer path creation until edges are made
                    insert_paths.push(Path {
                        id: item.new_value(pk_column).unwrap().as_i64().unwrap() as i32,
                        block_group_id: item.new_value(1).unwrap().as_i64().unwrap() as i32,
                        name: str::from_utf8(item.new_value(2).unwrap().as_bytes().unwrap())
                            .unwrap()
                            .to_string(),
                    });
                }
                "nodes" => {
                    let node_pk = item.new_value(pk_column).unwrap().as_i64().unwrap() as i32;
                    node_map.insert(
                        node_pk,
                        str::from_utf8(item.new_value(1).unwrap().as_bytes().unwrap())
                            .unwrap()
                            .to_string(),
                    );
                }
                "edges" => {
                    let edge_pk = item.new_value(pk_column).unwrap().as_i64().unwrap() as i32;
                    edge_map.insert(
                        edge_pk,
                        EdgeData {
                            source_node_id: item.new_value(1).unwrap().as_i64().unwrap() as i32,
                            source_coordinate: item.new_value(2).unwrap().as_i64().unwrap() as i32,
                            source_strand: Strand::column_result(item.new_value(3).unwrap())
                                .unwrap(),
                            target_node_id: item.new_value(4).unwrap().as_i64().unwrap() as i32,
                            target_coordinate: item.new_value(5).unwrap().as_i64().unwrap() as i32,
                            target_strand: Strand::column_result(item.new_value(6).unwrap())
                                .unwrap(),
                            chromosome_index: item.new_value(7).unwrap().as_i64().unwrap() as i32,
                            phased: item.new_value(8).unwrap().as_i64().unwrap() as i32,
                        },
                    );
                }
                "path_edges" => {
                    let path_id = item.new_value(1).unwrap().as_i64().unwrap() as i32;
                    let path_index = item.new_value(2).unwrap().as_i64().unwrap() as i32;
                    // the edge_id here may not be valid and in this database may have a different pk
                    let edge_id = item.new_value(3).unwrap().as_i64().unwrap() as i32;
                    path_edges
                        .entry(path_id)
                        .or_default()
                        .push((path_index, edge_id));
                }
                "block_group_edges" => {
                    // make sure blockgroup_map has blockgroups for bg ids made in external changes.
                    let bg_id = item.new_value(1).unwrap().as_i64().unwrap() as i32;
                    let edge_id = item.new_value(2).unwrap().as_i64().unwrap() as i32;
                    insert_block_group_edges.push((bg_id, edge_id));
                }
                "collection" => {
                    Collection::create(
                        conn,
                        str::from_utf8(item.new_value(pk_column).unwrap().as_bytes().unwrap())
                            .unwrap(),
                    );
                }
                _ => {
                    panic!("unhandled table is {v}", v = op.table_name());
                }
            }
        }
    }

    let mut node_id_map: HashMap<i32, i32> = HashMap::new();
    for (node_id, sequence_hash) in node_map {
        let new_node_id = Node::create(conn, &sequence_hash);
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
                chromosome_index: edge.chromosome_index,
                phased: edge.phased,
            },
        );
    }

    let sorted_edge_ids = updated_edge_map
        .keys()
        .copied()
        .sorted()
        .collect::<Vec<i32>>();
    let created_edges = Edge::bulk_create(
        conn,
        sorted_edge_ids
            .iter()
            .map(|id| updated_edge_map[id].clone())
            .collect::<Vec<EdgeData>>(),
    );
    let mut edge_id_map: HashMap<i32, i32> = HashMap::new();
    for (index, edge_id) in created_edges.iter().enumerate() {
        edge_id_map.insert(sorted_edge_ids[index], *edge_id);
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
        Path::create(conn, &path.name, path.block_group_id, &sorted_edges);
    }

    let mut block_group_edges: HashMap<i32, Vec<i32>> = HashMap::new();

    for (bg_id, edge_id) in insert_block_group_edges {
        let bg_id = *dep_bg_map
            .get(&bg_id)
            .or(blockgroup_map.get(&bg_id).or(Some(&bg_id)))
            .unwrap();
        let edge_id = dep_edge_map
            .get(&edge_id)
            .or(edge_id_map.get(&edge_id).or(Some(&edge_id)))
            .unwrap();
        block_group_edges.entry(bg_id).or_default().push(*edge_id);
    }
    for (bg_id, edges) in block_group_edges.iter() {
        BlockGroupEdge::bulk_create(conn, *bg_id, edges);
    }

    conn.pragma_update(None, "foreign_keys", "1").unwrap();
}

pub fn revert_changeset(conn: &Connection, operation: &Operation) {
    let change_path =
        get_changeset_path(operation).join(format!("{op_id}.cs", op_id = operation.id));
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

pub fn reset(conn: &Connection, operation_conn: &Connection, db_uuid: &str, op_id: i32) {
    let current_op = OperationState::get_operation(operation_conn, db_uuid).unwrap();
    let current_branch_id = OperationState::get_current_branch(operation_conn, db_uuid).unwrap();
    let current_branch = Branch::get_by_id(operation_conn, current_branch_id).unwrap();
    let branch_operations: Vec<i32> = Branch::get_operations(operation_conn, current_branch_id)
        .iter()
        .map(|b| b.id)
        .collect();
    if !branch_operations.contains(&current_op) {
        panic!("{op_id} is not contained in this branch's operations.");
    }
    move_to(
        conn,
        operation_conn,
        &Operation::get_by_id(operation_conn, op_id),
    );

    if current_branch.name != "main" {
        operation_conn.execute(
            "UPDATE branch SET start_operation_id = ?2 WHERE id = ?1",
            (current_branch_id, op_id),
        );
    }

    // hide all child operations from this point
    for op in Operation::query(
        operation_conn,
        "select * from operation where parent_id = ?1",
        vec![Value::from(op_id)],
    )
    .iter()
    {
        Branch::mask_operation(operation_conn, current_branch_id, op.id);
    }
    OperationState::set_operation(operation_conn, db_uuid, op_id);
}

pub fn apply(conn: &Connection, operation_conn: &Connection, db_uuid: &str, op_id: i32) {
    let mut session = session::Session::new(conn).unwrap();
    attach_session(&mut session);
    let change = FileAddition::create(operation_conn, &format!("{op_id}.cs"), FileTypes::Changeset);
    apply_changeset(conn, &Operation::get_by_id(operation_conn, op_id));
    let operation = Operation::create(
        operation_conn,
        db_uuid,
        None,
        "changeset_application",
        change.id,
    );

    OperationSummary::create(
        operation_conn,
        operation.id,
        &format!("Applied changeset {op_id}."),
    );
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    write_changeset(conn, &operation, &output);
}

pub fn move_to(conn: &Connection, operation_conn: &Connection, operation: &Operation) {
    let current_op_id = OperationState::get_operation(operation_conn, &operation.db_uuid).unwrap();
    let op_id = operation.id;
    if current_op_id == op_id {
        return;
    }
    let path = Operation::get_path_between(operation_conn, current_op_id, op_id);
    if path.is_empty() {
        println!("No path exists from {current_op_id} to {op_id}.");
        return;
    }
    for (operation_id, direction, next_op) in path.iter() {
        match direction {
            Direction::Incoming => {
                println!("Reverting operation {operation_id}");
                revert_changeset(conn, &Operation::get_by_id(operation_conn, *operation_id));
                OperationState::set_operation(operation_conn, &operation.db_uuid, *next_op);
            }
            Direction::Outgoing => {
                println!("Applying operation {next_op}");
                apply_changeset(conn, &Operation::get_by_id(operation_conn, *next_op));
                OperationState::set_operation(operation_conn, &operation.db_uuid, *next_op);
            }
        }
    }
}

pub fn attach_session(session: &mut session::Session) {
    for table in [
        "collection",
        "sample",
        "sequence",
        "block_group",
        "path",
        "nodes",
        "edges",
        "path_edges",
        "block_group_edges",
    ] {
        session.attach(Some(table)).unwrap();
    }
}

pub fn checkout(
    conn: &Connection,
    operation_conn: &Connection,
    db_uuid: &str,
    branch_name: &Option<String>,
    operation_id: Option<i32>,
) {
    let mut dest_op_id = operation_id.unwrap_or(0);
    if let Some(name) = branch_name {
        let current_branch = OperationState::get_current_branch(operation_conn, db_uuid)
            .expect("No current branch set");
        let branch = Branch::get_by_name(operation_conn, db_uuid, name)
            .unwrap_or_else(|| panic!("No branch named {name}"));
        if current_branch != branch.id {
            OperationState::set_branch(operation_conn, db_uuid, name);
        }
        if dest_op_id == 0 {
            dest_op_id = branch.current_operation_id.unwrap();
        }
    }
    if dest_op_id == 0 {
        panic!("No operation defined.");
    }
    move_to(
        conn,
        operation_conn,
        &Operation::get_by_id(operation_conn, dest_op_id),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::models::file_types::FileTypes;
    use crate::models::operations::{setup_db, Branch, FileAddition, Operation, OperationState};
    use crate::models::{edge::Edge, metadata, node::Node, sample::Sample};
    use crate::test_helpers::{
        get_connection, get_operation_connection, setup_block_group, setup_gen_dir,
    };
    use crate::updates::vcf::update_with_vcf;
    use rusqlite::{session::Session, types::Value};
    use std::path::{Path, PathBuf};

    #[test]
    fn test_writes_operation_id() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);
        let change = FileAddition::create(op_conn, "test", FileTypes::Fasta);
        let operation = Operation::create(op_conn, &db_uuid, "test".to_string(), "test", change.id);
        OperationState::set_operation(op_conn, &db_uuid, operation.id);
        assert_eq!(OperationState::get_operation(op_conn, &db_uuid).unwrap(), 1);
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
            "select * from block_group where id = ?1;",
            vec![Value::from(bg_id)],
        );
        let dep_bg = binding.first().unwrap();

        let existing_seq = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAATTTT")
            .save(conn);
        let existing_node_id = Node::create(conn, existing_seq.hash.as_str());

        let mut session = Session::new(conn).unwrap();
        attach_session(&mut session);

        let random_seq = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCG")
            .save(conn);
        let random_node_id = Node::create(conn, random_seq.hash.as_str());

        let new_edge = Edge::create(
            conn,
            random_node_id,
            0,
            Strand::Forward,
            existing_node_id,
            0,
            Strand::Forward,
            0,
            0,
        );
        BlockGroupEdge::bulk_create(conn, bg_id, &[new_edge.id]);
        let mut output = Vec::new();
        session.changeset_strm(&mut output).unwrap();
        let change = FileAddition::create(op_conn, "test", FileTypes::Fasta);
        let operation = Operation::create(op_conn, &db_uuid, "test".to_string(), "test", change.id);
        write_changeset(conn, &operation, &output);

        let dependency_path =
            get_changeset_path(&operation).join(format!("{op_id}.dep", op_id = operation.id));
        let dependencies: DependencyModels =
            serde_json::from_reader(fs::File::open(dependency_path).unwrap()).unwrap();
        assert_eq!(dependencies.sequences.len(), 1);
        assert_eq!(
            dependencies.block_group[0].collection_name,
            dep_bg.collection_name
        );
        assert_eq!(dependencies.block_group[0].name, dep_bg.name);
        assert_eq!(dependencies.block_group[0].sample_name, dep_bg.sample_name);
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
            false,
            conn,
            operation_conn,
        );
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let node_count = Node::query(conn, "select * from nodes", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 2);
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
        );
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let node_count = Node::query(conn, "select * from nodes", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        // NOTE: The edge count is 14 because of the following:
        // * 1 edge from the source node to the node created by the fasta import
        // * 1 edge from the node created by the fasta import to the sink node
        // * 8 edges to and from nodes representing the first alt sequence.  Topologically there are
        // just 2 edges, but there is redundancy because of phasing.  There is further redundancy
        // because there are 2 non-reference samples, causing 2 nodes to be created for each alt
        // sequence.
        // * 4 edges to and from nodes representing the second alt sequence.  (One sample uses the
        // reference part instead of the alt sequence in this case.)
        assert_eq!(edge_count, 14);
        // NOTE: The node count is 9:
        // * 2 source and sink nodes
        // * 1 node created by the initial fasta import
        // * 6 nodes created by the VCF update.  See above explanation of edge count for more details.
        assert_eq!(node_count, 9);
        assert_eq!(sample_count, 3);
        assert_eq!(op_count, 2);

        // revert back to state 1 where vcf samples and blockpaths do not exist
        revert_changeset(
            conn,
            &Operation::get_by_id(
                operation_conn,
                OperationState::get_operation(operation_conn, &db_uuid).unwrap(),
            ),
        );

        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let node_count = Node::query(conn, "select * from nodes", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 2);
        assert_eq!(node_count, 3);
        assert_eq!(sample_count, 0);
        assert_eq!(op_count, 2);

        apply_changeset(
            conn,
            &Operation::get_by_id(
                operation_conn,
                OperationState::get_operation(operation_conn, &db_uuid).unwrap(),
            ),
        );
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let node_count = Node::query(conn, "select * from nodes", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 14);
        assert_eq!(node_count, 9);
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

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            operation_conn,
        );

        Branch::create(operation_conn, &db_uuid, "branch-1");
        Branch::create(operation_conn, &db_uuid, "branch-2");
        checkout(
            conn,
            operation_conn,
            &db_uuid,
            &Some("branch-1".to_string()),
            None,
        );

        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );

        let foo_bg_id = BlockGroup::get_id(conn, &collection, Some("foo"), "m123");
        let patch_1_seqs = HashSet::from_iter(vec![
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATCATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
        ]);

        assert_eq!(BlockGroup::get_all_sequences(conn, foo_bg_id), patch_1_seqs);
        assert_eq!(
            BlockGroup::query(conn, "select * from block_group;", vec![])
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
        update_with_vcf(
            &vcf2_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );

        let foo_bg_id = BlockGroup::get_id(conn, &collection, Some("foo"), "m123");
        let patch_2_seqs = HashSet::from_iter(vec![
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATCGATCGATCGACGATCGGGAACACACAGAGA".to_string(),
        ]);
        assert_eq!(BlockGroup::get_all_sequences(conn, foo_bg_id), patch_2_seqs);
        assert_ne!(patch_1_seqs, patch_2_seqs);
        assert_eq!(
            BlockGroup::query(conn, "select * from block_group;", vec![])
                .iter()
                .map(|v| v.sample_name.clone().unwrap_or("".to_string()))
                .collect::<Vec<String>>(),
            vec!["".to_string(), "foo".to_string()]
        );

        // apply changes from branch-1, it will be operation id 2
        apply(conn, operation_conn, &db_uuid, 2);

        let foo_bg_id = BlockGroup::get_id(conn, &collection, Some("foo"), "m123");
        let patch_2_seqs = HashSet::from_iter(vec![
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATCGATCGATCGACGATCGGGAACACACAGAGA".to_string(),
            "ATCATCGATCGACGATCGGGAACACACAGAGA".to_string(),
            "ATCATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
        ]);
        assert_eq!(BlockGroup::get_all_sequences(conn, foo_bg_id), patch_2_seqs);
        assert_eq!(
            BlockGroup::query(conn, "select * from block_group;", vec![])
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
        let unknown_seqs = HashSet::from_iter(vec![
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATCGATCGATAGAGATCGATCGGGAACACACAGAGA".to_string(),
            "ATCATCGATCGATCGATCGGGAACACACAGAGA".to_string(),
            "ATCATCGATAGAGATCGATCGGGAACACACAGAGA".to_string(),
        ]);
        assert_eq!(
            BlockGroup::get_all_sequences(conn, unknown_bg_id),
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
        let conn = &mut get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(None);
        setup_db(operation_conn, &db_uuid);
        let collection = "test".to_string();
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            operation_conn,
        );
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let node_count = Node::query(conn, "select * from nodes", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 2);
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
        );
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let node_count = Node::query(conn, "select * from nodes", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 14);
        assert_eq!(node_count, 9);
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
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let node_count = Node::query(conn, "select * from nodes", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 2);
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
        );
        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let node_count = Node::query(conn, "select * from nodes", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 6);
        assert_eq!(node_count, 5);
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

        let edge_count = Edge::query(conn, "select * from edges", vec![]).len() as i32;
        let node_count = Node::query(conn, "select * from nodes", vec![]).len() as i32;
        let sample_count = Sample::query(conn, "select * from sample", vec![]).len() as i32;
        let op_count =
            Operation::query(operation_conn, "select * from operation", vec![]).len() as i32;
        assert_eq!(edge_count, 14);
        assert_eq!(node_count, 9);
        assert_eq!(sample_count, 3);
        assert_eq!(op_count, 3);
    }

    #[test]
    fn test_reset_hides_operations() {
        setup_gen_dir();
        let fasta_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.fa");
        let vcf_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.vcf");
        let conn = &mut get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(None);
        setup_db(operation_conn, &db_uuid);
        let collection = "test".to_string();

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            operation_conn,
        );

        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );

        let branch_id = OperationState::get_current_branch(operation_conn, &db_uuid).unwrap();

        assert!(Branch::get_masked_operations(operation_conn, branch_id).is_empty());
        assert_eq!(
            Branch::get_operations(operation_conn, branch_id)
                .iter()
                .map(|op| op.id)
                .collect::<Vec<i32>>(),
            vec![1, 2, 3, 4, 5]
        );

        reset(conn, operation_conn, &db_uuid, 2);
        assert_eq!(
            Branch::get_masked_operations(operation_conn, branch_id),
            vec![3]
        );
        assert_eq!(
            Branch::get_operations(operation_conn, branch_id)
                .iter()
                .map(|op| op.id)
                .collect::<Vec<i32>>(),
            vec![1, 2]
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
        let fasta_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.fa");
        let vcf_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.vcf");
        let conn = &mut get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let operation_conn = &get_operation_connection(None);
        setup_db(operation_conn, &db_uuid);
        let collection = "test".to_string();

        let main_branch = Branch::get_by_name(operation_conn, &db_uuid, "main").unwrap();

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            operation_conn,
        );

        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );

        let branch_a = Branch::create(operation_conn, &db_uuid, "branch-a");
        OperationState::set_branch(operation_conn, &db_uuid, "branch-a");
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );
        OperationState::set_branch(operation_conn, &db_uuid, "main");
        OperationState::set_operation(operation_conn, &db_uuid, 2);
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );
        OperationState::set_branch(operation_conn, &db_uuid, "branch-a");
        OperationState::set_operation(operation_conn, &db_uuid, 5);
        let branch_b = Branch::create(operation_conn, &db_uuid, "branch-b");
        OperationState::set_branch(operation_conn, &db_uuid, "branch-b");
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );
        OperationState::set_branch(operation_conn, &db_uuid, "branch-a");
        OperationState::set_operation(operation_conn, &db_uuid, 5);
        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            operation_conn,
        );

        assert_eq!(
            Branch::get_operations(operation_conn, main_branch.id)
                .iter()
                .map(|op| op.id)
                .collect::<Vec<i32>>(),
            vec![1, 2, 6, 7, 8]
        );
        assert_eq!(
            Branch::get_operations(operation_conn, branch_a.id)
                .iter()
                .map(|op| op.id)
                .collect::<Vec<i32>>(),
            vec![1, 2, 3, 4, 5, 10]
        );
        assert_eq!(
            Branch::get_operations(operation_conn, branch_b.id)
                .iter()
                .map(|op| op.id)
                .collect::<Vec<i32>>(),
            vec![1, 2, 3, 4, 5, 9]
        );
        reset(conn, operation_conn, &db_uuid, 2);
        assert_eq!(
            Branch::get_masked_operations(operation_conn, branch_a.id),
            vec![3, 6]
        );
        assert_eq!(
            Branch::get_operations(operation_conn, main_branch.id)
                .iter()
                .map(|op| op.id)
                .collect::<Vec<i32>>(),
            vec![1, 2, 6, 7, 8]
        );
        assert_eq!(
            Branch::get_operations(operation_conn, branch_a.id)
                .iter()
                .map(|op| op.id)
                .collect::<Vec<i32>>(),
            vec![1, 2]
        );
        assert_eq!(
            Branch::get_operations(operation_conn, branch_b.id)
                .iter()
                .map(|op| op.id)
                .collect::<Vec<i32>>(),
            vec![1, 2, 3, 4, 5, 9]
        );
    }
}
