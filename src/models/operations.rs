use crate::graph::{all_simple_paths, OperationGraph};
use crate::models::file_types::FileTypes;
use crate::models::traits::*;
use petgraph::graphmap::UnGraphMap;
use petgraph::visit::{Dfs, Reversed};
use petgraph::Direction;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection, Result as SQLResult, Row};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::string::ToString;

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Operation {
    pub hash: String,
    pub db_uuid: String,
    pub parent_hash: Option<String>,
    pub branch_id: i64,
    pub change_type: String,
    pub change_id: i64,
}

impl Operation {
    pub fn create(
        conn: &Connection,
        db_uuid: &str,
        change_type: &str,
        change_id: i64,
        hash: &str,
    ) -> SQLResult<Operation> {
        let current_op = OperationState::get_operation(conn, db_uuid);
        let current_branch_id =
            OperationState::get_current_branch(conn, db_uuid).expect("No branch is checked out.");

        // if we are in the middle of a branch's operations, and not on a new branch's creation point
        // we cannot create a new operation as that would create a bifurcation in a branch's order
        // of operations. We ensure there is no child operation in this branch of the current operation.

        if let Some(op_hash) = current_op.clone() {
            let count: i64 = conn
                .query_row(
                    "select count(*) from operation where branch_id = ?1 AND parent_hash = ?2 AND hash not in (select operation_hash from branch_masked_operations where branch_id = ?1);",
                    (current_branch_id, op_hash),
                    |row| row.get(0),
                )
                .unwrap();
            if count != 0 {
                panic!("The current operation is in the middle of a branch. A new operation would create a bifurcation in the branch lineage. Create a new branch if you wish to bifurcate the current set of operations.");
            }
        }

        let query = "INSERT INTO operation (hash, db_uuid, change_type, change_id, parent_hash, branch_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6);";
        let mut stmt = conn.prepare(query).unwrap();
        stmt.execute(params_from_iter(vec![
            Value::from(hash.to_string()),
            Value::from(db_uuid.to_string()),
            Value::from(change_type.to_string()),
            Value::from(change_id),
            Value::from(current_op.clone()),
            Value::from(current_branch_id),
        ]))?;
        let operation = Operation {
            hash: hash.to_string(),
            db_uuid: db_uuid.to_string(),
            parent_hash: current_op.clone(),
            branch_id: current_branch_id,
            change_type: change_type.to_string(),
            change_id,
        };
        // TODO: error condition here where we can write to disk but transaction fails
        OperationState::set_operation(conn, &operation.db_uuid, &operation.hash);
        Branch::set_start_operation(conn, current_branch_id, &operation.hash);
        Ok(operation)
    }

    pub fn get_upstream(conn: &Connection, operation_hash: String) -> Vec<String> {
        let query = "WITH RECURSIVE operations(operation_hash, depth) AS ( \
        select ?1, 0 UNION \
        select parent_hash, depth + 1 from operation join operations ON hash=operation_hash \
        ) SELECT operation_hash, depth from operations where operation_hash is not null order by depth desc;";
        let mut stmt = conn.prepare(query).unwrap();
        stmt.query_map((operation_hash,), |row| row.get(0))
            .unwrap()
            .map(|id| id.unwrap())
            .collect::<Vec<String>>()
    }

    pub fn get_operation_graph(conn: &Connection) -> OperationGraph {
        let mut graph = OperationGraph::new();
        let operations = Operation::query(conn, "select * from operation;", rusqlite::params![]);
        for op in operations.iter() {
            graph.add_node(&op.hash);
            if let Some(v) = op.parent_hash.clone() {
                graph.add_node(&v);
                graph.add_edge(&v, &op.hash);
            }
        }
        graph
    }

    pub fn get_path_between(
        conn: &Connection,
        source_id: &str,
        target_id: &str,
    ) -> Vec<(String, Direction, String)> {
        let directed_graph = Operation::get_operation_graph(conn);
        let source_node = directed_graph.get_node(source_id);
        let target_node = directed_graph.get_node(target_id);
        let mut undirected_graph: UnGraphMap<usize, ()> = Default::default();

        for node in directed_graph.graph.nodes() {
            undirected_graph.add_node(node);
        }
        for (source, target, _weight) in directed_graph.graph.all_edges() {
            undirected_graph.add_edge(source, target, ());
        }
        let mut patch_path: Vec<(String, Direction, String)> = vec![];
        for path in all_simple_paths(&undirected_graph, source_node, target_node) {
            let mut last_node = source_node;
            for node in &path[1..] {
                if *node != source_node {
                    for (_edge_src, edge_target, _edge_weight) in directed_graph
                        .graph
                        .edges_directed(last_node, Direction::Outgoing)
                    {
                        if edge_target == *node {
                            patch_path.push((
                                directed_graph.get_key(last_node),
                                Direction::Outgoing,
                                directed_graph.get_key(*node),
                            ));
                            break;
                        }
                    }
                    for (edge_src, _edge_target, _edge_weight) in directed_graph
                        .graph
                        .edges_directed(last_node, Direction::Incoming)
                    {
                        if edge_src == *node {
                            patch_path.push((
                                directed_graph.get_key(last_node),
                                Direction::Incoming,
                                directed_graph.get_key(*node),
                            ));
                            break;
                        }
                    }
                }
                last_node = *node;
            }
        }
        patch_path
    }

    pub fn get(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Operation {
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(Self::process_row(row))
            })
            .unwrap();
        rows.next().unwrap().unwrap()
    }

    pub fn get_by_hash(conn: &Connection, op_hash: &str) -> Operation {
        Operation::get(
            conn,
            "select * from operation where hash = ?1",
            vec![Value::from(op_hash.to_string())],
        )
    }
}

impl Query for Operation {
    type Model = Operation;
    fn process_row(row: &Row) -> Self::Model {
        Operation {
            hash: row.get(0).unwrap(),
            db_uuid: row.get(1).unwrap(),
            parent_hash: row.get(2).unwrap(),
            branch_id: row.get(3).unwrap(),
            change_type: row.get(4).unwrap(),
            change_id: row.get(5).unwrap(),
        }
    }
}

pub struct FileAddition {
    pub id: i64,
    pub file_path: String,
    pub file_type: FileTypes,
}

impl FileAddition {
    pub fn create(conn: &Connection, file_path: &str, file_type: FileTypes) -> FileAddition {
        let query =
            "INSERT INTO file_addition (file_path, file_type) VALUES (?1, ?2) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map(
                params_from_iter(vec![
                    Value::from(file_path.to_string()),
                    Value::from(file_type),
                ]),
                |row| {
                    Ok(FileAddition {
                        id: row.get(0)?,
                        file_path: file_path.to_string(),
                        file_type,
                    })
                },
            )
            .unwrap();
        rows.next().unwrap().unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct OperationSummary {
    pub id: i64,
    pub operation_hash: String,
    pub summary: String,
}

impl OperationSummary {
    pub fn create(conn: &Connection, operation_hash: &str, summary: &str) -> OperationSummary {
        let query =
            "INSERT INTO operation_summary (operation_hash, summary) VALUES (?1, ?2) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let operation_hash = operation_hash.to_string();
        let mut rows = stmt
            .query_map(
                params_from_iter(vec![
                    Value::from(operation_hash.clone()),
                    Value::from(summary.to_string()),
                ]),
                |row| {
                    Ok(OperationSummary {
                        id: row.get(0)?,
                        operation_hash: operation_hash.clone(),
                        summary: summary.to_string(),
                    })
                },
            )
            .unwrap();
        rows.next().unwrap().unwrap()
    }

    pub fn query(
        conn: &Connection,
        query: &str,
        placeholders: Vec<Value>,
    ) -> Vec<OperationSummary> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(OperationSummary {
                    id: row.get(0)?,
                    operation_hash: row.get(1)?,
                    summary: row.get(2)?,
                })
            })
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }
}

#[derive(Clone, Debug)]
pub struct Branch {
    pub id: i64,
    pub db_uuid: String,
    pub name: String,
    pub start_operation_hash: Option<String>,
    pub current_operation_hash: Option<String>,
}

impl Branch {
    pub fn create(conn: &Connection, db_uuid: &str, branch_name: &str) -> Branch {
        let current_operation_hash = OperationState::get_operation(conn, db_uuid);
        let mut stmt = conn.prepare_cached("insert into branch (db_uuid, name, start_operation_hash, current_operation_hash) values (?1, ?2, ?3, ?3) returning (id);").unwrap();

        let mut rows = stmt
            .query_map(
                (db_uuid, branch_name, current_operation_hash.clone()),
                |row| {
                    Ok(Branch {
                        id: row.get(0)?,
                        db_uuid: db_uuid.to_string(),
                        name: branch_name.to_string(),
                        start_operation_hash: current_operation_hash.clone(),
                        current_operation_hash: current_operation_hash.clone(),
                    })
                },
            )
            .unwrap();
        match rows.next().unwrap() {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    panic!("Branch already exists");
                } else {
                    panic!("something bad happened querying the database {err:?} {details:?}");
                }
            }
            Err(_) => {
                panic!("something bad happened querying the database");
            }
        }
    }

    pub fn delete(conn: &Connection, db_uuid: &str, branch_name: &str) {
        if branch_name == "main" {
            panic!("Main branch cannot be deleted");
        }
        if let Some(branch) = Branch::get_by_name(conn, db_uuid, branch_name) {
            let branch_id = branch.id;
            if let Some(current_branch) = OperationState::get_current_branch(conn, db_uuid) {
                if current_branch == branch_id {
                    panic!("Unable to delete the branch that is currently active.");
                }
            }
            conn.execute("delete from branch where id = ?1", (branch_id,))
                .expect("Error deleting from branch table.");
        } else {
            panic!("No branch named {branch_name} in database.");
        }
    }

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<Branch> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(Branch {
                    id: row.get(0)?,
                    db_uuid: row.get(1)?,
                    name: row.get(2)?,
                    start_operation_hash: row.get(3)?,
                    current_operation_hash: row.get(4)?,
                })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }

    pub fn get_by_name(conn: &Connection, db_uuid: &str, branch_name: &str) -> Option<Branch> {
        let mut branch: Option<Branch> = None;
        for result in Branch::query(
            conn,
            "select * from branch where db_uuid = ?1 and name = ?2",
            vec![
                Value::from(db_uuid.to_string()),
                Value::from(branch_name.to_string()),
            ],
        )
        .iter()
        {
            branch = Some(result.clone());
        }
        branch
    }

    pub fn get_by_id(conn: &Connection, branch_id: i64) -> Option<Branch> {
        let mut branch: Option<Branch> = None;
        for result in Branch::query(
            conn,
            "select * from branch where id = ?1",
            vec![Value::from(branch_id)],
        )
        .iter()
        {
            branch = Some(result.clone());
        }
        branch
    }

    pub fn set_current_operation(conn: &Connection, branch_id: i64, operation_hash: &str) {
        conn.execute(
            "UPDATE branch set current_operation_hash = ?2 where id = ?1",
            (branch_id, operation_hash.to_string()),
        )
        .unwrap();
    }

    pub fn set_start_operation(conn: &Connection, branch_id: i64, operation_hash: &str) {
        conn.execute(
            "UPDATE branch set start_operation_hash = ?2 where id = ?1 and start_operation_hash is null",
            (branch_id, operation_hash.to_string()),
        )
        .unwrap();
    }

    pub fn get_operations(conn: &Connection, branch_id: i64) -> Vec<Operation> {
        let branch = Branch::get_by_id(conn, branch_id)
            .unwrap_or_else(|| panic!("No branch with id {branch_id}."));
        let mut graph = Operation::get_operation_graph(conn);
        let mut operations: Vec<Operation> = vec![];
        let masked_operations = Branch::get_masked_operations(conn, branch_id);
        for op in masked_operations.iter() {
            graph.remove_node(op);
        }

        if let Some(creation_hash) = branch.start_operation_hash {
            let rev_graph = Reversed(&graph.graph);
            let creation_node = graph.get_node(&creation_hash);
            let mut dfs = Dfs::new(rev_graph, creation_node);

            while let Some(ancestor) = dfs.next(rev_graph) {
                let ancestor_node = graph.get_key(ancestor);
                operations.insert(0, Operation::get_by_hash(conn, &ancestor_node));
            }

            let mut branch_operations: HashSet<String> = HashSet::from_iter(
                Operation::query(
                    conn,
                    "select * from operation where branch_id = ?1;",
                    rusqlite::params!(Value::from(branch_id)),
                )
                .iter()
                .map(|op| op.hash.clone())
                .collect::<Vec<String>>(),
            );
            branch_operations.extend(
                operations
                    .iter()
                    .map(|op| op.hash.clone())
                    .collect::<Vec<String>>(),
            );

            println!("g is {graph:?}");
            // remove all nodes not in our branch operations. We do this here because upstream operations
            // may be created in a different branch_id but shared with this branch.
            for node in graph.node_ids.clone().keys() {
                if !branch_operations.contains(node) {
                    graph.remove_node(node);
                }
            }

            // Now traverse down from our starting point, we should only have 1 valid path that is not
            // cutoff and in our branch operations
            let mut dfs = Dfs::new(&graph.graph, creation_node);
            // get rid of the first node which is creation_id
            dfs.next(&graph.graph);

            while let Some(child) = dfs.next(&graph.graph) {
                let child_hash = graph.get_key(child);
                operations.push(Operation::get_by_hash(conn, &child_hash));
            }
        }

        operations
    }

    pub fn mask_operation(conn: &Connection, branch_id: i64, operation_hash: &str) {
        conn.execute("INSERT OR IGNORE into branch_masked_operations (branch_id, operation_hash) values (?1, ?2);", (branch_id, operation_hash.to_string())).unwrap();
    }

    pub fn get_masked_operations(conn: &Connection, branch_id: i64) -> Vec<String> {
        let mut stmt = conn
            .prepare("select operation_hash from branch_masked_operations where branch_id = ?1")
            .unwrap();

        stmt.query_map((branch_id,), |row| row.get(0))
            .unwrap()
            .map(|res| res.unwrap())
            .collect::<Vec<String>>()
    }
}

pub struct OperationState {}

impl OperationState {
    pub fn set_operation(conn: &Connection, db_uuid: &str, op_hash: &str) {
        let mut stmt = conn
            .prepare(
                "INSERT INTO operation_state (db_uuid, operation_hash)
          VALUES (?1, ?2)
          ON CONFLICT (db_uuid) DO
          UPDATE SET operation_hash=excluded.operation_hash;",
            )
            .unwrap();
        stmt.execute((db_uuid.to_string(), op_hash.to_string()))
            .unwrap();
        let branch_id =
            OperationState::get_current_branch(conn, db_uuid).expect("No current branch set.");
        Branch::set_current_operation(conn, branch_id, op_hash);
    }

    pub fn get_operation(conn: &Connection, db_uuid: &str) -> Option<String> {
        let mut hash: Option<String> = None;
        let mut stmt = conn
            .prepare("SELECT operation_hash from operation_state where db_uuid = ?1;")
            .unwrap();
        let rows = stmt
            .query_map((db_uuid.to_string(),), |row| row.get(0))
            .unwrap();
        for row in rows {
            hash = row.unwrap();
        }
        hash
    }

    pub fn set_branch(conn: &Connection, db_uuid: &str, branch_name: &str) {
        let branch = Branch::get_by_name(conn, db_uuid, branch_name)
            .unwrap_or_else(|| panic!("No branch named {branch_name}."));
        let mut stmt = conn
            .prepare(
                "INSERT INTO operation_state (db_uuid, branch_id)
          VALUES (?1, ?2)
          ON CONFLICT (db_uuid) DO
          UPDATE SET branch_id=excluded.branch_id;",
            )
            .unwrap();
        stmt.execute(params_from_iter(vec![
            Value::from(db_uuid.to_string()),
            Value::from(branch.id),
        ]))
        .unwrap();
        if let Some(current_branch_id) = OperationState::get_current_branch(conn, db_uuid) {
            if current_branch_id != branch.id {
                panic!("Failed to set branch to {branch_name}");
            }
        } else {
            panic!("Failed to set branch.");
        }
    }

    pub fn get_current_branch(conn: &Connection, db_uuid: &str) -> Option<i64> {
        let mut id: Option<i64> = None;
        let mut stmt = conn
            .prepare("SELECT branch_id from operation_state where db_uuid = ?1;")
            .unwrap();
        let rows = stmt
            .query_map((db_uuid.to_string(),), |row| row.get(0))
            .unwrap();
        for row in rows {
            id = row.unwrap();
        }
        id
    }
}

pub fn setup_db(conn: &Connection, db_uuid: &str) {
    // check if the database is known. If not, initialize it.
    if Branch::query(
        conn,
        "select * from branch where db_uuid = ?1",
        vec![Value::from(db_uuid.to_string())],
    )
    .is_empty()
    {
        Branch::create(conn, db_uuid, "main");
        OperationState::set_branch(conn, db_uuid, "main");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::metadata;
    use crate::operation_management;
    use crate::test_helpers::{
        create_operation, get_connection, get_operation_connection, keys_match, setup_gen_dir,
    };

    #[test]
    fn test_gets_operations_of_branch() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = &metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, db_uuid);

        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-1");
        // operations will be made in ascending order.
        // The branch topology is as follows. () indicate where a branch starts
        //
        //                     -> 4 -> 5
        //                   /
        //         -> 2 -> 3 (branch-1-sub-1)
        //        /
        //      branch-1
        //    /
        //   1 (main, branch-1, branch-2)
        //    \
        //    branch-2
        //       \
        //        -> 6 -> 7 (branch-2-midpoint-1) -> 8 (branch-2-sub-1)
        //                 \                           \
        //                   -> 12 -> 13                9 -> 10 -> 11
        //
        //
        //
        //
        let branch_1 = Branch::create(op_conn, db_uuid, "branch-1");
        let branch_2 = Branch::create(op_conn, db_uuid, "branch-2");
        OperationState::set_branch(op_conn, db_uuid, "branch-1");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-2");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-3");
        let branch_1_sub_1 = Branch::create(op_conn, db_uuid, "branch-1-sub-1");
        OperationState::set_branch(op_conn, db_uuid, "branch-1-sub-1");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-4");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-5");

        // TODO: We should merge the set branch/operation stuff, now that operations track branches we likely don't need set_branch
        OperationState::set_branch(op_conn, db_uuid, "branch-2");
        OperationState::set_operation(op_conn, db_uuid, "op-1");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-6");
        let branch_2_midpoint =
            create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-7");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-8");

        let branch_2_sub_1 = Branch::create(op_conn, db_uuid, "branch-2-sub-1");
        OperationState::set_branch(op_conn, db_uuid, "branch-2-sub-1");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-9");
        create_operation(
            conn,
            op_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-10",
        );
        create_operation(
            conn,
            op_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-11",
        );

        OperationState::set_operation(op_conn, db_uuid, &branch_2_midpoint.hash);
        OperationState::set_branch(op_conn, db_uuid, &branch_2.name);
        let branch_2_midpoint_1 = Branch::create(op_conn, db_uuid, "branch-2-midpoint-1");
        OperationState::set_branch(op_conn, db_uuid, &branch_2_midpoint_1.name);
        create_operation(
            conn,
            op_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-12",
        );
        create_operation(
            conn,
            op_conn,
            "test.fasta",
            FileTypes::Fasta,
            "foo",
            "op-13",
        );

        let ops = Branch::get_operations(op_conn, branch_2_midpoint_1.id)
            .iter()
            .map(|f| f.hash.clone())
            .collect::<Vec<String>>();
        assert_eq!(
            ops,
            vec![
                "op-1".to_string(),
                "op-6".to_string(),
                "op-7".to_string(),
                "op-12".to_string(),
                "op-13".to_string()
            ]
        );

        let ops = Branch::get_operations(op_conn, branch_1.id)
            .iter()
            .map(|f| f.hash.clone())
            .collect::<Vec<String>>();
        assert_eq!(
            ops,
            vec!["op-1".to_string(), "op-2".to_string(), "op-3".to_string()]
        );

        let ops = Branch::get_operations(op_conn, branch_2.id)
            .iter()
            .map(|f| f.hash.clone())
            .collect::<Vec<String>>();
        assert_eq!(
            ops,
            vec![
                "op-1".to_string(),
                "op-6".to_string(),
                "op-7".to_string(),
                "op-8".to_string()
            ]
        );

        let ops = Branch::get_operations(op_conn, branch_1_sub_1.id)
            .iter()
            .map(|f| f.hash.clone())
            .collect::<Vec<String>>();
        assert_eq!(
            ops,
            vec![
                "op-1".to_string(),
                "op-2".to_string(),
                "op-3".to_string(),
                "op-4".to_string(),
                "op-5".to_string()
            ]
        );

        let ops = Branch::get_operations(op_conn, branch_2_sub_1.id)
            .iter()
            .map(|f| f.hash.clone())
            .collect::<Vec<String>>();
        assert_eq!(
            ops,
            vec![
                "op-1".to_string(),
                "op-6".to_string(),
                "op-7".to_string(),
                "op-8".to_string(),
                "op-9".to_string(),
                "op-10".to_string(),
                "op-11".to_string()
            ]
        );
    }

    #[test]
    fn test_graph_representation() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = &metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, db_uuid);

        let change = FileAddition::create(op_conn, "foo", FileTypes::Fasta);
        // operations will be made in ascending order.
        // The branch topology is as follows. () indicate where a branch starts
        //
        //
        //
        //    branch-3   /-> 7
        //    main      1 -> 2 -> 3
        //    branch-1             \-> 4 -> 5
        //    branch-2                  \-> 6

        let mut expected_graph = OperationGraph::new();
        expected_graph.add_edge("op-1", "op-2");
        expected_graph.add_edge("op-2", "op-3");
        expected_graph.add_edge("op-3", "op-4");
        expected_graph.add_edge("op-4", "op-5");
        expected_graph.add_edge("op-4", "op-6");
        expected_graph.add_edge("op-1", "op-7");

        let _ = Operation::create(op_conn, db_uuid, "vcf_addition", change.id, "op-1").unwrap();
        let _ = Operation::create(op_conn, db_uuid, "vcf_addition", change.id, "op-2").unwrap();
        let _ = Operation::create(op_conn, db_uuid, "vcf_addition", change.id, "op-3").unwrap();
        Branch::create(op_conn, db_uuid, "branch-1");
        OperationState::set_branch(op_conn, db_uuid, "branch-1");
        let _ = Operation::create(op_conn, db_uuid, "vcf_addition", change.id, "op-4").unwrap();
        let _ = Operation::create(op_conn, db_uuid, "vcf_addition", change.id, "op-5").unwrap();
        OperationState::set_operation(op_conn, db_uuid, "op-4");
        Branch::create(op_conn, db_uuid, "branch-2");
        OperationState::set_branch(op_conn, db_uuid, "branch-2");
        let _ = Operation::create(op_conn, db_uuid, "vcf_addition", change.id, "op-6").unwrap();
        OperationState::set_operation(op_conn, db_uuid, "op-1");
        Branch::create(op_conn, db_uuid, "branch-3");
        OperationState::set_branch(op_conn, db_uuid, "branch-3");
        let _ = Operation::create(op_conn, db_uuid, "vcf_addition", change.id, "op-7").unwrap();
        let graph = Operation::get_operation_graph(op_conn);

        assert!(keys_match(&graph.node_ids, &expected_graph.node_ids));
        assert_eq!(
            graph
                .graph
                .all_edges()
                .map(|(src, dest, _)| (graph.get_key(src), graph.get_key(dest)))
                .collect::<Vec<(String, String)>>(),
            expected_graph
                .graph
                .all_edges()
                .map(|(src, dest, _)| (expected_graph.get_key(src), expected_graph.get_key(dest)))
                .collect::<Vec<(String, String)>>()
        );
    }

    #[test]
    fn test_path_between() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = &metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, db_uuid);

        // operations will be made in ascending order.
        // The branch topology is as follows. () indicate where a branch starts
        //
        //
        //
        //    branch-3   /-> 7
        //    main      1 -> 2 -> 3
        //    branch-1             \-> 4 -> 5
        //    branch-2                  \-> 6

        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-1");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-2");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-3");
        Branch::create(op_conn, db_uuid, "branch-1");
        OperationState::set_branch(op_conn, db_uuid, "branch-1");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-4");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-5");
        OperationState::set_operation(op_conn, db_uuid, "op-4");
        Branch::create(op_conn, db_uuid, "branch-2");
        OperationState::set_branch(op_conn, db_uuid, "branch-2");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-6");
        OperationState::set_operation(op_conn, db_uuid, "op-1");
        Branch::create(op_conn, db_uuid, "branch-3");
        OperationState::set_branch(op_conn, db_uuid, "branch-3");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-7");

        assert_eq!(
            Operation::get_path_between(op_conn, "op-1", "op-6"),
            vec![
                ("op-1".to_string(), Direction::Outgoing, "op-2".to_string()),
                ("op-2".to_string(), Direction::Outgoing, "op-3".to_string()),
                ("op-3".to_string(), Direction::Outgoing, "op-4".to_string()),
                ("op-4".to_string(), Direction::Outgoing, "op-6".to_string()),
            ]
        );

        assert_eq!(
            Operation::get_path_between(op_conn, "op-7", "op-1"),
            vec![("op-7".to_string(), Direction::Incoming, "op-1".to_string()),]
        );

        assert_eq!(
            Operation::get_path_between(op_conn, "op-3", "op-7"),
            vec![
                ("op-3".to_string(), Direction::Incoming, "op-2".to_string()),
                ("op-2".to_string(), Direction::Incoming, "op-1".to_string()),
                ("op-1".to_string(), Direction::Outgoing, "op-7".to_string()),
            ]
        );
    }

    #[test]
    #[should_panic(
        expected = "The current operation is in the middle of a branch. A new operation would create a bifurcation in the branch lineage. Create a new branch if you wish to bifurcate the current set of operations."
    )]
    fn test_prevents_bifurcation() {
        // We make a simple branch from 1 -> 2 -> 3 -> 4 and ensure we cannot checkout operation 2
        // and create a new operation from that point on the same branch.

        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = &metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, db_uuid);

        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-1");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-2");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-3");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-4");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-5");
        OperationState::set_operation(op_conn, db_uuid, "op-2");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-6");
    }

    #[test]
    fn test_bifurcation_allowed_on_new_branch() {
        // We make a simple branch from 1 -> 2 -> 3 -> 4 and ensure we can checkout operation 2
        // because there is a new branch made

        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = &metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, db_uuid);

        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-1");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-2");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-3");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-4");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-5");
        OperationState::set_operation(op_conn, db_uuid, "op-2");
        Branch::create(op_conn, db_uuid, "branch-1");
        OperationState::set_branch(op_conn, db_uuid, "branch-1");
        create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-6");
    }

    #[test]
    fn test_bifurcation_allowed_on_reset() {
        // We make a simple branch from 1 -> 2 -> 3 -> 4 and ensure we can reset to operation 2
        // and create a new operation from that point on the same branch because we reset.

        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = &metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, db_uuid);

        let op_1 = create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-1");
        let op_2 = create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-2");
        let _op_3 = create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-3");
        let _op_4 = create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-4");

        operation_management::reset(conn, op_conn, db_uuid, "op-2");
        let op_5 = create_operation(conn, op_conn, "test.fasta", FileTypes::Fasta, "foo", "op-5");
        assert_eq!(
            Branch::get_operations(
                op_conn,
                OperationState::get_current_branch(op_conn, db_uuid).unwrap()
            )
            .iter()
            .map(|op| op.hash.clone())
            .collect::<Vec<String>>(),
            vec![op_1.hash.clone(), op_2.hash.clone(), op_5.hash.clone()]
        );
    }

    #[test]
    fn test_sets_start_operation_hash_on_first_change() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = &metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection("t3.db");
        setup_db(op_conn, db_uuid);

        let db_uuid2 = "another-thing";
        setup_db(op_conn, db_uuid2);

        let db1_main = Branch::get_by_name(op_conn, db_uuid, "main").unwrap().id;
        let db2_main = Branch::get_by_name(op_conn, db_uuid2, "main").unwrap().id;

        let change = FileAddition::create(op_conn, "foo", FileTypes::Fasta);
        let op_1 =
            Operation::create(op_conn, db_uuid, "vcf_addition", change.id, "op-1-hash").unwrap();

        assert_eq!(Branch::get_operations(op_conn, db2_main), vec![]);

        let op_2 =
            Operation::create(op_conn, db_uuid2, "vcf_addition", change.id, "op-2-hash").unwrap();

        assert_eq!(
            Branch::get_operations(op_conn, db1_main)
                .iter()
                .map(|op| op.hash.clone())
                .collect::<Vec<String>>(),
            vec![op_1.hash.clone()]
        );
        assert_eq!(
            Branch::get_operations(op_conn, db2_main)
                .iter()
                .map(|op| op.hash.clone())
                .collect::<Vec<String>>(),
            vec![op_2.hash.clone()]
        );
    }
}
