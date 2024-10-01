use crate::graph::all_simple_paths;
use crate::models::file_types::FileTypes;
use petgraph::graphmap::{DiGraphMap, UnGraphMap};
use petgraph::visit::{Dfs, Reversed};
use petgraph::Direction;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use std::collections::HashSet;
use std::string::ToString;

#[derive(Clone, Debug)]
pub struct Operation {
    pub id: i32,
    pub db_uuid: String,
    pub parent_id: Option<i32>,
    pub branch_id: i32,
    pub collection_name: Option<String>,
    pub change_type: String,
    pub change_id: i32,
}

impl Operation {
    pub fn create(
        conn: &Connection,
        db_uuid: &str,
        collection_name: impl Into<Option<String>>,
        change_type: &str,
        change_id: i32,
    ) -> Operation {
        let collection_name = collection_name.into();
        let current_op = OperationState::get_operation(conn, db_uuid);
        let current_branch_id =
            OperationState::get_current_branch(conn, db_uuid).expect("No branch is checked out.");

        // if we are in the middle of a branch's operations, and not on a new branch's creation point
        // we cannot create a new operation as that would create a bifurcation in a branch's order
        // of operations. We ensure there is no child operation in this branch of the current operation.

        if let Some(op_id) = current_op {
            let count: i32 = conn
                .query_row(
                    "select count(*) from operation where branch_id = ?1 AND parent_id = ?2 AND id not in (select operation_id from branch_masked_operations where branch_id = ?1);",
                    (current_branch_id, op_id),
                    |row| row.get(0),
                )
                .unwrap();
            if count != 0 {
                panic!("The current operation is in the middle of a branch. A new operation would create a bifurcation in the branch lineage. Create a new branch if you wish to bifurcate the current set of operations.");
            }
        }

        let query = "INSERT INTO operation (db_uuid, collection_name, change_type, change_id, parent_id, branch_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map(
                params_from_iter(vec![
                    Value::from(db_uuid.to_string()),
                    Value::from(collection_name.clone()),
                    Value::from(change_type.to_string()),
                    Value::from(change_id),
                    Value::from(current_op),
                    Value::from(current_branch_id),
                ]),
                |row| {
                    Ok(Operation {
                        id: row.get(0)?,
                        db_uuid: db_uuid.to_string(),
                        parent_id: current_op,
                        branch_id: current_branch_id,
                        collection_name: collection_name.clone(),
                        change_type: change_type.to_string(),
                        change_id,
                    })
                },
            )
            .unwrap();
        let operation = rows.next().unwrap().unwrap();
        // TODO: error condition here where we can write to disk but transaction fails
        OperationState::set_operation(conn, &operation.db_uuid, operation.id);
        operation
    }

    pub fn get_upstream(conn: &Connection, operation_id: i32) -> Vec<i32> {
        let query = "WITH RECURSIVE operations(operation_id) AS ( \
        select ?1 UNION \
        select parent_id from operation join operations ON id=operation_id \
        ) SELECT operation_id from operations where operation_id is not null order by operation_id desc;";
        let mut stmt = conn.prepare(query).unwrap();
        stmt.query_map((operation_id,), |row| row.get(0))
            .unwrap()
            .map(|id| id.unwrap())
            .collect::<Vec<i32>>()
    }

    pub fn get_operation_graph(conn: &Connection) -> DiGraphMap<i32, ()> {
        let mut graph: DiGraphMap<i32, ()> = DiGraphMap::new();
        let operations = Operation::query(conn, "select * from operation;", vec![]);
        for op in operations.iter() {
            graph.add_node(op.id);
            if let Some(v) = op.parent_id {
                graph.add_node(v);
                graph.add_edge(v, op.id, ());
            }
        }
        graph
    }

    pub fn get_path_between(
        conn: &Connection,
        source_id: i32,
        target_id: i32,
    ) -> Vec<(i32, Direction, i32)> {
        let directed_graph = Operation::get_operation_graph(conn);
        let mut undirected_graph: UnGraphMap<i32, ()> = Default::default();

        for node in directed_graph.nodes() {
            undirected_graph.add_node(node);
        }
        for (source, target, _weight) in directed_graph.all_edges() {
            undirected_graph.add_edge(source, target, ());
        }
        let mut patch_path: Vec<(i32, Direction, i32)> = vec![];
        for path in all_simple_paths(&undirected_graph, source_id, target_id) {
            let mut last_node = 0;
            for node in path {
                if node != source_id {
                    for (_edge_src, edge_target, _edge_weight) in
                        directed_graph.edges_directed(last_node, Direction::Outgoing)
                    {
                        if edge_target == node {
                            patch_path.push((last_node, Direction::Outgoing, node));
                            break;
                        }
                    }
                    for (edge_src, _edge_target, _edge_weight) in
                        directed_graph.edges_directed(last_node, Direction::Incoming)
                    {
                        if edge_src == node {
                            patch_path.push((last_node, Direction::Incoming, node));
                            break;
                        }
                    }
                }
                last_node = node;
            }
        }
        patch_path
    }

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<Operation> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(Operation {
                    id: row.get(0)?,
                    db_uuid: row.get(1)?,
                    parent_id: row.get(2)?,
                    branch_id: row.get(3)?,
                    collection_name: row.get(4)?,
                    change_type: row.get(5)?,
                    change_id: row.get(6)?,
                })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }

    pub fn get(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Operation {
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(Operation {
                    id: row.get(0)?,
                    db_uuid: row.get(1)?,
                    parent_id: row.get(2)?,
                    branch_id: row.get(3)?,
                    collection_name: row.get(4)?,
                    change_type: row.get(5)?,
                    change_id: row.get(6)?,
                })
            })
            .unwrap();
        rows.next().unwrap().unwrap()
    }

    pub fn get_by_id(conn: &Connection, op_id: i32) -> Operation {
        Operation::get(
            conn,
            "select * from operation where id = ?1",
            vec![Value::from(op_id)],
        )
    }
}

pub struct FileAddition {
    pub id: i32,
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
    pub id: i32,
    pub operation_id: i32,
    pub summary: String,
}

impl OperationSummary {
    pub fn create(conn: &Connection, operation_id: i32, summary: &str) -> OperationSummary {
        let query =
            "INSERT INTO operation_summary (operation_id, summary) VALUES (?1, ?2) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map(
                params_from_iter(vec![
                    Value::from(operation_id),
                    Value::from(summary.to_string()),
                ]),
                |row| {
                    Ok(OperationSummary {
                        id: row.get(0)?,
                        operation_id,
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
                    operation_id: row.get(1)?,
                    summary: row.get(2)?,
                })
            })
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }
}

#[derive(Clone, Debug)]
pub struct Branch {
    pub id: i32,
    pub db_uuid: String,
    pub name: String,
    pub start_operation_id: Option<i32>,
    pub current_operation_id: Option<i32>,
}

impl Branch {
    pub fn create(conn: &Connection, db_uuid: &str, branch_name: &str) -> Branch {
        let current_operation_id = OperationState::get_operation(conn, db_uuid);
        let mut stmt = conn.prepare_cached("insert into branch (db_uuid, name, start_operation_id, current_operation_id) values (?1, ?2, ?3, ?3) returning (id);").unwrap();

        let mut rows = stmt
            .query_map((db_uuid, branch_name, current_operation_id), |row| {
                Ok(Branch {
                    id: row.get(0)?,
                    db_uuid: db_uuid.to_string(),
                    name: branch_name.to_string(),
                    start_operation_id: current_operation_id,
                    current_operation_id,
                })
            })
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
                    start_operation_id: row.get(3)?,
                    current_operation_id: row.get(4)?,
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

    pub fn get_by_id(conn: &Connection, branch_id: i32) -> Option<Branch> {
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

    pub fn set_current_operation(conn: &Connection, branch_id: i32, operation_id: i32) {
        conn.execute(
            "UPDATE branch set current_operation_id = ?2 where id = ?1",
            (branch_id, operation_id),
        )
        .unwrap();
    }

    pub fn get_operations(conn: &Connection, branch_id: i32) -> Vec<Operation> {
        let branch = Branch::get_by_id(conn, branch_id)
            .unwrap_or_else(|| panic!("No branch with id {branch_id}."));
        let mut graph = Operation::get_operation_graph(conn);
        let mut operations: Vec<Operation> = vec![];
        let masked_operations = Branch::get_masked_operations(conn, branch_id);
        for op in masked_operations.iter() {
            graph.remove_node(*op);
        }

        let creation_id = branch.start_operation_id.unwrap_or(1);

        let rev_graph = Reversed(&graph);
        let mut dfs = Dfs::new(rev_graph, creation_id);

        while let Some(ancestor) = dfs.next(rev_graph) {
            operations.insert(0, Operation::get_by_id(conn, ancestor));
        }

        let mut branch_operations: HashSet<i32> = HashSet::from_iter(
            Operation::query(
                conn,
                "select * from operation where branch_id = ?1;",
                vec![Value::from(branch_id)],
            )
            .iter()
            .map(|op| op.id)
            .collect::<Vec<i32>>(),
        );
        branch_operations.extend(operations.iter().map(|op| op.id).collect::<Vec<i32>>());

        // remove all nodes not in our branch operations. We do this here because upstream operations
        // may be created in a different branch_id but shared with this branch.
        for node in graph.clone().nodes() {
            if !branch_operations.contains(&node) {
                graph.remove_node(node);
            }
        }

        // Now traverse down from our starting point, we should only have 1 valid path that is not
        // cutoff and in our branch operations
        let mut dfs = Dfs::new(&graph, creation_id);
        // get rid of the first node which is creation_id
        dfs.next(&graph);

        while let Some(child) = dfs.next(&graph) {
            operations.push(Operation::get_by_id(conn, child));
        }

        operations
    }

    pub fn mask_operation(conn: &Connection, branch_id: i32, operation_id: i32) {
        conn.execute("INSERT OR IGNORE into branch_masked_operations (branch_id, operation_id) values (?1, ?2);", (branch_id, operation_id)).unwrap();
    }

    pub fn get_masked_operations(conn: &Connection, branch_id: i32) -> Vec<i32> {
        let mut stmt = conn
            .prepare("select operation_id from branch_masked_operations where branch_id = ?1")
            .unwrap();

        stmt.query_map((branch_id,), |row| row.get(0))
            .unwrap()
            .map(|res| res.unwrap())
            .collect::<Vec<i32>>()
    }
}

pub struct OperationState {}

impl OperationState {
    pub fn set_operation(conn: &Connection, db_uuid: &str, op_id: i32) {
        let mut stmt = conn
            .prepare(
                "INSERT INTO operation_state (db_uuid, operation_id)
          VALUES (?1, ?2)
          ON CONFLICT (db_uuid) DO
          UPDATE SET operation_id=excluded.operation_id;",
            )
            .unwrap();
        stmt.execute((db_uuid.to_string(), op_id)).unwrap();
        let branch_id =
            OperationState::get_current_branch(conn, db_uuid).expect("No current branch set.");
        Branch::set_current_operation(conn, branch_id, op_id);
    }

    pub fn get_operation(conn: &Connection, db_uuid: &str) -> Option<i32> {
        let mut id: Option<i32> = None;
        let mut stmt = conn
            .prepare("SELECT operation_id from operation_state where db_uuid = ?1;")
            .unwrap();
        let rows = stmt
            .query_map((db_uuid.to_string(),), |row| row.get(0))
            .unwrap();
        for row in rows {
            id = row.unwrap();
        }
        id
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

    pub fn get_current_branch(conn: &Connection, db_uuid: &str) -> Option<i32> {
        let mut id: Option<i32> = None;
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
    use crate::imports::fasta::import_fasta;
    use crate::models::metadata;
    use crate::operation_management;
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use std::path::PathBuf;

    #[test]
    fn test_gets_operations_of_branch() {
        setup_gen_dir();
        let db_uuid = "something";
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, db_uuid);

        let change = FileAddition::create(op_conn, "foo", FileTypes::Fasta);
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
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
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        let branch_1_sub_1 = Branch::create(op_conn, db_uuid, "branch-1-sub-1");
        OperationState::set_branch(op_conn, db_uuid, "branch-1-sub-1");
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );

        // TODO: We should merge the set branch/operation stuff, now that operations track branches we likely don't need set_branch
        OperationState::set_branch(op_conn, db_uuid, "branch-2");
        OperationState::set_operation(op_conn, db_uuid, 1);
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        let branch_2_midpoint = Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );

        let branch_2_sub_1 = Branch::create(op_conn, db_uuid, "branch-2-sub-1");
        OperationState::set_branch(op_conn, db_uuid, "branch-2-sub-1");
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );

        OperationState::set_operation(op_conn, db_uuid, branch_2_midpoint.id);
        OperationState::set_branch(op_conn, db_uuid, &branch_2.name);
        let branch_2_midpoint_1 = Branch::create(op_conn, db_uuid, "branch-2-midpoint-1");
        OperationState::set_branch(op_conn, db_uuid, &branch_2_midpoint_1.name);
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );

        let ops = Branch::get_operations(op_conn, branch_2_midpoint_1.id)
            .iter()
            .map(|f| f.id)
            .collect::<Vec<i32>>();
        assert_eq!(ops, vec![1, 6, 7, 12, 13]);

        let ops = Branch::get_operations(op_conn, branch_1.id)
            .iter()
            .map(|f| f.id)
            .collect::<Vec<i32>>();
        assert_eq!(ops, vec![1, 2, 3]);

        let ops = Branch::get_operations(op_conn, branch_2.id)
            .iter()
            .map(|f| f.id)
            .collect::<Vec<i32>>();
        assert_eq!(ops, vec![1, 6, 7, 8]);

        let ops = Branch::get_operations(op_conn, branch_1_sub_1.id)
            .iter()
            .map(|f| f.id)
            .collect::<Vec<i32>>();
        assert_eq!(ops, vec![1, 2, 3, 4, 5]);

        let ops = Branch::get_operations(op_conn, branch_2_sub_1.id)
            .iter()
            .map(|f| f.id)
            .collect::<Vec<i32>>();
        assert_eq!(ops, vec![1, 6, 7, 8, 9, 10, 11]);
    }

    #[test]
    fn test_graph_representation() {
        setup_gen_dir();
        let db_uuid = "something";
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

        let mut expected_graph: DiGraphMap<i32, ()> = DiGraphMap::new();
        expected_graph.add_edge(1, 2, ());
        expected_graph.add_edge(2, 3, ());
        expected_graph.add_edge(3, 4, ());
        expected_graph.add_edge(4, 5, ());
        expected_graph.add_edge(4, 6, ());
        expected_graph.add_edge(1, 7, ());

        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Branch::create(op_conn, db_uuid, "branch-1");
        OperationState::set_branch(op_conn, db_uuid, "branch-1");
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        OperationState::set_operation(op_conn, db_uuid, 4);
        Branch::create(op_conn, db_uuid, "branch-2");
        OperationState::set_branch(op_conn, db_uuid, "branch-2");
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        OperationState::set_operation(op_conn, db_uuid, 1);
        Branch::create(op_conn, db_uuid, "branch-3");
        OperationState::set_branch(op_conn, db_uuid, "branch-3");
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        let graph = Operation::get_operation_graph(op_conn);

        assert_eq!(
            graph.nodes().collect::<Vec<i32>>(),
            expected_graph.nodes().collect::<Vec<i32>>()
        );
        assert_eq!(
            graph
                .all_edges()
                .map(|(src, dest, _)| (src, dest))
                .collect::<Vec<(i32, i32)>>(),
            expected_graph
                .all_edges()
                .map(|(src, dest, _)| (src, dest))
                .collect::<Vec<(i32, i32)>>()
        );
    }

    #[test]
    fn test_path_between() {
        setup_gen_dir();
        let db_uuid = "something";
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

        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Branch::create(op_conn, db_uuid, "branch-1");
        OperationState::set_branch(op_conn, db_uuid, "branch-1");
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        OperationState::set_operation(op_conn, db_uuid, 4);
        Branch::create(op_conn, db_uuid, "branch-2");
        OperationState::set_branch(op_conn, db_uuid, "branch-2");
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        OperationState::set_operation(op_conn, db_uuid, 1);
        Branch::create(op_conn, db_uuid, "branch-3");
        OperationState::set_branch(op_conn, db_uuid, "branch-3");
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );

        assert_eq!(
            Operation::get_path_between(op_conn, 1, 6),
            vec![
                (1, Direction::Outgoing, 2),
                (2, Direction::Outgoing, 3),
                (3, Direction::Outgoing, 4),
                (4, Direction::Outgoing, 6),
            ]
        );

        assert_eq!(
            Operation::get_path_between(op_conn, 7, 1),
            vec![(7, Direction::Incoming, 1),]
        );

        assert_eq!(
            Operation::get_path_between(op_conn, 3, 7),
            vec![
                (3, Direction::Incoming, 2),
                (2, Direction::Incoming, 1),
                (1, Direction::Outgoing, 7),
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
        let db_uuid = "something";
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, db_uuid);

        let change = FileAddition::create(op_conn, "foo", FileTypes::Fasta);
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );

        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        OperationState::set_operation(op_conn, db_uuid, 2);
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
    }

    #[test]
    fn test_bifurcation_allowed_on_new_branch() {
        // We make a simple branch from 1 -> 2 -> 3 -> 4 and ensure we can checkout operation 2
        // because there is a new branch made

        setup_gen_dir();
        let db_uuid = "something";
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, db_uuid);

        let change = FileAddition::create(op_conn, "foo", FileTypes::Fasta);
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );

        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
        OperationState::set_operation(op_conn, db_uuid, 2);
        Branch::create(op_conn, db_uuid, "branch-1");
        OperationState::set_branch(op_conn, db_uuid, "branch-1");
        Operation::create(
            op_conn,
            db_uuid,
            "foo".to_string(),
            "vcf_addition",
            change.id,
        );
    }

    #[test]
    fn test_bifurcation_allowed_on_reset() {
        // We make a simple branch from 1 -> 2 -> 3 -> 4 and ensure we can reset to operation 2
        // and create a new operation from that point on the same branch because we reset.

        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = &metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection("t.db");
        setup_db(op_conn, db_uuid);

        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let collection = "test".to_string();
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );

        operation_management::reset(conn, op_conn, db_uuid, 2);
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            &collection,
            false,
            conn,
            op_conn,
        );
        assert_eq!(
            Branch::get_operations(
                op_conn,
                OperationState::get_current_branch(op_conn, db_uuid).unwrap()
            )
            .iter()
            .map(|op| op.id)
            .collect::<Vec<i32>>(),
            vec![1, 2, 5]
        );
    }
}
