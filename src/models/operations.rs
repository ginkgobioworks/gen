use crate::graph::all_simple_paths;
use crate::models::file_types::FileTypes;
use itertools::Itertools;
use petgraph::graphmap::{DiGraphMap, UnGraphMap};
use petgraph::Direction;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};

#[derive(Clone, Debug)]
pub struct Operation {
    pub id: i32,
    pub db_uuid: String,
    pub parent_id: Option<i32>,
    pub collection_name: String,
    pub change_type: String,
    pub change_id: i32,
}

impl Operation {
    pub fn create(
        conn: &Connection,
        db_uuid: &str,
        collection_name: &str,
        change_type: &str,
        change_id: i32,
    ) -> Operation {
        let current_op = OperationState::get_operation(conn, db_uuid);
        let query = "INSERT INTO operation (db_uuid, collection_name, change_type, change_id, parent_id) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map(
                params_from_iter(vec![
                    Value::from(db_uuid.to_string()),
                    Value::from(collection_name.to_string()),
                    Value::from(change_type.to_string()),
                    Value::from(change_id),
                    Value::from(current_op),
                ]),
                |row| {
                    Ok(Operation {
                        id: row.get(0)?,
                        db_uuid: db_uuid.to_string(),
                        parent_id: current_op,
                        collection_name: collection_name.to_string(),
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
                graph.add_edge(op.id, v, ());
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
        for (source, target, weight) in directed_graph.all_edges() {
            undirected_graph.add_edge(source, target, ());
        }
        let mut patch_path: Vec<(i32, Direction, i32)> = vec![];
        for path in all_simple_paths(&undirected_graph, source_id, target_id) {
            let mut last_node = 0;
            for node in path {
                if node != source_id {
                    for (edge_src, edge_target, edge_weight) in
                        directed_graph.edges_directed(last_node, Direction::Outgoing)
                    {
                        if edge_target == node {
                            patch_path.push((last_node, Direction::Outgoing, node));
                            break;
                        }
                    }
                    for (edge_src, edge_target, edge_weight) in
                        directed_graph.edges_directed(last_node, Direction::Incoming)
                    {
                        if edge_src == node {
                            patch_path.push((node, Direction::Incoming, last_node));
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
                    collection_name: row.get(3)?,
                    change_type: row.get(4)?,
                    change_id: row.get(5)?,
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
                    collection_name: row.get(3)?,
                    change_type: row.get(4)?,
                    change_id: row.get(5)?,
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
        if let Some(branch) = Branch::get_by_name(conn, db_uuid, branch_name) {
            let branch_id = branch.id;
            if let Some(current_branch) = OperationState::get_current_branch(conn, db_uuid) {
                if current_branch == branch_id {
                    panic!("Unable to delete the branch that is currently active.");
                }
            }
            conn.execute(
                "delete from branch_operation where branch_id = ?1",
                (branch_id,),
            )
            .expect("Error deleting from branch_operation table.");
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

    pub fn set_current_operation(conn: &Connection, branch_id: i32, operation_id: i32) {
        conn.execute(
            "UPDATE branch set current_operation_id = ?2 where id = ?1",
            (branch_id, operation_id),
        )
        .unwrap();
    }
}

pub struct OperationState {
    operation_id: i32,
}

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
        println!("setting branc to {branch_name}");
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
