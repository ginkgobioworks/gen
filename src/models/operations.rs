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
        db_uuid: &String,
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
                    Value::from(db_uuid.clone()),
                    Value::from(collection_name.to_string()),
                    Value::from(change_type.to_string()),
                    Value::from(change_id),
                    Value::from(current_op),
                ]),
                |row| {
                    Ok(Operation {
                        id: row.get(0)?,
                        db_uuid: db_uuid.clone(),
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

pub struct OperationState {
    operation_id: i32,
}

impl OperationState {
    pub fn set_operation(conn: &Connection, db_uuid: &String, op_id: i32) {
        let mut stmt = conn
            .prepare(
                "INSERT INTO operation_state (db_uuid, operation_id)
          VALUES (?1, ?2)
          ON CONFLICT (db_uuid) DO
          UPDATE SET operation_id=excluded.operation_id;",
            )
            .unwrap();
        stmt.execute((db_uuid, op_id)).unwrap();
    }

    pub fn get_operation(conn: &Connection, db_uuid: &String) -> Option<i32> {
        let mut id: Option<i32> = None;
        let mut stmt = conn
            .prepare("SELECT operation_id from operation_state where db_uuid = ?1;")
            .unwrap();
        let rows = stmt.query_map((db_uuid,), |row| row.get(0)).unwrap();
        for row in rows {
            id = Some(row.unwrap());
        }
        id
    }
}