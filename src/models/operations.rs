use crate::graph::all_simple_paths;
use crate::models::file_types::FileTypes;
use crate::operation_management::{get_operation, set_operation};
use itertools::Itertools;
use petgraph::graphmap::{DiGraphMap, UnGraphMap};
use petgraph::Direction;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};

#[derive(Clone, Debug)]
pub struct Operation {
    pub id: i32,
    pub parent_id: Option<i32>,
    pub collection_name: String,
    pub change_type: String,
    pub change_id: i32,
}

impl Operation {
    pub fn create(
        conn: &Connection,
        collection_name: &str,
        change_type: &str,
        change_id: i32,
    ) -> Operation {
        let current_op = get_operation(conn);
        let query = "INSERT INTO operation (collection_name, change_type, change_id, parent_id) VALUES (?1, ?2, ?3, ?4) RETURNING (id)";
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = stmt
            .query_map(
                params_from_iter(vec![
                    Value::from(collection_name.to_string()),
                    Value::from(change_type.to_string()),
                    Value::from(change_id),
                    Value::from(current_op),
                ]),
                |row| {
                    Ok(Operation {
                        id: row.get(0)?,
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
        set_operation(conn, operation.id);
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
                    parent_id: row.get(1)?,
                    collection_name: row.get(2)?,
                    change_type: row.get(3)?,
                    change_id: row.get(4)?,
                })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
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
