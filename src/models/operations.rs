use crate::config::{get_operation, set_operation};
use crate::models::file_types::FileTypes;
use itertools::Itertools;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Operation {
    pub id: i32,
    pub parent_id: Option<i32>,
    pub collection_name: String,
    // pub sample: Option<String>,
    pub change_type: String,
    pub change_id: i32,
}
//
// pub struct NewOperation {
//     pub collection_name: String,
//     pub sample: Option<String>,
//     pub change_type: String,
//     pub change_id: i32,
// }
//
// pub struct OperationCache<'a> {
//     pub cache: HashMap<NewOperation, Operation>,
//     pub conn: &'a Connection,
// }
//
// impl OperationCache<'_> {
//     pub fn new(conn: &Connection) -> OperationCache {
//         OperationCache {
//             cache: HashMap::<NewOperation, Operation>::new(),
//             conn,
//         }
//     }
//
//     pub fn lookup(operation_cache: &mut OperationCache, collection_name: String, sample: Option<String>, change_type: String, change_id: i32) -> Operation {
//         let key = NewOperation {
//             collection_name: collection_name.clone(),
//             sample: sample.clone(),
//             change_type: change_type.clone(),
//             change_id
//         };
//         let lookup = operation_cache.cache.get(&key);
//         if let Some(operation) = lookup {
//             operation.copy()
//         } else {
//             let obj = Operation::create(operation_cache.conn, collection_name, sample, change_type, change_id);
//
//             operation_cache.cache.insert(key, obj.clone());
//             obj
//         }
//     }
// }

impl Operation {
    pub fn create(
        conn: &Connection,
        collection_name: &str,
        change_type: &str,
        change_id: i32,
    ) -> Operation {
        let current_op = get_operation();
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
        // TODO: error condition here where we can write to disk but rollback a transaction
        set_operation(operation.id);
        operation
    }

    pub fn get_valid_blockgroup_edge_ids(conn: &Connection) -> Option<Vec<i32>> {
        let operation_id = get_operation()?;
        let query = "WITH RECURSIVE operations(operation_id) AS ( \
        select ?1 UNION \
        select parent_id from operation join operations ON id=operation_id \
        ) SELECT operation_id from operations where operation_id is not null;";
        let mut stmt = conn.prepare(query).unwrap();
        let mut operation_ids = stmt
            .query_map((operation_id,), |row| row.get(0))
            .unwrap()
            .map(|id| id.unwrap())
            .collect::<Vec<i32>>();
        let op_ids = operation_ids.iter().map(|id| id.to_string()).join(", ");
        println!("op ids {op_ids}");
        let query = format!(
            "SELECT block_group_edge_id from operation_edge where operation_id IN ({op_ids});"
        );
        println!("e q {query}");
        let mut stmt = conn.prepare(&query).unwrap();
        Some(
            stmt.query_map([], |row| row.get(0))
                .unwrap()
                .map(|id| id.unwrap())
                .collect::<Vec<i32>>(),
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

pub struct OperationEdge {
    pub id: i32,
    operation_id: i32,
    path_id: i32,
    block_group_edge_id: i32,
}

impl OperationEdge {
    pub fn bulk_create(
        conn: &Connection,
        operation_id: i32,
        path_id: Option<i32>,
        sample_name: Option<String>,
        block_group_edge_ids: &[i32],
    ) {
        for chunk in block_group_edge_ids.chunks(100000) {
            let mut rows_to_insert = vec![];
            for id in chunk {
                let sample = match sample_name {
                    Some(ref v) => format!("\"{v}\""),
                    None => "NULL".to_string(),
                };
                let path = match path_id {
                    Some(ref v) => format!("{v}"),
                    None => "NULL".to_string(),
                };
                rows_to_insert.push(format!("({operation_id}, {path}, {sample}, {id})"));
            }

            let formatted_rows_to_insert = rows_to_insert.join(", ");

            let insert_statement = format!(
                "INSERT INTO operation_edge (operation_id, path_id, sample_name, block_group_edge_id) VALUES {0};",
                formatted_rows_to_insert
            );
            let _ = conn.execute(&insert_statement, ());
        }
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
