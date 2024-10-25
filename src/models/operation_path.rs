use crate::models::traits::Query;
use rusqlite::{params_from_iter, types::Value as SQLValue, Connection, Row};

#[derive(Clone, Debug)]
pub struct OperationPath {
    pub id: i64,
    pub operation_id: i64,
    pub path_id: i64,
}

impl Query for OperationPath {
    type Model = OperationPath;
    fn process_row(row: &Row) -> Self::Model {
        OperationPath {
            id: row.get(0).unwrap(),
            operation_id: row.get(1).unwrap(),
            path_id: row.get(2).unwrap(),
        }
    }
}

impl OperationPath {
    pub fn create(conn: &Connection, operation_id: i64, path_id: i64) -> i64 {
        let insert_statement =
            "INSERT INTO operation_paths (operation_id, path_id) VALUES (?1, ?2) RETURNING (id);";
        let mut stmt = conn.prepare_cached(insert_statement).unwrap();
        let mut rows = stmt
            .query_map(
                params_from_iter(vec![SQLValue::from(operation_id), SQLValue::from(path_id)]),
                |row| row.get(0),
            )
            .unwrap();
        match rows.next().unwrap() {
            Ok(res) => res,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    let placeholders = vec![operation_id, path_id];
                    let query = "SELECT id from operation_paths where id = ?1 and path_id = ?2;";
                    conn.query_row(query, params_from_iter(&placeholders), |row| row.get(0))
                        .unwrap()
                } else {
                    panic!("something bad happened querying the database")
                }
            }
            Err(_) => {
                panic!("something bad happened querying the database")
            }
        }
    }

    pub fn paths_for_operation(conn: &Connection, operation_id: i64) -> Vec<OperationPath> {
        let select_statement = format!(
            "SELECT * FROM operation_paths WHERE operation_id = {0};",
            operation_id
        );
        OperationPath::query(conn, &select_statement, vec![])
    }
}
