use rusqlite::{params_from_iter, types::Value as SQLValue, Connection};

#[derive(Clone, Debug)]
pub struct OperationPath {
    pub id: i64,
    pub operation_id: i64,
    pub path_id: i64,
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

    // pub fn query(conn: &Connection, query: &str, placeholders: Vec<SQLValue>) -> Vec<OperationPath> {
    //     let mut stmt = conn.prepare(query).unwrap();
    //     let mut objs = vec![];
    //     let rows = stmt
    //         .query_map(params_from_iter(placeholders), |row| {
    //             Ok(Node {
    //                 id: row.get(0)?,
    //                 sequence_hash: row.get(1)?,
    //             })
    //         })
    //         .unwrap();
    //     for row in rows {
    //         objs.push(row.unwrap());
    //     }
    //     objs
    // }
}
