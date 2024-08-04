use std::str;

pub mod migrations;
pub mod models;

use crate::migrations::run_migrations;
use rusqlite::Connection;
use sha2::{Digest, Sha256};

pub fn get_connection(db_path: &str) -> Connection {
    let mut conn =
        Connection::open(db_path).unwrap_or_else(|_| panic!("Error connecting to {}", db_path));
    rusqlite::vtab::array::load_module(&conn).unwrap();
    run_migrations(&mut conn);
    conn
}

pub fn run_query(conn: &Connection, query: &str) {
    let mut stmt = conn.prepare(query).unwrap();
    for entry in stmt.query_map([], |row| Ok(println!("{row:?}"))).unwrap() {
        println!("{entry:?}");
    }
}

pub fn calculate_hash(t: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(t);
    let result = hasher.finalize();

    format!("{:x}", result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_migrations;

    fn get_connection() -> Connection {
        let mut conn = Connection::open_in_memory()
            .unwrap_or_else(|_| panic!("Error opening in memory test db"));
        run_migrations(&mut conn);
        conn
    }

    #[test]
    fn it_hashes() {
        assert_eq!(
            calculate_hash("a test"),
            "a82639b6f8c3a6e536d8cc562c3b86ff4b012c84ab230c1e5be649aa9ad26d21"
        );
    }

    #[test]
    fn it_queries() {
        let mut conn = get_connection();
        let sequence_count: i32 = conn
            .query_row(
                "SELECT count(*) from sequence where hash = 'foo'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sequence_count, 0);
    }
}
