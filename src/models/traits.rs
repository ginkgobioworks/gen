use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection, Row};

pub trait Query {
    type Model;
    fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<Self::Model> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(Self::process_row(row))
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }

    fn get(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Self::Model {
        let mut stmt = conn.prepare(query).unwrap();
        stmt.query_row(params_from_iter(placeholders), |row| {
            Ok(Self::process_row(row))
        })
        .unwrap()
    }

    fn process_row(row: &Row) -> Self::Model;
}
