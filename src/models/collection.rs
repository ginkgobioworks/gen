use rusqlite::{params_from_iter, Connection};

use crate::models::block_group::BlockGroup;

#[derive(Debug)]
pub struct Collection {
    pub name: String,
}

impl Collection {
    pub fn exists(conn: &Connection, name: &str) -> bool {
        let mut stmt = conn
            .prepare("select name from collection where name = ?1")
            .unwrap();
        stmt.exists([name]).unwrap()
    }
    pub fn create(conn: &Connection, name: &str) -> Collection {
        let mut stmt = conn
            .prepare("INSERT INTO collection (name) VALUES (?1) RETURNING *")
            .unwrap();
        let mut rows = stmt
            .query_map((name,), |row| Ok(Collection { name: row.get(0)? }))
            .unwrap();
        rows.next().unwrap().unwrap()
    }

    pub fn bulk_create(conn: &Connection, names: &Vec<String>) -> Vec<Collection> {
        let placeholders = names.iter().map(|_| "(?)").collect::<Vec<_>>().join(", ");
        let q = format!(
            "INSERT INTO collection (name) VALUES {} RETURNING *",
            placeholders
        );
        let mut stmt = conn.prepare(&q).unwrap();
        let rows = stmt
            .query_map(params_from_iter(names), |row| {
                Ok(Collection { name: row.get(0)? })
            })
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }

    pub fn get_block_groups(conn: &Connection, collection_name: &str) -> Vec<BlockGroup> {
        // Load all block groups that have the given collection_name
        let mut stmt = conn
            .prepare("SELECT * FROM block_group WHERE collection_name = ?1")
            .unwrap();
        let block_group_iter = stmt
            .query_map([collection_name], |row| {
                Ok(BlockGroup {
                    id: row.get(0)?,
                    collection_name: row.get(1)?,
                    sample_name: row.get(2)?,
                    name: row.get(3)?,
                })
            })
            .unwrap();
        block_group_iter.map(|bg| bg.unwrap()).collect()
    }
}
