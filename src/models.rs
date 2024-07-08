use std::fmt::*;

use rusqlite::{params_from_iter, Connection, ToSql};
use crate::models;

#[derive(Debug)]
pub struct Node {
    pub id: i32,
    pub base: String,
}

impl Node {
    pub fn create(conn: &mut Connection, base: String) -> Node {
        let mut stmt   = conn.prepare("INSERT INTO nodes (base) VALUES (?1) RETURNING *").unwrap();
        let mut rows = stmt.query_map((base,), |row| {Ok(models::Node {id: row.get(0)?, base: row.get(1)?})}).unwrap();
        let node = rows.next().unwrap().unwrap();
        Node { id: node.id, base: node.base}
    }

    pub fn bulk_create(conn: &mut Connection, bases: &Vec<String>) -> Vec<Node> {
        let placeholders = bases.iter().map(|_| "(?)").collect::<Vec<_>>().join(", ");
        let q = format!("INSERT INTO nodes (base) VALUES {} RETURNING *", placeholders);
        let mut stmt   = conn.prepare(&q).unwrap();
        let rows = stmt.query_map(params_from_iter(bases), |row| {Ok(models::Node {id: row.get(0)?, base: row.get(1)?})}).unwrap();
        let mut nodes : Vec<Node> = vec![];
        for row in rows {
            let node = row.unwrap();
            nodes.push(Node{ id: node.id, base: node.base});
        }
        return nodes;

    }
}

#[derive(Debug)]
pub struct Edge {
    pub id: i32,
    pub source_id: i32,
    pub target_id: i32,
}


impl Edge {
    pub fn create(conn: &mut Connection, source_id: i32, target_id: i32) -> Edge {
        let mut stmt   = conn.prepare("INSERT INTO edges (source_id, target_id) VALUES (?1, ?2) RETURNING *").unwrap();
        stmt.query_row((source_id, target_id), |row| {Ok(models::Edge {id: row.get(0)?, source_id: row.get(1)?, target_id: row.get(2)?})}).unwrap()
    }

    pub fn bulk_create(conn: &mut Connection, edges: &Vec<Edge>) -> Vec<Edge> {
        let tx = conn.transaction().unwrap();
        let mut results = vec![];
        for edge in edges {
            let mut stmt   = tx.prepare("INSERT INTO edges (source_id, target_id) VALUES (?1, ?2) RETURNING *").unwrap();
            let result = stmt.query_row([edge.source_id, edge.target_id], |row| {Ok(models::Edge {id: row.get(0)?, source_id: row.get(1)?, target_id: row.get(2)?})}).unwrap();
            results.push(result);
        }
        tx.commit();
        return results;

    }
}