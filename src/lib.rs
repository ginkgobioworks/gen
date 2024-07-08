pub mod models;
pub mod migrations;

use rusqlite::{Connection};
use crate::migrations::run_migrations;

pub fn get_connection(db_path: &str) -> Connection {
    let mut conn = Connection::open(db_path).unwrap_or_else(|_| panic!("Error connecting to {}", db_path));
    run_migrations(&mut conn);
    return conn;
}

#[cfg(test)]
mod tests {
    use crate::migrations::run_migrations;
    use super::*;

    fn get_connection() -> Connection {
        let mut conn = Connection::open_in_memory().unwrap_or_else(|_| panic!("Error opening in memory test db"));
        run_migrations(&mut conn);
        return conn;
    }


    #[test]
    fn create_node() {
        let mut conn = get_connection();
        let node = models::Node::create(&mut conn, "A".to_string());
        assert_eq!(node.base, "A");
    }

    #[test]
    fn create_nodes() {
        let mut conn = get_connection();
        let nodes = models::Node::bulk_create(&mut conn, &vec!["A".to_string(), "T".to_string()]);
        assert_eq!(nodes[0].base, "A");
        assert_eq!(nodes[1].base, "T");
    }

    #[test]
    fn create_edge() {
        let mut conn = get_connection();
        let node = models::Node::create(&mut conn, "A".to_string());
        let node2 = models::Node::create(&mut conn, "T".to_string());
        let edge = models::Edge::create(&mut conn, node.id, node2.id);
        assert_eq!(edge.source_id, node.id);
        assert_eq!(edge.target_id, node2.id);
    }

    #[test]
    fn create_edges() {
        let mut conn = get_connection();
        let node = models::Node::create(&mut conn, "A".to_string());
        let node2 = models::Node::create(&mut conn, "T".to_string());
        let node3 = models::Node::create(&mut conn, "C".to_string());
        let edges = models::Edge::bulk_create(&mut conn, &vec![models::Edge{id: 0, source_id: node.id, target_id: node2.id}, models::Edge{id: 0, source_id: node2.id, target_id: node3.id}]);
        assert_eq!(edges[0].source_id, node.id);
        assert_eq!(edges[0].target_id, node2.id);
        assert_eq!(edges[1].source_id, node2.id);
        assert_eq!(edges[1].target_id, node3.id);
    }
}
