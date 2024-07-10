use std::str;

pub mod migrations;
pub mod models;

use crate::migrations::run_migrations;
use rusqlite::Connection;
use sha2::{Digest, Sha256};

pub fn get_connection(db_path: &str) -> Connection {
    let mut conn =
        Connection::open(db_path).unwrap_or_else(|_| panic!("Error connecting to {}", db_path));
    run_migrations(&mut conn);
    conn
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

    // #[test]
    // fn create_node() {
    //     let mut conn = get_connection();
    //     let node = models::Node::create(&mut conn, "A".to_string());
    //     assert_eq!(node.base, "A");
    // }
    //
    // #[test]
    // fn create_nodes() {
    //     let mut conn = get_connection();
    //     let nodes = models::Node::bulk_create(&mut conn, &vec!["A".to_string(), "T".to_string()]);
    //     assert_eq!(nodes[0].base, "A");
    //     assert_eq!(nodes[1].base, "T");
    // }
    //
    // #[test]
    // fn create_edge() {
    //     let mut conn = get_connection();
    //     let node = models::Node::create(&mut conn, "A".to_string());
    //     let node2 = models::Node::create(&mut conn, "T".to_string());
    //     let edge = models::Edge::create(&mut conn, node.id, node2.id);
    //     assert_eq!(edge.source_id, node.id);
    //     assert_eq!(edge.target_id, node2.id);
    // }
    //
    // #[test]
    // fn create_edges() {
    //     let mut conn = get_connection();
    //     let node = models::Node::create(&mut conn, "A".to_string());
    //     let node2 = models::Node::create(&mut conn, "T".to_string());
    //     let node3 = models::Node::create(&mut conn, "C".to_string());
    //     let edges = models::Edge::bulk_create(&mut conn, &vec![models::Edge{id: 0, source_id: node.id, target_id: node2.id}, models::Edge{id: 0, source_id: node2.id, target_id: node3.id}]);
    //     assert_eq!(edges[0].source_id, node.id);
    //     assert_eq!(edges[0].target_id, node2.id);
    //     assert_eq!(edges[1].source_id, node2.id);
    //     assert_eq!(edges[1].target_id, node3.id);
    // }
    //
    // #[test]
    // fn create_genome() {
    //     let mut conn = get_connection();
    //     let obj = models::Genome::create(&mut conn, "hg19".to_string());
    //     assert_eq!(obj.name, "hg19");
    // }
    //
    // #[test]
    // fn create_genomes() {
    //     let mut conn = get_connection();
    //     let objs = models::Genome::bulk_create(&mut conn, &vec!["hg19".to_string(), "mm9".to_string()]);
    //     assert_eq!(objs[0].name, "hg19");
    //     assert_eq!(objs[1].name, "mm9");
    // }
    //
    // #[test]
    // fn create_contig() {
    //     let mut conn = get_connection();
    //     let genome = models::Genome::create(&mut conn, String::from("hg19"));
    //     let obj_id = models::GenomeContig::create(&mut conn, genome.id, String::from("chr1"), &String::from("atcg"), false);
    //     let contig = models::GenomeContig::get(&mut conn, obj_id);
    //     assert_eq!(contig.name, "chr1");
    // }
    //
    // #[test]
    // fn create_genome_fragment() {
    //     let mut conn = get_connection();
    //     let obj = models::Genome::create(&mut conn, "hg19".to_string());
    //     assert_eq!(obj.name, "hg19");
    // }
    //
    // #[test]
    // fn create_genome_fragments() {
    //     let mut conn = get_connection();
    //     let objs = models::Genome::bulk_create(&mut conn, &vec!["hg19".to_string(), "mm9".to_string()]);
    //     assert_eq!(objs[0].name, "hg19");
    //     assert_eq!(objs[1].name, "mm9");
    // }

    #[test]
    fn it_hases() {
        assert_eq!(
            calculate_hash("a test"),
            "a82639b6f8c3a6e536d8cc562c3b86ff4b012c84ab230c1e5be649aa9ad26d21"
        );
    }

    #[test]
    fn xx() {
        let mut conn = get_connection();
        let obj_id: i32 = conn
            .query_row("SELECT id from sequence where hash = 'foo'", [], |row| {
                row.get(0)
            })
            .unwrap();
        println!("{:?}", obj_id);
    }
}
