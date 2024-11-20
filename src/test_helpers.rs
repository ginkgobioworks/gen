use intervaltree::IntervalTree;
use petgraph::graphmap::DiGraphMap;
use rusqlite::{types::Value, Connection};
use std::fmt::Debug;
use std::fs;
use std::io::Write;
use std::ops::Add;
use tempdir::TempDir;

use crate::config::{get_or_create_gen_dir, BASE_DIR};
use crate::graph::{GraphEdge, GraphNode};
use crate::migrations::{run_migrations, run_operation_migrations};
use crate::models::block_group::BlockGroup;
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::collection::Collection;
use crate::models::edge::Edge;
use crate::models::file_types::FileTypes;
use crate::models::node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID};
use crate::models::operations::Operation;
use crate::models::path::Path;
use crate::models::sequence::Sequence;
use crate::models::strand::Strand;
use crate::models::traits::*;
use crate::operation_management::{end_operation, start_operation};

pub fn get_connection<'a>(db_path: impl Into<Option<&'a str>>) -> Connection {
    let path: Option<&str> = db_path.into();
    let mut conn;
    if let Some(v) = path {
        if fs::metadata(v).is_ok() {
            fs::remove_file(v).unwrap();
        }
        conn = Connection::open(v).unwrap_or_else(|_| panic!("Error connecting to {}", v));
    } else {
        conn = Connection::open_in_memory()
            .unwrap_or_else(|_| panic!("Error opening in memory test db"));
    }
    rusqlite::vtab::array::load_module(&conn).unwrap();
    run_migrations(&mut conn);
    conn
}

pub fn get_operation_connection<'a>(db_path: impl Into<Option<&'a str>>) -> Connection {
    let path: Option<&str> = db_path.into();
    let mut conn;
    if let Some(v) = path {
        if fs::metadata(v).is_ok() {
            fs::remove_file(v).unwrap();
        }
        conn = Connection::open(v).unwrap_or_else(|_| panic!("Error connecting to {}", v));
    } else {
        conn = Connection::open_in_memory()
            .unwrap_or_else(|_| panic!("Error opening in memory test db"));
    }
    run_operation_migrations(&mut conn);
    conn
}

pub fn setup_gen_dir() {
    let tmp_dir = TempDir::new("gen").unwrap().into_path();
    {
        BASE_DIR.with(|v| {
            let mut writer = v.write().unwrap();
            *writer = tmp_dir;
        });
    }
    get_or_create_gen_dir();
}

pub fn setup_block_group(conn: &Connection) -> (i64, Path) {
    let a_seq = Sequence::new()
        .sequence_type("DNA")
        .sequence("AAAAAAAAAA")
        .save(conn);
    let a_node_id = Node::create(conn, a_seq.hash.as_str(), None);
    let t_seq = Sequence::new()
        .sequence_type("DNA")
        .sequence("TTTTTTTTTT")
        .save(conn);
    let t_node_id = Node::create(conn, t_seq.hash.as_str(), None);
    let c_seq = Sequence::new()
        .sequence_type("DNA")
        .sequence("CCCCCCCCCC")
        .save(conn);
    let c_node_id = Node::create(conn, c_seq.hash.as_str(), None);
    let g_seq = Sequence::new()
        .sequence_type("DNA")
        .sequence("GGGGGGGGGG")
        .save(conn);
    let g_node_id = Node::create(conn, g_seq.hash.as_str(), None);
    let _collection = Collection::create(conn, "test");
    let block_group = BlockGroup::create(conn, "test", None, "chr1");
    let edge0 = Edge::create(
        conn,
        PATH_START_NODE_ID,
        0,
        Strand::Forward,
        a_node_id,
        0,
        Strand::Forward,
        0,
        0,
    );
    let edge1 = Edge::create(
        conn,
        a_node_id,
        10,
        Strand::Forward,
        t_node_id,
        0,
        Strand::Forward,
        0,
        0,
    );
    let edge2 = Edge::create(
        conn,
        t_node_id,
        10,
        Strand::Forward,
        c_node_id,
        0,
        Strand::Forward,
        0,
        0,
    );
    let edge3 = Edge::create(
        conn,
        c_node_id,
        10,
        Strand::Forward,
        g_node_id,
        0,
        Strand::Forward,
        0,
        0,
    );
    let edge4 = Edge::create(
        conn,
        g_node_id,
        10,
        Strand::Forward,
        PATH_END_NODE_ID,
        0,
        Strand::Forward,
        0,
        0,
    );
    BlockGroupEdge::bulk_create(
        conn,
        block_group.id,
        &[edge0.id, edge1.id, edge2.id, edge3.id, edge4.id],
    );
    let path = Path::create(
        conn,
        "chr1",
        block_group.id,
        &[edge0.id, edge1.id, edge2.id, edge3.id, edge4.id],
    );
    (block_group.id, path)
}

pub fn save_graph(graph: &DiGraphMap<GraphNode, GraphEdge>, path: &str) {
    use petgraph::dot::{Config, Dot};
    use std::fs::File;
    let mut file = File::create(path).unwrap();
    let _ = file.write_all(
        format!(
            "{dot:?}",
            dot = Dot::with_attr_getters(
                &graph,
                &[Config::NodeNoLabel, Config::EdgeNoLabel],
                &|_, (_, _, edge_weight)| format!("label = \"{}\"", edge_weight.chromosome_index),
                &|_, (node, _weight)| format!(
                    "label = \"{}[{}-{}]\"",
                    node.node_id, node.sequence_start, node.sequence_end
                ),
            )
        )
        .as_bytes(),
    );
}

pub fn interval_tree_verify<K, V>(tree: &IntervalTree<K, V>, i: K, expected: &[V])
where
    K: Ord + Add<i64, Output = K> + Copy,
    V: Copy + Ord + Debug,
{
    let mut v1: Vec<_> = tree.query_point(i).map(|x| x.value).collect();
    v1.sort();
    let mut v2: Vec<_> = tree.query(i..(i + 1)).map(|x| x.value).collect();
    v2.sort();
    assert_eq!(v1, expected);
    assert_eq!(v2, expected);
}

pub fn get_sample_bg<'a>(conn: &Connection, sample_name: impl Into<Option<&'a str>>) -> BlockGroup {
    let sample_name = sample_name.into();
    let mut results;
    if let Some(name) = sample_name {
        let query = "select * from block_groups where sample_name = ?1";
        let params = rusqlite::params!(Value::from(name.to_string()));
        results = BlockGroup::query(conn, query, params);
    } else {
        let query = "select * from block_groups where sample_name is null";
        results = BlockGroup::query(conn, query, rusqlite::params!());
    }
    results.pop().unwrap()
}

fn create_operation<'a>(
        conn: &Connection,
        op_conn: &Connection,
        file_path: &str,
        file_type: FileTypes,
        description: &str,
        hash: impl Into<Option<&'a str>>,
    ) -> Operation {
        let mut session = start_operation(conn);
        end_operation(
            conn,
            op_conn,
            &mut session,
            None,
            file_path,
            file_type,
            description,
            "test operation",
            hash.into(),
        )
        .unwrap()
    }