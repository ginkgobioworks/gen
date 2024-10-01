use std::fs;

use rusqlite::Connection;
use tempdir::TempDir;

use crate::config::{get_or_create_gen_dir, BASE_DIR};
use crate::migrations::{run_migrations, run_operation_migrations};
use crate::models::block_group::BlockGroup;
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::collection::Collection;
use crate::models::edge::Edge;
use crate::models::node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID};
use crate::models::path::Path;
use crate::models::sequence::Sequence;
use crate::models::strand::Strand;

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

pub fn setup_block_group(conn: &Connection) -> (i32, Path) {
    let a_seq = Sequence::new()
        .sequence_type("DNA")
        .sequence("AAAAAAAAAA")
        .save(conn);
    let a_node_id = Node::create(conn, a_seq.hash.as_str());
    let t_seq = Sequence::new()
        .sequence_type("DNA")
        .sequence("TTTTTTTTTT")
        .save(conn);
    let t_node_id = Node::create(conn, t_seq.hash.as_str());
    let c_seq = Sequence::new()
        .sequence_type("DNA")
        .sequence("CCCCCCCCCC")
        .save(conn);
    let c_node_id = Node::create(conn, c_seq.hash.as_str());
    let g_seq = Sequence::new()
        .sequence_type("DNA")
        .sequence("GGGGGGGGGG")
        .save(conn);
    let g_node_id = Node::create(conn, g_seq.hash.as_str());
    let _collection = Collection::create(conn, "test");
    let block_group = BlockGroup::create(conn, "test", None, "hg19");
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
