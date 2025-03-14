use intervaltree::IntervalTree;
use petgraph::graphmap::DiGraphMap;
use rusqlite::Connection;
use std::collections::HashMap;
use std::env;
use std::fmt::Debug;
use std::fs;
use std::hash::Hash;
use std::io::Write;
use std::ops::Add;
use std::path::PathBuf;
use tempfile::tempdir;

use crate::config::{get_or_create_gen_dir, BASE_DIR};
use crate::graph::{GraphEdge, GraphNode};
use crate::migrations::{run_migrations, run_operation_migrations};
use crate::models::block_group::BlockGroup;
use crate::models::block_group_edge::{BlockGroupEdge, BlockGroupEdgeData};
use crate::models::collection::Collection;
use crate::models::edge::Edge;
use crate::models::file_types::FileTypes;
use crate::models::node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID};
use crate::models::operations::{Operation, OperationFile, OperationInfo};
use crate::models::path::Path;
use crate::models::sample::Sample;
use crate::models::sequence::Sequence;
use crate::models::strand::Strand;
use crate::operation_management::{end_operation, start_operation};

/// Activates the .venv virtual environment for Unix-like systems
#[cfg(all(not(windows), feature = "python-bindings"))]
pub fn activate_venv() -> Result<(), String> {
    // Check if .venv directory exists
    let project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let venv_path = project_root.join(".venv");
    if !venv_path.exists() || !venv_path.is_dir() {
        return Err("Virtual environment directory .venv not found".to_string());
    }

    // Set VIRTUAL_ENV environment variable
    let venv_str = venv_path
        .to_str()
        .ok_or_else(|| "Failed to convert venv path to string".to_string())?;
    env::set_var("VIRTUAL_ENV", venv_str);

    // Add the venv bin directory to the PATH
    let current_path = env::var("PATH").unwrap_or_default();
    let bin_dir = venv_path.join("bin");
    if !bin_dir.exists() {
        return Err(format!("bin directory not found at: {:?}", bin_dir));
    }
    let bin_str = bin_dir.to_str().unwrap();
    let new_path = format!("{}:{}", bin_str, current_path);
    env::set_var("PATH", new_path);

    // Unset PYTHONHOME if it exists, it can interfere with the virtual environment
    env::remove_var("PYTHONHOME");

    Ok(())
}

/// Activates the .venv virtual environment for Windows
#[cfg(all(windows, feature = "python-bindings"))]
pub fn activate_venv() -> Result<(), String> {
    // Check if .venv directory exists
    let project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let venv_path = project_root.join(".venv");
    if !venv_path.exists() || !venv_path.is_dir() {
        return Err("Virtual environment directory .venv not found".to_string());
    }

    // Set VIRTUAL_ENV environment variable
    let venv_str = venv_path
        .to_str()
        .ok_or_else(|| "Failed to convert venv path to string".to_string())?;
    env::set_var("VIRTUAL_ENV", venv_str);

    // On Windows, executables are in the Scripts directory
    let scripts_dir = venv_path.join("Scripts");
    if !scripts_dir.exists() {
        return Err(format!("Scripts directory not found at: {:?}", scripts_dir));
    }

    // Get current Path (Windows uses "Path" by convention)
    let current_path = env::var("Path").unwrap_or_default();

    // Convert scripts_dir to string
    let scripts_str = scripts_dir
        .to_str()
        .ok_or_else(|| "Failed to convert Scripts path to string".to_string())?;

    // Create new Path with scripts directory at the front
    let new_path = format!("{};{}", scripts_str, current_path);
    env::set_var("Path", new_path);

    // Unset PYTHONHOME if it exists
    env::remove_var("PYTHONHOME");

    Ok(())
}

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
    let tmp_dir = tempdir().unwrap().into_path();
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
    let a_node_id = Node::create(
        conn,
        a_seq.hash.as_str(),
        format!("test-a-node.{}", a_seq.hash),
    );
    let t_seq = Sequence::new()
        .sequence_type("DNA")
        .sequence("TTTTTTTTTT")
        .save(conn);
    let t_node_id = Node::create(
        conn,
        t_seq.hash.as_str(),
        format!("test-t-node.{}", a_seq.hash),
    );
    let c_seq = Sequence::new()
        .sequence_type("DNA")
        .sequence("CCCCCCCCCC")
        .save(conn);
    let c_node_id = Node::create(
        conn,
        c_seq.hash.as_str(),
        format!("test-c-node.{}", a_seq.hash),
    );
    let g_seq = Sequence::new()
        .sequence_type("DNA")
        .sequence("GGGGGGGGGG")
        .save(conn);
    let g_node_id = Node::create(
        conn,
        g_seq.hash.as_str(),
        format!("test-g-node.{}", a_seq.hash),
    );
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
    );
    let edge1 = Edge::create(
        conn,
        a_node_id,
        10,
        Strand::Forward,
        t_node_id,
        0,
        Strand::Forward,
    );
    let edge2 = Edge::create(
        conn,
        t_node_id,
        10,
        Strand::Forward,
        c_node_id,
        0,
        Strand::Forward,
    );
    let edge3 = Edge::create(
        conn,
        c_node_id,
        10,
        Strand::Forward,
        g_node_id,
        0,
        Strand::Forward,
    );
    let edge4 = Edge::create(
        conn,
        g_node_id,
        10,
        Strand::Forward,
        PATH_END_NODE_ID,
        0,
        Strand::Forward,
    );

    let block_group_edges = vec![
        BlockGroupEdgeData {
            block_group_id: block_group.id,
            edge_id: edge0.id,
            chromosome_index: 0,
            phased: 0,
        },
        BlockGroupEdgeData {
            block_group_id: block_group.id,
            edge_id: edge1.id,
            chromosome_index: 0,
            phased: 0,
        },
        BlockGroupEdgeData {
            block_group_id: block_group.id,
            edge_id: edge2.id,
            chromosome_index: 0,
            phased: 0,
        },
        BlockGroupEdgeData {
            block_group_id: block_group.id,
            edge_id: edge3.id,
            chromosome_index: 0,
            phased: 0,
        },
        BlockGroupEdgeData {
            block_group_id: block_group.id,
            edge_id: edge4.id,
            chromosome_index: 0,
            phased: 0,
        },
    ];
    BlockGroupEdge::bulk_create(conn, &block_group_edges);

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

pub fn get_sample_bg<'a>(
    conn: &Connection,
    collection_name: &str,
    sample_name: impl Into<Option<&'a str>>,
) -> BlockGroup {
    let sample_name = sample_name.into();
    let mut results = Sample::get_block_groups(conn, collection_name, sample_name);
    results.pop().unwrap()
}

pub fn create_operation<'a>(
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
        &OperationInfo {
            files: vec![OperationFile {
                file_path: file_path.to_string(),
                file_type,
            }],
            description: description.to_string(),
        },
        "test operation",
        hash.into(),
    )
    .unwrap()
}

pub fn keys_match<T: Eq + Hash, U, V>(map1: &HashMap<T, U>, map2: &HashMap<T, V>) -> bool {
    map1.len() == map2.len() && map1.keys().all(|k| map2.contains_key(k))
}
