use std::fs;

use rusqlite::Connection;
use tempdir::TempDir;

use crate::config::{get_or_create_gen_dir, BASE_DIR};
use crate::migrations::{run_migrations, run_operation_migrations};

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
