use std::{env, fs};

use rusqlite::Connection;

use crate::migrations::run_migrations;

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

pub fn setup_gen_dir() {
    let cur_dir = env::current_dir().unwrap();
    let mut gen_path = cur_dir.join(".gen");

    if gen_path.is_dir() {
        fs::remove_dir_all(&gen_path).unwrap()
    }
    fs::create_dir(&gen_path).unwrap();
}
