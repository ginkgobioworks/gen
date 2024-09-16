use crate::migrations::run_operation_migrations;
use crate::models::operations::Operation;
use rusqlite::Connection;
use std::io::{IsTerminal, Read, Write};
use std::string::ToString;
use std::sync::RwLock;
use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::LazyLock,
};

thread_local! {
pub static BASE_DIR: LazyLock<RwLock<PathBuf>> =
    LazyLock::new(|| RwLock::new(env::current_dir().unwrap()));
}

pub fn get_operation_connection() -> Connection {
    let db_path = get_gen_db_path();
    let mut conn =
        Connection::open(&db_path).unwrap_or_else(|_| panic!("Error connecting to {:?}", &db_path));
    run_operation_migrations(&mut conn);
    conn
}

fn ensure_dir(path: &PathBuf) {
    if !path.is_dir() {
        fs::create_dir_all(path).unwrap();
    }
}

pub fn get_or_create_gen_dir() -> PathBuf {
    let start_dir = BASE_DIR.with(|v| v.read().unwrap().clone());
    let mut cur_dir = start_dir.as_path();
    let gen_path = cur_dir.join(".gen");
    ensure_dir(&gen_path);
    gen_path
}

// TODO: maybe just store all these things in a sqlite file too in .gen
pub fn get_gen_dir() -> String {
    let start_dir = BASE_DIR.with(|v| v.read().unwrap().clone());
    let mut cur_dir = start_dir.as_path();
    let mut gen_path = cur_dir.join(".gen");
    while !gen_path.is_dir() {
        match cur_dir.parent() {
            Some(v) => {
                cur_dir = v;
            }
            None => {
                // TOOD: make gen init
                panic!("No .gen directory found. Run gen init in project root directory to initialize gen.");
            }
        };
        gen_path = cur_dir.join(".gen");
    }
    return gen_path.to_str().unwrap().to_string();
}

pub fn get_gen_db_path() -> PathBuf {
    Path::new(&get_gen_dir()).join("gen.db")
}

pub fn get_changeset_path(operation: &Operation) -> PathBuf {
    let path = Path::new(&get_gen_dir())
        .join(operation.db_uuid.clone())
        .join("changeset");
    ensure_dir(&path);
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::setup_gen_dir;

    #[test]
    fn test_finds_gen_dir() {
        setup_gen_dir();
        assert!(!get_gen_dir().is_empty());
    }
}
