use rusqlite::Connection;
use std::io::{IsTerminal, Read, Write};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

use crate::models::metadata;

fn ensure_dir(path: &PathBuf) {
    if !path.is_dir() {
        fs::create_dir_all(path).unwrap();
    }
}

pub fn get_or_create_gen_dir() {
    let start_dir = env::current_dir().unwrap();
    let mut cur_dir = start_dir.as_path();
    let gen_path = cur_dir.join(".gen");
    ensure_dir(&gen_path);
}

// TODO: maybe just store all these things in a sqlite file too in .gen
pub fn get_gen_dir() -> String {
    let start_dir = env::current_dir().unwrap();
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

pub fn get_operation_path(conn: &Connection) -> PathBuf {
    let db_id = metadata::get_db_uuid(conn);
    let path = Path::new(&get_gen_dir()).join(db_id);
    ensure_dir(&path);
    path.join("operation")
}

pub fn get_changeset_path(conn: &Connection) -> PathBuf {
    let db_id = metadata::get_db_uuid(conn);
    let path = Path::new(&get_gen_dir()).join(db_id).join("changeset");
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
