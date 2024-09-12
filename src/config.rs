use std::io::{IsTerminal, Read, Write};
use std::{
    env,
    path::{Path, PathBuf},
};

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

// TODO: make a random database uuid each time a db is made to make operations unique to a given db
pub fn get_operation_path() -> PathBuf {
    Path::new(&get_gen_dir()).join("operation")
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
