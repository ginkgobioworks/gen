use std::io::{IsTerminal, Read, Write};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

// TODO: maybe just store all these things in a sqlite file too in .gen
pub fn get_gen_dir() -> Option<String> {
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
    return Some(gen_path.to_str().unwrap().to_string());
}

fn get_operation_path() -> PathBuf {
    Path::new(&get_gen_dir().unwrap()).join("operation")
}
pub fn read_operation_file() -> fs::File {
    let operation_path = get_operation_path();
    let mut file;
    if fs::metadata(&operation_path).is_ok() {
        file = fs::File::open(operation_path);
    } else {
        file = fs::File::create_new(operation_path);
    }
    file.unwrap()
}

pub fn write_operation_file() -> fs::File {
    let operation_path = get_operation_path();
    fs::File::create(operation_path).unwrap()
}

pub fn get_operation() -> Option<i32> {
    let mut file = read_operation_file();
    let mut contents: String = "".to_string();
    file.read_to_string(&mut contents).unwrap();
    match contents.parse::<i32>().unwrap_or(0) {
        0 => None,
        v => Some(v),
    }
}

pub fn set_operation(op_id: i32) {
    let mut file = write_operation_file();
    file.write_all(&format!("{op_id}").into_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_gen_dir() {
        let cur_dir = env::current_dir().unwrap();
        let mut gen_path = cur_dir.join(".gen");

        if !gen_path.is_dir() {
            fs::create_dir(gen_path).unwrap();
        }
    }

    #[test]
    fn find_gen_dir() {
        get_gen_dir();
    }

    #[test]
    fn test_writes_operation_id() {
        setup_gen_dir();
        set_operation(1);
        assert_eq!(get_operation().unwrap(), 1);
    }
}
