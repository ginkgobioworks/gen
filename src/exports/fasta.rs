use noodles::fasta;
use rusqlite::{types::Value as SQLValue, Connection};
use std::fs::File;
use std::io;
use std::path::PathBuf;

use crate::models::{block_group::BlockGroup, operation_path::OperationPath, path::Path};

pub fn export_fasta(conn: &Connection, operation_id: i64, filename: &PathBuf) {
    let operation_paths = OperationPath::paths_for_operation(conn, operation_id);
    for operation_path in operation_paths {
        let path = Path::get(conn, operation_path.path_id);
        let block_group = BlockGroup::get_by_id(conn, path.block_group_id);

        let file = File::create(filename).unwrap();
        let mut writer = fasta::io::Writer::new(file);

        let definition = fasta::record::Definition::new(block_group.name, None);
        let sequence = fasta::record::Sequence::from(Path::sequence(conn, path).into_bytes());
        let record = fasta::Record::new(definition, sequence);

        let _ = writer.write_record(&record);
    }
}
