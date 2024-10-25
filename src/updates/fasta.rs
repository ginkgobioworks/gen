use noodles::fasta;
use rusqlite::{types::Value as SQLValue, Connection};
use std::collections::HashMap;
use std::{io, str};

use crate::models::{
    block_group::{BlockGroup, PathCache, PathChange},
    edge::Edge,
    file_types::FileTypes,
    node::Node,
    operation_path::OperationPath,
    path::{Path, PathBlock},
    sequence::Sequence,
    strand::Strand,
};
use crate::{calculate_hash, operation_management};

pub fn update_with_fasta(
    conn: &Connection,
    operation_conn: &Connection,
    name: &str,
    path_name: &str,
    start_coordinate: i64,
    end_coordinate: i64,
    fasta_file_path: &str,
) -> io::Result<()> {
    let (mut session, operation) = operation_management::start_operation(
        conn,
        operation_conn,
        fasta_file_path,
        FileTypes::Fasta,
        "fasta_update",
        name,
    );

    let mut fasta_reader = fasta::io::reader::Builder.build_from_path(fasta_file_path)?;

    let path = Path::get_paths(
        conn,
        "select * from path where name = ?1",
        vec![SQLValue::from(path_name.to_string())],
    )[0]
    .clone();

    // Assuming just one entry in the fasta file
    let record = fasta_reader.records().next().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "No records found in fasta file")
    })??;

    let sequence = str::from_utf8(record.sequence().as_ref())
        .unwrap()
        .to_string();
    let seq = Sequence::new()
        .sequence_type("DNA")
        .sequence(&sequence)
        .save(conn);
    let node_id = Node::create(
        conn,
        &seq.hash,
        calculate_hash(&format!(
            "{path_id}:{ref_start}-{ref_end}->{sequence_hash}",
            path_id = path.id,
            ref_start = 0,
            ref_end = seq.length,
            sequence_hash = seq.hash
        )),
    );

    let path_block = PathBlock {
        id: -1,
        node_id,
        block_sequence: sequence,
        sequence_start: 0,
        sequence_end: seq.length,
        path_start: start_coordinate,
        path_end: end_coordinate,
        strand: Strand::Forward,
    };

    let path_change = PathChange {
        block_group_id: path.block_group_id,
        path: path.clone(),
        start: start_coordinate,
        end: end_coordinate,
        block: path_block,
        chromosome_index: 0,
        phased: 0,
    };

    let path_cache = PathCache {
        cache: HashMap::new(),
        intervaltree_cache: HashMap::new(),
        conn,
    };

    BlockGroup::insert_changes(conn, &vec![path_change], &path_cache);

    let edge_to_new_node = Edge::query(
        conn,
        "select * from edge where target_node_id = ?1",
        vec![SQLValue::from(node_id)],
    )[0]
    .clone();
    let edge_from_new_node = Edge::query(
        conn,
        "select * from edge where source_node_id = ?1",
        vec![SQLValue::from(node_id)],
    )[0]
    .clone();
    let new_path = path.new_path_for(
        conn,
        start_coordinate,
        end_coordinate,
        &edge_to_new_node,
        &edge_from_new_node,
    );

    // let operation_paths = OperationPath::paths_for_operation(conn, operation.id);
    // for operation_path in operation_paths {
    // 	if operation_path.path_id != path.id {
    // 	    OperationPath::create(conn, operation.id, operation_path.path_id);
    // 	}
    // }
    // OperationPath::create(conn, operation.id, new_path.id);

    let summary_str = format!(" {}: 1 change", new_path.name);
    operation_management::end_operation(
        conn,
        operation_conn,
        &operation,
        &mut session,
        &summary_str,
    );

    println!("Updated with fasta file: {}", fasta_file_path);

    Ok(())
}
