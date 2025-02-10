use crate::calculate_hash;
use crate::models::operations::OperationFile;
use crate::models::{
    block_group::BlockGroup,
    block_group_edge::{BlockGroupEdge, BlockGroupEdgeData},
    collection::Collection,
    edge::{Edge, EdgeData},
    file_types::FileTypes,
    node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID},
    operations::OperationInfo,
    path::Path,
    sample::Sample,
    sequence::Sequence,
    strand::Strand,
};
use crate::operation_management;
use itertools::Itertools;
use noodles::fasta;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::str;

pub fn import_library<'a>(
    conn: &Connection,
    operation_conn: &Connection,
    collection_name: &str,
    sample: impl Into<Option<&'a str>>,
    parts_file_path: &str,
    library_file_path: &str,
    region_name: &str,
) -> std::io::Result<()> {
    let mut session = operation_management::start_operation(conn);

    if !Collection::exists(conn, collection_name) {
        Collection::create(conn, collection_name);
    }

    let sample = sample.into();
    if let Some(sample_name) = sample {
        Sample::get_or_create(conn, sample_name);
    }
    let new_block_group = BlockGroup::create(conn, collection_name, sample, region_name);

    let mut parts_reader = fasta::io::reader::Builder.build_from_path(parts_file_path)?;

    let mut node_ids_by_name = HashMap::new();
    let mut sequence_lengths_by_node_id = HashMap::new();
    for result in parts_reader.records() {
        let record = result?;
        let sequence = str::from_utf8(record.sequence().as_ref())
            .unwrap()
            .to_string();
        let name = String::from_utf8(record.name().to_vec()).unwrap();
        let seq = Sequence::new()
            .sequence_type("DNA")
            .sequence(&sequence)
            .save(conn);
        let node_id = Node::create(
            conn,
            &seq.hash,
            calculate_hash(&format!(
                "{region_name}:{name}:{ref_start}-{ref_end}->{sequence_hash}",
                ref_start = 0,
                ref_end = seq.length,
                sequence_hash = seq.hash
            )),
        );

        node_ids_by_name.insert(name, node_id);
        sequence_lengths_by_node_id.insert(node_id, seq.length);
    }

    let library_file = File::open(library_file_path)?;
    let library_reader = BufReader::new(library_file);

    let mut parts_by_index = HashMap::new();
    let mut library_csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(library_reader);
    let mut max_index = 0;
    for result in library_csv_reader.records() {
        let record = result?;
        for (index, part) in record.iter().enumerate() {
            if !part.is_empty() {
                let part_id = node_ids_by_name.get(part).unwrap();
                parts_by_index.entry(index).or_insert(vec![]).push(part_id);
                if index >= max_index {
                    max_index = index + 1;
                }
            }
        }
    }

    let mut parts_list = vec![];
    for index in 0..max_index {
        parts_list.push(parts_by_index.get(&index).unwrap());
    }

    let mut new_edges = HashSet::new();
    let start_parts = parts_list.first().unwrap();
    for start_part in *start_parts {
        let edge = EdgeData {
            source_node_id: PATH_START_NODE_ID,
            source_coordinate: 0,
            source_strand: Strand::Forward,
            target_node_id: **start_part,
            target_coordinate: 0,
            target_strand: Strand::Forward,
        };
        new_edges.insert(edge);
    }

    let end_parts = parts_list.last().unwrap();
    for end_part in *end_parts {
        let end_part_source_coordinate = sequence_lengths_by_node_id.get(end_part).unwrap();
        let edge = EdgeData {
            source_node_id: **end_part,
            source_coordinate: *end_part_source_coordinate,
            source_strand: Strand::Forward,
            target_node_id: PATH_END_NODE_ID,
            target_coordinate: 0,
            target_strand: Strand::Forward,
        };
        new_edges.insert(edge);
    }

    let mut path_changes_count = 1;
    for (parts1, parts2) in parts_list.iter().tuple_windows() {
        path_changes_count *= parts1.len();
        for part1 in *parts1 {
            for part2 in *parts2 {
                let part1_source_coordinate = sequence_lengths_by_node_id.get(part1).unwrap();
                let edge = EdgeData {
                    source_node_id: **part1,
                    source_coordinate: *part1_source_coordinate,
                    source_strand: Strand::Forward,
                    target_node_id: **part2,
                    target_coordinate: 0,
                    target_strand: Strand::Forward,
                };
                new_edges.insert(edge);
            }
        }
    }

    path_changes_count *= end_parts.len();

    let new_edge_ids = Edge::bulk_create(conn, &new_edges.iter().cloned().collect());

    let new_block_group_edges = new_edge_ids
        .iter()
        .map(|edge_id| BlockGroupEdgeData {
            block_group_id: new_block_group.id,
            edge_id: *edge_id,
            chromosome_index: *edge_id, // TODO: This is a hack, clean it up with phase layers
            phased: 0,
            source_phase_layer_id: 0,
            target_phase_layer_id: 0,
        })
        .collect::<Vec<_>>();
    BlockGroupEdge::bulk_create(conn, &new_block_group_edges);

    let mut path_node_ids = vec![];
    path_node_ids.push(PATH_START_NODE_ID);
    for parts in &parts_list {
        path_node_ids.push(*parts[0]);
    }
    path_node_ids.push(PATH_END_NODE_ID);

    let new_edges = BlockGroupEdge::edges_for_block_group(conn, new_block_group.id);
    let new_edge_ids_by_source_and_target_node = new_edges
        .iter()
        .map(|edge| {
            (
                (edge.edge.source_node_id, edge.edge.target_node_id),
                edge.block_group_edge_id,
            )
        })
        .collect::<HashMap<_, _>>();
    let path_edge_ids = path_node_ids
        .iter()
        .tuple_windows()
        .map(|(source_node_id, target_node_id)| {
            *new_edge_ids_by_source_and_target_node
                .get(&(*source_node_id, *target_node_id))
                .unwrap()
        })
        .collect::<Vec<_>>();
    Path::create(
        conn,
        format!("{} default path", region_name).as_str(),
        new_block_group.id,
        &path_edge_ids,
    );

    let summary_str = format!("{region_name}: {path_changes_count} changes.\n");
    operation_management::end_operation(
        conn,
        operation_conn,
        &mut session,
        &OperationInfo {
            files: vec![OperationFile {
                file_path: library_file_path.to_string(),
                file_type: FileTypes::CSV,
            }],
            description: "library_csv_import".to_string(),
        },
        &summary_str,
        None,
    )
    .unwrap();

    println!(
        "Imported library file {} and parts file {}",
        library_file_path, parts_file_path
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{block_group::BlockGroup, metadata, operations::setup_db};
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use std::path::PathBuf;

    #[test]
    fn imports_a_library() {
        setup_gen_dir();
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);
        let collection = "test";

        let mut parts_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        parts_path.push("fixtures/affix_parts.fa");
        let mut library_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        library_path.push("fixtures/affix_layout.csv");

        let _ = import_library(
            conn,
            op_conn,
            collection,
            None,
            parts_path.to_str().unwrap(),
            library_path.to_str().unwrap(),
            "library graph",
        );

        let block_groups = Sample::get_block_groups(conn, collection, None);
        let block_group = &block_groups[0];

        let mut expected_sequences = HashSet::new();
        for part1 in &[
            "TCTAGAGAAAGAGGGGACAAACTAG",
            "TCTAGAGAAAGACAGGACCCACTAG",
            "TCTAGAGAAAGATCCGATGTACTAG",
            "TCTAGAGAAAGATTAGACAAACTAG",
            "TCTAGAGAAAGAAGGGACAGACTAG",
            "TCTAGAGAAAGACATGACGTACTAG",
            "TCTAGAGAAAGATAGGAGACACTAG",
            "TCTAGAGAAAGAAGAGACTCACTAG",
        ] {
            for part2 in &["ATGCGTAAAGGAGAAGAACTTTAA", "ATGAGTAAGGGTGAAGAGCTGTAA"] {
                expected_sequences.insert(format!("{}{}", part1, part2));
            }
        }

        let actual_sequences = BlockGroup::get_all_sequences(conn, block_group.id, false);
        assert_eq!(actual_sequences, expected_sequences);

        let current_path = BlockGroup::get_current_path(conn, block_group.id);
        assert_eq!(
            current_path.sequence(conn),
            "TCTAGAGAAAGAGGGGACAAACTAGATGCGTAAAGGAGAAGAACTTTAA"
        );
    }
}
