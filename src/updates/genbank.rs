use crate::calculate_hash;
use crate::genbank::{process_sequence, EditType, GenBankError};
use crate::models::block_group::{BlockGroup, PathChange};
use crate::models::block_group_edge::{BlockGroupEdge, BlockGroupEdgeData};
use crate::models::collection::Collection;
use crate::models::edge::Edge;
use crate::models::node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID};
use crate::models::operations::{Operation, OperationInfo};
use crate::models::path::{Path, PathBlock};
use crate::models::sequence::Sequence;
use crate::models::strand::Strand;
use crate::models::traits::Query;
use crate::operation_management::{end_operation, start_operation};
use gb_io::reader;
use rusqlite::{params, types::Value, Connection};
use std::io::Read;
use std::str;

pub fn update_with_genbank<'a, R>(
    conn: &Connection,
    op_conn: &Connection,
    data: R,
    collection: impl Into<Option<&'a str>>,
    create_missing: bool,
    operation_info: OperationInfo,
) -> Result<Operation, GenBankError>
where
    R: Read,
{
    let mut session = start_operation(conn);
    let reader = reader::SeqReader::new(data);
    let collection = Collection::create(conn, collection.into().unwrap_or_default());
    for result in reader {
        match result {
            Ok(seq) => {
                let locus = process_sequence(seq)?;
                let original_sequence = locus.original_sequence();
                let mut seq_model = Sequence::new().sequence(&original_sequence);
                if !locus.name.is_empty() {
                    seq_model = seq_model.name(&locus.name);
                }
                if let Some(ref mol_type) = locus.molecule_type {
                    seq_model = seq_model.sequence_type(mol_type);
                }
                let sequence = seq_model.save(conn);
                let wt_node_id = Node::create(
                    conn,
                    &sequence.hash,
                    calculate_hash(&format!(
                        "{collection}.{contig}:{hash}",
                        contig = &locus.name,
                        collection = &collection.name,
                        hash = sequence.hash
                    )),
                );

                let block_group = if let Ok(bg) = BlockGroup::get(conn, "select * from block_groups where collection_name = ?1 AND sample_name is null AND name = ?2", params![Value::from(collection.name.clone()), Value::from(locus.name.clone())]) {
                    bg
                }
                     else {
                        if !create_missing {
                            return Err(GenBankError::LookupError(format!("No block group named {contig} exists. Try importing first or pass --create-missing.", contig=&locus.name)));
                        }
                        BlockGroup::create(conn, &collection.name, None, &locus.name)
                    };
                let paths = Path::query(
                    conn,
                    "select * from paths where block_group_id = ?1 AND name = ?2",
                    params![Value::from(block_group.id), Value::from(locus.name.clone())],
                );
                let path = if let Some(first) = paths.first() {
                    first.clone()
                } else {
                    if !create_missing {
                        return Err(GenBankError::LookupError(format!("No path named {contig} exists. Try importing first or pass --create-missing.", contig=&locus.name)));
                    }
                    let edge_into = Edge::create(
                        conn,
                        PATH_START_NODE_ID,
                        0,
                        Strand::Forward,
                        wt_node_id,
                        0,
                        Strand::Forward,
                    );
                    let edge_out_of = Edge::create(
                        conn,
                        wt_node_id,
                        sequence.length,
                        Strand::Forward,
                        PATH_END_NODE_ID,
                        0,
                        Strand::Forward,
                    );
                    BlockGroupEdge::bulk_create(
                        conn,
                        &[
                            BlockGroupEdgeData {
                                block_group_id: block_group.id,
                                edge_id: edge_into.id,
                                chromosome_index: 0,
                                phased: 0,
                            },
                            BlockGroupEdgeData {
                                block_group_id: block_group.id,
                                edge_id: edge_out_of.id,
                                chromosome_index: 0,
                                phased: 0,
                            },
                        ],
                    );
                    Path::create(
                        conn,
                        &locus.name,
                        block_group.id,
                        &[edge_into.id, edge_out_of.id],
                    )
                };
                for edit in locus.changes_to_wt() {
                    let start = edit.start;
                    let end = edit.end;
                    let change = match edit.edit_type {
                        EditType::Insertion | EditType::Replacement => {
                            let change_seq = Sequence::new()
                                .sequence(&edit.new_sequence)
                                .name(&format!(
                                    "Geneious type: Editing History {edit_type}",
                                    edit_type = edit.edit_type
                                ))
                                .sequence_type("DNA")
                                .save(conn);
                            let change_node = Node::create(
                                conn,
                                &change_seq.hash,
                                calculate_hash(&format!(
                                    "{parent_hash}:{start}-{end}->{new_hash}",
                                    parent_hash = &sequence.hash,
                                    new_hash = &change_seq.hash,
                                )),
                            );
                            PathChange {
                                block_group_id: block_group.id,
                                path: path.clone(),
                                path_accession: None,
                                start,
                                end,
                                block: PathBlock {
                                    id: 0,
                                    node_id: change_node,
                                    block_sequence: edit.new_sequence.clone(),
                                    sequence_start: 0,
                                    sequence_end: change_seq.length,
                                    path_start: start,
                                    path_end: end + change_seq.length,
                                    strand: Strand::Forward,
                                },
                                chromosome_index: 0,
                                phased: 0,
                            }
                        }
                        EditType::Deletion => PathChange {
                            block_group_id: block_group.id,
                            path: path.clone(),
                            path_accession: None,
                            start,
                            end,
                            block: PathBlock {
                                id: 0,
                                node_id: wt_node_id,
                                block_sequence: "".to_string(),
                                sequence_start: 0,
                                sequence_end: 0,
                                path_start: start,
                                path_end: end,
                                strand: Strand::Forward,
                            },
                            chromosome_index: 0,
                            phased: 0,
                        },
                    };
                    let tree = path.intervaltree(conn);
                    BlockGroup::insert_change(conn, &change, &tree);
                }
            }
            Err(e) => return Err(GenBankError::ParseError(format!("Failed to parse {}", e))),
        }
    }
    let filename = operation_info.file_path.clone();
    end_operation(
        conn,
        op_conn,
        &mut session,
        operation_info,
        &format!("Update with GenBank {filename}",),
        None,
    )
    .map_err(GenBankError::OperationError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::file_types::FileTypes;
    use crate::models::metadata;
    use crate::models::operations::setup_db;
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use noodles::fasta;
    use std::collections::HashSet;
    use std::fs::File;
    use std::io::BufReader;
    use std::path::PathBuf;

    fn get_unmodified_sequence() -> String {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/geneious_genbank/unmodified.fa");
        let mut reader = fasta::io::reader::Builder.build_from_path(path).unwrap();
        let mut records = reader.records();
        let record = records.next().unwrap().unwrap();
        let seq = record.sequence();
        str::from_utf8(seq.as_ref()).unwrap().to_string()
    }

    #[test]
    fn test_error_on_invalid_file() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);
        assert_eq!(
            update_with_genbank(
                conn,
                op_conn,
                BufReader::new("this is not valid".as_bytes()),
                None,
                false,
                OperationInfo {
                    file_path: "".to_string(),
                    file_type: FileTypes::GenBank,
                    description: "test".to_string(),
                }
            ),
            Err(GenBankError::ParseError(
                "Failed to parse Syntax error: Error Tag while parsing [this is not valid]"
                    .to_string()
            ))
        )
    }

    #[test]
    fn test_records_operation() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/geneious_genbank/insertion.gb");
        let file = File::open(&path).unwrap();
        let operation = update_with_genbank(
            conn,
            op_conn,
            BufReader::new(file),
            None,
            true,
            OperationInfo {
                file_path: path.to_str().unwrap().to_string(),
                file_type: FileTypes::GenBank,
                description: "test".to_string(),
            },
        )
        .unwrap();
        assert_eq!(
            Operation::get_by_hash(op_conn, &operation.hash).unwrap(),
            operation
        );
    }

    #[cfg(test)]
    mod geneious_genbanks {
        use super::*;
        use crate::imports::genbank::import_genbank;

        #[test]
        fn test_incorporates_updates() {
            // This tests that we are able to take a genbank that has been further modified
            // and update it, mimicking a workflow of going between gen <-> 3rd party tool <-> gen
            setup_gen_dir();
            let conn = &get_connection(None);
            let db_uuid = metadata::get_db_uuid(conn);
            let op_conn = &get_operation_connection(None);
            setup_db(op_conn, &db_uuid);
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/geneious_genbank/insertion.gb");
            let file = File::open(&path).unwrap();
            let _ = import_genbank(
                conn,
                op_conn,
                BufReader::new(file),
                None,
                None,
                OperationInfo {
                    file_path: "".to_string(),
                    file_type: FileTypes::GenBank,
                    description: "test".to_string(),
                },
            );

            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/geneious_genbank/multiple_insertions_deletions.gb");
            let file = File::open(&path).unwrap();
            let _ = update_with_genbank(
                conn,
                op_conn,
                BufReader::new(file),
                None,
                true,
                OperationInfo {
                    file_path: "".to_string(),
                    file_type: FileTypes::GenBank,
                    description: "test".to_string(),
                },
            );

            let f = reader::parse_file(&path).unwrap();
            let mod_seq = str::from_utf8(&f[0].seq).unwrap().to_string();
            let sequences: HashSet<String> = BlockGroup::get_all_sequences(conn, 1, false)
                .iter()
                .map(|s| s.to_lowercase())
                .collect();
            let unchanged_seq = get_unmodified_sequence();
            assert!(sequences.contains(&mod_seq));
            assert!(sequences.contains(&unchanged_seq));
        }

        #[test]
        fn test_creates_missing_entries() {
            // This tests that we are able to take a genbank that has been further modified
            // and includes new sequences and update it.
            setup_gen_dir();
            let conn = &get_connection(None);
            let db_uuid = metadata::get_db_uuid(conn);
            let op_conn = &get_operation_connection(None);
            setup_db(op_conn, &db_uuid);
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/geneious_genbank/insertion.gb");
            let file = File::open(&path).unwrap();
            let _ = import_genbank(
                conn,
                op_conn,
                BufReader::new(file),
                None,
                None,
                OperationInfo {
                    file_path: "".to_string(),
                    file_type: FileTypes::GenBank,
                    description: "test".to_string(),
                },
            );

            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/geneious_genbank/concat.gb");
            let file = File::open(&path).unwrap();
            let _ = update_with_genbank(
                conn,
                op_conn,
                BufReader::new(file),
                None,
                true,
                OperationInfo {
                    file_path: "".to_string(),
                    file_type: FileTypes::GenBank,
                    description: "test".to_string(),
                },
            );

            let f = reader::parse_file(&path).unwrap();
            let sequences: HashSet<String> = BlockGroup::get_all_sequences(conn, 1, false)
                .iter()
                .map(|s| s.to_lowercase())
                .collect();
            let unchanged_seq = get_unmodified_sequence();
            assert!(sequences.contains(&unchanged_seq));
            let mod_seq = str::from_utf8(&f[0].seq).unwrap().to_string();
            assert!(sequences.contains(&mod_seq));

            // we have a new blockgroup called deletion that uses the same base sequence but
            // has a deletion in it.
            let sequences: HashSet<String> = BlockGroup::get_all_sequences(conn, 2, false)
                .iter()
                .map(|s| s.to_lowercase())
                .collect();
            let mod_seq = str::from_utf8(&f[1].seq).unwrap().to_string();
            assert!(sequences.contains(&unchanged_seq));
            assert!(sequences.contains(&mod_seq));
        }

        #[test]
        fn test_errors_on_missing_locus() {
            // This tests that if a genbank file has sequences we are missing, it's an error. This
            // is an attempt to avoid updating the database with the wrong file.
            setup_gen_dir();
            let conn = &get_connection(None);
            let db_uuid = metadata::get_db_uuid(conn);
            let op_conn = &get_operation_connection(None);
            setup_db(op_conn, &db_uuid);
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/geneious_genbank/insertion.gb");
            let file = File::open(&path).unwrap();
            let _ = import_genbank(
                conn,
                op_conn,
                BufReader::new(file),
                None,
                None,
                OperationInfo {
                    file_path: "".to_string(),
                    file_type: FileTypes::GenBank,
                    description: "test".to_string(),
                },
            );

            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/geneious_genbank/concat.gb");
            let file = File::open(&path).unwrap();
            let op = update_with_genbank(
                conn,
                op_conn,
                BufReader::new(file),
                None,
                false,
                OperationInfo {
                    file_path: "".to_string(),
                    file_type: FileTypes::GenBank,
                    description: "test".to_string(),
                },
            );
            assert!(op.is_err());
            assert_eq!(op, Err(GenBankError::LookupError("No block group named deletion exists. Try importing first or pass --create-missing.".to_string())));
        }
    }
}
