use crate::models::block_group::{BlockGroup, PathChange};
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::collection::Collection;
use crate::models::edge::Edge;
use crate::models::node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID};
use crate::models::path::{Path, PathBlock};
use crate::models::sequence::Sequence;
use crate::models::strand::Strand;
use crate::{calculate_hash, normalize_string};
use gb_io::{reader, seq::Location};
use regex::Regex;
use rusqlite::Connection;
use std::io::Read;
use std::str;

pub fn import_genbank<'a, R>(conn: &Connection, data: R, collection: impl Into<Option<&'a str>>)
where
    R: Read,
{
    let reader = reader::SeqReader::new(data);
    let collection = Collection::create(conn, collection.into().unwrap_or_default());
    let geneious_edit = Regex::new(r"Geneious type: Editing History (?P<edit_type>\w+)").unwrap();
    for result in reader {
        match result {
            Ok(seq) => {
                let mut seq_model = Sequence::new();
                let contig = &seq.name.unwrap_or_default();
                if !contig.is_empty() {
                    seq_model = seq_model.name(contig);
                }
                if let Ok(sequence) = str::from_utf8(&seq.seq) {
                    seq_model = seq_model.sequence(sequence);
                }
                if let Some(mol_type) = &seq.molecule_type {
                    seq_model = seq_model.sequence_type(mol_type);
                }
                let sequence = seq_model.save(conn);
                let node_id = Node::create(
                    conn,
                    &sequence.hash,
                    calculate_hash(&format!(
                        "{collection}.{contig}:{hash}",
                        collection = &collection.name,
                        hash = sequence.hash
                    )),
                );
                let block_group = BlockGroup::create(conn, &collection.name, None, contig);
                let edge_into = Edge::create(
                    conn,
                    PATH_START_NODE_ID,
                    0,
                    Strand::Forward,
                    node_id,
                    0,
                    Strand::Forward,
                    0,
                    0,
                );
                let edge_out_of = Edge::create(
                    conn,
                    node_id,
                    sequence.length,
                    Strand::Forward,
                    PATH_END_NODE_ID,
                    0,
                    Strand::Forward,
                    0,
                    0,
                );
                BlockGroupEdge::bulk_create(conn, block_group.id, &[edge_into.id, edge_out_of.id]);
                let path = Path::create(
                    conn,
                    contig,
                    block_group.id,
                    &[edge_into.id, edge_out_of.id],
                );

                for feature in seq.features.iter() {
                    for (key, value) in feature.qualifiers.iter() {
                        if key == "note" {
                            if let Some(v) = value {
                                let geneious_mod = geneious_edit.captures(v);
                                if let Some(edit) = geneious_mod {
                                    let (mut start, mut end) =
                                        feature.location.find_bounds().unwrap();
                                    match &edit["edit_type"] {
                                        "Insertion" => {
                                            // If there is an insertion, it means that the WT is missing
                                            // this sequence, so we actually treat it as a deletion
                                            let change_seq = Sequence::new()
                                                .sequence("")
                                                .name(v)
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
                                            let change = PathChange {
                                                block_group_id: block_group.id,
                                                path: path.clone(),
                                                path_accession: Some(v.clone()),
                                                start,
                                                end,
                                                block: PathBlock {
                                                    id: 0,
                                                    node_id: change_node,
                                                    block_sequence: "".to_string(),
                                                    sequence_start: 0,
                                                    sequence_end: 0,
                                                    path_start: start,
                                                    path_end: end,
                                                    strand: Strand::Forward,
                                                },
                                                chromosome_index: 0,
                                                phased: 0,
                                            };
                                            let tree = path.intervaltree(conn);
                                            BlockGroup::insert_change(conn, &change, &tree);
                                        }
                                        "Deletion" | "Replacement" => {
                                            // If there is a deletion, it means that found sequence is missing
                                            // this sequence, so we treat it as an insertion
                                            let deleted_seq = normalize_string(
                                                &feature
                                                    .qualifiers
                                                    .iter()
                                                    .filter(|(k, _v)| k == "Original_Bases")
                                                    .map(|(_k, v)| v.clone())
                                                    .collect::<Option<String>>()
                                                    .expect("Deleted sequence is not annotated."),
                                            );
                                            let del_len = deleted_seq.len() as i64;
                                            let change_seq = Sequence::new()
                                                .sequence(&deleted_seq)
                                                .name(v)
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
                                            if matches!(feature.location, Location::Between(_, _)) {
                                                start += 1;
                                                end -= 1;
                                            }
                                            let change = PathChange {
                                                block_group_id: block_group.id,
                                                path: path.clone(),
                                                path_accession: Some(v.clone()),
                                                start,
                                                end,
                                                block: PathBlock {
                                                    id: 0,
                                                    node_id: change_node,
                                                    block_sequence: deleted_seq,
                                                    sequence_start: 0,
                                                    sequence_end: del_len,
                                                    path_start: start,
                                                    path_end: end,
                                                    strand: Strand::Forward,
                                                },
                                                chromosome_index: 0,
                                                phased: 0,
                                            };
                                            let tree = path.intervaltree(conn);
                                            BlockGroup::insert_change(conn, &change, &tree);
                                        }
                                        t => {
                                            println!("Unknown edit type {t}.")
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => println!("Failed to parse {e:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::metadata;
    use crate::models::operations::setup_db;
    use crate::test_helpers::{get_connection, get_operation_connection};
    use std::collections::HashSet;
    use std::fs::File;
    use std::io::BufReader;
    use std::path::PathBuf;

    #[cfg(test)]
    mod geneious_genbanks {
        use super::*;
        #[test]
        fn test_parses_insertion() {
            // this file has an insertion from 1426-2220
            let conn = &get_connection(None);
            let db_uuid = metadata::get_db_uuid(conn);
            let op_conn = &get_operation_connection(None);
            setup_db(op_conn, &db_uuid);
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/geneious_genbank/insertion.gb");
            let file = File::open(&path).unwrap();
            import_genbank(conn, BufReader::new(file), None);
            let f = reader::parse_file(&path).unwrap();
            let seq = str::from_utf8(&f[0].seq).unwrap().to_string();
            let seqs = BlockGroup::get_all_sequences(conn, 1, false);
            assert_eq!(
                seqs,
                HashSet::from_iter([
                    seq.clone(),
                    format!("{}{}", &seq[..1425].to_string(), &seq[2220..].to_string()).to_string()
                ])
            );
        }

        #[test]
        fn test_parses_deletion() {
            // this file has a deletion from 765-766
            let conn = &get_connection(None);
            let db_uuid = metadata::get_db_uuid(conn);
            let op_conn = &get_operation_connection(None);
            setup_db(op_conn, &db_uuid);
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/geneious_genbank/deletion.gb");
            let file = File::open(&path).unwrap();
            import_genbank(conn, BufReader::new(file), None);
            let f = reader::parse_file(&path).unwrap();
            let seq = str::from_utf8(&f[0].seq).unwrap().to_string();
            let deleted: String = normalize_string(
                "TTACGCCCCGCCCTGCCACTCATCGCAGTACTGTTGTAATT
        CATTAAGCATTCTGCCGACATGGAAGCCATCACAAACGGCATGATGAACCTGAATCGCCAGCG
        GCATCAGCACCTTGTCGCCTTGCGTATAATATTTGCCCATGGTGAAAACGGGGGCGAAGAAGT
        TGTCCATATTGGCCACGTTTAAATCAAAACTGGTGAAACTCACCCAGGGATTGGCTGAGACGA
        AAAACATATTCTCAATAAACCCTTTAGGGAAATAGGCCAGGTTTTCACCGTAACACGCCACAT
        CTTGCGAATATATGTGTAGAAACTGCCGGAAATCGTCGTGGTATTCACTCCAGAGCGATGAAA
        ACGTTTCAGTTTGCTCATGGAAAACGGTGTAACAAGGGTGAACACTATCCCATATCACCAGCT
        CACCGTCTTTCATTGCCATACGGAATTCCGGATGAGCATTCATCAGGCGGGCAAGAATGTGAA
        TAAAGGCCGGATAAAACTTGTGCTTATTTTTCTTTACGGTCTTTAAAAAGGCCGTAATATCCA
        GCTGAACGGTCTGGTTATAGGTACATTGAGCAACTGACTGAAATGCCTCAAAATGTTCTTTAC
        GATGCCATTGGGATATATCAACGGTGGTATATCCAGTGATTTTTTTCTCCAT",
            );
            let seqs = BlockGroup::get_all_sequences(conn, 1, false);
            assert_eq!(
                seqs,
                HashSet::from_iter([
                    seq.clone(),
                    format!(
                        "{}{deleted}{}",
                        &seq[..765].to_string(),
                        &seq[765..].to_string()
                    )
                    .to_string()
                ])
            );
        }

        #[test]
        fn test_parses_deletion_and_insertion() {
            let conn = &get_connection(None);
            let db_uuid = metadata::get_db_uuid(conn);
            let op_conn = &get_operation_connection(None);
            setup_db(op_conn, &db_uuid);
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/geneious_genbank/deletion_and_insertion.gb");
            let file = File::open(&path).unwrap();
            import_genbank(conn, BufReader::new(file), None);
            let f = reader::parse_file(&path).unwrap();
            let seq = str::from_utf8(&f[0].seq).unwrap().to_string();
            let deleted: String = normalize_string(
                "TACGCCCCGCCCTGCCACTCATCGCAGTACTGTTGTAATTC
             ATTAAGCATTCTGCCGACATGGAAGCCATCACAAACGGCATGATGAACCTGAATCGCC
             AGCGGCATCAGCACCTTGTCGCCTTGCGTATAATATTTGCCCATGGTGAAAACGGGGG
             CGAAGAAGTTGTCCATATTGGCCACGTTTAAATCAAAACTGGTGAAACTCACCCAGGG
             ATTGGCTGAGACGAAAAACATATTCTCAATAAACCCTTTAGGGAAATAGGCCAGGTTT
             TCACCGTAACACGCCACATCTTGCGAATATATGTGTAGAAACTGCCGGAAATCGTCGT
             GGTATTCACTCCAGAGCGATGAAAACGTTTCAGTTTGCTCATGGAAAACGGTGTAACA
             AGGGTGAACACTATCCCATATCACCAGCTCACCGTCTTTCATTGCCATACGGAATTCC
             GGATGAGCATTCATCAGGCGGGCAAGAATGTGAATAAAGGCCGGATAAAACTTGTGCT
             TATTTTTCTTTACGGTCTTTAAAAAGGCCGTAATATCCAGCTGAACGGTCTGGTTATA
             GGTACATTGAGCAACTGACTGAAATGCCTCAAAATGTTCTTTACGATGCCATTGGGAT
             ATATCAACGGTGGTATATCCAGTGATTTTTTTCTC",
            );
            let seqs = BlockGroup::get_all_sequences(conn, 1, false);
            assert_eq!(
                seqs,
                HashSet::from_iter([
                    seq.clone(),
                    format!(
                        "{}{deleted}{}",
                        &seq[..766].to_string(),
                        &seq[1557..].to_string()
                    )
                    .to_string()
                ])
            );
        }

        #[test]
        fn test_parses_substitution() {
            // replacing a sequence ends up with the same result as doing a compound delete + insert
            // in the above test.
            let conn = &get_connection(None);
            let db_uuid = metadata::get_db_uuid(conn);
            let op_conn = &get_operation_connection(None);
            setup_db(op_conn, &db_uuid);
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/geneious_genbank/substitution.gb");
            let file = File::open(&path).unwrap();
            import_genbank(conn, BufReader::new(file), None);
            let f = reader::parse_file(&path).unwrap();
            let seq = str::from_utf8(&f[0].seq).unwrap().to_string();
            let deleted: String = normalize_string(
                "TACGCCCCGCCCTGCCACTCATCGCAGTACTGTTGTAATTC
             ATTAAGCATTCTGCCGACATGGAAGCCATCACAAACGGCATGATGAACCTGAATCGCC
             AGCGGCATCAGCACCTTGTCGCCTTGCGTATAATATTTGCCCATGGTGAAAACGGGGG
             CGAAGAAGTTGTCCATATTGGCCACGTTTAAATCAAAACTGGTGAAACTCACCCAGGG
             ATTGGCTGAGACGAAAAACATATTCTCAATAAACCCTTTAGGGAAATAGGCCAGGTTT
             TCACCGTAACACGCCACATCTTGCGAATATATGTGTAGAAACTGCCGGAAATCGTCGT
             GGTATTCACTCCAGAGCGATGAAAACGTTTCAGTTTGCTCATGGAAAACGGTGTAACA
             AGGGTGAACACTATCCCATATCACCAGCTCACCGTCTTTCATTGCCATACGGAATTCC
             GGATGAGCATTCATCAGGCGGGCAAGAATGTGAATAAAGGCCGGATAAAACTTGTGCT
             TATTTTTCTTTACGGTCTTTAAAAAGGCCGTAATATCCAGCTGAACGGTCTGGTTATA
             GGTACATTGAGCAACTGACTGAAATGCCTCAAAATGTTCTTTACGATGCCATTGGGAT
             ATATCAACGGTGGTATATCCAGTGATTTTTTTCTC",
            );
            let seqs = BlockGroup::get_all_sequences(conn, 1, false);
            assert_eq!(
                seqs,
                HashSet::from_iter([
                    seq.clone(),
                    format!(
                        "{}{deleted}{}",
                        &seq[..766].to_string(),
                        &seq[1557..].to_string()
                    )
                    .to_string()
                ])
            );
        }
    }
}