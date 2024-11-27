use crate::calculate_hash;
use crate::models::block_group::{BlockGroup, PathChange};
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::collection::Collection;
use crate::models::edge::Edge;
use crate::models::node::{Node, PATH_END_NODE_ID, PATH_START_NODE_ID};
use crate::models::path::{Path, PathBlock};
use crate::models::sequence::Sequence;
use crate::models::strand::Strand;
use crate::test_helpers::save_graph;
use gb_io::reader;
use rusqlite::Connection;
use std::io::Read;
use std::str;

pub fn import_genbank<R>(conn: &Connection, data: R)
where
    R: Read,
{
    let reader = reader::SeqReader::new(data);
    let collection = Collection::create(conn, "");
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
                                if v.starts_with("Geneious type: Editing") {
                                    // this returns a 0-indexed coordinate
                                    let (start, end) = feature.location.find_bounds().unwrap();
                                    let feature_seq = &sequence.get_sequence(start, end);
                                    let change_seq = Sequence::new()
                                        .sequence(feature_seq)
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
                                            block_sequence: feature_seq.clone(),
                                            sequence_start: 0,
                                            sequence_end: feature_seq.len() as i64,
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
                            }
                        }
                    }
                }
                save_graph(&BlockGroup::get_graph(conn, block_group.id), "gb.dot");
            }
            Err(e) => println!("Failed to parse {e:?}"),
        }
    }
    println!("Created it");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::metadata;
    use crate::models::operations::setup_db;
    use crate::test_helpers::{get_connection, get_operation_connection};
    use std::fs::File;
    use std::io::BufReader;
    use std::path::PathBuf;

    #[test]
    fn test_parse_genbank() {
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/ecoli_geneious_edited.gb");
        let file = File::open(&path).unwrap();
        import_genbank(conn, BufReader::new(file));
    }
}
