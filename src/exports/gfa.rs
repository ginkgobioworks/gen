use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};

use crate::models::{
    self, block_group::BlockGroup, block_group_edge::BlockGroupEdge, collection::Collection,
    path::Path, sequence::Sequence, strand::Strand,
};

pub fn export_gfa(conn: &Connection, collection_name: &str, filename: &str) {
    let block_groups = Collection::get_block_groups(conn, collection_name);

    let mut edges = HashSet::new();
    for block_group in block_groups {
        let block_group_edges = BlockGroupEdge::edges_for_block_group(conn, block_group.id);
        edges.extend(block_group_edges.into_iter());
    }

    let mut hashes = HashSet::new();
    for edge in &edges {
        if edge.source_hash != Sequence::PATH_START_HASH {
            hashes.insert(edge.source_hash.as_str());
        }
        if edge.target_hash != Sequence::PATH_END_HASH {
            hashes.insert(edge.target_hash.as_str());
        }
    }

    let mut file = File::create(filename).unwrap();
    let mut writer = BufWriter::new(file);

    let sequences_by_hash = Sequence::sequences_by_hash(conn, hashes.into_iter().collect());
    let mut hash_to_index = HashMap::new();
    for (index, (hash, sequence)) in sequences_by_hash.into_iter().enumerate() {
        writer
            .write_all(&segment_line(&sequence.get_sequence(None, None), index).into_bytes())
            .unwrap_or_else(|_| {
                panic!(
                    "Error writing segment with sequence {} to GFA stream",
                    sequence.get_sequence(None, None),
                )
            });

        hash_to_index.insert(hash, index);
    }

    for edge in &edges {
        if edge.source_hash == Sequence::PATH_START_HASH
            || edge.target_hash == Sequence::PATH_END_HASH
        {
            continue;
        }
        let source_index = hash_to_index.get(edge.source_hash.as_str()).unwrap();
        let target_index = hash_to_index.get(edge.target_hash.as_str()).unwrap();
        writer
            .write_all(
                &link_line(
                    *source_index,
                    edge.source_strand,
                    *target_index,
                    edge.target_strand,
                )
                .into_bytes(),
            )
            .unwrap_or_else(|_| {
                panic!(
                    "Error writing link from segment {} to {}  to GFA stream",
                    source_index, target_index,
                )
            });
    }
}

fn segment_line(sequence: &str, index: usize) -> String {
    format!("S\t{}\t{}\t{}\n", index, sequence, "*")
}

fn link_line(
    source_index: usize,
    source_strand: Strand,
    target_index: usize,
    target_strand: Strand,
) -> String {
    format!(
        "L\t{}\t{}\t{}\t{}\t*\n",
        source_index, source_strand, target_index, target_strand
    )
}
