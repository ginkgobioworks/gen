use std::collections::HashMap;
use std::path::PathBuf;
use std::str;

use crate::models::file_types::FileTypes;
use crate::models::operations::{FileAddition, Operation, OperationEdge, OperationSummary};
use crate::models::{
    self, block_group::BlockGroup, block_group_edge::BlockGroupEdge, edge::Edge, path::Path,
    sequence::Sequence, strand::Strand,
};
use noodles::fasta;
use rusqlite::Connection;

pub fn import_fasta(fasta: &String, name: &str, shallow: bool, conn: &mut Connection) {
    let change = FileAddition::create(conn, fasta, FileTypes::Fasta);

    let mut reader = fasta::io::reader::Builder.build_from_path(fasta).unwrap();

    let operation = Operation::create(conn, name, "fasta_addition", change.id);

    if !models::Collection::exists(conn, name) {
        let collection = models::Collection::create(conn, name);
        let mut summary: HashMap<String, i32> = HashMap::new();

        for result in reader.records() {
            let record = result.expect("Error during fasta record parsing");
            let sequence = str::from_utf8(record.sequence().as_ref())
                .unwrap()
                .to_string();
            let name = String::from_utf8(record.name().to_vec()).unwrap();
            let sequence_length = record.sequence().len() as i32;
            let seq = if shallow {
                Sequence::new()
                    .sequence_type("DNA")
                    .name(&name)
                    .file_path(fasta)
                    .save(conn)
            } else {
                Sequence::new()
                    .sequence_type("DNA")
                    .sequence(&sequence)
                    .save(conn)
            };
            let block_group = BlockGroup::create(conn, &collection.name, None, &name);
            let edge_into = Edge::create(
                conn,
                Edge::PATH_START_HASH.to_string(),
                0,
                Strand::Forward,
                seq.hash.to_string(),
                0,
                Strand::Forward,
                0,
                0,
            );
            let edge_out_of = Edge::create(
                conn,
                seq.hash.to_string(),
                sequence_length,
                Strand::Forward,
                Edge::PATH_END_HASH.to_string(),
                0,
                Strand::Forward,
                0,
                0,
            );
            let bge_ids = BlockGroupEdge::bulk_create(
                conn,
                block_group.id,
                vec![edge_into.id, edge_out_of.id],
            )
            .iter()
            .map(|bge| bge.id)
            .collect::<Vec<i32>>();
            let path = Path::create(conn, &name, block_group.id, &[edge_into.id, edge_out_of.id]);
            OperationEdge::bulk_create(conn, operation.id, Some(path.id), None, &bge_ids);
            summary.entry(path.name).or_insert(sequence_length);
        }
        let mut summary_str = "".to_string();
        for (path_name, change_count) in summary.iter() {
            summary_str.push_str(&format!(" {path_name}: {change_count} changes.\n"));
        }
        OperationSummary::create(conn, operation.id, &summary_str);
        println!("Created it");
    } else {
        println!("Collection {:1} already exists", name);
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::test_helpers::get_connection;
    use std::collections::HashSet;

    #[test]
    fn test_add_fasta() {
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let mut conn = get_connection(None);
        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            "test",
            false,
            &mut conn,
        );
        assert_eq!(
            BlockGroup::get_all_sequences(&conn, 1),
            HashSet::from_iter(vec!["ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string()])
        );

        let path = Path::get(&conn, 1);
        assert_eq!(
            Path::sequence(&conn, path),
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA".to_string()
        );
    }
}
