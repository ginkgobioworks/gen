use std::path::PathBuf;
use std::str;

use crate::models::{
    self, block_group::BlockGroup, block_group_edge::BlockGroupEdge, edge::Edge, path::Path,
    sequence::Sequence, strand::Strand,
};
use noodles::fasta;
use rusqlite::Connection;

pub fn import_fasta(fasta: &String, name: &str, shallow: bool, conn: &mut Connection) {
    // TODO: support gz
    let mut reader = fasta::io::reader::Builder.build_from_path(fasta).unwrap();

    if !models::Collection::exists(conn, name) {
        let collection = models::Collection::create(conn, name);

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
                Sequence::PATH_START_HASH.to_string(),
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
                Sequence::PATH_END_HASH.to_string(),
                0,
                Strand::Forward,
                0,
                0,
            );
            BlockGroupEdge::bulk_create(conn, block_group.id, vec![edge_into.id, edge_out_of.id]);
            Path::create(
                conn,
                &name,
                block_group.id,
                vec![edge_into.id, edge_out_of.id],
            );
        }
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
