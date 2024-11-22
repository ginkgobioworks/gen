use crate::models::block_group::BlockGroup;
use crate::models::path::{Annotation, Path};
use crate::models::sample::Sample;
use noodles::core::Position;
use noodles::gff;
use rusqlite::Connection;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufReader;

pub fn propagate_gff(
    conn: &Connection,
    collection_name: &str,
    from_sample_name: Option<&str>,
    to_sample_name: &str,
    gff_input_filename: &str,
    gff_output_filename: &str,
) -> io::Result<()> {
    let mut reader = File::open(gff_input_filename)
        .map(BufReader::new)
        .map(gff::io::Reader::new)?;

    let output_file = File::create(gff_output_filename).unwrap();
    let mut writer = gff::io::Writer::new(output_file);

    let source_block_groups = Sample::get_block_groups(conn, collection_name, from_sample_name);
    let target_block_groups = Sample::get_block_groups(conn, collection_name, Some(to_sample_name));
    let source_paths_by_bg_name = source_block_groups
        .iter()
        .map(|bg| (bg.name.clone(), BlockGroup::get_current_path(conn, bg.id)))
        .collect::<HashMap<String, Path>>();
    let target_paths_by_bg_name = target_block_groups
        .iter()
        .map(|bg| (bg.name.clone(), BlockGroup::get_current_path(conn, bg.id)))
        .collect::<HashMap<String, Path>>();

    let mut path_mappings_by_bg_name = HashMap::new();
    for (name, target_path) in target_paths_by_bg_name.iter() {
        let source_path = source_paths_by_bg_name.get(name).unwrap();
        let mapping = source_path.get_mapping_tree(conn, target_path);
        path_mappings_by_bg_name.insert(name, mapping);
    }

    let sequence_lengths_by_path_name = target_paths_by_bg_name
        .iter()
        .map(|(name, path)| (name.clone(), path.sequence(conn).len() as i64))
        .collect::<HashMap<String, i64>>();

    for result in reader.records() {
        let record = result?;
        let path_name = record.reference_sequence_name().to_string();
        let annotation = Annotation {
            name: "".to_string(),
            start: record.start().get() as i64,
            end: record.end().get() as i64,
        };
        let mapping_tree = path_mappings_by_bg_name.get(&path_name).unwrap();
        let sequence_length = sequence_lengths_by_path_name.get(&path_name).unwrap();
        let propagated_annotation =
            Path::propagate_annotation(annotation, mapping_tree, *sequence_length).unwrap();

        let score = record.score();
        let phase = record.phase();
        let mut updated_record_builder = gff::Record::builder()
            .set_reference_sequence_name(path_name)
            .set_source(record.source().to_string())
            .set_type(record.ty().to_string())
            .set_start(
                Position::new(propagated_annotation.start.try_into().unwrap())
                    .expect("Could not convert start ({start}) to usize for propagation"),
            )
            .set_end(
                Position::new(propagated_annotation.end.try_into().unwrap())
                    .expect("Could not convert end ({end}) to usize for propagation"),
            )
            .set_strand(record.strand())
            .set_attributes(record.attributes().clone());

        if let Some(score) = score {
            updated_record_builder = updated_record_builder.set_score(score);
        }
        if let Some(phase) = phase {
            updated_record_builder = updated_record_builder.set_phase(phase);
        }

        writer.write_record(&updated_record_builder.build())?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::imports::fasta::import_fasta;
    use crate::models::metadata;
    use crate::models::operations::setup_db;
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use crate::updates::fasta::update_with_fasta;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn test_simple_propagate() {
        setup_gen_dir();
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let mut fasta_update_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_update_path.push("fixtures/aa.fa");
        let mut gff_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        gff_path.push("fixtures/simple.gff");
        let conn = get_connection(None);
        let db_uuid = metadata::get_db_uuid(&conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        import_fasta(
            &fasta_path.to_str().unwrap().to_string(),
            "test",
            false,
            &conn,
            op_conn,
        );

        let _ = update_with_fasta(
            &conn,
            op_conn,
            "test",
            None,
            "child sample",
            "m123",
            15,
            25,
            fasta_update_path.to_str().unwrap(),
        );

        let temp_dir = tempdir().expect("Couldn't get handle to temp directory");
        let mut output_path = PathBuf::from(temp_dir.path());
        output_path.push("output.gff");
        let _ = propagate_gff(
            &conn,
            "test",
            None,
            "child sample",
            gff_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
        );

        let reader = File::open(output_path.to_str().unwrap())
            .map(BufReader::new)
            .map(gff::io::Reader::new);

        for (i, result) in reader
            .expect("Could not read output file!")
            .records()
            .enumerate()
        {
            let record = result.unwrap();
            assert_eq!(record.reference_sequence_name(), "m123");
            if i == 0 {
                assert_eq!(record.reference_sequence_name(), "m123");
                assert_eq!(record.source(), "gen-test");
                assert_eq!(record.ty(), "Region");
                // Full region annotation
                // Original sequence has 34 bp
                // The edit replaces (15, 25) with a 2 bp sequence
                // New sequence length is 26
                assert_eq!(record.start().get(), 1);
                assert_eq!(record.end().get(), 26);
            } else {
                assert_eq!(record.reference_sequence_name(), "m123");
                assert_eq!(record.source(), "gen-test");
                assert_eq!(record.ty(), "Gene");
                // Gene annotation, was on (5, 20)
                // Replaced (15, 25) with a 2 bp sequence
                // New gene annotation is (5, 15)
                assert_eq!(record.start().get(), 5);
                assert_eq!(record.end().get(), 15);
            }
        }
    }
}
