use crate::graph::{connect_all_boundary_edges, project_path, GraphNode};
use crate::models::block_group::BlockGroup;
use crate::models::node::Node;
use crate::models::sample::Sample;
use crate::models::strand::Strand;
use interavl::IntervalTree;
use noodles::bed;
use noodles::bed::feature::record_buf::{other_fields::Value, OtherFields};
use noodles::core::Position;
use rusqlite::Connection;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::io::{Error, Read, Write};

pub fn translate_bed<'a, R, W>(
    conn: &Connection,
    collection: &str,
    sample: impl Into<Option<&'a str>>,
    reader: R,
    writer: &mut W,
) -> Result<(), Error>
where
    R: Read,
    W: Write,
{
    let sample = sample.into();
    let mut record = bed::Record::default();
    let mut bed_reader = bed::io::reader::Builder::<3>.build_from_reader(reader);
    let mut bed_writer = bed::io::Writer::<3, _>::new(writer);

    let bgs = Sample::get_block_groups(conn, collection, sample);
    let sample_bgs: HashMap<String, &BlockGroup> = HashMap::from_iter(
        bgs.iter()
            .map(|bg| (bg.name.clone(), bg))
            .collect::<Vec<(String, &BlockGroup)>>(),
    );
    let mut paths: HashMap<i64, IntervalTree<i64, (GraphNode, Strand)>> = HashMap::new();

    while bed_reader.read_record(&mut record)? != 0 {
        let ref_name = record.reference_sequence_name().to_string();
        // noodles converts to 1 index, keep it 0.
        let start = record.feature_start().unwrap().get() as i64 - 1;
        let end = record.feature_end().unwrap().unwrap().get() as i64;
        if let Some(bg) = sample_bgs.get(&ref_name) {
            let projection = paths.entry(bg.id).or_insert_with(|| {
                let path = BlockGroup::get_current_path(conn, bg.id);
                let mut graph = BlockGroup::get_graph(conn, bg.id);
                connect_all_boundary_edges(&mut graph);
                let mut tree = IntervalTree::default();
                let mut position: i64 = 0;
                for (node, strand) in project_path(&graph, &path.blocks(conn)) {
                    if !Node::is_terminal(node.node_id) {
                        let end_position = position + node.length();
                        tree.insert(position..end_position, (node, strand));
                        position = end_position;
                    }
                }
                tree
            });
            let range = start..end;
            let values: Vec<_> = record.other_fields().iter().map(Value::from).collect();
            let other_fields = OtherFields::from(values);
            for (overlap, (node, _strand)) in projection.iter_overlaps(&range) {
                let overlap_start = max(start, overlap.start) as usize;
                let overlap_end = min(end, overlap.end) as usize;
                let out_record = bed::feature::RecordBuf::<3>::builder()
                    .set_reference_sequence_name(format!("{nid}", nid = node.node_id))
                    .set_feature_start(Position::try_from(overlap_start + 1).unwrap())
                    .set_feature_end(Position::try_from(overlap_end).unwrap())
                    .set_other_fields(other_fields.clone())
                    .build();
                bed_writer.write_feature_record(&out_record)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use crate::models::metadata;
    use crate::models::operations::setup_db;
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use crate::translate::bed::translate_bed;
    use crate::translate::test_helpers::get_simple_sequence;
    use crate::updates::vcf::update_with_vcf;
    use std::fs::File;
    use std::path::PathBuf;

    #[test]
    fn translates_coordinates_to_nodes() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, &db_uuid);

        let vcf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/simple.vcf");
        let bed_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/beds/simple.bed");
        let collection = "test".to_string();

        get_simple_sequence(conn);

        update_with_vcf(
            &vcf_path.to_str().unwrap().to_string(),
            &collection,
            "".to_string(),
            "".to_string(),
            conn,
            op_conn,
            None,
        )
        .unwrap();
        let mut buffer = Vec::new();
        // "foo" is a sample from simple.vcf
        translate_bed(
            conn,
            &collection,
            "foo",
            File::open(bed_path.clone()).unwrap(),
            &mut buffer,
        )
        .unwrap();
        let results = String::from_utf8(buffer).unwrap();
        assert_eq!(
            results,
            "\
        3\t1\t3\tabc123.1\t0\t-\t1\t10\t0,0,0\t3\t102,188,129,\t0,3508,4691,\n\
        3\t3\t4\tabc123.1\t0\t-\t1\t10\t0,0,0\t3\t102,188,129,\t0,3508,4691,\n\
        3\t4\t10\tabc123.1\t0\t-\t1\t10\t0,0,0\t3\t102,188,129,\t0,3508,4691,\n\
        3\t5\t8\txyz.1\t0\t-\t5\t8\t0,0,0\t1\t113,\t0,\n\
        3\t10\t16\txyz.2\t0\t+\t10\t16\t0,0,0\t2\t142,326,\t0,10710,\n\
        3\t14\t17\tfoo.1\t0\t+\t14\t23\t0,0,0\t2\t142,326,\t0,10710,\n\
        4\t17\t23\tfoo.1\t0\t+\t14\t23\t0,0,0\t2\t142,326,\t0,10710,\n"
        );

        // The None sample has no variants, so should be a simple mapping and covers the split node
        let mut buffer = Vec::new();
        translate_bed(
            conn,
            &collection,
            None,
            File::open(bed_path).unwrap(),
            &mut buffer,
        )
        .unwrap();
        let results = String::from_utf8(buffer).unwrap();
        assert_eq!(
            results,
            "\
        3\t1\t10\tabc123.1\t0\t-\t1\t10\t0,0,0\t3\t102,188,129,\t0,3508,4691,\n\
        3\t5\t8\txyz.1\t0\t-\t5\t8\t0,0,0\t1\t113,\t0,\n\
        3\t10\t16\txyz.2\t0\t+\t10\t16\t0,0,0\t2\t142,326,\t0,10710,\n\
        3\t14\t17\tfoo.1\t0\t+\t14\t23\t0,0,0\t2\t142,326,\t0,10710,\n\
        4\t17\t23\tfoo.1\t0\t+\t14\t23\t0,0,0\t2\t142,326,\t0,10710,\n"
        );
    }
}
