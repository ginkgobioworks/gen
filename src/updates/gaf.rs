use crate::graph::GraphNode;
use crate::models::sample::Sample;
use crate::models::strand::Strand;
use crate::read_lines;
use crate::test_helpers::save_graph;
use itertools::Itertools;
use petgraph::visit::IntoNodeReferences;
use regex::Regex;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;

pub fn update_with_gaf<'a, P>(
    conn: &Connection,
    op_conn: &Connection,
    path: P,
    collection_name: &'a str,
    sample_name: impl Into<Option<&'a str>>,
) where
    P: AsRef<Path> + Clone,
{
    // Given a gaf, this will incorporate the alignment into the specified graph, creating new nodes.

    let sample_name = sample_name.into();
    let sample_graph = Sample::get_graph(conn, collection_name, sample_name);

    let mut node_map: HashMap<String, &GraphNode> = HashMap::new();
    for (node, node_ref) in sample_graph.node_references() {
        node_map.insert(
            format!(
                "{node}.{start}",
                node = node.node_id,
                start = node.sequence_start
            ),
            node_ref,
        );
        node_map.insert(format!("{node}", node = node.node_id), node_ref);
    }

    // our GFA export encodes segments like node_id.sequence_start, where sequence_end can be inferred by the
    // node sequence length
    let re = Regex::new(
        r"(?x)
        ^
        (?P<query_name>[^\t]+)
        \t
        (?P<query_length>\d+)
        \t
        (?P<query_start>\d+)
        \t
        (?P<query_end>\d+)
        \t
        (?P<strand>[+-])
        \t
        (?P<path>[^\t]+)
        \t
        (?P<path_length>\d+)
        \t
        (?P<path_start>\d+)
        \t
        (?P<path_end>\d+)
        \t
        (?P<residue_match>\d+)
        \t
        (?P<align_block_len>\d+)
        \t
        (?P<mapq>\d+)
        ",
    )
    .unwrap();

    let orient_id_re = Regex::new(r"(?x)(?P<orient>[><])(?P<node>[^><]+(:\d+-\d+)?)").unwrap();

    if let Ok(lines) = read_lines(path) {
        for line in lines.map_while(Result::ok) {
            let entry = re.captures(&line).unwrap();
            let aln_path = &entry["path"];
            let mut nodes = vec![];
            if [">", "<"].iter().any(|s| aln_path.starts_with(*s)) {
                // orient id
                for sub_match in orient_id_re.captures_iter(aln_path) {
                    let orientation = if &sub_match["orient"] == ">" {
                        Strand::Forward
                    } else {
                        Strand::Reverse
                    };
                    let node = sub_match["node"].to_string();
                    nodes.push((orientation, node));
                }
            } else {
                // we're a stable id
                nodes.push((Strand::Forward, aln_path.to_string()));
            }
            for ((src_strand, src_segment), (dest_strand, dest_segment)) in
                nodes.iter().tuple_windows()
            {
                if let Some(src_node) = node_map.get(src_segment) {
                    if let Some(dest_node) = node_map.get(dest_segment) {
                        if let Some(edge) = sample_graph.edge_weight(**src_node, **dest_node) {
                            if edge.source_strand == *src_strand
                                && edge.target_strand == *dest_strand
                            {
                                println!("found us {edge:?} {line:?}");
                            }
                        }
                    }
                }
            }
        }
    }
}

mod tests {
    use super::*;
    use crate::imports::gfa::import_gfa;
    use crate::models::metadata;
    use crate::models::operations::setup_db;
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use std::path::PathBuf;

    #[test]
    fn test_x() {
        setup_gen_dir();
        let conn = &get_connection(None);
        let db_uuid = &metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, db_uuid);

        let collection = "test".to_string();

        let gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/chr22_het.gfa");

        import_gfa(&gfa_path, &collection, None, conn);
        let gaf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/chr22_het.gaf");
        update_with_gaf(conn, op_conn, gaf_path, "test", None);
    }
}
