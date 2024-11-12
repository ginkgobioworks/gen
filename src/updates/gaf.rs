use crate::graph::GraphNode;
use crate::models::block_group::BlockGroup;
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::edge::{Edge, EdgeData};
use crate::models::node::Node;
use crate::models::sample::Sample;
use crate::models::sequence::Sequence;
use crate::models::strand::Strand;
use crate::models::traits::*;
use crate::read_lines;
use crate::test_helpers::save_graph;
use itertools::Itertools;
use petgraph::visit::IntoNodeReferences;
use regex::Regex;
use rusqlite::types::Value;
use rusqlite::Connection;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

#[derive(Debug, serde::Deserialize)]
struct CSVRow {
    id: Option<String>,
    left: String,
    sequence: String,
    right: String,
}

struct GafChange {
    id: String,
    left: String,
    right: String,
}

pub fn transform_csv_to_fasta<P>(path: P)
where
    P: AsRef<Path>,
{
    let csv_file = File::open(path).unwrap();
    let csv_bufreader = BufReader::new(csv_file);

    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_bufreader);
    let headers = csv_reader
        .headers()
        .expect("Input csv missing headers. Headers should be id,left,sequence,right.")
        .clone();
    for (index, result) in csv_reader.records().enumerate() {
        let record = result.unwrap();
        let row: CSVRow = record.deserialize(Some(&headers)).unwrap();
        let id = row.id.clone().unwrap_or_else(|| index.to_string());
        println!(
            ">{id}_left\n{left}\n>{id}_right\n{right}",
            left = row.left,
            right = row.right
        );
    }
}

pub fn update_with_gaf<'a, P>(
    conn: &Connection,
    op_conn: &Connection,
    gaf_path: P,
    csv_path: P,
    collection_name: &'a str,
    sample_name: impl Into<Option<&'a str>>,
) where
    P: AsRef<Path> + Clone,
{
    // Given a gaf, this will incorporate the alignment into the specified graph, creating new nodes.

    let sample_name = sample_name.into();

    let mut node_lengths: HashMap<String, (i64, i64)> = HashMap::new();

    let mut get_node_info = |node_id: &str| -> (i64, i64) {
        *node_lengths.entry(node_id.to_string()).or_insert_with(|| {
            let node_info : Vec<&str> = node_id.rsplitn(2, '.').collect();
            let node_id = *node_info.last().unwrap();
            let id = node_id.parse::<i64>().unwrap();
            let mut stmt = conn.prepare_cached("select s.length from nodes n left join sequences s on (s.hash = n.sequence_hash) where n.id = ?1;").unwrap();
            let res = stmt.query_row([id], |row| row.get(0)).unwrap();
            (id, res)
        })
    };

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
        .+
        cg:Z:(?P<cigar>[^\t]+)
        ",
    )
    .unwrap();

    let query_re = Regex::new(
        r"(?x)
        ^(?P<query_id>.+)_(left|right)$
        ",
    )
    .unwrap();

    let orient_id_re = Regex::new(r"(?x)(?P<orient>[><])(?P<node>[^><]+(:\d+-\d+)?)").unwrap();

    let csv_file = File::open(csv_path).unwrap();
    let csv_bufreader = BufReader::new(csv_file);

    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_bufreader);
    let headers = csv_reader
        .headers()
        .expect("Input csv missing headers. Headers should be id,left,sequence,right.")
        .clone();
    let mut change_spec = HashMap::new();
    for (index, result) in csv_reader.records().enumerate() {
        let record = result.unwrap();
        let row: CSVRow = record.deserialize(Some(&headers)).unwrap();
        change_spec.insert(row.id.clone().unwrap_or_else(|| index.to_string()), row);
    }

    let mut gaf_changes: HashMap<String, HashMap<String, (i64, Strand, i64)>> = HashMap::new();

    if let Ok(lines) = read_lines(gaf_path) {
        for line in lines.map_while(Result::ok) {
            let entry = re.captures(&line).unwrap();
            let aln_path = &entry["path"];
            let mut node_start: i64 = entry["path_start"].parse::<i64>().unwrap();
            let mut segments = vec![];
            if [">", "<"].iter().any(|s| aln_path.starts_with(*s)) {
                // orient id
                for sub_match in orient_id_re.captures_iter(aln_path) {
                    let orientation = if &sub_match["orient"] == ">" {
                        Strand::Forward
                    } else {
                        Strand::Reverse
                    };
                    let node = sub_match["node"].to_string();
                    segments.push((orientation, node));
                }
            } else {
                // we're a stable id
                segments.push((Strand::Forward, aln_path.to_string()));
            }
            let query = entry["query_name"].to_string();
            if let Some(id_re) = query_re.captures(&query) {
                let query_id = id_re["query_id"].to_string();
                if change_spec.contains_key(&query_id) {
                    let mut strand: Option<Strand> = None;
                    let mut node_id: Option<i64> = None;
                    let mut query_key = "";
                    if query.ends_with("left") {
                        query_key = "left";
                        let mut matches = entry["residue_match"].parse::<i64>().unwrap();
                        for (segment_strand, segment_id) in segments.iter() {
                            let (segment_node_id, node_length) = get_node_info(segment_id);
                            if node_length >= matches {
                                strand = Some(*segment_strand);
                                node_id = Some(segment_node_id);
                                node_start = matches;
                                break;
                            }
                            matches -= node_length;
                        }
                    } else if query.ends_with("right") {
                        query_key = "right";
                        let (segment_strand, segment_id) = segments.first().unwrap();
                        let (segmnet_node_id, _node_length) = get_node_info(segment_id);
                        strand = Some(*segment_strand);
                        node_id = Some(segmnet_node_id);
                    } else {
                        continue;
                    };

                    if let Some(node_id) = node_id {
                        if let Some(strand) = strand {
                            gaf_changes
                                .entry(query_id)
                                .and_modify(|change| {
                                    change
                                        .entry(query_key.to_string())
                                        .or_insert((node_id, strand, node_start));
                                })
                                .or_insert_with(|| {
                                    let mut change = HashMap::new();
                                    change.insert(
                                        query_key.to_string(),
                                        (node_id, strand, node_start),
                                    );
                                    change
                                });
                        }
                    }
                }
            }
        }
    }

    for (path_id, path_changes) in gaf_changes.iter() {
        if let Some(change) = change_spec.get(path_id) {
            // todo: handle extremes where no left/right path
            let (left_node, left_strand, left_pos) = path_changes["left"];
            let (right_node, right_strand, right_pos) = path_changes["right"];
            let sequence = Sequence::new()
                .sequence(&change.sequence)
                .sequence_type("DNA")
                .save(conn);
            let seq_node = Node::create(
                conn,
                &sequence.hash,
                format!(
                    "{left_node:?}:{left_strand}:{left_pos}->{hash}",
                    hash = sequence.hash
                ),
            );
            let left_edge = EdgeData {
                source_node_id: left_node,
                source_coordinate: left_pos,
                source_strand: left_strand,
                target_node_id: seq_node,
                target_coordinate: 0,
                target_strand: Strand::Forward,
                chromosome_index: 0,
                phased: 0,
            };
            let right_edge = EdgeData {
                source_node_id: seq_node,
                source_coordinate: sequence.length,
                source_strand: Strand::Forward,
                target_node_id: right_node,
                target_coordinate: right_pos,
                target_strand: right_strand,
                chromosome_index: 0,
                phased: 0,
            };
            let edges = Edge::bulk_create(conn, &vec![left_edge, right_edge]);
            let bgs = if let Some(sample) = sample_name {
                BlockGroup::query(conn, "select distinct bg.* from block_groups bg left join block_group_edges bge on (bg.id = bge.block_group_id) left join edges e on (e.id = bge.edge_id and (e.source_node_id in (?3, ?4) or e.target_node_id in (?3, ?4))) where collection_name = ?1 and sample_name = ?2", vec![Value::from(collection_name.to_string()), Value::from(sample.to_string()), Value::from(left_node), Value::from(right_node)])
            } else {
                BlockGroup::query(conn, "select distinct bg.* from block_groups bg left join block_group_edges bge on (bg.id = bge.block_group_id) left join edges e on (e.id = bge.edge_id and (e.source_node_id in (?2, ?3) or e.target_node_id in (?2, ?3))) where collection_name = ?1 and sample_name is null", vec![Value::from(collection_name.to_string()), Value::from(left_node), Value::from(right_node)])
            };
            for bg in bgs.iter() {
                BlockGroupEdge::bulk_create(conn, bg.id, &edges);
            }
        }
    }
}

mod tests {
    use super::*;
    use crate::graph::GraphEdge;
    use crate::imports::gfa::import_gfa;
    use crate::models::metadata;
    use crate::models::operations::setup_db;
    use crate::models::traits::Query;
    use crate::test_helpers::{get_connection, get_operation_connection, setup_gen_dir};
    use std::path::PathBuf;

    #[test]
    fn test_insertion_from_gaf() {
        setup_gen_dir();
        let conn = &get_connection("x.db");
        let db_uuid = &metadata::get_db_uuid(conn);
        let op_conn = &get_operation_connection(None);
        setup_db(op_conn, db_uuid);

        let collection = "test".to_string();

        let gfa_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/chr22_het.gfa");
        let csv_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/chr22_insert.csv");

        import_gfa(&gfa_path, &collection, None, conn);
        let gaf_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/chr22_het.gaf");
        update_with_gaf(conn, op_conn, gaf_path, csv_path, "test", None);
        let graph = Sample::get_graph(conn, "test", None);

        let query = Node::query(conn, "select n.* from nodes n left join sequences s on (n.sequence_hash = s.hash) where s.sequence = ?1", vec![Value::from("AATCGAATCG".to_string())]);
        let insert_node_id = query.first().unwrap().id;
        let insert_node = graph
            .nodes()
            .filter(|node| node.node_id == insert_node_id)
            .collect::<Vec<GraphNode>>();
        let insert_node = insert_node.first().unwrap();
        let left_node_id = graph
            .nodes()
            .filter(|node| node.node_id == 138)
            .collect::<Vec<GraphNode>>();
        let left_node = left_node_id.first().unwrap();
        let right_node_id = graph
            .nodes()
            .filter(|node| node.node_id == 140)
            .collect::<Vec<GraphNode>>();
        let right_node = right_node_id.first().unwrap();

        // Here we should be making an edge from our left node -> insert, and an edge from insert -> right node. .edges gives us outgoing edges
        // only so that is why we use the insert_node for the right_edge lookup.

        let left_edges: Vec<(GraphNode, GraphNode, &GraphEdge)> = graph.edges(*left_node).collect();
        let right_edges: Vec<(GraphNode, GraphNode, &GraphEdge)> =
            graph.edges(*insert_node).collect();
        assert!(
            left_edges
                .iter()
                .filter(|(source, dest, _)| source.node_id == left_node.node_id
                    && dest.node_id == insert_node.node_id)
                .collect::<Vec<_>>()
                .len()
                == 1
        );
        assert!(
            right_edges
                .iter()
                .filter(|(source, dest, _)| source.node_id == insert_node.node_id
                    && dest.node_id == right_node.node_id)
                .collect::<Vec<_>>()
                .len()
                == 1
        );
    }
}
