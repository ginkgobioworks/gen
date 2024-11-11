use crate::graph::GraphNode;
use crate::models::block_group::BlockGroup;
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::edge::{Edge, EdgeData};
use crate::models::node::Node;
use crate::models::sample::Sample;
use crate::models::sequence::Sequence;
use crate::models::strand::Strand;
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

    let mut gaf_changes: HashMap<String, HashMap<String, (GraphNode, Strand, i64)>> =
        HashMap::new();

    if let Ok(lines) = read_lines(gaf_path) {
        for line in lines.map_while(Result::ok) {
            let entry = re.captures(&line).unwrap();
            let aln_path = &entry["path"];
            let node_start: i64 = entry["path_start"].parse::<i64>().unwrap();
            let node_end: i64 = entry["path_end"].parse::<i64>().unwrap();
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
            match nodes.len().cmp(&1) {
                Ordering::Greater => {
                    for ((src_strand, src_segment), (dest_strand, dest_segment)) in
                        nodes.iter().tuple_windows()
                    {
                        if let Some(src_node) = node_map.get(src_segment) {
                            if let Some(dest_node) = node_map.get(dest_segment) {
                                if let Some(edge) =
                                    sample_graph.edge_weight(**src_node, **dest_node)
                                {
                                    if edge.source_strand == *src_strand
                                        && edge.target_strand == *dest_strand
                                    {
                                        let query = entry["query_name"].to_string();
                                        println!("found us {edge:?} {line:?}");
                                        if let Some(id_re) = query_re.captures(&query) {
                                            let query_id = id_re["query_id"].to_string();
                                            println!("match with {qn:?} {query_id}", qn = query);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Ordering::Less => {}
                Ordering::Equal => {
                    let (src_strand, src_segment) = &nodes[0];
                    if let Some(src_node) = node_map.get(src_segment) {
                        let query = entry["query_name"].to_string();
                        if let Some(id_re) = query_re.captures(&query) {
                            let query_id = id_re["query_id"].to_string();
                            if let Some(user_change) = change_spec.get(&query_id) {
                                println!(
                                    "match with {qn:?} {query_id} {user_change:?}",
                                    qn = query
                                );
                                gaf_changes
                                    .entry(query_id)
                                    .and_modify(|change| {
                                        if query.ends_with("left") {
                                            change.entry("left".to_string()).or_insert((
                                                **src_node,
                                                *src_strand,
                                                node_end,
                                            ));
                                        } else {
                                            change.entry("right".to_string()).or_insert((
                                                **src_node,
                                                *src_strand,
                                                node_start,
                                            ));
                                        }
                                    })
                                    .or_insert_with(|| {
                                        let mut change = HashMap::new();
                                        if query.ends_with("left") {
                                            change.insert(
                                                "left".to_string(),
                                                (**src_node, *src_strand, node_end),
                                            );
                                        } else {
                                            change.insert(
                                                "right".to_string(),
                                                (**src_node, *src_strand, node_start),
                                            );
                                        }
                                        change
                                    });
                            }
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
                source_node_id: left_node.node_id,
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
                target_node_id: right_node.node_id,
                target_coordinate: right_pos,
                target_strand: right_strand,
                chromosome_index: 0,
                phased: 0,
            };
            let edges = Edge::bulk_create(conn, &vec![left_edge, right_edge]);
            let bgs = if let Some(sample) = sample_name {
                BlockGroup::query(conn, "select distinct bg.* from block_groups bg left join block_group_edges bge on (bg.id = bge.block_group_id) left join edges e on (e.id = bge.edge_id and (e.source_node_id in (?3, ?4) or e.target_node_id in (?3, ?4))) where collection_name = ?1 and sample_name = ?2", vec![Value::from(collection_name.to_string()), Value::from(sample.to_string()), Value::from(left_node.node_id), Value::from(right_node.node_id)])
            } else {
                BlockGroup::query(conn, "select distinct bg.* from block_groups bg left join block_group_edges bge on (bg.id = bge.block_group_id) left join edges e on (e.id = bge.edge_id and (e.source_node_id in (?2, ?3) or e.target_node_id in (?2, ?3))) where collection_name = ?1 and sample_name is null", vec![Value::from(collection_name.to_string()), Value::from(left_node.node_id), Value::from(right_node.node_id)])
            };
            for bg in bgs.iter() {
                BlockGroupEdge::bulk_create(conn, bg.id, &edges);
            }
        }
    }

    println!("changes to make {gaf_changes:?}");
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
