use crate::graph::GraphNode;
use crate::models::block_group::BlockGroup;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rusqlite::Connection;
use std::collections::HashMap;

// Private factory struct for BlockGroup transformations
// Not exposed to Python, only used internally by the Repository
#[derive(Default)]
pub struct Factory {}

impl Factory {
    pub fn new() -> Self {
        Self::default()
    }

    // Convert a BlockGroup to a dictionary representation
    pub fn to_dict(&self, conn: &Connection, block_group_id: i64) -> PyResult<PyObject> {
        let graph = BlockGroup::get_graph(conn, block_group_id);

        // Convert the graph to a Python dictionary
        Python::with_gil(|py| {
            let dict = PyDict::new(py);

            // Add nodes to the dictionary
            let nodes = PyDict::new(py);
            for node in graph.nodes() {
                let node_dict = PyDict::new(py);
                node_dict.set_item("block_id", node.block_id)?;
                node_dict.set_item("node_id", node.node_id)?;
                node_dict.set_item("sequence_start", node.sequence_start)?;
                node_dict.set_item("sequence_end", node.sequence_end)?;

                // Use a tuple of the node's fields as the key
                let key = (
                    node.block_id,
                    node.node_id,
                    node.sequence_start,
                    node.sequence_end,
                );
                nodes.set_item(key, node_dict)?;
            }
            dict.set_item("nodes", nodes)?;

            // Add edges to the dictionary
            let edges = PyDict::new(py);
            for (src, dst, edge) in graph.all_edges() {
                let edge_dict = PyDict::new(py);
                edge_dict.set_item("edge_id", edge.edge_id)?;
                edge_dict.set_item("source_strand", edge.source_strand.to_string())?;
                edge_dict.set_item("target_strand", edge.target_strand.to_string())?;
                edge_dict.set_item("chromosome_index", edge.chromosome_index)?;
                edge_dict.set_item("phased", edge.phased)?;

                // Use a tuple of the source and target nodes as the key
                let src_key = (
                    src.block_id,
                    src.node_id,
                    src.sequence_start,
                    src.sequence_end,
                );
                let dst_key = (
                    dst.block_id,
                    dst.node_id,
                    dst.sequence_start,
                    dst.sequence_end,
                );
                edges.set_item((src_key, dst_key), edge_dict)?;
            }
            dict.set_item("edges", edges)?;

            Ok(dict.into_pyobject(py)?.into())
        })
    }

    // Convert a BlockGroup to a rustworkx graph representation
    pub fn to_rustworkx(&self, conn: &Connection, block_group_id: i64) -> PyResult<PyObject> {
        let graph = BlockGroup::get_graph(conn, block_group_id);

        Python::with_gil(|py| {
            // Import rustworkx module
            let rustworkx = PyModule::import(py, "rustworkx")?;

            // Create a new PyDiGraph
            let py_digraph = rustworkx.getattr("PyDiGraph")?.call0()?;

            // Create a mapping from our GraphNode to rustworkx node indices
            let mut node_map: HashMap<GraphNode, usize> = HashMap::new();

            // Add nodes to the rustworkx graph
            for node in graph.nodes() {
                // Create a Python dictionary to store node data
                let node_data = PyDict::new(py);
                node_data.set_item("block_id", node.block_id)?;
                node_data.set_item("node_id", node.node_id)?;
                node_data.set_item("sequence_start", node.sequence_start)?;
                node_data.set_item("sequence_end", node.sequence_end)?;

                // Add the node to the rustworkx graph and store its index
                let index: usize = py_digraph
                    .call_method1("add_node", (node_data,))?
                    .extract()?;
                node_map.insert(node, index);
            }

            // Add edges to the rustworkx graph
            for (src, dst, edge) in graph.all_edges() {
                // Get the rustworkx node indices
                let src_idx = *node_map.get(&src).unwrap();
                let dst_idx = *node_map.get(&dst).unwrap();

                // Create a Python dictionary to store edge data
                let edge_data = PyDict::new(py);
                edge_data.set_item("edge_id", edge.edge_id)?;
                edge_data.set_item("source_strand", edge.source_strand.to_string())?;
                edge_data.set_item("target_strand", edge.target_strand.to_string())?;
                edge_data.set_item("chromosome_index", edge.chromosome_index)?;
                edge_data.set_item("phased", edge.phased)?;

                // Add the edge to the rustworkx graph
                py_digraph.call_method1("add_edge", (src_idx, dst_idx, edge_data))?;
            }

            Ok(py_digraph.into_pyobject(py)?.into())
        })
    }

    // Convert a BlockGroup to a NetworkX graph representation
    pub fn to_networkx(&self, conn: &Connection, block_group_id: i64) -> PyResult<PyObject> {
        let graph = BlockGroup::get_graph(conn, block_group_id);

        Python::with_gil(|py| {
            // Import networkx module
            let networkx = PyModule::import(py, "networkx")?;

            // Create a new DiGraph
            let nx_digraph = networkx.getattr("DiGraph")?.call0()?;

            // NetworkX uses node objects directly as keys, so we don't need a separate mapping
            // Add nodes to the networkx graph
            for node in graph.nodes() {
                // Create a Python dictionary to store node data
                let node_data = PyDict::new(py);
                node_data.set_item("block_id", node.block_id)?;
                node_data.set_item("node_id", node.node_id)?;
                node_data.set_item("sequence_start", node.sequence_start)?;
                node_data.set_item("sequence_end", node.sequence_end)?;

                // Create a tuple key for the node
                let node_key = (
                    node.block_id,
                    node.node_id,
                    node.sequence_start,
                    node.sequence_end,
                );

                // Add the node to the NetworkX graph with its attributes
                // NetworkX add_node expects the node key as the first argument and attributes as a keyword argument
                // We need to pass the attributes as a named parameter
                let kwargs = PyDict::new(py);
                kwargs.set_item("attr_dict", node_data)?;
                nx_digraph.call_method("add_node", (node_key,), Some(&kwargs))?;
            }

            // Add edges to the networkx graph
            for (src, dst, edge) in graph.all_edges() {
                // Create tuple keys for source and target nodes
                let src_key = (
                    src.block_id,
                    src.node_id,
                    src.sequence_start,
                    src.sequence_end,
                );
                let dst_key = (
                    dst.block_id,
                    dst.node_id,
                    dst.sequence_start,
                    dst.sequence_end,
                );

                // Create a Python dictionary to store edge data
                let edge_data = PyDict::new(py);
                edge_data.set_item("edge_id", edge.edge_id)?;
                edge_data.set_item("source_strand", edge.source_strand.to_string())?;
                edge_data.set_item("target_strand", edge.target_strand.to_string())?;
                edge_data.set_item("chromosome_index", edge.chromosome_index)?;
                edge_data.set_item("phased", edge.phased)?;

                // Add the edge to the NetworkX graph with its attributes
                let kwargs = PyDict::new(py);
                kwargs.set_item("attr_dict", edge_data)?;
                nx_digraph.call_method("add_edge", (src_key, dst_key), Some(&kwargs))?;
            }

            Ok(nx_digraph.into_pyobject(py)?.into())
        })
    }
}
