use std::fs::File;
use std::io::BufRead;
use std::path::Path;
use std::{io, str};
pub mod annotations;
pub mod config;
pub mod diffs;
pub mod exports;
pub mod genbank;
pub mod gfa;
pub mod gfa_reader;
pub mod graph;
pub mod graph_operators;
pub mod imports;
pub mod migrations;
pub mod models;
pub mod operation_management;
pub mod patch;
mod progress_bar;
pub mod range;
#[cfg(test)]
pub mod test_helpers;
pub mod updates;
pub mod views;

use crate::migrations::run_migrations;
use noodles::vcf::variant::record::samples::series::value::genotype::Phasing;
use rusqlite::Connection;
use sha2::{Digest, Sha256};

pub fn get_connection(db_path: &str) -> Connection {
    let mut conn =
        Connection::open(db_path).unwrap_or_else(|_| panic!("Error connecting to {}", db_path));
    rusqlite::vtab::array::load_module(&conn).unwrap();
    run_migrations(&mut conn);
    conn
}

pub fn run_query(conn: &Connection, query: &str) {
    let mut stmt = conn.prepare(query).unwrap();
    for entry in stmt.query_map([], |_| Ok(())).unwrap() {
        println!("{entry:?}");
    }
}

pub fn calculate_hash(t: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(t);
    let result = hasher.finalize();

    format!("{:x}", result)
}

pub struct Genotype {
    pub allele: i64,
    pub phasing: Phasing,
}

pub fn parse_genotype(gt: &str) -> Vec<Option<Genotype>> {
    let mut genotypes = vec![];
    let mut phase = match gt.contains('/') {
        true => Phasing::Unphased,
        false => Phasing::Phased,
    };
    for entry in gt.split_inclusive(['|', '/']) {
        let allele;
        let mut phasing = Phasing::Unphased;
        if entry.ends_with(['/', '|']) {
            let (allele_str, phasing_str) = entry.split_at(entry.len() - 1);
            allele = allele_str;
            phasing = match phasing_str == "|" {
                true => Phasing::Phased,
                false => Phasing::Unphased,
            }
        } else {
            allele = entry;
        }
        if allele == "." {
            genotypes.push(None);
        } else {
            genotypes.push(Some(Genotype {
                allele: allele.parse::<i64>().unwrap(),
                phasing: phase,
            }));
        }
        // we're always 1 behind on phase, e.g. 0|1, the | is the phase of the next allele
        phase = phasing;
    }
    genotypes
}

pub fn get_overlap(a: i64, b: i64, x: i64, y: i64) -> (bool, bool, bool) {
    let contains_start = a <= x && x < b;
    let contains_end = a <= y && y < b;
    let overlap = a < y && x < b;
    (contains_start, contains_end, overlap)
}

pub fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

pub fn normalize_string(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::get_connection;

    #[cfg(test)]
    mod test_normalize_string {
        use super::*;

        #[test]
        fn test_removes_whitespace() {
            assert_eq!(normalize_string(" this has a space "), "thishasaspace")
        }

        #[test]
        fn test_removes_newlines() {
            assert_eq!(
                normalize_string("\nthis\nhas\n\nnew\nlines"),
                "thishasnewlines"
            )
        }
    }

    #[test]
    fn it_hashes() {
        assert_eq!(
            calculate_hash("a test"),
            "a82639b6f8c3a6e536d8cc562c3b86ff4b012c84ab230c1e5be649aa9ad26d21"
        );
    }

    #[test]
    fn it_queries() {
        let conn = get_connection(None);
        let sequence_count: i64 = conn
            .query_row(
                "SELECT count(*) from sequences where hash = 'foo'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sequence_count, 0);
    }

    #[test]
    fn parses_genotype() {
        let genotypes = parse_genotype("1");
        let genotype_1 = genotypes[0].as_ref().unwrap();
        assert_eq!(genotype_1.allele, 1);
        assert_eq!(genotype_1.phasing, Phasing::Phased);
        let genotypes = parse_genotype("0|1");
        let genotype_1 = genotypes[0].as_ref().unwrap();
        let genotype_2 = genotypes[1].as_ref().unwrap();
        assert_eq!(genotype_1.allele, 0);
        assert_eq!(genotype_1.phasing, Phasing::Phased);
        assert_eq!(genotype_2.allele, 1);
        assert_eq!(genotype_2.phasing, Phasing::Phased);
        let genotypes = parse_genotype("0/1");
        let genotype_1 = genotypes[0].as_ref().unwrap();
        let genotype_2 = genotypes[1].as_ref().unwrap();
        assert_eq!(genotype_1.allele, 0);
        assert_eq!(genotype_1.phasing, Phasing::Unphased);
        assert_eq!(genotype_2.allele, 1);
        assert_eq!(genotype_2.phasing, Phasing::Unphased);
        let genotypes = parse_genotype("0/1|2");
        let genotype_1 = genotypes[0].as_ref().unwrap();
        let genotype_2 = genotypes[1].as_ref().unwrap();
        let genotype_3 = genotypes[2].as_ref().unwrap();
        assert_eq!(genotype_1.allele, 0);
        assert_eq!(genotype_1.phasing, Phasing::Unphased);
        assert_eq!(genotype_2.allele, 1);
        assert_eq!(genotype_2.phasing, Phasing::Unphased);
        assert_eq!(genotype_3.allele, 2);
        assert_eq!(genotype_3.phasing, Phasing::Phased);
        let genotypes = parse_genotype("2|1|2");
        let genotype_1 = genotypes[0].as_ref().unwrap();
        let genotype_2 = genotypes[1].as_ref().unwrap();
        let genotype_3 = genotypes[2].as_ref().unwrap();
        assert_eq!(genotype_1.allele, 2);
        assert_eq!(genotype_1.phasing, Phasing::Phased);
        assert_eq!(genotype_2.allele, 1);
        assert_eq!(genotype_2.phasing, Phasing::Phased);
        assert_eq!(genotype_3.allele, 2);
        assert_eq!(genotype_3.phasing, Phasing::Phased);
        let genotypes = parse_genotype("2|.|2");
        let genotype_1 = genotypes[0].as_ref().unwrap();
        let genotype_3 = genotypes[2].as_ref().unwrap();
        assert_eq!(genotype_1.allele, 2);
        assert_eq!(genotype_1.phasing, Phasing::Phased);
        assert_eq!(genotype_3.allele, 2);
        assert_eq!(genotype_3.phasing, Phasing::Phased);
        assert!(genotypes[1].is_none());
    }

    #[test]
    fn test_overlaps() {
        assert_eq!(get_overlap(0, 10, 10, 10), (false, false, false));
        assert_eq!(get_overlap(10, 20, 10, 20), (true, false, true));
        assert_eq!(get_overlap(10, 20, 5, 15), (false, true, true));
        assert_eq!(get_overlap(10, 20, 0, 10), (false, true, false));
    }
}

// Python bindings also have to go here, for clarity I'm putting them after all existing modules and functions
#[cfg(feature = "python-bindings")]
mod python_bindings {
    use pyo3::prelude::*;
    use pyo3::types::PyDict;
    use rusqlite::Connection;
    use std::collections::HashMap;
    use std::path::Path;

    use crate::models::block_group::BlockGroup;

    use crate::graph::GraphNode;
    use crate::models::traits::Query;
    use crate::views::block_group_viewer::PlotParameters;
    use crate::views::block_layout::{BaseLayout, ScaledLayout};

    /// Function to convert SQLite errors to Python exceptions
    fn sqlite_err_to_pyerr(err: rusqlite::Error) -> PyErr {
        pyo3::exceptions::PyRuntimeError::new_err(format!("SQLite error: {}", err))
    }

    #[pymodule]
    fn gen(_py: Python, m: &PyModule) -> PyResult<()> {
        // You can add functions directly to the module
        m.add_function(wrap_pyfunction!(connect, m)?)?;
        m.add_function(wrap_pyfunction!(get_gen_dir, m)?)?;
        m.add_function(wrap_pyfunction!(get_gen_db_path, m)?)?;
        // TODO: more useful functions
        //m.add_function(wrap_pyfunction!(derive_chunks, m)?)?;

        // Now add the classes to the module
        m.add_class::<PyConnection>()?;
        m.add_class::<PyBlockGroup>()?;
        m.add_class::<PyBaseLayout>()?;

        // Rename classes to match what goes in __init__.py
        m.add("Database", m.getattr("PyConnection")?)?;
        m.add("Graph", m.getattr("PyBlockGroup")?)?;
        m.add("BaseLayout", m.getattr("PyBaseLayout")?)?;
        Ok(())
    }

    #[pyfunction]
    fn connect(db_path: &str) -> PyResult<PyConnection> {
        let path = Path::new(db_path);
        if !path.exists() {
            return Err(pyo3::exceptions::PyFileNotFoundError::new_err(format!(
                "Database file not found: {}",
                db_path
            )));
        }

        let conn = crate::get_connection(db_path);

        Ok(PyConnection { conn })
    }

    #[pyclass]
    struct PyConnection {
        conn: Connection,
    }

    #[pymethods]
    impl PyConnection {
        #[new]
        fn new(db_path: &str) -> PyResult<Self> {
            let path = Path::new(db_path);
            if !path.exists() {
                return Err(pyo3::exceptions::PyFileNotFoundError::new_err(format!(
                    "Database file not found: {}",
                    db_path
                )));
            }

            let conn = crate::get_connection(db_path);

            Ok(PyConnection { conn })
        }

        fn execute(&self, query: &str) -> PyResult<()> {
            self.conn.execute(query, []).map_err(sqlite_err_to_pyerr)?;
            Ok(())
        }

        fn query(&self, query: &str) -> PyResult<Vec<Vec<String>>> {
            let mut stmt = self.conn.prepare(query).map_err(sqlite_err_to_pyerr)?;
            let cols = stmt.column_count();

            let rows = stmt
                .query_map([], |row| {
                    let mut values = Vec::with_capacity(cols);
                    for i in 0..cols {
                        let value: String = match row.get(i) {
                            Ok(Some(s)) => s,
                            Ok(None) => "NULL".to_string(),
                            Err(_) => "".to_string(),
                        };
                        values.push(value);
                    }
                    Ok(values)
                })
                .map_err(sqlite_err_to_pyerr)?;

            let mut results = Vec::new();
            for row in rows {
                results.push(row.map_err(sqlite_err_to_pyerr)?);
            }

            Ok(results)
        }
    }

    #[pyclass]
    struct PyBlockGroup {
        #[pyo3(get)]
        id: i64,
        #[pyo3(get)]
        collection_name: String,
        #[pyo3(get)]
        sample_name: Option<String>,
        #[pyo3(get)]
        name: String,
    }

    #[pymethods]
    impl PyBlockGroup {
        #[new]
        fn new(
            conn: &PyConnection,
            name: String,
            collection_name: String,
            sample_name: Option<String>,
        ) -> PyResult<Self> {
            let block_group = BlockGroup::create(
                &conn.conn,
                &collection_name,
                sample_name.as_deref(), // Converts Option<String> to Option<&str>
                &name,
            );
            Ok(PyBlockGroup {
                id: block_group.id,
                collection_name: block_group.collection_name,
                sample_name: block_group.sample_name,
                name: block_group.name,
            })
        }

        fn __repr__(&self) -> PyResult<String> {
            Ok(format!(
                "BlockGroup({}, {}, {:?}, {})",
                self.id, self.collection_name, self.sample_name, self.name
            ))
        }

        #[staticmethod]
        fn get_by_id(conn: &PyConnection, id: i64) -> PyResult<Self> {
            let block_group = BlockGroup::get_by_id(&conn.conn, id);
            Ok(PyBlockGroup {
                id: block_group.id,
                collection_name: block_group.collection_name,
                sample_name: block_group.sample_name,
                name: block_group.name,
            })
        }

        #[staticmethod]
        fn get_by_collection(conn: &PyConnection, collection_name: &str) -> PyResult<Vec<Self>> {
            let block_groups = BlockGroup::query(
                &conn.conn,
                "SELECT * FROM block_groups WHERE collection_name = ?1",
                rusqlite::params![collection_name],
            );

            let result = block_groups
                .into_iter()
                .map(|bg| PyBlockGroup {
                    id: bg.id,
                    collection_name: bg.collection_name,
                    sample_name: bg.sample_name,
                    name: bg.name,
                })
                .collect();

            Ok(result)
        }

        fn as_dict(&self, conn: &PyConnection) -> PyResult<PyObject> {
            let graph = BlockGroup::get_graph(&conn.conn, self.id);

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

                Ok(dict.into())
            })
        }

        fn as_graph(&self, conn: &PyConnection) -> PyResult<PyObject> {
            let graph = BlockGroup::get_graph(&conn.conn, self.id);

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

                Ok(py_digraph.into())
            })
        }

        /*  NOT DONE YET
        /// Convert a rustworkx PyDiGraph back to a petgraph DiGraphMap and update the database
        /// by creating a new sample
        #[staticmethod]
        fn from_graph(conn: &PyConnection, operation_conn: &PyConnection, block_group_id: i64, new_sample_name: &str, py_graph: &PyAny) -> PyResult<Self> {
            // Start an operation session to track changes
            let mut session = operation_management::start_operation(&conn.conn);

            // Process the graph and get the new block group ID
            let result = Python::with_gil(|py| {
                // Check if the input is a rustworkx PyDiGraph
                let rustworkx = PyModule::import(py, "rustworkx")?;
                let py_digraph_type = rustworkx.getattr("PyDiGraph")?;

                if !py_graph.is_instance(py_digraph_type)? {
                    return Err(PyTypeError::new_err("Expected a rustworkx.PyDiGraph object"));
                }

                // Begin transaction inside of the closure so we don't have borrow issues
                conn.execute("BEGIN TRANSACTION")?;

                // Use a result to track success/failure for proper transaction handling
                let result = (|| {
                    ...

                })();

                // Handle transaction based on result
                match result {
                    Ok((new_block_group_id, _)) => {
                        conn.execute("COMMIT")?;
                        // Return the new block group
                        Self::get_by_id(conn, new_block_group_id)
                    },
                    Err(e) => {
                        // Rollback transaction on error
                        let _ = conn.execute("ROLLBACK");
                        Err(e)
                    }
                }
            })?;
        } */
    }

    // Expose configuration functions to Python
    #[pyfunction]
    fn get_gen_db_path() -> PyResult<String> {
        crate::config::get_gen_db_path()
            .to_str()
            .ok_or_else(|| {
                pyo3::exceptions::PyOSError::new_err("Failed to convert database path to string")
            })
            .map(|s| s.to_string())
    }

    #[pyfunction]
    fn get_gen_dir() -> PyResult<String> {
        Ok(crate::config::get_gen_dir())
    }

    // BaseLayout class for visualization
    #[pyclass]
    struct PyBaseLayout {
        layout: BaseLayout,
    }

    #[pymethods]
    impl PyBaseLayout {
        #[new]
        fn new(_py_graph: &PyDict) -> PyResult<Self> {
            // In our main code, we build a BaseLayout from a graph object, not a blockgroup ID
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "Direct instantiation from dictionary not yet implemented",
            ))
        }

        #[staticmethod]
        fn from_graph(conn: &PyConnection, block_group_id: i64) -> PyResult<Self> {
            // Create a layout from a block group
            let graph = BlockGroup::get_graph(&conn.conn, block_group_id);

            let block_layout = BaseLayout::new(&graph);

            Ok(PyBaseLayout {
                layout: block_layout,
            })
        }

        fn expand_right(&mut self) -> PyResult<()> {
            self.layout.expand_right();
            Ok(())
        }

        fn expand_left(&mut self) -> PyResult<()> {
            self.layout.expand_left();
            Ok(())
        }

        fn get_size(&self) -> PyResult<(f64, f64)> {
            Ok(self.layout.size)
        }

        fn create_scaled_layout(&self, label_width: u32, scale: u32) -> PyResult<PyScaledLayout> {
            let params = PlotParameters {
                label_width,
                scale,
                aspect_ratio: 1.0,
                edge_style: crate::views::block_group_viewer::EdgeStyle::Straight,
            };

            let scaled = ScaledLayout::from_base_layout(&self.layout, &params);

            Ok(PyScaledLayout {
                layout: scaled,
                parameters: params,
            })
        }
    }

    // ScaledLayout class for visualization
    #[pyclass]
    struct PyScaledLayout {
        layout: ScaledLayout,
        parameters: PlotParameters,
    }

    #[pymethods]
    impl PyScaledLayout {
        fn refresh(&mut self, base_layout: &PyBaseLayout) -> PyResult<()> {
            self.layout.refresh(&base_layout.layout, &self.parameters);
            Ok(())
        }

        fn get_node_positions(&self, py: Python) -> PyResult<PyObject> {
            let mut result_dict = PyDict::new(py);

            for (node, pos) in self.layout.labels.iter() {
                let node_key = (
                    node.block_id,
                    node.node_id,
                    node.sequence_start,
                    node.sequence_end,
                );

                let position_value = (pos.0, pos.1);
                result_dict.set_item(node_key, position_value)?;
            }

            Ok(result_dict.into())
        }
        fn get_edge_positions(&self, py: Python) -> PyResult<PyObject> {
            let mut result_dict = PyDict::new(py);

            for ((src, dst), pos) in self.layout.lines.iter() {
                let edge_key = (
                    (
                        src.block_id,
                        src.node_id,
                        src.sequence_start,
                        src.sequence_end,
                    ),
                    (
                        dst.block_id,
                        dst.node_id,
                        dst.sequence_start,
                        dst.sequence_end,
                    ),
                );

                let position_value = *pos;
                result_dict.set_item(edge_key, position_value)?;
            }

            Ok(result_dict.into())
        }

        fn set_scale(&mut self, scale: u32) -> PyResult<()> {
            self.parameters.scale = scale;
            Ok(())
        }

        fn set_aspect_ratio(&mut self, aspect_ratio: f32) -> PyResult<()> {
            self.parameters.aspect_ratio = aspect_ratio;
            Ok(())
        }

        fn set_label_size(&mut self, width: u32, _height: u32) -> PyResult<()> {
            self.parameters.label_width = width;
            Ok(())
        }

        fn to_dict(&self) -> PyResult<PyObject> {
            Python::with_gil(|py| {
                let result = PyDict::new(py);

                // Add basic layout parameters
                result.set_item("label_width", self.parameters.label_width)?;
                result.set_item("scale", self.parameters.scale)?;
                result.set_item("aspect_ratio", self.parameters.aspect_ratio)?;

                // Add node positions
                let nodes = PyDict::new(py);
                for (node, pos) in self.layout.labels.iter() {
                    let node_key = format!(
                        "{}:{}:{}:{}",
                        node.block_id, node.node_id, node.sequence_start, node.sequence_end
                    );
                    let pos_value = (pos.0, pos.1); // ((x1, y1), (x2, y2))
                    nodes.set_item(node_key, pos_value)?;
                }
                result.set_item("nodes", nodes)?;

                // Add edge positions
                let edges = PyDict::new(py);
                for ((src, dst), pos) in self.layout.lines.iter() {
                    let edge_key = format!(
                        "{}:{}:{}:{}_{}:{}:{}:{}",
                        src.block_id,
                        src.node_id,
                        src.sequence_start,
                        src.sequence_end,
                        dst.block_id,
                        dst.node_id,
                        dst.sequence_start,
                        dst.sequence_end
                    );
                    let pos_value = (pos.0, pos.1); // ((x1, y1), (x2, y2))
                    edges.set_item(edge_key, pos_value)?;
                }
                result.set_item("edges", edges)?;

                Ok(result.to_object(py))
            })
        }
    }
}
