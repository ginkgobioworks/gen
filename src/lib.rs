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
    use std::path::Path;

    use crate::models::block_group::BlockGroup;
    use crate::views::block_group_viewer::PlotParameters;
    use crate::views::block_layout::{BaseLayout, ScaledLayout};
    use rusqlite::Connection;

    /// Function to convert SQLite errors to Python exceptions
    fn sqlite_err_to_pyerr(err: rusqlite::Error) -> PyErr {
        pyo3::exceptions::PyRuntimeError::new_err(format!("SQLite error: {}", err))
    }

    #[pymodule]
    fn gen(_py: Python, m: &PyModule) -> PyResult<()> {
        // You can added functions directly to the module
        m.add_function(wrap_pyfunction!(connect, m)?)?;
        m.add_function(wrap_pyfunction!(get_accessions, m)?)?;
        // TODO: more useful functions
        //m.add_function(wrap_pyfunction!(derive_chunks, m)?)?;

        // Add the classes to the module
        m.add_class::<PyConnection>()?;
        m.add_class::<PyAccession>()?;
        m.add_class::<PyBaseLayout>()?;
        m.add_class::<PyScaledLayout>()?;

        // Rename classes to match what goes in __init__.py
        m.add("Database", m.getattr("PyConnection")?)?;
        m.add("Accession", m.getattr("PyAccession")?)?;
        m.add("BaseLayout", m.getattr("PyBaseLayout")?)?;
        m.add("ScaledLayout", m.getattr("PyScaledLayout")?)?;

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
    struct PyAccession {
        #[pyo3(get)]
        id: i64,
        #[pyo3(get)]
        name: String,
        #[pyo3(get)]
        path_id: i64,
        #[pyo3(get)]
        parent_accession_id: Option<i64>,
    }

    #[pymethods]
    impl PyAccession {
        #[new]
        fn new(id: i64, name: String, path_id: i64, parent_accession_id: Option<i64>) -> Self {
            PyAccession {
                id,
                name,
                path_id,
                parent_accession_id,
            }
        }

        fn __repr__(&self) -> PyResult<String> {
            Ok(format!(
                "Accession({}, {}, {}, {:?})",
                self.id, self.name, self.path_id, self.parent_accession_id
            ))
        }
    }

    #[pyfunction]
    fn get_accessions(conn: &PyConnection) -> PyResult<Vec<PyAccession>> {
        let query = "SELECT id, name, path_id, parent_accession_id FROM accessions";
        let mut stmt = conn.conn.prepare(query).map_err(sqlite_err_to_pyerr)?;

        let rows = stmt
            .query_map([], |row| {
                Ok(PyAccession {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    path_id: row.get(2)?,
                    parent_accession_id: row.get(3)?,
                })
            })
            .map_err(sqlite_err_to_pyerr)?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row.map_err(sqlite_err_to_pyerr)?);
        }

        Ok(results)
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

        // TODO: return a proper object or dict (if we keep this for fancy figures in jupyter notebooks)
        fn get_node_positions(
            &self,
        ) -> PyResult<Vec<((i64, i64, i64, i64), ((f64, f64), (f64, f64)))>> {
            let mut results = Vec::new();

            for (node, pos) in self.layout.labels.iter() {
                results.push((
                    (
                        node.block_id,
                        node.node_id,
                        node.sequence_start,
                        node.sequence_end,
                    ),
                    *pos,
                ));
            }

            Ok(results)
        }

        // TODO: see get_node_positions
        fn get_edge_positions(
            &self,
        ) -> PyResult<
            Vec<(
                ((i64, i64, i64, i64), (i64, i64, i64, i64)),
                ((f64, f64), (f64, f64)),
            )>,
        > {
            let mut results = Vec::new();

            for ((src, dst), pos) in self.layout.lines.iter() {
                results.push((
                    (
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
                    ),
                    *pos,
                ));
            }

            Ok(results)
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
