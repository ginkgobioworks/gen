use crate::config::get_gen_dir;
#[cfg(feature = "python-bindings")]
use pyo3::prelude::*;
#[cfg(feature = "python-bindings")]
use pyo3::types::{PyBytes, PyModule};
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use std::path::Path;

/// Helper function to convert SQLite errors to Python exceptions
#[cfg(feature = "python-bindings")]
pub fn sqlite_err_to_pyerr(err: rusqlite::Error) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("SQLite error: {}", err))
}

/// Helper function to convert a Rust path to a Python pathlib.Path object
#[cfg(feature = "python-bindings")]
pub fn path_to_py_path(py: Python, path: &Path) -> PyResult<PyObject> {
    let pathlib = PyModule::import(py, "pathlib")?;
    let path_class = pathlib.getattr("Path")?;
    let py_path = path_class.call1((path.to_str().unwrap(),))?;
    Ok(py_path.to_object(py))
}

/// Helper function return sqlite query results as a list of lists of Python objects
#[cfg(feature = "python-bindings")]
pub fn py_query(conn: &Connection, query: &str) -> PyResult<Vec<Vec<PyObject>>> {
    let mut stmt = conn.prepare(query).map_err(sqlite_err_to_pyerr)?;
    let column_count = stmt.column_count();
    let mut rows = Vec::new();
    let mut row_iter = stmt.query([]).map_err(sqlite_err_to_pyerr)?;

    Python::with_gil(|py| -> PyResult<_> {
        while let Some(row) = row_iter.next().map_err(sqlite_err_to_pyerr)? {
            let mut row_data = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let value: PyObject = match row.get_ref(i).map_err(sqlite_err_to_pyerr)? {
                    ValueRef::Null => py.None(),
                    ValueRef::Integer(i) => i.to_object(py),
                    ValueRef::Real(f) => f.to_object(py),
                    ValueRef::Text(s) => std::str::from_utf8(s)
                        .map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyUnicodeDecodeError, _>(e.to_string())
                        })?
                        .to_object(py),
                    ValueRef::Blob(b) => PyBytes::new(py, b).to_object(py),
                };
                row_data.push(value);
            }
            rows.push(row_data);
        }
        Ok(())
    })?;

    Ok(rows)
}

/// Returns the path to the .gen directory as a Python pathlib.Path object.
#[cfg(feature = "python-bindings")]
#[pyfunction(name = "get_gen_dir")]
pub fn get_gen_dir_py(py: Python) -> PyResult<PyObject> {
    match get_gen_dir() {
        Some(dir) => {
            let path = Path::new(&dir);
            path_to_py_path(py, path)
        },
        None => Err(pyo3::exceptions::PyFileNotFoundError::new_err(
            "No .gen directory found. Run 'gen init' in the project root directory to initialize gen."
        ))
    }
}
