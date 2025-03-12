#[cfg(feature = "python-bindings")]
use pyo3::prelude::*;
#[cfg(feature = "python-bindings")]
use pyo3::types::PyModule;

// Define modules for Python API components
#[cfg(feature = "python-bindings")]
pub mod block_group;
#[cfg(feature = "python-bindings")]
pub mod converters;
#[cfg(feature = "python-bindings")]
pub mod factory;
#[cfg(feature = "python-bindings")]
pub mod layouts;
#[cfg(feature = "python-bindings")]
pub mod repository;
#[cfg(feature = "python-bindings")]
pub mod utils;

// Re-export components for use in the main module
#[cfg(feature = "python-bindings")]
use crate::python_api::block_group::PyBlockGroup;
#[cfg(feature = "python-bindings")]
use crate::python_api::layouts::{PyBaseLayout, PyScaledLayout};
#[cfg(feature = "python-bindings")]
use crate::python_api::repository::PyRepository;
#[cfg(feature = "python-bindings")]
use crate::python_api::utils::get_gen_dir_py;

/// Adds functions and classes to the Python module.
/// Remember to also add them to the __init__.py file
/// to expose them to the user.
#[cfg(feature = "python-bindings")]
#[pymodule]
pub fn gen(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_gen_dir_py, m)?)?;

    m.add_class::<PyRepository>()?;
    m.add_class::<PyBlockGroup>()?;
    m.add_class::<PyBaseLayout>()?;
    m.add_class::<PyScaledLayout>()?;

    Ok(())
}
