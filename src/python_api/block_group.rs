#[cfg(feature = "python-bindings")]
use pyo3::prelude::*;

/// Exposes a BlockGroup to Python.
#[cfg(feature = "python-bindings")]
#[pyclass]
pub struct PyBlockGroup {
    #[pyo3(get)]
    pub id: i64,
    #[pyo3(get)]
    pub collection_name: String,
    #[pyo3(get)]
    pub sample_name: Option<String>,
    #[pyo3(get)]
    pub name: String,
}

#[cfg(feature = "python-bindings")]
#[pymethods]
impl PyBlockGroup {
    #[new]
    pub fn new(
        id: i64,
        collection_name: String,
        name: String,
        sample_name: Option<String>,
    ) -> Self {
        PyBlockGroup {
            id,
            collection_name,
            sample_name,
            name,
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "BlockGroup({}, {}, {:?}, {})",
            self.id, self.collection_name, self.sample_name, self.name
        ))
    }
}
