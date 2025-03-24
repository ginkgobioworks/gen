use crate::views::block_group_viewer::PlotParameters;
use crate::views::block_layout::{BaseLayout, ScaledLayout};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::node_key::PyNodeKey;

// BaseLayout class for visualization
#[pyclass]
pub struct PyBaseLayout {
    pub layout: BaseLayout,
}

#[pymethods]
impl PyBaseLayout {
    #[new]
    fn new(_py_graph: Bound<'_, PyDict>) -> PyResult<Self> {
        // In our main code, we build a BaseLayout from a graph object, not a blockgroup ID
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "Direct instantiation from dictionary not yet implemented",
        ))
    }

    fn expand_right(&mut self) -> PyResult<()> {
        self.layout.expand_right();
        Ok(())
    }

    fn expand_left(&mut self) -> PyResult<()> {
        self.layout.expand_left();
        Ok(())
    }
    
    /// Get all layer positions (x-coordinates of each rank)
    fn get_layers(&self) -> PyResult<Vec<f64>> {
        Ok(self.layout.partition.parts.iter().map(|part| part.layer_widths.clone()).collect())
    }
    
    /// Get a mapping from nodes to their layer indices
    fn get_node_layers(&self, py: Python) -> PyResult<PyObject> {
        let result_dict = PyDict::new(py);
        
        for (node, layer_idx) in &self.layout.node_layers {
            let node_key = PyNodeKey::new(
                node.node_id,
                node.sequence_start,
                node.sequence_end,
            );
            
            result_dict.set_item(node_key, layer_idx)?;
        }
        
        Ok(result_dict.into_pyobject(py)?.into_any().unbind())
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
pub struct PyScaledLayout {
    pub layout: ScaledLayout,
    pub parameters: PlotParameters,
}

#[pymethods]
impl PyScaledLayout {
    fn refresh(&mut self, base_layout: &PyBaseLayout) -> PyResult<()> {
        self.layout.refresh(&base_layout.layout, &self.parameters);
        Ok(())
    }

    fn get_node_positions(&self, py: Python) -> PyResult<PyObject> {
        let result_dict = PyDict::new(py);

        for (node, pos) in self.layout.labels.iter() {
            // Use PyNodeKey without block_id
            let node_key = PyNodeKey::new(
                node.node_id,
                node.sequence_start,
                node.sequence_end,
            );

            let position_value = (pos.0, pos.1);
            result_dict.set_item(node_key, position_value)?;
        }

        Ok(result_dict.into_pyobject(py)?.into_any().unbind())
    }

    fn get_edge_positions(&self, py: Python) -> PyResult<PyObject> {
        let result_dict = PyDict::new(py);

        for ((src, dst), pos) in self.layout.lines.iter() {
            // Use PyNodeKey without block_id for source and destination
            let src_key = PyNodeKey::new(
                src.node_id,
                src.sequence_start,
                src.sequence_end,
            );
            
            let dst_key = PyNodeKey::new(
                dst.node_id,
                dst.sequence_start,
                dst.sequence_end,
            );

            let edge_key = (src_key, dst_key);
            let position_value = *pos;
            result_dict.set_item(edge_key, position_value)?;
        }

        Ok(result_dict.into_pyobject(py)?.into_any().unbind())
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
                // Use a string representation instead of tuples for serialization
                let node_key = format!(
                    "{}:{}:{}",
                    node.node_id, node.sequence_start, node.sequence_end
                );
                let pos_value = (pos.0, pos.1); // ((x1, y1), (x2, y2))
                nodes.set_item(node_key, pos_value)?;
            }
            result.set_item("nodes", nodes)?;

            Ok(result.into_pyobject(py)?.into_any().unbind())
        })
    }
}
