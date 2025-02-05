use crate::views::block_group_viewer::PlotParameters;
use itertools::Itertools; 
use std::collections::HashMap;
use std::u32;

/// Holds processed and scaled layout data.
/// - `lines` = pairs of coordinates for each edge.
/// - `labels` = truncated sequences or symbols for each block.
/// - `highlight_[a|b]` = block ID or (block ID, coordinate) to highlight in color A or B.
/// The raw layout from the Sugiyama algorithm is processed as follow:
/// - The coordinates are rounded to the nearest integer and transposed to go from top-to-bottom to left-to-right.
/// - Each block is assigned a layer (or rank) based on its y-coordinate.
/// - The width of each layer is determined by the widest label in that layer.
/// - The distance between layers is scaled horizontally and vertically 
pub struct ScaledLayout {
    pub lines: Vec<((f64, f64), (f64, f64))>, // Pairs of coordinates for each edge
    pub labels: HashMap<u32, (String, u32, u32)>, // K:Block ID, V: label, x, y
    pub highlight_a: Option<(u32, Option<(u32, u32)>)>, // Block ID or (Block ID, coordinate) to highlight in color A
    pub highlight_b: Option<(u32, Option<(u32, u32)>)>, // Block ID or (Block ID, coordinate) to highlight in color B

    _edges: Vec<(u32, u32)>, // Block ID pairs
    _raw_layout: Vec<(u32, (f64, f64))>, // Raw layout from the Sugiyama algorithm
    _sequences: Option<HashMap<u32, String>>, // Block ID to sequence (full length)

}

impl ScaledLayout {
    pub fn new(
        raw_layout: Vec<(u32, (f64, f64))>, // Block ID, (x, y) coordinates
        edges: Vec<(u32, u32)>, // Block ID pairs
        parameters: &PlotParameters,
        sequences: Option<HashMap<u32, String>>
    ) -> Self {
        let mut layout = ScaledLayout {
            lines: Vec::new(),
            labels: HashMap::new(),
            highlight_a: None,
            highlight_b: None,
            _raw_layout: raw_layout,
            _edges: edges,
            _sequences: sequences
        };
        layout.rescale(parameters);
        layout
    }
    pub fn rescale(&mut self, parameters: &PlotParameters) {
        // Scale the overall layout, round it to the nearest integer, transpose x and y, and sort by x-coordinate
        let scale_x = parameters.scale as f64;
        let scale_y = parameters.scale as f64 * parameters.aspect_ratio as f64;
        let layout: Vec<(u32, (u32, u32))> = self._raw_layout.iter()
            .map(|(id, (x, y))| (*id, ((y * scale_x).round() as u32, (x * scale_y).round() as u32)))
            .sorted_by(|a, b| a.1 .0.cmp(&b.1 .0))
            .collect();

        // We can stop here if:
        // - the target label width is < 5 or no labels were given
        if self._sequences.is_none() || (parameters.label_width < 5)  {
            // Turn the layout into a hashmap so we can easily look up the coordinates for each block
            let layout: HashMap<u32, (u32, u32)> = layout.iter().map(|(id, (x, y))| (*id, (*x, *y))).collect();
            self.lines = self._edges.iter()
                .map(|(source, target)| {
                    let source_coord = layout.get(source).map(|&(x, y)| (x as f64 + 0.5, y as f64 + 0.25)).unwrap();
                    let target_coord = layout.get(target).map(|&(x, y)| (x as f64 - 1.0, y as f64 + 0.25)).unwrap();
                    (source_coord, target_coord)
                })
                .collect();
            self.labels = layout.iter()
                .map(|(id, (x, y))| (*id, ("●".to_string(), *x, *y)))
                .collect();
            return;
        }

        // Loop over the sorted layout and group the blocks by rank (y-coordinate)
        let mut processed_layout: Vec<(u32, String, (u32, u32))> = Vec::new(); //
        let mut current_x = layout[0].1 .0;
        let mut current_layer: Vec<(u32, String, u32)> = Vec::new(); // Block ID, label, y-coordinate
        let mut layer_width = std::cmp::min(self._sequences.as_ref().unwrap().get(&layout[0].0).unwrap().len() as u32, parameters.label_width);
        let mut cumulative_offset = 0;
        for (id, (x, y)) in layout.iter() {
            let full_label = self._sequences
                .as_ref()
                .and_then(|labels| labels.get(id))
                .unwrap();
            let truncated_label = inner_truncation(full_label, parameters.label_width);
                    
            if *x == current_x {
                // This means we are still in the same layer
                // Keep a tally of the maximum label width
                layer_width = std::cmp::max(layer_width, truncated_label.len() as u32);

                // Add the block to the current layer vector
                current_layer.push((*id, truncated_label, *y));
            } else {
                // We switched to a new layer
                // Loop over the current layer and:
                // - increment the x-coordinate by the cumulative offset so far
                // - horizontally center the block in its layer
                for (id, label, y) in current_layer {
                    let centering_offset = (layer_width - label.len() as u32) / 2;
                    let x = current_x + centering_offset + cumulative_offset;
                    // Store the new x-coordinate and truncated label in the combined vector
                    processed_layout.push((id, label, (x, y)));
                }
                // Increment the cumulative offset for the next layer by the width of the current layer
                cumulative_offset += layer_width;

                // Reset the layer width and the current layer
                layer_width = truncated_label.len() as u32;
                current_layer = vec![(*id, truncated_label, *y)];
                current_x = *x;
            }
        }
        // Loop over the last layer (wasn't processed yet)
        for (id, label, y) in current_layer {
            let centering_offset = (layer_width - label.len() as u32) / 2;
            let x = current_x + centering_offset + cumulative_offset;
            processed_layout.push((id, label, (x, y)));
        }

        // Make a hashmap of the processed layout so we can quickly find labels with coordinates
        self.labels = processed_layout.into_iter().map(|(id, label, (x, y))| (id, (label, x, y))).collect();

        // Recalculate all the edges so they meet labels on the sides instead of the center
        self.lines = self._edges.iter()
            .map(|(source, target)| {
            let (source_label, source_x, source_y) = self.labels.get(source).unwrap();
            let (_, target_x, target_y) = self.labels.get(target).unwrap();
            let source_x = *source_x as f64 + source_label.len() as f64;
            let source_y = *source_y as f64 + 0.5;
            let target_x = *target_x as f64 - 1.5;
            let target_y = *target_y as f64 + 0.5;
            ((source_x, source_y), (target_x, target_y))
            })
            .collect();

    }
}



/// Truncate a string to a certain length, adding an ellipsis in the middle
fn inner_truncation(s: &str, target_length: u32) -> String {
    let input_length = s.len() as u32;
    if input_length <= target_length {
        return s.to_string();
    } else if target_length < 3 {
        return "●".to_string();
    }
    // length - 3 because we need space for the ellipsis
    let left_len = (target_length-3) / 2 + ((target_length-3)  % 2);
    let right_len = (target_length-3) - left_len;
    
    format!("{}...{}", &s[..left_len as usize], 
        &s[input_length as usize - right_len as usize..])
}

#[cfg(test)]
mod tests{
    use super::*;

    #[test]
    fn test_inner_truncation_no_truncation_needed() {
        let s = "hello";
        let truncated = inner_truncation(s, 5);
        assert_eq!(truncated, "hello");
    }

    #[test]
    fn test_inner_truncation_truncate_to_odd_length() {
        let s = "hello world";
        let truncated = inner_truncation(s, 5);
        assert_eq!(truncated, "h...d");
    }

    #[test]
    fn test_inner_truncation_truncate_to_even_length() {
        let s = "hello world";
        let truncated = inner_truncation(s, 6);
        assert_eq!(truncated, "he...d");
    }

    #[test]
    fn test_inner_truncation_empty_string() {
        let s = "";
        let truncated = inner_truncation(s, 5);
        assert_eq!(truncated, "");
    }


    #[test]
    fn test_scaled_layout_new_unlabeled() {
        let _ = env_logger::try_init();

        let edges = vec![(0, 1), (0, 2), (2, 3), (1,3)];
        let raw_layout = vec![(0, (10.0, 0.0)), 
                                                      (1, (5.0, 1.0)), 
                                                      (2, (15.0, 1.0)), 
                                                      (3, (10.0, 2.0))];

        let parameters = PlotParameters {
            label_width: 5,
            scale: 1,
            aspect_ratio: 1.0
        };
        let scaled_layout = ScaledLayout::new(raw_layout, edges, &parameters, None);

        // This should only round and transpose the coordinates
        let expected_labels = HashMap::from([
            (0, ("●".to_string(), 0, 10)),
            (1, ("●".to_string(), 1, 5)),
            (2, ("●".to_string(), 1, 15)),
            (3, ("●".to_string(), 2, 10)),
        ]);
        assert_eq!(scaled_layout.labels, expected_labels);

        
    }

    #[test]
    fn test_scaled_layout_new_unlabeled_scaled() {
        let _ = env_logger::try_init();

        let edges = vec![(0, 1), (0, 2), (2, 3), (1,3)];
        let raw_layout = vec![(0, (10.0, 0.0)), 
                                                      (1, (5.0, 1.0)), 
                                                      (2, (15.0, 1.0)), 
                                                      (3, (10.0, 2.0))];

        let parameters = PlotParameters {
            label_width: 5,
            scale: 10,
            aspect_ratio: 1.0
        };
        let scaled_layout = ScaledLayout::new(raw_layout, edges, &parameters, None);

        let expected_labels = HashMap::from([
            (0, ("●".to_string(), 0, 100)),
            (1, ("●".to_string(), 10, 50)),
            (2, ("●".to_string(), 10, 150)),
            (3, ("●".to_string(), 20, 100)),
        ]);
        assert_eq!(scaled_layout.labels, expected_labels);
    }

    #[test]
    fn test_scaled_layout_new() {
        let _ = env_logger::try_init();

        let edges = vec![(0, 1), (0, 2), (2, 3), (1,3)];
        let raw_layout = vec![(0, (10.0, 0.0)),
                                                      (1, (5.0, 1.0)), 
                                                      (2, (15.0, 1.0)), 
                                                      (3, (10.0, 2.0))];
        let full_labels = HashMap::from([
            (0, "ABCDEFGH".to_string()), 
            (1, "IJKLMNOP".to_string()), 
            (2, "QRSTUV".to_string()), 
            (3, "WXYZ".to_string())
        ]);

        let parameters: PlotParameters = PlotParameters {
            label_width: u32::MAX,
            scale: 1,
            aspect_ratio: 1.0
        };
        let scaled_layout = ScaledLayout::new(raw_layout, edges, &parameters, Some(full_labels));

        let expected_labels = HashMap::from([
            (0, ("ABCDEFGH".to_string(), 0, 10)),
            (1, ("IJKLMNOP".to_string(), 9, 5)),
            (2, ("QRSTUV".to_string(), 10, 15)),
            (3, ("WXYZ".to_string(), 18, 10)),
        ]);
        assert_eq!(scaled_layout.labels, expected_labels);
    }

    #[test]
    fn test_scaled_layout_new_truncations() {
        let _ = env_logger::try_init();

        let edges = vec![(0, 1), (0, 2), (2, 3), (1,3)];
        let raw_layout = vec![(0, (10.0, 0.0)), (1, (5.0, 1.0)), (2, (15.0, 1.0)), (3, (10.0, 2.0))];
        let full_labels = HashMap::from([
            (0, "ABCDEFGH".to_string()), 
            (1, "IJKLMNOP".to_string()), 
            (2, "QRSTUV".to_string()), 
            (3, "WXYZ".to_string())
        ]);

        let parameters = PlotParameters {
            label_width: 5,
            scale: 1,
            aspect_ratio: 1.0
        };
        let scaled_layout = ScaledLayout::new(raw_layout, edges, &parameters, Some(full_labels));

        let expected_labels = HashMap::from([
            (0, ("A...H".to_string(), 0, 10)),
            (1, ("I...P".to_string(), 6, 5)),
            (2, ("Q...V".to_string(), 6, 15)),
            (3, ("WXYZ".to_string(), 12, 10)),
        ]);
        assert_eq!(scaled_layout.labels, expected_labels);
    }

    #[test]
    fn test_scaled_layout_new_edges() {
        let _ = env_logger::try_init();

        let edges = vec![(0, 1)];
        let raw_layout = vec![(0, (5.0, 0.0)),
                                                      (1, (5.0, 10.0))];
        let full_labels = HashMap::from([
            (0, "ABCDEFGH".to_string()), 
            (1, "IJKLMNOP".to_string())
        ]);

        let parameters: PlotParameters = PlotParameters {
            label_width: u32::MAX,
            scale: 1,
            aspect_ratio: 1.0
        };
        let scaled_layout = ScaledLayout::new(raw_layout, edges, &parameters, Some(full_labels));

        let expected_labels = HashMap::from([
            (0, ("ABCDEFGH".to_string(), 0, 5)),
            (1, ("IJKLMNOP".to_string(), 18, 5))]);
        assert_eq!(scaled_layout.labels, expected_labels);

        let expected_lines = vec![((8.0, 5.5), (16.5, 5.5))];
        assert_eq!(scaled_layout.lines, expected_lines);
    }
}
