use crate::graph::{GraphEdge, GraphNode};
use crate::models::node;
use crate::models::node::Node;
use crate::models::sequence::Sequence;
use crate::views::block_layout::{BaseLayout, ScaledLayout};

use core::panic;
use petgraph::graphmap::DiGraphMap;
use petgraph::Direction;
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::Span,
    widgets::canvas::{Canvas, Line},
    widgets::Block,
};
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};

/// Labels used in the graph visualization (selected, not-selected)
/// the trick is to get them to align with the braille characters
/// we use to draw lines:
/// ⣿●⣿*⣿∘⣿⦿⣿○⣿◉⣿⏣⣿⏸⣿⏹⣿⏺⣿⏼⣿⎔⣿
pub mod label {
    pub const START: &str = "Start > ";
    pub const END: &str = " > End";
    pub const NODE: &str = "⏺";
}

/// Holds parameters that don't change when you scroll.
/// - `label_width` = how many characters to show at most in each block label. If 0, labels are not shown.
/// - `scale` = data units per 1 terminal cell.  
///   - If `scale` = 1.0, each cell is 1 data unit.  
///   - If `scale` = 2.0, each cell is 2 data units (you see *more* data).  
///   - If `scale` = 0.5, each cell is 0.5 data units (you see *less* data, zoomed in).
/// - `aspect_ratio` = width / height of a terminal cell in data units.
/// - `vertical_offset` = how much to offset the lines vertically to align the braille characters with the labels
pub struct PlotParameters {
    pub label_width: u32,
    pub scale: u32,
    pub aspect_ratio: f32,
    pub line_offset_y: f64,
}

impl Default for PlotParameters {
    fn default() -> Self {
        PlotParameters {
            label_width: 11,
            scale: 4,
            aspect_ratio: 0.5,
            line_offset_y: 0.125,
        }
    }
}

/// Holds parameters that do change as the user scrolls through the graph.
/// - `offset_x` and `offset_y` = coordinates of the top-left corner of the viewport (y-axis is upside down)
/// - `selected_block` = the block that is currently selected by the user.
// Remove Default derive since we have a manual impl
pub struct State {
    pub offset_x: i32,
    pub offset_y: i32,
    pub viewport: Rect,
    pub selected_block: Option<GraphNode>,
    pub first_render: bool,
}
impl Default for State {
    fn default() -> Self {
        State {
            offset_x: 0,
            offset_y: 0,
            viewport: Rect::new(0, 0, 0, 0),
            selected_block: None,
            first_render: true,
        }
    }
}

pub enum NavDirection {
    Left,
    Right,
    Up,
    Down,
}

pub struct Viewer<'a> {
    pub block_graph: &'a DiGraphMap<GraphNode, GraphEdge>,
    pub conn: &'a Connection,
    pub base_layout: BaseLayout,
    pub scaled_layout: ScaledLayout,
    pub node_sequences: HashMap<i64, Sequence>,
    pub state: State,
    pub parameters: PlotParameters,
    pub origin_block: Option<GraphNode>,
}

impl<'a> Viewer<'a> {
    pub fn new(
        block_graph: &'a DiGraphMap<GraphNode, GraphEdge>,
        conn: &'a Connection,
        plot_parameters: PlotParameters,
    ) -> Viewer<'a> {
        Self::with_origin(
            block_graph,
            conn,
            plot_parameters,
            (Node::get_start_node(), 0),
        )
    }

    pub fn with_origin(
        block_graph: &'a DiGraphMap<GraphNode, GraphEdge>,
        conn: &'a Connection,
        plot_parameters: PlotParameters,
        origin: (Node, i64),
    ) -> Viewer<'a> {
        let base_layout = BaseLayout::with_origin(block_graph, origin.clone());

        // Get the sequences for the nodes in the base_layout subgraph
        let node_sequences = Node::get_sequences_by_node_ids(
            conn,
            &base_layout
                .layout_graph
                .nodes()
                .map(|node| node.node_id)
                .collect::<HashSet<i64>>()
                .into_iter()
                .collect::<Vec<i64>>(),
        )
        .into_iter()
        .collect::<HashMap<i64, Sequence>>();

        // Stretch and scale the base layout to account for the sequence labels and plot parameters
        let scaled_layout = ScaledLayout::from_base_layout(&base_layout, &plot_parameters);

        // Capture the origin block from the base_layout's nodes (matching on node_id)
        let origin_block = base_layout
            .layout_graph
            .nodes()
            .find(|node| node.node_id == origin.0.id);

        Viewer {
            block_graph,
            conn,
            base_layout,
            scaled_layout,
            node_sequences,
            state: State::default(),
            parameters: plot_parameters,
            origin_block,
        }
    }

    /// Refresh based on changed parameters or layout.
    pub fn refresh(&mut self) {
        self.scaled_layout
            .refresh(&self.base_layout, &self.parameters);
    }

    /// Check if a block is visible in the viewport.
    pub fn is_block_visible(&self, block: GraphNode) -> bool {
        if let Some(((x, y), _)) = self.scaled_layout.labels.get(&block) {
            return (*y as i32) >= self.state.offset_y
                && (*y as i32) < self.state.offset_y + self.state.viewport.height as i32
                && (*x as i32) >= self.state.offset_x
                && (*x as i32) < self.state.offset_x + self.state.viewport.width as i32;
        }
        false
    }

    /// Unselect the currently selected block if it's not visible in the viewport.
    pub fn unselect_if_not_visible(&mut self) {
        if let Some(selected_block) = self.state.selected_block {
            if !self.is_block_visible(selected_block) {
                self.state.selected_block = None;
            }
        }
    }

    /// Center the viewport on a specific block with minimal whitespace around the layout bounds.
    /// Only show at most 5 units of margin on the left side.
    pub fn center_on_block(&mut self, block: GraphNode) {
        if let Some(((start, y), (end, _))) = self.scaled_layout.labels.get(&block) {
            let block_center_x = (start + end) / 2.0;
            let block_center_y = *y;
            self.update_scroll_for_cursor(block_center_x, block_center_y);
        } else {
            panic!("Block ID {:?} not found in layout", block);
        }
    }

    /// Draw and render blocks and lines to a canvas through a scrollable window.
    /// TODO: turn this into the render function of a custom stateful widget
    pub fn paint_canvas(&mut self, frame: &mut ratatui::Frame, area: Rect) {
        // Set up the coordinate systems for the window and the canvas,
        // we need to keep a 1:1 mapping between coordinates to avoid glitches.

        // From the terminal's perspective, the viewport is the inner area of the scrolling canvas area.
        // Terminal coordinates are referenced from the top-left corner of the terminal, with the y-axis
        // pointing downwards.
        //
        // From the data's perspective, the viewport is a moving window defined by a width, height, and
        // offset from the data origin. When offset_x = 0 and offset_y = 0, the bottom-left corner of the
        // viewport has data coordinates (0,0). We must keep a 1:1 mapping between the size of the viewport
        // in data units and in terminal cells to avoid glitches.

        let canvas_block = Block::default();
        let viewport = canvas_block.inner(area);

        // Check if the viewport has changed size, and if so, update the offset to keep our reference
        // frame intact.
        if self.state.viewport != viewport {
            let x_diff = viewport.x as i32 - self.state.viewport.x as i32;
            let y_diff = viewport.y as i32 - self.state.viewport.y as i32;
            let height_diff = viewport.height as i32 - self.state.viewport.height as i32;

            // Adjust offsets based on viewport changes
            self.state.offset_x -= x_diff;
            // Accommodate inverted y-axis between terminal and data coordinates
            self.state.offset_y += y_diff - height_diff;
            self.state.viewport = viewport;
        }

        // Set initial scroll offset if not already set, aligning the origin block:
        if self.state.first_render
            && self.state.viewport.width > 0
            && self.state.viewport.height > 0
        {
            if let Some(origin) = self.origin_block {
                if Node::is_start_node(origin.node_id) {
                    // Find the first non-start/end node by looking at outgoing neighbors of the start node
                    self.state.selected_block = self
                        .base_layout
                        .layout_graph
                        .neighbors_directed(origin, Direction::Outgoing)
                        .find(|node| {
                            !Node::is_start_node(node.node_id) && !Node::is_end_node(node.node_id)
                        });
                } else {
                    self.state.selected_block = Some(origin);
                }
                self.center_on_block(self.state.selected_block.unwrap());
                self.state.first_render = false;
            }
        }

        // Create the canvas
        let canvas = Canvas::default()
            .background_color(Color::Black)
            .block(canvas_block)
            // Adjust the x_bounds and y_bounds by the scroll offsets.
            .x_bounds([
                self.state.offset_x as f64,
                (self.state.offset_x + self.state.viewport.width as i32) as f64,
            ])
            .y_bounds([
                self.state.offset_y as f64,
                (self.state.offset_y + self.state.viewport.height as i32) as f64,
            ])
            .paint(|ctx| {
                // Draw the lines described in the processed layout
                for &((x1, y1), (x2, y2)) in self.scaled_layout.lines.iter() {
                    // Clip the line to the visible area, skip if it's not visible itself
                    if let Some(((x1c, y1c), (x2c, y2c))) = clip_line(
                        (x1, y1 + self.parameters.line_offset_y),
                        (x2, y2 + self.parameters.line_offset_y),
                        (self.state.offset_x as f64, self.state.offset_y as f64),
                        (
                            (self.state.offset_x + self.state.viewport.width as i32) as f64,
                            (self.state.offset_y + self.state.viewport.height as i32) as f64,
                        ),
                    ) {
                        ctx.draw(&Line {
                            x1: x1c,
                            y1: y1c,
                            x2: x2c,
                            y2: y2c,
                            color: Color::DarkGray,
                        });
                    }
                }
                // Print the labels
                for (block, ((x, y), (x2, _y2))) in self.scaled_layout.labels.iter() {
                    // Skip labels that are not in the visible area (vertical)
                    if (*y as i32) < self.state.offset_y
                        || (*y as i32) >= (self.state.offset_y + self.state.viewport.height as i32)
                    {
                        continue;
                    }

                    // Handle dummy nodes (start and end) differently than other nodes
                    if Node::is_start_node(block.node_id) || Node::is_end_node(block.node_id) {
                        continue;
                    }

                    let label = if let Some(sequence) = self.node_sequences.get(&block.node_id) {
                        inner_truncation(
                            sequence
                                .get_sequence(block.sequence_start, block.sequence_end)
                                .as_str(),
                            (x2 - x) as u32,
                        )
                    } else {
                        label::NODE.to_string()
                    };
                    // Style the label depending on whether it's selected
                    let style = if Some(block) == self.state.selected_block.as_ref() {
                        // Selected blocks
                        match label.as_str() {
                            label::NODE => Style::default().fg(Color::LightGreen),
                            _ => Style::default().fg(Color::Black).bg(Color::White),
                        }
                    } else {
                        Style::default().fg(Color::White)
                    };

                    // Clip labels that are potentially in the window (horizontal)
                    let clipped_label = clip_label(
                        &label,
                        *x as isize,
                        (self.state.offset_x + 1) as isize,
                        self.state.viewport.width as usize,
                    );
                    if !clipped_label.is_empty() {
                        ctx.print(
                            f64::max(*x, self.state.offset_x as f64),
                            *y,
                            Span::styled(clipped_label, style),
                        );
                    }

                    // Indicate if the block is connected to the start node (not shown)
                    if self
                        .base_layout
                        .layout_graph
                        .neighbors_directed(*block, Direction::Incoming)
                        .any(|neighbor| Node::is_start_node(neighbor.node_id))
                    {
                        let x_pos = *x as isize - (label::START.len() as isize);
                        let arrow = clip_label(
                            label::START,
                            x_pos,
                            (self.state.offset_x + 1) as isize,
                            self.state.viewport.width as usize,
                        );
                        if !arrow.is_empty() {
                            ctx.print(
                                x_pos as f64,
                                *y,
                                Span::styled(arrow, Style::default().fg(Color::DarkGray)),
                            );
                        }
                    }

                    // Indicate if the block is connected to the end node (not shown)
                    if self
                        .base_layout
                        .layout_graph
                        .neighbors_directed(*block, Direction::Outgoing)
                        .any(|neighbor| neighbor.node_id == node::PATH_END_NODE_ID)
                    {
                        let x_pos = *x2 as isize + 1;
                        let arrow = clip_label(
                            label::END,
                            x_pos,
                            (self.state.offset_x + 1) as isize,
                            self.state.viewport.width as usize,
                        );
                        if !arrow.is_empty() {
                            ctx.print(
                                x_pos as f64,
                                *y,
                                Span::styled(arrow, Style::default().fg(Color::DarkGray)),
                            );
                        }
                    }
                }
            });
        frame.render_widget(canvas, area);

        // Compute more of the base layout if we're getting close to the ends.
        self.auto_expand();
    }

    /// Check the viewport bounds against the layout and trigger expansion if needed.
    pub fn auto_expand(&mut self) {
        // Find the minimum and maximum x-coordinates of (left side of) labels in the layout so far
        let xs: Vec<f64> = self
            .scaled_layout
            .labels
            .values()
            .map(|((x, _), _)| *x)
            .collect();
        if xs.is_empty() {
            return;
        }
        // For floats, min and max are not defined, so use fold instead.
        let x_min = xs.iter().cloned().fold(f64::INFINITY, f64::min);
        let x_max = xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        // Check if we're a screen width away from the left/right boundary and expand if needed
        // - if we can't expand any further, this will do nothing
        if (x_min as i32) > (self.state.offset_x - self.state.viewport.width as i32) {
            self.base_layout.expand_left();
        }
        if (x_max as i32) < (self.state.offset_x + 2 * self.state.viewport.width as i32) {
            self.base_layout.expand_right();
        }
    }

    /// Cycle through nodes in a specified direction based on the label coordinates.
    /// For moves to the left, it uses the end coordinate of the label; for right, the start coordinate;
    /// and for up/down, the average of the start and end coordinates.
    pub fn move_selection(&mut self, direction: NavDirection) {
        // Determine the reference point from BaseLayout's node_positions.
        let current_point = if let Some(selected) = self.state.selected_block {
            self.base_layout
                .node_positions
                .get(&selected)
                .cloned()
                .unwrap_or_else(|| {
                    (
                        self.state.offset_x as f64 + self.state.viewport.width as f64 / 2.0,
                        self.state.offset_y as f64 + self.state.viewport.height as f64 / 2.0,
                    )
                })
        } else {
            (
                self.state.offset_x as f64 + self.state.viewport.width as f64 / 2.0,
                self.state.offset_y as f64 + self.state.viewport.height as f64 / 2.0,
            )
        };

        let mut best_candidate: Option<(GraphNode, f64)> = None;
        for (node, &position) in self.base_layout.node_positions.iter() {
            // Skip the current selection and the start/end nodes.
            if let Some(selected) = self.state.selected_block {
                if *node == selected
                    || Node::is_start_node(node.node_id)
                    || Node::is_end_node(node.node_id)
                {
                    continue;
                }
            }

            let candidate_point = position;

            // For vertical movement, only consider candidates that are nearly horizontally aligned.
            if matches!(direction, NavDirection::Up | NavDirection::Down) {
                let horizontal_threshold = 1.0;
                if (candidate_point.0 - current_point.0).abs() > horizontal_threshold {
                    continue;
                }
            }

            let is_candidate = match direction {
                NavDirection::Left => candidate_point.0 < current_point.0,
                NavDirection::Right => candidate_point.0 > current_point.0,
                NavDirection::Up => candidate_point.1 < current_point.1,
                NavDirection::Down => candidate_point.1 > current_point.1,
            };
            if !is_candidate {
                continue;
            }

            let dx = candidate_point.0 - current_point.0;
            let dy = candidate_point.1 - current_point.1;
            let distance = (dx * dx + dy * dy).sqrt();
            if distance == 0.0 {
                continue;
            }

            if let Some((_, best_distance)) = best_candidate {
                if distance < best_distance {
                    best_candidate = Some((*node, distance));
                }
            } else {
                best_candidate = Some((*node, distance));
            }
        }

        if let Some((new_selection, _)) = best_candidate {
            self.state.selected_block = Some(new_selection);
            self.center_on_block(new_selection);
        }
    }

    /// Select the block closest to the center of the viewport using coordinates from scaled_layout.
    pub fn select_center_block(&mut self) -> Option<GraphNode> {
        let center = (
            self.state.offset_x as f64 + self.state.viewport.width as f64 / 2.0,
            self.state.offset_y as f64 + self.state.viewport.height as f64 / 2.0,
        );
        let mut best_candidate: Option<(GraphNode, f64)> = None;
        for (node, &((start, y), (end, _))) in self.scaled_layout.labels.iter() {
            let candidate_center = ((start + end) / 2.0, y);
            let dx = candidate_center.0 - center.0;
            let dy = candidate_center.1 - center.1;
            let dist = (dx * dx + dy * dy).sqrt();
            best_candidate = match best_candidate {
                Some((_, best)) if dist < best => Some((*node, dist)),
                None => Some((*node, dist)),
                other => other,
            };
        }
        if let Some((node, _)) = best_candidate {
            self.state.selected_block = Some(node);
            Some(node)
        } else {
            None
        }
    }

    /// Update scroll offset based on the cursor position (world coordinates of the selected label).
    /// This method computes the world bounds from all labels and clamps the viewport's offset
    /// so that the cursor is centered when possible, but moves towards the viewport edges when near world bounds.
    pub fn update_scroll_for_cursor(&mut self, cursor_x: f64, cursor_y: f64) {
        // The tolerance_y parameter allows for flexibility on what's considered "centered" to avoid jitter,
        // it's a ratio of the viewport height.
        let margin = 5.0;
        let tolerance_y = 0.3;

        let vp_width = self.state.viewport.width as f64;
        let vp_height = self.state.viewport.height as f64;

        let mut xs = Vec::new();
        let mut ys = Vec::new();
        for ((x, _), (x2, _)) in self.scaled_layout.labels.values() {
            xs.push(*x);
            xs.push(*x2);
        }
        for ((_, y), (_, _)) in self.scaled_layout.labels.values() {
            ys.push(*y);
        }
        if xs.is_empty() || ys.is_empty() {
            return;
        }

        let world_min_x = xs.iter().cloned().fold(f64::INFINITY, f64::min) - margin;
        let world_max_x = xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max) + margin;
        let world_min_y = ys.iter().cloned().fold(f64::INFINITY, f64::min) - margin;
        let world_max_y = ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max) + margin;

        let world_width = world_max_x - world_min_x;
        let world_height = world_max_y - world_min_y;

        let desired_x = cursor_x - vp_width / 2.0;
        let desired_y = cursor_y - vp_height / 2.0;

        let new_offset_x = if world_width >= vp_width {
            desired_x.clamp(world_min_x, world_max_x - vp_width)
        } else {
            world_min_x - (vp_width - world_width) / 2.0
        };

        let new_offset_y = if world_height >= vp_height {
            desired_y.clamp(world_min_y, world_max_y - vp_height)
        } else {
            world_min_y - (vp_height - world_height) / 2.0
        };

        self.state.offset_x = new_offset_x.round() as i32;

        if (new_offset_y - self.state.offset_y as f64).abs() > tolerance_y * vp_height {
            self.state.offset_y = new_offset_y.round() as i32;
        }
    }
}

/// Truncate a string to a certain length, adding an ellipsis in the middle
fn inner_truncation(s: &str, target_length: u32) -> String {
    let input_length = s.chars().count() as u32;
    if input_length <= target_length {
        return s.to_string();
    } else if target_length < 5 {
        return "●".to_string(); // ○ is U+25CB; ● is U+25CF
    }
    // length - 3 because we need space for the ellipsis
    let left_len = (target_length - 3) / 2 + ((target_length - 3) % 2);
    let right_len = (target_length - 3) - left_len;

    format!(
        "{}...{}",
        &s[..left_len as usize],
        &s[input_length as usize - right_len as usize..]
    )
}

/// Clips a string to a specific window, indicating that it has been clipped.
/// - If the string is empty, it returns an empty string.
/// - If the string is shorter than the window, it returns the string.
/// - If the string is longer than the window, it clips the string and replaces the last character with a period.
/// - If the string is not within the window at all, it returns an empty string.
pub fn clip_label(
    label: &str,
    label_start: isize,
    window_start: isize,
    window_width: usize,
) -> String {
    if label.is_empty() {
        return "".to_string();
    }

    let label_end = label_start + label.chars().count() as isize - 1;
    let window_end = window_start + window_width as isize - 1;
    if label_end < window_start || label_start > window_end {
        return "".to_string();
    }

    if label_start >= window_start && label_end <= window_end {
        return label.to_string();
    }

    let mut clipped = label.to_string();

    // Process the right side first so we don't lose alignment:
    if label_end > window_end {
        let delta_right = label_end - window_end;
        // Make sure we don't try to cut in the middle of a multibyte character
        let character_cutoff = (label.chars().count() as isize - delta_right - 1) as usize;
        let byte_cutoff = label
            .char_indices()
            .nth(character_cutoff)
            .map(|(i, _)| i)
            .unwrap_or(label.len());
        clipped.replace_range(byte_cutoff.., "…");
    }
    if window_start > label_start {
        let delta_left = window_start - label_start;
        let character_cutoff = delta_left as usize + 1;
        let byte_cutoff = label
            .char_indices()
            .nth(character_cutoff)
            .map(|(i, _)| i)
            .unwrap_or(label.len());
        clipped.replace_range(..byte_cutoff, "…");
    }

    clipped
}

/// Clip a line given as two points to a specific window, also given by two points
/// - If the line is completely outside the window, it returns None.
/// - If the line is completely inside the window, it returns the original line.
/// - If the line is partially inside the window, it clips the line to the window.
///
/// This may be made more efficient through bitwise comparisons (see Cohen-Sutherland line clipping algorithm)
pub fn clip_line(
    (x1, y1): (f64, f64),   // Line start
    (x2, y2): (f64, f64),   // Line end
    (wx1, wy1): (f64, f64), // Window top left
    (wx2, wy2): (f64, f64), // Window bottom right
) -> Option<((f64, f64), (f64, f64))> {
    let mut t0 = 0.0;
    let mut t1 = 1.0;
    let dx = x2 - x1;
    let dy = y2 - y1;

    let clip = |p: f64, q: f64, t0: &mut f64, t1: &mut f64| -> bool {
        if p == 0.0 {
            return q >= 0.0;
        }
        let r = q / p;
        if p < 0.0 {
            if r > *t1 {
                return false;
            }
            if r > *t0 {
                *t0 = r;
            }
        } else {
            if r < *t0 {
                return false;
            }
            if r < *t1 {
                *t1 = r;
            }
        }
        true
    };

    if clip(-dx, x1 - wx1, &mut t0, &mut t1)
        && clip(dx, wx2 - x1, &mut t0, &mut t1)
        && clip(-dy, y1 - wy1, &mut t0, &mut t1)
        && clip(dy, wy2 - y1, &mut t0, &mut t1)
    {
        let nx1 = x1 + t0 * dx;
        let ny1 = y1 + t0 * dy;
        let nx2 = x1 + t1 * dx;
        let ny2 = y1 + t1 * dy;
        Some(((nx1, ny1), (nx2, ny2)))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
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

    #[allow(clippy::too_many_arguments)]
    fn test_clip_line_helper(
        x1: f64,
        y1: f64,
        x2: f64,
        y2: f64,
        wx1: f64,
        wy1: f64,
        wx2: f64,
        wy2: f64,
        expected: Option<((f64, f64), (f64, f64))>,
    ) {
        let clipped = clip_line((x1, y1), (x2, y2), (wx1, wy1), (wx2, wy2));
        assert_eq!(clipped, expected);
    }

    #[test]
    fn test_clip_line_outside() {
        test_clip_line_helper(0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, None);
    }

    #[test]
    fn test_clip_line_inside() {
        test_clip_line_helper(
            0.0,
            0.0,
            1.0,
            1.0,
            -1.0,
            -1.0,
            1.5,
            1.5,
            Some(((0.0, 0.0), (1.0, 1.0))),
        );
    }

    #[test]
    fn test_clip_line_partial() {
        test_clip_line_helper(
            0.0,
            0.0,
            1.0,
            1.0,
            0.5,
            0.5,
            1.5,
            1.5,
            Some(((0.5, 0.5), (1.0, 1.0))),
        );
    }

    #[test]
    fn test_clip_label_multibyte_character() {
        // str.len() counts bytes, not characters (this is a bug in the original implementation)
        let clipped = clip_label("●", 160, -6, 168);
        assert_eq!(clipped, "●");
    }

    #[test]
    fn test_clip_label_negative_offset() {
        //  -2   0 1 2 3 4 5 6 7 8 9
        //  [        A B C]D E
        let clipped = clip_label("ABCDE", 2, -2, 7);
        assert_eq!(clipped, "AB…");
    }

    #[test]
    fn test_clip_label_internal() {
        // 0 1 2 3 4 5 6 7 8 9
        //  [  A B C D E  ]
        let clipped = clip_label("ABCDE", 2, 1, 7);
        assert_eq!(clipped, "ABCDE");
    }

    #[test]
    fn test_clip_label_external() {
        // 0 1 2 3 4 5 6 7 8 9
        //     A B  [     ]
        let clipped = clip_label("AB", 2, 5, 3);
        assert_eq!(clipped, "");
    }

    #[test]
    fn test_clip_label_left() {
        // 0 1 2 3 4 5 6 7 8 9
        //     A B[C D E F G H]
        let clipped = clip_label("ABCDEFGH", 2, 4, 10);
        assert_eq!(clipped, "…DEFGH");
    }

    #[test]
    fn test_clip_label_right() {
        // 0 1 2 3 4 5 6 7 8 9
        //[    A B C D E]F G H
        let clipped = clip_label("ABCDEFGH", 2, 0, 7);
        assert_eq!(clipped, "ABCD…");
    }

    #[test]
    fn test_clip_label_both() {
        // 0 1 2 3 4 5 6 7 8 9
        //     A B[C D E]F G H
        let clipped = clip_label("ABCDEFGH", 2, 4, 3);
        assert_eq!(clipped, "…D…");
    }
}
