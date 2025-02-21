use crate::graph::{GraphEdge, GraphNode};
use crate::models::node;
use crate::models::node::Node;
use crate::models::sequence::Sequence;
use crate::views::block_layout::{BaseLayout, ScaledLayout};

use core::panic;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
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

/// Used for scrolling through the graph.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum NavDirection {
    Left,
    Right,
    Up,
    Down,
}

/// Holds parameters that don't change when you scroll.
/// - `label_width` = how many characters to show at most in each block label. If 0, labels are not shown.
/// - `scale` = data units per 1 terminal cell.  
///   - If `scale` = 1.0, each cell is 1 data unit.  
///   - If `scale` = 2.0, each cell is 2 data units (you see *more* data).  
///   - If `scale` = 0.5, each cell is 0.5 data units (you see *less* data, zoomed in).
/// - `aspect_ratio` = width / height of a terminal cell in data units.
/// - `line_offset_y` = how much to offset the lines vertically to align the braille characters with the labels
/// - `edge_style` = draw the edges as straight lines or as splines.
pub struct PlotParameters {
    pub label_width: u32,
    pub scale: u32,
    pub aspect_ratio: f32,
    pub line_offset_y: f64,
    pub edge_style: EdgeStyle,
}

pub enum EdgeStyle {
    Straight,
    Spline,
}

impl Default for PlotParameters {
    fn default() -> Self {
        PlotParameters {
            label_width: 11,
            scale: 4,
            aspect_ratio: 0.5,
            line_offset_y: 0.125,
            edge_style: EdgeStyle::Straight,
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
    pub world: ((f64, f64), (f64, f64)), // (min_x, min_y), (max_x, max_y)
    pub selected_block: Option<GraphNode>,
    pub first_render: bool,
}
impl Default for State {
    fn default() -> Self {
        State {
            offset_x: 0,
            offset_y: 0,
            viewport: Rect::new(0, 0, 0, 0),
            world: ((0.0, 0.0), (0.0, 0.0)),
            selected_block: None,
            first_render: true,
        }
    }
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
    view_block: Block<'a>,
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
            view_block: Block::default(),
        }
    }

    /// Refresh based on changed parameters or layout.
    pub fn refresh(&mut self) {
        self.scaled_layout
            .refresh(&self.base_layout, &self.parameters);
        self.state.world = self.compute_bounding_box();
    }

    pub fn set_block(&mut self, block: Block<'a>) {
        self.view_block = block;
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

    /// Center the viewport on a specific block.
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
    pub fn draw(&mut self, frame: &mut ratatui::Frame, area: Rect) {
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

        let canvas_block = self.view_block.clone();
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
                self.state.world = self.compute_bounding_box();
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
                    match self.parameters.edge_style {
                        EdgeStyle::Straight => {
                            if let Some(((x1c, y1c), (x2c, y2c))) = clip_line(
                                (x1, y1 + self.parameters.line_offset_y),
                                (x2, y2 + self.parameters.line_offset_y),
                                (self.state.offset_x as f64, self.state.offset_y as f64),
                                (
                                    (self.state.offset_x + self.state.viewport.width as i32) as f64,
                                    (self.state.offset_y + self.state.viewport.height as i32)
                                        as f64,
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
                        EdgeStyle::Spline => {
                            // Bezier curves are always contained within the box defined by their endpoints,
                            // so we reject any curves that don't have a bounding box that intersects the viewport.
                            if !rectangles_intersect(
                                (x1, y1 + self.parameters.line_offset_y),
                                (x2, y2 + self.parameters.line_offset_y),
                                (self.state.offset_x as f64, self.state.offset_y as f64),
                                (
                                    (self.state.offset_x + self.state.viewport.width as i32) as f64,
                                    (self.state.offset_y + self.state.viewport.height as i32)
                                        as f64,
                                ),
                            ) {
                                continue;
                            }
                            let num_points = ((x2.round() - x1.round() + 1.0) as u32).min(16); // Don't go too crazy
                            let curve_points = generate_cubic_bezier_curve(
                                (x1, y1 + self.parameters.line_offset_y),
                                (x2, y2 + self.parameters.line_offset_y),
                                num_points,
                            );

                            // Draw lines between consecutive points of the curve
                            for points in curve_points.windows(2) {
                                if let Some(((x1c, y1c), (x2c, y2c))) = clip_line(
                                    points[0],
                                    points[1],
                                    (self.state.offset_x as f64, self.state.offset_y as f64),
                                    (
                                        (self.state.offset_x + self.state.viewport.width as i32)
                                            as f64,
                                        (self.state.offset_y + self.state.viewport.height as i32)
                                            as f64,
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
                        }
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

    /// Check the viewport bounds against the world bounds and trigger expansion if needed.
    pub fn auto_expand(&mut self) {
        let ((x_min, _), (x_max, _)) = self.state.world;
        // Check if we're a screen width away from the left/right boundary and expand if needed
        // - if we can't expand any further, this will do nothing
        if (x_min as i32) > (self.state.offset_x - self.state.viewport.width as i32) {
            self.base_layout.expand_left();
        }
        if (x_max as i32) < (self.state.offset_x + 2 * self.state.viewport.width as i32) {
            self.base_layout.expand_right();
        }
    }

    /// Cycle through nodes in a specified direction.
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

        // Try to find a node in the specified direction closest to the current point.
        if let Some(new_selection) = self.find_closest_block_in_direction(current_point, direction)
        {
            self.state.selected_block = Some(new_selection);
            self.center_on_block(new_selection);
        } else {
            // If no valid node was found, scroll the viewport a bit to keep the user moving.
            let scroll_amount = match direction {
                NavDirection::Up => -3,
                NavDirection::Down => 3,
                // If left/right has no best candidate, do nothing for now
                _ => 0,
            };
            self.state.offset_y += scroll_amount;
        }
    }

    // Helper function to find the closest node in the given direction, skipping
    // the currently selected block and ignoring start/end dummy nodes.
    fn find_closest_block_in_direction(
        &self,
        current_point: (f64, f64),
        direction: NavDirection,
    ) -> Option<GraphNode> {
        let mut closest_candidate: Option<(GraphNode, f64)> = None;

        for (&node, &node_pos) in &self.base_layout.node_positions {
            // Skip the currently selected block and any start/end node
            if Some(node) == self.state.selected_block
                || Node::is_start_node(node.node_id)
                || Node::is_end_node(node.node_id)
            {
                continue;
            }

            let dx = node_pos.0 - current_point.0;
            let dy = node_pos.1 - current_point.1;

            // Depending on the direction, decide if this node is a candidate
            //   - Up/Down: only consider nodes that are vertically aligned
            //   - Left/Right: consider all nodes in the correct half of the screen
            if !match direction {
                NavDirection::Up => node_pos.1 < current_point.1 && dx.abs() < f64::EPSILON,
                NavDirection::Down => node_pos.1 > current_point.1 && dx.abs() < f64::EPSILON,
                NavDirection::Left => dx < 0.0,
                NavDirection::Right => dx > 0.0,
            } {
                continue;
            }

            // Calculate Euclidean distance
            let distance = (dx * dx + dy * dy).sqrt();

            // Keep track if it's closer than any previous candidate
            if let Some((_, best_dist)) = closest_candidate {
                // When scrolling horizontally, break ties by preferring the down direction
                // (otherwise it looks random to the user)
                if (direction == NavDirection::Left || direction == NavDirection::Right)
                    && (distance - best_dist).abs() < f64::EPSILON
                    && dy < 0.0
                {
                    closest_candidate = Some((node, distance));
                } else if distance < best_dist {
                    // No tie-breaking needed
                    closest_candidate = Some((node, distance));
                }
            } else {
                closest_candidate = Some((node, distance));
            }
        }

        // Return the node with the minimum distance in the chosen direction
        closest_candidate.map(|(n, _)| n)
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

    fn compute_bounding_box(&self) -> ((f64, f64), (f64, f64)) {
        let labels = &self.scaled_layout.labels;

        let mut xs = Vec::new();
        let mut ys = Vec::new();
        for ((x, _), (x2, _)) in labels.values() {
            xs.push(*x);
            xs.push(*x2);
        }
        for ((_, y), (_, _)) in labels.values() {
            ys.push(*y);
        }
        let world_min_x = xs.iter().cloned().fold(f64::INFINITY, f64::min);
        let world_max_x = xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let world_min_y = ys.iter().cloned().fold(f64::INFINITY, f64::min);
        let world_max_y = ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        ((world_min_x, world_min_y), (world_max_x, world_max_y))
    }

    /// Update scroll offset based on the cursor position (world coordinates of the selected label).
    /// This method computes the world bounds from all labels and clamps the viewport's offset.
    /// On the initial call (first_render), it remains centered. Afterwards, we allow the cursor
    /// to drift within a tolerance range vertically before scrolling.
    pub fn update_scroll_for_cursor(&mut self, cursor_x: f64, cursor_y: f64) {
        let margin = 10.0;
        let vp_width = self.state.viewport.width as f64;
        let vp_height = self.state.viewport.height as f64;
        let bandwidth = 0.4;

        let ((world_min_x, world_min_y), (world_max_x, world_max_y)) = self.state.world;
        let min_x = world_min_x - margin;
        let max_x = world_max_x + margin;
        let min_y = world_min_y - margin;
        let max_y = world_max_y + margin;

        let total_width = max_x - min_x;
        let total_height = max_y - min_y;

        // If it's the initial positioning, there is only one allowed position.
        if self.state.first_render {
            let desired_x = cursor_x - vp_width / 2.0;
            let desired_y = cursor_y - vp_height / 2.0;

            let new_offset_x = if total_width >= vp_width {
                desired_x.clamp(min_x, max_x - vp_width)
            } else {
                min_x - (vp_width - total_width) / 2.0
            };
            let new_offset_y = if total_height >= vp_height {
                desired_y.clamp(min_y, max_y - vp_height)
            } else {
                min_y - (vp_height - total_height) / 2.0
            };

            self.state.offset_x = new_offset_x.round() as i32;
            self.state.offset_y = new_offset_y.round() as i32;

            return;
        }

        // In later iterations we treat vertical and horizontal movement differently.
        // Horizontal still clamps to one point
        let desired_x = cursor_x - vp_width / 2.0;
        let new_offset_x = if total_width >= vp_width {
            desired_x.clamp(min_x, max_x - vp_width)
        } else {
            min_x - (vp_width - total_width) / 2.0
        };

        // Vertical is centering and clamping to a range of y-coordinates.
        let current_offset_y = self.state.offset_y as f64;
        let top_boundary = current_offset_y + bandwidth * vp_height;
        let bottom_boundary = current_offset_y + (1.0 - bandwidth) * vp_height;

        let mut desired_y = current_offset_y;
        if cursor_y < top_boundary {
            desired_y -= top_boundary - cursor_y;
        } else if cursor_y > bottom_boundary {
            desired_y += cursor_y - bottom_boundary;
        }

        // Clamp vertically
        if total_height >= vp_height {
            desired_y = desired_y.clamp(min_y, max_y - vp_height);
        } else {
            desired_y = min_y - (vp_height - total_height) / 2.0;
        }

        self.state.offset_x = new_offset_x.round() as i32;
        self.state.offset_y = desired_y.round() as i32;
    }

    pub fn handle_input(&mut self, key: KeyEvent) {
        // Scrolling through the graph
        match key.code {
            KeyCode::Left => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    self.state.offset_x -= self.state.viewport.width as i32 / 3;
                    self.unselect_if_not_visible();
                } else if key.modifiers.contains(KeyModifiers::ALT) {
                    self.state.offset_x -= 1;
                } else {
                    // Try to select center block if no block is selected
                    if self.state.selected_block.is_none() && self.select_center_block().is_none() {
                        // If we couldn't find a center block, move the offset directly
                        self.state.offset_x -= self.state.viewport.width as i32 / 3;
                    } else {
                        // Move selection if we have a block selected (either from before or just now)
                        self.move_selection(NavDirection::Left);
                    }
                }
            }
            KeyCode::Right => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    self.state.offset_x += self.state.viewport.width as i32 / 3;
                    self.unselect_if_not_visible();
                } else if key.modifiers.contains(KeyModifiers::ALT) {
                    self.state.offset_x += 1;
                } else if self.state.selected_block.is_none()
                    && self.select_center_block().is_none()
                {
                    self.state.offset_x += self.state.viewport.width as i32 / 3;
                } else {
                    self.move_selection(NavDirection::Right);
                }
            }
            KeyCode::Up => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    self.state.offset_y += self.state.viewport.height as i32 / 3;
                    self.unselect_if_not_visible();
                } else if key.modifiers.contains(KeyModifiers::ALT) {
                    self.state.offset_y += 1;
                } else if self.state.selected_block.is_none()
                    && self.select_center_block().is_none()
                {
                    self.state.offset_y += self.state.viewport.height as i32 / 3;
                } else {
                    self.move_selection(NavDirection::Down);
                }
            }
            KeyCode::Down => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    self.state.offset_y -= self.state.viewport.height as i32 / 3;
                    self.unselect_if_not_visible();
                } else if key.modifiers.contains(KeyModifiers::ALT) {
                    self.state.offset_y -= 1;
                } else if self.state.selected_block.is_none()
                    && self.select_center_block().is_none()
                {
                    self.state.offset_y -= self.state.viewport.height as i32 / 3;
                } else {
                    self.move_selection(NavDirection::Up);
                }
            }
            // Zooming in and out
            KeyCode::Char('+') | KeyCode::Char('=') => {
                // Increase how much of the sequence is shown in each block label.
                if self.parameters.label_width == u32::MAX {
                    self.parameters.scale += 2; // Even increments look better
                } else {
                    self.parameters.label_width = match self.parameters.label_width {
                        1 => 11,
                        11 => 100,
                        100 => u32::MAX,
                        _ => u32::MAX,
                    }
                }

                // If no block is selected, try to select the center block
                if self.state.selected_block.is_none() {
                    self.select_center_block();
                }

                // Recalculate the layout.
                self.scaled_layout
                    .refresh(&self.base_layout, &self.parameters);

                // Only center on block if we have one selected
                if let Some(block) = self.state.selected_block {
                    self.center_on_block(block);
                }
            }
            KeyCode::Char('-') | KeyCode::Char('_') => {
                // Decrease how much of the sequence is shown in each block label.
                if self.parameters.scale > 2 {
                    self.parameters.scale -= 2;
                } else {
                    self.parameters.label_width = match self.parameters.label_width {
                        u32::MAX => 100,
                        100 => 11,
                        11 => 1,
                        _ => 1,
                    };
                }

                if self.state.selected_block.is_none() {
                    self.select_center_block();
                }

                self.scaled_layout
                    .refresh(&self.base_layout, &self.parameters);

                if let Some(block) = self.state.selected_block {
                    self.center_on_block(block);
                }
            }
            KeyCode::Char('s') | KeyCode::Char('S') => {
                // Toggle between straight lines and splines
                // I ended up not liking them as much as I thought I would,
                // so it's not the default and I'm not documenting it in the status bar.
                self.parameters.edge_style = match self.parameters.edge_style {
                    EdgeStyle::Straight => EdgeStyle::Spline,
                    EdgeStyle::Spline => EdgeStyle::Straight,
                };
                self.scaled_layout
                    .refresh(&self.base_layout, &self.parameters);
            }
            _ => {}
        }
    }

    pub fn get_status_line() -> String {
        "◀ ▼ ▲ ▶ select blocks (+shift/alt to scroll) | +/- zoom".to_string()
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

/// Check if two rectangles intersect.
/// - Each rectangle is defined by any two opposite corners.
/// - Returns true if the rectangles overlap or touch, false otherwise.
pub fn rectangles_intersect(
    (x1, y1): (f64, f64), // First corner of rectangle 1
    (x2, y2): (f64, f64), // Opposite corner of rectangle 1
    (x3, y3): (f64, f64), // First corner of rectangle 2
    (x4, y4): (f64, f64), // Opposite corner of rectangle 2
) -> bool {
    // For each axis, one rectangle's maximum must be >= other's minimum
    // and one rectangle's minimum must be <= other's maximum
    x1.max(x2) >= x3.min(x4)
        && x3.max(x4) >= x1.min(x2)
        && y1.max(y2) >= y3.min(y4)
        && y3.max(y4) >= y1.min(y2)
}

/// Generate a cubic bezier curve between two points A and B, given a resolution value.
/// - Control points 0 and 3 are equal to A and B.
/// - Control point 1 is halfway between A and B, at the same height as A.
/// - Control point 2 is halfway between A and B, at the same height as B.
///
/// The function returns resolution + 2 points:
/// - First point is exactly A
/// - Last point is exactly B
/// - For resolution=0: returns [A, B]
/// - For resolution=1: returns [A, midpoint, B] where midpoint is the true curve midpoint at t=0.5
/// - For resolution>1: returns [A, ...resolution points along the curve..., B]
pub fn generate_cubic_bezier_curve(
    a: (f64, f64),
    b: (f64, f64),
    num_points: u32,
) -> Vec<(f64, f64)> {
    let (ax, ay) = a;
    let (bx, by) = b;
    // Define control points following Graphviz's style:
    // p0: a, p1: midpoint between a and b at the same height as a,
    // p2: midpoint between a and b at the same height as b, p3: b
    let p0 = a;
    let p1 = (((ax + bx) / 2.0), ay);
    let p2 = (((ax + bx) / 2.0), by);
    let p3 = b;

    let mut points = Vec::with_capacity(num_points as usize);
    // First point is exactly a
    points.push(a);

    // Calculate intermediate points
    for i in 1..num_points - 1 {
        let t = i as f64 / ((num_points - 1) as f64);
        let one_minus_t = 1.0_f64 - t;
        let x = one_minus_t.powi(3) * p0.0
            + 3.0_f64 * one_minus_t.powi(2) * t * p1.0
            + 3.0_f64 * one_minus_t * t.powi(2) * p2.0
            + t.powi(3) * p3.0;
        let y = one_minus_t.powi(3) * p0.1
            + 3.0_f64 * one_minus_t.powi(2) * t * p1.1
            + 3.0_f64 * one_minus_t * t.powi(2) * p2.1
            + t.powi(3) * p3.1;
        points.push((x, y));
    }

    // Last point is exactly b
    points.push(b);
    points
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

    #[test]
    fn test_rectangles_intersect() {
        // Overlapping rectangles (corners in standard order)
        assert!(rectangles_intersect(
            (0.0, 0.0),
            (2.0, 2.0),
            (1.0, 1.0),
            (3.0, 3.0)
        ));

        // Overlapping rectangles (corners in reverse order)
        assert!(rectangles_intersect(
            (2.0, 2.0),
            (0.0, 0.0), // bottom-right to top-left
            (3.0, 3.0),
            (1.0, 1.0) // bottom-right to top-left
        ));

        // Touching rectangles (edge) with mixed corner order
        assert!(rectangles_intersect(
            (2.0, 2.0),
            (0.0, 0.0), // reversed
            (2.0, 0.0),
            (4.0, 2.0) // standard
        ));

        // Touching rectangles (corner) with diagonal corners
        assert!(rectangles_intersect(
            (0.0, 2.0),
            (2.0, 0.0), // top-right to bottom-left
            (2.0, 2.0),
            (4.0, 4.0) // standard
        ));

        // Non-intersecting rectangles with mixed corners
        assert!(!rectangles_intersect(
            (1.0, 1.0),
            (0.0, 0.0), // reversed
            (3.0, 3.0),
            (2.0, 2.0) // reversed
        ));

        // One rectangle inside another with diagonal corners
        assert!(rectangles_intersect(
            (0.0, 4.0),
            (4.0, 0.0), // top-right to bottom-left
            (1.0, 2.0),
            (2.0, 1.0) // bottom-right to top-left
        ));
    }
}
