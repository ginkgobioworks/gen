use crate::models::{
    block_group_edge::BlockGroupEdge,
    edge::Edge,
    sample::Sample,
};

use crossterm::event::KeyEventKind;
use itertools::Itertools; // for tuple_windows
use rusqlite::Connection;

use core::panic;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::io::Write;
use std::time::{Duration, Instant};
use std::u32;

use log::info;


use crossterm::{
    event::{self, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::Rect,
    widgets::canvas::{Canvas, Line},
    widgets::{Block, Borders},
    Terminal,
    style::Color,
};
use rust_sugiyama::{configure::Config, from_edges};

/// Holds current scrolling offset and a zoom factors for data units per terminal cell.
///
/// - `scale` = data units per 1 terminal cell.  
///   - If `scale` = 1.0, each cell is 1 data unit.  
///   - If `scale` = 2.0, each cell is 2 data units (you see *more* data).  
///   - If `scale` = 0.5, each cell is 0.5 data units (you see *less* data, zoomed in).
/// - `aspect_ratio` = width / height of a terminal cell in data units.
/// - `block_len` = how much of the sequence to show in each block label.
/// 
struct ScrollState {
    offset_x: i32,
    offset_y: i32
}

/// Holds data for the viewer.
struct Viewer {
    layout: ScaledLayout, // Coordinates and labels
    scroll: ScrollState,
    plot_area: Rect, 
    plot_parameters: PlotParameters,
}

/// Holds the parameters for plotting the graph.
struct PlotParameters {
    label_width: u32, // Truncate each label to this width
    scaling: u32, // Stretch the edges by this factor, taking into account the aspect ratio
    aspect_ratio: f32, // Scale differently in x and y directions
}

/// Holds processed and scaled layout data.
/// - `lines` = pairs of coordinates for each edge.
/// - `labels` = truncated sequences or symbols for each block.
/// - `highlight_[a|b]` = block ID or (block ID, coordinate) to highlight in color A or B.
/// The raw layout from the Sugiyama algorithm is processed as follow:
/// - The coordinates are rounded to the nearest integer and transposed to go from top-to-bottom to left-to-right.
/// - Each block is assigned a layer (or rank) based on its y-coordinate.
/// - The width of each layer is determined by the widest label in that layer.
/// - The distance between layers is scaled horizontally and vertically 
struct ScaledLayout {
    lines: Vec<((f64, f64), (f64, f64))>, // Pairs of coordinates for each edge
    labels: HashMap<u32, (String, u32, u32)>, // K:Block ID, V: label, x, y
    highlight_a: Option<(u32, Option<(u32, u32)>)>, // Block ID or (Block ID, coordinate) to highlight in color A
    highlight_b: Option<(u32, Option<(u32, u32)>)>, // Block ID or (Block ID, coordinate) to highlight in color B
    // Input data that is stored but not supposed to be used directly
    #[doc(hidden)]
    _edges: Vec<(u32, u32)>, // Block ID pairs
    _raw_layout: Vec<(u32, (f64, f64))>, // Raw layout from the Sugiyama algorithm
    _sequences: Option<HashMap<u32, String>>, // Block ID to sequence (full length)

}

impl ScaledLayout {
    fn new(
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
        let scale_x = parameters.scaling as f64;
        let scale_y = parameters.scaling as f64 * parameters.aspect_ratio as f64;
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
                    let source_coord = layout.get(source).map(|&(x, y)| (x as f64, y as f64)).unwrap();
                    let target_coord = layout.get(target).map(|&(x, y)| (x as f64, y as f64)).unwrap();
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
                info!("Processing layer: {:#?}", current_layer);
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
            let source_x = *source_x as f64 + source_label.len() as f64 + 0.5;
            let source_y = *source_y as f64;
            let target_x = *target_x as f64 - 1.0;
            let target_y = *target_y as f64;
            ((source_x, source_y), (target_x, target_y))
            })
            .collect();

    }
}


/// Convert the x-y offset between two coordinate schemes so that the same node stays centered
/// - This is used when zooming in and out

fn convert_offset(
    offset_x: i32,
    offset_y: i32,
    old_coordinates: &HashMap<u32, (i32, i32)>,
    new_coordinates: &HashMap<u32, (i32, i32)>,
    plot_area: Rect,
    focus_block: Option<u32>
) -> (i32, i32) {
    // Calculate the center of the plot area in the old coordinate system
    let center_x = offset_x + plot_area.width as i32 / 2;
    let center_y = offset_y + plot_area.height as i32 / 2;

    // If a focus block is not specified, find the block closest to the center of the plot area
    let selected_block = focus_block.or_else(|| {
        // Find the node closest to the center of the plot area
        let mut closest_node = 0;
        let mut closest_distance = i32::MAX;
        for (block_id, (x, y)) in old_coordinates {
            let distance = (x - center_x).abs() + (y - center_y).abs();
            if distance < closest_distance {
                closest_distance = distance;
                closest_node = *block_id;
            }
        }
        Some(closest_node)
    });

    // Change the offset so that position 1 of the selected block stays in the exact same spot
    // in the new coordinate system
    let selected_block = selected_block.unwrap();
    let (old_x, old_y) = old_coordinates.get(&selected_block).unwrap();
    let (new_x, new_y) = new_coordinates.get(&selected_block).unwrap();
    let new_offset_x = offset_x + (old_x - new_x);
    let new_offset_y = offset_y + (old_y - new_y);

    (new_offset_x, new_offset_y)
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






/// Draw the canvas, ensuring a 1 to 1 mapping between data units and terminal cells.
fn draw_scrollable_canvas(frame: &mut ratatui::Frame, viewer: &Viewer) {

    let plot_area = viewer.plot_area;

    // Ratatui block.inner method could avoid the manual accounting shown below

    // The top-left corner of our view is (offset_x, offset_y).
    let x_start = viewer.scroll.offset_x;
    let x_end = x_start + plot_area.width as i32 - 3; // -1 because width is 1-based, -2 because of the border
    let y_start = viewer.scroll.offset_y;
    let y_end = y_start + plot_area.height as i32 - 3;

    // Create the canvas
    let canvas = Canvas::default()
        .block(
            Block::default()
                .title("Graph Viewer (arrows=scroll, +/-=zoom, q=quit)")
                .borders(Borders::ALL),
        )
        // Adjust the x_bounds and y_bounds by the scroll offsets.
        .x_bounds([x_start as f64, x_end as f64])
        .y_bounds([y_start as f64, y_end as f64])
        .paint(|ctx| {
            // Draw the lines described in the processed layout
            for ((x1, y1), (x2, y2)) in &viewer.layout.lines {
                ctx.draw(&Line {
                    x1: { *x1 },
                    y1: { *y1 },
                    x2: { *x2 },
                    y2: { *y2 },
                    color: Color::White,
                });
            }
            // Print the labels
            for (block_id, (label, x, y)) in &viewer.layout.labels {
                ctx.print(*x as f64, *y as f64, label.clone());
            }

        });
    frame.render_widget(canvas, plot_area);   
}

pub fn view_block_group(
    conn: &Connection,
    name: &str,
    sample_name: Option<String>,
    collection_name: &str,
) -> Result<(), Box<dyn Error>> {
    let mut edge_set = HashSet::new();

    let sample_block_groups =
        Sample::get_block_groups(conn, collection_name, sample_name.as_deref());

    let block_group = sample_block_groups.iter().find(|&bg| bg.name == name);

    if block_group.is_none() {
        panic!(
            "No block group found with name {} and sample {:?} in collection {} ",
            name, sample_name, collection_name
        );
    }

    let block_group_id = block_group.unwrap().id;

    let block_group_edges = BlockGroupEdge::edges_for_block_group(conn, block_group_id);
    edge_set.extend(block_group_edges);

    let mut edges = edge_set.into_iter().collect::<Vec<_>>();

    let mut blocks = Edge::blocks_from_edges(conn, &edges);

    // Panic if there are no blocks
    if blocks.is_empty() {
        panic!("No blocks found for block group {}", name);
    }

    blocks.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    let boundary_edges = Edge::boundary_edges_from_sequences(&blocks);
    edges.extend(boundary_edges.clone());

    //println!("edges: {:#?}", &edges[0..3]);
    //println!();
    //println!("blocks: {:#?}", &blocks[0..3]);
    //println!();

    // TODO: somehow there are edges missing (particularly from the start node))

    // Build a block graph
    let (block_graph, block_pairs) = Edge::build_graph(&edges, &blocks);

    //println!("block_graph: {:#?}", &block_graph);

    // Convert the keys of block_pairs from (i64, i64) to (u32, u32)
    let block_pairs_u32 = block_pairs
        .keys()
        .map(|(b1, b2)| (*b1 as u32, *b2 as u32))
        .collect::<Vec<_>>();

    // Run the Sugiyama layout algorithm
    // TODO: fix/remedy core dump when there are too many blocks)
    info!("Running Sugiyama layout algorithm");
    let layouts = from_edges(
        &block_pairs_u32,
        &Config {
            vertex_spacing: 8.0,
            dummy_vertices: true,
            ..Default::default()
        },
    );

    // Confirm that there is only one layout, which means that the graph is connected
    assert_eq!(layouts.len(), 1);

    // Store that one layout and convert the block ids from usize to u32
    let layout: Vec<(u32, (f64, f64))> = layouts[0].0.iter().map(|(id, (x, y))| (*id as u32, (*x, *y))).collect();

    // Confirm that every block has a corresponding layout
    for block in &blocks {
        assert!(layout.iter().any(|(id, _)| *id == block.id as u32), "Block ID {} not found in layout", block.id);
    }

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Initialize viewer state
    // TODO: set up the viewer with sensible defaults and add parameters later
    // or borrow parameters instead
    // 
    let initial_parameters = PlotParameters {
        label_width: 11,
        scaling: 2,
        aspect_ratio: 0.5
    };

    // The sugiyama layout was raw and unprocessed, so we need to stretch it to account for the labels
    // TODO: refactor this so that we work with sequence lengths rather than the actual sequence,
    // so that we don't need the actual sequences in memory to make layout calculations.
    let sequences = blocks.iter()
        .map(|block| (block.id as u32, block.sequence()))
        .collect::<HashMap<u32, String>>();

    let scaled_layout = ScaledLayout::new(layout, block_pairs_u32, &initial_parameters, Some(sequences));

    // Calculate the center point of the coordinates from the labels in the ScaledLayout
    let center_x = scaled_layout.labels.values().map(|(_, x, _)| x).sum::<u32>() as f64 / scaled_layout.labels.len() as f64;
    let center_y = scaled_layout.labels.values().map(|(_, _, y)| y).sum::<u32>() as f64 / scaled_layout.labels.len() as f64;

    // Set the initial offset so that the center point is in the center of the terminal
    // We haven't set up a canvas yet, so we don't know more about the plot area
    let initial_scroll_state = ScrollState {
        offset_x: center_x.round() as i32 - (terminal.get_frame().area().width as f64 / 2.0).round() as i32,
        offset_y: center_y.round() as i32 - (terminal.get_frame().area().height as f64 / 2.0).round() as i32,
    };

    let mut viewer = Viewer {
        layout: scaled_layout,
        scroll: initial_scroll_state,
        plot_area: Rect::default(),
        plot_parameters: PlotParameters {
            label_width: 11,
            scaling: 2,
            aspect_ratio: 0.5
        },
    };

    // Basic event loop
    let tick_rate = Duration::from_millis(100);
    let mut last_tick = Instant::now();
    loop {
        // Draw the UI
        terminal.draw(|frame| {
            let viewer = &mut viewer;
            // Here we just have a single widget, so we use the entire frame.
            // If you have multiple widgets, use `Layout` to split the frame area.
            viewer.plot_area = Rect::new(frame.area().left(), frame.area().top(), frame.area().width, frame.area().height);
            draw_scrollable_canvas(frame, viewer);
            
            //let status_bar = Paragraph::new(Text::styled("Hello, world!", Style::default().bg(Color::DarkGray).fg(Color::White)));
            //let status_bar_area = Rect::new(frame.area().left(), frame.area().bottom()-1, frame.area().width, 1);
            //status_bar.render(status_bar_area, frame.buffer_mut());
        })?;

        // Handle input
        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));
        if crossterm::event::poll(timeout)? {
            if let event::Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') => {
                            // Exit on 'q'
                            break;
                        }
                        KeyCode::Left => {
                            // Scroll left
                            if key.modifiers == KeyModifiers::SHIFT {
                                viewer.scroll.offset_x -= 10;
                            } else {
                                viewer.scroll.offset_x -= 1;
                            }
                        }
                        KeyCode::Right => {
                            // Scroll right
                            if key.modifiers == KeyModifiers::SHIFT {
                                viewer.scroll.offset_x += 10;
                            } else {
                                viewer.scroll.offset_x += 1;
                            }
                        }
                        KeyCode::Up => {
                            // Scroll up
                            viewer.scroll.offset_y += 1;
                        }
                        KeyCode::Down => {
                            // Scroll down
                            viewer.scroll.offset_y -= 1;
                        }
                        KeyCode::Char('+') | KeyCode::Char('=') => {
                            // Increase how much of the sequence is shown in each block label: 0 vs 11 vs 100 characters.
                            // 11 was picked as the default because it results in symmetrical labels.
                            // After 100 it just becomes the full length.
                            viewer.plot_parameters.label_width = match viewer.plot_parameters.label_width {
                                0 => 11,
                                11 => 100,
                                100 => u32::MAX,
                                _ => u32::MAX,
                            };
                            // Once we're at the full length, start increasing the scale instead
                            if viewer.plot_parameters.label_width == u32::MAX {
                                viewer.plot_parameters.scaling += 1;
                            }
                            viewer.layout.rescale(&viewer.plot_parameters);
                        }
                        KeyCode::Char('-') | KeyCode::Char('_') => {
                            // Decrease how much of the sequence is shown in each block label: 11 vs 100 vs 1000 characters.
                            // 11 was picked as the default because it results in symmetrical labels.
                            // After 1000 it just becomes the full length.

                            if viewer.plot_parameters.scaling > 1 {
                                // Decrease the scale if we're not at the minimum scale (1)
                                viewer.plot_parameters.scaling -= 1;
                            } else {
                                // If we're at the minimum scale, start decreasing the label width
                                viewer.plot_parameters.label_width = match viewer.plot_parameters.label_width {
                                    u32::MAX => 100,
                                    100 => 11,
                                    11 => 0,
                                    _ => 0,
                                };
                            }
                            viewer.layout.rescale(&viewer.plot_parameters);
                        }
                        _ => {}
                    }
                }
                
            }
        }
        // Update tick
        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }
    }

    // Clean up terminal
    disable_raw_mode()?;
    let stdout = terminal.backend_mut();
    execute!(stdout, LeaveAlternateScreen)?;
    Ok(())
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
        assert_eq!(truncated, "he…ld");
    }

    #[test]
    fn test_inner_truncation_truncate_to_even_length() {
        let s = "hello world";
        let truncated = inner_truncation(s, 6);
        println!("{}", truncated);
        assert_eq!(truncated, "hel…ld");
    }

    #[test]
    #[should_panic(expected = "Length must be at least 3")]
    fn test_inner_truncation_panic_on_length_two() {
        let s = "hello";
        inner_truncation(s, 2);
    }
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
            scaling: 1,
            aspect_ratio: 1.0
        };
        let scaled_layout = ScaledLayout::new(raw_layout, edges, &parameters, None);
        info!("scaled_layout: {:#?}", scaled_layout.labels);


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
            scaling: 10,
            aspect_ratio: 1.0
        };
        let scaled_layout = ScaledLayout::new(raw_layout, edges, &parameters, None);
        info!("scaled_layout: {:#?}", scaled_layout.labels);

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
            scaling: 1,
            aspect_ratio: 1.0
        };
        let scaled_layout = ScaledLayout::new(raw_layout, edges, &parameters, Some(full_labels));
        info!("scaled_layout: {:#?}", scaled_layout.labels);

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
            scaling: 1,
            aspect_ratio: 1.0
        };
        let scaled_layout = ScaledLayout::new(raw_layout, edges, &parameters, Some(full_labels));
        info!("scaled_layout: {:#?}", scaled_layout.labels);

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
            scaling: 1,
            aspect_ratio: 1.0
        };
        let scaled_layout = ScaledLayout::new(raw_layout, edges, &parameters, Some(full_labels));

        let expected_labels = HashMap::from([
            (0, ("ABCDEFGH".to_string(), 0, 5)),
            (1, ("IJKLMNOP".to_string(), 18, 5))]);
        assert_eq!(scaled_layout.labels, expected_labels);

        let expected_lines = vec![((8., 5.), (17., 5.))];
        assert_eq!(scaled_layout.lines, expected_lines);
    }