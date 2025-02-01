use crate::graph::{GraphEdge, GraphNode};
use crate::models::{
    block_group::BlockGroup,
    block_group_edge::{AugmentedEdge, BlockGroupEdge},
    collection::Collection,
    edge::{Edge, GroupBlock},
    node::Node,
    path::Path,
    path_edge::PathEdge,
    sample::Sample,
    strand::Strand,
};

use chrono::offset;
use gb_io::seq;
use itertools::Itertools;
use noodles::vcf::header;
use petgraph::dot::Dot;
use petgraph::graphmap::DiGraphMap;
use petgraph::prelude::GraphMap;
use petgraph::stable_graph::StableDiGraph;
use ratatui::Frame;
use rusqlite::Connection;
use ruzstd::blocks;

use core::panic;
use std::collections::{HashMap, HashSet, BTreeMap};
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Stdout, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crossterm::{
    event::{self, Event, KeyCode, KeyEvent},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout, Rect},
    style::Color,
    widgets::canvas::{Canvas, Line, Points},
    widgets::{Block, Borders, Paragraph},
    Terminal,
    symbols
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
    offset_x: f64,
    offset_y: f64,
    scale: f32,
    aspect_ratio: f32,
    max_label: u32,
}

/// Holds data for the viewer.
struct Viewer {
    edges: Vec<(u32, u32)>, // Block ID pairs
    labels: HashMap<u32, String>, // Block ID to label
    coordinates: HashMap<u32, (i32, i32)>, // Block ID to (x, y) coordinates
    scroll: ScrollState}

/// Convert the coordinates from the layout algorithm to canvas coordinates.
/// This depends on widest label in each rank.
fn stretch_layout(
    layout: &Vec<(u32, (f64, f64))>,
    labels: &HashMap<u32, String>,
    scale: f32,
    aspect_ratio: f32) -> HashMap<u32, (i32, i32)> {

    // A hierarchical layout groups nodes by rank into layers (top to bottom)
    // which for indentically sized nodes corresponds to an y-coordinate.
    // Assign a rank (1 .. n) to each node id by grouping the layout by y-coordinate.
    let mut ranks: HashMap<u32, usize> = HashMap::new();
    let mut current_rank: usize = 0;
    let mut current_y = layout[0].1 .1;
    for (id, (_, y)) in layout.iter().sorted_by(|a, b| a.1 .1.partial_cmp(&b.1 .1).unwrap()) {
        if *y != current_y {
            current_rank += 1;
            current_y = *y;
        }
        ranks.insert(*id, current_rank);
    }
    // Store the current rank as the maximum rank
    let max_rank = current_rank;

    // Transpose, scale and round the coordinates from the layout algorithm
    // (layout is top-to-bottom, we want left-to-right)
    let scale_x = (scale * aspect_ratio) as f64;
    let scale_y = scale as f64;
    let layout: Vec<(u32, (i32, i32))> = layout.iter()
                                            .map(|(id, (x, y))|
                                            (*id, 
                                            ((*y * scale_x).round() as i32,
                                             (*x * scale_y).round() as i32))).collect();

    // If all labels are length 1, we can skip the stretching
    if labels.values().all(|label| label.len() == 1) {
        // Return the layout as a hashmap
        return layout.iter().map(|(id, (x, y))| (*id, (*x as i32, *y as i32))).collect();
    }

    // Initially each layer is one unit wide, we stretch it to fit the labels.
    // Note that the inter-layer distance does not change anymore at this point
    let mut layer_widths: Vec<u32> = vec![3; max_rank + 1];
    for (block_id, label) in labels {
        let rank = *ranks.get(&block_id).unwrap();
        // Make sure each layer is at least 1 units wide for the block + 2 for padding on each side
        let width = std::cmp::max(1, label.len() as u32) + 2;
        layer_widths[rank] = std::cmp::max(layer_widths[rank], width);
    }

    // Loop over the ranks and:
    // - determine the cumulative padding needed to stretch the layers up to that rank
    let layer_offsets: Vec<u32> = layer_widths.iter()
        .enumerate()
        .scan(0, |accumulator, (rank, &width)| {
            *accumulator += width - 1;
            Some(*accumulator)})
         .collect();

    // Loop over the blocks and:
    // - increment the new x-coordinate by the offset for the rank
    // - horizontally center the block in the rank
    // - store the results in a hashmap instead of a vector
    let mut coordinates = HashMap::new();
    for (block_id, (x, y)) in layout {
        let rank = ranks.get(&block_id).unwrap();
        let layer_offset = layer_offsets[rank.clone()];
        let layer_width = layer_widths[rank.clone()];
        let label_width = labels.get(&block_id).unwrap().len() as u32;
        let centering_offset = (layer_width - label_width) / 2;
        coordinates.insert(block_id, (x + layer_offset as i32 + centering_offset as i32, 
                                            y));
    }

    coordinates
} 

/// Convert the x-y offset between two coordinate schemes so that the same node stays centered
/// - This is used when zooming in and out

fn convert_offset(
    offset_x: i32,
    offset_y: i32,
    old_coordinates: &HashMap<u32, (i32, i32)>,
    new_coordinates: &HashMap<u32, (i32, i32)>,
    frame: Frame,
) -> (i32, i32) {
    //  - Find the node (block) closest to the old center 
    let center_x = offset_x + frame.area().width as i32 / 2;
    let center_y = offset_y + frame.area().height as i32 / 2;
    let mut closest_node = 0;
    let mut closest_distance = i32::MAX;
    for (block_id, (x, y)) in old_coordinates {
        let distance = (x - center_x).abs() + (y - center_y).abs();
        if distance < closest_distance {
            closest_distance = distance;
            closest_node = *block_id;
        }
    }

    // - Record the position of that node in each coordinate system
    let (old_x, old_y) = old_coordinates.get(&closest_node).unwrap();
    let (new_x, new_y) = new_coordinates.get(&closest_node).unwrap();

    //  - Set the new offset so that, in screen coordinates, the node stays in place
    let new_offset_x = offset_x + (old_x - new_x);
    let new_offset_y = offset_y + (old_y - new_y);

    (new_offset_x, new_offset_y)
}

/// Truncate a string to a certain length, adding an ellipsis in the middle
fn inner_truncation(s: &str, length: u32) -> String {
    if length < 3 {
        panic!("Length must be at least 3");
    }
    if s.len() as u32 <= length {
        return s.to_string();
    }
    // length - 3 because we need space for the ellipsis
    let left_len = (length-3) / 2 + ((length-3)  % 2);
    let right_len = (length-3) - left_len;
    
    format!("{}...{}", &s[..left_len as usize], 
        &s[s.len() - right_len as usize..])
}



fn make_labels(blocks: &Vec<GroupBlock>, length: u32) -> HashMap<u32, String> {
    let mut labels = HashMap::new();
    for block in blocks {
        if block.sequence().is_empty() {
            labels.insert(block.id as u32, "?".to_string());
        } else if length < 3 {
            labels.insert(block.id as u32, "•".to_string());
        } else {
            labels.insert(block.id as u32, inner_truncation(&block.sequence(), length));
        }
    }
    labels
}



/// Draw the canvas. Note that we compute the coordinate range
/// from the current `offset` and the widget `size`.
fn draw_scrollable_canvas(frame: &mut ratatui::Frame, viewer: &Viewer) {

    let plot_area = Rect::new(frame.area().left(), frame.area().top(), frame.area().width, frame.area().height);

    // The top-left corner of our view is (offset_x, offset_y).
    let x_start = viewer.scroll.offset_x;
    let x_end = x_start + plot_area.width as f64 - 3.0; // -1 because width is 1-based, -2 because of the border
    let y_start = viewer.scroll.offset_y;
    let y_end = y_start + plot_area.height as f64 - 3.0;


    // Create the canvas
    let canvas = Canvas::default()
        .block(
            Block::default()
                .title("Graph Viewer (arrows=scroll, +/-=zoom, q=quit)")
                .borders(Borders::ALL),
        )
        // Adjust the x_bounds and y_bounds by the scroll offsets.
        .x_bounds([x_start, x_end])
        .y_bounds([y_start, y_end])
        .paint(|ctx| {
            // Draw a line for each block pair
            for (block_id1, block_id2) in &viewer.edges {
                let (x1, y1) = viewer.coordinates.get(block_id1).unwrap();
                let (x2, y2) = viewer.coordinates.get(block_id2).unwrap();

                // Length of the label of the first block
                let label_len = viewer.labels.get(&block_id1).unwrap().len() as i32;
                ctx.draw(&Line {
                    x1: (x1.clone() + label_len) as f64 + 0.5, // Leaving extra space for aesthetics
                    y1: y1.clone() as f64 ,
                    x2: (x2.clone() - 1) as f64,
                    y2: y2.clone() as f64 ,
                    color: Color::White,
                });
            }


            // Draw the labels as text.
            for (block_id, (x, y)) in &viewer.coordinates {
                let label = viewer.labels.get(block_id).unwrap();
                ctx.print(*x as f64, *y as f64, label.clone());
                //ctx.draw(&Points {
                //    coords: &vec![(*x as f64, *y as f64)],
                //    color: Color::Red,
                //});
            }
        });

    //frame.render_widget(canvas, frame.area());
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

    // Build a block graph
    let (block_graph, block_pairs) = Edge::build_graph(&edges, &blocks);

    //println!("block_graph: {:#?}", &block_graph);

    // Convert the keys of block_pairs from (i64, i64) to (u32, u32)
    let block_pairs_u32 = block_pairs
        .keys()
        .map(|(b1, b2)| (*b1 as u32, *b2 as u32))
        .collect::<Vec<_>>();

    let layouts = from_edges(
        &block_pairs_u32,
        &Config {
            vertex_spacing: 8.0,
            dummy_vertices: false,
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
    let label_length = 20;
    let scale: f32 = 1.0;
    let aspect_ratio: f32 = 2.0;
    let label_length = 10;
    let labels = make_labels(&blocks, label_length);
    let coordinates = stretch_layout(&layout, &labels, scale, aspect_ratio);
    // Calculate the center point of the coordinates
    let center_x = coordinates.values().map(|(x, _)| x).sum::<i32>() as f64 / coordinates.len() as f64;
    let center_y = coordinates.values().map(|(_, y)| y).sum::<i32>() as f64 / coordinates.len() as f64;
    // Set the initial offset so that the center point is in the center of the terminal
    let offset_x = center_x - (terminal.get_frame().area().width as f64 / 2.0);
    let offset_y = center_y - (terminal.get_frame().area().height as f64 / 2.0) ;

    let mut viewer = Viewer {
        edges: block_pairs_u32,
        labels: labels,
        coordinates: coordinates,
        scroll: ScrollState {
            offset_x: offset_x.round() as f64,
            offset_y: offset_y.round() as f64,
            scale: scale,
            aspect_ratio: aspect_ratio,
            max_label: label_length
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
            draw_scrollable_canvas(frame, &viewer);
        })?;

        // Handle input
        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));
        if crossterm::event::poll(timeout)? {
            if let Event::Key(KeyEvent { code, .. }) = event::read()? {
                match code {
                    KeyCode::Char('q') => {
                        // Exit on 'q'
                        break;
                    }
                    // Scroll the data window
                    KeyCode::Left => {
                        viewer.scroll.offset_x -= 5.0;
                    }
                    KeyCode::Right => {
                        viewer.scroll.offset_x += 5.0;
                    }
                    KeyCode::Up => {
                        viewer.scroll.offset_y += 5.0;
                    }
                    KeyCode::Down => {
                        viewer.scroll.offset_y -= 5.0;
                    }
                     KeyCode::Char('+') => {
                        // Increase how much of the sequence is shown in each block label: 10, 100, 1000.
                        // After 1000 it just becomes the full length, and any further zooming in starts increasing 
                        // the scale.
                        viewer.scroll.max_label = match viewer.scroll.max_label {
                            10 => 100,
                            100 => 1000,
                            1000 => u32::MAX,
                            _ => u32::MAX,
                        };
                        if viewer.scroll.max_label == u32::MAX {
                            viewer.scroll.scale *= 2.0;
                        }

                        // Regenerate labels and coordinates
                        viewer.labels = make_labels(&blocks, viewer.scroll.max_label);
                        let new_coordinates = stretch_layout(&layout, &viewer.labels, viewer.scroll.scale, viewer.scroll.aspect_ratio);
                        /*// Convert the offset so that the same block stays centered
                        terminal.draw(|frame| {
                            let (new_offset_x, new_offset_y) = convert_offset(viewer.scroll.offset_x as f64, 
                                viewer.scroll.offset_y as f64, 
                                &viewer.coordinates,
                                &new_coordinates, 
                                frame);
                            viewer.scroll.offset_x = new_offset_x;
                            viewer.scroll.offset_y = new_offset_y;
                        })?;*/


                    }    
                    // Zoom out => bigger scale => see more data => "less magnified"
                    KeyCode::Char('-') => {
                        // Switch to a lower horizontal zoom level
                        viewer.scroll.max_label = std::cmp::max(viewer.scroll.max_label - 1, 1);
                        // Regenerate labels and coordinates
                        viewer.labels = make_labels(&blocks, viewer.scroll.max_label);
                        let new_coordinates = stretch_layout(&layout, &viewer.labels, viewer.scroll.scale, viewer.scroll.aspect_ratio);
                        /* 
                        // Convert the offset so that the same block stays centered
                        let (new_offset_x, new_offset_y) = convert_offset(
                            viewer.scroll.offset_x as i32, 
                            viewer.scroll.offset_y as i32, 
                            &viewer.coordinates,
                            &new_coordinates, 
                            terminal.get_frame());
                        viewer.scroll.offset_x = new_offset_x as f64;
                        viewer.scroll.offset_y = new_offset_y as f64;*/
                    } 
                    _ => {}
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
    let mut stdout = terminal.backend_mut();
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

