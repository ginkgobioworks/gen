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
use petgraph::graphmap::DiGraphMap;
use petgraph::prelude::GraphMap;
use ratatui::Frame;
use rusqlite::Connection;
use ruzstd::blocks;

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
};
use rust_sugiyama::{configure::Config, from_edges};

/// Holds current scrolling offset and a scale factor for data units per terminal cell.
///
/// - `scale` = data units per 1 terminal cell.  
///   - If `scale` = 1.0, each cell is 1 data unit.  
///   - If `scale` = 2.0, each cell is 2 data units (you see *more* data).  
///   - If `scale` = 0.5, each cell is 0.5 data units (you see *less* data, zoomed in).
struct ScrollState {
    offset_x: f64,
    offset_y: f64,
    zoom_level: u8
}

/// Holds data for the viewer.
struct Viewer {
    edges: Vec<(u32, u32)>, // Block ID pairs
    labels: HashMap<u32, String>, // Block ID to label
    coordinates: HashMap<u32, (f64, f64)>, // Block ID to (x, y) coordinates
    scroll: ScrollState}

/// Convert the coordinates from the layout algorithm to canvas coordinates.
/// This depends on widest label in each rank.
fn stretch_layout(
    layout: &Vec<(u32, (f64, f64))>,
    label_lengths: &HashMap<u32, u32>,) -> HashMap<u32, (f64, f64)> {
    // Find the widest label in each rank (y-coordinate because layouts are top to bottomn)
    let mut rank_widths = BTreeMap::new();
    for (block_id, (_, y)) in layout {
        let rank = *y as i32; // Convert to integer to use as key, since floats are hard to compare
        let width = *label_lengths.get(block_id).unwrap() + 1;
        let max_width = rank_widths.entry(rank).or_insert(1);
        *max_width = std::cmp::max(*max_width, width);
    }

    // Build a BTreeMap of cumulative rank widths (keys are rank, values are cumulative width)
    let cumulative_widths: BTreeMap<i32, u32> = rank_widths.clone()
        .into_iter()
        .sorted_by(|a, b| a.0.partial_cmp(&b.0).unwrap())
        .scan(0, |accumulator, (rank, width)| {
            *accumulator += width;
            Some((rank, *accumulator))
        })
        .collect();

    // Loop over the blocks and:
    // - transpose the coordinates so we go to a left-to-right layout
    // - increment the new x-coordinate by the cumulative width of that rank - 1
    // - horizontally center the block in the rank
    // - store the results in a hashmap instead of a vector
    let mut coordinates = HashMap::new();
    for (block_id, (x, y)) in layout {
        let rank = *y as i32;
        let rank_offset = *cumulative_widths.get(&rank).unwrap() as f64 - 1.0;
        let centering_offset = (rank_widths.get(&rank).unwrap() - label_lengths.get(block_id).unwrap()) as f64 / 2.0;
        coordinates.insert(*block_id, (*y + rank_offset + centering_offset, 
                                            *x));
    }

    coordinates
}

/// Convert the x-y offset between two coordinate schemes so that the same node stays centered
/// - This is used when zooming in and out

fn convert_offset( offset_x: f64, offset_y: f64,
                   old_coordinates: &HashMap<u32, (f64, f64)>, 
                   new_coordinates: &HashMap<u32, (f64, f64)>, 
                   frame: Frame) -> (f64, f64) {
    //  - Find the node (block) closest to the horizontal center 
    let center_x = offset_x + frame.area().width as f64 / 2.0;
    let center_y = offset_y + frame.area().height as f64 / 2.0;
    let mut closest_node = 0;
    let mut closest_distance = f64::MAX;
    for (block_id, (x, y)) in old_coordinates {
        let distance = (x - center_x).abs() + (y - center_y).abs();
        if distance < closest_distance {
            closest_distance = distance;
            closest_node = *block_id;
        }
    }
    //  - Set the new offset so that the same node is central again
    // (we're not taking into account the width of the label here)
    let (new_x, new_y) = new_coordinates.get(&closest_node).unwrap();
    let new_offset_x = new_x - frame.area().width as f64 / 2.0;
    let new_offset_y = new_y - frame.area().height as f64 / 2.0;
    (new_offset_x, new_offset_y)
}

/// Generate labels for the blocks depending on the zoom level and the block sequence.
/// - If the zoom level is 1, show only one character: o.
/// - If the zoom level is 2, show the 3 first and 3 last characters of the block sequence, separated by an ellipsis.
/// - If the zoom level is 3, show the entire block sequence.
fn make_labels(blocks: &Vec<GroupBlock>, zoom_level: u8) -> HashMap<u32, String> {
    let mut labels = HashMap::new();
    for block in blocks {
        // If the block is a boundary block, just show a >
        if block.sequence().is_empty() {
            labels.insert(block.id as u32, ">".to_string());
        } else {
            let label = match zoom_level {
                1 => "o".to_string(),
                2 => {
                    let sequence = block.sequence();
                    if sequence.len() > 6 {
                        format!("{}...{}", &sequence[0..3], &sequence[sequence.len() - 3..])
                    } else {
                        sequence
                    }
                }
                3 => block.sequence(),
                _ => panic!("Invalid zoom level: {}", zoom_level),
            };
            labels.insert(block.id as u32, label);
        }
    }
    labels
}



/// Draw the canvas. Note that we compute the coordinate range
/// from the current `offset` and the widget `size`.
fn draw_scrollable_canvas(frame: &mut ratatui::Frame, viewer: &Viewer) {
    // The top-left corner of our view is (offset_x, offset_y).
    let x_start = viewer.scroll.offset_x;
    let x_end = x_start + frame.area().width as f64;
    let y_start = viewer.scroll.offset_y;
    let y_end = y_start + frame.area().height as f64;

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

                ctx.draw(&Line {
                    x1: x1.clone() + viewer.labels.get(&block_id1).unwrap().len() as f64 - 1.0,
                    y1: y1.clone() + 0.5,
                    x2: x2.clone() - 1.0,
                    y2: y2.clone() + 0.5,
                    color: Color::White,
                });
            }

            // Draw the labels as text.
            for (block_id, (x, y)) in &viewer.coordinates {
                let label = viewer.labels.get(block_id).unwrap();
                ctx.print(*x, *y, label.clone());
            }
        });
    frame.render_widget(canvas, frame.area());

   
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
    let mut zoom_level = 2;
    let labels = make_labels(&blocks, zoom_level);
    let label_lengths: HashMap<u32, u32> = labels.iter().map(|(&id, s)| (id, s.len() as u32)).collect();
    let coordinates = stretch_layout(&layout, &label_lengths);
    // Calculate the center point of the coordinates
    let center_x = coordinates.values().map(|(x, _)| x).sum::<f64>() / coordinates.len() as f64;
    let center_y = coordinates.values().map(|(_, y)| y).sum::<f64>() / coordinates.len() as f64;
    // Set the initial offset so that the center point is in the center of the terminal
    let offset_x = center_x - terminal.get_frame().area().width as f64 / 2.0;
    let offset_y = center_y - terminal.get_frame().area().height as f64 / 2.0;

    let mut viewer = Viewer {
        edges: block_pairs_u32,
        labels: labels,
        coordinates: coordinates,
        scroll: ScrollState {
            offset_x: offset_x,
            offset_y: offset_y,
            zoom_level: zoom_level
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
                        viewer.scroll.offset_x -= 1.0;
                    }
                    KeyCode::Right => {
                        viewer.scroll.offset_x += 1.0;
                    }
                    KeyCode::Up => {
                        viewer.scroll.offset_y += 1.0;
                    }
                    KeyCode::Down => {
                        viewer.scroll.offset_y -= 1.0;
                    }
                    KeyCode::Char('+') => {
                        // Switch to a higher zoom level, but max out at 3
                        viewer.scroll.zoom_level = std::cmp::min(viewer.scroll.zoom_level + 1, 3);
                        // Regenerate labels and coordinates
                        viewer.labels = make_labels(&blocks, viewer.scroll.zoom_level);
                        let label_lengths: HashMap<u32, u32> = viewer.labels.iter().map(|(&id, s)| (id, s.len() as u32)).collect();
                        let new_coordinates = stretch_layout(&layout, &label_lengths);
                        // Convert the offset so that the same block stays centered
                        let (new_offset_x, new_offset_y) = convert_offset(viewer.scroll.offset_x, 
                            viewer.scroll.offset_y, 
                            &viewer.coordinates,
                            &new_coordinates, 
                            terminal.get_frame());
                        viewer.scroll.offset_x = new_offset_x;
                        viewer.scroll.offset_y = new_offset_y;
                    }   
                    // Zoom out => bigger scale => see more data => "less magnified"
                    KeyCode::Char('-') => {
                        // Switch to a lower zoom level, but min out at 1
                        viewer.scroll.zoom_level = std::cmp::max(viewer.scroll.zoom_level - 1, 1);
                        // Regenerate labels and coordinates
                        viewer.labels = make_labels(&blocks, viewer.scroll.zoom_level);
                        let label_lengths: HashMap<u32, u32> = viewer.labels.iter().map(|(&id, s)| (id, s.len() as u32)).collect();
                        let new_coordinates = stretch_layout(&layout, &label_lengths);
                        // Convert the offset so that the same block stays centered
                        let (new_offset_x, new_offset_y) = convert_offset(viewer.scroll.offset_x, 
                            viewer.scroll.offset_y, 
                            &viewer.coordinates,
                            &new_coordinates, 
                            terminal.get_frame());
                        viewer.scroll.offset_x = new_offset_x;
                        viewer.scroll.offset_y = new_offset_y;
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
