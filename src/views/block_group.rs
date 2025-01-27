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

use gb_io::seq;
use itertools::Itertools;
use noodles::vcf::header;
use petgraph::graphmap::DiGraphMap;
use petgraph::prelude::GraphMap;
use rusqlite::Connection;

use std::collections::{HashMap, HashSet};
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
    scale: f64,
}

/// Holds data for the viewer.
struct Viewer {
    blocks: Vec<GroupBlock>,
    block_pairs: Vec<(u32, u32)>,        // Real edges and boundary edges
    coordinates: Vec<(u32, (f64, f64))>, // Block ID to (x, y) coordinates
    graph_width: f64,
    graph_height: f64,
    margin: f64,
    scroll: ScrollState,
}

/// Draw the canvas. Note that we compute the coordinate range
/// from the current `offset`, the `scale`, and the widget `size`.
fn draw_scrollable_canvas(frame: &mut ratatui::Frame, viewer: &Viewer) {
    // The number of cells in each direction is obtained from the frame area.
    // We multiply by `scale` (data units per cell) to find how wide/tall
    // the visible data range is.
    let data_width = frame.area().width as f64 * viewer.scroll.scale;
    let data_height = frame.area().height as f64 * viewer.scroll.scale;

    // The top-left corner of our view is (offset_x, offset_y).
    let x_start = viewer.scroll.offset_x;
    let x_end = x_start + data_width;
    let y_start = viewer.scroll.offset_y;
    let y_end = y_start + data_height;

    // Get the edges corresponding to the blocks
    //let edges = viewer.graph.edges().collect::<Vec<_>>();

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
            // TODO: change this to iterate over the blocks instead, using the petgraph graph to find outgoing edges
            // (this will allow me to properly offset the lines to account for block width)
            for (block_id1, block_id2) in &viewer.block_pairs {
                let (x1, y1) = viewer
                    .coordinates
                    .iter()
                    .find(|(id, _)| id == block_id1)
                    .unwrap()
                    .1;
                let (x2, y2) = viewer
                    .coordinates
                    .iter()
                    .find(|(id, _)| id == block_id2)
                    .unwrap()
                    .1;
                ctx.draw(&Line {
                    x1: x1 + 6.0 * viewer.scroll.scale, // Hack that assumes the width of the label
                    y1: y1 / 3.0, // Squish the y-direction because cells are taller than they are wide
                    x2: x2 - 6.0 * viewer.scroll.scale,
                    y2: y2 / 3.0,
                    color: Color::White,
                });
            }

            // Draw the block IDs from the layout as text.
            // TODO: do this as widgets instead of directly on the canvas
            for (block_id, (x, y)) in &viewer.coordinates {
                // Get the sequence of the block
                // TODO: figure out why not all blocks from the coordinates are in the viewer.blocks (starter block?)
                if let Some(block) = viewer.blocks.iter().find(|b| b.node_id == *block_id as i64) {
                    let sequence = block.sequence();
                    // If the sequence is longer than 9 characters, only show the first and last 3, separated by ellipsis
                    let seq_label = if sequence.len() > 9 {
                        format!("{}...{}", &sequence[0..3], &sequence[sequence.len() - 3..])
                    } else {
                        sequence
                    };
                    // Center the label on the block coordinate, taking into account the current zoom level
                    let label_offset_x = seq_label.len() as f64 / 2.0 * viewer.scroll.scale;
                    let label_offset_y = 0.5 * viewer.scroll.scale;

                    // Squish the y-direction because cells are taller than they are wide
                    ctx.print(*x - label_offset_x, (*y - label_offset_y) / 3.0, seq_label);
                } else {
                    ctx.print(*x, *y, "X");
                }
            }
        });
    frame.render_widget(canvas, frame.area());

    // FIXME: the widgets below aren't rendered in the proper coordinate reference compared to the canvas
    /*for (block_id, (x, y)) in &viewer.coordinates {
        // Get the sequence of the block
        // TODO: figure out why not all blocks from the coordinates are in the viewer.blocks (starter block?)
        if let Some(block) = viewer.blocks.iter().find(|b| b.node_id == *block_id as i64) {
            let sequence = block.sequence();
            // If the sequence is longer than 9 characters, only show the first and last 3, separated by ellipsis
            let seq_label = if sequence.len() > 9 {
                format!("{}...{}", &sequence[0..3], &sequence[sequence.len()-3..])
            } else {
                sequence
            };

            // Draw the block ID from the layout as text.
            let widget_width = (seq_label.len() as f64 + 2.0);
            let widget_height = 3.0;
            let area = Rect {
                x: (x_start + (*x - widget_width/2.0)) as u16,
                y: (y_start + (*y - widget_height/2.0)) as u16,
                width: widget_width as u16,
                height: widget_height as u16,
            };
            frame.render_widget(Paragraph::new(seq_label).block(Block::bordered()), area);

        }

    }
    */
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
            dummy_vertices: true,
            ..Default::default()
        },
    );

    // Confirm that there is only one layout, which means that the graph is connected
    assert_eq!(layouts.len(), 1);

    let (layout, width, height) = &layouts[0];

    // Get coordinates from the layout by transposing the x and y values (the layout is vertical by default)
    let coordinates = layout
        .iter()
        .map(|(id, (x, y))| (*id as u32, (*y, *x)))
        .collect::<Vec<_>>();
    // Width and height are also transposed
    let (width, height) = (*height, *width);

    //println!("Coordinates: {:?}", coordinates);
    //println!("width: {width}, height: {height}");

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Initialize viewer state
    let margin = 2.0;
    let mut viewer = Viewer {
        blocks: blocks,
        block_pairs: block_pairs_u32,
        coordinates: coordinates,
        graph_width: width,
        graph_height: height,
        margin: margin,
        scroll: ScrollState {
            offset_x: margin,
            offset_y: margin,
            scale: 1.0,
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
                    // Zoom in => smaller scale => see less data => "magnified"
                    KeyCode::Char('+') => {
                        // If we interpret "zoom in" as "fewer data units per cell":
                        // scale *= 0.9 => 0.9 units per cell (less data).
                        viewer.scroll.scale *= 0.9;
                        if viewer.scroll.scale < 0.01 {
                            viewer.scroll.scale = 0.01;
                        }
                    }
                    // Zoom out => bigger scale => see more data => "less magnified"
                    KeyCode::Char('-') => {
                        // scale *= 1.1 => 1.1 units per cell (more data).
                        viewer.scroll.scale *= 1.1;
                        if viewer.scroll.scale > 100.0 {
                            viewer.scroll.scale = 100.0;
                        }
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
