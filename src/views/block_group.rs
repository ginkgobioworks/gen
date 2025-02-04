use crate::models::{
    block_group_edge::BlockGroupEdge,
    edge::Edge,
    sample::Sample,
};
use crate::views::block_group_viewer::{Viewer, PlotParameters, ScrollState};
use crate::views::block_layout::ScaledLayout;

use rusqlite::Connection;

use core::panic;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::time::{Duration, Instant};
use std::u32;

use log::info;

use crossterm::{
    event::{self, KeyCode, KeyModifiers, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    init,
    backend::{Backend, CrosstermBackend},
    layout::{Rect, Constraint},
    widgets::{Block, Borders, Paragraph, Wrap, Clear},
    widgets::canvas::{Canvas, Line},
    Terminal,
    style::{Style, Color, Stylize},
    text::{Span, Text},
};
use rust_sugiyama::{configure::Config, from_edges};



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
    // (TODO: fix/remedy core dump when there are too many blocks)
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


    // TODO: the below code should go into its own module

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let mut terminal = ratatui::init();


    // Initialize viewer state
    let initial_parameters = PlotParameters {
        label_width: 11,
        scale: 2,
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
    //let center_x = scaled_layout.labels.values().map(|(_, x, _)| x).sum::<u32>() as f64 / scaled_layout.labels.len() as f64;
    let center_y = scaled_layout.labels.values().map(|(_, _, y)| y).sum::<u32>() as f64 / scaled_layout.labels.len() as f64;

    // Set the initial offset so that graph is vertically centered, and left-aligned with some margin
    // We haven't set up a canvas yet, so we don't know more about the plot area
    let initial_scroll_state = ScrollState {
        offset_x: -5,
        offset_y: center_y.round() as i32 - (terminal.get_frame().area().height as f64 / 2.0).round() as i32,
        selected_block: None,
    };

    let mut viewer = Viewer {
        layout: scaled_layout,
        scroll: initial_scroll_state,
        plot_area: Rect::default(),
        plot_parameters: PlotParameters {
            label_width: 11,
            scale: 2,
            aspect_ratio: 0.5
        },
    };

    // Basic event loop
    let tick_rate = Duration::from_millis(100);
    let mut last_tick = Instant::now();
    let mut show_panel = false; // Informational popup
    loop {
        // Draw the UI
        terminal.draw(|frame| {
            /// A layout consisting of a canvas and a status bar, with optionally a panel
            /// - The canvas is where the graph is drawn
            /// - The status bar is where the controls are displayed
            /// - The panel is a scrollable paragraph that can be toggled on and off
            
            let status_bar_height: u16 = 1;

            // Define the layouts
            // The outer layout is a vertical split between the canvas and the status bar
            // The inner layout is a vertical split between the canvas and the panel

            let outer_layout = ratatui::layout::Layout::default()
                .direction(ratatui::layout::Direction::Vertical)
                .constraints(vec!
                    [
                        ratatui::layout::Constraint::Min(1),
                        ratatui::layout::Constraint::Length(status_bar_height),
                    ]
                )
                .split(frame.area());

            let inner_layout = ratatui::layout::Layout::default()
                .direction(ratatui::layout::Direction::Vertical)
                .constraints(vec!
                    [
                        Constraint::Percentage(75),
                        Constraint::Percentage(25),
                    ]
                )
                .split(outer_layout[0]);

            
            let canvas_area = if show_panel { inner_layout[0] } else { outer_layout[0] }; 
            let panel_area = if show_panel { inner_layout[1] } else { Rect::default() };
            let status_bar_area = outer_layout[1];

            // Ask the viewer to paint the canvas
            viewer.paint_canvas(frame, canvas_area);

            // Status bar
            let status_bar_contents = format!(
                "{:width$}",
                " Controls: arrows(+shift)=scroll, +/-=zoom, tab=select blocks, return=show information on block, q=quit",
                width = status_bar_area.width as usize);

            let status_bar = Paragraph::new(Text::styled(status_bar_contents, 
                Style::default().bg(Color::DarkGray).fg(Color::White)));

            frame.render_widget(status_bar, status_bar_area);

            // Panel
            if show_panel {
                let panel_block = Block::default().borders(Borders::ALL);
                
                let content_area = panel_block.inner(panel_area);
                let mut panel_text = Text::from("No content found");

                // Get information about the currently selected block
                if viewer.scroll.selected_block.is_some() {
                    let selected_block = viewer.scroll.selected_block.unwrap();
                    let block = blocks.iter().find(|block| block.id == selected_block as i64).unwrap();
                    panel_text = Text::from(format!("Block ID: {}\nNode ID: {}\nStart: {}\nEnd: {}\n", 
                        block.node_id, block.id, block.start, block.end));
                } 

                let panel_content = Paragraph::new(panel_text)
                    .wrap(Wrap { trim: true })
                    .scroll((0, 0))
                    .style(Style::default().bg(Color::Reset));
  
                // First clear the area, then render
                frame.render_widget(Clear, content_area);
                frame.render_widget(panel_content, content_area);
            }
        })?;

        // Handle input
        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));
        if crossterm::event::poll(timeout)? {
            if let event::Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    // Exit on q
                    if key.code == KeyCode::Char('q') {
                        break;
                    }
                    match key.code {
                    // Scrolling through the graph
                        KeyCode::Left => {
                            // Scroll left
                            if key.modifiers == KeyModifiers::SHIFT {
                                viewer.scroll.offset_x -= 10;
                            } else {
                                viewer.scroll.offset_x -= 1;
                            }
                            // Forget the selected block if it's not visible
                            viewer.unselect_if_not_visible();
                        }
                        KeyCode::Right => {
                            // Scroll right
                            if key.modifiers == KeyModifiers::SHIFT {
                                viewer.scroll.offset_x += 10;
                            } else {
                                viewer.scroll.offset_x += 1;
                            }
                            viewer.unselect_if_not_visible();
                        }
                        KeyCode::Up => {
                            // Scroll up
                            if key.modifiers == KeyModifiers::SHIFT {
                                viewer.scroll.offset_y += 10;
                            } else {
                                viewer.scroll.offset_y += 1;
                            }
                            viewer.unselect_if_not_visible();
                        }
                        KeyCode::Down => {
                            // Scroll down
                            if key.modifiers == KeyModifiers::SHIFT {
                                viewer.scroll.offset_y -= 10;
                            } else {
                                viewer.scroll.offset_y -= 1;
                            }
                            viewer.unselect_if_not_visible();
                        }
                    // Zooming in and out
                        KeyCode::Char('+') | KeyCode::Char('=') => {
                            // Increase how much of the sequence is shown in each block label: 0 vs 11 vs 100 characters.
                            // 11 was picked as the default because it results in symmetrical labels.
                            // After 100 it just becomes the full length.
                            if viewer.plot_parameters.label_width == u32::MAX {
                                // If we're already maximizing the length, start increasing the scale
                                viewer.plot_parameters.scale += 1;
                            } else {
                                // Otherwise, increase the label width
                                viewer.plot_parameters.label_width = match viewer.plot_parameters.label_width {
                                    0 => 11,
                                    11 => 100,
                                    100 => u32::MAX,
                                    _ => u32::MAX,
                                }
                            };
                            // Recalculate the layout
                            viewer.layout.rescale(&viewer.plot_parameters);

                            // Center the viewport on the selected block if there is one
                            if let Some(selected_block) = viewer.scroll.selected_block {
                                viewer.center_on_block(selected_block);
                            }
                        }
                        KeyCode::Char('-') | KeyCode::Char('_') => {
                            // Decrease how much of the sequence is shown in each block label: 11 vs 100 vs 1000 characters.
                            // 11 was picked as the default because it results in symmetrical labels.
                            // After 1000 it just becomes the full length.

                            if viewer.plot_parameters.scale > 1 {
                                // Decrease the scale if we're not at the minimum scale (1)
                                viewer.plot_parameters.scale -= 1;
                            } else {
                                // If we're at the minimum scale, start decreasing the label width
                                viewer.plot_parameters.label_width = match viewer.plot_parameters.label_width {
                                    u32::MAX => 100,
                                    100 => 11,
                                    11 => 1,
                                    _ => 1,
                                };
                            }
                            viewer.layout.rescale(&viewer.plot_parameters);
                            if let Some(selected_block) = viewer.scroll.selected_block {
                                viewer.center_on_block(selected_block);
                            }
                        }
                    // Performing actions on blocks
                        KeyCode::Tab => {
                            // Cycle through visible blocks in the viewport
                            viewer.cycle_blocks(false);
                        }
                        KeyCode::BackTab => {
                            // Reverse cycle through visible blocks in the viewport
                            viewer.cycle_blocks(true);
                        }
                        KeyCode::Esc => {
                            // If we have a popup open, close it, otherwise unselect the selected block
                            if show_panel {
                                show_panel = false;
                            } else {
                                viewer.scroll.selected_block = None;
                            }
                        }    
                        KeyCode::Enter => {
                            // Show information on the selected block
                            //viewer.show_block_info();
                            if let Some(selected_block) = viewer.scroll.selected_block {
                                show_panel = true;

                            }
                            
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
 
}