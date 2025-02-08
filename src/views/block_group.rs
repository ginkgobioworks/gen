use crate::models::{
    block_group::BlockGroup, block_group_edge::BlockGroupEdge, edge::Edge, node::Node,
    traits::Query,
};

use crate::graph::{GraphEdge, GraphNode};
use crate::views::block_group_viewer::{PlotParameters, ScrollState, Viewer};
use crate::views::block_layout::{BaseLayout, ScaledLayout};

use rusqlite::{params, Connection};

use core::panic;
use log::{info, warn};
use std::collections::HashMap;
use std::error::Error;
use std::time::{Duration, Instant};

use crossterm::{
    event::{self, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use petgraph::graph::NodeIndex;
use petgraph::graphmap::DiGraphMap;
use petgraph::stable_graph::StableDiGraph;
use petgraph::visit::Bfs;
use petgraph::Direction;
use ratatui::{
    layout::{Constraint, Rect},
    style::{Color, Style},
    text::Text,
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};
use rust_sugiyama::{configure::Config, from_graph};



pub fn view_block_group(
    conn: &Connection,
    name: &str,
    sample_name: Option<String>,
    collection_name: &str,
) -> Result<(), Box<dyn Error>> {
    // Get the block group for two cases: with and without a sample
    let block_group = if let Some(ref sample_name) = sample_name {
        BlockGroup::get(conn, "select * from block_groups where collection_name = ?1 AND sample_name = ?2 AND name = ?3", 
                        params![collection_name, sample_name, name])
    } else {
        // modified version:
        BlockGroup::get(conn, "select * from block_groups where collection_name = ?1 AND sample_name is null AND name = ?2", params![collection_name, name])
    };

    if block_group.is_err() {
        panic!(
            "No block group found with name {} and sample {:?} in collection {} ",
            name,
            sample_name.clone().unwrap_or_else(|| "null".to_string()),
            collection_name
        );
    }

    let block_group_id = block_group.unwrap().id;

    let mut edges = BlockGroupEdge::edges_for_block_group(conn, block_group_id);

    let mut blocks = Edge::blocks_from_edges(conn, &edges);
    info!("{} blocks found", blocks.len());

    // Panic if there are no blocks
    if blocks.is_empty() {
        panic!("No blocks found for block group {}", name);
    }

    blocks.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    let boundary_edges = Edge::boundary_edges_from_sequences(&blocks);
    edges.extend(boundary_edges.clone());

    // TODO: somehow there are edges missing (particularly from the start node))

    // Build a block graph
    let (block_graph, _block_pairs) = Edge::build_graph(&edges, &blocks);


    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let mut terminal = ratatui::init();

    /* 

    // The sugiyama layout was raw and unprocessed, so we need to stretch it to account for the sequence labels
    // Pass the plotting parameters to the ScaledLayout constructor
    let scaled_layout = ScaledLayout::new(&base_layout, &initial_parameters);

    // Calculate the center point of the coordinates from the labels in the ScaledLayout
    //let center_x = scaled_layout.labels.values().map(|(_, x, _)| x).sum::<u32>() as f64 / scaled_layout.labels.len() as f64;
    let center_y = scaled_layout
        .labels
        .values()
        .map(|((_, y), _)| y)
        .sum::<f64>()
        / scaled_layout.labels.len() as f64;

    // Set the initial offset so that graph is vertically centered, and left-aligned with some margin
    // We haven't set up a canvas yet, so we don't know more about the plot area
    let initial_scroll_state = ScrollState {
        offset_x: -5,
        offset_y: center_y.round() as i32
            - (terminal.get_frame().area().height as f64 / 2.0).round() as i32,
        selected_block: None,
    };
*/

    let mut viewer = Viewer::new(&block_graph, conn, PlotParameters::default());

    // Basic event loop
    let tick_rate = Duration::from_millis(100);
    let mut last_tick = Instant::now();
    let mut show_panel = false; // Informational popup
    loop {
        // Draw the UI
        terminal.draw(|frame| {
            // A layout consisting of a canvas and a status bar, with optionally a panel
            // - The canvas is where the graph is drawn
            // - The status bar is where the controls are displayed
            // - The panel is a scrollable paragraph that can be toggled on and off
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
                    panel_text = Text::from(format!("Block ID: {}\nNode ID: {}\nStart: {}\nEnd: {}\n", 
                        selected_block.block_id, selected_block.node_id, selected_block.sequence_start, selected_block.sequence_end));
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
                                viewer.scroll.offset_x -= viewer.plot_area.width as i32 / 3;
                            } else {
                                viewer.scroll.offset_x -= 1;
                            }
                            // Forget the selected block if it's not visible
                            viewer.unselect_if_not_visible();
                        }
                        KeyCode::Right => {
                            // Scroll right
                            if key.modifiers == KeyModifiers::SHIFT {
                                viewer.scroll.offset_x += viewer.plot_area.width as i32 / 3;
                            } else {
                                viewer.scroll.offset_x += 1;
                            }
                            viewer.unselect_if_not_visible();
                        }
                        KeyCode::Up => {
                            // Scroll up
                            if key.modifiers == KeyModifiers::SHIFT {
                                viewer.scroll.offset_y += viewer.plot_area.height as i32 / 3;
                            } else {
                                viewer.scroll.offset_y += 1;
                            }
                            viewer.unselect_if_not_visible();
                        }
                        KeyCode::Down => {
                            // Scroll down
                            if key.modifiers == KeyModifiers::SHIFT {
                                viewer.scroll.offset_y -= viewer.plot_area.height as i32 / 3;
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
                            if viewer.parameters.label_width == u32::MAX {
                                // If we're already maximizing the length, start increasing the scale
                                viewer.parameters.scale += 1;
                            } else {
                                // Otherwise, increase the label width
                                viewer.parameters.label_width =
                                    match viewer.parameters.label_width {
                                        1 => 11,
                                        11 => 100,
                                        100 => u32::MAX,
                                        _ => u32::MAX,
                                    }
                            };
                            // Recalculate the layout
                            viewer.scaled_layout.refresh(&viewer.base_layout, &viewer.parameters);

                            // Center the viewport on the selected block if there is one
                            if let Some(selected_block) = viewer.scroll.selected_block {
                                viewer.center_on_block(selected_block);
                            }
                        }
                        KeyCode::Char('-') | KeyCode::Char('_') => {
                            // Decrease how much of the sequence is shown in each block label: 11 vs 100 vs 1000 characters.
                            // 11 was picked as the default because it results in symmetrical labels.
                            // After 1000 it just becomes the full length.

                            if viewer.parameters.scale > 2 {
                                // Decrease the scale if we're not at the minimum scale (2 because we use a 0.5 aspect ratio)
                                viewer.parameters.scale -= 1;
                            } else {
                                // If we're at the minimum scale, start decreasing the label width
                                viewer.parameters.label_width =
                                    match viewer.parameters.label_width {
                                        u32::MAX => 100,
                                        100 => 11,
                                        11 => 1,
                                        _ => 1,
                                    };
                            }
                            viewer.scaled_layout.refresh(&viewer.base_layout, &viewer.parameters);
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
                            // Show information on the selected block, if there is one
                            show_panel = viewer.scroll.selected_block.is_some()
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
