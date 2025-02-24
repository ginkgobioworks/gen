use crate::models::{block_group::BlockGroup, node::Node, traits::Query};
use crate::progress_bar::{get_handler, get_time_elapsed_bar};
use crate::views::block_group_viewer::{PlotParameters, Viewer};
use crate::views::collection::CollectionExplorer;
use rusqlite::{params, Connection};

use crossterm::{
    event::{self, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    layout::Constraint,
    style::{Color, Style},
    text::Text,
    widgets::{Block, Clear, Padding, Paragraph, Wrap},
};
use std::error::Error;
use std::time::{Duration, Instant};
use tui_scrollview::ScrollViewState;

pub fn view_block_group(
    conn: &Connection,
    name: &str,
    sample_name: Option<String>,
    collection_name: &str,
    position: Option<String>, // Node ID and offset
) -> Result<(), Box<dyn Error>> {
    let progress_bar = get_handler();
    let bar = progress_bar.add(get_time_elapsed_bar());
    let _ = progress_bar.println("Loading block group");
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

    // Get the node object corresponding to a node id
    let origin = if let Some(position_str) = position {
        let parts = position_str.split(":").collect::<Vec<&str>>();
        if parts.len() != 2 {
            panic!("Invalid position: {}", position_str);
        }
        let node_id = parts[0].parse::<i64>().unwrap();
        let offset = parts[1].parse::<i64>().unwrap();
        Some((
            Node::get(conn, "select * from nodes where id = ?1", params![node_id]).unwrap(),
            offset,
        ))
    } else {
        None
    };

    let block_group_id = block_group.unwrap().id;
    let block_graph = BlockGroup::get_graph(conn, block_group_id);
    bar.finish();

    // Create the viewer
    let bar = progress_bar.add(get_time_elapsed_bar());
    let _ = progress_bar.println("Pre-computing layout in chunks");
    let mut viewer = if let Some(origin) = origin {
        Viewer::with_origin(&block_graph, conn, PlotParameters::default(), origin)
    } else {
        Viewer::new(&block_graph, conn, PlotParameters::default())
    };
    bar.finish();

    // Styling:
    //let sidebar_style = Style::default().bg(Color::DarkGray).fg(Color::White);
    let panel_style = Style::default().bg(Color::DarkGray).fg(Color::White);

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let mut terminal = ratatui::init();

    // Basic event loop
    let tick_rate = Duration::from_millis(100);
    let mut last_tick = Instant::now();
    let mut show_panel = false;
    let show_sidebar = true;
    let mut tui_layout_change = false;
    loop {
        // Draw the UI
        terminal.draw(|frame| {
            // A layout consisting of a sidebar, canvas and a status bar, with optionally a panel
            // - The sidebar is where the samples and block groups are displayed
            // - The canvas is where the graph is drawn
            // - The status bar is where the controls are displayed
            // - The panel is a scrollable paragraph that can be toggled on and off
            let status_bar_height: u16 = 1;

            // The outer layout is a vertical split between the status bar and everything else
            let outer_layout = ratatui::layout::Layout::default()
                .direction(ratatui::layout::Direction::Vertical)
                .constraints(vec![
                    ratatui::layout::Constraint::Min(1),
                    ratatui::layout::Constraint::Length(status_bar_height),
                ])
                .split(frame.area());

            let status_bar_area = outer_layout[1];

            // The sidebar is a horizontal split of the area above the status bar
            let sidebar_layout = ratatui::layout::Layout::default()
                .direction(ratatui::layout::Direction::Horizontal)
                .constraints(vec![Constraint::Percentage(20), Constraint::Percentage(80)])
                .split(outer_layout[0]);
            let sidebar_area = sidebar_layout[0];

            // The panel pops up in the canvas area, it does not overlap with the sidebar
            let panel_layout = ratatui::layout::Layout::default()
                .direction(ratatui::layout::Direction::Vertical)
                .constraints(vec![Constraint::Percentage(80), Constraint::Percentage(20)])
                .split(if show_sidebar {
                    sidebar_layout[1]
                } else {
                    outer_layout[0]
                });
            let panel_area = panel_layout[1];

            let canvas_area = match (show_sidebar, show_panel) {
                (true, true) => panel_layout[0],
                (true, false) => sidebar_layout[1],
                (false, true) => panel_layout[0],
                (false, false) => outer_layout[0],
            };

            // Sidebar
            if show_sidebar {
                let sidebar_block = Block::default()
                    .padding(Padding::new(2, 2, 1, 1))
                    .style(Style::default().bg(Color::Indexed(233)));
                let sidebar_content_area = sidebar_block.inner(sidebar_area);

                // Get the default collection from the defaults table
                // let mut stmt = conn
                //     .prepare("select collection_name from defaults where id = 1")
                //     .unwrap();
                // let default_collection = stmt.query_row((), |row| row.get(0))
                //     .unwrap_or("default".to_string());

                let default_collection = "default";

                let explorer = CollectionExplorer::new(conn, default_collection);

                let mut scroll_state = ScrollViewState::default();

                // Render the block first to set the background
                frame.render_widget(sidebar_block.clone(), sidebar_area);
                // Then render the explorer content
                frame.render_stateful_widget(explorer, sidebar_content_area, &mut scroll_state);
            }

            // Status bar
            let status_message = format!(
                "{message} | return: show information on block | q=quit",
                message = Viewer::get_status_line()
            );

            let status_bar_contents = format!(
                "{status_message:width$}",
                width = status_bar_area.width as usize
            );

            let status_bar = Paragraph::new(Text::styled(
                status_bar_contents,
                Style::default().bg(Color::DarkGray).fg(Color::White),
            ));

            frame.render_widget(status_bar, status_bar_area);

            // Ask the viewer to paint the canvas
            viewer.draw(frame, canvas_area);

            // Panel
            if show_panel {
                let panel_block = Block::bordered()
                    .padding(Padding::new(2, 2, 1, 1))
                    .title("Details")
                    .style(panel_style);
                let mut panel_text = Text::from("No content found");

                // Get information about the currently selected block
                if viewer.state.selected_block.is_some() {
                    let selected_block = viewer.state.selected_block.unwrap();
                    panel_text = Text::from(format!(
                        "Block ID: {}\nNode ID: {}\nStart: {}\nEnd: {}\n",
                        selected_block.block_id,
                        selected_block.node_id,
                        selected_block.sequence_start,
                        selected_block.sequence_end
                    ));
                }

                let panel_content = Paragraph::new(panel_text)
                    .wrap(Wrap { trim: true })
                    .scroll((0, 0))
                    .style(Style::default().bg(Color::Reset))
                    .block(panel_block);

                // Clear the panel area if we just changed the layout
                if tui_layout_change {
                    frame.render_widget(Clear, panel_area);
                }
                frame.render_widget(panel_content, panel_area);

                // Reset the layout change flag
                tui_layout_change = false;
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
                        // Performing actions on blocks
                        KeyCode::Tab => {
                            // Future implementation: switch between panels
                        }
                        KeyCode::BackTab => {
                            // Future implementation: switch between panels
                        }
                        KeyCode::Esc => {
                            show_panel = false;
                        }
                        KeyCode::Enter => {
                            // Show information on the selected block, if there is one
                            show_panel = viewer.state.selected_block.is_some();
                        }
                        _ => {
                            viewer.handle_input(key);
                        }
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
