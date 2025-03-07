use crate::models::{block_group::BlockGroup, node::Node, path::Path, traits::Query};
use crate::progress_bar::{get_handler, get_time_elapsed_bar};
use crate::views::block_group_viewer::{PlotParameters, Viewer};
use crate::views::collection::{CollectionExplorer, CollectionExplorerState, FocusZone};
use crossterm::{
    event::{self, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use log::warn;
use ratatui::{
    layout::Constraint,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Clear, Padding, Paragraph, Wrap},
};
use rusqlite::{params, types::Value as SQLValue, Connection};
use std::error::Error;
use std::time::{Duration, Instant};

// Frequency by which we check for external updates to the db
const REFRESH_INTERVAL: u64 = 3; // seconds

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

    let block_group = block_group.unwrap();
    let block_group_id = block_group.id;

    // Get the node object corresponding to the position given by the user
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
    bar.finish();

    // Create the viewer and the initial graph
    let bar = progress_bar.add(get_time_elapsed_bar());
    let _ = progress_bar.println("Pre-computing layout in chunks");
    let mut block_graph = BlockGroup::get_graph(conn, block_group_id);

    let mut viewer = if let Some(origin) = origin {
        Viewer::with_origin(&block_graph, conn, PlotParameters::default(), origin)
    } else {
        Viewer::new(&block_graph, conn, PlotParameters::default())
    };

    bar.finish();

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

    // Focus management
    let mut focus_zone = FocusZone::Canvas;

    // Create explorer and its state that persists across frames
    let mut explorer = CollectionExplorer::new(conn, collection_name);
    let mut explorer_state =
        CollectionExplorerState::with_selected_block_group(Some(block_group_id));
    if let Some(ref s) = sample_name {
        explorer_state.toggle_sample(s);
    }

    // Track the last selected block group to detect changes
    let mut last_selected_block_group_id = Some(block_group_id);
    // Track if we're loading a new block group
    let mut is_loading = false;
    let mut last_refresh = Instant::now();
    loop {
        // Refresh explorer data and force reload on change
        // I do this every REFRESH_INTERVAL seconds.
        if last_refresh.elapsed() >= Duration::from_secs(REFRESH_INTERVAL) {
            if explorer.refresh(conn, collection_name) {
                explorer.force_reload(&mut explorer_state);
            }
            last_refresh = Instant::now();
        }

        // Trigger reload if selection changed to a new block group
        if explorer_state.selected_block_group_id != last_selected_block_group_id {
            is_loading = true;
            last_selected_block_group_id = explorer_state.selected_block_group_id;
        }

        // Draw the UI
        terminal.draw(|frame| {
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
                .split(sidebar_layout[1]);
            let panel_area = panel_layout[1];

            let canvas_area = if show_panel {
                panel_layout[0]
            } else {
                sidebar_layout[1]
            };

            // Sidebar
            explorer_state.has_focus = focus_zone == FocusZone::Sidebar;
            if show_sidebar {
                let sidebar_block = Block::default()
                    .padding(Padding::new(0, 0, 1, 1))
                    .style(Style::default().bg(Color::Indexed(233)));
                let sidebar_content_area = sidebar_block.inner(sidebar_area);

                frame.render_widget(sidebar_block.clone(), sidebar_area);
                frame.render_stateful_widget(&explorer, sidebar_content_area, &mut explorer_state);
            }

            // Status bar
            let mut status_message = match focus_zone {
                FocusZone::Canvas => Viewer::get_status_line(),
                FocusZone::Panel => "esc: close panel".to_string(),
                FocusZone::Sidebar => CollectionExplorer::get_status_line(),
            };

            // Paths may be too specific an application, so we don't include the controls in the Viewer widget itself
            // Instead, we add them to the status bar along with the other controls
            status_message.push_str(" | p: show current path | tab: cycle focus | q: quit");

            let status_bar_contents = format!(
                "{status_message:^width$}",
                width = status_bar_area.width as usize
            );

            let status_bar = Paragraph::new(Text::styled(
                status_bar_contents,
                Style::default().bg(Color::Black).fg(Color::DarkGray),
            ));

            frame.render_widget(status_bar, status_bar_area);

            // Canvas area
            if is_loading {
                // Draw loading message in canvas area
                let loading_text = Text::styled(
                    "Loading...",
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                );
                let loading_para =
                    Paragraph::new(loading_text).alignment(ratatui::layout::Alignment::Center);

                // Center the loading message vertically in the canvas area
                let loading_area = ratatui::layout::Layout::default()
                    .direction(ratatui::layout::Direction::Vertical)
                    .constraints([
                        ratatui::layout::Constraint::Percentage(45),
                        ratatui::layout::Constraint::Length(1),
                        ratatui::layout::Constraint::Percentage(45),
                    ])
                    .split(canvas_area)[1];

                frame.render_widget(Clear, canvas_area); // Clear the canvas area first
                frame.render_widget(loading_para, loading_area);
            } else {
                // Ask the viewer to paint the canvas
                viewer.has_focus = focus_zone == FocusZone::Canvas;
                viewer.draw(frame, canvas_area);
            }

            // Panel
            if show_panel {
                let panel_block = Block::bordered()
                    .padding(Padding::new(2, 2, 1, 1))
                    .title("Details")
                    .style(Style::default().bg(Color::Indexed(233)))
                    .border_style(if focus_zone == FocusZone::Panel {
                        Style::default()
                            .fg(Color::Blue)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default().fg(Color::Gray)
                    });

                let panel_text = if let Some(selected_block) = viewer.state.selected_block {
                    vec![
                        Line::from(vec![
                            Span::styled(
                                "Block ID: ",
                                Style::default().add_modifier(Modifier::BOLD),
                            ),
                            Span::raw(selected_block.block_id.to_string()),
                        ]),
                        Line::from(vec![
                            Span::styled(
                                "Node ID: ",
                                Style::default().add_modifier(Modifier::BOLD),
                            ),
                            Span::raw(selected_block.node_id.to_string()),
                        ]),
                        Line::from(vec![
                            Span::styled("Start: ", Style::default().add_modifier(Modifier::BOLD)),
                            Span::raw(selected_block.sequence_start.to_string()),
                        ]),
                        Line::from(vec![
                            Span::styled("End: ", Style::default().add_modifier(Modifier::BOLD)),
                            Span::raw(selected_block.sequence_end.to_string()),
                        ]),
                    ]
                } else {
                    vec![Line::from(vec![Span::styled(
                        "No block selected",
                        Style::default().fg(Color::DarkGray),
                    )])]
                };

                let panel_content = Paragraph::new(panel_text)
                    .wrap(Wrap { trim: true })
                    .alignment(ratatui::layout::Alignment::Left)
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

        // After drawing, update the viewer if needed
        if is_loading {
            if let Some(new_block_group_id) = explorer_state.selected_block_group_id {
                // Create a new graph for the selected block group
                block_graph = BlockGroup::get_graph(conn, new_block_group_id);
                // Update the viewer
                viewer = Viewer::new(&block_graph, conn, PlotParameters::default());
                viewer.state.selected_block = None;
                is_loading = false;
            }
        }

        // Handle input
        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));
        if crossterm::event::poll(timeout)? {
            if let event::Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    // Global handlers
                    match key.code {
                        KeyCode::Char('q') => break,
                        KeyCode::Tab => {
                            // Tab - cycle forwards
                            focus_zone = match focus_zone {
                                FocusZone::Canvas => {
                                    if show_panel {
                                        FocusZone::Panel
                                    } else {
                                        FocusZone::Sidebar
                                    }
                                }
                                FocusZone::Sidebar => FocusZone::Canvas,
                                FocusZone::Panel => FocusZone::Sidebar,
                            }
                        }
                        KeyCode::BackTab => {
                            // Shift+Tab - cycle backwards
                            focus_zone = match focus_zone {
                                FocusZone::Canvas => FocusZone::Sidebar,
                                FocusZone::Sidebar => {
                                    if show_panel {
                                        FocusZone::Panel
                                    } else {
                                        FocusZone::Canvas
                                    }
                                }
                                FocusZone::Panel => FocusZone::Canvas,
                            }
                        }
                        _ => {}
                    }

                    // Focus-specific handlers
                    match focus_zone {
                        FocusZone::Canvas => match key.code {
                            KeyCode::Enter => {
                                if viewer.state.selected_block.is_some() {
                                    show_panel = true;
                                    focus_zone = FocusZone::Panel;
                                    tui_layout_change = true;
                                }
                            }
                            KeyCode::Char('p') => {
                                // TODO: make current path highlighted by default, and boundary edges indicated as dashed lines
                                // Toggle current path highlighting
                                if viewer.has_highlight(Color::Red) {
                                    viewer.clear_highlight(Color::Red);
                                } else if let Some(bg_id) = explorer_state.selected_block_group_id {
                                    // BlockGroup::get_current_path will panic if there's no path,
                                    // so we roll our own query here. (todo: have get_current_path return an Option)
                                    let current_path = <Path as Query>::get(
                                        conn,
                                        "SELECT * FROM paths WHERE block_group_id = ?1 ORDER BY id DESC LIMIT 1",
                                        rusqlite::params!(SQLValue::from(bg_id)),
                                    );
                                    match current_path {
                                        Ok(path) => {
                                            if let Err(err) = viewer.show_path(&path, Color::Red) {
                                                // todo: pop up a message in the panel
                                                warn!("{}", err);
                                            }
                                        }
                                        Err(err) => {
                                            warn!(
                                                "No path found for block group {}: {}",
                                                bg_id, err
                                            );
                                        }
                                    }
                                } else {
                                    warn!("No block group selected");
                                }
                            }
                            _ => {
                                viewer.handle_input(key);
                            }
                        },
                        FocusZone::Panel => {
                            if key.code == KeyCode::Esc {
                                show_panel = false;
                                focus_zone = FocusZone::Canvas;
                                tui_layout_change = true;
                            }
                        }
                        FocusZone::Sidebar => {
                            explorer.handle_input(&mut explorer_state, key);
                            // Check if focus change was requested by the explorer
                            if let Some(requested_zone) = explorer_state.focus_change_requested {
                                focus_zone = requested_zone;
                                explorer_state.focus_change_requested = None;
                            }
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
