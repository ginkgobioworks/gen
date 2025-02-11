use crate::models::operations::{Operation, OperationSummary};
use crate::models::traits::Query;
use crossterm::{
    event::{self, KeyCode, KeyEvent},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use itertools::Itertools;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    widgets::{Block, Borders, List, ListItem},
    Terminal,
};
use rusqlite::{params, types::Value, Connection};
use std::io;
use std::rc::Rc;
use tui_textarea::TextArea;

pub fn view_operations(conn: &Connection, operations: &[Operation]) -> Result<(), io::Error> {
    let mut operation_summaries = OperationSummary::query(
        conn,
        "select * from operation_summary where operation_hash in rarray(?1)",
        params![Rc::new(
            operations
                .iter()
                .map(|x| Value::from(x.hash.clone()))
                .collect::<Vec<Value>>()
        )],
    );

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut textarea = TextArea::default();

    let mut editing_message = false;

    let mut selected = 0;
    loop {
        terminal.draw(|f| {
            let items: Vec<ListItem> = operation_summaries
                .iter()
                .enumerate()
                .map(|(i, op)| {
                    let prefix = if i == selected { "> " } else { "  " };
                    ListItem::new(format!("{}{} - {}", prefix, op.operation_hash, op.summary))
                })
                .collect();

            let list =
                List::new(items).block(Block::default().title("Operations").borders(Borders::ALL));

            if editing_message {
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
                    .split(f.area());
                f.render_widget(list, chunks[0]);
                f.render_widget(&textarea, chunks[1]);
            } else {
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Percentage(100)].as_ref())
                    .split(f.area());
                f.render_widget(list, chunks[0]);
            };
        })?;

        if event::poll(std::time::Duration::from_millis(100))? {
            if editing_message {
                if let event::Event::Key(key) = event::read()? {
                    if key.code == KeyCode::Esc {
                        editing_message = false;
                        let new_summary = textarea.lines().iter().join("\n");
                        OperationSummary::set_message(
                            conn,
                            operation_summaries[selected].id,
                            &new_summary,
                        );
                        operation_summaries[selected].summary = new_summary;
                    } else {
                        textarea.input(key);
                    }
                }
            } else if let event::Event::Key(KeyEvent { code, .. }) = event::read()? {
                match code {
                    KeyCode::Esc | KeyCode::Char('q') => break,
                    KeyCode::Up => {
                        if selected > 0 {
                            selected = selected.saturating_sub(1);
                        }
                    }
                    KeyCode::Down => {
                        if selected < operations.len() - 1 {
                            selected += 1;
                        }
                    }
                    KeyCode::Enter | KeyCode::Char('e') => {
                        textarea =
                            TextArea::from_iter(operation_summaries[selected].summary.split("\n"));
                        editing_message = true;
                    }
                    _ => {}
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}
