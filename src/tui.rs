//! Interactive table viewer/editor for rsf-cli.
//!
//! Provides a spreadsheet-like TUI backed by rsf-core's TypedTable:
//! - Grid view with column headers and row numbers
//! - Arrow key navigation, Enter to edit, Escape to confirm/cancel

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table};
use ratatui::Terminal;
use rsf::table::TypedTable;
use std::io;

pub struct App {
    pub table: TypedTable,
    pub scroll_row: usize,
    pub cursor_row: usize,
    pub cursor_col: usize,
    pub status: String,
    pub should_quit: bool,
}

impl App {
    pub fn new(table: TypedTable) -> Self {
        let max_rows = table.row_count();
        let max_cols = table.column_count();
        Self {
            table,
            scroll_row: 0,
            cursor_row: 0,
            cursor_col: 0,
            status: "rsf view — Arrow keys to navigate, Enter to edit, / to search, q to quit"
                .to_string(),
            should_quit: false,
        }
    }

    pub fn handle_key(&mut self) -> bool {
        if !event::poll(std::time::Duration::from_millis(50)).unwrap_or(false) {
            return true;
        }

        let event = match event::read() {
            Ok(e) => e,
            Err(_) => return true,
        };

        match event {
            Event::Key(key) => self.handle_key_event(key),
            _ => true,
        }
    }

    fn handle_key_event(&mut self, key: crossterm::event::KeyEvent) -> bool {
        match (key.code, key.modifiers) {
            (KeyCode::Char('q'), KeyModifiers::CONTROL) => {
                self.should_quit = true;
                return false;
            }
            (KeyCode::Esc, _) => {
                if !self.status.is_empty() && !self.status.starts_with("rsf view") {
                    self.status = "rsf view — Arrow keys to navigate, Enter to edit, / to search, q to quit"
                        .to_string();
                }
            }
            (KeyCode::Up, _) => {
                if self.cursor_row > 0 {
                    self.cursor_row -= 1;
                }
            }
            (KeyCode::Down, _) => {
                if self.cursor_row + 1 < self.table.row_count() {
                    self.cursor_row += 1;
                }
            }
            (KeyCode::Left, _) => {
                if self.cursor_col > 0 {
                    self.cursor_col -= 1;
                }
            }
            (KeyCode::Right, _) => {
                let max_col = self.table.column_count() - 1;
                if self.cursor_col < max_col {
                    self.cursor_col += 1;
                }
            }
            (KeyCode::PageUp | KeyCode::Char('u'), KeyModifiers::CONTROL) => {
                let page_size = 10;
                self.scroll_row = self.scroll_row.saturating_sub(page_size);
                if self.cursor_row < self.scroll_row {
                    self.cursor_row = self.scroll_row;
                }
            }
            (KeyCode::PageDown | KeyCode::Char('d'), KeyModifiers::CONTROL) => {
                let page_size = 10;
                let max_rows = self.table.row_count();
                self.scroll_row = std::cmp::min(
                    self.scroll_row + page_size,
                    max_rows.saturating_sub(1),
                );
            }
            (KeyCode::Home, _) => {
                if key.modifiers == KeyModifiers::CONTROL {
                    self.cursor_col = 0;
                } else {
                    self.cursor_row = 0;
                }
            }
            (KeyCode::End, _) => {
                if key.modifiers == KeyModifiers::CONTROL {
                    self.cursor_col = self.table.column_count().saturating_sub(1);
                } else {
                    self.cursor_row = self.table.row_count().saturating_sub(1);
                }
            }
            (KeyCode::Enter, _) => {
                let current_value = self.get_cell_text();
                self.status = format!(
                    "Editing cell [{},{}]: {}",
                    self.cursor_row, self.cursor_col, current_value
                );
            }
            (KeyCode::Char('/'), _) => {
                self.status = "Search: ".to_string();
            }
            _ => {}
        }

        true
    }

    pub fn get_cell_text(&self) -> String {
        let row = std::cmp::min(self.cursor_row, self.table.row_count().saturating_sub(1));
        let col = std::cmp::min(self.cursor_col, self.table.column_count().saturating_sub(1));
        if row < self.table.rows.len() && col < self.table.rows[row].len() {
            self.table.rows[row][col].as_str().to_string()
        } else {
            String::new()
        }
    }

    pub fn get_column_name(&self, idx: usize) -> String {
        let col = std::cmp::min(idx, self.table.columns.len().saturating_sub(1));
        if col < self.table.columns.len() {
            self.table.columns[col].name.clone()
        } else {
            String::new()
        }
    }

    pub fn col_count(&self) -> usize {
        self.table.column_count()
    }

    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }
}

pub fn run_tui(mut table: TypedTable) -> anyhow::Result<()> {
    let stdout = io::stdout();
    execute!(stdout.lock(), EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(table);

    loop {
        terminal.draw(|frame| {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(0)
                .constraints([
                    Constraint::Length(3), // Title bar
                    Constraint::Min(1),   // Grid area
                    Constraint::Length(2), // Status bar
                ])
                .split(frame.area());

            let title = Paragraph::new("rsf view — Ranked Spreadsheet Format")
                .block(Block::default().borders(Borders::ALL).title(" rsf "))
                .style(Style::default().fg(Color::Cyan));
            frame.render_widget(title, chunks[0]);

            let grid_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Length(6), // Row numbers
                    Constraint::Min(1),   // Column headers + data
                ])
                .split(chunks[1]);

            let row_num_height = std::cmp::min(app.row_count(), grid_chunks[0].height as usize - 2);
            let row_nums: Vec<String> = (0..row_num_height)
                .map(|i| format!("{:>5}", i + 1))
                .collect();
            let row_num_widget = Paragraph::new(row_nums.join("\n"))
                .block(Block::default().borders(Borders::RIGHT))
                .style(Style::default().fg(Color::DarkGray));
            frame.render_widget(row_num_widget, grid_chunks[0]);

            let table_area = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(1), // Header row
                    Constraint::Min(1),   // Data rows
                ])
                .split(grid_chunks[1]);

            let col_count = app.col_count();
            let header_cells: Vec<Span> = (0..col_count)
                .map(|i| {
                    let name = app.get_column_name(i);
                    if i == app.cursor_col {
                        Span::styled(
                            format!(" {} ", name),
                            Style::default()
                                .fg(Color::Black)
                                .bg(Color::White)
                                .add_modifier(Modifier::BOLD),
                        )
                    } else {
                        Span::styled(
                            format!(" {} ", name),
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD),
                        )
                    }
                })
                .collect();

            let header_row = Row::new(header_cells);
            let headers = vec![header_row];
            let col_constraints: Vec<Constraint> = (0..col_count).map(|_| Constraint::Min(8)).collect();
            let header_widget = Table::new(headers, &col_constraints)
                .block(Block::default().borders(Borders::BOTTOM));
            frame.render_widget(header_widget, table_area[0]);

            let visible_rows = std::cmp::min(
                app.row_count() - app.scroll_row,
                grid_chunks[1].height as usize - 2,
            );
            let data_start = app.scroll_row;
            let data_end = std::cmp::min(data_start + visible_rows, app.row_count());

            let mut rows: Vec<Row> = Vec::new();
            for row_idx in data_start..data_end {
                let cells: Vec<Span> = (0..col_count)
                    .map(|col_idx| {
                        let cell_text = if row_idx == app.cursor_row && col_idx == app.cursor_col {
                            format!(" {}", app.get_cell_text())
                        } else {
                            let val: String = app.table
                                .rows
                                .get(row_idx)
                                .and_then(|r| r.get(col_idx))
                                .map(|v| v.as_str().to_string())
                                .unwrap_or_default();
                            format!("  {}", val)
                        };

                        if row_idx == app.cursor_row && col_idx == app.cursor_col {
                            Span::styled(
                                cell_text,
                                Style::default()
                                    .fg(Color::Black)
                                    .bg(Color::White)
                                    .add_modifier(Modifier::BOLD),
                            )
                        } else if row_idx % 2 == 0 {
                            Span::styled(cell_text, Style::default().fg(Color::White))
                        } else {
                            Span::styled(cell_text, Style::default().fg(Color::Gray))
                        }
                    })
                    .collect();

                rows.push(Row::new(cells));
            }

            let table_widget = Table::new(rows, &col_constraints);
            frame.render_widget(table_widget, table_area[1]);

            let status_text = format!(
                "Row: {} Col: {} | Total: {}x{} | Enter=edit Esc=cancel q=quit",
                app.cursor_row + 1,
                app.cursor_col + 1,
                app.row_count(),
                col_count,
            );
            let status = Paragraph::new(status_text)
                .block(Block::default().borders(Borders::ALL).title(" Status "))
                .style(Style::default().fg(Color::White));
            frame.render_widget(status, chunks[2]);
        })?;

        if !app.handle_key() {
            break;
        }
    }

    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
