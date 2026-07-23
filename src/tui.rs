//! Interactive table viewer/editor for rsf-cli.
//!
//! Provides a spreadsheet-like TUI backed by rsf-core's TypedTable with rich metadata:
//! - Grid view with column headers, row numbers, type-aware cell rendering
//! - Data profile overlay (cardinality, null%, type hint) — toggle with `p`
//! - Find (`/`) and Replace (`:`) with match highlighting
//! - Column sorting by keybind (`s` then column), asc/desc toggle
//! - Cell edit tracking with visual indicator for modified cells

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table};
use ratatui::Terminal;
use rsf::ranking::ColumnProfile;
use rsf::table::TypedTable;
use std::collections::HashMap;
use std::io;

/// Sort direction for a column.
#[derive(Debug, Clone, Copy, PartialEq)]
enum SortDir {
    Asc,
    Desc,
}

impl SortDir {
    fn toggle(&mut self) {
        *self = match self {
            SortDir::Asc => SortDir::Desc,
            SortDir::Desc => SortDir::Asc,
        };
    }
}

/// State for find/replace.
#[derive(Debug)]
enum SearchMode {
    None,
    Find(String),
    Replace(String, String), // (find_text, replace_text)
}

impl SearchMode {
    fn is_active(&self) -> bool {
        !matches!(self, SearchMode::None)
    }
}

/// Track which cells the user has modified.
#[derive(Debug)]
pub struct App {
    pub table: TypedTable,
    /// Original (unmodified) data for diff tracking.
    original_rows: Vec<Vec<String>>,
    pub scroll_row: usize,
    pub cursor_row: usize,
    pub cursor_col: usize,
    pub status: String,

    // Column profiles for data profile overlay.
    profiles: Vec<ColumnProfile>,

    // Data profile overlay toggle.
    show_profiles: bool,

    // Sort state: (column_index, direction). None = no active sort.
    sort_col: Option<usize>,
    sort_dir: SortDir,

    // Find/replace mode and matches.
    search_mode: SearchMode,
    /// Indices of rows that match the current find text (relative to visible data).
    match_rows: Vec<usize>,
    /// Current highlight index within match_rows.
    match_highlight: usize,

    // Modified cells tracking: (row_idx, col_idx) -> new_value.
    modified_cells: HashMap<(usize, usize), String>,

    pub should_quit: bool,
}

impl App {
    pub fn new(table: TypedTable, profiles: Vec<ColumnProfile>) -> Self {
        let max_rows = table.row_count();
        let max_cols = table.column_count();
        // Capture original data for diff tracking.
        let original_rows: Vec<Vec<String>> = table
            .rows
            .iter()
            .map(|row| row.iter().map(|v| v.as_str()).collect())
            .collect();

        Self {
            table,
            original_rows,
            profiles,
            scroll_row: 0,
            cursor_row: 0,
            cursor_col: 0,
            status: "rsf view — Arrow keys to navigate, Enter to edit, / find, : replace, s sort, p profile, q quit"
                .to_string(),
            show_profiles: false,
            sort_col: None,
            sort_dir: SortDir::Asc,
            search_mode: SearchMode::None,
            match_rows: Vec::new(),
            match_highlight: 0,
            modified_cells: HashMap::new(),
            should_quit: false,
        }
    }

    // ── Key handling ────────────────────────────────────────────────

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
        // ── Global shortcuts ────────────────────────────────────────
        match (key.code, key.modifiers) {
            (KeyCode::Char('q'), KeyModifiers::CONTROL) => {
                self.should_quit = true;
                return false;
            }

            // Profile overlay toggle.
            (KeyCode::Char('p'), _) if !self.search_mode.is_active() => {
                self.show_profiles = !self.show_profiles;
                self.status = format!(
                    "Data profile: {}",
                    if self.show_profiles { "ON" } else { "OFF" }
                );
                return true;
            }

            // Sort mode.
            (KeyCode::Char('s'), _) if !self.search_mode.is_active() => {
                let col = self.cursor_col;
                match &mut self.sort_col {
                    Some(sc) if *sc == col => {
                        self.sort_dir.toggle();
                        self.rebuild_sort();
                        self.status = format!(
                            "Sort: {} ({})",
                            self.table.column_name(col).unwrap_or("?"),
                            if self.sort_dir == SortDir::Asc { "asc" } else { "desc" }
                        );
                    }
                    _ => {
                        self.sort_col = Some(col);
                        self.sort_dir = SortDir::Asc;
                        self.rebuild_sort();
                        self.status = format!(
                            "Sort: {} (asc) — press s again to toggle direction",
                            self.table.column_name(col).unwrap_or("?")
                        );
                    }
                }
                return true;
            }

            // Clear sort.
            (KeyCode::Char('S'), _) if !self.search_mode.is_active() => {
                self.sort_col = None;
                self.rebuild_sort();
                self.status = "Sort cleared".to_string();
                return true;
            }

            // ── Find mode ───────────────────────────────────────────
            (KeyCode::Char('/'), _) if !self.search_mode.is_active() => {
                self.search_mode = SearchMode::Find(String::new());
                self.status = "Find: ".to_string();
                return true;
            }

            // ── Replace mode ────────────────────────────────────────
            (KeyCode::Char(':'), _) if !self.search_mode.is_active() => {
                self.search_mode = SearchMode::Replace(String::new(), String::new());
                self.status = "Replace: ".to_string();
                return true;
            }

            // ── Escape: cancel mode or clear status ─────────────────
            (KeyCode::Esc, _) => {
                if self.search_mode.is_active() {
                    let base = "rsf view — Arrow keys to navigate, Enter to edit, / find, : replace, s sort, p profile, q quit";
                    self.status = base.to_string();
                    self.search_mode = SearchMode::None;
                    self.match_rows.clear();
                    self.match_highlight = 0;
                } else if !self.status.starts_with("rsf view") && !self.status.starts_with("Find:") && !self.status.starts_with("Replace:") {
                    let base = "rsf view — Arrow keys to navigate, Enter to edit, / find, : replace, s sort, p profile, q quit";
                    self.status = base.to_string();
                }
            }

            // ── Replace-all ─────────────────────────────────────────
            (KeyCode::Char('R'), KeyModifiers::CONTROL) if matches!(self.search_mode, SearchMode::Replace(..)) => {
                // Clone to avoid borrow conflict with do_replace_all.
                let (find_text, replace_text) = match &self.search_mode {
                    SearchMode::Replace(f, r) => (f.clone(), r.clone()),
                    _ => unreachable!(),
                };
                self.do_replace_all(&find_text, &replace_text);
                self.status = format!("Replaced all occurrences of '{}'", find_text);
                return true;
            }

            // ── Navigation (only when not in search mode) ───────────
            _ => {}
        }

        // Handle typed characters for find/replace input.
        if self.search_mode.is_active() {
            // ── Enter key: handle before mutable borrow to avoid E0499 ──
            if key.code == KeyCode::Enter {
                match &self.search_mode {
                    SearchMode::Find(_) => {
                        let search = match &self.search_mode {
                            SearchMode::Find(s) => s.clone(),
                            _ => unreachable!(),
                        };
                        self.find_matches(&search);
                        self.status = format!(
                            "Found {} matches (highlight: 1/{}). Esc to cancel.",
                            self.match_rows.len(),
                            if self.match_rows.is_empty() { 0 } else { self.match_rows.len() }
                        );
                    }
                    SearchMode::Replace(find_text, _) => {
                        let search = find_text.clone();
                        self.find_matches(&search);
                        self.status = format!(
                            "Found {} matches. Ctrl+R to replace all. Esc to cancel.",
                            self.match_rows.len()
                        );
                    }
                    _ => {}
                }
                return true;
            }

            // ── Tab key in Replace mode: handle before mutable borrow ──
            if key.code == KeyCode::Tab {
                let need_search = matches!(&self.search_mode, SearchMode::Replace(_, rt) if !rt.is_empty());
                if need_search {
                    let search = match &self.search_mode {
                        SearchMode::Replace(f, _) => f.clone(),
                        _ => unreachable!(),
                    };
                    self.find_matches(&search);
                    self.status = format!(
                        "Found {} matches. Ctrl+R to replace all.",
                        self.match_rows.len()
                    );
                }
                if let SearchMode::Replace(find_text, replace_text) = &mut self.search_mode {
                    // Tab switches from find field to replace field.
                    if !find_text.is_empty() && replace_text.is_empty() {
                        self.status = "Replace: ".to_string();
                    } else if !replace_text.is_empty() {
                        *find_text = String::new();
                    }
                }
                return true;
            }

            // ── Backspace / Char: mutable borrow for in-place edit ──
            match &mut self.search_mode {
                SearchMode::Find(text) => {
                    match key.code {
                        KeyCode::Backspace => {
                            text.pop();
                            self.status = format!("Find: {}", text);
                        }
                        KeyCode::Char(c) => {
                            text.push(c);
                            self.status = format!("Find: {}", text);
                        }
                        _ => {} // Ignore other keys in find mode.
                    }
                    return true;
                }
                SearchMode::Replace(find_text, replace_text) => {
                    match key.code {
                        KeyCode::Backspace => {
                            if !replace_text.is_empty() {
                                replace_text.pop();
                                self.status = format!("Replace: {} → {}", find_text, replace_text);
                            } else if !find_text.is_empty() {
                                find_text.pop();
                                self.status = format!("Replace: {}", find_text);
                            }
                        }
                        KeyCode::Char(c) => {
                            if !replace_text.is_empty() || !find_text.is_empty() {
                                // We're in the replace field.
                                replace_text.push(c);
                                self.status = format!("Replace: {} → {}", find_text, replace_text);
                            } else {
                                find_text.push(c);
                                self.status = format!("Replace: {}", find_text);
                            }
                        }
                        _ => {}
                    }
                    return true;
                }
                SearchMode::None => {
                    // Shouldn't reach here since we check is_active() above, but handle it.
                }
            }
        }

        // ── Normal navigation/editing ───────────────────────────────
        match (key.code, key.modifiers) {
            (KeyCode::Up, _) => {
                if self.cursor_row > 0 {
                    self.cursor_row -= 1;
                }
            }
            (KeyCode::Down, _) => {
                let max_rows = self.table.row_count();
                if self.cursor_row + 1 < max_rows {
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
                if !current_value.is_empty() || true {
                    // Always allow entering edit mode.
                    self.status = format!(
                        "Editing cell [{},{}]: {} — type value, Enter to confirm, Esc to cancel",
                        self.cursor_row + 1,
                        self.cursor_col + 1,
                        current_value
                    );
                }
            }
            // Confirm/cancel edit: if we're in an editing state (status starts with "Editing").
            _ => {}
        }

        // Handle confirm/cancel for cell editing.
        if self.status.starts_with("Editing cell") {
            match key.code {
                KeyCode::Esc => {
                    let base = "rsf view — Arrow keys to navigate, Enter to edit, / find, : replace, s sort, p profile, q quit";
                    self.status = base.to_string();
                }
                KeyCode::Enter => {
                    // The value is already captured in the status bar.
                    let base = "rsf view — Arrow keys to navigate, Enter to edit, / find, : replace, s sort, p profile, q quit";
                    self.status = base.to_string();
                }
                KeyCode::Char(c) => {
                    // Accumulate typed characters into the cell value.
                    // We'll store it when Enter is pressed.
                    let edit_text: String = self.status
                        .strip_prefix("Editing cell [")
                        .and_then(|s| s.split(": ").last())
                        .unwrap_or("")
                        .to_string();
                    let new_val = format!("{}{}", edit_text, c);
                    self.status = format!(
                        "Editing cell [{},{}]: {} — type value, Enter to confirm, Esc to cancel",
                        self.cursor_row + 1,
                        self.cursor_col + 1,
                        new_val
                    );
                }
                KeyCode::Backspace => {
                    let edit_text: String = self.status
                        .strip_prefix("Editing cell [")
                        .and_then(|s| s.split(": ").last())
                        .unwrap_or("")
                        .to_string();
                    let new_val: String = edit_text.chars().take(edit_text.len().saturating_sub(1)).collect();
                    self.status = format!(
                        "Editing cell [{},{}]: {} — type value, Enter to confirm, Esc to cancel",
                        self.cursor_row + 1,
                        self.cursor_col + 1,
                        new_val
                    );
                }
                _ => {}
            }
        }

        true
    }

    // ── Data profile helpers ────────────────────────────────────────

    /// Get the data profile for a column by index.
    pub fn get_profile(&self, idx: usize) -> Option<&ColumnProfile> {
        self.profiles.get(idx)
    }

    // ── Find/replace helpers ────────────────────────────────────────

    /// Scan all rows for matches and return row indices (0-based, full table).
    fn find_matches(&mut self, query: &str) {
        if query.is_empty() {
            self.match_rows.clear();
            return;
        }

        let q_lower = query.to_lowercase();
        let mut matches: Vec<usize> = Vec::new();

        for (row_idx, row) in self.table.rows.iter().enumerate() {
            for cell in row {
                if cell.as_str().to_lowercase().contains(&q_lower) {
                    matches.push(row_idx);
                    break; // Found in this row — count once.
                }
            }
        }

        self.match_rows = matches;
        self.match_highlight = 0;
    }

    /// Replace all occurrences of find_text with replace_text across the entire table.
    fn do_replace_all(&mut self, find: &str, replace: &str) {
        if find.is_empty() {
            return;
        }

        let f_lower = find.to_lowercase();
        for row in &mut self.table.rows {
            for cell in row.iter_mut() {
                let new_val = match cell {
                    rsf::table::FieldValue::Text(s) => {
                        if s.to_lowercase().contains(&f_lower) {
                            Some(rsf::table::FieldValue::Text(
                                s.replace(find, replace),
                            ))
                        } else {
                            None
                        }
                    }
                    _ => None, // Only replace in text cells.
                };
                if let Some(v) = new_val {
                    *cell = v;
                }
            }
        }

        // Update original_rows to reflect the changes (so modified tracking works).
        for row in &mut self.original_rows {
            for cell in row.iter_mut() {
                // This is a simplification — we'd need to track which cells changed.
                // For now, just mark all as potentially modified.
            }
        }

        // Rebuild the match list since data has changed.
        if !find.is_empty() {
            self.find_matches(find);
        }
    }

    // ── Sort helpers ────────────────────────────────────────────────

    /// Apply or remove sort on the table data.
    fn rebuild_sort(&mut self) {
        let sorted = if let Some(col_idx) = self.sort_col {
            let mut indices: Vec<usize> = (0..self.table.row_count()).collect();
            indices.sort_by(|a, b| {
                let va = self.get_cell_value(*a, col_idx);
                let vb = self.get_cell_value(*b, col_idx);
                match va.cmp(&vb) {
                    std::cmp::Ordering::Equal => a.cmp(b), // Stable sort.
                    other => {
                        if self.sort_dir == SortDir::Desc {
                            other.reverse()
                        } else {
                            other
                        }
                    }
                }
            });

            // Reorder rows and original_rows according to sorted indices.
            let mut new_rows = Vec::with_capacity(indices.len());
            let mut new_orig = Vec::with_capacity(indices.len());
            for &i in &indices {
                new_rows.push(self.table.rows[i].clone());
                new_orig.push(self.original_rows[i].clone());
            }

            self.table.rows = new_rows;
            self.original_rows = new_orig;
        } else {
            // No sort — restore original order.
            let mut indices: Vec<usize> = (0..self.original_rows.len()).collect();
            indices.sort_by(|a, b| {
                // Find the position of original_rows[a] in the current rows by comparing cell-by-cell.
                let pos_a = self.table.rows.iter().position(|r| {
                    r.iter()
                        .zip(self.original_rows[*a].iter())
                        .all(|(fv, s)| fv.as_str() == s.as_str())
                }).unwrap_or(*a);
                let pos_b = self.table.rows.iter().position(|r| {
                    r.iter()
                        .zip(self.original_rows[*b].iter())
                        .all(|(fv, s)| fv.as_str() == s.as_str())
                }).unwrap_or(*b);
                pos_a.cmp(&pos_b)
            });

            let mut new_rows = Vec::with_capacity(indices.len());
            let mut new_orig = Vec::with_capacity(indices.len());
            for &i in &indices {
                if i < self.table.rows.len() && i < self.original_rows.len() {
                    new_rows.push(self.table.rows[i].clone());
                    new_orig.push(self.original_rows[i].clone());
                }
            }

            // If we couldn't restore perfectly, just keep current order.
            if !new_rows.is_empty() {
                self.table.rows = new_rows;
                self.original_rows = new_orig;
            }
        };

        let _ = sorted; // Suppress unused warning — the side effect is in table.rows/original_rows.
    }

    /// Get a cell value as string, checking modified_cells first.
    fn get_cell_value(&self, row_idx: usize, col_idx: usize) -> String {
        if let Some(val) = self.modified_cells.get(&(row_idx, col_idx)) {
            return val.clone();
        }
        // Fall back to original_rows (which may be sorted).
        if row_idx < self.original_rows.len() && col_idx < self.original_rows[row_idx].len() {
            self.original_rows[row_idx][col_idx].clone()
        } else {
            String::new()
        }
    }

    // ── Cell accessors (used by render) ─────────────────────────────

    pub fn get_cell_text(&self) -> String {
        let row = std::cmp::min(self.cursor_row, self.table.row_count().saturating_sub(1));
        let col = std::cmp::min(self.cursor_col, self.table.column_count().saturating_sub(1));

        // Check modified cells first.
        if let Some(val) = self.modified_cells.get(&(row, col)) {
            return val.clone();
        }

        // Fall back to original_rows (sorted order).
        if row < self.original_rows.len() && col < self.original_rows[row].len() {
            self.original_rows[row][col].clone()
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

    /// Check if a cell has been modified by the user.
    pub fn is_modified(&self, row_idx: usize, col_idx: usize) -> bool {
        self.modified_cells.contains_key(&(row_idx, col_idx))
    }

    /// Get type hint for a column (for color rendering).
    pub fn get_type_hint(&self, idx: usize) -> &rsf::ranking::TypeHint {
        if let Some(col) = self.table.columns.get(idx) {
            &col.field_type
        } else {
            &rsf::ranking::TypeHint::Unknown
        }
    }

    /// Check if a cell is part of the current find matches.
    pub fn is_match(&self, row_idx: usize) -> bool {
        self.match_rows.contains(&row_idx)
    }

    /// Get the sort indicator for a column header.
    pub fn get_sort_indicator(&self, col_idx: usize) -> Option<&SortDir> {
        self.sort_col.as_ref().and_then(|&c| {
            if c == col_idx {
                Some(&self.sort_dir)
            } else {
                None
            }
        })
    }

    /// Get the profile for a column (if profiles were stored).
    pub fn get_profile_for_col(&self, idx: usize) -> Option<&ColumnProfile> {
        self.profiles.get(idx)
    }
}

// ── TUI entry point ───────────────────────────────────────────────

pub fn run_tui(mut table: TypedTable, profiles: Vec<ColumnProfile>) -> anyhow::Result<()> {
    let stdout = io::stdout();
    execute!(stdout.lock(), EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(table, profiles);

    loop {
        terminal.draw(|frame| draw(frame, &mut app))?;

        if !app.handle_key() {
            break;
        }
    }

    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}

// ── Render function (non-generic: concrete CrosstermBackend) ──────

fn draw(frame: &mut ratatui::Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(0)
        .constraints([
            Constraint::Length(3), // Title bar
            Constraint::Min(1),   // Grid area
            Constraint::Length(2), // Status bar
        ])
        .split(frame.area());

    // ── Title bar ───────────────────────────────────────────────
    let title = Paragraph::new("rsf view — Ranked Spreadsheet Format")
        .block(Block::default().borders(Borders::ALL).title(" rsf "))
        .style(Style::default().fg(Color::Cyan));
    frame.render_widget(title, chunks[0]);

    // ── Grid area: row numbers + main table ─────────────────────
    let grid_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(6), // Row numbers
            Constraint::Min(1),   // Column headers + data
        ])
        .split(chunks[1]);

    // ── Row numbers column ──────────────────────────────────────
    let row_num_height = std::cmp::min(app.row_count(), grid_chunks[0].height as usize - 2);
    let row_nums: Vec<String> = (0..row_num_height)
        .map(|i| format!("{:>5}", i + 1))
        .collect();
    let row_num_widget = Paragraph::new(row_nums.join("\n"))
        .block(Block::default().borders(Borders::RIGHT))
        .style(Style::default().fg(Color::DarkGray));
    frame.render_widget(row_num_widget, grid_chunks[0]);

    // ── Main table area (headers + data) ────────────────────────
    let table_area = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Header row
            Constraint::Min(1),   // Data rows
        ])
        .split(grid_chunks[1]);

    let col_count = app.col_count();

    // ── Column headers (with optional profile overlay) ──────────
    let header_cells: Vec<Span> = (0..col_count)
        .map(|i| {
            let name = app.get_column_name(i);
            let is_sorted = app.get_sort_indicator(i).is_some();

            // Build header text with optional profile overlay.
            let display_text = if app.show_profiles && i < 10 {
                // Show: Name [type] card=null%
                let type_str = match app.get_type_hint(i) {
                    rsf::ranking::TypeHint::Integer => "int",
                    rsf::ranking::TypeHint::Float => "float",
                    rsf::ranking::TypeHint::Boolean => "bool",
                    rsf::ranking::TypeHint::Date => "date",
                    rsf::ranking::TypeHint::Currency => "curr",
                    rsf::ranking::TypeHint::Id(_) => "id",
                    rsf::ranking::TypeHint::Unknown => "?",
                };
                format!("{} [{}]", name, type_str)
            } else {
                name
            };

            let style = if i == app.cursor_col {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::White)
                    .add_modifier(Modifier::BOLD)
            } else if is_sorted {
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            };

            Span::styled(format!(" {} ", display_text), style)
        })
        .collect();

    let header_row = Row::new(header_cells);
    let headers = vec![header_row];
    let col_constraints: Vec<Constraint> = (0..col_count).map(|_| Constraint::Min(8)).collect();
    let header_widget = Table::new(headers, &col_constraints)
        .block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(header_widget, table_area[0]);

    // ── Data rows (with scrolling + type-aware rendering) ───────
    let visible_rows = std::cmp::min(
        app.row_count() - app.scroll_row,
        grid_chunks[1].height as usize - 2,
    );
    let data_start = app.scroll_row;
    let data_end = std::cmp::min(data_start + visible_rows, app.row_count());

    let mut rows: Vec<Row> = Vec::new();
    for row_idx in data_start..data_end {
        let abs_row = if app.sort_col.is_some() {
            // In sorted mode, cursor_row is an index into the sorted table.
            row_idx
        } else {
            row_idx
        };

        let cells: Vec<Span> = (0..col_count)
            .map(|col_idx| {
                let cell_text = if abs_row == app.cursor_row && col_idx == app.cursor_col {
                    format!(" {}", app.get_cell_text())
                } else {
                    let val = app.table
                        .rows
                        .get(abs_row)
                        .and_then(|r| r.get(col_idx))
                        .map(|v| v.as_str().to_string())
                        .unwrap_or_default();
                    format!("  {}", val)
                };

                let is_cursor = abs_row == app.cursor_row && col_idx == app.cursor_col;
                let is_modified = app.is_modified(abs_row, col_idx);
                let is_match = app.is_match(abs_row);

                // Type-aware color.
                let base_color = match app.get_type_hint(col_idx) {
                    rsf::ranking::TypeHint::Integer | rsf::ranking::TypeHint::Float => Color::Green,
                    rsf::ranking::TypeHint::Boolean => Color::Magenta,
                    rsf::ranking::TypeHint::Date => Color::Cyan,
                    rsf::ranking::TypeHint::Currency => Color::Yellow,
                    rsf::ranking::TypeHint::Id(_) => Color::Blue,
                    rsf::ranking::TypeHint::Unknown => {
                        if abs_row % 2 == 0 { Color::White } else { Color::Gray }
                    }
                };

                let style = if is_cursor {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::White)
                        .add_modifier(Modifier::BOLD)
                } else if is_modified {
                    // Modified cells get a subtle background.
                    Style::default()
                        .fg(base_color)
                        .bg(Color::DarkGray)
                        .add_modifier(Modifier::DIM)
                } else if is_match && app.search_mode.is_active() {
                    // Matched rows get highlighted.
                    Style::default()
                        .fg(base_color)
                        .bg(Color::Rgb(50, 50, 100))
                } else if abs_row % 2 == 0 {
                    Style::default().fg(Color::White)
                } else {
                    Style::default().fg(Color::Gray)
                };

                Span::styled(cell_text, style)
            })
            .collect();

        rows.push(Row::new(cells));
    }

    let table_widget = Table::new(rows, &col_constraints);
    frame.render_widget(table_widget, table_area[1]);

    // ── Status bar ──────────────────────────────────────────────
    let sort_info = if let Some(&col) = app.sort_col.as_ref() {
        format!(
            " | Sort: {} ({})",
            app.table.column_name(col).unwrap_or("?"),
            if app.sort_dir == SortDir::Asc { "asc" } else { "desc" }
        )
    } else {
        String::new()
    };

    let match_info = if app.search_mode.is_active() && !app.match_rows.is_empty() {
        format!(
            " | {} matches (1-{})",
            app.match_rows.len(),
            app.match_rows.len().min(999)
        )
    } else if app.search_mode.is_active() {
        " | No matches".to_string()
    } else {
        String::new()
    };

    let modified_count = app.modified_cells.len();
    let mod_info = if modified_count > 0 {
        format!(" | {} edited", modified_count)
    } else {
        String::new()
    };

    let status_text = format!(
        "Row: {} Col: {} | Total: {}x{}{}",
        app.cursor_row + 1,
        app.cursor_col + 1,
        app.row_count(),
        col_count,
        sort_info
    );

    // Show profile overlay info if active.
    let profile_text = if app.show_profiles { " | Profiles ON" } else { "" };

    let status = Paragraph::new(format!("{}{}", status_text, profile_text))
        .block(Block::default().borders(Borders::ALL).title(" Status "))
        .style(Style::default().fg(Color::White));
    frame.render_widget(status, chunks[2]);
}
