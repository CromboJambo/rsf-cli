//! Ratatui stdout rendering — inspired by xan's ratatui.rs
//!
//! Renders a ratatui Frame to ANSI-colored text on stdout, enabling non-interactive
//! preview commands (`rsf view`, `rsf hist`) that compose in pipes alongside
//! interactive TUI output.

use colored::{ColoredString, Colorize};
use ratatui::backend::TestBackend;
use ratatui::buffer::Buffer;
use ratatui::style::{Color, Modifier};
use ratatui::{Frame, Terminal};

/// Render a ratatui Frame to stdout as ANSI-colored text.
///
/// This is the xan-inspired pattern: draw into a TestBackend, serialize the buffer
/// with color runs, emit to stdout. Enables pipe-friendly preview commands.
pub fn render_frame_to_stdout<F>(cols: usize, rows: usize, callback: F) -> std::io::Result<()>
where
    F: FnOnce(&mut Frame),
{
    let backend = TestBackend::new(cols as u16, rows as u16);
    let mut terminal = Terminal::new(backend)?;

    terminal.draw(callback)?;

    let buffer = terminal.backend().buffer();
    print_buffer(buffer.content(), cols);

    Ok(())
}

fn group_cells_by_color(cells: &[ratatui::buffer::Cell]) -> Vec<Vec<ratatui::buffer::Cell>> {
    let mut groups: Vec<Vec<ratatui::buffer::Cell>> = Vec::new();
    let mut current_run: Vec<ratatui::buffer::Cell> = Vec::new();

    for cell in cells {
        if current_run.is_empty() || (current_run[0].style() == cell.style()) {
            current_run.push(cell.clone());
            continue;
        }
        groups.push(current_run);
        current_run = vec![cell.clone()];
    }

    if !current_run.is_empty() {
        groups.push(current_run);
    }

    groups
}

fn colorize(s: &str, color: Color, modifier: Modifier) -> ColoredString {
    let colored = match color {
        Color::Reset | Color::White => s.normal(),
        Color::Red => s.red(),
        Color::Blue => s.blue(),
        Color::Cyan => s.cyan(),
        Color::Green => s.green(),
        Color::Yellow => s.yellow(),
        Color::Magenta => s.magenta(),
        Color::Rgb(r, g, b) => s.truecolor(r, g, b),
        _ => s.normal(),
    };

    if modifier.is_empty() {
        return colored;
    }

    match modifier {
        Modifier::DIM => colored.dimmed(),
        _ => colored,
    }
}

fn print_buffer(contents: &[ratatui::buffer::Cell], cols: usize) {
    let mut i = 0;

    while i < contents.len() {
        let end = std::cmp::min(i + cols, contents.len());
        let line = group_cells_by_color(&contents[i..end])
            .iter()
            .map(|cells| {
                colorize(
                    &cells.iter().map(|cell| cell.symbol()).collect::<String>(),
                    cells[0].fg,
                    cells[0].modifier,
                )
                .to_string()
            })
            .collect::<String>();

        println!("{}", line);

        i = end;
    }
}
