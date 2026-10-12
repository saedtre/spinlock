//! The TUI: pick a spinlock-consumer example, read its explanation (or its
//! source), and run it in-process on a background thread while watching its
//! output and a live picture of its traffic through the lock.
//!
//! Keys: `h`/`l` (or `Left`/`Right`) highlight the example list or the
//! right-hand panel; `k`/`j` (or `Up`/`Down`) select in the list or scroll
//! the panel, whichever is highlighted. Each example keeps its own scroll
//! position for its explanation and for its source, the source starting
//! where the example's code begins. `Enter`/`r` run, `c` toggles the code
//! view, `[`/`]` scroll the output pane back and forth, `q`/`Esc` quit.

use std::io::{self, IsTerminal};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, List, ListItem, ListState, Paragraph};
use ratatui::{Frame, Terminal};

use spinlock::examples::{self, Example, ExampleOutput, LaneState, Viz};

/// How long the event loop waits for a key before redrawing / polling the run.
const TICK: Duration = Duration::from_millis(50);

/// One display pause unit handed to the examples: long enough that a run
/// lasts about a second and its picture can be followed.
const PACE: Duration = Duration::from_millis(4);

/// Key hints, most important first: a narrow terminal cuts off the tail.
const HINTS: &str = " q quit  Enter run  c code  h/l pane  j/k move/scroll";

/// Each example file has one comment line with this text, and the line under
/// it is where that example's source is first shown.
const SNIPPET_MARKER: &str = "// snippet:start";

/// RAII terminal setup: raw mode + alternate screen on `new`, both restored
/// on `drop` — so the terminal is put back even if the app panics.
struct TerminalGuard;

impl TerminalGuard {
    fn new() -> io::Result<Self> {
        enable_raw_mode()?;
        // Build the guard before entering the alternate screen so a failure
        // there still restores raw mode through `drop`.
        let guard = TerminalGuard;
        crossterm::execute!(io::stdout(), EnterAlternateScreen)?;
        Ok(guard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = crossterm::execute!(io::stdout(), LeaveAlternateScreen);
    }
}

/// The receiving ends of a run in progress.
struct ActiveRun {
    /// Each output line, the moment the example writes it.
    lines: Receiver<String>,
    /// One message when the run ends: `true` = completed, `false` = cancelled.
    result: Receiver<bool>,
}

/// The two top panes that `h` / `l` move between.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pane {
    List,
    Panel,
}

struct AppState {
    examples: Vec<Box<dyn Example>>,
    selected: usize,
    /// The highlighted top pane; `j` / `k` act on it.
    focus: Pane,
    output: Vec<String>,
    running: bool,
    view_code: bool,
    /// The right-hand panel row each example was last scrolled to, one entry
    /// per example in `examples` order; index 0 of the pair is the
    /// explanation, index 1 the source. `None` = not scrolled in this run,
    /// so the default is shown.
    positions: Vec<[Option<usize>; 2]>,
    /// Cancel token shared with the example currently (or last) running.
    cancel: Arc<AtomicBool>,
    /// The run in progress, if any.
    run: Option<ActiveRun>,
    /// Name of the example that is running or ran last (the selection may
    /// have moved on since).
    run_name: &'static str,
    /// Live picture of the current (or last) run's traffic through the lock.
    viz: Option<Viz>,
    /// Display pause unit given to examples (see [`PACE`]).
    pace: Duration,
    /// Text rows the right-hand panel showed on the last draw (page size).
    panel_rows: usize,
    /// Columns the right-hand panel's text was wrapped to on the last draw:
    /// the panel's full inner width for the explanation, the inner width less
    /// the line number column for the source.
    panel_cols: usize,
    /// The first wrapped row the right-hand panel showed on the last draw,
    /// or was last scrolled to by key.
    panel_top: usize,
    /// Wrapped rows of the right-hand panel's text on the last draw.
    panel_len: usize,
    /// How many (wrapped) rows the output pane is scrolled back from the
    /// newest line; 0 = follow the newest.
    output_scroll: usize,
    /// Text rows the output pane showed on the last draw (page size).
    output_rows: usize,
}

impl AppState {
    fn new() -> Self {
        let examples = examples::all();
        let positions = vec![[None, None]; examples.len()];
        Self {
            examples,
            selected: 0,
            focus: Pane::List,
            output: vec!["press Enter or r to run the selected example".to_string()],
            running: false,
            view_code: false,
            positions,
            cancel: Arc::new(AtomicBool::new(false)),
            run: None,
            run_name: "",
            viz: None,
            pace: PACE,
            panel_rows: 1,
            panel_cols: 1,
            panel_top: 0,
            panel_len: 0,
            output_scroll: 0,
            output_rows: 1,
        }
    }

    fn select_previous(&mut self) {
        let n = self.examples.len();
        self.selected = (self.selected + n - 1) % n;
    }

    fn select_next(&mut self) {
        self.selected = (self.selected + 1) % self.examples.len();
    }

    /// Move every line the running example has written so far into the
    /// output pane, and notice when the run has ended. Never blocks.
    fn poll_run(&mut self) {
        let Some(run) = &self.run else {
            return;
        };
        // Check for the end first: the worker sends its result only after
        // its last line, so draining afterwards cannot miss one.
        let ended = match run.result.try_recv() {
            Ok(true) => Some("[done]"),
            Ok(false) => Some("[cancelled]"),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => Some("[example panicked before finishing]"),
        };
        self.output.extend(run.lines.try_iter());
        if let Some(marker) = ended {
            self.output.push(marker.to_string());
            self.running = false;
            self.run = None;
        }
    }

    /// Handle one key press. Returns `true` when the app should quit.
    fn handle_key(&mut self, key: KeyEvent) -> bool {
        // Raw mode delivers Ctrl-C as a key, not a signal: treat it as quit.
        let ctrl_c =
            key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL);
        let output_page = self.output_rows.saturating_sub(1).max(1);
        match key.code {
            _ if ctrl_c => return self.quit(),
            KeyCode::Char('q') | KeyCode::Esc => return self.quit(),
            KeyCode::Left | KeyCode::Char('h') => self.focus = Pane::List,
            KeyCode::Right | KeyCode::Char('l') => self.focus = Pane::Panel,
            KeyCode::Up | KeyCode::Char('k') => match self.focus {
                Pane::List => self.select_previous(),
                Pane::Panel => {
                    if self.panel_top > 0 {
                        let row = self.panel_top - 1;
                        self.positions[self.selected][usize::from(self.view_code)] = Some(row);
                        self.panel_top = row;
                    }
                }
            },
            KeyCode::Down | KeyCode::Char('j') => match self.focus {
                Pane::List => self.select_next(),
                Pane::Panel => {
                    // The bound and the starting row come from the last draw.
                    if self.panel_top < self.panel_len.saturating_sub(self.panel_rows) {
                        let row = self.panel_top + 1;
                        self.positions[self.selected][usize::from(self.view_code)] = Some(row);
                        self.panel_top = row;
                    }
                }
            },
            KeyCode::Enter | KeyCode::Char('r') => run_example(self),
            KeyCode::Char('c') => self.view_code = !self.view_code,
            // Output pane: `[` back towards the first line (`ui` clamps it),
            // `]` forward to the newest.
            KeyCode::Char('[') => {
                self.output_scroll = self.output_scroll.saturating_add(output_page)
            }
            KeyCode::Char(']') => {
                self.output_scroll = self.output_scroll.saturating_sub(output_page)
            }
            _ => {}
        }
        false
    }

    /// Ask a run still in progress to wind down (it is bounded either way).
    fn quit(&mut self) -> bool {
        self.cancel.store(true, Ordering::Relaxed);
        true
    }
}

/// Start the selected example on a detached background thread. The thread
/// owns a `clone_box()` of the example and its `ExampleOutput`; the event
/// loop keeps running, receives each line as it is written and samples the
/// run's picture, both through `AppState`.
fn run_example(state: &mut AppState) {
    if state.running {
        return;
    }
    let mut example = state.examples[state.selected].clone_box();
    let cancel = Arc::new(AtomicBool::new(false));
    let (mut out, lines) = ExampleOutput::streaming(Arc::clone(&cancel), state.pace);
    let (tx, result) = mpsc::channel();

    state.run_name = example.name();
    state.output = vec![if state.pace.is_zero() {
        format!("running: {} ...", example.name())
    } else {
        format!("running: {} ... (slowed down so it can be watched)", example.name())
    }];
    state.output_scroll = 0;
    state.cancel = cancel;
    state.viz = Some(out.viz());
    state.run = Some(ActiveRun { lines, result });
    state.running = true;

    thread::spawn(move || {
        let completed = example.run(&mut out);
        let _ = tx.send(completed);
    });
}

/// Greedy word wrap to `width` columns. Explicit newlines are kept; a word
/// longer than the width is split.
fn wrap_words(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut rows = Vec::new();
    for paragraph in text.lines() {
        let mut row = String::new();
        let mut len = 0;
        for word in paragraph.split_whitespace() {
            let mut word: Vec<char> = word.chars().collect();
            if len > 0 && len + 1 + word.len() > width {
                rows.push(std::mem::take(&mut row));
                len = 0;
            }
            while word.len() > width {
                rows.push(word.drain(..width).collect());
            }
            if len > 0 {
                row.push(' ');
                len += 1;
            }
            len += word.len();
            row.extend(word);
        }
        rows.push(row);
    }
    rows
}

/// Hard wrap to `width` columns for source code. Each row carries the
/// 1-based number of its file line on that line's first row, and nothing on
/// the line's continuation rows.
fn wrap_code_numbered(text: &str, width: usize) -> Vec<(Option<usize>, String)> {
    let width = width.max(1);
    let mut rows = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let number = i + 1;
        let chars: Vec<char> = line.chars().collect();
        if chars.is_empty() {
            rows.push((Some(number), String::new()));
        }
        for (k, chunk) in chars.chunks(width).enumerate() {
            rows.push(((k == 0).then_some(number), chunk.iter().collect()));
        }
    }
    rows
}

/// Hard wrap to `width` columns, keeping indentation (for source code).
fn wrap_code(text: &str, width: usize) -> Vec<String> {
    wrap_code_numbered(text, width)
        .into_iter()
        .map(|(_, row)| row)
        .collect()
}

/// How many decimal digits the largest line number of `text` has (at least
/// one): the width of the source view's line number column.
fn line_number_digits(text: &str) -> usize {
    text.lines().count().max(1).to_string().len()
}

/// The byte offset at which an example's snippet begins: the start of the
/// line that follows the first line whose text, with leading and trailing
/// whitespace removed, is exactly [`SNIPPET_MARKER`]. If no line matches,
/// the result is `0`.
fn snippet_start(code: &str) -> usize {
    let mut offset = 0;
    for line in code.split_inclusive('\n') {
        offset += line.len();
        if line.trim() == SNIPPET_MARKER {
            return offset;
        }
    }
    0
}

fn ui(frame: &mut Frame, state: &mut AppState) -> io::Result<()> {
    let area = frame.area();
    let example = &state.examples[state.selected];
    let picture = state.viz.as_ref().map(Viz::snapshot).filter(|p| !p.is_empty());

    // Heights: the picture gets exactly what it needs; the output pane half
    // of the rest once there is a run to report on (two fifths before that,
    // so the idle screen shows the whole list), at least 3 text rows; the
    // list + explanation everything else.
    let viz_height = picture.as_ref().map_or(1, |p| p.lanes.len() as u16 + 2) + 2;
    let rest = area.height.saturating_sub(1 + viz_height);
    let output_share = if picture.is_some() { rest / 2 } else { rest * 2 / 5 };
    let output_height = output_share.clamp(5, 14);
    let [status_area, middle_area, viz_area, output_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(viz_height),
        Constraint::Length(output_height),
    ])
    .areas(area);

    // Row 0: status / help bar. The name and the run indicator keep their
    // width; the key hints take what is left, most important first.
    let title = " spinlock ";
    let indicator = if state.running {
        " ● running "
    } else {
        " ○ idle "
    };
    let [title_area, hints_area, indicator_area] = Layout::horizontal([
        Constraint::Length(title.chars().count() as u16),
        Constraint::Min(0),
        Constraint::Length(indicator.chars().count() as u16),
    ])
    .areas(status_area);
    let bar = Style::new().add_modifier(Modifier::REVERSED);
    frame.render_widget(
        Paragraph::new(title).style(bar.add_modifier(Modifier::BOLD)),
        title_area,
    );
    frame.render_widget(Paragraph::new(HINTS).style(bar), hints_area);
    frame.render_widget(Paragraph::new(indicator).style(bar), indicator_area);

    // Row 1: example list (left) + explanation or code (right).
    let [list_area, panel_area] =
        Layout::horizontal([Constraint::Percentage(45), Constraint::Min(1)]).areas(middle_area);

    let items: Vec<ListItem> = state
        .examples
        .iter()
        .enumerate()
        .map(|(i, e)| {
            ListItem::new(Line::from(vec![
                Span::raw(format!("{:>2}. {}", i + 1, e.name())),
                Span::styled(
                    format!(" — {}", e.one_liner()),
                    Style::new().add_modifier(Modifier::DIM),
                ),
            ]))
        })
        .collect();
    let list = List::new(items)
        .block(
            Block::bordered()
                .border_type(if state.focus == Pane::List {
                    BorderType::Thick
                } else {
                    BorderType::Plain
                })
                .title("Examples"),
        )
        .highlight_style(Style::new().add_modifier(Modifier::REVERSED))
        .highlight_symbol("> ");
    let mut list_state = ListState::default().with_selected(Some(state.selected));
    frame.render_stateful_widget(list, list_area, &mut list_state);

    // The panel text is wrapped here rather than by the widget, so the row
    // count is known and the scroll offset can stop exactly at the last page.
    let panel_block = Block::bordered().border_type(if state.focus == Pane::Panel {
        BorderType::Thick
    } else {
        BorderType::Plain
    });
    let inner = panel_block.inner(panel_area);
    let (cols, visible) = (inner.width as usize, inner.height as usize);
    // The source gives up a column for its line numbers (`digits` wide, plus
    // one blank) and is wrapped to what is left; the explanation has none.
    let (panel_title, text_cols, digits, numbers, rows) = if state.view_code {
        let digits = line_number_digits(example.code());
        let text_cols = cols.saturating_sub(digits + 1);
        let (numbers, rows): (Vec<Option<usize>>, Vec<String>) =
            wrap_code_numbered(example.code(), text_cols)
                .into_iter()
                .unzip();
        ("Code (c: explanation)", text_cols, digits, numbers, rows)
    } else {
        (
            "Why a spinlock here? (c: code)",
            cols,
            0,
            Vec::new(),
            wrap_words(example.explanation(), cols),
        )
    };
    // Where the view on display starts until it is scrolled: the
    // explanation at its first row, the source at the row its snippet begins
    // on at the width its code is wrapped to.
    let default_top = if state.view_code {
        let code = example.code();
        wrap_code(&code[..snippet_start(code)], text_cols).len()
    } else {
        0
    };
    // The stored row if this example's view has been scrolled, else the
    // default; neither is written back to `positions` here.
    let top = state.positions[state.selected][usize::from(state.view_code)]
        .unwrap_or(default_top)
        .min(rows.len().saturating_sub(visible));
    state.panel_rows = visible;
    state.panel_len = rows.len();
    state.panel_cols = text_cols;
    state.panel_top = top;
    let last = (top + visible).min(rows.len());
    let mut panel_block = panel_block.title(panel_title);
    if rows.len() > visible {
        // Say that there is more, and how to reach it.
        panel_block = panel_block.title_bottom(
            Line::from(format!(
                " rows {}-{} of {} (l, then j/k) ",
                top + 1,
                last,
                rows.len()
            ))
            .right_aligned(),
        );
    }
    let shown: Vec<Line> = if state.view_code {
        // Each row: its file line number, dimmed, on the line's first row (a
        // blank column of the same width on continuation rows), then the code.
        rows[top..last]
            .iter()
            .zip(&numbers[top..last])
            .map(|(r, number)| {
                let label = match number {
                    Some(n) => format!("{n:>digits$} "),
                    None => " ".repeat(digits + 1),
                };
                Line::from(vec![
                    Span::styled(label, Style::new().add_modifier(Modifier::DIM)),
                    Span::raw(r.as_str()),
                ])
            })
            .collect()
    } else {
        rows[top..last]
            .iter()
            .map(|r| Line::raw(r.as_str()))
            .collect()
    };
    frame.render_widget(Paragraph::new(shown).block(panel_block), panel_area);

    // Row 2: the live picture of the run's traffic through the lock.
    let viz_block = Block::bordered().title(if state.run_name.is_empty() {
        "Through the spinlock".to_string()
    } else {
        format!("Through the spinlock — {}", state.run_name)
    });
    let viz_cols = viz_block.inner(viz_area).width as usize;
    let viz_lines: Vec<Line> = match &picture {
        None => vec![Line::styled(
            "run an example to watch its threads take the lock",
            Style::new().add_modifier(Modifier::DIM),
        )],
        Some(p) => p
            .render(viz_cols)
            .into_iter()
            .enumerate()
            .map(|(i, text)| {
                // Line 0 is the lock, the last line the total; the lines in
                // between are the lanes, coloured by what the thread is doing.
                let style = match i.checked_sub(1).and_then(|lane| p.lanes.get(lane)) {
                    Some(lane) => match lane.state {
                        LaneState::Holding => Style::new().fg(Color::Green).add_modifier(Modifier::BOLD),
                        LaneState::Spinning => Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                        LaneState::Done => Style::new().add_modifier(Modifier::DIM),
                        LaneState::Idle => Style::new(),
                    },
                    None => Style::new().add_modifier(Modifier::BOLD),
                };
                Line::styled(text, style)
            })
            .collect(),
    };
    frame.render_widget(Paragraph::new(viz_lines).block(viz_block), viz_area);

    // Row 3: output pane, wrapped to its width. It follows the newest line
    // unless it has been scrolled back with `[`.
    let output_block = Block::bordered().title("Output");
    let inner = output_block.inner(output_area);
    let visible = inner.height as usize;
    let wrapped: Vec<String> = state
        .output
        .iter()
        .flat_map(|l| wrap_words(l, inner.width as usize))
        .collect();
    let hidden = wrapped.len().saturating_sub(visible);
    state.output_rows = visible;
    state.output_scroll = state.output_scroll.min(hidden);
    let first = hidden - state.output_scroll;
    let last = (first + visible).min(wrapped.len());
    let mut output_block = output_block;
    if hidden > 0 {
        // Say that there is more, and how to reach it.
        output_block = output_block.title_bottom(
            Line::from(format!(
                " lines {}-{} of {} ([ back, ] forward) ",
                first + 1,
                last,
                wrapped.len()
            ))
            .right_aligned(),
        );
    }
    let lines: Vec<Line> = wrapped[first..last]
        .iter()
        .map(|l| Line::raw(l.as_str()))
        .collect();
    frame.render_widget(Paragraph::new(lines).block(output_block), output_area);

    Ok(())
}

fn main() -> io::Result<()> {
    // Raw mode needs a real terminal. Without one (output piped, or started
    // from a script) say so, rather than failing with a bare OS error.
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        eprintln!(
            "spinlock: this is an interactive terminal program; run `cargo run` \
             from a terminal window (stdin and stdout must both be a terminal)"
        );
        std::process::exit(1);
    }

    let _guard = TerminalGuard::new()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let mut state = AppState::new();

    loop {
        state.poll_run();

        let mut drawn = Ok(());
        terminal.draw(|frame| drawn = ui(frame, &mut state))?;
        drawn?;

        if !event::poll(TICK)? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            // Resize and other events: the next draw recomputes the layout.
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        if state.handle_key(key) {
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    use std::time::Instant;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    /// Draw one frame on an in-memory terminal and return its rows as text.
    fn draw(state: &mut AppState, width: u16, height: u16) -> Vec<String> {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal.draw(|frame| ui(frame, state).unwrap()).unwrap();
        let buffer = terminal.backend().buffer();
        (0..height)
            .map(|y| (0..width).map(|x| buffer[(x, y)].symbol()).collect())
            .collect()
    }

    /// Poll until the run in progress ends, failing the test if it hangs.
    fn finish_run(state: &mut AppState) {
        let started = Instant::now();
        while state.running {
            assert!(started.elapsed() < Duration::from_secs(10), "run hung");
            thread::sleep(Duration::from_millis(1));
            state.poll_run();
        }
    }

    #[test]
    fn selection_wraps_in_both_directions_with_all_four_keys() {
        let mut state = AppState::new();
        assert!(!state.handle_key(key(KeyCode::Char('k'))));
        assert_eq!(state.selected, 9);
        state.handle_key(key(KeyCode::Char('j')));
        assert_eq!(state.selected, 0);
        state.handle_key(key(KeyCode::Down));
        assert_eq!(state.selected, 1);
        state.handle_key(key(KeyCode::Up));
        state.handle_key(key(KeyCode::Up));
        assert_eq!(state.selected, 9);
    }

    #[test]
    fn c_toggles_code_view_and_shows_the_source_from_its_snippet() {
        let mut state = AppState::new();
        state.handle_key(key(KeyCode::Char('c')));
        assert!(state.view_code);
        let screen = draw(&mut state, 100, 30).join("\n");
        assert!(screen.contains("Code (c: explanation)"));
        assert!(screen.contains("fn run(&mut self"), "{screen}");
        assert!(!screen.contains(SNIPPET_MARKER), "{screen}");
        assert!(state.panel_top > 0);

        state.handle_key(key(KeyCode::Char('c')));
        let screen = draw(&mut state, 100, 30).join("\n");
        assert!(!state.view_code);
        assert!(screen.contains("Why a spinlock here? (c: code)"));
        assert_eq!(state.panel_top, 0);
    }

    #[test]
    fn quit_keys_quit_and_cancel_the_run() {
        for quit in [
            key(KeyCode::Char('q')),
            key(KeyCode::Esc),
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
        ] {
            let mut state = AppState::new();
            state.handle_key(key(KeyCode::Enter));
            assert!(state.running);
            let cancel = Arc::clone(&state.cancel);
            assert!(state.handle_key(quit));
            assert!(cancel.load(Ordering::Relaxed));
            // The paced run (about a second if left alone) stops promptly.
            let started = Instant::now();
            finish_run(&mut state);
            assert!(started.elapsed() < Duration::from_millis(500));
            assert_eq!(state.output.last().unwrap(), "[cancelled]");
        }
    }

    /// AC-4: lines reach the output pane while the example is still running.
    #[test]
    fn output_is_streamed_into_the_pane_during_the_run() {
        let mut state = AppState::new();
        state.pace = Duration::ZERO;
        state.selected = 5; // "Held across a syscall": ~100 ms, lines up front
        run_example(&mut state);
        assert_eq!(state.output.len(), 1);

        let started = Instant::now();
        while state.output.len() < 3 {
            assert!(started.elapsed() < Duration::from_secs(10), "no output");
            thread::sleep(Duration::from_millis(1));
            state.poll_run();
        }
        assert!(state.running, "lines only arrived once the run had ended");
        assert!(state.output[1].starts_with("the holder will sleep"));
        // ...and the pane shows them, mid-run.
        let screen = draw(&mut state, 80, 24).join("\n");
        assert!(screen.contains("the holder will sleep"), "{screen}");
        assert!(screen.contains("● running"), "{screen}");

        finish_run(&mut state);
        let n = state.output.len();
        assert_eq!(state.output[n - 1], "[done]");
        assert!(state.output[n - 2].starts_with("holder released; final value = 9"));
    }

    #[test]
    fn a_second_run_request_is_ignored_while_running() {
        let mut state = AppState::new();
        state.selected = 5;
        run_example(&mut state);
        let cancel = Arc::clone(&state.cancel);
        state.selected = 0;
        state.handle_key(key(KeyCode::Char('r')));
        assert!(Arc::ptr_eq(&cancel, &state.cancel));
        assert_eq!(state.run_name, "Held across a syscall (when NOT to use)");
        state.quit();
        finish_run(&mut state);
    }

    /// Every example runs to completion from the TUI and leaves its final
    /// line, a `[done]` marker and a finished picture on screen.
    #[test]
    fn every_example_runs_from_the_tui_and_draws_its_picture() {
        let mut state = AppState::new();
        state.pace = Duration::ZERO;
        for i in 0..state.examples.len() {
            state.selected = i;
            state.handle_key(key(KeyCode::Enter));
            finish_run(&mut state);
            assert_eq!(state.output.last().unwrap(), "[done]", "example {i}");

            let screen = draw(&mut state, 80, 24);
            let text = screen.join("\n");
            assert!(text.contains("Through the spinlock — "), "{text}");
            assert!(text.contains("through the lock"), "{text}");
            assert!(text.contains("[done]"), "{text}");
            for row in &screen {
                assert_eq!(row.chars().count(), 80);
            }
        }
    }

    /// A picture sampled mid-run shows a thread holding the lock and
    /// others spinning behind it.
    #[test]
    fn the_picture_is_live_during_a_run() {
        let mut state = AppState::new();
        state.pace = Duration::ZERO;
        state.selected = 5; // holder sleeps ~100 ms with the lock
        run_example(&mut state);
        let started = Instant::now();
        let seen = loop {
            assert!(state.running, "run ended before a contended frame was seen");
            assert!(started.elapsed() < Duration::from_secs(10));
            let text = draw(&mut state, 80, 24).join("\n");
            if text.contains("HELD by holder") && text.contains("SPIN") {
                break text;
            }
            thread::sleep(Duration::from_millis(1));
            state.poll_run();
        };
        assert!(seen.contains("contender 1"), "{seen}");
        finish_run(&mut state);
    }

    /// At 80x24 the key hints are all visible and every explanation can be
    /// read to its last row with `j`.
    #[test]
    fn at_80x24_hints_fit_and_every_explanation_scrolls_to_its_end() {
        let mut state = AppState::new();
        state.handle_key(key(KeyCode::Char('l')));
        for i in 0..state.examples.len() {
            state.selected = i;
            let first = draw(&mut state, 80, 24);
            assert!(first[0].contains(HINTS.trim()), "{:?}", first[0]);
            assert!(first[0].contains("○ idle"), "{:?}", first[0]);

            let rows = wrap_words(state.examples[i].explanation(), state.panel_cols);
            assert!(rows.len() > 1);
            for row in &rows {
                assert!(row.chars().count() <= state.panel_cols);
            }
            let needs_scroll = rows.len() > state.panel_rows;
            if needs_scroll {
                assert!(first.join("\n").contains("(l, then j/k)"));
                assert!(!first.join("\n").contains("PgUp"));
            }
            for _ in 0..rows.len() {
                state.handle_key(key(KeyCode::Char('j')));
                draw(&mut state, 80, 24);
            }
            let end = draw(&mut state, 80, 24).join("\n");
            assert!(end.contains(rows.last().unwrap().as_str()), "example {i}:\n{end}");
            assert_eq!(state.panel_top > 0, needs_scroll);
            // ...and back to the top again.
            for _ in 0..rows.len() {
                state.handle_key(key(KeyCode::Char('k')));
            }
            assert_eq!(state.panel_top, 0);
            let top = draw(&mut state, 80, 24).join("\n");
            assert!(top.contains(rows[0].as_str()));
        }
    }

    /// At 80x24 every line a finished run wrote can be read: what does not
    /// fit in the output pane is reachable with `[`, and `]` returns to the
    /// newest line.
    #[test]
    fn at_80x24_every_output_line_of_every_example_can_be_read() {
        let mut state = AppState::new();
        state.pace = Duration::ZERO;
        for i in 0..state.examples.len() {
            state.selected = i;
            state.handle_key(key(KeyCode::Enter));
            assert_eq!(state.output_scroll, 0);
            finish_run(&mut state);

            // The newest lines are shown first...
            let newest = draw(&mut state, 80, 24);
            assert!(newest.join("\n").contains("[done]"), "example {i}");
            // ...and paging back shows every other row at some point.
            let rows: Vec<String> = state
                .output
                .iter()
                .flat_map(|l| wrap_words(l, 78))
                .collect();
            let scrolls = rows.len() > state.output_rows;
            let mut seen = newest.join("\n");
            for _ in 0..rows.len() {
                state.handle_key(key(KeyCode::Char('[')));
                let screen = draw(&mut state, 80, 24).join("\n");
                assert_eq!(screen.contains("([ back, ] forward)"), scrolls, "example {i}");
                seen.push_str(&screen);
            }
            for row in &rows {
                assert!(seen.contains(row.as_str()), "example {i}: never shown: {row:?}");
            }
            // Scrolled right back, the pane starts at the run's first line.
            assert_eq!(state.output_scroll, rows.len().saturating_sub(state.output_rows));
            let top = draw(&mut state, 80, 24).join("\n");
            assert!(top.contains(rows[0].as_str()), "example {i}:\n{top}");

            for _ in 0..rows.len() {
                state.handle_key(key(KeyCode::Char(']')));
            }
            assert_eq!(state.output_scroll, 0);
            assert!(draw(&mut state, 80, 24).join("\n").contains("[done]"));
        }
    }

    /// The idle screen at 80x24 lists all ten examples.
    #[test]
    fn at_80x24_the_idle_screen_lists_all_ten_examples() {
        let mut state = AppState::new();
        let screen = draw(&mut state, 80, 24).join("\n");
        for n in 1..=10 {
            assert!(screen.contains(&format!("{n:>2}. ")), "{n}:\n{screen}");
        }
    }

    #[test]
    fn long_output_lines_are_wrapped_not_clipped() {
        let mut state = AppState::new();
        state.output = vec![format!("{} END-OF-LINE", "word ".repeat(30))];
        let screen = draw(&mut state, 80, 24).join("\n");
        assert!(screen.contains("END-OF-LINE"), "{screen}");
    }

    #[test]
    fn ui_draws_at_tiny_and_large_sizes() {
        let mut state = AppState::new();
        state.pace = Duration::ZERO;
        state.handle_key(key(KeyCode::Enter));
        finish_run(&mut state);
        for (w, h) in [(1, 1), (10, 4), (20, 8), (40, 12), (200, 60)] {
            for view_code in [false, true] {
                state.view_code = view_code;
                for focus in [Pane::List, Pane::Panel] {
                    for pair in [[None, None], [Some(usize::MAX), Some(usize::MAX)]] {
                        state.focus = focus;
                        state.positions[state.selected] = pair;
                        draw(&mut state, w, h);
                    }
                }
            }
        }
    }

    #[test]
    fn wrap_words_keeps_every_word_and_respects_the_width() {
        let text = "alpha beta  gamma\ndelta averyveryverylongword end";
        for width in 1..40 {
            let rows = wrap_words(text, width);
            assert!(rows.iter().all(|r| r.chars().count() <= width), "{width}");
            let joined: String = rows.concat().split_whitespace().collect();
            assert_eq!(joined, "alphabetagammadeltaaveryveryverylongwordend");
        }
        assert_eq!(wrap_words("one two three", 7), ["one two", "three"]);
        assert_eq!(wrap_code("    let x = 1;\n\nab", 8), ["    let ", "x = 1;", "", "ab"]);
    }

    /// AC-1, AC-2, AC-3, AC-4: the list is highlighted at start-up, `l` and
    /// `h` move the highlight, and it does not wrap.
    #[test]
    fn the_list_is_highlighted_at_start_and_h_l_move_the_highlight_without_wrapping() {
        let mut state = AppState::new();
        assert_eq!(state.focus, Pane::List);
        let positions = state.positions.clone();
        for (code, expected) in [
            ('h', Pane::List),
            ('l', Pane::Panel),
            ('l', Pane::Panel),
            ('h', Pane::List),
        ] {
            assert!(!state.handle_key(key(KeyCode::Char(code))));
            assert_eq!(state.focus, expected, "after {code}");
            assert_eq!(state.selected, 0);
            assert!(!state.view_code);
            assert_eq!(state.positions, positions);
        }
    }

    /// AC-15: `Left` / `Right` / `Down` / `Up` behave as `h` / `l` / `j` /
    /// `k`, on whichever pane is highlighted.
    #[test]
    fn arrow_keys_mirror_the_vim_keys() {
        let mut state = AppState::new();
        state.handle_key(key(KeyCode::Left));
        assert_eq!(state.focus, Pane::List);
        state.handle_key(key(KeyCode::Right));
        assert_eq!(state.focus, Pane::Panel);
        state.handle_key(key(KeyCode::Right));
        assert_eq!(state.focus, Pane::Panel);

        state.view_code = true;
        draw(&mut state, 80, 24);
        let d = state.panel_top;
        assert!(d > 0);
        assert!(d < state.panel_len - state.panel_rows);
        state.handle_key(key(KeyCode::Down));
        assert_eq!(state.panel_top, d + 1);
        assert_eq!(state.selected, 0);
        state.handle_key(key(KeyCode::Up));
        assert_eq!(state.panel_top, d);
        state.handle_key(key(KeyCode::Up));
        assert_eq!(state.panel_top, d - 1);

        state.handle_key(key(KeyCode::Left));
        assert_eq!(state.focus, Pane::List);
        state.handle_key(key(KeyCode::Down));
        assert_eq!(state.selected, 1);
        state.handle_key(key(KeyCode::Up));
        assert_eq!(state.selected, 0);
        state.handle_key(key(KeyCode::Up));
        assert_eq!(state.selected, 9);
        state.handle_key(key(KeyCode::Down));
        assert_eq!(state.selected, 0);
        // The position stored with the arrow keys is the one shown on return.
        draw(&mut state, 80, 24);
        assert_eq!(state.panel_top, d - 1);
    }

    /// AC-5: the highlighted pane has a thick border, the other a plain one.
    #[test]
    fn the_highlighted_pane_is_drawn_with_a_thick_border() {
        let mut state = AppState::new();
        let list = draw(&mut state, 80, 24);
        assert!(list[1].starts_with('┏'), "{:?}", list[1]);
        assert!(list[1].ends_with('┐'), "{:?}", list[1]);

        state.handle_key(key(KeyCode::Char('l')));
        let panel = draw(&mut state, 80, 24);
        assert!(panel[1].starts_with('┌'), "{:?}", panel[1]);
        assert!(panel[1].ends_with('┓'), "{:?}", panel[1]);

        for row in list.iter().chain(&panel) {
            assert_eq!(row.chars().count(), 80);
        }
    }

    /// Supports AC-16: the snippet begins on the line after the first line
    /// that is the marker and nothing else.
    #[test]
    fn snippet_start_is_the_line_after_the_first_marker() {
        let text = "// one\n// two\n    // snippet:start\nlet a = 1;\n// snippet:start\nlet b = 2;\n";
        let start = snippet_start(text);
        assert_eq!(start, text.find("let a").unwrap());
        assert!(text[start..].starts_with("let a = 1;\n"));

        // The marker after other text on a line is not a marker line.
        let trailing = "let a = 1; // snippet:start\nlet b = 2;\n";
        assert_eq!(snippet_start(trailing), 0);
        // Trailing spaces do not hide it.
        let spaces = "// one\n// snippet:start   \nlet a = 1;\n";
        assert_eq!(snippet_start(spaces), spaces.find("let a").unwrap());

        assert_eq!(snippet_start("// one\nlet a = 1;\n"), 0);
        assert_eq!(snippet_start(""), 0);
        let last = "let a = 1;\n// snippet:start";
        assert_eq!(snippet_start(last), last.len());
    }

    /// AC-16: every example file has exactly one marker line, directly above
    /// the line approved as the beginning of its snippet.
    #[test]
    fn every_example_marks_its_snippet_once_on_the_approved_line() {
        let run = "    fn run(&mut self, out: &mut ExampleOutput) -> bool {";
        let stats = "/// A small multi-field stat block that must be updated as one unit.";
        let ring = "/// A fixed-capacity ring: pre-allocated slots, no heap growth, ever.";
        // In `examples::all()` order.
        let approved = [run, stats, run, run, run, run, run, ring, run, run];
        let examples = examples::all();
        assert_eq!(examples.len(), approved.len());
        for (i, example) in examples.iter().enumerate() {
            let lines: Vec<&str> = example.code().lines().collect();
            let markers: Vec<usize> = (0..lines.len())
                .filter(|&n| lines[n].trim() == SNIPPET_MARKER)
                .collect();
            assert_eq!(markers.len(), 1, "example {i}");
            assert_eq!(lines[markers[0] + 1], approved[i], "example {i}");
        }
    }

    /// AC-16: an example's source is first shown from its own snippet, and
    /// showing that default stores nothing.
    #[test]
    fn the_source_view_starts_at_each_examples_own_snippet() {
        let mut tops = Vec::new();
        for i in 0..examples::all().len() {
            let mut state = AppState::new();
            state.selected = i;
            state.view_code = true;
            let screen = draw(&mut state, 80, 24);
            let rows = wrap_code(state.examples[i].code(), state.panel_cols);
            let marker_row = rows
                .iter()
                .position(|r| r.trim_start() == SNIPPET_MARKER)
                .unwrap();
            let expected = marker_row + 1;
            assert_eq!(state.panel_top, expected, "example {i}");
            assert!(state.panel_top > 0, "example {i}");
            assert!(screen[2].contains(rows[expected].as_str()), "example {i}: {:?}", screen[2]);
            assert!(!screen.join("\n").contains(SNIPPET_MARKER), "example {i}");
            assert!(state.positions.iter().all(|pair| *pair == [None, None]));
            tops.push(state.panel_top);

            if i == 0 {
                // Where the whole file fits, the last position the content
                // allows is shown instead...
                draw(&mut state, 200, 200);
                assert_eq!(
                    state.panel_top,
                    state.panel_len.saturating_sub(state.panel_rows)
                );
                // ...and the real default again once it does not.
                draw(&mut state, 80, 24);
                assert_eq!(state.panel_top, expected);
            }
        }
        assert!(tops.iter().any(|top| *top != tops[0]), "{tops:?}");
    }

    /// AC-17: an example's explanation is first shown from its first row.
    #[test]
    fn the_explanation_view_starts_at_its_first_row() {
        for i in 0..examples::all().len() {
            let mut state = AppState::new();
            state.selected = i;
            let screen = draw(&mut state, 80, 24).join("\n");
            assert!(!state.view_code);
            assert_eq!(state.panel_top, 0, "example {i}");
            let rows = wrap_words(state.examples[i].explanation(), state.panel_cols);
            assert!(screen.contains(rows[0].as_str()), "example {i}:\n{screen}");
        }
    }

    /// AC-6, AC-18 (and AC-15 for selection): `j` / `k` select while the list
    /// is highlighted, and every example comes back at the position it was
    /// left at, never at another example's.
    #[test]
    fn j_and_k_select_in_the_highlighted_list_and_each_example_is_shown_at_its_own_position() {
        let mut state = AppState::new();
        state.view_code = true;
        draw(&mut state, 80, 24);
        let press = |state: &mut AppState, code: KeyCode| {
            state.handle_key(key(code));
            draw(state, 80, 24).join("\n")
        };
        let (j, k) = (KeyCode::Char('j'), KeyCode::Char('k'));
        let (h, l) = (KeyCode::Char('h'), KeyCode::Char('l'));

        // Defaults on first visit, with wrap.
        let d0 = state.panel_top;
        press(&mut state, j);
        assert_eq!(state.selected, 1);
        let d1 = state.panel_top;
        press(&mut state, k);
        assert_eq!((state.selected, state.panel_top), (0, d0));
        press(&mut state, k);
        assert_eq!(state.selected, 9);
        let d9 = state.panel_top;
        press(&mut state, j);
        assert_eq!((state.selected, state.panel_top), (0, d0));
        assert!(d0 + 2 <= state.panel_len - state.panel_rows);
        assert_ne!(d1, d0 + 2);

        // Leave example 0 scrolled: its position is not applied to example 1.
        press(&mut state, l);
        press(&mut state, j);
        press(&mut state, j);
        assert_eq!(state.panel_top, d0 + 2);
        press(&mut state, h);
        let frame = press(&mut state, j);
        assert_eq!((state.selected, state.panel_top), (1, d1));
        assert!(frame.contains(&format!(" rows {}-", d1 + 1)), "{frame}");

        // Return to it.
        press(&mut state, l);
        press(&mut state, j);
        assert_eq!(state.panel_top, d1 + 1);
        press(&mut state, h);
        let frame = press(&mut state, k);
        assert_eq!((state.selected, state.panel_top), (0, d0 + 2));
        assert!(frame.contains(&format!(" rows {}-", d0 + 3)), "{frame}");

        // Cycle.
        press(&mut state, j);
        assert_eq!((state.selected, state.panel_top), (1, d1 + 1));
        press(&mut state, k);
        assert_eq!((state.selected, state.panel_top), (0, d0 + 2));

        // Across the wrap.
        press(&mut state, k);
        assert_eq!((state.selected, state.panel_top), (9, d9));
        press(&mut state, j);
        assert_eq!((state.selected, state.panel_top), (0, d0 + 2));

        // Through several others.
        for code in [j, j, j, k, k, k] {
            press(&mut state, code);
        }
        assert_eq!((state.selected, state.panel_top), (0, d0 + 2));

        // The arrow keys take the same path.
        press(&mut state, KeyCode::Down);
        assert_eq!((state.selected, state.panel_top), (1, d1 + 1));
        press(&mut state, KeyCode::Up);
        assert_eq!((state.selected, state.panel_top), (0, d0 + 2));
    }

    /// AC-19 (and AC-13's `c` clause): an example's explanation and source
    /// positions are separate, and `c` shows each view at its own.
    #[test]
    fn explanation_and_source_positions_are_independent_and_c_shows_each_at_its_own() {
        let mut state = AppState::new();
        let n = state.examples.len();
        let i = (0..n)
            .find(|&i| {
                state.selected = i;
                draw(&mut state, 80, 24);
                state.panel_len > state.panel_rows
            })
            .expect("no explanation overflows at 80x24");
        let press = |state: &mut AppState, code: char| {
            state.handle_key(key(KeyCode::Char(code)));
            draw(state, 80, 24);
        };

        press(&mut state, 'l');
        press(&mut state, 'j');
        assert_eq!(state.panel_top, 1);
        assert_eq!(state.positions[i], [Some(1), None]);

        press(&mut state, 'c');
        assert!(state.view_code);
        let d = state.panel_top;
        assert_ne!(d, 1);
        press(&mut state, 'j');
        press(&mut state, 'j');
        assert_eq!(state.panel_top, d + 2);
        assert_eq!(state.positions[i], [Some(1), Some(d + 2)]);

        press(&mut state, 'c');
        assert!(!state.view_code);
        assert_eq!(state.panel_top, 1);
        press(&mut state, 'c');
        assert!(state.view_code);
        assert_eq!(state.panel_top, d + 2);

        // The same with the list highlighted.
        press(&mut state, 'h');
        press(&mut state, 'c');
        assert!(!state.view_code);
        assert_eq!(state.panel_top, 1);
        press(&mut state, 'c');
        assert!(state.view_code);
        assert_eq!(state.panel_top, d + 2);
    }

    /// AC-7, AC-8, AC-9: with the panel highlighted, `j` / `k` scroll it one
    /// row and stop at both ends, in the explanation and in the source.
    #[test]
    fn j_and_k_scroll_the_highlighted_panel_one_row_and_stop_at_both_ends() {
        let (j, k) = (key(KeyCode::Char('j')), key(KeyCode::Char('k')));
        let mut scrolled = [false, false];
        for i in 0..examples::all().len() {
            for view_code in [false, true] {
                let mut state = AppState::new();
                state.selected = i;
                state.view_code = view_code;
                state.focus = Pane::Panel;
                state.positions[i][usize::from(view_code)] = Some(0);
                draw(&mut state, 80, 24);
                let max = state.panel_len.saturating_sub(state.panel_rows);

                state.handle_key(k);
                assert_eq!(state.panel_top, 0);
                draw(&mut state, 80, 24);
                assert_eq!(state.panel_top, 0);

                if max == 0 {
                    state.handle_key(j);
                    assert_eq!(state.panel_top, 0);
                    draw(&mut state, 80, 24);
                    assert_eq!(state.panel_top, 0);
                    assert_eq!(state.selected, i);
                    continue;
                }
                scrolled[usize::from(view_code)] = true;

                state.handle_key(j);
                assert_eq!(state.panel_top, 1);
                let mut frame = draw(&mut state, 80, 24).join("\n");
                assert!(frame.contains(" rows 2-"), "{frame}");
                while state.panel_top < max {
                    let before = state.panel_top;
                    state.handle_key(j);
                    assert_eq!(state.panel_top, before + 1);
                    frame = draw(&mut state, 80, 24).join("\n");
                    assert_eq!(state.panel_top, before + 1);
                }
                let n = state.panel_len;
                assert!(frame.contains(&format!("-{n} of {n} ")), "{frame}");

                state.handle_key(j);
                assert_eq!(state.panel_top, max);
                draw(&mut state, 80, 24);
                assert_eq!(state.panel_top, max);
                state.handle_key(k);
                assert_eq!(state.panel_top, max - 1);
                assert_eq!(state.selected, i);
            }
        }
        assert_eq!(scrolled, [true, true]);
    }

    /// AC-10: `PageUp` / `PageDown` do nothing, whichever pane is highlighted.
    #[test]
    fn page_keys_are_not_navigation_keys() {
        fn press_both_and_expect_no_change(state: &mut AppState) {
            let before = (
                state.panel_top,
                state.selected,
                state.focus,
                state.view_code,
                state.positions.clone(),
            );
            for code in [KeyCode::PageDown, KeyCode::PageUp] {
                assert!(!state.handle_key(key(code)));
                draw(state, 80, 24);
                let after = (
                    state.panel_top,
                    state.selected,
                    state.focus,
                    state.view_code,
                    state.positions.clone(),
                );
                assert_eq!(after, before, "{code:?}");
            }
        }

        let mut state = AppState::new();
        state.view_code = true;
        draw(&mut state, 80, 24);
        // Strictly inside the range, so a page key working either way shows.
        assert!(state.panel_top > 0);
        assert!(state.panel_top < state.panel_len - state.panel_rows);

        assert_eq!(state.focus, Pane::List);
        press_both_and_expect_no_change(&mut state);
        state.handle_key(key(KeyCode::Char('l')));
        assert_eq!(state.focus, Pane::Panel);
        press_both_and_expect_no_change(&mut state);

        // ...nor once a stored position exists.
        state.handle_key(key(KeyCode::Char('j')));
        draw(&mut state, 80, 24);
        assert!(state.positions[0][1].is_some());
        press_both_and_expect_no_change(&mut state);
    }

    /// AC-13: run, `c`, the output keys and the quit keys do the same
    /// whichever pane is highlighted.
    #[test]
    fn other_keys_behave_the_same_whichever_pane_is_highlighted() {
        for focus in [Pane::List, Pane::Panel] {
            let mut state = AppState::new();
            state.pace = Duration::ZERO;
            if focus == Pane::Panel {
                state.handle_key(key(KeyCode::Char('l')));
            }
            draw(&mut state, 80, 24);
            let same_pane = |state: &AppState| {
                assert_eq!(state.focus, focus);
                assert_eq!(state.selected, 0);
            };
            same_pane(&state);

            state.handle_key(key(KeyCode::Char('c')));
            assert!(state.view_code);
            same_pane(&state);
            state.handle_key(key(KeyCode::Char('c')));
            assert!(!state.view_code);
            same_pane(&state);

            for run in [KeyCode::Enter, KeyCode::Char('r')] {
                state.handle_key(key(run));
                assert!(state.running);
                assert_eq!(state.run_name, state.examples[state.selected].name());
                finish_run(&mut state);
                assert_eq!(state.output.last().unwrap(), "[done]");
                same_pane(&state);
            }

            draw(&mut state, 80, 24);
            state.handle_key(key(KeyCode::Char('[')));
            assert_eq!(state.output_scroll, state.output_rows.saturating_sub(1).max(1));
            same_pane(&state);
            state.handle_key(key(KeyCode::Char(']')));
            assert_eq!(state.output_scroll, 0);
            same_pane(&state);

            for quit in [
                key(KeyCode::Char('q')),
                key(KeyCode::Esc),
                KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
            ] {
                state.cancel.store(false, Ordering::Relaxed);
                assert!(state.handle_key(quit));
                assert!(state.cancel.load(Ordering::Relaxed));
                same_pane(&state);
            }
        }
    }

    /// AC-20: moving the highlight and running an example change no
    /// example's position in either view.
    #[test]
    fn moving_the_highlight_and_running_change_no_position() {
        let mut state = AppState::new();
        state.pace = Duration::ZERO;
        draw(&mut state, 80, 24);
        let press = |state: &mut AppState, code: KeyCode| {
            state.handle_key(key(code));
            if matches!(code, KeyCode::Enter | KeyCode::Char('r')) {
                finish_run(state);
            }
            draw(state, 80, 24);
        };

        // A stored position in both views of example 0 (the explanation's
        // only if it overflows).
        press(&mut state, KeyCode::Char('l'));
        press(&mut state, KeyCode::Char('j'));
        press(&mut state, KeyCode::Char('c'));
        press(&mut state, KeyCode::Char('j'));
        assert!(state.positions[0][1].is_some());
        let top = state.panel_top;
        let positions = state.positions.clone();

        for code in [
            KeyCode::Char('h'),
            KeyCode::Char('l'),
            KeyCode::Left,
            KeyCode::Right,
            KeyCode::Enter,
            KeyCode::Char('r'),
        ] {
            press(&mut state, code);
            assert_eq!(state.positions, positions, "{code:?}");
            assert_eq!(state.panel_top, top, "{code:?}");
        }

        // Example 1, never scrolled: showing it at its default and running
        // it stores nothing.
        press(&mut state, KeyCode::Char('h'));
        press(&mut state, KeyCode::Char('j'));
        assert_eq!(state.selected, 1);
        for code in [KeyCode::Char('l'), KeyCode::Char('h'), KeyCode::Enter] {
            press(&mut state, code);
        }
        assert_eq!(state.positions, positions);
    }

    /// AC-21: positions last for one program run; a new `AppState` shows
    /// every example at its default in both views.
    #[test]
    fn every_position_is_back_at_its_default_in_a_new_run() {
        {
            let mut state = AppState::new();
            state.view_code = true;
            draw(&mut state, 80, 24);
            for code in ['l', 'j', 'j', 'h', 'j', 'l', 'j'] {
                state.handle_key(key(KeyCode::Char(code)));
                draw(&mut state, 80, 24);
            }
            let stored = state.positions.iter().flatten().filter(|p| p.is_some()).count();
            assert_eq!(stored, 2);
            assert!(state.positions[0][1].is_some() && state.positions[1][1].is_some());
        }

        let mut state = AppState::new();
        assert!(state.positions.iter().all(|pair| *pair == [None, None]));
        for i in 0..state.examples.len() {
            for view_code in [false, true] {
                state.selected = i;
                state.view_code = view_code;
                draw(&mut state, 80, 24);
                let default = if view_code {
                    let rows = wrap_code(state.examples[i].code(), state.panel_cols);
                    rows.iter()
                        .position(|r| r.trim_start() == SNIPPET_MARKER)
                        .unwrap()
                        + 1
                } else {
                    0
                };
                assert_eq!(state.panel_top, default, "example {i}, code {view_code}");
            }
        }
    }

    /// AC-2, AC-5: a file line's number is on its first row only, the texts
    /// are the plain wrap's, and the column is as wide as the line count.
    #[test]
    fn wrap_code_numbered_numbers_the_first_row_of_each_line() {
        let text = "    let x = 1;\n\nab";
        let rows = wrap_code_numbered(text, 8);
        assert_eq!(
            rows,
            [
                (Some(1), "    let ".to_string()),
                (None, "x = 1;".to_string()),
                (Some(2), String::new()),
                (Some(3), "ab".to_string()),
            ]
        );
        let texts: Vec<String> = rows.into_iter().map(|(_, row)| row).collect();
        assert_eq!(texts, wrap_code(text, 8));
        assert_eq!(wrap_code_numbered(text, 0), wrap_code_numbered(text, 1));
        assert!(wrap_code_numbered("", 8).is_empty());

        assert_eq!(line_number_digits(""), 1);
        assert_eq!(line_number_digits(&"x\n".repeat(9)), 1);
        assert_eq!(line_number_digits(&"x\n".repeat(10)), 2);
        assert_eq!(line_number_digits(&"x\n".repeat(99)), 2);
        assert_eq!(line_number_digits(&"x\n".repeat(100)), 3);
    }

    /// The `inner` characters of one screen row that sit just inside the
    /// right-hand panel's right border (the panel is the right-most pane).
    fn panel_text(row: &str, inner: usize) -> String {
        let chars: Vec<char> = row.chars().collect();
        let end = chars.len() - 1;
        chars[end - inner..end].iter().collect()
    }

    /// The rows the source view should show for `code`, built from its lines
    /// without the production wrap functions: each row's file line number
    /// (first row of a line only) and its text, number column included.
    fn expected_source_rows(
        code: &str,
        text_cols: usize,
        digits: usize,
    ) -> Vec<(Option<usize>, String)> {
        let mut rows = Vec::new();
        for (i, line) in code.lines().enumerate() {
            let n = i + 1;
            let chars: Vec<char> = line.chars().collect();
            if chars.is_empty() {
                rows.push((Some(n), format!("{n:>digits$} ")));
                continue;
            }
            for (k, chunk) in chars.chunks(text_cols.max(1)).enumerate() {
                let chunk: String = chunk.iter().collect();
                if k == 0 {
                    rows.push((Some(n), format!("{n:>digits$} {chunk}")));
                } else {
                    rows.push((None, format!("{}{chunk}", " ".repeat(digits + 1))));
                }
            }
        }
        rows
    }

    /// AC-1 to AC-6, AC-11: every visible source row is its file line's
    /// number, right-aligned in one column, a blank, then the code; at the
    /// default position, after scrolling by key, and at both ends of the file.
    #[test]
    fn the_source_view_numbers_every_line_with_its_file_line_number() {
        /// The check every draw must pass; returns the numbers on screen.
        fn check(
            state: &AppState,
            screen: &[String],
            expected: &[(Option<usize>, String)],
            digits: usize,
            inner: usize,
        ) -> Vec<usize> {
            assert_eq!(state.panel_len, expected.len());
            let visible = state.panel_rows.min(state.panel_len - state.panel_top);
            let mut seen = Vec::new();
            for k in 0..visible {
                let (number, text) = &expected[state.panel_top + k];
                let shown = panel_text(&screen[2 + k], inner);
                assert_eq!(shown, format!("{text:<inner$}"), "row {k}");
                assert_eq!(shown.chars().nth(digits), Some(' '), "row {k}");
                seen.extend(number);
            }
            assert!(!seen.is_empty(), "no number on screen");
            for pair in seen.windows(2) {
                assert_eq!(pair[1], pair[0] + 1, "{seen:?}");
            }
            seen
        }

        for i in 0..examples::all().len() {
            let mut state = AppState::new();
            state.selected = i;
            state.view_code = true;
            let code = state.examples[i].code();
            let count = code.lines().count();
            let digits = count.to_string().len();
            let mut seen = Vec::new();

            // Default position: the line under the marker, with its number.
            let screen = draw(&mut state, 80, 24);
            let inner = state.panel_cols + digits + 1;
            let expected = expected_source_rows(code, state.panel_cols, digits);
            assert_eq!(state.positions[i], [None, None]);
            let numbers = check(&state, &screen, &expected, digits, inner);
            let marker = code
                .lines()
                .position(|l| l.trim() == SNIPPET_MARKER)
                .unwrap();
            let first = panel_text(&screen[2], inner);
            assert_eq!(first[..digits].trim_start(), (marker + 2).to_string());
            assert_ne!(marker + 2, 1);
            let under: String = code
                .lines()
                .nth(marker + 1)
                .unwrap()
                .chars()
                .take(state.panel_cols)
                .collect();
            let shown_code: String = first.chars().skip(digits + 1).collect();
            assert_eq!(shown_code, format!("{under:<w$}", w = state.panel_cols));
            seen.extend(numbers);

            // Scrolled by key: the numbers move with the code.
            state.handle_key(key(KeyCode::Char('l')));
            let screen = draw(&mut state, 80, 24);
            seen.extend(check(&state, &screen, &expected, digits, inner));
            for (code_key, down) in [
                (KeyCode::Char('j'), true),
                (KeyCode::Char('j'), true),
                (KeyCode::Char('j'), true),
                (KeyCode::Char('k'), false),
                (KeyCode::Down, true),
                (KeyCode::Up, false),
            ] {
                let before = state.panel_top;
                state.handle_key(key(code_key));
                let screen = draw(&mut state, 80, 24);
                if down {
                    assert_eq!(state.panel_top, before + 1);
                } else {
                    assert_eq!(state.panel_top, before - 1);
                }
                seen.extend(check(&state, &screen, &expected, digits, inner));
            }

            // Top of the file: line 1.
            state.positions[i][1] = Some(0);
            let screen = draw(&mut state, 80, 24);
            assert_eq!(state.panel_top, 0);
            seen.extend(check(&state, &screen, &expected, digits, inner));
            let first = panel_text(&screen[2], inner);
            assert_eq!(first[..digits], format!("{:>digits$}", 1));

            // End of the file: its last line's number is the line count.
            state.positions[i][1] = Some(usize::MAX);
            let screen = draw(&mut state, 80, 24);
            assert_eq!(state.panel_top, state.panel_len - state.panel_rows);
            let numbers = check(&state, &screen, &expected, digits, inner);
            assert!(numbers.contains(&count), "{numbers:?}");
            seen.extend(numbers);

            // One column for numbers of one, two and three digits.
            for len in 1..=3 {
                assert!(
                    seen.iter().any(|n| n.to_string().len() == len),
                    "example {i}: no {len}-digit number seen"
                );
            }
        }
    }

    /// AC-7: the number column is dim, the code beside it is not.
    #[test]
    fn line_numbers_are_dim_and_the_code_is_not() {
        let mut state = AppState::new();
        state.view_code = true;
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        terminal
            .draw(|frame| ui(frame, &mut state).unwrap())
            .unwrap();
        let buffer = terminal.backend().buffer();
        let digits = state.examples[0].code().lines().count().to_string().len();
        let inner = state.panel_cols + digits + 1;
        let left = 80 - 1 - inner;
        let visible = state.panel_rows.min(state.panel_len - state.panel_top);
        assert!(visible > 0);
        for k in 0..visible {
            let y = (2 + k) as u16;
            for x in left..left + inner {
                let dim = buffer[(x as u16, y)].modifier.contains(Modifier::DIM);
                assert_eq!(dim, x < left + digits + 1, "cell ({x}, {y})");
            }
        }
    }

    /// AC-8: the explanation, the list, the picture and the output pane have
    /// no number column and are drawn as they were.
    #[test]
    fn only_the_source_view_has_a_line_number_column() {
        for i in 0..examples::all().len() {
            let mut state = AppState::new();
            state.selected = i;
            state.view_code = true;
            let source = draw(&mut state, 80, 24);
            let digits = state.examples[i].code().lines().count().to_string().len();
            let inner = state.panel_cols + digits + 1;

            state.handle_key(key(KeyCode::Char('c')));
            assert!(!state.view_code);
            let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
            terminal
                .draw(|frame| ui(frame, &mut state).unwrap())
                .unwrap();
            let buffer = terminal.backend().buffer();
            let explanation: Vec<String> = (0..24)
                .map(|y| (0..80).map(|x| buffer[(x, y)].symbol()).collect())
                .collect();

            // The explanation still fills the panel's whole inner width.
            assert_eq!(state.panel_cols, inner);
            let rows = wrap_words(state.examples[i].explanation(), inner);
            let visible = state.panel_rows.min(state.panel_len - state.panel_top);
            for k in 0..visible {
                assert_eq!(
                    panel_text(&explanation[2 + k], inner),
                    format!("{:<inner$}", rows[state.panel_top + k]),
                    "example {i} row {k}"
                );
            }
            let left = 80 - 1 - inner;
            for y in 2..2 + state.panel_rows {
                for x in left..left + inner {
                    assert!(
                        !buffer[(x as u16, y as u16)].modifier.contains(Modifier::DIM),
                        "example {i} cell ({x}, {y})"
                    );
                }
            }

            // The list, left of the panel's left border, is the same.
            for (s, e) in source.iter().zip(&explanation) {
                let s: String = s.chars().take(80 - 2 - inner).collect();
                let e: String = e.chars().take(80 - 2 - inner).collect();
                assert_eq!(s, e, "example {i}");
            }
            // So are the picture and the output pane, below the panel.
            let below = 2 + state.panel_rows + 1;
            assert!(below < 24);
            assert_eq!(source[below..], explanation[below..], "example {i}");
        }
    }
}
