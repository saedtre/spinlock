//! The TUI: pick a spinlock-consumer example, read its explanation (or its
//! source), and run it in-process on a background thread while watching its
//! output and a live picture of its traffic through the lock.
//!
//! Keys: `Up`/`k` and `Down`/`j` select, `Enter`/`r` run, `c` toggles the
//! code view, `PageUp`/`PageDown` scroll the right-hand panel, `[`/`]` scroll
//! the output pane back and forth, `q`/`Esc` quit.

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
use ratatui::widgets::{Block, List, ListItem, ListState, Paragraph};
use ratatui::{Frame, Terminal};

use spinlock::examples::{self, Example, ExampleOutput, LaneState, Viz};

/// How long the event loop waits for a key before redrawing / polling the run.
const TICK: Duration = Duration::from_millis(50);

/// One display pause unit handed to the examples: long enough that a run
/// lasts about a second and its picture can be followed.
const PACE: Duration = Duration::from_millis(4);

/// Key hints, most important first: a narrow terminal cuts off the tail.
const HINTS: &str = " q quit  Enter run  c code  j/k move  PgUp/PgDn scroll";

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

struct AppState {
    examples: Vec<Box<dyn Example>>,
    selected: usize,
    output: Vec<String>,
    running: bool,
    view_code: bool,
    scroll: usize,
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
    /// Text columns the right-hand panel had on the last draw.
    panel_cols: usize,
    /// How many (wrapped) rows the output pane is scrolled back from the
    /// newest line; 0 = follow the newest.
    output_scroll: usize,
    /// Text rows the output pane showed on the last draw (page size).
    output_rows: usize,
}

impl AppState {
    fn new() -> Self {
        Self {
            examples: examples::all(),
            selected: 0,
            output: vec!["press Enter or r to run the selected example".to_string()],
            running: false,
            view_code: false,
            scroll: 0,
            cancel: Arc::new(AtomicBool::new(false)),
            run: None,
            run_name: "",
            viz: None,
            pace: PACE,
            panel_rows: 1,
            panel_cols: 1,
            output_scroll: 0,
            output_rows: 1,
        }
    }

    fn select_previous(&mut self) {
        let n = self.examples.len();
        self.selected = (self.selected + n - 1) % n;
        self.scroll = 0;
    }

    fn select_next(&mut self) {
        self.selected = (self.selected + 1) % self.examples.len();
        self.scroll = 0;
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
        let page = self.panel_rows.saturating_sub(1).max(1);
        let output_page = self.output_rows.saturating_sub(1).max(1);
        match key.code {
            _ if ctrl_c => return self.quit(),
            KeyCode::Char('q') | KeyCode::Esc => return self.quit(),
            KeyCode::Up | KeyCode::Char('k') => self.select_previous(),
            KeyCode::Down | KeyCode::Char('j') => self.select_next(),
            KeyCode::Enter | KeyCode::Char('r') => run_example(self),
            KeyCode::Char('c') => {
                self.view_code = !self.view_code;
                self.scroll = 0;
            }
            KeyCode::PageUp => self.scroll = self.scroll.saturating_sub(page),
            // `ui` clamps this to the last page of the text.
            KeyCode::PageDown => self.scroll = self.scroll.saturating_add(page),
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

/// Hard wrap to `width` columns, keeping indentation (for source code).
fn wrap_code(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut rows = Vec::new();
    for line in text.lines() {
        let chars: Vec<char> = line.chars().collect();
        if chars.is_empty() {
            rows.push(String::new());
        }
        for chunk in chars.chunks(width) {
            rows.push(chunk.iter().collect());
        }
    }
    rows
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
        .block(Block::bordered().title("Examples"))
        .highlight_style(Style::new().add_modifier(Modifier::REVERSED))
        .highlight_symbol("> ");
    let mut list_state = ListState::default().with_selected(Some(state.selected));
    frame.render_stateful_widget(list, list_area, &mut list_state);

    // The panel text is wrapped here rather than by the widget, so the row
    // count is known and the scroll offset can stop exactly at the last page.
    let panel_block = Block::bordered();
    let inner = panel_block.inner(panel_area);
    let (cols, visible) = (inner.width as usize, inner.height as usize);
    let (panel_title, rows) = if state.view_code {
        ("Code (c: explanation)", wrap_code(example.code(), cols))
    } else {
        (
            "Why a spinlock here? (c: code)",
            wrap_words(example.explanation(), cols),
        )
    };
    state.panel_rows = visible;
    state.panel_cols = cols;
    state.scroll = state.scroll.min(rows.len().saturating_sub(visible));
    let last = (state.scroll + visible).min(rows.len());
    let mut panel_block = panel_block.title(panel_title);
    if rows.len() > visible {
        // Say that there is more, and how to reach it.
        panel_block = panel_block.title_bottom(
            Line::from(format!(
                " rows {}-{} of {} (PgUp/PgDn) ",
                state.scroll + 1,
                last,
                rows.len()
            ))
            .right_aligned(),
        );
    }
    let shown: Vec<Line> = rows[state.scroll..last]
        .iter()
        .map(|r| Line::raw(r.as_str()))
        .collect();
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
    fn c_toggles_code_view_and_resets_scroll() {
        let mut state = AppState::new();
        state.scroll = 3;
        state.handle_key(key(KeyCode::Char('c')));
        assert!(state.view_code);
        assert_eq!(state.scroll, 0);
        let screen = draw(&mut state, 100, 30).join("\n");
        assert!(screen.contains("Code (c: explanation)"));
        assert!(screen.contains("//! Example 1"));
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
    /// read to its last row with PageDown.
    #[test]
    fn at_80x24_hints_fit_and_every_explanation_scrolls_to_its_end() {
        let mut state = AppState::new();
        for i in 0..state.examples.len() {
            state.selected = i;
            state.scroll = 0;
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
                assert!(first.join("\n").contains("(PgUp/PgDn)"));
            }
            for _ in 0..rows.len() {
                state.handle_key(key(KeyCode::PageDown));
                draw(&mut state, 80, 24);
            }
            let end = draw(&mut state, 80, 24).join("\n");
            assert!(end.contains(rows.last().unwrap().as_str()), "example {i}:\n{end}");
            assert_eq!(state.scroll > 0, needs_scroll);
            // ...and back to the top again.
            for _ in 0..rows.len() {
                state.handle_key(key(KeyCode::PageUp));
            }
            assert_eq!(state.scroll, 0);
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
                state.scroll = usize::MAX;
                draw(&mut state, w, h);
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
}
