use super::{FOLDER, MARKER, PROGRESS_MARKER, SCRIPT};
use crate::interrupt::Interrupt;
use crate::progress::{Display, Event};
#[cfg(windows)]
use process_wrap::std::JobObject;
#[cfg(unix)]
use process_wrap::std::ProcessGroup;
use process_wrap::std::{CommandWrap, KillOnDrop};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(all(test, windows))]
mod windows_tests;

#[cfg(target_os = "macos")]
#[link(name = "proc")]
unsafe extern "C" {
    fn proc_listpgrppids(group: libc::pid_t, buffer: *mut std::ffi::c_void, size: libc::c_int) -> libc::c_int;
}

const CLEANUP_BUDGET: Duration = Duration::from_secs(1);
const TICK: Duration = Duration::from_millis(100);

#[derive(Debug, PartialEq)]
enum Message {
    Printed(String),
    Event(Event),
    Results(String),
}

pub(super) fn deadline(started: Instant, timeout: u64) -> Result<Option<Instant>, String> {
    if timeout == 0 {
        return Ok(None);
    }

    started
        .checked_add(Duration::from_secs(timeout))
        .map(Some)
        .ok_or_else(|| format!("The standalone timeout of {timeout} seconds is too large for this platform."))
}

fn poll_interval(deadline: Option<Instant>, now: Instant) -> Duration {
    match deadline {
        Some(deadline) => deadline.saturating_duration_since(now).min(TICK),
        None => TICK,
    }
}

fn collection_deadline(execution: Option<Instant>, now: Instant) -> Instant {
    let limit = now + Duration::from_secs(1);
    execution.map_or(limit, |execution| execution.min(limit))
}

fn record_report(accepted: &mut Option<String>, report: &str, deadline: Option<Instant>, now: Instant) {
    if accepted.is_none() && deadline.is_none_or(|deadline| now < deadline) {
        *accepted = Some(report.to_owned());
    }
}

fn report_deadline(execution: Option<Instant>, accepted: &Option<String>) -> Option<Instant> {
    execution.filter(|_| accepted.is_none())
}

struct OwnedChild {
    child: Option<Box<dyn process_wrap::std::ChildWrapper>>,
    stopped: Option<Result<ExitStatus, String>>,
}

impl OwnedChild {
    fn stop(&mut self, shown: &str) -> Result<ExitStatus, String> {
        if let Some(stopped) = &self.stopped {
            return stopped.clone();
        }
        let stopped = stop_owned(self.child.take().expect("The run still owns its child."), shown);
        self.stopped = Some(stopped.clone());
        stopped
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Some(child) = self.child.take() {
            let _ = stop_owned(child, "the standalone runtime");
        }
    }
}

struct Watchdog {
    finished: mpsc::Sender<()>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        let _ = self.finished.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub fn run(project_dir: &Path, run: &[String], specs: &[String], timeout: u64, interrupt: &Interrupt) -> Result<String, String> {
    let script = format!("{FOLDER}/{SCRIPT}");
    let shown = format!("{} {script}", run.join(" "));
    let deadline = deadline(Instant::now(), timeout)?;
    let budget = Duration::from_secs(timeout);

    let mut command = CommandWrap::with_new(&run[0], |command| {
        command.args(&run[1..]).arg(&script).current_dir(project_dir).stdin(Stdio::null()).stdout(Stdio::piped());
    });
    #[cfg(unix)]
    command.wrap(ProcessGroup::leader());
    #[cfg(windows)]
    command.wrap(JobObject);
    command.wrap(KillOnDrop);

    let _active = interrupt.arm();
    let mut child = command.spawn().map_err(|error| {
        format!("couldn't start `{shown}`, which begins with what litmus.toml gives as `run` under [standalone]: {error}")
    })?;
    let output = child.stdout().take().expect("The runtime stdout was piped.");
    let child = Arc::new(Mutex::new(OwnedChild { child: Some(child), stopped: None }));
    let owned = Arc::downgrade(&child);
    let interrupted_command = shown.clone();
    interrupt.on_signal(move |signal| {
        if let Some(owned) = owned.upgrade() {
            let mut owned = owned.lock().unwrap();
            let _ = owned.stop(&interrupted_command);
            // Hold ownership until termination so the output thread cannot race cleanup.
            crate::interrupt::terminate(signal);
        }
        crate::interrupt::terminate(signal);
    });

    let accepted = Arc::new(Mutex::new(None));
    let relayed_report = Arc::clone(&accepted);
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        relay(BufReader::new(output), |message| {
            if let Message::Results(report) = &message {
                // Sample under the same lock as expiry decisions, independently of display I/O.
                let mut accepted = relayed_report.lock().unwrap();
                record_report(&mut accepted, report, deadline, Instant::now());
            }
            sender.send(message).is_ok()
        });
    });
    let execution_wait = || {
        let accepted = accepted.lock().unwrap();
        poll_interval(report_deadline(deadline, &accepted), Instant::now())
    };

    let _watchdog = deadline.map(|deadline| {
        let (finished, completion) = mpsc::channel();
        let timed_child = Arc::downgrade(&child);
        let timed_command = shown.clone();
        let worker = thread::spawn(move || {
            if matches!(completion.recv_timeout(deadline.saturating_duration_since(Instant::now())), Err(RecvTimeoutError::Timeout))
                && let Some(child) = timed_child.upgrade()
            {
                // Cleanup cannot depend on the output consumer continuing to read stdout.
                let _ = child.lock().unwrap().stop(&timed_command);
            }
        });
        Watchdog { finished, worker: Some(worker) }
    });
    let mut display = Display::new(specs.to_vec());
    let mut output_closed = false;

    let reported = loop {
        if interrupt.cancelled() {
            break Ok(None);
        }
        let wait = execution_wait();
        if wait.is_zero() {
            break Err(format!("`{shown}` was stopped, still running after {}", in_words(budget)));
        }
        let message = if output_closed {
            thread::sleep(wait);
            None
        } else {
            Some(receiver.recv_timeout(wait))
        };
        if interrupt.cancelled() {
            break Ok(None);
        }
        if execution_wait().is_zero() {
            break Err(format!("`{shown}` was stopped, still running after {}", in_words(budget)));
        }
        match message {
            Some(Ok(Message::Printed(line))) => display.print(&line),
            Some(Ok(Message::Event(event))) => display.take(event),
            Some(Ok(Message::Results(_))) => break Ok(accepted.lock().unwrap().clone()),
            Some(Err(RecvTimeoutError::Disconnected)) => output_closed = true,
            Some(Err(RecvTimeoutError::Timeout)) | None => {}
        }

        if execution_wait().is_zero() {
            break Err(format!("`{shown}` was stopped, still running after {}", in_words(budget)));
        }

        // Poll the launcher, not the group or job. Descendants can keep stdout open.
        let launcher_status = {
            let mut owned = child.lock().unwrap();
            match owned.child.as_mut() {
                Some(child) => {
                    // SAFETY: Polling preserves the native child and ownership handles.
                    let launcher = unsafe { child.try_inner_child_mut() }.expect("The command has a native child.");
                    launcher_exited(launcher)
                }
                None => Ok(true),
            }
        };
        if execution_wait().is_zero() {
            break Err(format!("`{shown}` was stopped, still running after {}", in_words(budget)));
        }
        match launcher_status {
            Ok(true) => break Ok(None),
            Ok(false) => {}
            Err(error) => break Err(format!("couldn't tell how `{shown}` ended: {error}")),
        }

        display.refresh();
    };

    display.finish();

    let stopped = {
        let mut owned = child.lock().unwrap();
        owned.stop(&shown)
    };
    finalize_report(
        &shown,
        reported,
        stopped,
        || {
            let collecting = Instant::now();
            let collection_wait = || {
                let accepted = accepted.lock().unwrap();
                let drain_deadline = collection_deadline(report_deadline(deadline, &accepted), collecting);
                poll_interval(Some(drain_deadline), Instant::now())
            };
            // Timely completion lifts execution expiry, not the one-second collection bound.
            let _ = collect_and_replay(
                &receiver,
                collection_wait,
                || interrupt.cancelled(),
                |message| match message {
                    Message::Printed(line) => display.print(&line),
                    Message::Event(event) => display.take(event),
                    Message::Results(_) => unreachable!("Collected results are returned, not forwarded."),
                },
            );
            display.finish();
            accepted.lock().unwrap().clone()
        },
        || interrupt.cancelled(),
        |warning| eprintln!("litmus: {warning}"),
    )
}

fn launcher_exited(child: &mut std::process::Child) -> std::io::Result<bool> {
    #[cfg(unix)]
    {
        // Keep an exited group leader unreaped so its numeric PGID cannot be reused.
        let mut info = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
        let result = unsafe { libc::waitid(libc::P_PID, child.id(), &mut info, libc::WEXITED | libc::WNOHANG | libc::WNOWAIT) };
        if result == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(unsafe { info.si_pid() } != 0)
        }
    }
    #[cfg(windows)]
    child.try_wait().map(|status| status.is_some())
}

#[cfg(target_os = "macos")]
fn only_zombie_launcher(launcher: &mut std::process::Child) -> bool {
    if !matches!(launcher_exited(launcher), Ok(true)) {
        return false;
    }
    // Two slots distinguish the reserved leader from any other group member. Unknown
    // or truncated observations must not suppress a genuine permission failure.
    let mut members: [libc::pid_t; 2] = [0; 2];
    // SAFETY: The buffer is writable and sized in bytes; the leader remains unreaped.
    let count = unsafe {
        proc_listpgrppids(launcher.id() as libc::pid_t, members.as_mut_ptr().cast(), std::mem::size_of_val(&members) as libc::c_int)
    };
    count == 1 && members[0] == launcher.id() as libc::pid_t
}

fn collect_and_replay(
    receiver: &Receiver<Message>,
    mut wait: impl FnMut() -> Duration,
    mut cancelled: impl FnMut() -> bool,
    mut forward: impl FnMut(Message),
) -> Option<String> {
    // Collect before replaying logs so output backpressure cannot hide queued results.
    let mut pending = Vec::new();
    while !cancelled() {
        let duration = wait();
        if duration.is_zero() {
            break;
        }
        let message = receiver.recv_timeout(duration);
        if wait().is_zero() {
            break;
        }
        match message {
            Ok(message) => {
                let last = matches!(message, Message::Results(_));
                pending.push(message);
                if last {
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    for message in pending {
        match message {
            Message::Results(results) => return Some(results),
            message => forward(message),
        }
    }
    None
}

fn stop_owned(mut child: Box<dyn process_wrap::std::ChildWrapper>, shown: &str) -> Result<ExitStatus, String> {
    let stopped = stop(&mut child, shown);
    // The bounded cleanup already attempted both owners. Never enter the wrapper's
    // unbounded kill-and-wait again, or signal a PGID after reaping its launcher.
    drop(child.into_inner());
    stopped
}

// Stops the owned process group or job, then reaps the launcher.
fn stop(child: &mut Box<dyn process_wrap::std::ChildWrapper>, shown: &str) -> Result<ExitStatus, String> {
    #[cfg(unix)]
    let stop_deadline = Instant::now() + CLEANUP_BUDGET;
    #[cfg(unix)]
    let stopped = loop {
        match child.start_kill() {
            #[cfg(target_os = "macos")]
            Err(error) if error.raw_os_error() == Some(libc::EPERM) && {
                // macOS rejects signals to a zombie-only group. Its reserved leader
                // and an authoritative membership query distinguish that from denial.
                let launcher = unsafe { child.try_inner_child_mut() }.expect("The command has a native child.");
                only_zombie_launcher(launcher)
            } => break Ok(()),
            Err(error) if error.raw_os_error() == Some(libc::EPERM) && Instant::now() < stop_deadline => {
                // Keep the leader reserved through retries, even if macOS rejects the
                // signal while it exits. Persistent permission errors remain visible.
                thread::sleep(TICK);
            }
            result => break result,
        }
    };
    #[cfg(windows)]
    let stopped = child.start_kill();
    let tree_problem = match stopped {
        Ok(()) => None,
        #[cfg(unix)]
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => None,
        Err(error) => Some(format!("couldn't stop the process tree for `{shown}`, so it may still be running: {error}")),
    };

    // A launcher can leave its original group. Its native child remains ours, so
    // terminate it independently before any wait can release its numeric identity.
    let native_error = {
        // SAFETY: The wrapper keeps this native child and all ownership handles alive.
        let launcher = unsafe { child.try_inner_child_mut() }.expect("The command has a native child.");
        launcher.kill().err()
    };
    let status = wait_for_exit(
        || {
            #[cfg(unix)]
            {
                // Group polling can miss a launcher that moved groups. No more group
                // signals follow this native reap, so PID reuse is harmless here.
                let launcher = unsafe { child.try_inner_child_mut() }.expect("The command has a native child.");
                launcher.try_wait()
            }
            #[cfg(windows)]
            child.try_wait()
        },
        Instant::now() + CLEANUP_BUDGET,
    )
    .map_err(|error| match native_error {
        Some(kill) => format!("couldn't stop or reap the launcher for `{shown}`, so it may still be running ({kill}; {error})"),
        None => format!("couldn't reap the launcher for `{shown}` before cleanup ended, so it may still be running: {error}"),
    });
    match (tree_problem, status) {
        (Some(problem), Err(wait)) => Err(format!("{problem}, and {wait}")),
        (Some(problem), Ok(_)) => Err(problem),
        (None, status) => status,
    }
}

fn wait_for_exit(
    mut poll: impl FnMut() -> std::io::Result<Option<ExitStatus>>,
    deadline: Instant,
) -> std::io::Result<ExitStatus> {
    loop {
        if let Some(status) = poll()? {
            return Ok(status);
        }
        let wait = deadline.saturating_duration_since(Instant::now()).min(TICK);
        if wait.is_zero() {
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "Cleanup could not confirm process termination."));
        }
        thread::sleep(wait);
    }
}

fn finalize_report(
    shown: &str,
    reported: Result<Option<String>, String>,
    stopped: Result<ExitStatus, String>,
    collect: impl FnOnce() -> Option<String>,
    cancelled: impl FnOnce() -> bool,
    emit_warning: impl FnOnce(&str),
) -> Result<String, String> {
    let mut reported = collect_report(reported, collect);
    if cancelled() {
        reported = Err("the standalone run was interrupted".to_owned());
    }
    let (outcome, warning) = outcome(shown, reported, stopped);
    if let Some(warning) = warning {
        emit_warning(&warning);
    }
    outcome
}

fn collect_report(reported: Result<Option<String>, String>, collect: impl FnOnce() -> Option<String>) -> Result<Option<String>, String> {
    reported.map(|report| report.or(collect()))
}

// What the run comes to. Results the runtime reported still count when stopping it then
// failed, with a warning, since the tests themselves finished.
fn outcome(
    shown: &str,
    reported: Result<Option<String>, String>,
    stopped: Result<ExitStatus, String>,
) -> (Result<String, String>, Option<String>) {
    match (reported, stopped) {
        (Ok(Some(results)), Ok(_)) => (Ok(results), None),
        (Ok(Some(results)), Err(problem)) => (Ok(results), Some(problem)),
        (Ok(None), Ok(status)) => (Err(format!("`{shown}` ended without reporting any results ({status})")), None),
        (Ok(None), Err(problem)) => (Err(format!("`{shown}` ended without reporting any results, and {problem}")), None),
        (Err(timeout), Ok(_)) => (Err(timeout), None),
        (Err(timeout), Err(problem)) => (Err(format!("{timeout}, and {problem}")), None),
    }
}

fn in_words(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let (count, unit) = if seconds >= 60 && seconds.is_multiple_of(60) { (seconds / 60, "minute") } else { (seconds, "second") };

    format!("{count} {unit}{}", if count == 1 { "" } else { "s" })
}

fn relay(mut output: impl BufRead, mut pass: impl FnMut(Message) -> bool) {
    let mut line = Vec::new();

    while output.read_until(b'\n', &mut line).is_ok_and(|read| read > 0) {
        let text = String::from_utf8_lossy(&line);
        let message = read(text.trim_end_matches(['\n', '\r']));
        let last = matches!(message, Some(Message::Results(_)));

        if message.is_some_and(|message| !pass(message)) || last {
            return;
        }

        line.clear();
    }
}

fn read(text: &str) -> Option<Message> {
    if let Some(results) = text.strip_prefix(MARKER) {
        return Some(Message::Results(results.to_owned()));
    }

    match text.strip_prefix(PROGRESS_MARKER) {
        Some(event) => serde_json::from_str(event).ok().map(Message::Event),
        None => Some(Message::Printed(text.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::TempProject;
    use std::fs;
    use std::io::{Cursor, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::process::Command;

    fn relayed(output: &str) -> Vec<Message> {
        let mut passed = Vec::new();

        relay(Cursor::new(output), |message| {
            passed.push(message);
            true
        });

        passed
    }

    fn printed(line: &str) -> Message {
        Message::Printed(line.to_owned())
    }

    #[test]
    fn passes_lines_on_until_the_results_and_reads_no_further() {
        let passed = relayed("first\r\nsecond\n@@litmus-run@@{\"specs\":[]}\nlast");

        assert_eq!(passed, [printed("first"), printed("second"), Message::Results("{\"specs\":[]}".to_owned())]);
    }

    #[test]
    fn tells_a_line_of_progress_from_one_a_spec_printed() {
        let passed = relayed("@@litmus-progress@@{\"kind\":\"settled\"}\nprinted\n");

        assert_eq!(passed, [Message::Event(Event::Settled), printed("printed")]);
    }

    #[test]
    fn drops_a_line_of_progress_it_cannot_read() {
        assert_eq!(relayed("@@litmus-progress@@{\"kind\":\"unheard-of\"}\nprinted\n"), [printed("printed")]);
    }

    #[test]
    fn a_run_that_printed_no_marked_line_reported_nothing() {
        assert_eq!(relayed("only a print\n"), [printed("only a print")]);
        assert_eq!(relayed(""), []);
    }

    #[test]
    fn stops_reading_once_nobody_takes_what_it_passes_on() {
        let mut passed = 0;

        relay(Cursor::new("first\nsecond\n"), |_| {
            passed += 1;
            false
        });

        assert_eq!(passed, 1);
    }

    #[test]
    fn collects_through_results_before_forwarding_any_queued_output() {
        let (sender, receiver) = mpsc::channel();
        for message in [
            printed("first"),
            Message::Event(Event::Loading { spec: 1 }),
            printed("second"),
            printed("second"),
            Message::Event(Event::Settled),
            Message::Results("{\"specs\":[]}".to_owned()),
        ] {
            sender.send(message).unwrap();
        }
        let mut forwarded = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(60);

        let results = collect_and_replay(
            &receiver,
            || poll_interval(Some(deadline), Instant::now()),
            || false,
            |message| {
                // Even the first callback must see that collection reached the results.
                assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty));
                forwarded.push(message);
            },
        );

        assert_eq!(results.as_deref(), Some("{\"specs\":[]}"));
        assert_eq!(
            forwarded,
            [printed("first"), Message::Event(Event::Loading { spec: 1 }), printed("second"), printed("second"), Message::Event(Event::Settled)]
        );
    }

    #[test]
    fn returns_the_first_results_without_consuming_later_queued_messages() {
        let (sender, receiver) = mpsc::channel();
        for message in [
            printed("before"),
            Message::Results("first results".to_owned()),
            printed("after"),
            Message::Event(Event::Settled),
            Message::Results("later results".to_owned()),
        ] {
            sender.send(message).unwrap();
        }
        let mut forwarded = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(60);

        let results = collect_and_replay(
            &receiver,
            || poll_interval(Some(deadline), Instant::now()),
            || false,
            |message| forwarded.push(message),
        );

        assert_eq!(results.as_deref(), Some("first results"));
        assert_eq!(forwarded, [printed("before")]);
        assert_eq!(
            receiver.try_iter().collect::<Vec<_>>(),
            [printed("after"), Message::Event(Event::Settled), Message::Results("later results".to_owned())]
        );
    }

    #[test]
    fn replays_collected_output_when_the_channel_disconnects_without_results() {
        let (sender, receiver) = mpsc::channel();
        sender.send(printed("first")).unwrap();
        sender.send(Message::Event(Event::Settled)).unwrap();
        drop(sender);
        let mut forwarded = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(60);

        let results = collect_and_replay(
            &receiver,
            || poll_interval(Some(deadline), Instant::now()),
            || false,
            |message| forwarded.push(message),
        );

        assert_eq!(results, None);
        assert_eq!(forwarded, [printed("first"), Message::Event(Event::Settled)]);
        assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Disconnected));
    }

    #[test]
    fn an_expired_collection_deadline_leaves_queued_results_untouched() {
        let (sender, receiver) = mpsc::channel();
        sender.send(Message::Results("queued results".to_owned())).unwrap();

        let results = collect_and_replay(
            &receiver,
            || Duration::ZERO,
            || false,
            |_| panic!("Expired collection must not forward output."),
        );

        assert_eq!(results, None);
        assert_eq!(receiver.try_recv(), Ok(Message::Results("queued results".to_owned())));
    }

    #[test]
    fn rejects_results_received_when_the_collection_deadline_expires() {
        let (sender, receiver) = mpsc::channel();
        sender.send(Message::Results("late results".to_owned())).unwrap();
        let mut before_receive = true;

        let report = collect_and_replay(
            &receiver,
            || {
                if before_receive {
                    before_receive = false;
                    TICK
                } else {
                    Duration::ZERO
                }
            },
            || false,
            |_| panic!("Expired collection must not forward output."),
        );

        assert_eq!(report, None);
        assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty));
    }

    #[test]
    fn cancellation_stops_collection_but_replays_already_collected_output() {
        for collect_count in [0, 2] {
            let (sender, receiver) = mpsc::channel();
            sender.send(printed("first")).unwrap();
            sender.send(Message::Event(Event::Settled)).unwrap();
            sender.send(Message::Results("queued results".to_owned())).unwrap();
            let mut checked = 0;
            let mut forwarded = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(60);

            let results = collect_and_replay(
                &receiver,
                || poll_interval(Some(deadline), Instant::now()),
                || {
                    checked += 1;
                    checked > collect_count
                },
                |message| forwarded.push(message),
            );

            assert_eq!(results, None);
            if collect_count == 0 {
                assert!(forwarded.is_empty());
                assert_eq!(receiver.try_recv(), Ok(printed("first")));
                assert_eq!(receiver.try_recv(), Ok(Message::Event(Event::Settled)));
            } else {
                assert_eq!(forwarded, [printed("first"), Message::Event(Event::Settled)]);
            }
            assert_eq!(receiver.try_recv(), Ok(Message::Results("queued results".to_owned())));
            assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty));
        }
    }

    #[test]
    fn retains_results_received_during_final_collection() {
        let (sender, receiver) = mpsc::channel();
        sender.send(Message::Results("{\"specs\":[]}".to_owned())).unwrap();
        drop(sender);
        let now = Instant::now();

        let report = collect_and_replay(
            &receiver,
            || poll_interval(Some(now + Duration::from_secs(1)), now),
            || false,
            |_| panic!("Results must be returned, not forwarded."),
        );

        assert_eq!(report.as_deref(), Some("{\"specs\":[]}"));
    }

    #[test]
    fn elapsed_deadline_stops_final_collection_with_messages_always_queued() {
        for initially_elapsed in [false, true] {
            let (sender, receiver) = mpsc::channel();
            sender.send(printed("first")).unwrap();
            sender.send(printed("second")).unwrap();
            sender.send(Message::Results("too late".to_owned())).unwrap();
            drop(sender);
            let start = Instant::now();
            let deadline = start + Duration::from_secs(1);
            let mut checked = 0;
            let mut forwarded = Vec::new();

            let report = collect_and_replay(
                &receiver,
                || {
                    checked += 1;
                    // The first receive needs both its pre-receive and post-receive checks.
                    let now = if !initially_elapsed && checked <= 2 { start } else { deadline };
                    poll_interval(Some(deadline), now)
                },
                || false,
                |message| forwarded.push(message),
            );

            assert_eq!(report, None);
            if initially_elapsed {
                assert!(forwarded.is_empty());
                assert_eq!(receiver.try_recv().unwrap(), printed("first"));
            } else {
                assert_eq!(forwarded, [printed("first")]);
            }
            assert_eq!(receiver.try_recv().unwrap(), printed("second"));
            assert_eq!(receiver.try_recv().unwrap(), Message::Results("too late".to_owned()));
        }
    }

    #[test]
    fn final_collection_with_an_open_idle_sender_finishes_at_its_deadline() {
        let (sender, receiver) = mpsc::channel();
        let (finished, completion) = mpsc::channel();
        let collector = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_millis(20);
            let report = collect_and_replay(
                &receiver,
                || poll_interval(Some(deadline), Instant::now()),
                || false,
                |_| panic!("The open sender has no data."),
            );
            finished.send((report, Instant::now() >= deadline)).unwrap();
        });

        let (report, elapsed) = completion.recv_timeout(Duration::from_secs(5)).expect("The idle collector did not finish.");
        // Keep the sender open until collection completes, rather than testing disconnection.
        drop(sender);
        collector.join().unwrap();
        assert_eq!(report, None);
        assert!(elapsed, "Collection must wait for the future deadline.");
    }

    fn fixture_command(name: &str) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", name, "--ignored", "--nocapture"]);
        #[cfg(unix)]
        unsafe {
            use std::os::unix::process::CommandExt;
            // Signal and broken-pipe regressions must not leave core files behind.
            command.pre_exec(|| {
                let limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
                if libc::setrlimit(libc::RLIMIT_CORE, &limit) == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
            });
        }
        command
    }

    #[test]
    #[ignore = "invoked as a subprocess by process cleanup tests"]
    #[allow(clippy::zombie_processes, reason = "This launcher must exit before its descendant to exercise cleanup.")]
    fn wrapper_fixture() {
        #[cfg(windows)]
        super::windows_tests::ignore_ctrl_c(true);
        let mode = fs::read_to_string("mode").unwrap();
        if matches!(mode.as_str(), "natural-backlog-results" | "timely-backlog-results") {
            descendant_fixture();
            return;
        }
        #[cfg(unix)]
        if matches!(mode.as_str(), "moved-launcher" | "moved-launcher-broken-pipe") {
            let group = fs::read_to_string("parent-group").unwrap().parse::<libc::pid_t>().unwrap();
            assert_eq!(unsafe { libc::setpgid(0, group) }, 0);
            descendant_fixture();
            return;
        }
        let mut child = fixture_command("standalone::process::tests::descendant_fixture").spawn().unwrap();
        if matches!(mode.as_str(), "backlog-results" | "escaped" | "missing" | "exited-results") {
            let deadline = Instant::now() + Duration::from_secs(5);
            let ready = if mode == "backlog-results" { "reported" } else { "ready" };
            while !Path::new(ready).exists() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            if mode == "exited-results" {
                println!("\n{MARKER}{{\"specs\":[]}}");
                std::io::stdout().flush().unwrap();
            }
        } else {
            child.wait().unwrap();
        }
    }

    #[test]
    #[ignore = "invoked as a subprocess by process cleanup tests"]
    fn descendant_fixture() {
        let address = fs::read_to_string("address").unwrap();
        let mode = fs::read_to_string("mode").unwrap();
        #[cfg(unix)]
        if mode == "escaped" {
            assert_ne!(unsafe { libc::setsid() }, -1);
        }
        let mut connection = TcpStream::connect(address).unwrap();
        connection.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        connection.write_all(&[1]).unwrap();
        let mut acknowledged = [0];
        connection.read_exact(&mut acknowledged).unwrap();
        assert_eq!(acknowledged, [1]);
        fs::write("ready", "").unwrap();
        if mode == "interrupt-results" {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !Path::new("report").exists() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
        }
        if mode == "delayed-results" {
            thread::sleep(Duration::from_millis(1200));
        }
        if matches!(mode.as_str(), "results" | "inactive" | "interrupt-results" | "delayed-results") {
            println!("\n{MARKER}{{\"specs\":[]}}");
            std::io::stdout().flush().unwrap();
        }
        let prints = match mode.as_str() {
            "backlog-results" | "interrupt-output" | "natural-backlog-results" | "timely-backlog-results" => 10_000,
            "blocked-output" | "timeout-output" => 1_000_000,
            "broken-pipe" | "moved-launcher-broken-pipe" | "retained-reader" => 1,
            _ => 0,
        };
        for _ in 0..prints {
            println!("The runtime is still running.");
        }
        if mode == "timely-backlog-results" {
            // The controller releases the report only after downstream stdout stalls.
            connection.read_exact(&mut acknowledged).unwrap();
            assert_eq!(acknowledged, [2]);
        }
        if matches!(mode.as_str(), "backlog-results" | "natural-backlog-results" | "timely-backlog-results") {
            println!("\n{MARKER}{{\"specs\":[]}}");
            std::io::stdout().flush().unwrap();
            fs::write("reported", "").unwrap();
        }
        if mode == "timely-backlog-results" {
            connection.write_all(&[3]).unwrap();
            // Process-driven socket disconnection witnesses exit, not a dropped stream.
            std::process::exit(0);
        }
        if mode == "retained-reader" {
            let mut repeat = [0];
            connection.read_exact(&mut repeat).unwrap();
            assert_eq!(repeat, [2]);
            println!("The runtime is still running.");
            std::io::stdout().flush().unwrap();
        }
        if mode != "natural-backlog-results" {
            let _ = connection.read(&mut [0]);
        }
    }

    fn wrapper_run() -> Vec<String> {
        vec![
            std::env::current_exe().unwrap().to_str().unwrap().to_owned(),
            "--exact".to_owned(),
            "standalone::process::tests::wrapper_fixture".to_owned(),
            "--ignored".to_owned(),
            "--nocapture".to_owned(),
            "--skip".to_owned(),
        ]
    }

    fn cleaned_up(mode: &str) -> Result<String, String> {
        cleaned_up_with_timeout(mode, 5)
    }

    fn cleaned_up_with_timeout(mode: &str, timeout: u64) -> Result<String, String> {
        let project = TempProject::new(&format!("process-cleanup-{mode}"));
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        project.write("address", &listener.local_addr().unwrap().to_string());
        project.write("mode", mode);
        #[cfg(unix)]
        if mode == "moved-launcher" {
            project.write("parent-group", &unsafe { libc::getpgrp() }.to_string());
        }
        let command = wrapper_run();
        thread::scope(|scope| {
            let (finished, completion) = mpsc::sync_channel(1);
            let project = &project;
            let command = &command;
            let runner = scope.spawn(move || {
                let result = run(project, command, &[], timeout, &Interrupt::default());
                let _ = finished.send(result);
            });
            let deadline = Instant::now() + Duration::from_secs(3);
            let mut connection = loop {
                match listener.accept() {
                    Ok((connection, _)) => break connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => {
                        let result = completion.recv_timeout(Duration::from_secs(8));
                        panic!("The descendant did not connect ({error}). Runner returned {result:?}.");
                    }
                }
            };
            connection.set_nonblocking(false).unwrap();
            connection.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
            let mut ready = [0];
            connection.read_exact(&mut ready).unwrap();
            assert_eq!(ready, [1]);
            connection.write_all(&[1]).unwrap();
            let result = completion.recv_timeout(Duration::from_secs(8));
            if result.is_err() {
                let _ = connection.write_all(&[0]);
            }
            runner.join().unwrap();
            let result = result.expect("The runner did not finish before the fixture could expire.");
            let read = connection.read(&mut [0]);
            let closed = match &read {
                Ok(0) => true,
                Err(error) => matches!(error.kind(), std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::ConnectionReset),
                _ => false,
            };
            // Release a surviving fixture before asserting, so a regression does not leak it.
            if !closed {
                let _ = connection.write_all(&[0]);
            }
            if mode == "escaped" {
                assert!(!closed, "The escaped fixture must retain the inherited output pipe.");
            } else {
                assert!(closed, "The runtime descendant survived the {mode} exit ({read:?}). Runner returned {result:?}.");
            }
            result
        })
    }

    #[test]
    fn cleanup_waits_return_a_status_or_a_bounded_error() {
        let deadline = Instant::now();
        assert!(wait_for_exit(|| Ok(Some(exited())), deadline).unwrap().success());
        let pending = wait_for_exit(|| Ok(None), deadline).unwrap_err();
        assert_eq!(pending.kind(), std::io::ErrorKind::TimedOut);
        let refused = wait_for_exit(|| Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied)), deadline).unwrap_err();
        assert_eq!(refused.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn cleanup_waits_poll_until_a_pending_launcher_exits() {
        let expected = exited();
        let mut observations = [None, None, Some(expected)].into_iter();
        let guard = Instant::now() + Duration::from_secs(5);
        let status = wait_for_exit(
            || {
                assert!(Instant::now() < guard, "Polling did not finish before the bounded guard.");
                Ok(observations.next().expect("Polling continued after the launcher exited."))
            },
            guard,
        )
        .unwrap();

        assert_eq!(status, expected);
        assert_eq!(observations.next(), None, "The launcher exit must be observed after the pending polls.");
    }

    #[test]
    fn cleanup_waits_propagate_an_error_after_pending_polls() {
        let expected = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "The delayed poll failed.");
        let mut observations = [Ok(None), Ok(None), Err(expected)].into_iter();
        let guard = Instant::now() + Duration::from_secs(5);
        let problem = wait_for_exit(
            || {
                assert!(Instant::now() < guard, "Polling did not finish before the bounded guard.");
                observations.next().expect("Polling continued after the launcher observation failed.")
            },
            guard,
        )
        .unwrap_err();

        assert_eq!(problem.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(problem.to_string(), "The delayed poll failed.");
        assert!(observations.next().is_none(), "The delayed error must be observed after the pending polls.");
    }

    #[test]
    fn cleanup_waits_time_out_when_a_launcher_remains_pending_until_a_future_deadline() {
        let started = Instant::now();
        let deadline = started + TICK * 2;
        let guard = started + Duration::from_secs(5);
        let problem = wait_for_exit(
            || {
                assert!(Instant::now() < guard, "Polling ignored the cleanup deadline.");
                Ok(None)
            },
            deadline,
        )
        .unwrap_err();

        assert_eq!(problem.kind(), std::io::ErrorKind::TimedOut);
        let finished = Instant::now();
        assert!(finished >= deadline, "Cleanup timed out before the future deadline.");
        assert!(finished < guard, "Cleanup exceeded the bounded guard.");
    }

    #[cfg(unix)]
    fn with_native_child(mut child: std::process::Child, test: impl FnOnce(&mut std::process::Child)) {
        use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

        let outcome = catch_unwind(AssertUnwindSafe(|| test(&mut child)));
        let reaped = match child.try_wait() {
            Ok(Some(status)) => Ok(status),
            Ok(None) => {
                let _ = child.kill();
                wait_for_exit(|| child.try_wait(), Instant::now() + Duration::from_secs(5))
            }
            // If a regression reaped the child early, its PID is no longer ours to kill.
            Err(error) => Err(error),
        };
        if let Err(problem) = outcome {
            resume_unwind(problem);
        }
        reaped.expect("The native child fixture could not be reaped.");
    }

    #[cfg(unix)]
    #[test]
    fn launcher_polling_keeps_its_process_group_identifier_reserved_until_cleanup() {
        use std::os::unix::process::CommandExt;

        let child = Command::new("sh").args(["-c", "exit 0"]).process_group(0).spawn().unwrap();
        with_native_child(child, |child| {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !launcher_exited(child).unwrap() {
                assert!(Instant::now() < deadline, "The launcher did not exit.");
                thread::sleep(Duration::from_millis(10));
            }
            let mut info = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
            // Bypass Child's cached status to prove that polling left the PID unreaped.
            // WNOWAIT also leaves with_native_child responsible for the final reap.
            let result = unsafe { libc::waitid(libc::P_PID, child.id(), &mut info, libc::WEXITED | libc::WNOHANG | libc::WNOWAIT) };
            assert_eq!(result, 0, "The unreaped launcher could not be observed ({}).", std::io::Error::last_os_error());
            assert_eq!(unsafe { info.si_pid() }, child.id() as libc::pid_t, "Polling must leave the exited launcher waitable.");
            #[cfg(target_os = "macos")]
            assert!(only_zombie_launcher(child), "The reserved zombie must not be mistaken for a live process tree.");
            assert!(child.try_wait().unwrap().expect("Polling reaped the process-group leader early.").success());
        });
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_live_launcher_is_not_a_zombie_only_group() {
        use std::os::unix::process::CommandExt;

        let child = Command::new("sh").args(["-c", "exec sleep 60"]).process_group(0).spawn().unwrap();
        with_native_child(child, |child| assert!(!only_zombie_launcher(child)));
    }

    #[test]
    fn owned_cleanup_caches_its_first_real_stop_and_releases_the_child() {
        // The native harness help command is self-contained, spawns no descendants,
        // and exits on its own even if cleanup regresses.
        let mut command = CommandWrap::with_new(std::env::current_exe().unwrap(), |command| {
            command.arg("--help").stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        });
        #[cfg(unix)]
        command.wrap(ProcessGroup::leader());
        #[cfg(windows)]
        command.wrap(JobObject);
        command.wrap(KillOnDrop);
        let mut owned = OwnedChild { child: Some(command.spawn().unwrap()), stopped: None };

        let first = owned.stop("the owned native fixture");

        assert!(first.is_ok(), "The first real cleanup failed ({first:?}).");
        assert_eq!(owned.stopped.as_ref(), Some(&first), "The first cleanup outcome must be cached.");
        assert!(owned.child.is_none(), "The first cleanup must take ownership of the child.");
        assert_eq!(owned.stop("the fixture without a child"), first);
    }

    #[test]
    fn owned_cleanup_retains_its_first_outcome() {
        for stopped in [Ok(exited()), Err("The cleanup attempt failed.".to_owned())] {
            let mut owned = OwnedChild { child: None, stopped: Some(stopped.clone()) };
            assert_eq!(owned.stop("the already stopped fixture"), stopped);
            assert_eq!(owned.stop("the same fixture again"), stopped);
        }
    }

    #[test]
    fn reported_results_stop_the_wrapper_and_runtime() {
        assert_eq!(cleaned_up("results").unwrap(), "{\"specs\":[]}");
    }

    #[test]
    fn disabled_deadlines_allow_delayed_results_and_still_clean_up_the_runtime() {
        let started = Instant::now();
        assert_eq!(cleaned_up_with_timeout("delayed-results", 0).unwrap(), "{\"specs\":[]}");
        assert!(started.elapsed() >= Duration::from_millis(1200));
    }

    #[test]
    fn keeps_results_printed_just_before_the_wrapper_exits() {
        assert_eq!(cleaned_up("exited-results").unwrap(), "{\"specs\":[]}");
    }

    #[cfg(unix)]
    #[test]
    fn escaped_output_writer_does_not_hold_the_runner_open() {
        let problem = cleaned_up("escaped").unwrap_err();
        assert!(problem.contains("ended without reporting any results"), "{problem}");
    }

    #[test]
    fn timeout_stops_the_wrapper_and_runtime() {
        assert!(cleaned_up("timeout").unwrap_err().contains("was stopped, still running"));
    }

    #[cfg(unix)]
    #[test]
    fn a_launcher_that_changes_groups_is_still_stopped_and_reaped() {
        assert!(cleaned_up("moved-launcher").unwrap_err().contains("was stopped, still running"));
    }

    #[test]
    fn missing_results_stop_a_runtime_after_its_wrapper_exits() {
        let problem = cleaned_up("missing").unwrap_err();

        assert!(problem.contains("ended without reporting any results"), "{problem}");
    }

    #[test]
    #[ignore = "invoked as a subprocess by termination tests"]
    fn interrupt_fixture() -> std::process::ExitCode {
        let mode = fs::read_to_string("mode").unwrap();
        #[cfg(windows)]
        if matches!(mode.as_str(), "interrupt" | "interrupt-output" | "interrupt-results" | "inactive" | "blocked-output") {
            super::windows_tests::ignore_ctrl_c(false);
        }
        if mode == "timeout-output" {
            let outcome = run(Path::new("."), &wrapper_run(), &[], 1, &Interrupt::default());
            std::process::exit(if outcome.is_err() { 2 } else { 0 });
        }
        let command = crate::Command::parse(&["test", "--standalone"]).unwrap();
        let outcome = crate::dispatch(command);
        if mode == "inactive" {
            fs::write("finished", "").unwrap();
            thread::sleep(Duration::from_secs(10));
        }
        outcome
    }

    pub(super) fn termination_case(signal: Option<i32>, mode: &str) {
        termination_case_with_timeout(signal, mode, 300);
    }

    fn termination_case_with_timeout(signal: Option<i32>, mode: &str, timeout: u64) {
        #[cfg(unix)]
        use std::os::unix::process::{CommandExt, ExitStatusExt};
        use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

        let project = TempProject::new(&format!("process-{signal:?}-{mode}-{timeout}"));
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        project.write("address", &listener.local_addr().unwrap().to_string());
        project.write("mode", mode);
        #[cfg(unix)]
        if mode == "moved-launcher-broken-pipe" {
            project.write("parent-group", &unsafe { libc::getpgrp() }.to_string());
        }
        project.write("tests/a.spec.luau", "");
        project.write(
            "litmus.toml",
            &format!("[standalone]\nrun = {}\ntimeout = {timeout}\nlib = \"lib\"\nspecs = [\"tests\"]\n", serde_json::to_string(&wrapper_run()).unwrap()),
        );
        let mut command = fixture_command("standalone::process::tests::interrupt_fixture");
        #[cfg(unix)]
        command.process_group(0);
        #[cfg(windows)]
        if signal.is_some() {
            super::windows_tests::new_console(&mut command);
        }
        let started = Instant::now();
        let mut child = command
            .current_dir(&project)
            .stdin(Stdio::null())
            .stdout(if signal.is_some() && mode != "blocked-output" { Stdio::null() } else { Stdio::piped() })
            .spawn()
            .unwrap();
        let mut connection = None;
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let deadline = Instant::now() + Duration::from_secs(5);
            connection = Some(loop {
                match listener.accept() {
                    Ok((connection, _)) => break connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => {
                        panic!("The termination fixture did not connect ({mode}, {error}).")
                    }
                }
            });
            let connection = connection.as_mut().unwrap();
            connection.set_nonblocking(false).unwrap();
            connection.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
            connection.read_exact(&mut [0]).unwrap();
            if signal.is_none()
                && !matches!(mode, "backlog-results" | "natural-backlog-results" | "retained-reader" | "timeout-output" | "timely-backlog-results")
            {
                drop(child.stdout.take());
            }
            connection.write_all(&[1]).unwrap();
            let ready = project.join(if mode == "inactive" { "finished" } else { "ready" });
            while !ready.exists() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            assert!(ready.exists(), "The termination fixture was not ready ({mode}).");
            let runtime_ready = Instant::now();
            if mode == "retained-reader" {
                let output = child.stdout.take().unwrap();
                let (printed, first) = mpsc::channel();
                let reader = thread::spawn(move || {
                    let mut output = BufReader::new(output);
                    loop {
                        let mut line = String::new();
                        assert_ne!(output.read_line(&mut line).unwrap(), 0, "The CLI closed stdout before forwarding the first line.");
                        if line.trim_end().ends_with("The runtime is still running.") {
                            printed.send(output).unwrap();
                            break;
                        }
                    }
                });
                let output = first.recv_timeout(Duration::from_secs(5)).expect("The CLI did not forward the first runtime line.");
                reader.join().unwrap();
                assert!(child.try_wait().unwrap().is_none(), "A successful write must not stop the CLI.");
                connection.set_nonblocking(true).unwrap();
                assert_eq!(connection.read(&mut [0]).unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
                connection.set_nonblocking(false).unwrap();
                drop(output);
                // A vanished reader is observable only on the next write, not retroactively.
                connection.write_all(&[2]).unwrap();
            }
            if matches!(mode, "blocked-output" | "timeout-output" | "timely-backlog-results") {
                #[cfg(unix)]
                use std::os::fd::AsRawFd;
                let output = child.stdout.as_ref().unwrap();
                let mut previous = 0;
                loop {
                    #[cfg(unix)]
                    let available = {
                        let mut available = 0;
                        assert_eq!(unsafe { libc::ioctl(output.as_raw_fd(), libc::FIONREAD, &mut available) }, 0);
                        available
                    };
                    #[cfg(windows)]
                    let available = super::windows_tests::available(output) as i32;
                    if available >= 4096 && available == previous {
                        break;
                    }
                    assert!(Instant::now() < deadline, "The downstream output pipe did not fill.");
                    previous = available;
                    thread::sleep(Duration::from_millis(100));
                }
            }
            #[cfg(unix)]
            if let Some(signal) = signal {
                assert_eq!(unsafe { libc::kill(-(child.id() as libc::pid_t), signal) }, 0);
            }
            #[cfg(windows)]
            if signal.is_some() {
                super::windows_tests::send_ctrl_c(child.id());
            }
            if mode == "interrupt-results" {
                project.write("report", "");
            }
            let output = if mode == "timely-backlog-results" {
                // Neither the report nor exit is inferred from a sleep. Require a flushed
                // report acknowledgement and process-driven socket disconnection before
                // the earliest possible deadline (which starts after this CLI was spawned).
                connection.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                connection.write_all(&[2]).unwrap();
                let mut reported = [0];
                connection.read_exact(&mut reported).unwrap();
                assert_eq!(reported, [3]);
                assert!(project.join("reported").exists(), "The runtime did not flush its report.");
                let read = connection.read(&mut [0]);
                assert!(
                    matches!(read, Ok(0)) || read.as_ref().is_err_and(|error| matches!(
                        error.kind(), std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::ConnectionReset
                    )),
                    "The completed runtime did not exit ({read:?})."
                );
                assert!(Instant::now() + TICK < started + Duration::from_secs(timeout), "The report and exit were not timely.");
                assert!(child.try_wait().unwrap().is_none(), "The CLI must still be blocked on downstream stdout.");
                // Readiness is observed after run starts, so this target is later than
                // its fixed execution deadline, even when CLI startup was slow.
                thread::sleep((runtime_ready + Duration::from_secs(timeout) + TICK).saturating_duration_since(Instant::now()));
                assert!(child.try_wait().unwrap().is_none(), "The downstream pipe must remain stalled past expiry.");
                let mut output = child.stdout.take().unwrap();
                Some(thread::spawn(move || {
                    let mut printed = Vec::new();
                    output.read_to_end(&mut printed).unwrap();
                    printed
                }))
            } else if mode == "timeout-output" {
                connection.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                let read = connection.read(&mut [0]);
                assert!(
                    matches!(read, Ok(0)) || read.as_ref().is_err_and(|error| matches!(
                        error.kind(), std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::ConnectionReset
                    )),
                    "The runtime survived its deadline while stdout was blocked ({read:?})."
                );
                let mut output = child.stdout.take().unwrap();
                Some(thread::spawn(move || {
                    let mut printed = Vec::new();
                    output.read_to_end(&mut printed).unwrap();
                    printed
                }))
            } else if matches!(mode, "backlog-results" | "natural-backlog-results") {
                let reported = project.join("reported");
                while !reported.exists() && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(10));
                }
                assert!(reported.exists(), "The runtime did not queue its results.");
                let mut output = child.stdout.take().unwrap();
                let mut prefix = Vec::new();
                connection.set_nonblocking(true).unwrap();
                let cleanup_deadline = Instant::now() + Duration::from_secs(3);
                loop {
                    match connection.read(&mut [0]) {
                        Ok(0) => break,
                        Err(error)
                            if matches!(error.kind(), std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::ConnectionReset) =>
                        {
                            break;
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                        result => panic!("The runtime lifetime socket was not idle ({result:?})."),
                    }
                    assert!(Instant::now() < cleanup_deadline, "Cleanup did not precede output replay.");
                    let mut chunk = [0; 4096];
                    let read = output.read(&mut chunk).unwrap();
                    prefix.extend_from_slice(&chunk[..read]);
                    thread::sleep(Duration::from_millis(10));
                }
                connection.set_nonblocking(false).unwrap();
                assert!(prefix.len() < 100_000, "The queued logs must still exceed the downstream pipe capacity.");
                // Keep the downstream pipe stalled beyond the result-collection window.
                thread::sleep(Duration::from_millis(1200));
                Some(thread::spawn(move || {
                    output.read_to_end(&mut prefix).unwrap();
                    prefix
                }))
            } else {
                None
            };
            let deadline = Instant::now() + Duration::from_secs(5);
            let status = loop {
                match child.try_wait().unwrap() {
                    Some(status) => break Some(status),
                    None if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
                    None => break None,
                }
            };
            let read = connection.read(&mut [0]);
            assert!(
                matches!(read, Ok(0))
                    || read.as_ref().is_err_and(|error| matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::ConnectionReset
                    )),
                "The terminated runtime survived ({mode}, {read:?})."
            );
            let status = status.expect("The terminated CLI did not exit.");
            #[cfg(unix)]
            if let Some(signal) = signal {
                assert_eq!(status.signal(), Some(signal), "{mode}, {status:?}");
            }
            #[cfg(windows)]
            if let Some(signal) = signal {
                assert_eq!(status.code(), Some(signal), "{mode}, {status:?}");
            }
            if let Some(output) = output {
                let output = output.join().unwrap();
                let tail = &output[output.len().saturating_sub(2048)..];
                if mode == "timeout-output" {
                    assert_eq!(status.code(), Some(2), "The blocked-output timeout did not fail the run.");
                } else {
                    assert!(status.success(), "Queued results were lost ({status:?}). Output tail follows. {}", String::from_utf8_lossy(tail));
                    if mode == "timely-backlog-results" {
                        assert_eq!(
                            String::from_utf8_lossy(&output).matches("The runtime is still running.").count(),
                            10_000,
                            "Final collection must preserve the logs preceding the accepted report."
                        );
                    }
                }
            } else if signal.is_none() {
                assert!(!status.success(), "The broken stdout did not interrupt the fixture.");
            }
        }));
        if let Some(mut connection) = connection {
            let _ = connection.write_all(&[0]);
        }
        let _ = child.kill();
        let _ = child.wait();
        if let Err(problem) = outcome {
            resume_unwind(problem);
        }
    }

    #[test]
    fn disabled_deadlines_preserve_native_interruption_and_process_cleanup() {
        #[cfg(unix)]
        let signal = libc::SIGINT;
        #[cfg(windows)]
        let signal = 0xC000013A_u32 as i32;
        for mode in ["interrupt", "interrupt-results", "blocked-output"] {
            termination_case_with_timeout(Some(signal), mode, 0);
        }
    }

    #[cfg(unix)]
    #[test]
    fn group_termination_cleans_up_the_runtime_and_keeps_the_original_signal() {
        for signal in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP, libc::SIGQUIT] {
            for mode in ["interrupt", "interrupt-output", "interrupt-results", "inactive"] {
                termination_case(Some(signal), mode);
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn termination_cleans_up_even_when_the_output_consumer_stops_reading() {
        for signal in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP, libc::SIGQUIT] {
            termination_case(Some(signal), "blocked-output");
        }
    }

    #[test]
    fn timeout_stops_the_runtime_even_when_the_output_consumer_stops_reading() {
        termination_case(None, "timeout-output");
    }

    #[test]
    fn timely_results_survive_output_blocked_beyond_the_execution_deadline() {
        termination_case_with_timeout(None, "timely-backlog-results", 8);
    }

    #[test]
    fn queued_results_survive_a_natural_exit_without_any_descendants() {
        termination_case(None, "natural-backlog-results");
    }

    #[test]
    fn queued_results_survive_slow_output_replay_after_launcher_exit() {
        termination_case(None, "backlog-results");
    }

    fn isolated_broken_stdout(mode: &str) {
        let project = TempProject::new(&format!("isolated-{mode}"));
        project.write("isolated-mode", mode);
        // Create the tested pipe inside a helper, beyond unrelated suite forks.
        let mut child = fixture_command("standalone::process::tests::broken_stdout_fixture")
            .current_dir(&project)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let status = wait_for_exit(|| child.try_wait(), Instant::now() + Duration::from_secs(20));
        if status.is_err() {
            let _ = child.kill();
            let _ = wait_for_exit(|| child.try_wait(), Instant::now() + Duration::from_secs(5));
        }
        assert!(status.expect("The isolated stdout fixture did not finish.").success(), "The isolated stdout fixture failed ({mode}).");
    }

    #[test]
    #[ignore = "invoked as an isolated subprocess by broken stdout tests"]
    fn broken_stdout_fixture() {
        let mode = fs::read_to_string("isolated-mode").unwrap();
        assert!(matches!(mode.as_str(), "broken-pipe" | "moved-launcher-broken-pipe" | "retained-reader"));
        termination_case(None, &mode);
    }

    #[cfg(unix)]
    #[test]
    fn broken_stdout_stops_a_launcher_that_moved_to_another_group() {
        isolated_broken_stdout("moved-launcher-broken-pipe");
    }

    #[test]
    fn broken_stdout_unwinds_without_leaving_the_runtime_running() {
        isolated_broken_stdout("broken-pipe");
    }

    #[test]
    fn a_reader_that_disappears_requires_another_write_before_cleanup() {
        isolated_broken_stdout("retained-reader");
    }

    fn exited() -> ExitStatus {
        #[cfg(unix)]
        use std::os::unix::process::ExitStatusExt;
        #[cfg(windows)]
        use std::os::windows::process::ExitStatusExt;

        ExitStatus::from_raw(0)
    }

    #[test]
    fn finalization_keeps_queued_results_and_emits_the_exact_cleanup_warning() {
        let payload = "{\"specs\":[]}";
        let (sender, receiver) = mpsc::channel();
        sender.send(Message::Results(payload.to_owned())).unwrap();
        drop(sender);
        let mut warnings = Vec::new();

        let result = finalize_report(
            "lune run",
            Ok(None),
            Err("couldn't stop it".to_owned()),
            || match receiver.try_recv().unwrap() {
                Message::Results(results) => Some(results),
                message => panic!("Expected queued results, got {message:?}."),
            },
            || false,
            |warning| warnings.push(warning.to_owned()),
        );

        assert_eq!(result, Ok(payload.to_owned()));
        assert_eq!(warnings, ["couldn't stop it"]);
    }

    #[test]
    fn finalization_obeys_the_reporting_and_cleanup_truth_table() {
        let payload = "{\"specs\":[]}";
        for reported in [Ok(None), Ok(Some("accepted results".to_owned())), Err("it timed out".to_owned())] {
            for queued in [false, true] {
                for cleanup_succeeded in [false, true] {
                    let (sender, receiver) = mpsc::channel();
                    if queued {
                        sender.send(Message::Results(payload.to_owned())).unwrap();
                    }
                    drop(sender);
                    let mut collected = false;
                    let mut warnings = Vec::new();
                    let stopped = if cleanup_succeeded { Ok(exited()) } else { Err("couldn't stop it".to_owned()) };
                    let result = finalize_report(
                        "lune run",
                        reported.clone(),
                        stopped,
                        || {
                            collected = true;
                            receiver.try_recv().ok().map(|message| match message {
                                Message::Results(results) => results,
                                message => panic!("Expected queued results, got {message:?}."),
                            })
                        },
                        || false,
                        |warning| warnings.push(warning.to_owned()),
                    );
                    let expected = match (&reported, queued, cleanup_succeeded) {
                        (Ok(Some(results)), _, true) => (Ok(results.clone()), vec![]),
                        (Ok(Some(results)), _, false) => (Ok(results.clone()), vec!["couldn't stop it".to_owned()]),
                        (Ok(None), true, true) => (Ok(payload.to_owned()), vec![]),
                        (Ok(None), true, false) => (Ok(payload.to_owned()), vec!["couldn't stop it".to_owned()]),
                        (Ok(None), false, true) => {
                            (Err(format!("`lune run` ended without reporting any results ({})", exited())), vec![])
                        }
                        (Ok(None), false, false) => {
                            (Err("`lune run` ended without reporting any results, and couldn't stop it".to_owned()), vec![])
                        }
                        (Err(problem), _, true) => (Err(problem.clone()), vec![]),
                        (Err(problem), _, false) => (Err(format!("{problem}, and couldn't stop it")), vec![]),
                    };

                    assert_eq!(collected, reported.is_ok(), "{reported:?}, {queued}, {cleanup_succeeded}");
                    assert_eq!((result, warnings), expected, "{reported:?}, {queued}, {cleanup_succeeded}");
                    let remaining = if reported.is_err() && queued { vec![Message::Results(payload.to_owned())] } else { vec![] };
                    assert_eq!(receiver.try_iter().collect::<Vec<_>>(), remaining);
                }
            }
        }
    }

    #[test]
    fn finalization_cancellation_during_collection_overrides_queued_and_accepted_results() {
        let payload = "{\"specs\":[]}";
        for accepted in [None, Some("accepted results".to_owned())] {
            for cleanup_succeeded in [false, true] {
                let (sender, receiver) = mpsc::channel();
                sender.send(Message::Results(payload.to_owned())).unwrap();
                drop(sender);
                let cancelled = std::cell::Cell::new(false);
                let mut warnings = Vec::new();
                let stopped = if cleanup_succeeded { Ok(exited()) } else { Err("couldn't stop it".to_owned()) };
                let result = finalize_report(
                    "lune run",
                    Ok(accepted.clone()),
                    stopped,
                    || {
                        assert!(!cancelled.get());
                        let Message::Results(results) = receiver.try_recv().unwrap() else {
                            panic!("Expected queued results.");
                        };
                        cancelled.set(true);
                        Some(results)
                    },
                    || cancelled.get(),
                    |warning| warnings.push(warning.to_owned()),
                );
                let expected = if cleanup_succeeded {
                    "the standalone run was interrupted"
                } else {
                    "the standalone run was interrupted, and couldn't stop it"
                };

                assert!(cancelled.get(), "Collection must run before checking cancellation.");
                assert_eq!(result, Err(expected.to_owned()), "{accepted:?}, {cleanup_succeeded}");
                assert!(warnings.is_empty(), "Cancellation must not emit a results-success warning.");
            }
        }
    }

    #[test]
    fn keeps_queued_results_with_a_warning_when_cleanup_fails() {
        let reported = collect_report(Ok(None), || Some("queued results".to_owned()));
        let (outcome, warning) = outcome("lune run", reported, Err("couldn't stop it".to_owned()));

        assert_eq!(outcome, Ok("queued results".to_owned()));
        assert_eq!(warning.as_deref(), Some("couldn't stop it"));
    }

    #[test]
    fn collects_queued_reports_without_replacing_already_accepted_results() {
        for accepted in [None, Some("accepted results".to_owned())] {
            for queued in [None, Some("queued results".to_owned())] {
                let mut collected = false;
                let reported = collect_report(Ok(accepted.clone()), || {
                    collected = true;
                    queued.clone()
                });

                assert!(collected, "A valid report state must collect queued messages even with accepted results.");
                assert_eq!(reported, Ok(accepted.clone().or(queued)));
            }
        }
    }

    #[test]
    fn does_not_collect_queued_reports_after_reporting_failed() {
        let reported = collect_report(Err("it timed out".to_owned()), || panic!("A failed report must not collect queued messages."));

        assert_eq!(reported, Err("it timed out".to_owned()));
    }

    #[test]
    fn report_collection_depends_on_valid_reporting_not_cleanup_success() {
        for valid_report in [false, true] {
            for cleanup_succeeded in [false, true] {
                let mut collected = false;
                let reported = if valid_report { Ok(None) } else { Err("it timed out".to_owned()) };
                let stopped = if cleanup_succeeded { Ok(exited()) } else { Err("couldn't stop it".to_owned()) };
                let reported = collect_report(reported, || {
                    collected = true;
                    Some("queued results".to_owned())
                });
                let expected = match (valid_report, cleanup_succeeded) {
                    (true, true) => (Ok("queued results".to_owned()), None),
                    (true, false) => (Ok("queued results".to_owned()), Some("couldn't stop it".to_owned())),
                    (false, true) => (Err("it timed out".to_owned()), None),
                    (false, false) => (Err("it timed out, and couldn't stop it".to_owned()), None),
                };

                assert_eq!(collected, valid_report);
                assert_eq!(outcome("lune run", reported, stopped), expected, "{valid_report}, {cleanup_succeeded}");
            }
        }
    }

    #[test]
    fn keeps_reported_results_with_a_warning_when_the_runtime_cannot_be_stopped() {
        let (outcome, warning) = outcome("lune run", Ok(Some("{}".to_owned())), Err("couldn't stop it".to_owned()));

        assert_eq!(outcome, Ok("{}".to_owned()));
        assert_eq!(warning.as_deref(), Some("couldn't stop it"));
    }

    #[test]
    fn reports_results_without_a_warning_when_the_runtime_stopped() {
        assert_eq!(outcome("lune run", Ok(Some("{}".to_owned())), Ok(exited())), (Ok("{}".to_owned()), None));
    }

    #[test]
    fn names_both_problems_when_a_run_timed_out_and_could_not_be_stopped() {
        let (outcome, warning) = outcome("lune run", Err("it timed out".to_owned()), Err("couldn't stop it".to_owned()));

        assert_eq!(outcome, Err("it timed out, and couldn't stop it".to_owned()));
        assert_eq!(warning, None);
    }

    #[test]
    fn says_a_run_reported_nothing_even_when_it_could_not_be_stopped() {
        let (outcome, _) = outcome("lune run", Ok(None), Err("couldn't stop it".to_owned()));

        assert_eq!(outcome, Err("`lune run` ended without reporting any results, and couldn't stop it".to_owned()));
    }

    #[test]
    fn gives_the_budget_in_minutes_when_it_is_whole_minutes() {
        assert_eq!(in_words(Duration::from_secs(300)), "5 minutes");
        assert_eq!(in_words(Duration::from_secs(60)), "1 minute");
        assert_eq!(in_words(Duration::from_secs(2)), "2 seconds");
        assert_eq!(in_words(Duration::from_secs(90)), "90 seconds");
    }

    #[test]
    fn a_disabled_deadline_has_no_expiry() {
        let started = Instant::now();

        assert_eq!(deadline(started, 0), Ok(None));
    }

    #[test]
    fn a_configured_deadline_uses_exact_seconds() {
        let started = Instant::now();

        for timeout in [1, 300, 900] {
            let expires = deadline(started, timeout).unwrap().unwrap();

            assert_eq!(expires.duration_since(started), Duration::from_secs(timeout));
            assert!(started < expires);
            assert!(started + Duration::from_secs(timeout) >= expires);
        }
    }

    #[test]
    fn polling_never_waits_past_expiry_or_accepts_the_exact_boundary() {
        let now = Instant::now();

        assert_eq!(poll_interval(None, now), TICK);
        assert_eq!(poll_interval(Some(now + Duration::from_secs(1)), now), TICK);
        assert_eq!(poll_interval(Some(now + Duration::from_millis(25)), now), Duration::from_millis(25));
        assert_eq!(poll_interval(Some(now), now), Duration::ZERO);
        assert_eq!(poll_interval(Some(now), now + Duration::from_millis(1)), Duration::ZERO);
    }

    #[test]
    fn relay_acceptance_is_strictly_before_the_fixed_deadline() {
        let expires = Instant::now() + Duration::from_secs(1);
        for (arrival, timely) in [
            (expires - Duration::from_nanos(1), true),
            (expires, false),
            (expires + Duration::from_nanos(1), false),
        ] {
            let mut accepted = None;
            record_report(&mut accepted, "reported results", Some(expires), arrival);

            assert_eq!(accepted.as_deref(), timely.then_some("reported results"), "{arrival:?}");
        }
    }

    #[test]
    fn relay_acceptance_without_an_execution_deadline_remains_unlimited() {
        let now = Instant::now();
        let mut accepted = None;
        record_report(&mut accepted, "first results", None, now + Duration::from_secs(3600));
        record_report(&mut accepted, "replacement results", None, now + Duration::from_secs(7200));

        assert_eq!(accepted.as_deref(), Some("first results"));
        assert_eq!(poll_interval(report_deadline(None, &accepted), now), TICK);
    }

    #[test]
    fn timely_relay_acceptance_survives_later_display_expiry_and_collects_queued_messages() {
        let expires = Instant::now() + Duration::from_secs(1);
        let execution = Some(expires);
        let mut accepted = None;
        record_report(&mut accepted, "timely results", execution, expires - Duration::from_nanos(1));
        // The display resumes long after expiry, but receipt happened on the relay thread.
        let resumed = expires + Duration::from_secs(5);
        record_report(&mut accepted, "late replacement", execution, resumed);
        assert_eq!(poll_interval(execution, resumed), Duration::ZERO);
        assert_eq!(poll_interval(report_deadline(execution, &accepted), resumed), TICK);
        let drain = collection_deadline(report_deadline(execution, &accepted), resumed);
        assert_eq!(drain, resumed + CLEANUP_BUDGET);
        assert_eq!(poll_interval(Some(drain), resumed), TICK);
        assert_eq!(poll_interval(Some(drain), drain), Duration::ZERO);
        let mut replayed = Vec::new();
        let reported = collect_report(Ok(accepted.clone()), || {
            replayed = relayed("preceding log\n@@litmus-progress@@{\"kind\":\"settled\"}\n@@litmus-run@@timely results\n");
            accepted.clone()
        });

        assert_eq!(replayed, [printed("preceding log"), Message::Event(Event::Settled), Message::Results("timely results".to_owned())]);
        assert_eq!(outcome("runtime", reported, Ok(exited())), (Ok("timely results".to_owned()), None));
    }

    #[test]
    fn timely_acceptance_during_collection_lifts_only_execution_expiry() {
        let collecting = Instant::now();
        let expires = collecting + Duration::from_millis(25);
        let mut accepted = None;
        assert_eq!(collection_deadline(report_deadline(Some(expires), &accepted), collecting), expires);
        record_report(&mut accepted, "timely results", Some(expires), expires - Duration::from_nanos(1));

        let drain = collection_deadline(report_deadline(Some(expires), &accepted), collecting);
        assert_eq!(drain, collecting + CLEANUP_BUDGET);
        assert_eq!(poll_interval(Some(drain), expires), TICK);
        assert_eq!(poll_interval(Some(drain), drain), Duration::ZERO);
    }

    #[test]
    fn genuinely_late_relay_reports_cannot_rescue_expiry_or_extend_collection() {
        let expires = Instant::now();
        for arrival in [expires, expires + Duration::from_nanos(1)] {
            let mut accepted = None;
            record_report(&mut accepted, "late results", Some(expires), arrival);
            let execution = report_deadline(Some(expires), &accepted);
            assert_eq!(accepted, None);
            assert_eq!(poll_interval(execution, arrival), Duration::ZERO);
            assert_eq!(collection_deadline(execution, arrival), expires);
            let reported = collect_report(Err("it timed out".to_owned()), || panic!("Late receipt must not enable collection."));

            assert_eq!(outcome("runtime", reported, Ok(exited())), (Err("it timed out".to_owned()), None));
        }
    }

    #[test]
    fn final_collection_cannot_extend_the_execution_deadline() {
        let now = Instant::now();
        assert_eq!(collection_deadline(None, now), now + Duration::from_secs(1));
        assert_eq!(collection_deadline(Some(now + Duration::from_secs(5)), now), now + Duration::from_secs(1));
        assert_eq!(collection_deadline(Some(now + Duration::from_millis(25)), now), now + Duration::from_millis(25));
        assert_eq!(collection_deadline(Some(now), now), now);
        assert_eq!(collection_deadline(Some(now - Duration::from_millis(1)), now), now - Duration::from_millis(1));
    }

    #[test]
    fn accepted_results_survive_cleanup_that_passes_the_execution_deadline() {
        let now = Instant::now();
        let execution = Some(now - Duration::from_secs(1));
        let reported = collect_report(Ok(Some("accepted results".to_owned())), || {
            assert!(poll_interval(Some(collection_deadline(execution, now)), now).is_zero());
            None
        });

        assert_eq!(outcome("lune run", reported, Ok(exited())), (Ok("accepted results".to_owned()), None));
    }

    #[test]
    fn an_unrepresentable_deadline_is_rejected_before_spawning_the_command() {
        let command = ["litmus-no-such-command".to_owned()];
        let problem = run(Path::new("."), &command, &[], u64::MAX, &Interrupt::default()).unwrap_err();

        assert!(problem.contains("timeout"), "{problem}");
        assert!(!problem.contains("couldn't start"), "{problem}");
    }

    #[test]
    fn a_deadline_that_does_not_fit_is_an_error_instead_of_a_panic() {
        let started = Instant::now();
        let timeout = u64::MAX;

        assert!(started.checked_add(Duration::from_secs(timeout)).is_none());
        let problem = deadline(started, timeout).unwrap_err();
        assert!(problem.contains("timeout") && problem.contains(&timeout.to_string()), "{problem}");
    }

    #[cfg(unix)]
    #[test]
    fn a_silent_command_stops_at_the_configured_deadline() {
        let dir = crate::testing::TempProject::new("silent-deadline");
        let command = ["sh".to_owned(), "-c".to_owned(), "exec sleep 60".to_owned()];
        let problem = run(&dir, &command, &[], 1, &Interrupt::default()).unwrap_err();

        assert!(problem.contains("after 1 second"), "{problem}");
        assert!(!problem.contains("after 1 seconds"), "{problem}");
    }

    #[cfg(unix)]
    #[test]
    fn progress_does_not_replace_the_configured_deadline() {
        let dir = crate::testing::TempProject::new("progress-deadline");
        let command = [
            "sh".to_owned(),
            "-c".to_owned(),
            "for tick in 1 2 3 4 5 6 7 8; do printf '%s\\n' '@@litmus-progress@@{\"kind\":\"settled\"}'; sleep 0.25; done; printf '%s\\n' '@@litmus-run@@{\"specs\":[]}'".to_owned(),
        ];
        let started = Instant::now();
        let problem = run(&dir, &command, &[], 1, &Interrupt::default()).unwrap_err();

        assert!(problem.contains("after 1 second"), "{problem}");
        assert!(started.elapsed() < Duration::from_secs(3), "{:?}", started.elapsed());
    }

    #[cfg(unix)]
    #[test]
    fn a_result_after_expiry_is_not_accepted_during_a_receive_tick() {
        let dir = crate::testing::TempProject::new("late-result-deadline");
        let command = [
            "sh".to_owned(),
            "-c".to_owned(),
            "sleep 0.95; printf '%s\\n' '@@litmus-progress@@{\"kind\":\"settled\"}'; sleep 0.1; printf '%s\\n' '@@litmus-run@@{\"specs\":[]}'".to_owned(),
        ];
        let problem = run(&dir, &command, &[], 1, &Interrupt::default()).unwrap_err();

        assert!(problem.contains("after 1 second"), "{problem}");
    }

    #[cfg(unix)]
    #[test]
    fn closing_stdout_does_not_escape_the_deadline() {
        let dir = crate::testing::TempProject::new("closed-output-deadline");
        let command = ["sh".to_owned(), "-c".to_owned(), "exec 1>&-; exec sleep 60".to_owned()];
        let problem = run(&dir, &command, &[], 1, &Interrupt::default()).unwrap_err();

        assert!(problem.contains("after 1 second"), "{problem}");
    }

    #[cfg(unix)]
    #[test]
    fn a_disabled_timeout_accepts_a_result_after_a_short_deadline_would_expire() {
        let dir = crate::testing::TempProject::new("disabled-deadline");
        let command = ["sh".to_owned(), "-c".to_owned(), "sleep 2; printf '%s\\n' '@@litmus-run@@{\"specs\":[]}'".to_owned()];

        assert_eq!(run(&dir, &command, &[], 0, &Interrupt::default()), Ok("{\"specs\":[]}".to_owned()));
    }

    #[test]
    fn a_command_that_cannot_start_is_named() {
        let command = ["litmus-no-such-command".to_owned(), "run".to_owned()];
        let problem = run(Path::new("."), &command, &[], 300, &Interrupt::default()).unwrap_err();

        assert!(problem.contains("`litmus-no-such-command run .litmus/run.luau`"), "{problem}");
    }
}
