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
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

const BUDGET: Duration = Duration::from_secs(300);
const TICK: Duration = Duration::from_millis(100);

#[derive(Debug, PartialEq)]
enum Message {
    Printed(String),
    Event(Event),
    Results(String),
}

pub fn run(project_dir: &Path, run: &[String], specs: &[String], interrupt: &Interrupt) -> Result<String, String> {
    run_with_budget(project_dir, run, specs, BUDGET, interrupt)
}

fn run_with_budget(
    project_dir: &Path,
    run: &[String],
    specs: &[String],
    budget: Duration,
    interrupt: &Interrupt,
) -> Result<String, String> {
    let script = format!("{FOLDER}/{SCRIPT}");
    let shown = format!("{} {script}", run.join(" "));

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

    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || relay(BufReader::new(output), |message| sender.send(message).is_ok()));

    let deadline = Instant::now() + budget;
    let mut display = Display::new(specs.to_vec());

    let mut reported = loop {
        if interrupt.cancelled() {
            break Ok(None);
        }
        let message = receiver.recv_timeout(TICK);
        if interrupt.cancelled() {
            break Ok(None);
        }
        match message {
            Ok(Message::Printed(line)) => display.print(&line),
            Ok(Message::Event(event)) => display.take(event),
            Ok(Message::Results(results)) => break Ok(Some(results)),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break Ok(None),
        }

        if Instant::now() >= deadline {
            break Err(format!("`{shown}` was stopped, still running after {}", in_words(budget)));
        }

        display.refresh();
    };

    display.finish();

    let stopped = stop_owned(child, &shown);
    if interrupt.cancelled() {
        reported = Err("the standalone run was interrupted".to_owned());
    }
    let (outcome, warning) = outcome(&shown, reported, stopped);
    if let Some(warning) = warning {
        eprintln!("litmus: {warning}");
    }

    outcome
}

fn stop_owned(mut child: Box<dyn process_wrap::std::ChildWrapper>, shown: &str) -> Result<ExitStatus, String> {
    let stopped = stop(&mut child, shown);
    if stopped.is_ok() {
        // Disarm the outer drop guard once its group/job has been stopped and reaped.
        drop(child.into_inner());
    }
    stopped
}

// Stops the owned process group or job, then reaps the launcher.
fn stop(child: &mut Box<dyn process_wrap::std::ChildWrapper>, shown: &str) -> Result<ExitStatus, String> {
    #[cfg(unix)]
    let stop_deadline = Instant::now() + Duration::from_secs(1);
    let stopped = loop {
        match child.start_kill() {
            #[cfg(unix)]
            Err(error) if error.raw_os_error() == Some(libc::EPERM) && Instant::now() < stop_deadline => {
                // macOS can reject a signal while a launcher is exiting. Reap it before
                // retrying, without suppressing a persistent permission error.
                child.try_wait().map_err(|error| format!("couldn't tell how `{shown}` ended: {error}"))?;
                thread::sleep(TICK);
            }
            result => break result,
        }
    };
    if let Err(error) = stopped {
        #[cfg(unix)]
        let already_gone = error.raw_os_error() == Some(libc::ESRCH);
        #[cfg(not(unix))]
        let already_gone = false;
        if !already_gone {
            return Err(format!("couldn't stop the process tree for `{shown}`, so it may still be running: {error}"));
        }
    }

    child.wait().map_err(|error| format!("couldn't tell how `{shown}` ended: {error}"))
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
    #[allow(clippy::zombie_processes, reason = "The outer process owner must stop descendants after this launcher exits.")]
    fn wrapper_fixture() {
        let mode = fs::read_to_string("mode").unwrap();
        let mut command = fixture_command("standalone::process::tests::descendant_fixture");
        if mode == "missing" {
            command.stdout(Stdio::null());
        }
        let mut child = command.spawn().unwrap();
        if mode == "missing" {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !Path::new("ready").exists() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
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
        if matches!(mode.as_str(), "results" | "inactive" | "interrupt-results") {
            println!("\n{MARKER}{{\"specs\":[]}}");
            std::io::stdout().flush().unwrap();
        }
        let prints = match mode.as_str() {
            "interrupt-output" => 10_000,
            "broken-pipe" => 1,
            _ => 0,
        };
        for _ in 0..prints {
            println!("The runtime is still running.");
        }
        let _ = connection.read(&mut [0]);
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
        let project = TempProject::new(&format!("process-cleanup-{mode}"));
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        project.write("address", &listener.local_addr().unwrap().to_string());
        project.write("mode", mode);
        let command = wrapper_run();
        thread::scope(|scope| {
            let (finished, completion) = mpsc::sync_channel(1);
            let project = &project;
            let command = &command;
            let runner = scope.spawn(move || {
                let result = run_with_budget(project, command, &[], Duration::from_secs(5), &Interrupt::default());
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
            assert!(closed, "The runtime descendant survived the {mode} exit ({read:?}). Runner returned {result:?}.");
            result
        })
    }

    #[test]
    fn reported_results_stop_the_wrapper_and_runtime() {
        assert_eq!(cleaned_up("results").unwrap(), "{\"specs\":[]}");
    }

    #[test]
    fn timeout_stops_the_wrapper_and_runtime() {
        assert!(cleaned_up("timeout").unwrap_err().contains("was stopped, still running"));
    }

    #[test]
    fn missing_results_stop_a_runtime_after_its_wrapper_exits() {
        let problem = cleaned_up("missing").unwrap_err();

        // A Windows descendant inherits its launcher's output pipe, so the pipe stays open after
        // the launcher exits and the run stops at its budget. Either way the descendant is gone.
        let ended = problem.contains("ended without reporting any results");
        let stopped = cfg!(windows) && problem.contains("was stopped, still running");
        assert!(ended || stopped, "{problem}");
    }

    #[test]
    #[ignore = "invoked as a subprocess by termination tests"]
    fn interrupt_fixture() -> std::process::ExitCode {
        let command = crate::Command::parse(&["test", "--standalone"]).unwrap();
        let outcome = crate::dispatch(command);
        if fs::read_to_string("mode").unwrap() == "inactive" {
            fs::write("finished", "").unwrap();
            thread::sleep(Duration::from_secs(10));
        }
        outcome
    }

    fn termination_case(signal: Option<i32>, mode: &str) {
        #[cfg(unix)]
        use std::os::unix::process::{CommandExt, ExitStatusExt};
        use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

        let project = TempProject::new(&format!("process-{signal:?}-{mode}"));
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        project.write("address", &listener.local_addr().unwrap().to_string());
        project.write("mode", mode);
        project.write("tests/a.spec.luau", "");
        project.write(
            "litmus.toml",
            &format!("[standalone]\nrun = {}\nlib = \"lib\"\nspecs = [\"tests\"]\n", serde_json::to_string(&wrapper_run()).unwrap()),
        );
        let mut command = fixture_command("standalone::process::tests::interrupt_fixture");
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .current_dir(&project)
            .stdin(Stdio::null())
            .stdout(if signal.is_some() { Stdio::null() } else { Stdio::piped() })
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
            if signal.is_none() {
                drop(child.stdout.take());
            }
            connection.write_all(&[1]).unwrap();
            let ready = project.join(if mode == "inactive" { "finished" } else { "ready" });
            while !ready.exists() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            assert!(ready.exists(), "The termination fixture was not ready ({mode}).");
            #[cfg(unix)]
            if let Some(signal) = signal {
                assert_eq!(unsafe { libc::kill(-(child.id() as libc::pid_t), signal) }, 0);
            }
            if mode == "interrupt-results" {
                project.write("report", "");
            }
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
            if signal.is_none() {
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

    #[cfg(unix)]
    #[test]
    fn group_termination_cleans_up_the_runtime_and_keeps_the_original_signal() {
        for signal in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP, libc::SIGQUIT] {
            for mode in ["interrupt", "interrupt-output", "interrupt-results", "inactive"] {
                termination_case(Some(signal), mode);
            }
        }
    }

    #[test]
    fn broken_stdout_unwinds_without_leaving_the_runtime_running() {
        termination_case(None, "broken-pipe");
    }

    fn exited() -> ExitStatus {
        #[cfg(unix)]
        use std::os::unix::process::ExitStatusExt;
        #[cfg(windows)]
        use std::os::windows::process::ExitStatusExt;

        ExitStatus::from_raw(0)
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
    fn a_command_that_cannot_start_is_named() {
        let command = ["litmus-no-such-command".to_owned(), "run".to_owned()];
        let problem = run(Path::new("."), &command, &[], &Interrupt::default()).unwrap_err();

        assert!(problem.contains("`litmus-no-such-command run .litmus/run.luau`"), "{problem}");
    }
}
