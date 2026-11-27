#![cfg(all(test, windows))]

use std::ffi::c_void;
use std::io;
use std::os::windows::io::AsRawHandle;
use std::os::windows::process::CommandExt;
use std::panic::{catch_unwind, resume_unwind};
use std::process::{Child, ChildStdout, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const CREATE_NEW_CONSOLE: u32 = 0x0000_0010;
const CTRL_C_EVENT: u32 = 0;
const SIGNAL_FIXTURE: &str = "standalone::process::windows_tests::console_signal_fixture";
const TARGET_PID_ENV: &str = "LITMUS_TEST_CTRL_C_TARGET_PID";

#[link(name = "kernel32")]
unsafe extern "system" {
    fn AttachConsole(process_id: u32) -> i32;
    fn FreeConsole() -> i32;
    fn GenerateConsoleCtrlEvent(event: u32, process_group_id: u32) -> i32;
    fn PeekNamedPipe(
        pipe: *mut c_void,
        buffer: *mut c_void,
        buffer_size: u32,
        bytes_read: *mut u32,
        total_bytes_available: *mut u32,
        bytes_left_in_message: *mut u32,
    ) -> i32;
    fn SetConsoleCtrlHandler(handler: Option<unsafe extern "system" fn(u32) -> i32>, add: i32) -> i32;
}

pub(super) fn new_console(command: &mut Command) {
    command.creation_flags(CREATE_NEW_CONSOLE);
}

// Call only in subprocess fixtures, never in the parent test harness.
pub(super) fn ignore_ctrl_c(ignore: bool) {
    // SAFETY: A null handler changes only the calling process's inheritable attribute.
    assert_ne!(
        unsafe { SetConsoleCtrlHandler(None, i32::from(ignore)) },
        0,
        "Could not change Ctrl+C handling ({ignore}, {}).",
        io::Error::last_os_error()
    );
}

pub(super) fn available(output: &ChildStdout) -> u32 {
    let mut available = 0;
    // SAFETY: The borrowed stdout keeps its pipe handle alive. Only the writable count
    // is requested; all optional output buffers are null and no bytes are consumed.
    let result = unsafe {
        PeekNamedPipe(output.as_raw_handle(), std::ptr::null_mut(), 0, std::ptr::null_mut(), &mut available, std::ptr::null_mut())
    };
    assert_ne!(result, 0, "Could not inspect the downstream output pipe ({}).", io::Error::last_os_error());
    available
}

// The target must be a live CLI fixture started with new_console and ready to handle
// Ctrl+C. Only the short-lived sender attaches to that isolated console.
pub(super) fn send_ctrl_c(pid: u32) {
    assert!(pid != 0 && pid != u32::MAX && pid != std::process::id(), "Ctrl+C requires an explicit subprocess PID.");
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", SIGNAL_FIXTURE, "--ignored", "--nocapture", "--test-threads=1"])
        .env(TARGET_PID_ENV, pid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    // Do not inherit the cargo or CI runner console, even briefly, in the sender.
    new_console(&mut command);
    let mut child = command.spawn().expect("Could not start the console signal fixture.");
    let status = match wait_for_sender(&mut child) {
        Ok(status) => status,
        Err(problem) => {
            // Cleanup also has a deadline, so even an OS failure cannot hang the suite.
            let killed = child.kill();
            let reaped = wait_for_sender(&mut child);
            panic!(
                "The console signal fixture did not finish ({problem}). Forced termination returned {killed:?}. Cleanup returned {reaped:?}."
            );
        }
    };
    assert!(status.success(), "The console signal fixture failed ({status:?}).");
}

fn wait_for_sender(child: &mut Child) -> io::Result<ExitStatus> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "The console signal fixture did not exit before its deadline."));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[ignore = "invoked as a subprocess by Windows termination tests"]
fn console_signal_fixture() {
    // Validate the explicit test-only target before changing any console state. Never
    // accept ATTACH_PARENT_PROCESS, which is represented by u32::MAX.
    let pid = std::env::var(TARGET_PID_ENV).expect("The console signal fixture requires an explicit target PID.").parse::<u32>().unwrap();
    assert!(pid != 0 && pid != u32::MAX && pid != std::process::id(), "The console signal target must be an explicit subprocess PID.");
    // SAFETY: This runs only in the ignored sender subprocess. FreeConsole detaches
    // this process alone and succeeds even if it has no console.
    assert_ne!(unsafe { FreeConsole() }, 0, "Could not detach the sender console ({}).", io::Error::last_os_error());
    // SAFETY: The target PID names the CLI fixture's dedicated console, not a parent.
    assert_ne!(unsafe { AttachConsole(pid) }, 0, "Could not attach to the target console ({pid}, {}).", io::Error::last_os_error());
    let outcome = catch_unwind(|| {
        // Attaching resets console handlers, so enable the sender's ignore flag now.
        ignore_ctrl_c(true);
        // SAFETY: Only the isolated CLI console is attached. Ctrl+C cannot be targeted
        // at a nonzero process group; zero broadcasts within this console only.
        assert_ne!(
            unsafe { GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0) },
            0,
            "Could not send Ctrl+C to the target console ({}).",
            io::Error::last_os_error()
        );
    });
    // Detach even if setting the ignore attribute or generating the event failed.
    let detached = unsafe { FreeConsole() };
    if let Err(problem) = outcome {
        resume_unwind(problem);
    }
    assert_ne!(detached, 0, "Could not detach from the target console ({}).", io::Error::last_os_error());
}

#[test]
fn console_interrupt_cleans_up_the_runtime_and_keeps_the_original_exit_code() {
    for mode in ["interrupt", "interrupt-output", "interrupt-results", "inactive", "blocked-output"] {
        super::tests::termination_case(Some(0xC000013A_u32 as i32), mode);
    }
}
