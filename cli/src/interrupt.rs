use std::sync::{Arc, Mutex};

#[cfg(windows)]
const CONTROL_C_EXIT: i32 = 0xC000_013A_u32 as i32;

#[derive(Clone, Default)]
pub struct Interrupt {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    active: bool,
    signal: Option<i32>,
}

pub struct ActiveRun<'a>(&'a Interrupt);

impl Interrupt {
    pub fn install(&self) -> std::io::Result<()> {
        let interrupt = self.clone();
        #[cfg(unix)]
        {
            use signal_hook::{
                consts::{SIGHUP, SIGINT, SIGQUIT, SIGTERM},
                iterator::Signals,
            };

            let mut signals = Signals::new([SIGINT, SIGTERM, SIGHUP, SIGQUIT])?;
            let _ = std::thread::Builder::new().spawn(move || {
                for signal in signals.forever() {
                    if !interrupt.request(signal) {
                        terminate(interrupt.signal().unwrap_or(signal));
                    }
                }
            })?;
            Ok(())
        }
        #[cfg(windows)]
        {
            ctrlc::set_handler(move || {
                if !interrupt.request(CONTROL_C_EXIT) {
                    terminate(interrupt.signal().unwrap_or(CONTROL_C_EXIT));
                }
            })
            .map_err(std::io::Error::other)
        }
    }

    pub fn arm(&self) -> ActiveRun<'_> {
        self.state.lock().unwrap().active = true;
        ActiveRun(self)
    }

    pub fn signal(&self) -> Option<i32> {
        self.state.lock().unwrap().signal
    }

    pub fn cancelled(&self) -> bool {
        self.signal().is_some()
    }

    fn request(&self, signal: i32) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.active {
            state.signal.get_or_insert(signal);
        }
        state.active
    }
}

impl Drop for ActiveRun<'_> {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().active = false;
    }
}

pub fn terminate(signal: i32) -> ! {
    #[cfg(unix)]
    {
        let _ = signal_hook::low_level::emulate_default_handler(signal);
        std::process::exit(128 + signal);
    }
    #[cfg(windows)]
    std::process::exit(signal);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inactive_interrupts_keep_normal_termination() {
        let interrupt = Interrupt::default();
        assert!(!interrupt.request(1));
        assert!(!interrupt.cancelled());
    }

    #[test]
    fn active_interrupts_keep_the_first_signal_through_repeated_requests_and_cleanup() {
        let interrupt = Interrupt::default();
        let active = interrupt.arm();
        assert!(interrupt.request(1));
        assert!(interrupt.request(2));
        assert_eq!(interrupt.signal(), Some(1));
        drop(active);
        assert!(!interrupt.request(2));
        assert_eq!(interrupt.signal(), Some(1));
    }

    #[test]
    fn finishing_without_interruption_leaves_no_active_run() {
        let interrupt = Interrupt::default();
        drop(interrupt.arm());
        assert!(!interrupt.request(1));
        assert!(!interrupt.cancelled());
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "invoked as a subprocess by the Windows termination test"]
    fn windows_termination_fixture() {
        terminate(CONTROL_C_EXIT);
    }

    #[cfg(windows)]
    #[test]
    fn windows_termination_keeps_the_native_control_c_status() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "interrupt::tests::windows_termination_fixture", "--ignored"])
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(CONTROL_C_EXIT));
    }
}
