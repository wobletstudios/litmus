mod harness;
mod process;
mod specs;

use crate::interrupt::Interrupt;
use crate::selection::{Selection, relative};
use serde::Deserialize;
use serde_json::Value;
use std::fs;
use std::path::Path;

const FOLDER: &str = ".litmus";
const SCRIPT: &str = "run.luau";
const MARKER: &str = "@@litmus-run@@";
const PROGRESS_MARKER: &str = "@@litmus-progress@@";

const OWN_OPTIONS: [&str; 5] = ["plugin", "cloud", "standalone", "help", "version"];

#[derive(Deserialize, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Standalone {
    pub run: Vec<String>,
    pub lib: String,
    pub specs: Vec<String>,
    pub adapter: Option<String>,
    pub timeout: u64,
    #[serde(default)]
    pub flags: Vec<String>,
    pub specs_at_once: Option<u32>,
}

impl Standalone {
    pub fn check(&self) -> Result<(), String> {
        process::deadline(std::time::Instant::now(), self.timeout)?;
        if self.run.first().is_none_or(String::is_empty) {
            return Err("`run` has to start with the command that runs a Luau file, as in [\"lune\", \"run\"]".to_owned());
        }

        for path in self.specs.iter() {
            relative(path)?;
        }

        if self.specs_at_once == Some(0) {
            return Err("`specs_at_once` has to let at least 1 spec run".to_owned());
        }

        for path in [&self.lib].into_iter().chain(&self.adapter) {
            if relative(path)?.is_empty() {
                return Err(format!("\"{path}\" has to name something inside the project's folder"));
            }
        }

        match self.flags.iter().find(|flag| !is_flag(flag)) {
            Some(flag) => Err(format!(
                "`flags` can't hold \"{flag}\"; a flag is lowercase letters, digits and dashes, \
                 and none of litmus's own options"
            )),
            None => Ok(()),
        }
    }
}

pub fn run(
    project_dir: &Path,
    standalone: &Standalone,
    flags: &[String],
    selection: &Selection,
    interrupt: &Interrupt,
) -> Result<Value, String> {
    if let Some(flag) = flags.iter().find(|flag| !standalone.flags.contains(flag)) {
        return Err(format!("--{flag} isn't a flag litmus.toml lists under [standalone]"));
    }

    let found = specs::find(project_dir, &standalone.specs)?;
    if found.is_empty() {
        return Err("the project has no .spec files where litmus.toml says to look".to_owned());
    }

    let specs = selection.narrow(found)?;

    let folder = project_dir.join(FOLDER);
    let script = harness::script(standalone, &specs, flags)?;

    fs::create_dir_all(&folder)
        .and_then(|()| fs::write(folder.join(SCRIPT), script))
        .map_err(|error| format!("couldn't write {FOLDER}/{SCRIPT}: {error}"))?;

    let reported = process::run(project_dir, &standalone.run, &specs, standalone.timeout, interrupt)?;

    serde_json::from_str(&reported).map_err(|error| format!("couldn't read what the run reported: {error}"))
}

fn is_flag(flag: &str) -> bool {
    let spelled = flag.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');

    spelled && !flag.is_empty() && !flag.starts_with('-') && !OWN_OPTIONS.contains(&flag)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::TempProject;

    pub(super) fn standalone() -> Standalone {
        Standalone {
            run: vec!["lune".to_owned(), "run".to_owned()],
            lib: "submodules/litmus/lib".to_owned(),
            specs: vec!["tests".to_owned()],
            adapter: None,
            timeout: 300,
            flags: vec!["coverage".to_owned()],
            specs_at_once: None,
        }
    }

    pub(super) fn project(name: &str, files: &[&str]) -> TempProject {
        let dir = TempProject::new(&format!("standalone-{name}"));

        for file in files {
            dir.write(file, "");
        }

        dir
    }

    #[test]
    fn accepts_a_table_that_adds_up() {
        assert_eq!(standalone().check(), Ok(()));
    }

    #[test]
    fn refuses_a_table_without_a_command() {
        assert!(Standalone { run: vec![], ..standalone() }.check().is_err());
        assert!(Standalone { run: vec![String::new()], ..standalone() }.check().is_err());
    }

    #[test]
    fn refuses_a_limit_that_lets_no_spec_run() {
        assert!(Standalone { specs_at_once: Some(0), ..standalone() }.check().is_err());
        assert_eq!(Standalone { specs_at_once: Some(1), ..standalone() }.check(), Ok(()));
    }

    #[test]
    fn refuses_paths_that_do_not_sit_in_the_project() {
        let lib = |lib: &str| Standalone { lib: lib.to_owned(), ..standalone() };

        assert!(lib("/opt/litmus").check().is_err());
        assert!(lib(".").check().is_err());
        assert!(Standalone { specs: vec!["D:\\tests".to_owned()], ..standalone() }.check().is_err());
        assert!(Standalone { adapter: Some("/adapter.luau".to_owned()), ..standalone() }.check().is_err());
        assert_eq!(Standalone { specs: vec![".".to_owned()], ..standalone() }.check(), Ok(()));
    }

    #[test]
    fn refuses_flags_it_could_not_tell_from_its_own_options() {
        let flags = |flag: &str| Standalone { flags: vec![flag.to_owned()], ..standalone() };

        assert_eq!(flags("with-dashes-2").check(), Ok(()));

        for flag in ["plugin", "cloud", "standalone", "help", "version", "", "Coverage", "--coverage", "-c", "two words"] {
            assert!(flags(flag).check().is_err(), "{flag}");
        }
    }

    #[test]
    fn refuses_a_flag_the_project_never_listed() {
        let problem = run(
            Path::new("a folder that doesn't exist"),
            &standalone(),
            &["verbose".to_owned()],
            &Selection::default(),
            &Interrupt::default(),
        )
        .unwrap_err();

        assert!(problem.contains("--verbose"), "{problem}");
    }

    #[cfg(unix)]
    #[test]
    fn a_run_is_what_the_command_printed_on_its_marked_line() {
        let dir = project("runs", &["tests/a.spec.luau"]);
        let standalone =
            Standalone { run: vec!["sh".to_owned(), "-c".to_owned(), "echo '@@litmus-run@@{\"specs\":[]}'".to_owned()], ..standalone() };

        let reported = run(&dir, &standalone, &[], &Selection::default(), &Interrupt::default());
        let written = fs::read_to_string(dir.join(".litmus/run.luau"));

        assert_eq!(reported, Ok(serde_json::json!({ "specs": [] })));
        assert!(written.unwrap().contains("\"tests/a.spec.luau\""));
    }

    #[cfg(unix)]
    #[test]
    fn a_run_is_over_once_it_reported_even_if_the_command_goes_on() {
        let dir = project("lingers", &["tests/a.spec.luau"]);
        let standalone = Standalone {
            run: vec!["sh".to_owned(), "-c".to_owned(), "echo '@@litmus-run@@{\"specs\":[]}'; exec sleep 60".to_owned()],
            ..standalone()
        };

        let started = std::time::Instant::now();
        let reported = run(&dir, &standalone, &[], &Selection::default(), &Interrupt::default());

        assert_eq!(reported, Ok(serde_json::json!({ "specs": [] })));
        assert!(started.elapsed() < std::time::Duration::from_secs(30), "{:?}", started.elapsed());
    }

    #[cfg(unix)]
    #[test]
    fn a_command_that_reports_nothing_is_an_error() {
        let dir = project("silent", &["tests/a.spec.luau"]);
        let standalone = Standalone { run: vec!["sh".to_owned(), "-c".to_owned(), "exit 3".to_owned()], ..standalone() };

        let problem = run(&dir, &standalone, &[], &Selection::default(), &Interrupt::default()).unwrap_err();

        assert!(problem.contains("without reporting any results"), "{problem}");
    }
}
