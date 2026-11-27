use crate::cloud::Target;
use crate::standalone::Standalone;
use serde::Deserialize;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

const FILE: &str = "litmus.toml";

#[derive(Deserialize, Default, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    cloud: Option<Target>,
    standalone: Option<Standalone>,
    rojo: Option<Rojo>,
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
struct Rojo {
    project: PathBuf,
}

impl Config {
    pub fn read(project_dir: &Path) -> Result<Config, String> {
        match fs::read_to_string(project_dir.join(FILE)) {
            Ok(text) => Config::parse(&text),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(Config::default()),
            Err(error) => Err(format!("couldn't read {FILE}: {error}")),
        }
    }

    fn parse(text: &str) -> Result<Config, String> {
        let config: Config = toml::from_str(text).map_err(|error| format!("couldn't read {FILE}: {error}"))?;

        if let Some(standalone) = &config.standalone {
            standalone
                .check()
                .map_err(|problem| format!("{FILE} has a [standalone] table that doesn't add up: {problem}"))?;
        }

        if let Some(rojo) = &config.rojo
            && rojo.project.components().count() != 1
        {
            return Err(format!(
                "{FILE} gives \"{}\" as the Rojo project, which has to be a file in the project's own folder",
                rojo.project.display()
            ));
        }

        Ok(config)
    }

    pub fn rojo_project(&self) -> Option<PathBuf> {
        self.rojo.as_ref().map(|rojo| rojo.project.clone())
    }

    pub fn cloud(self) -> Result<Target, String> {
        self.cloud.ok_or_else(|| {
            format!(
                "{FILE} names no place for a cloud run; \
                 it needs a [cloud] table with the experience as `universe` and its place as `place`"
            )
        })
    }

    pub fn standalone(self) -> Result<Standalone, String> {
        self.standalone.ok_or_else(|| {
            format!(
                "{FILE} names no command to run the tests with; it needs a [standalone] table with \
                 the command that runs a Luau file as `run`, the folder of litmus's lib as `lib`, \
                 where to look for .spec files as `specs`, and the whole-process limit in seconds as `timeout` (0 disables it)"
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    const STANDALONE: &str = "[standalone]\nrun = [\"lune\", \"run\"]\nlib = \"lib\"\nspecs = [\"tests\"]\ntimeout = 300\n";

    #[test]
    fn reads_where_a_cloud_run_happens() {
        let config = Config::parse("[cloud]\nuniverse = 11\nplace = 22\n").unwrap();

        assert_eq!(
            config.cloud(),
            Ok(Target {
                universe: 11,
                place: 22,
                build: None,
            })
        );
    }

    #[test]
    fn reads_the_place_file_a_cloud_run_uploads() {
        let config = Config::parse("[cloud]\nuniverse = 11\nplace = 22\nbuild = \"build.rbxl\"\n").unwrap();

        assert_eq!(config.cloud().unwrap().build, Some(PathBuf::from("build.rbxl")));
    }

    #[test]
    fn a_project_without_the_file_has_no_place_for_a_cloud_run() {
        let nowhere = Path::new("a folder that doesn't exist");

        assert_eq!(Config::read(nowhere), Ok(Config::default()));
        assert!(Config::default().cloud().unwrap_err().contains("[cloud]"));
        assert!(Config::parse("").unwrap().cloud().is_err());
    }

    #[test]
    fn refuses_a_place_it_could_not_run_in() {
        assert!(Config::parse("[cloud]\nuniverse = 11\n").is_err());
        assert!(Config::parse("[cloud]\nuniverse = \"eleven\"\nplace = 22\n").is_err());
        assert!(Config::parse("[cloud]\nuniverse = -11\nplace = 22\n").is_err());
    }

    #[test]
    fn refuses_a_name_it_does_not_know() {
        assert!(Config::parse("[cloud]\nuniverse = 11\nplace = 22\nplaec = 33\n").is_err());
        assert!(Config::parse("[clowd]\nuniverse = 11\nplace = 22\n").is_err());
        assert!(Config::parse(&format!("{STANDALONE}adaptor = \"adapter.luau\"\n")).is_err());
    }

    #[test]
    fn reads_the_rojo_project_a_place_is_built_from() {
        let config = Config::parse("[rojo]\nproject = \"place.project.json\"\n").unwrap();

        assert_eq!(config.rojo_project(), Some(PathBuf::from("place.project.json")));
        assert_eq!(Config::default().rojo_project(), None);
    }

    #[test]
    fn refuses_a_rojo_project_it_could_not_place_files_from() {
        assert!(Config::parse("[rojo]\n").is_err());
        assert!(Config::parse("[rojo]\nproject = \"place.project.json\"\nbuild = true\n").is_err());

        let problem = Config::parse("[rojo]\nproject = \"projects/place.project.json\"\n").unwrap_err();
        assert!(problem.contains("projects/place.project.json"), "{problem}");
    }

    #[test]
    fn reads_the_command_that_runs_the_tests() {
        let config = Config::parse(STANDALONE).unwrap();

        assert_eq!(
            config.standalone(),
            Ok(Standalone {
                run: vec!["lune".to_owned(), "run".to_owned()],
                lib: "lib".to_owned(),
                specs: vec!["tests".to_owned()],
                adapter: None,
                timeout: 300,
                flags: vec![],
                specs_at_once: None,
            })
        );
    }

    #[test]
    fn requires_an_explicit_standalone_timeout() {
        let text = STANDALONE.replace("timeout = 300\n", "");
        let problem = Config::parse(&text).unwrap_err();

        assert!(problem.contains("timeout"), "{problem}");
    }

    #[test]
    fn reads_positive_and_disabled_standalone_timeouts() {
        for timeout in [0, 1, 300, 900] {
            let text = STANDALONE.replace("timeout = 300", &format!("timeout = {timeout}"));
            let standalone = Config::parse(&text).unwrap().standalone().unwrap();

            assert_eq!(standalone.timeout, timeout);
        }
    }

    #[test]
    fn refuses_invalid_standalone_timeouts() {
        for timeout in ["-1", "1.5", "\"900\"", "true", "[]", "18446744073709551616"] {
            let text = STANDALONE.replace("timeout = 300", &format!("timeout = {timeout}"));
            let problem = Config::parse(&text).unwrap_err();

            assert!(problem.contains("timeout"), "{timeout} produced {problem}");
        }
    }

    #[test]
    fn standalone_timeouts_follow_platform_clock_limits() {
        for timeout in [i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
            let text = STANDALONE.replace("timeout = 300", &format!("timeout = {timeout}"));
            let parsed = Config::parse(&text);
            let fits = std::time::Instant::now().checked_add(std::time::Duration::from_secs(timeout)).is_some();
            if fits {
                assert_eq!(parsed.unwrap().standalone().unwrap().timeout, timeout);
            } else {
                let problem = parsed.unwrap_err();
                assert!(problem.contains("timeout"), "{problem}");
            }
        }
    }

    #[cfg(windows)]
    #[test]
    fn reads_an_unsigned_timeout_above_the_signed_toml_range_on_windows() {
        let timeout = i64::MAX as u64 + 1;
        assert!(std::time::Instant::now().checked_add(std::time::Duration::from_secs(timeout)).is_some());
        let text = STANDALONE.replace("timeout = 300", &format!("timeout = {timeout}"));

        assert_eq!(Config::parse(&text).unwrap().standalone().unwrap().timeout, timeout);
    }

    #[test]
    fn reads_the_adapter_and_the_flags_a_project_adds() {
        let text = format!("{STANDALONE}adapter = \"tools/adapter.luau\"\nflags = [\"coverage\"]\n");
        let standalone = Config::parse(&text).unwrap().standalone().unwrap();

        assert_eq!(standalone.adapter, Some("tools/adapter.luau".to_owned()));
        assert_eq!(standalone.flags, ["coverage"]);
    }

    #[test]
    fn reads_how_many_specs_may_run_at_once() {
        let text = format!("{STANDALONE}specs_at_once = 2\n");
        assert_eq!(Config::parse(&text).unwrap().standalone().unwrap().specs_at_once, Some(2));

        let problem = Config::parse(&format!("{STANDALONE}specs_at_once = 0\n")).unwrap_err();
        assert!(problem.contains("specs_at_once"), "{problem}");
    }

    #[test]
    fn a_project_without_the_table_has_no_command_to_run() {
        assert!(Config::default().standalone().unwrap_err().contains("[standalone]"));
        assert!(Config::parse("[cloud]\nuniverse = 11\nplace = 22\n").unwrap().standalone().is_err());
    }

    #[test]
    fn refuses_a_command_it_could_not_run() {
        let missing = STANDALONE.replace("run = [\"lune\", \"run\"]\n", "");
        assert!(Config::parse(&missing).unwrap_err().contains("missing field `run`"));

        let empty = STANDALONE.replace("run = [\"lune\", \"run\"]", "run = []");
        assert!(Config::parse(&empty).unwrap_err().contains("`run` has to start with the command"));

        let string = STANDALONE.replace("run = [\"lune\", \"run\"]", "run = \"lune run\"");
        assert!(Config::parse(&string).unwrap_err().contains("expected a sequence"));

        let problem = Config::parse(&format!("{STANDALONE}flags = [\"cloud\"]\n")).unwrap_err();
        assert!(problem.contains("[standalone]") && problem.contains("\"cloud\""), "{problem}");
    }
}
