mod cloud;
mod config;
mod interrupt;
mod luau;
mod manifest;
mod piece;
mod plugin;
mod progress;
mod render;
mod results;
mod rojo;
mod selection;
mod server;
mod show;
mod standalone;
mod style;
#[cfg(test)]
mod testing;

use config::Config;
use interrupt::Interrupt;
use manifest::Manifest;
use render::Sources;
use results::Run;
use selection::Selection;
use serde::Deserialize;
use serde_json::Value;
use std::env;
use std::io::{self, IsTerminal};
use std::path::Path;
use std::process::ExitCode;
use terminal_size::{Width, terminal_size_of};

const VERSION: &str = env!("CARGO_PKG_VERSION");

const HELP: &str = "\
Runs a project's tests, in Roblox Studio or wherever litmus.toml says

Usage: litmus <command>

Commands:
  test [paths]        Run the project's .spec files, wherever the options for test say; with paths, only the
                      ones at or under those files and folders
  plugin install      Install the Studio plugin, replacing any copy installed before
  plugin uninstall    Remove the Studio plugin

Options for test:
  --plugin            Run in an open Studio place, through the plugin, as without any of these three
  --cloud             Run in Roblox's cloud, in the place litmus.toml names, which the project is built into as a
                      new unpublished version
  --standalone        Run in a runtime on this machine, started by the command litmus.toml gives; from a terminal,
                      the results then stay on offer for three seconds, for any open Studio place to show
  --<flag>            A flag of the project's own for its adapter, with --standalone

litmus.toml:
  [rojo]              The Rojo project a plugin or cloud run reads the place from, as `project`, when it isn't
                      the default one
  [cloud]             The experience and its place, as `universe` and `place`; and as `build`, a .rbxl to use as
                      the new version, when Rojo alone doesn't build the whole place
  [standalone]        The command that runs a Luau file, as `run`; the folder of litmus's lib, as `lib`; where to
                      look for .spec files, as `specs`; and the required whole-runtime limit in seconds, as
                      `timeout` (300 keeps the old 5-minute limit; 0 disables execution expiry, not cleanup)
                      Optional `specs_at_once` limits concurrent specs, not runtime duration. For a project with
                      an adapter of its own, that file is `adapter` and the flags it takes are `flags`.
                      `timeout` is separate from `specs_at_once` and adapter flags, not a command-line flag.

A cloud run also needs:
  LITMUS_API_KEY      The Open Cloud API key to run under, read from the environment, or else from the project's .env

Options:
  -h, --help          Print this help
  -V, --version       Print the version
";

#[derive(Debug, PartialEq)]
enum Backend {
    Plugin,
    Cloud,
    Standalone,
}

impl Backend {
    fn named(name: &str) -> Option<Backend> {
        match name {
            "plugin" => Some(Backend::Plugin),
            "cloud" => Some(Backend::Cloud),
            "standalone" => Some(Backend::Standalone),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq)]
struct TestCommand {
    backend: Backend,
    flags: Vec<String>,
    paths: Vec<String>,
}

impl TestCommand {
    fn parse(options: &[&str]) -> Result<TestCommand, String> {
        let mut backend: Option<&str> = None;
        let mut flags = Vec::new();
        let mut paths = Vec::new();

        for option in options {
            let Some(name) = option.strip_prefix("--") else {
                if option.starts_with('-') {
                    return Err(format!("{option} isn't an option; litmus's options and a project's flags start with --"));
                }

                paths.push((*option).to_owned());
                continue;
            };

            match Backend::named(name) {
                Some(_) if let Some(first) = backend => {
                    return Err(format!("--{first} and --{name} each pick a backend, and a run takes one"));
                }
                Some(_) => backend = Some(name),
                None if name.is_empty() => return Err("-- names no flag".to_owned()),
                None => flags.push(name.to_owned()),
            }
        }

        Ok(TestCommand { backend: backend.and_then(Backend::named).unwrap_or(Backend::Plugin), flags, paths })
    }
}

struct Ran {
    results: Value,
    sources: Sources,
}

#[derive(Debug, PartialEq)]
enum Command {
    Test(TestCommand),
    PluginInstall,
    PluginUninstall,
    Help,
    Version,
}

impl Command {
    fn parse(arguments: &[impl AsRef<str>]) -> Result<Command, String> {
        let words: Vec<&str> = arguments.iter().map(AsRef::as_ref).collect();

        if words.iter().any(|word| matches!(*word, "--help" | "-h")) {
            return Ok(Command::Help);
        }

        if words.iter().any(|word| matches!(*word, "--version" | "-V")) {
            return Ok(Command::Version);
        }

        match words.as_slice() {
            ["test", options @ ..] => TestCommand::parse(options).map(Command::Test),
            ["plugin", "install"] => Ok(Command::PluginInstall),
            ["plugin", "uninstall"] => Ok(Command::PluginUninstall),
            ["plugin", ..] => Err("plugin takes install or uninstall".to_owned()),
            [] => Err("no command was given".to_owned()),
            [word, ..] => Err(format!("{word} isn't a command")),
        }
    }

    fn run(self, interrupt: &Interrupt) -> Result<bool, String> {
        match self {
            Command::Test(command) => run_tests(&command, interrupt),
            Command::PluginInstall => plugin::install().and(Ok(true)),
            Command::PluginUninstall => plugin::uninstall().and(Ok(true)),
            Command::Help => {
                print!("{HELP}");
                Ok(true)
            }
            Command::Version => {
                println!("litmus {VERSION}");
                Ok(true)
            }
        }
    }
}

fn main() -> ExitCode {
    let arguments: Vec<String> = env::args().skip(1).collect();

    let command = match Command::parse(&arguments) {
        Ok(command) => command,
        Err(problem) => {
            eprintln!("litmus: {problem}; litmus --help lists the commands and options");
            return ExitCode::from(2);
        }
    };

    dispatch(command)
}

fn dispatch(command: Command) -> ExitCode {
    let interrupt = Interrupt::default();
    if matches!(&command, Command::Test(command) if command.backend == Backend::Standalone)
        && let Err(error) = interrupt.install()
    {
        eprintln!("litmus: couldn't install the interrupt handler: {error}");
        return ExitCode::from(2);
    }

    let outcome = command.run(&interrupt);
    if let Some(signal) = interrupt.signal() {
        if let Err(message) = &outcome {
            eprintln!("litmus: {message}");
        }
        interrupt::terminate(signal);
    }

    match outcome {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(message) => {
            eprintln!("litmus: {message}");
            ExitCode::from(2)
        }
    }
}

fn run_tests(command: &TestCommand, interrupt: &Interrupt) -> Result<bool, String> {
    let project_dir = Path::new(".");

    if command.backend != Backend::Standalone
        && let Some(flag) = command.flags.first()
    {
        return Err(format!("--{flag} isn't an option of litmus; a flag of the project's own goes with --standalone"));
    }

    let selection = Selection::of(&command.paths)?;

    let ran = match command.backend {
        Backend::Plugin => run_with_plugin(project_dir, &selection)?,
        Backend::Cloud => run_in_cloud(project_dir, &selection)?,
        Backend::Standalone => run_standalone(project_dir, &command.flags, &selection, interrupt)?,
    };

    let run = Run::deserialize(&ran.results)
        .map_err(|error| format!("couldn't read the results, so the project's lib may not match this CLI: {error}"))?;

    let columns = terminal_size_of(io::stdout()).map(|(Width(columns), _)| usize::from(columns));

    anstream::print!("{}", render::run(&run, &ran.sources, columns));

    if command.backend == Backend::Standalone && io::stdout().is_terminal() {
        show::offer(project_dir, &ran.results, &run);
    }

    Ok(run.passed())
}

fn run_standalone(project_dir: &Path, flags: &[String], selection: &Selection, interrupt: &Interrupt) -> Result<Ran, String> {
    let standalone = Config::read(project_dir)?.standalone()?;

    Ok(Ran { results: standalone::run(project_dir, &standalone, flags, selection, interrupt)?, sources: Sources::Disk })
}

fn run_with_plugin(project_dir: &Path, selection: &Selection) -> Result<Ran, String> {
    let config = Config::read(project_dir)?;
    let manifest = manifest_of(project_dir, &config, selection)?;

    Ok(Ran { results: plugin::run(&manifest)?, sources: Sources::of(&manifest) })
}

fn run_in_cloud(project_dir: &Path, selection: &Selection) -> Result<Ran, String> {
    let config = Config::read(project_dir)?;
    let manifest = manifest_of(project_dir, &config, selection)?;

    let rojo_project = config.rojo_project();
    let target = config.cloud()?;

    Ok(Ran { results: cloud::run(project_dir, &manifest, &target, rojo_project.as_deref())?, sources: Sources::of(&manifest) })
}

fn manifest_of(project_dir: &Path, config: &Config, selection: &Selection) -> Result<Manifest, String> {
    let mut manifest = Manifest::read(project_dir, config.rojo_project().as_deref())?;
    if manifest.specs().is_empty() {
        return Err("the project has no .spec files".to_owned());
    }

    manifest.narrow(selection)?;

    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_on(backend: Backend, flags: &[&str]) -> Result<Command, String> {
        test_of(backend, flags, &[])
    }

    fn test_of(backend: Backend, flags: &[&str], paths: &[&str]) -> Result<Command, String> {
        Ok(Command::Test(TestCommand {
            backend,
            flags: flags.iter().map(|flag| (*flag).to_owned()).collect(),
            paths: paths.iter().map(|path| (*path).to_owned()).collect(),
        }))
    }

    #[test]
    fn reads_each_command() {
        assert_eq!(Command::parse(&["test"]), test_on(Backend::Plugin, &[]));
        assert_eq!(Command::parse(&["plugin", "install"]), Ok(Command::PluginInstall));
        assert_eq!(Command::parse(&["plugin", "uninstall"]), Ok(Command::PluginUninstall));
        assert_eq!(Command::parse(&["--help"]), Ok(Command::Help));
        assert_eq!(Command::parse(&["-h"]), Ok(Command::Help));
        assert_eq!(Command::parse(&["--version"]), Ok(Command::Version));
        assert_eq!(Command::parse(&["-V"]), Ok(Command::Version));
    }

    #[test]
    fn reads_help_and_version_wherever_they_are() {
        assert_eq!(Command::parse(&["test", "--help"]), Ok(Command::Help));
        assert_eq!(Command::parse(&["test", "--standalone", "--coverage", "-h"]), Ok(Command::Help));
        assert_eq!(Command::parse(&["plugin", "--version"]), Ok(Command::Version));
        assert_eq!(Command::parse(&["run", "-V"]), Ok(Command::Version));
    }

    #[test]
    fn says_why_it_refuses_anything_else() {
        assert_eq!(Command::parse(&[] as &[&str]), Err("no command was given".to_owned()));
        assert_eq!(Command::parse(&["run"]), Err("run isn't a command".to_owned()));
        assert_eq!(Command::parse(&["plugin"]), Err("plugin takes install or uninstall".to_owned()));
        assert_eq!(Command::parse(&["plugin", "remove"]), Err("plugin takes install or uninstall".to_owned()));
    }

    #[test]
    fn reads_the_paths_a_test_run_names() {
        assert_eq!(Command::parse(&["test", "tests/deep"]), test_of(Backend::Plugin, &[], &["tests/deep"]));
        assert_eq!(
            Command::parse(&["test", "a.spec.luau", "--standalone", "--coverage", "tests"]),
            test_of(Backend::Standalone, &["coverage"], &["a.spec.luau", "tests"])
        );
    }

    #[test]
    fn reads_the_backend_a_test_run_names() {
        assert_eq!(Command::parse(&["test", "--plugin"]), test_on(Backend::Plugin, &[]));
        assert_eq!(Command::parse(&["test", "--cloud"]), test_on(Backend::Cloud, &[]));
        assert_eq!(Command::parse(&["test", "--standalone"]), test_on(Backend::Standalone, &[]));
    }

    #[test]
    fn keeps_any_other_flag_for_the_project() {
        assert_eq!(
            Command::parse(&["test", "--coverage", "--standalone", "--full-trace"]),
            test_on(Backend::Standalone, &["coverage", "full-trace"])
        );
        assert_eq!(Command::parse(&["test", "--backend"]), test_on(Backend::Plugin, &["backend"]));
    }

    #[test]
    fn says_why_options_do_not_add_up() {
        let refused = |words: &[&str]| Command::parse(words).unwrap_err();

        assert_eq!(refused(&["test", "--cloud", "--standalone"]), "--cloud and --standalone each pick a backend, and a run takes one");
        assert_eq!(refused(&["test", "--plugin", "--plugin"]), "--plugin and --plugin each pick a backend, and a run takes one");
        assert!(refused(&["test", "-c"]).starts_with("-c isn't an option"));
        assert_eq!(refused(&["test", "--"]), "-- names no flag");
    }

    #[test]
    fn a_flag_of_the_project_only_goes_with_a_standalone_run() {
        let command = TestCommand { backend: Backend::Cloud, flags: vec!["coverage".to_owned()], paths: vec![] };

        let problem = run_tests(&command, &Interrupt::default()).unwrap_err();

        assert!(problem.contains("--coverage") && problem.contains("--standalone"), "{problem}");
    }
}
