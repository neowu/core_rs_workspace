use std::env;
use std::error::Error;
use std::io;
use std::io::BufRead as _;
use std::io::BufReader;
use std::io::Write as _;
use std::path::Path;
use std::process::Command;
use std::process::ExitCode;
use std::process::Stdio;

use serde_json::Value;
use serde_json::json;

mod diagnostic;
mod file_length;
mod workspace;
mod workspace_dependencies;

const USAGE: &str = "Usage: cargo lint [--custom-only] [--message-format=human|json]";

pub struct LintConfig {
    /// Maximum lines per Rust source file, not counting top-level `#[cfg(test)]` items.
    pub max_file_lines: usize,
}

impl Default for LintConfig {
    fn default() -> Self {
        Self { max_file_lines: 500 }
    }
}

/// Entry point for the workspace's lint binary, which must be started by `cargo run` from a workspace member,
/// e.g. through the alias `lint = "run --quiet -p xtask -- lint"`.
pub fn run(config: &LintConfig) -> Result<ExitCode, Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        writeln!(io::stdout(), "{USAGE}")?;
        return Ok(ExitCode::SUCCESS);
    }
    let Some(("lint", options)) = args.split_first().map(|(command, options)| (command.as_str(), options)) else {
        return Err(USAGE.into());
    };
    let mut custom_only = false;
    let mut json = false;
    for option in options {
        match option.as_str() {
            "--custom-only" => custom_only = true,
            "--message-format=json" => json = true,
            "--message-format=human" => json = false,
            _ => return Err(format!("unknown option `{option}`\n{USAGE}").into()),
        }
    }
    lint(config, custom_only, json)
}

/// Read at runtime, since a library compiled from a git checkout cannot rely on its own build environment.
fn cargo() -> Command {
    Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
}

fn lint(config: &LintConfig, custom_only: bool, json: bool) -> Result<ExitCode, Box<dyn Error>> {
    let workspace = workspace::load()?;
    let mut diagnostics = workspace_dependencies::check(&workspace);
    diagnostics.extend(file_length::check(&workspace, config.max_file_lines)?);
    let custom_ok = diagnostics.is_empty();
    let clippy_ok = if json {
        let mut stdout = io::stdout().lock();
        for diagnostic in &diagnostics {
            writeln!(stdout, "{}", diagnostic.to_json())?;
        }
        let clippy_ok = custom_only || clippy_json(&workspace.root, &mut stdout)?;
        writeln!(stdout, "{}", json!({ "reason": "build-finished", "success": custom_ok && clippy_ok }))?;
        clippy_ok
    } else {
        {
            let mut stderr = io::stderr().lock();
            for diagnostic in &diagnostics {
                writeln!(stderr, "{diagnostic}\n")?;
            }
            if !custom_ok {
                writeln!(stderr, "{} custom lint error(s)", diagnostics.len())?;
            }
        }
        custom_only || clippy_command(&workspace.root).args(["--", "-D", "warnings"]).status()?.success()
    };
    Ok(if custom_ok && clippy_ok { ExitCode::SUCCESS } else { ExitCode::FAILURE })
}

fn clippy_command(root: &Path) -> Command {
    let mut command = cargo();
    command.args(["clippy", "--workspace", "--all-targets"]).current_dir(root);
    command
}

/// Forwards clippy's JSON messages except `build-finished`, which the caller emits for the combined result.
fn clippy_json(root: &Path, stdout: &mut impl io::Write) -> Result<bool, Box<dyn Error>> {
    let mut child =
        clippy_command(root).args(["--message-format=json", "--", "-D", "warnings"]).stdout(Stdio::piped()).spawn()?;
    let output = child.stdout.take().ok_or("clippy stdout is not captured")?;
    for line in BufReader::new(output).lines() {
        let line = line?;
        let finished = serde_json::from_str::<Value>(&line)
            .is_ok_and(|message| message.get("reason").and_then(Value::as_str) == Some("build-finished"));
        if !finished {
            writeln!(stdout, "{line}")?;
        }
    }
    Ok(child.wait()?.success())
}
