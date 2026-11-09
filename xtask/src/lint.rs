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
mod workspace;
mod workspace_dependencies;

#[cfg(test)]
mod workspace_dependencies_test;

pub(crate) fn run(custom_only: bool, json: bool) -> Result<ExitCode, Box<dyn Error>> {
    let workspace = workspace::load()?;
    let diagnostics = workspace_dependencies::check(&workspace);
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
    let mut command = Command::new(env!("CARGO"));
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
