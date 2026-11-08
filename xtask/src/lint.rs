use std::error::Error;
use std::io;
use std::io::Write as _;
use std::process::Command;
use std::process::ExitCode;

mod diagnostic;
mod workspace;
mod workspace_dependencies;

#[cfg(test)]
mod workspace_dependencies_test;

pub(crate) fn run(custom_only: bool) -> Result<ExitCode, Box<dyn Error>> {
    let workspace = workspace::load()?;
    let diagnostics = workspace_dependencies::check(&workspace);
    let mut stderr = io::stderr().lock();
    for diagnostic in &diagnostics {
        writeln!(stderr, "{diagnostic}")?;
    }
    if !diagnostics.is_empty() {
        writeln!(stderr, "{} custom lint error(s)", diagnostics.len())?;
    }
    let clippy_ok = custom_only
        || Command::new(env!("CARGO"))
            .args(["clippy", "--workspace", "--all-targets", "--", "-D", "warnings"])
            .current_dir(&workspace.root)
            .status()?
            .success();
    Ok(if diagnostics.is_empty() && clippy_ok { ExitCode::SUCCESS } else { ExitCode::FAILURE })
}
