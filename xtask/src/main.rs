use std::env;
use std::error::Error;
use std::io;
use std::io::Write as _;
use std::process::ExitCode;

mod lint;

const USAGE: &str = "Usage: cargo lint [--custom-only] [--message-format=human|json]";

fn main() -> Result<ExitCode, Box<dyn Error>> {
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
    lint::run(custom_only, json)
}
