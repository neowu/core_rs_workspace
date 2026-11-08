use std::env;
use std::error::Error;
use std::io;
use std::io::Write as _;
use std::process::ExitCode;

mod lint;

fn main() -> Result<ExitCode, Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    match args.as_slice() {
        [command] if command == "lint" => lint::run(false),
        [command, option] if command == "lint" && option == "--custom-only" => lint::run(true),
        [option] | [_, option] if option == "--help" || option == "-h" => {
            writeln!(io::stdout(), "Usage: cargo lint [--custom-only]")?;
            Ok(ExitCode::SUCCESS)
        }
        _ => Err("Usage: cargo lint [--custom-only]".into()),
    }
}
