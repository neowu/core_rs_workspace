use std::error::Error;
use std::process::ExitCode;

use lint::LintConfig;

fn main() -> Result<ExitCode, Box<dyn Error>> {
    lint::run(&LintConfig::default())
}
