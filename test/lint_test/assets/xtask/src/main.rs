use std::error::Error;
use std::process::ExitCode;

use lint::LintConfig;

fn main() -> Result<ExitCode, Box<dyn Error>> {
    lint::run(&LintConfig { api_tests: vec!["e2e"], ..LintConfig::default() })
}
