use std::error::Error;
use std::process::ExitCode;

use framework_lint::LintConfig;

fn main() -> Result<ExitCode, Box<dyn Error>> {
    framework_lint::run(&LintConfig::default())
}
