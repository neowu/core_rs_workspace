use std::fmt;
use std::path::PathBuf;

pub(super) struct Diagnostic {
    pub(super) path: PathBuf,
    pub(super) line: usize,
    pub(super) column: usize,
    pub(super) rule: &'static str,
    pub(super) message: String,
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}: error[{}]: {}", self.path.display(), self.line, self.column, self.rule, self.message)
    }
}
