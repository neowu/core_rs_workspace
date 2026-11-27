use std::fmt;
use std::path::PathBuf;

use serde_json::Value;
use serde_json::json;

pub(super) struct Diagnostic {
    /// Manifest or source file path relative to the workspace root, as rustc reports file names.
    pub(super) path: PathBuf,
    pub(super) manifest_path: PathBuf,
    pub(super) package_id: String,
    pub(super) target: Value,
    pub(super) span: Span,
    pub(super) rule: &'static str,
    pub(super) message: String,
}

pub(super) struct Span {
    pub(super) byte_start: usize,
    pub(super) byte_end: usize,
    pub(super) line_start: usize,
    pub(super) line_end: usize,
    pub(super) column_start: usize,
    pub(super) column_end: usize,
    pub(super) lines: Vec<SpanLine>,
}

pub(super) struct SpanLine {
    pub(super) text: String,
    pub(super) highlight_start: usize,
    pub(super) highlight_end: usize,
}

impl Diagnostic {
    /// Same shape as a `cargo --message-format=json` compiler message, so tools reading clippy output can read it.
    pub(super) fn to_json(&self) -> Value {
        let span = &self.span;
        let text: Vec<Value> = span
            .lines
            .iter()
            .map(|line| {
                json!({
                    "text": line.text,
                    "highlight_start": line.highlight_start,
                    "highlight_end": line.highlight_end,
                })
            })
            .collect();
        json!({
            "reason": "compiler-message",
            "package_id": self.package_id,
            "manifest_path": self.manifest_path,
            "target": self.target,
            "message": {
                "$message_type": "diagnostic",
                "message": self.message,
                "code": { "code": self.rule, "explanation": null },
                "level": "error",
                "spans": [{
                    "file_name": self.path,
                    "byte_start": span.byte_start,
                    "byte_end": span.byte_end,
                    "line_start": span.line_start,
                    "line_end": span.line_end,
                    "column_start": span.column_start,
                    "column_end": span.column_end,
                    "is_primary": true,
                    "text": text,
                    "label": null,
                    "suggested_replacement": null,
                    "suggestion_applicability": null,
                    "expansion": null,
                }],
                "children": [],
                "rendered": format!("{self}\n"),
            },
        })
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "error[{}]: {}\n  --> {}:{}:{}",
            self.rule,
            self.message,
            self.path.display(),
            self.span.line_start,
            self.span.column_start
        )
    }
}
