//! The record file: one `key=value` line per run or per profiled hotspot.
//!
//! The text file is the data and the html is a view of it, so appending never loses anything and
//! the records stay greppable and diffable. Values never contain spaces, except `name` on a
//! hotspot line, which is why that field always goes last.

use std::fmt::Write as _;
use std::fs::OpenOptions;
use std::fs::create_dir_all;
use std::fs::read_to_string;
use std::io::Write as _;
use std::path::Path;
use std::process::Command;

pub struct Record {
    pub kind: String,
    fields: Vec<(String, String)>,
}

impl Record {
    pub fn get(&self, key: &str) -> &str {
        self.fields.iter().find(|(k, _)| k == key).map_or("", |(_, v)| v.as_str())
    }

    pub fn number(&self, key: &str) -> f64 {
        self.get(key).parse().unwrap_or(0.0)
    }

    fn parse(line: &str) -> Option<Self> {
        let (kind, mut rest) = line.trim_end().split_once(' ')?;
        let mut fields = Vec::new();
        loop {
            rest = rest.trim_start();
            if rest.is_empty() {
                break;
            }
            // the last field may hold spaces, so it swallows the remainder of the line
            let token = if rest.starts_with("name=") { rest } else { rest.split(' ').next()? };
            if let Some((key, value)) = token.split_once('=') {
                fields.push((key.to_owned(), value.to_owned()));
            }
            rest = &rest[token.len()..];
        }
        Some(Self { kind: kind.to_owned(), fields })
    }
}

pub fn read(path: &Path) -> Vec<Record> {
    let text = read_to_string(path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    text.lines().filter(|line| !line.trim().is_empty()).filter_map(Record::parse).collect()
}

pub fn append(path: &Path, lines: &[String]) {
    if let Some(dir) = path.parent() {
        create_dir_all(dir).expect("cannot create the report directory");
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|e| panic!("cannot append to {}: {e}", path.display()));
    for line in lines {
        writeln!(file, "{line}").expect("cannot write a record");
    }
}

/// A `run` line: what the caller measured, prefixed with what only the host knows.
///
/// The environment fields are collected here rather than by the caller so that every run records
/// them the same way, whoever writes the record.
pub fn run_line(time: &str, fields: &[String]) -> String {
    let mut line = format!("run time={time} host={} cores={} os={} commit={}", host(), cores(), os(), commit());
    for field in fields {
        let _ = write!(line, " {field}");
    }
    line
}

fn host() -> String {
    output("hostname", &["-s"]).unwrap_or_else(|| "unknown".to_owned())
}

fn cores() -> String {
    std::thread::available_parallelism().map_or_else(|_| "0".to_owned(), |n| n.to_string())
}

fn os() -> String {
    // underscores keep it one space free token, the report turns them back into spaces
    output("uname", &["-sr"]).map_or_else(|| "unknown".to_owned(), |s| s.replace(' ', "_"))
}

fn commit() -> String {
    output("git", &["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "none".to_owned())
}

fn output(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8(out.stdout).ok()?.trim().to_owned();
    if text.is_empty() { None } else { Some(text) }
}
