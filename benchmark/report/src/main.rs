//! Annotates benchmark result files and renders the html report of their directory.
//!
//! ```text
//! report run     <result.json> [key=value]...                              add the run fields, then render
//! report profile <result.json> <self.txt> <total.txt> <top> [key=value]...  also add the top methods, then render
//! report render  <dir>
//! ```
//!
//! A directory holds one day of one benchmark, one client result file per run, and renders to the
//! html beside it. The fields the building host knows (time, commit, cargo profile) go under `run`.

mod html;
mod profile;

use std::env;
use std::fs::read_dir;
use std::fs::read_to_string;
use std::fs::write;
use std::path::Path;
use std::path::PathBuf;
use std::process::exit;

use serde_json::Map;
use serde_json::Value;

const USAGE: &str = "usage:
  report run     <result.json> [key=value]...
  report profile <result.json> <self.txt> <total.txt> <top> [key=value]...
  report render  <dir>";

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut args = args.iter().map(String::as_str);
    let (Some(command), Some(path)) = (args.next(), args.next()) else {
        fail("missing command");
    };
    let path = PathBuf::from(path);

    let dir = match command {
        "run" => {
            annotate(&path, args, None);
            parent(&path)
        }
        "profile" => {
            let (Some(self_report), Some(total_report), Some(top)) = (args.next(), args.next(), args.next()) else {
                fail("profile needs a self and a total perf report and a top");
            };
            let top = top.parse().unwrap_or_else(|_| fail("invalid top"));
            annotate(&path, args, Some(profile::methods(self_report, total_report, top)));
            parent(&path)
        }
        "render" => path,
        other => fail(&format!("unknown command `{other}`")),
    };

    let results = results(&dir);
    if results.is_empty() {
        eprintln!("no results in {}", dir.display());
        exit(1);
    }
    println!("report written to {}", html::render(&dir, &results).display());
}

fn annotate<'a>(path: &Path, fields: impl Iterator<Item = &'a str>, profile: Option<Value>) {
    let mut result = read(path);
    let run: Map<String, Value> = fields
        .map(|field| {
            let (key, value) = field.split_once('=').unwrap_or_else(|| fail(&format!("invalid field `{field}`")));
            (key.to_owned(), Value::String(value.to_owned()))
        })
        .collect();
    result["run"] = Value::Object(run);
    if let Some(profile) = profile {
        result["profile"] = profile;
    }
    let text = serde_json::to_string_pretty(&result).expect("cannot serialize result");
    write(path, text).unwrap_or_else(|e| panic!("cannot write {}: {e}", path.display()));
}

/// Sorted by file name, which the scripts start with the time of the run.
fn results(dir: &Path) -> Vec<Value> {
    let entries = read_dir(dir).unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()));
    let mut paths: Vec<PathBuf> = entries
        .map(|entry| entry.expect("cannot read directory entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    paths.sort();
    paths.iter().map(|path| read(path)).collect()
}

fn read(path: &Path) -> Value {
    let text = read_to_string(path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("cannot parse {}: {e}", path.display()))
}

fn parent(path: &Path) -> PathBuf {
    path.parent().filter(|dir| !dir.as_os_str().is_empty()).map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

fn fail(message: &str) -> ! {
    eprintln!("{message}\n{USAGE}");
    exit(2);
}
