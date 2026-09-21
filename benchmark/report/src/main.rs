//! Records benchmark results and renders the html report.
//!
//! ```text
//! report run     <records.txt> <time> <key=value>...   append a run, then render
//! report hotspot <records.txt> <syms.json> <scenario> [top]   profile on stdin, append, render
//! report render  <records.txt>                         render only
//! ```
//!
//! Every subcommand renders, so the html beside the record file is never stale. `run` fills in the
//! host, core count, os and commit itself — the caller passes only what it measured.

mod hotspot;
mod html;
mod record;

use std::env;
use std::path::PathBuf;
use std::process::exit;

const USAGE: &str = "usage:
  report run     <records.txt> <time> <key=value>...
  report hotspot <records.txt> <syms.json> <scenario> [top]   # profile on stdin
  report render  <records.txt>";

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut args = args.iter().map(String::as_str);
    let (Some(command), Some(path)) = (args.next(), args.next()) else {
        eprintln!("{USAGE}");
        exit(2);
    };
    let records_path = PathBuf::from(path);

    match command {
        "run" => {
            let time = args.next().unwrap_or_else(|| fail("run needs a time"));
            let fields: Vec<String> = args.map(str::to_owned).collect();
            record::append(&records_path, &[record::run_line(time, &fields)]);
        }
        "hotspot" => {
            let syms = args.next().unwrap_or_else(|| fail("hotspot needs a syms.json"));
            let scenario = args.next().unwrap_or("unknown");
            let top = args.next().map_or(15, |value| value.parse().expect("invalid top"));
            record::append(&records_path, &hotspot::records(syms, scenario, top));
        }
        "render" => {}
        other => fail(&format!("unknown command `{other}`")),
    }

    let records = record::read(&records_path);
    if records.is_empty() {
        eprintln!("no records in {}", records_path.display());
        exit(1);
    }
    println!("report written to {}", html::render(&records_path, &records).display());
}

fn fail(message: &str) -> ! {
    eprintln!("{message}\n{USAGE}");
    exit(2);
}
