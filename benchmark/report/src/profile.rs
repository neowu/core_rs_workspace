//! Turns a `perf report` of the server into the `profile` part of its result file, rendered as one
//! section below the runs.
//!
//! Two views: self time over everything the server ran, and total time (including callees) over
//! the framework and app methods only — the runtime and hyper frames above them would otherwise take
//! every row of the second table. perf samples only threads on a cpu, so nothing parked needs
//! excluding.

use std::fs::read_to_string;

use serde_json::Value;
use serde_json::json;

// crate paths a method must start from to be listed in the total time table
const OWN_CRATES: [&str; 3] = ["framework::", "http_test_server::", "nats_api_test_server::"];

struct Entry {
    total: f64,
    self_pct: f64,
    dso: String,
    name: String,
}

pub fn methods(self_path: &str, total_path: &str, top: usize) -> Value {
    let self_report = read(self_path);
    let total_report = read(total_path);
    // perf's own header, e.g. `# Samples: 16K of event 'task-clock:ppp'`
    let samples = self_report
        .lines()
        .find_map(|line| line.strip_prefix("# Samples: "))
        .and_then(|value| value.split_whitespace().next())
        .unwrap_or("unknown");

    let self_rows: Vec<Value> = merge(parse(&self_report, false), false)
        .iter()
        .take(top)
        .map(|entry| json!({ "pct": round(entry.self_pct), "dso": entry.dso, "name": entry.name }))
        .collect();
    let own = parse(&total_report, true)
        .into_iter()
        .filter(|entry| OWN_CRATES.iter().any(|prefix| entry.name.trim_start_matches('<').starts_with(prefix)));
    let total_rows: Vec<Value> = merge(own.collect(), true)
        .iter()
        .take(top)
        .map(|entry| json!({ "total_pct": round(entry.total), "self_pct": round(entry.self_pct), "name": entry.name }))
        .collect();
    json!({ "samples": samples, "self": self_rows, "total": total_rows })
}

fn round(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

/// `perf report --stdio --sort dso,symbol -g none` lines: the percentages (children first when
/// `children`), the dso, `[.]` or `[k]`, then the symbol. Symbols never hold spaces while mangled,
/// so everything after is perf's ipc columns.
fn parse(report: &str, children: bool) -> Vec<Entry> {
    report
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let mut tokens = line.split_whitespace();
            let first = percent(tokens.next()?)?;
            let (total, self_pct) = if children { (first, percent(tokens.next()?)?) } else { (first, first) };
            let dso = tokens.next()?.to_owned();
            let kernel = tokens.next()? == "[k]";
            let symbol = tokens.next()?;
            let name = if kernel { format!("{symbol} (kernel)") } else { simplify(symbol) };
            Some(Entry { total, self_pct, dso, name })
        })
        .collect()
}

/// Monomorphizations of one method collapse into one row once their generics are stripped. Self
/// time is disjoint and sums; total time can nest, so the largest is taken rather than a sum.
fn merge(entries: Vec<Entry>, children: bool) -> Vec<Entry> {
    let mut merged: Vec<Entry> = Vec::new();
    for entry in entries {
        if let Some(existing) = merged.iter_mut().find(|existing| existing.name == entry.name) {
            existing.self_pct += entry.self_pct;
            existing.total = if children { existing.total.max(entry.total) } else { existing.total + entry.total };
        } else {
            merged.push(entry);
        }
    }
    if children {
        merged.sort_by(|a, b| b.total.total_cmp(&a.total));
    } else {
        merged.sort_by(|a, b| b.self_pct.total_cmp(&a.self_pct));
    }
    merged
}

/// Demangled, without hashes, `.llvm.` suffixes or generic arguments. A `<` right after a name or
/// a `::` opens generic arguments and is dropped with everything up to its `>`; any other `<` is a
/// qualified path like `<T as Trait>::method` and is kept.
fn simplify(symbol: &str) -> String {
    let symbol = symbol.split(".llvm.").next().unwrap_or(symbol);
    let demangled = format!("{:#}", rustc_demangle::demangle(symbol));
    let mut out = String::with_capacity(demangled.len());
    let mut chars = demangled.chars();
    while let Some(c) = chars.next() {
        let turbofish = c == '<' && out.ends_with("::");
        if turbofish || (c == '<' && out.ends_with(|last: char| last.is_alphanumeric() || last == '_')) {
            if turbofish {
                out.truncate(out.len() - 2);
            }
            let (mut depth, mut previous) = (1, c);
            for inner in chars.by_ref() {
                match inner {
                    '<' => depth += 1,
                    // `->` in a closure signature is not a close
                    '>' if previous != '-' => depth -= 1,
                    _ => {}
                }
                if depth == 0 {
                    break;
                }
                previous = inner;
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn percent(token: &str) -> Option<f64> {
    token.strip_suffix('%')?.parse().ok()
}

fn read(path: &str) -> String {
    read_to_string(path).unwrap_or_else(|e| panic!("cannot read {path}: {e}"))
}
