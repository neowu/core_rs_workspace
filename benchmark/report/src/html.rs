//! Renders a directory of result files into the html report beside it.
//!
//! Every result in the directory is rendered every time, so the html is always a complete view of
//! the day and regenerating it by hand is safe.

use std::fmt::Write as _;
use std::fs::write;
use std::path::Path;
use std::path::PathBuf;

use serde_json::Value;

const STYLE: &str = r"
:root{color-scheme:light dark;--bg:#fff;--fg:#1a1a1a;--dim:#666;--line:#e3e3e3;--head:#f7f7f7;--accent:#b35c00;--bar:#f0c9a0}
@media (prefers-color-scheme:dark){:root{--bg:#16181c;--fg:#e8e8e8;--dim:#9aa0a6;--line:#2c2f36;--head:#1e2126;--accent:#f0a458;--bar:#6b4520}}
*{box-sizing:border-box}body{margin:0;padding:24px 16px;background:var(--bg);color:var(--fg);
font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
main{max-width:1120px;margin:0 auto}h1{font-size:20px;margin:0 0 4px}
h2{font-size:16px;margin:28px 0 2px}h3{font-size:14px;margin:16px 0 6px}p.meta{color:var(--dim);margin:0 0 14px;font-size:13px}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}
th,td{padding:6px 7px;text-align:right;border-bottom:1px solid var(--line);white-space:nowrap;font-size:13px}
th{background:var(--head);font-weight:600;font-size:12px;color:var(--dim)}
th:first-child,td:first-child,td.s,td.m,th.m{text-align:left}td.s{font-weight:600}
td.t{color:var(--dim);font-size:12px}td.na{color:var(--dim)}td.n{color:var(--accent);font-weight:600}
td.m{font-family:ui-monospace,Menlo,monospace;font-size:12px;white-space:normal;word-break:break-word}
td.b{width:90px}.bar{height:9px;background:var(--bar);border-radius:2px;min-width:1px}
tbody tr:hover{background:var(--head)}.wrap{overflow-x:auto;border:1px solid var(--line);border-radius:6px}
footer{color:var(--dim);font-size:12px;margin-top:22px}footer code{font-size:12px}
";

const FOOTER: &str = r"<footer>
<p><b>req/s</b> measures client and server together, so it is only comparable within a run group.
<b>cpu µs/req</b> is the server process alone over the measured phase, and is the number to compare.
<b>server / client cpu %</b> is each process's cpu time over the measured phase as a share of all its host's cores;
the side closer to 100 is the one holding the rate down.</p>
<p>Runs and profiles come from <code>benchmark/remote.sh</code>, rendered by <code>benchmark/report</code> from the
result files in the directory beside this one. A profile adds no run row, the profiler skews throughput and cpu,
compare a profiling run only with other profiling runs.</p>
</footer></main></body></html>";

pub fn render(dir: &Path, results: &[Value]) -> PathBuf {
    // the directory is named <date>_<benchmark>, e.g. 2026-09-23_http, which is the whole title
    let file = dir.file_name().unwrap_or_default().to_string_lossy().into_owned();
    let date = file.get(..10).unwrap_or(&file);
    let target = file.get(11..).map_or_else(|| "benchmark".to_owned(), |name| name.replace('_', " "));

    let (profiles, runs): (Vec<&Value>, Vec<&Value>) = results.iter().partition(|r| r.get("profile").is_some());
    let mut page = String::new();

    let _ = write!(
        page,
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{target} {date}</title>\n<style>{STYLE}</style></head><body><main>\n\
         <h1>{target} — {date}</h1>\n\
         <p class=\"meta\">{} run(s) · {} profile(s)</p>\n",
        runs.len(),
        profiles.len()
    );

    // one line per distinct pair of machines and build, a day may span more than one
    let mut lines: Vec<String> = Vec::new();
    for run in &runs {
        let line = machines(run);
        if !lines.contains(&line) {
            lines.push(line);
        }
    }
    for line in &lines {
        let _ = writeln!(page, "<p class=\"meta\">{line}</p>");
    }

    if !runs.is_empty() {
        runs_table(&mut page, &runs);
    }
    for profile in &profiles {
        profile_section(&mut page, profile);
    }
    page.push_str(FOOTER);
    page.push('\n');

    let html = dir.with_extension("html");
    write(&html, page).unwrap_or_else(|e| panic!("cannot write {}: {e}", html.display()));
    html
}

fn runs_table(page: &mut String, runs: &[&Value]) {
    page.push_str(
        "<div class=\"wrap\"><table><thead><tr>\n\
         <th>time</th><th>scenario</th><th>proto</th><th>conc</th><th>client thr</th><th>server thr</th><th>dur</th>\n\
         <th>req/s</th><th>p50 ms</th><th>p99 ms</th><th>p99.9 ms</th>\n\
         <th>cpu µs/req</th><th>server cpu %</th><th>client cpu %</th><th>peak rss MB</th>\n\
         </tr></thead><tbody>\n",
    );
    let mut bad = 0;
    for run in runs {
        let _ = write!(
            page,
            "<tr><td class=\"t\">{}</td><td class=\"s\">{}</td><td>{}</td>",
            esc(get(run, "/run/time").get(11..).unwrap_or("")),
            esc(&get(run, "/config/scenario")),
            esc(&get(run, "/config/protocol")),
        );
        for (key, unit) in
            [("/config/concurrency", ""), ("/config/threads", ""), ("/server/threads", ""), ("/config/duration", "s")]
        {
            page.push_str(&cell(run, key, unit));
        }
        let _ = write!(page, "<td class=\"n\">{}</td>", esc(&get(run, "/result/throughput")));
        for key in ["/result/p50_ms", "/result/p99_ms", "/result/p999_ms"] {
            page.push_str(&cell(run, key, ""));
        }
        let _ = write!(page, "<td class=\"n\">{}</td>", esc(&get(run, "/server/cpu_us_per_request")));
        page.push_str(&cell(run, "/server/cpu_pct", ""));
        page.push_str(&cell(run, "/client/cpu_pct", ""));
        page.push_str(&cell(run, "/server/peak_rss_mb", ""));
        page.push_str("</tr>\n");
        if number(run, "/result/failed") + number(run, "/result/errors") > 0.0 {
            bad += 1;
        }
    }
    page.push_str("</tbody></table></div>\n");
    if bad > 0 {
        let _ = writeln!(page, "<p class=\"meta\">{bad} run(s) recorded failed or errored requests.</p>");
    }
}

/// Server and client machines, the build, as one line.
fn machines(result: &Value) -> String {
    let field = |pointer: &str| esc(&get(result, pointer));
    let mut line = format!(
        "server {} ({}) · {} cores · {} · {} MB · {} · client {} ({}) · {} cores",
        field("/server/machine/host"),
        field("/server/machine/ip"),
        field("/server/machine/cores"),
        field("/server/machine/cpu"),
        field("/server/machine/memory_mb"),
        field("/server/machine/os"),
        field("/client/machine/host"),
        field("/client/machine/ip"),
        field("/client/machine/cores"),
    );
    // only the nats benchmark has a broker between client and server
    if result.get("broker").is_some() {
        let _ = write!(line, " · nats {} at {}", field("/broker/version"), field("/broker/url"));
    }
    let _ = write!(line, " · commit {} · cargo profile {}", field("/run/commit"), field("/run/profile"));
    line
}

fn profile_section(page: &mut String, result: &Value) {
    let field = |pointer: &str| esc(&get(result, pointer));
    let _ = writeln!(
        page,
        "<h2>Top methods — {} · {}</h2>",
        esc(get(result, "/run/time").get(11..).unwrap_or("")),
        field("/config/scenario")
    );
    let _ = writeln!(
        page,
        "<p class=\"meta\">{}<br>{} s warmup + {} s measured · {} concurrent {} streams · {} client threads · \
         {} server threads · {} req/s · p99 {} ms · {} µs server cpu / request · \
         server cpu {}% · client cpu {}% · {} perf samples</p>",
        machines(result),
        field("/config/warmup"),
        field("/config/duration"),
        field("/config/concurrency"),
        field("/config/protocol"),
        field("/config/threads"),
        field("/server/threads"),
        field("/result/throughput"),
        field("/result/p99_ms"),
        field("/server/cpu_us_per_request"),
        field("/server/cpu_pct"),
        field("/client/cpu_pct"),
        field("/profile/samples"),
    );

    let self_rows = rows(result, "/profile/self");
    let _ = writeln!(page, "<h3>Self time, top {}</h3>", self_rows.len());
    page.push_str(
        "<div class=\"wrap\"><table><thead><tr><th>#</th><th>self %</th><th></th>\
         <th class=\"m\">method</th><th class=\"m\">in</th></tr></thead><tbody>\n",
    );
    let top = self_rows.first().map_or(0.0, |r| number(r, "/pct"));
    for (rank, row) in self_rows.iter().enumerate() {
        let _ = writeln!(
            page,
            "<tr><td class=\"t\">{}</td><td class=\"n\">{:.2}</td>{}\
             <td class=\"m\">{}</td><td class=\"t\">{}</td></tr>",
            rank + 1,
            number(row, "/pct"),
            bar(number(row, "/pct"), top),
            esc(&get(row, "/name")),
            esc(&get(row, "/dso")),
        );
    }
    page.push_str("</tbody></table></div>\n");

    let total_rows = rows(result, "/profile/total");
    let _ = writeln!(page, "<h3>Total time including callees, framework and app methods, top {}</h3>", total_rows.len());
    page.push_str(
        "<div class=\"wrap\"><table><thead><tr><th>#</th><th>total %</th><th>self %</th><th></th>\
         <th class=\"m\">method</th></tr></thead><tbody>\n",
    );
    let top = total_rows.first().map_or(0.0, |r| number(r, "/total_pct"));
    for (rank, row) in total_rows.iter().enumerate() {
        let _ = writeln!(
            page,
            "<tr><td class=\"t\">{}</td><td class=\"n\">{:.2}</td><td>{:.2}</td>{}<td class=\"m\">{}</td></tr>",
            rank + 1,
            number(row, "/total_pct"),
            number(row, "/self_pct"),
            bar(number(row, "/total_pct"), top),
            esc(&get(row, "/name")),
        );
    }
    page.push_str("</tbody></table></div>\n");
}

fn rows<'a>(result: &'a Value, pointer: &str) -> &'a [Value] {
    result.pointer(pointer).and_then(Value::as_array).map_or(&[], Vec::as_slice)
}

fn bar(value: f64, top: f64) -> String {
    let width = if top > 0.0 { value * 100.0 / top } else { 0.0 };
    format!("<td class=\"b\"><div class=\"bar\" style=\"width:{width:.0}%\"></div></td>")
}

fn cell(result: &Value, pointer: &str, unit: &str) -> String {
    let value = get(result, pointer);
    if value.is_empty() { "<td class=\"na\">—</td>".to_owned() } else { format!("<td>{}{unit}</td>", esc(&value)) }
}

/// A field as displayed, empty when missing.
fn get(result: &Value, pointer: &str) -> String {
    match result.pointer(pointer) {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
    }
}

fn number(result: &Value, pointer: &str) -> f64 {
    result.pointer(pointer).and_then(Value::as_f64).unwrap_or(0.0)
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}
