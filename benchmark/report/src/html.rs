//! Renders the record file into the html report beside it.
//!
//! Every record in the file is rendered every time, so the html is always a complete view of the
//! day and regenerating it by hand is safe.

use std::fmt::Write as _;
use std::fs::write;
use std::path::Path;

use crate::record::Record;

const STYLE: &str = r"
:root{color-scheme:light dark;--bg:#fff;--fg:#1a1a1a;--dim:#666;--line:#e3e3e3;--head:#f7f7f7;--accent:#b35c00;--bar:#f0c9a0}
@media (prefers-color-scheme:dark){:root{--bg:#16181c;--fg:#e8e8e8;--dim:#9aa0a6;--line:#2c2f36;--head:#1e2126;--accent:#f0a458;--bar:#6b4520}}
*{box-sizing:border-box}body{margin:0;padding:24px 16px;background:var(--bg);color:var(--fg);
font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
main{max-width:1120px;margin:0 auto}h1{font-size:20px;margin:0 0 4px}
h2{font-size:16px;margin:28px 0 2px}p.meta{color:var(--dim);margin:0 0 14px;font-size:13px}
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
<p><b>req/s</b> measures client and server together on one host, so it is only comparable within a run group.
<b>cpu µs/req</b> is the server process alone, sampled from <code>ps</code>, and is the number to compare.</p>
<p>Runs come from <code>benchmark/run_*.sh</code>, hotspots from <code>benchmark/profile_*.sh</code>, both recorded
and rendered by <code>benchmark/report</code> from the record file beside this one. A profiling run contributes no
result row, the profiler skews throughput and cpu.</p>
</footer></main></body></html>";

/// Top methods of one scenario, in the order the profile ranked them.
struct Hotspots {
    scenario: String,
    on_cpu: String,
    parked_pct: String,
    methods: Vec<(f64, String)>,
}

pub fn render(records_path: &Path, records: &[Record]) -> std::path::PathBuf {
    // the record file is named <date>_<target>.txt, which is the whole title this report needs
    let file = records_path.file_stem().unwrap_or_default().to_string_lossy().into_owned();
    let date = file.get(..10).unwrap_or(&file);
    let target = file.get(11..).map_or_else(|| "benchmark".to_owned(), |name| name.replace('_', " "));

    let runs: Vec<&Record> = records.iter().filter(|r| r.kind == "run").collect();
    let mut page = String::new();

    let _ = write!(
        page,
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{target} benchmark {date}</title>\n<style>{STYLE}</style></head><body><main>\n\
         <h1>{target} benchmark — {date}</h1>\n"
    );

    // the last run describes the host, the whole file is one day on one machine
    let last = runs.last();
    let meta = |key: &str| last.map_or("", |r| r.get(key));
    let _ = writeln!(
        page,
        "<p class=\"meta\">{} run(s) · {} · {} cores · {} · commit {} · cargo profile {}</p>",
        runs.len(),
        esc(meta("host")),
        esc(meta("cores")),
        esc(&meta("os").replace('_', " ")),
        esc(meta("commit")),
        esc(meta("profile")),
    );

    runs_table(&mut page, &runs);
    for set in hotspot_sets(records) {
        hotspot_table(&mut page, &set);
    }
    page.push_str(FOOTER);
    page.push('\n');

    let html = records_path.with_extension("html");
    write(&html, page).unwrap_or_else(|e| panic!("cannot write {}: {e}", html.display()));
    html
}

fn runs_table(page: &mut String, runs: &[&Record]) {
    page.push_str(
        "<div class=\"wrap\"><table><thead><tr>\n\
         <th>time</th><th>scenario</th><th>proto</th><th>conc</th><th>client thr</th><th>server thr</th><th>dur</th>\n\
         <th>req/s</th><th>p50 ms</th><th>p99 ms</th><th>p99.9 ms</th>\n\
         <th>cpu µs/req</th><th>peak rss MB</th>\n\
         </tr></thead><tbody>\n",
    );
    let mut bad = 0;
    for run in runs {
        let _ = write!(
            page,
            "<tr><td class=\"t\">{}</td><td class=\"s\">{}</td><td>{}</td>",
            esc(run.get("time").get(11..).unwrap_or("")),
            esc(run.get("scenario")),
            esc(run.get("protocol")),
        );
        for (key, unit) in [("concurrency", ""), ("threads", ""), ("server_threads", ""), ("duration", "s")] {
            page.push_str(&cell(run, key, unit));
        }
        let _ = write!(page, "<td class=\"n\">{}</td>", esc(run.get("throughput")));
        for key in ["p50_ms", "p99_ms", "p999_ms"] {
            page.push_str(&cell(run, key, ""));
        }
        let _ = write!(page, "<td class=\"n\">{}</td>", esc(run.get("cpu_us_per_request")));
        let _ = writeln!(page, "<td>{}</td></tr>", esc(run.get("peak_rss_mb")));
        if run.number("failed") + run.number("errors") > 0.0 {
            bad += 1;
        }
    }
    page.push_str("</tbody></table></div>\n");
    if bad > 0 {
        let _ = writeln!(page, "<p class=\"meta\">{bad} run(s) recorded failed or errored requests.</p>");
    }
}

fn hotspot_table(page: &mut String, set: &Hotspots) {
    let _ = writeln!(page, "<h2>Methods taking the most time — {}</h2>", esc(&set.scenario));
    let _ = writeln!(
        page,
        "<p class=\"meta\">self time, {} on-cpu samples at 1 kHz, {}% of samples parked (spare capacity)</p>",
        esc(&set.on_cpu),
        esc(&set.parked_pct),
    );
    page.push_str(
        "<div class=\"wrap\"><table><thead><tr><th>#</th><th>self %</th><th></th>\
         <th class=\"m\">method</th></tr></thead><tbody>\n",
    );
    let top = set.methods.first().map_or(0.0, |(pct, _)| *pct);
    for (rank, (pct, name)) in set.methods.iter().enumerate() {
        let width = if top > 0.0 { pct * 100.0 / top } else { 0.0 };
        let _ = writeln!(
            page,
            "<tr><td class=\"t\">{}</td><td class=\"n\">{pct:.2}</td>\
             <td class=\"b\"><div class=\"bar\" style=\"width:{width:.0}%\"></div></td>\
             <td class=\"m\">{}</td></tr>",
            rank + 1,
            esc(name),
        );
    }
    page.push_str("</tbody></table></div>\n");
}

/// One set per scenario, in first-seen order. Re-profiling a scenario replaces its previous set,
/// which `rank=1` marks — the record file keeps both, the report shows only the latest.
fn hotspot_sets(records: &[Record]) -> Vec<Hotspots> {
    let mut sets: Vec<Hotspots> = Vec::new();
    for record in records.iter().filter(|r| r.kind == "hotspot") {
        let scenario = record.get("scenario");
        let index = sets.iter().position(|set| set.scenario == scenario).unwrap_or_else(|| {
            sets.push(Hotspots {
                scenario: scenario.to_owned(),
                on_cpu: String::new(),
                parked_pct: String::new(),
                methods: Vec::new(),
            });
            sets.len() - 1
        });
        let set = &mut sets[index];
        if record.get("rank") == "1" {
            set.methods.clear();
        }
        set.on_cpu = record.get("on_cpu").to_owned();
        set.parked_pct = record.get("parked_pct").to_owned();
        set.methods.push((record.number("self_pct"), record.get("name").to_owned()));
    }
    sets
}

fn cell(record: &Record, key: &str, unit: &str) -> String {
    let value = record.get(key);
    if value.is_empty() { "<td class=\"na\">—</td>".to_owned() } else { format!("<td>{}{unit}</td>", esc(value)) }
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}
