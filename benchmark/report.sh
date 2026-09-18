#!/usr/bin/env bash
# Renders spec/benchmark/report/<date>_http_server.txt into the html report next to it.
# run.sh and profile.sh call this after every run; it is also safe to run by hand.
#
# The record file holds two kinds of line: `run` (one per run.sh) and `hotspot` (top methods by
# self time, one set per profile.sh). Re-profiling a scenario replaces its previous set.
set -euo pipefail

cd "$(dirname "$0")/.."

records="${1:-spec/benchmark/report/$(date +%F)_http_server.txt}"
[ -s "$records" ] || { echo "no records in $records"; exit 1; }
html="${records%.txt}.html"

awk -v file="$(basename "$records")" '
function esc(s) { gsub(/&/, "\\&amp;", s); gsub(/</, "\\&lt;", s); gsub(/>/, "\\&gt;", s); return s }
function get(k) { return (k in f && f[k] != "") ? f[k] : "" }
function cell(k, unit,   v) { v = get(k); return v == "" ? "<td class=\"na\">—</td>" : "<td>" esc(v) unit "</td>" }
function parse(s,   i, n, parts, kv) {
    delete f
    n = split(s, parts, " ")
    for (i = 1; i <= n; i++) { split(parts[i], kv, "="); f[kv[1]] = substr(parts[i], index(parts[i], "=") + 1) }
}

$1 == "run" {
    parse(substr($0, 5))
    runs++
    host = get("host"); cores = get("cores"); commit = get("commit"); profile = get("profile")
    os = get("os"); gsub(/_/, " ", os)
    rows = rows "<tr>" \
        "<td class=\"t\">" esc(substr(get("time"), 12)) "</td>" \
        "<td class=\"s\">" esc(get("scenario")) "</td>" \
        "<td>" esc(get("protocol")) "</td>" \
        cell("concurrency", "") cell("threads", "") cell("server_threads", "") cell("duration", "s") \
        "<td class=\"n\">" esc(get("throughput")) "</td>" \
        cell("p50_ms", "") cell("p99_ms", "") cell("p999_ms", "") \
        "<td class=\"n\">" esc(get("cpu_us_per_request")) "</td>" \
        cell("allocs_per_request", "") cell("bytes_per_request", "") \
        "<td>" esc(get("peak_rss_mb")) "</td>" \
        "</tr>\n"
    if (get("failed") + get("errors") > 0) bad++
    next
}

$1 == "hotspot" {
    at = index($0, " name=")
    name = substr($0, at + 6)
    parse(substr($0, 9, at - 9))
    s = get("scenario")
    if (!(s in seen)) { order[++scenarios] = s; seen[s] = 1 }
    if (get("rank") == 1) { count[s] = 0 }          # a re-profile starts the set over
    i = ++count[s]
    hs_pct[s, i] = get("self_pct"); hs_name[s, i] = name
    hs_oncpu[s] = get("on_cpu"); hs_parked[s] = get("parked_pct")
    next
}

END {
    printf "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\n"
    printf "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
    printf "<title>http server benchmark %s</title>\n", substr(file, 1, 10)
    print "<style>"
    print ":root{color-scheme:light dark;--bg:#fff;--fg:#1a1a1a;--dim:#666;--line:#e3e3e3;--head:#f7f7f7;--accent:#b35c00;--bar:#f0c9a0}"
    print "@media (prefers-color-scheme:dark){:root{--bg:#16181c;--fg:#e8e8e8;--dim:#9aa0a6;--line:#2c2f36;--head:#1e2126;--accent:#f0a458;--bar:#6b4520}}"
    print "*{box-sizing:border-box}body{margin:0;padding:24px 16px;background:var(--bg);color:var(--fg);"
    print "font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}"
    print "main{max-width:1120px;margin:0 auto}h1{font-size:20px;margin:0 0 4px}"
    print "h2{font-size:16px;margin:28px 0 2px}p.meta{color:var(--dim);margin:0 0 14px;font-size:13px}"
    print "table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}"
    print "th,td{padding:6px 7px;text-align:right;border-bottom:1px solid var(--line);white-space:nowrap;font-size:13px}"
    print "th{background:var(--head);font-weight:600;font-size:12px;color:var(--dim)}"
    print "th:first-child,td:first-child,td.s,td.m,th.m{text-align:left}td.s{font-weight:600}"
    print "td.t{color:var(--dim);font-size:12px}td.na{color:var(--dim)}td.n{color:var(--accent);font-weight:600}"
    print "td.m{font-family:ui-monospace,Menlo,monospace;font-size:12px;white-space:normal;word-break:break-word}"
    print "td.b{width:90px}.bar{height:9px;background:var(--bar);border-radius:2px;min-width:1px}"
    print "tbody tr:hover{background:var(--head)}.wrap{overflow-x:auto;border:1px solid var(--line);border-radius:6px}"
    print "footer{color:var(--dim);font-size:12px;margin-top:22px}footer code{font-size:12px}"
    print "</style></head><body><main>"
    printf "<h1>http server benchmark — %s</h1>\n", substr(file, 1, 10)
    printf "<p class=\"meta\">%d run(s) · %s · %s cores · %s · commit %s · cargo profile %s</p>\n",
           runs, esc(host), esc(cores), esc(os), esc(commit), esc(profile)

    print "<div class=\"wrap\"><table><thead><tr>"
    print "<th>time</th><th>scenario</th><th>proto</th><th>streams</th><th>client thr</th><th>server thr</th><th>dur</th>"
    print "<th>req/s</th><th>p50 ms</th><th>p99 ms</th><th>p99.9 ms</th>"
    print "<th>cpu µs/req</th><th>allocs/req</th><th>bytes/req</th><th>peak rss MB</th>"
    print "</tr></thead><tbody>"
    printf "%s", rows
    print "</tbody></table></div>"
    if (bad > 0) printf "<p class=\"meta\">%d run(s) recorded failed or errored requests.</p>\n", bad

    for (j = 1; j <= scenarios; j++) {
        s = order[j]
        printf "<h2>Methods taking the most time — %s</h2>\n", esc(s)
        printf "<p class=\"meta\">self time, %s on-cpu samples at 1 kHz, %s%% of samples parked (spare capacity)</p>\n",
               esc(hs_oncpu[s]), esc(hs_parked[s])
        print "<div class=\"wrap\"><table><thead><tr><th>#</th><th>self %</th><th></th><th class=\"m\">method</th></tr></thead><tbody>"
        for (i = 1; i <= count[s]; i++) {
            printf "<tr><td class=\"t\">%d</td><td class=\"n\">%s</td>", i, esc(hs_pct[s, i])
            printf "<td class=\"b\"><div class=\"bar\" style=\"width:%.0f%%\"></div></td>", hs_pct[s, i] * 100 / hs_pct[s, 1]
            printf "<td class=\"m\">%s</td></tr>\n", esc(hs_name[s, i])
        }
        print "</tbody></table></div>"
    }

    print "<footer>"
    print "<p><b>req/s</b> measures client and server together on one host, so it is only comparable within a run group."
    print "<b>cpu µs/req</b> is the server process alone, sampled from <code>ps</code>, and is the number to compare."
    print "<b>allocs/req</b> needs <code>ALLOC_STATS=1</code> and is deterministic, so it resolves differences cpu time cannot.</p>"
    print "<p>Runs come from <code>benchmark/run.sh</code>, hotspots from <code>benchmark/profile.sh</code>, both rendered by"
    print "<code>benchmark/report.sh</code> from the record file beside this one. A profiling run contributes no result row,"
    print "the profiler skews throughput and cpu.</p>"
    print "</footer></main></body></html>"
}
' "$records" > "$html"

echo "report written to $html"
