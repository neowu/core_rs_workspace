#!/usr/bin/env bash
# Records a cpu profile of the server while the client drives load against it.
# Any client option can be passed through, e.g. ./benchmark/profile.sh --scenario post --duration 30
#
# Needs samply (cargo install samply). Open the result with `samply load <file>`, the inverted call
# tree is what answers "where does the time go", the flame graph is what answers "who called it".
#
# The top methods by self time are folded into report/<date>_http_server.html by
# `benchmark/report`. A profiling run contributes no result row, the profiler skews throughput and
# cpu, only hotspots.
set -euo pipefail

cd "$(dirname "$0")/.."

URL="http://localhost:8080"
OUT="${OUT:-target/profile.json.gz}"
RATE="${RATE:-999}"

# profiling = release plus full debug info, release alone only carries line tables
cargo build --profile profiling -p http_test_server -p http_test_client -p report

samply record --save-only --no-open --unstable-presymbolicate -r "$RATE" -o "$OUT" \
    -- target/profiling/http_test_server &
samply=$!

server=""
trap '[ -n "$server" ] && kill "$server" 2>/dev/null; kill "$samply" 2>/dev/null; true' EXIT

until curl -sf -o /dev/null "$URL/health-check"; do
    kill -0 $samply 2>/dev/null || { echo "server failed to start"; exit 1; }
    sleep 0.2
done
# samply must outlive the server, killing samply itself discards the profile
server=$(pgrep -f "^target/profiling/http_test_server$")

target/profiling/http_test_client "$@"

kill -TERM "$server"
server=""
wait "$samply"

echo
echo "profile written to $OUT"
echo "view it with: samply load $OUT"

# the scenario is whatever was passed through to the client, the client's own default is get
scenario=get
for ((i = 1; i <= $#; i++)); do
    [ "${!i}" = "--scenario" ] && scenario="${@:i+1:1}"
done

# gunzip does the decompression so the tool needs no gzip dependency
gunzip -c "$OUT" | target/profiling/report hotspot \
    "report/$(date +%F)_http_server.txt" \
    "${OUT%.gz}.syms.json" "$scenario" 15
