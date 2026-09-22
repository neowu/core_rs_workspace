#!/usr/bin/env bash
# Records a cpu profile of the server while the client drives load against it.
# Any client option can be passed through, e.g.
# ./benchmark/profile_nats_api_test.sh --scenario post --duration 30
#
# Needs a nats server (`container start nats`) and samply (cargo install samply). Open the result
# with `samply load <file>`, the inverted call tree is what answers "where does the time go", the
# flame graph is what answers "who called it".
#
# The top methods by self time are folded into report/<date>_nats_api_server.html by
# `benchmark/report`. A profiling run contributes no result row, the profiler skews throughput and
# cpu, only hotspots.
set -euo pipefail

cd "$(dirname "$0")/.."

URL="${NATS_URL:-nats.test:4222}"
OUT="${OUT:-target/nats_profile.json.gz}"
RATE="${RATE:-999}"

# profiling = release plus full debug info, release alone only carries line tables.
# framework/alloc_stats is a framework default that nats_api_test_server opts out of, see
# run_nats_api_test.sh, so it is turned back on here too -- a profile has to show the server as
# apps ship it
cargo build --profile profiling --features framework/alloc_stats \
    -p nats_api_test_server -p nats_api_test_client -p report

NATS_URL="$URL" samply record --save-only --no-open --unstable-presymbolicate -r "$RATE" -o "$OUT" \
    -- target/profiling/nats_api_test_server &
samply=$!

server=""
trap '[ -n "$server" ] && kill "$server" 2>/dev/null; kill "$samply" 2>/dev/null; true' EXIT

# samply must outlive the server, killing samply itself discards the profile.
# readiness is the client's job -- it retries until the service subscribes -- so this only waits for
# the process to exist
for _ in $(seq 100); do
    server=$(pgrep -f "^target/profiling/nats_api_test_server$" || true)
    [ -n "$server" ] && break
    kill -0 $samply 2>/dev/null || { echo "server failed to start"; exit 1; }
    sleep 0.2
done
[ -n "$server" ] || { echo "server did not start"; exit 1; }

target/profiling/nats_api_test_client --url "$URL" "$@"

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
    "report/$(date +%F)_nats_api_server.txt" \
    "${OUT%.gz}.syms.json" "$scenario" 15
