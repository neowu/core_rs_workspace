#!/usr/bin/env bash
# Starts the server, runs one client scenario against it, stops the server, then hands the result to
# `benchmark/report`, which records it and regenerates the html report beside the record file.
#
# The server binds 8080 and the client defaults to it, so a benchmark host keeps that port free.
# Any client option passes through:
#
#   ./benchmark/run_http_test.sh --scenario post --concurrency 128
#   ALLOC_STATS=1 ./benchmark/run_http_test.sh --scenario get      # process wide heap accounting, see its cost
#   NO_ACTION_ALLOC_STATS=1 ./benchmark/run_http_test.sh --scenario get  # without framework's per-action one
set -euo pipefail

cd "$(dirname "$0")/.."

PROFILE="${PROFILE:-release}"
URL="http://localhost:8080"

# framework/alloc_stats is a default feature, so it is what apps ship and what a plain run has to
# measure; http_test_server takes framework with default-features = false and run_http_test.sh turns it back
# on here. the process wide counter installs a #[global_allocator] of its own, and two in one crate
# graph is a link error, so ALLOC_STATS runs without the per-action one and the record says so.
features=()
action_alloc_stats=yes
if [ -n "${ALLOC_STATS:-}" ]; then
    features=(--features http_test_server/alloc_stats)
    action_alloc_stats=no
elif [ -n "${NO_ACTION_ALLOC_STATS:-}" ]; then
    action_alloc_stats=no
else
    features=(--features framework/alloc_stats)
fi

cargo build --profile "$PROFILE" ${features[@]+"${features[@]}"} \
    -p http_test_server -p http_test_client -p report

dir="target/$([ "$PROFILE" = "dev" ] && echo debug || echo "$PROFILE")"
server_log=$(mktemp)
client_log=$(mktemp)
rss_file=$(mktemp)

"$dir/http_test_server" > >(tee "$server_log") 2>&1 &
server=$!
trap 'kill $server 2>/dev/null || true; rm -f "$server_log" "$client_log" "$rss_file"' EXIT

until curl -sf -o /dev/null "$URL/health-check"; do
    kill -0 $server 2>/dev/null || { echo "server failed to start"; exit 1; }
    sleep 0.2
done

# hundredths of a second, which is all `ps` resolves
cpu_cs() { ps -o time= -p "$1" | tr -d ' ' | awk -F'[:.]' '{print ($1*60+$2)*100+$3}'; }

( peak=0
  while kill -0 $server 2>/dev/null; do
      rss=$(ps -o rss= -p $server 2>/dev/null | tr -d ' ')
      [ -n "$rss" ] && [ "$rss" -gt "$peak" ] && { peak=$rss; echo "$peak" > "$rss_file"; }
      sleep 0.5
  done ) &

cpu_before=$(cpu_cs $server)
"$dir/http_test_client" --record "$@" | tee "$client_log"
cpu_after=$(cpu_cs $server)

kill -TERM $server
wait $server 2>/dev/null || true

data=$(sed -n 's/^data //p' "$client_log")
[ -n "$data" ] || { echo "client produced no result line"; exit 1; }

# every request the server served, warmup included, since cpu was sampled across both phases
warmup=$(echo "$data" | tr ' ' '\n' | sed -n 's/^warmup_requests=//p')
measured=$(echo "$data" | tr ' ' '\n' | sed -n 's/^requests=//p')
total=$(( warmup + measured ))
cpu=$(( cpu_after - cpu_before ))
peak_rss=$(cat "$rss_file" 2>/dev/null || echo 0)

server_side=$(awk -v cpu="$cpu" -v n="$total" -v rss="$peak_rss" \
    'BEGIN { printf "served_requests=%d cpu_us_per_request=%.2f peak_rss_mb=%.1f", n, cpu*10000/n, rss/1024 }')

heap=""
if grep -q alloc_stats "$server_log"; then
    heap=$(sed -n 's/.*alloc_stats: //p' "$server_log" | awk -v n="$total" -F'[=,]' \
        '{ printf "allocs_per_request=%.1f bytes_per_request=%.0f total_allocs=%d", $2/n, $4/n, $2 }')
fi

echo "--- server ---"
echo "$server_side $heap" | tr ' ' '\n' | sed 's/=/ = /'

# report fills in host, cores, os and commit itself; the rest is what this run knows.
# $data, $server_side and $heap are already key=value, so they split into arguments as they are.
"$dir/report" run \
    "report/$(date +%F)_http_server.txt" \
    "$(date +%Y-%m-%dT%H:%M:%S)" \
    "profile=$PROFILE" \
    "server_threads=${TOKIO_WORKER_THREADS:-$(sysctl -n hw.ncpu 2>/dev/null || nproc)}" \
    "alloc_stats=$([ -n "${ALLOC_STATS:-}" ] && echo yes || echo no)" \
    "action_alloc_stats=$action_alloc_stats" \
    $data $server_side $heap
