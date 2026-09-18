#!/usr/bin/env bash
# Starts the server, runs one client scenario against it, stops the server, records the result in
# spec/benchmark/report/<date>_http_server.txt and regenerates the html report beside it.
#
# The server binds 8080 and the client defaults to it, so a benchmark host keeps that port free.
# Any client option passes through:
#
#   ./benchmark/run.sh --scenario post --concurrency 128
#   ALLOC_STATS=1 ./benchmark/run.sh --scenario get      # adds heap accounting, see its cost
set -euo pipefail

cd "$(dirname "$0")/.."

PROFILE="${PROFILE:-release}"
URL="http://localhost:8080"

features=()
[ -n "${ALLOC_STATS:-}" ] && features=(--features http_test_server/alloc_stats)

cargo build --profile "$PROFILE" ${features[@]+"${features[@]}"} -p http_test_server -p http_test_client

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

report_dir=spec/benchmark/report
mkdir -p "$report_dir"
records="$report_dir/$(date +%F)_http_server.txt"
printf 'run time=%s host=%s cores=%s os=%s commit=%s profile=%s server_threads=%s alloc_stats=%s %s %s %s\n' \
    "$(date +%Y-%m-%dT%H:%M:%S)" \
    "$(hostname -s)" \
    "$(sysctl -n hw.ncpu 2>/dev/null || nproc)" \
    "$(uname -sr | tr ' ' '_')" \
    "$(git rev-parse --short HEAD 2>/dev/null || echo none)" \
    "$PROFILE" \
    "${TOKIO_WORKER_THREADS:-$(sysctl -n hw.ncpu 2>/dev/null || nproc)}" \
    "$([ -n "${ALLOC_STATS:-}" ] && echo yes || echo no)" \
    "$data" "$server_side" "$heap" >> "$records"

./benchmark/report.sh "$records"
