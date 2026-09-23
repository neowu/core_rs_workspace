#!/usr/bin/env bash
# Starts the server, runs one client scenario against it, stops the server, then hands the result to
# `benchmark/report`, which records it and regenerates the html report beside the record file.
#
# Needs a nats server: `container start nats`. Both sides default to nats.test:4222, override with
# NATS_URL. Any client option passes through:
#
#   ./benchmark/run_nats_api_test.sh --scenario post --concurrency 128
set -euo pipefail

cd "$(dirname "$0")/.."

PROFILE="${PROFILE:-release}"
URL="${NATS_URL:-nats.test:4222}"

cargo build --profile "$PROFILE" \
    -p nats_api_test_server -p nats_api_test_client -p report

dir="target/$([ "$PROFILE" = "dev" ] && echo debug || echo "$PROFILE")"
server_log=$(mktemp)
client_log=$(mktemp)
rss_file=$(mktemp)

NATS_URL="$URL" "$dir/nats_api_test_server" > >(tee "$server_log") 2>&1 &
server=$!
trap 'kill $server 2>/dev/null || true; rm -f "$server_log" "$client_log" "$rss_file"' EXIT

# nats has no health endpoint, the service logs its subjects once it has subscribed
until grep -q "start nats service" "$server_log"; do
    kill -0 $server 2>/dev/null || { echo "server failed to start, is nats running? \`container start nats\`"; exit 1; }
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
"$dir/nats_api_test_client" --url "$URL" --record "$@" | tee "$client_log"
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

echo "--- server ---"
echo "$server_side" | tr ' ' '\n' | sed 's/=/ = /'

# report fills in host, cores, os and commit itself; the rest is what this run knows.
# $data and $server_side are already key=value, so they split into arguments as they are.
"$dir/report" run \
    "report/$(date +%F)_nats_api_server.txt" \
    "$(date +%Y-%m-%dT%H:%M:%S)" \
    "profile=$PROFILE" \
    "server_threads=${TOKIO_WORKER_THREADS:-$(sysctl -n hw.ncpu 2>/dev/null || nproc)}" \
    $data $server_side
