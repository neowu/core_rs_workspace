#!/usr/bin/env bash
# Builds on the server host, deploys the server and the client to two remote hosts over ssh, runs
# one client scenario against the server, downloads the client's result into report/<date>_<name>/
# and renders report/<date>_<name>.html from every result of the day. Any client option passes through:
#
#   SERVER=35.236.186.204 CLIENT=35.229.228.183 ./benchmark/remote.sh run http --scenario post --concurrency 128
#   SERVER=35.236.186.204 CLIENT=35.229.228.183 ./benchmark/remote.sh profile nats_api --scenario post --duration 30
#
# `profile` also records the server with perf and puts its top methods in the report; its throughput
# and cpu are skewed by the profiler, so it shows below the runs, never as a run row.
#
# Remote hosts are debian with passwordless sudo, rsync and rust (~/.cargo/bin/cargo), both on the
# same os so a binary built on the server runs on the client; profile also needs perf
# (`apt install linux-perf`) on the server. Binaries go to /opt/<binary>/, the source to
# /opt/build/src and the build output to /opt/build/target, kept between runs so rebuilds are
# incremental. Only `report` is built on this host, which needs rsync too. The client reaches the
# server on its internal ip (`hostname -I`), SERVER_IP overrides. The nats broker runs on the server
# host, from debian's nats-server package unless one is on the path already.
set -euo pipefail

usage="usage: remote.sh <run|profile> <http|nats_api> [client options]"
mode="${1:?$usage}"
name="${2:?$usage}"
shift 2
case "$mode" in run | profile) ;; *) echo "$usage" >&2; exit 2 ;; esac
case "$name" in http | nats_api) ;; *) echo "$usage" >&2; exit 2 ;; esac

cd "$(dirname "${BASH_SOURCE[0]}")/.."

SERVER="${SERVER:?set SERVER to the ssh host of the server}"
CLIENT="${CLIENT:?set CLIENT to the ssh host of the client}"
PROFILE="${PROFILE:-release}"
RATE="${RATE:-999}"
TOP="${TOP:-25}"

target_dir="${CARGO_TARGET_DIR:-target}"
build_dir=/opt/build
server_bin=/opt/${name}_test_server/${name}_test_server
client_bin=/opt/${name}_test_client/${name}_test_client
remote_result=/opt/${name}_test_client/result.json
perf_data=/opt/${name}_test_server/perf.data
report="$target_dir/release/report"

# $1 is the cargo profile, the rest are extra rustflags for the remote build
build() {
    local profile=$1
    shift
    cargo build --release -p report

    # --delete so removed files never linger, rsync keeps mtimes so cargo sees unchanged files as such
    ssh "$SERVER" "sudo mkdir -p $build_dir && sudo chown \$(id -u):\$(id -g) $build_dir"
    rsync -az --delete --exclude=/target --exclude=/.git --exclude=/report --exclude=/.claude \
        ./ "$SERVER:$build_dir/src/"
    # shellcheck disable=SC2029 # expanded here on purpose
    ssh "$SERVER" "cd $build_dir/src && CARGO_TARGET_DIR=$build_dir/target RUSTFLAGS='$*' \
        ~/.cargo/bin/cargo build --profile $profile -p ${name}_test_server -p ${name}_test_client"
    remote_bin_dir="$build_dir/target/$([ "$profile" = "dev" ] && echo debug || echo "$profile")"
}

# copied beside and renamed over, so a binary still running from an earlier run is never in the way;
# the client binary goes server to client through this host, the two need no ssh trust between them
deploy() {
    local host=$1 path=$2
    ssh "$host" "sudo mkdir -p $(dirname "$path") && sudo chown \$(id -u):\$(id -g) $(dirname "$path")"
    scp -q -3 "$SERVER:$remote_bin_dir/$(basename "$path")" "$host:$path.new"
    ssh "$host" "mv $path.new $path"
}

# anchored, so the pattern never matches the remote shell running pkill itself
stop_server() { ssh "$SERVER" "pkill -f '^$server_bin' || true"; }

# left running between runs, it is the server host's infrastructure rather than part of a run
start_nats() {
    ssh "$SERVER" "command -v nats-server > /dev/null || sudo apt-get install -y -qq nats-server > /dev/null"
    ssh "$SERVER" "pgrep -x nats-server > /dev/null || nohup nats-server -p 4222 > ~/nats-server.log 2>&1 < /dev/null &"
}

# deploys both sides and starts the server, sets url once the client host can reach it
start() {
    deploy "$SERVER" "$server_bin"
    deploy "$CLIENT" "$client_bin"

    local server_ip="${SERVER_IP:-$(ssh "$SERVER" "hostname -I | awk '{print \$1}'")}"
    local server_env=""
    if [ "$name" = "nats_api" ]; then
        start_nats
        url="$server_ip:4222"
        server_env="NATS_URL=localhost:4222"
    else
        url="http://$server_ip:8080"
    fi

    stop_server
    ssh "$SERVER" "$server_env nohup $server_bin > $server_bin.log 2>&1 < /dev/null &"
    trap stop_server EXIT

    if ! ready; then
        echo "server not ready, url=$url"
        ssh "$SERVER" "tail -20 $server_bin.log"
        exit 1
    fi
}

# http is polled from the client host, which also proves the client can reach the server; nats has
# no health endpoint, the service logs once it has subscribed and the client retries until it answers
ready() {
    if [ "$name" = "nats_api" ]; then
        ssh "$SERVER" "for i in \$(seq 50); do grep -q 'start nats service' $server_bin.log && exit 0; sleep 0.2; done; exit 1"
    else
        ssh "$CLIENT" "for i in \$(seq 50); do curl -sf -o /dev/null $url/health-check && exit 0; sleep 0.2; done; exit 1"
    fi
}

# runs the client with the given options and downloads its result file into $1
run_client() {
    local result=$1
    shift
    # shellcheck disable=SC2029 # expanded here on purpose, the remote side gets the quoted options
    ssh "$CLIENT" "$client_bin --url $url --output $remote_result ${*:+$(printf '%q ' "$@")}"
    mkdir -p "$(dirname "$result")"
    scp -q "$CLIENT:$remote_result" "$result"
    echo "result downloaded to $result"
}

# anchored like stop_server, sudo's own command line starts with sudo
stop_perf() { ssh "$SERVER" "sudo pkill -INT -f '^perf record' || true; while pgrep -f '^perf record' > /dev/null; do sleep 0.2; done"; }

# profiling = release plus full debug info, frame pointers so perf can walk the stacks for the
# inclusive view without copying stack memory per sample
if [ "$mode" = "profile" ]; then
    PROFILE=profiling
    build profiling -Cforce-frame-pointers=yes
else
    build "$PROFILE"
fi
start

time=$(date +%Y-%m-%dT%H:%M:%S)
dir="report/$(date +%F)_$name"
stamp=$(date +%H%M%S)
fields=("time=$time" "commit=$(git rev-parse --short HEAD 2> /dev/null || echo none)" "profile=$PROFILE")

if [ "$mode" = "run" ]; then
    run_client "$dir/$stamp.json" "$@"
    stop_server
    "$report" run "$dir/$stamp.json" "${fields[@]}"
    exit
fi

pid=$(ssh "$SERVER" "pgrep -f '^$server_bin'")
ssh "$SERVER" "sudo nohup perf record -F $RATE -g -p $pid -o $perf_data > $perf_data.log 2>&1 < /dev/null &"
trap 'stop_perf; stop_server' EXIT

run_client "$dir/${stamp}_profile.json" "$@"
stop_perf

# perf only samples threads on a cpu, so a parked worker never shows up and needs no filtering;
# perf.data stays on the server for a closer look with `sudo perf report -i <perf.data>`
perf_dir=$(dirname $perf_data)
ssh "$SERVER" "cd $perf_dir \
    && sudo perf report -i $perf_data --stdio --no-children --sort dso,symbol -g none --percent-limit 0.2 > self.txt 2> /dev/null \
    && sudo perf report -i $perf_data --stdio --children --sort dso,symbol -g none --percent-limit 0.1 > total.txt 2> /dev/null"
out="$target_dir/benchmark/${stamp}_profile_$name"
mkdir -p "$out"
scp -q "$SERVER:$perf_dir/self.txt" "$SERVER:$perf_dir/total.txt" "$out/"

"$report" profile "$dir/${stamp}_profile.json" "$out/self.txt" "$out/total.txt" "$TOP" "${fields[@]}"
