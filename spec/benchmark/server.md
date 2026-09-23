# Benchmark Hosts

The remote hosts `remote.sh` runs on, shared by every benchmark.

For remote server ip, use `gcloud compute instances list` to find `office-agent-0` to be server,
`office-agent-1` to be client (the external ip is the ssh host for `SERVER` / `CLIENT`). Public ips
are ephemeral, look them up before each session, never hardcode them.

```
SERVER=<office-agent-0 ip> CLIENT=<office-agent-1 ip> ./benchmark/remote.sh run http --scenario get
```

Provisioned on the server host, outside of a run:

| service | port | access |
|---|---|---|
| `nats-server` | 4222 | systemd service |
| postgres | 5432 | user `postgres`, no password (trust auth), database `postgres` |

## A/B builds

To compare code variants, build each once on the server host, keep the binaries side by side and
alternate them per round, rather than letting `remote.sh` rebuild between runs.

- **Touch the changed file before every build.** rsync (`-a`) and scp restore a file with its
  original mtime, which is older than the last build output, so cargo sees it as fresh: it ships the
  previous variant's binary, or replays that build's cached warnings as errors (`build.warnings =
  "deny"`). Verify the variant binaries differ (`cmp`, or `nm` for a symbol only one has).
- `cargo build ... | tail` hides cargo's exit code, use `set -o pipefail`.
- A variant that removes code needs `#![allow(dead_code)]`, warnings are denied.
- Leave the server tree at the committed source afterwards (rsync + touch + build), or the next
  `remote.sh` run inherits the last variant.
