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
