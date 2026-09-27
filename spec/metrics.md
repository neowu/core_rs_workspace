# Metrics design

Code: [`lib/framework/src/metrics/collector.rs`](../lib/framework/src/metrics/collector.rs),
[`metrics/counter.rs`](../lib/framework/src/metrics/counter.rs) · siblings:
[`action_log.md`](action_log.md)

`MetricsCollector` emits one metrics record every 5s to the appender, only when an app registered
metrics through `System::add_metrics`. Framework stats plus each registered collector's stats go
into the same record.

Keys are `&'static str`, added through `Metrics::add_stat` / `add_info`. `MetricsMessage` holds
them as `Cow<'static, str>`, as `ActionMessage` does. So the source app allocates nothing for keys,
app or host, and only the log processor, running on another host, owns them after deserializing.

## Values cover the whole window, not one sample

A 5s sample misses short spikes, so every stat is either a delta of a cumulative counter over the
window, or a peak tracked since the last collect (`Counter::max()`, reported as `active_*`).
Instantaneous gauges such as tokio's `global_queue_depth` are left out for that reason.

## Framework stats

| key | source | meaning |
|---|---|---|
| `container_cpu_usage`, `process_cpu_usage` | cgroup `cpu.stat` / `/proc/self/stat` delta | percent of the cpu quota (of one core without a quota) |
| `runtime_busy_usage` | tokio `worker_total_busy_duration` delta | percent of worker capacity (workers × window) spent polling |
| `container_mem_max`, `container_mem_used`, `process_vm_rss` | cgroup memory files, `/proc/self/statm` | bytes, working set excludes inactive file cache |

cgroup v2 first, v1 fallback; a stat whose files are missing is skipped.

## Telling whether the runtime is a bottleneck

Tasks waiting for a worker has three causes, each covered by one signal:

- one action blocks a worker → that action's `poll_elapsed / poll_count`, see
  [`action_log.md`](action_log.md);
- the container is out of cpu quota → `container_cpu_usage` near 100%;
- every worker is busy with many short polls → `runtime_busy_usage` near 100%.

`runtime_busy_usage` measures the runtime rather than the container: `container_cpu_usage` is relative
to the quota, not the worker count, and includes non tokio threads such as rdkafka's. A worker
publishes busy time only when it parks or runs maintenance, so one long poll is counted in the window
it ends in, and that window can read over 100%.

No cfs throttling stats (`nr_throttled` / `throttled_usec`): apps keep tokio's default worker count,
`available_parallelism()`, which already honours the cgroup quota, so the runtime alone cannot
outrun the quota and throttling shows up as `container_cpu_usage` near 100%. The blind spot is
sub-period bursts from non tokio threads (e.g. rdkafka) throttled under a moderate 5s average; the
symptom would be `poll_elapsed` rising while cpu usage looks fine, add the stats back then.

Rejected: a schedule lag probe (a task that measures how late its `sleep` wakes). Catching spikes
needs sampling every 10–100ms and a peak tracker, and the result still only says tasks waited, not
why; the three signals above answer both at no runtime cost. What is lost is an actual latency
number.
