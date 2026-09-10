# Interpreting the EKS ARM64 measurements

The accompanying CSV is an allowlisted export from disposable namespace tests.
It retains 208 measured runs and 247 relative-time flush observations.
It contains resource sizes, workload parameters, binary hashes and measurements.
It excludes cluster identities, namespace names, node names, addresses, account
identifiers, volume identifiers, credentials and raw logs. The test methodology
and recommended starting configuration are in
[production sizing](../../../docs/production-sizing.md).

Use `healthy=True` **and** `excluded=False` before considering a row as capacity
evidence. A healthy row still needs a suitable P99, test duration, offered rate,
document shape and topology. Closed-loop short saturation is not a sustained
production budget. `verified` reports data reconciliation, including explicitly
accounted uncertain final writes; it can be true for an overload or fault test.

Compare `successful_rpcs_per_second` with `successful_operations_per_second`.
Client batching changes their ratio. Fixed-rate latency includes time spent
behind the intended arrival schedule. A nonzero `scheduled_not_issued` means
the offered rate was not sustained, even if completed RPCs succeeded.
`execution_p99_ms` excludes waiting behind the generator's arrival schedule;
use the full `p99_ms` for capacity acceptance. Recovery throughput in the final
30 seconds uses complete one-second timeline bins.
`rpc_timeout_seconds` applies after a call is issued; queued scheduled latency
can be longer without a transport timeout. `error_backoff_seconds` records the
bounded pause after a failed call.

The file includes intermediate experiments so the optimization can be audited.
Do not pool them into a single average: read/response budgets, GC, server and
client batching, completion modes and database copies deliberately vary. The
`server_binary_sha256` and `load_binary_sha256` distinguish the running binaries.
All reference builds use Go 1.27.0 for Linux ARM64 with CGO disabled.
Sink replicas and the load generator use different nodes. Database Pods may
share a node with Sink, as recorded by `sink_shares_node_with_backend`; hosts
can also run unrelated workloads. This is a container-quota comparison, not
a dedicated-host benchmark.

| Server SHA-256 prefix | Source / purpose |
| --- | --- |
| `e043090d7056` | `026d1d1` / v0.11.0 baseline |
| `523ea3b4953c` | `b33b3ad` / Lua templates and bounded conditional working set |
| `7c910988883a` | `f29c337` / MongoDB 8 conditional client bulk writes |
| `a50e42f73637` | Intermediate literal-pipeline prototype, before the singleton update in `7a2e9f8`; not the final server |
| `42741dd5ce9e` | `63a0d55` / final server implementation, including literal pipelines and admission fairness |
| `87b9ca40c5ad` | Instrumented diagnostic build; excluded from capacity recommendations |

Load binary `f6cdfbc65951` has source `7726178` and adapts reconciliation reads
to the server's configured byte limit. Build `21518af022cb` / `3792ea1` adds an
explicit per-index translog flush threshold without changing the workload or
reconciliation algorithms. `search_flush_mib=0` retains the backend default,
which was 512 MiB in these OpenSearch runs. Earlier
load builds evolved during diagnosis; compare warm connections, cold mappings,
document and batching columns before comparing their measurements. A failed
large-document reconciliation in an earlier build is explicitly excluded and
repeated with the corrected reader.

CPU and I/O rates are sampled over the steady-state interval. Memory values are
sampled peaks, not continuously measured high-water marks. Backend I/O sums the
test database members and does not measure network traffic. Older rows without
I/O sampling have empty I/O columns. Never use fault-run resource samples as
normal capacity: restarted or replaced containers do not have a continuous
cgroup counter history.

For recovery cases, inspect `fault_confirmed`, `verified`, the first/last error
second and `last_30s_failed_rpcs`. A confirmed fault with zero observed RPC errors
is still a fault experiment, not an undisturbed capacity sample. The injected
fault occurs after setup and connection warmup; its relative time is exported.

Excluded rows cover instrumentation, unexpected benchmark infrastructure
replacement, overlapping correctness work, and the earlier reconciliation
budget issue. They also include a MongoDB fault command rejected by shell
argument validation before it could step down the primary; the corrected
command was rerun successfully. Excluded rows remain available for audit but do not support production
capacity recommendations.

The `shared-single2-rich-*` pair uses the 32 MiB read budget; the
`shared-read8-single2-rich-*` pair uses 8 MiB. Each pair runs MongoDB and
OpenSearch simultaneously for ten minutes. Compare their combined workload and
P99, and do not add the duplicate measurements of the single Sink's resources.

`eks-search-flush.csv` exports sampled per-index flush and translog observations
for the explicitly instrumented comparisons. Times are relative to each
measured workload; absolute times and index names are omitted. Flush counts and
durations are cumulative, so compare successive rows. The observation following
a flush is not its precise start time. The original diagnostic sampler used a
separate loop; newer `run.py --search-stats` collects the same fields alongside
cgroup samples and records sample/error counts in the main result.
