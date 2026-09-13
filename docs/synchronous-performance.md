# Synchronous write performance

These are the original **local** measurements. See
[production sizing](production-sizing.md) and the
[Kubernetes harness](../benchmarks/kubernetes/README.md) for subsequent tests
with separate client/server Pods, actual CPU quotas and production sizing advice.

These measurements compare v0.11.0 (`026d1d1`) with the bounded synchronous
working set and Lua environment changes on 2026-09-10. They cover
`WAIT_UNTIL_APPLIED` and OpenSearch `WAIT_UNTIL_VISIBLE`. Kafka acceptance and
worker throughput are outside this optimization.

## What changed

Previously, each independent conditional-write RPC reserved two maximum-sized
document buffers. With the default 32 MiB read limit and 256 MiB execution
budget, a collected batch of small independent Merge RPCs was split into groups
of three. A 128-RPC microbenchmark therefore made 43 adapter reads and 43 writes.

Coalesced writes now share a bounded snapshot/output working set. Small records
can use one adapter read and write; larger records stream through multiple
chunks. Each original RPC keeps its own input, snapshot, output, and returned
document limits across chunks. Successfully committed records are completed
immediately; only records with an actual CAS conflict are retried. A batch with
multiple independent records reserves at most three read-sized working buffers
(96 MiB by default), plus its encoded request and any returned-document
reservations. This is an execution reservation, not a bound on process RSS:
driver buffers, Lua VM allocations, queued requests and Go runtime memory also
consume memory.

The Lua engine captures the restricted standard-library environment once per
engine. Each execution still creates a new VM, global/library/metatable graph,
JSON bridge, and request-bound Sink helpers. Only immutable native functions
are shared. The tests cover mutation followed by failure, concurrent executions,
string metatable aliases, and distinct request timestamps.

## Measurement conditions

- Apple M2 host, 8 cores and 24 GiB RAM; Docker Desktop configured with 8 cores
  and 8 GiB RAM. Go 1.27.0 client and server share one process with
  `GOMAXPROCS=4`.
- One Sink gRPC server over loopback TCP with the production VT protobuf codec.
- MongoDB 8.0 single-node replica set and OpenSearch 3.8.0 single node, both in
  disposable local containers. OpenSearch has one shard, no replicas, a 512 MiB
  JVM heap, and a 1 second index refresh interval.
- Each concurrent writer repeatedly increments its own existing document with
  a Lua Merge, preserving 1 KiB of padding. MongoDB uses BSON; OpenSearch uses
  JSON. One operation per RPC; applied-mode results do not return documents.
- There is no shared-key contention. Each sample checks every writer's final
  persisted counter against its successful RPC count. Any RPC failure fails
  the benchmark. Setup and final reconciliation are outside the measured time.
- Baseline and candidate run sequentially, without concurrent race tests or
  qualification workloads. Tables report observed samples, not confidence
  intervals or production capacity guarantees.

## With the production execution budget and batching wait

This comparison uses a 4 GiB execution reservation limit and 10 ms batching
wait. These two settings match the inspected deployment; the local hardware,
database topology and workload do not reproduce production. Each cell below is
one 5 second benchmark sample.

| Backend | Concurrent writers | v0.11.0 ops/s | Candidate ops/s | v0.11.0 P95 | Candidate P95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| MongoDB | 128 | 5,305 | 4,105 | 29.57 ms | 34.71 ms |
| MongoDB | 512 | 5,553 | 7,588 | 336.2 ms | 89.84 ms |
| OpenSearch | 128 | 7,125 | 4,833 | 21.15 ms | 29.31 ms |
| OpenSearch | 512 | 7,615 | 13,712 | 191.4 ms | 53.76 ms |

The larger batch lowers adapter overhead and high-concurrency tail latency.
It also changes the timing of completions: clients can finish together and
then wait for the next collection window. At 128 writers, keeping the 10 ms
wait regresses throughput in this experiment. Deployment tuning must include
the batching wait; increasing memory or concurrency alone is insufficient.

With the same 4 GiB budget and **2 ms** wait on both revisions, single 5 second
samples gave:

| Backend | Writers | v0.11.0 ops/s | Candidate ops/s | v0.11.0 P95 | Candidate P95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| MongoDB | 128 | 5,392 | 6,003 | 33.09 ms | 24.87 ms |
| MongoDB | 512 | 5,564 | 8,872 | 333.5 ms | 98.60 ms |
| OpenSearch | 128 | 7,894 | 9,727 | 22.85 ms | 15.79 ms |
| OpenSearch | 512 | 8,452 | 20,849 | 232.9 ms | 39.69 ms |

At 512 writers, this is approximately 1.59 times baseline throughput for
MongoDB and 2.47 times for OpenSearch under identical settings. Relative to
the baseline's deployment-sized **10 ms** wait, the combined code and wait
change is approximately 1.60 times and 2.74 times respectively. The short
samples should be repeated on deployment hardware before sizing replicas.

## With the default 256 MiB execution budget

At 10 ms batching wait, two interleaved baseline/candidate rounds produced:

| Backend / operation | Writers | v0.11.0 ops/s | Candidate ops/s |
| --- | ---: | ---: | ---: |
| MongoDB Merge | 32 | 1,887–1,931 | 1,407–1,545 |
| MongoDB Merge | 128 | 1,952–1,974 | 4,042–4,043 |
| OpenSearch Merge | 32 | 1,715–1,731 | 1,630–1,647 |
| OpenSearch Merge | 128 | 1,738–1,777 | 4,834–4,924 |
| MongoDB Upsert | 128 | 5,736–5,779 | 5,709–5,754 |
| OpenSearch Upsert | 128 | 6,405–6,431 | 6,181–6,204 |

At 128 writers, Merge allocations per RPC fell from approximately 156.5 KB /
1,319 allocations to 117.6 KB / 847 for MongoDB, and from 128.2 KB / 987 to
86.3 KB / 487 for OpenSearch. Upsert does not execute Lua or read snapshots;
it is a control workload and is not expected to gain the same throughput.

Using the library's existing default **2 ms** batching wait on both revisions
removes the small-concurrency regression in this workload (two samples each):

| Backend | Writers | v0.11.0 ops/s | Candidate ops/s | Candidate P95 |
| --- | ---: | ---: | ---: | ---: |
| MongoDB | 32 | 1,900–1,969 | 3,605–3,615 | 10.20–10.39 ms |
| MongoDB | 128 | 1,967–2,020 | 5,860–5,910 | 26.31–27.08 ms |
| OpenSearch | 32 | 1,765–1,766 | 4,002–4,094 | 9.70–9.92 ms |
| OpenSearch | 128 | 1,742–1,794 | 9,754–9,919 | 14.88–15.75 ms |

Longer 30 second candidate samples at 512 writers, 2 ms wait and the 256 MiB
execution budget completed 287,356 MongoDB operations and 615,626 OpenSearch
operations without an RPC or reconciliation failure:

| Backend | ops/s | P95 | P99 |
| --- | ---: | ---: | ---: |
| MongoDB | 8,052 | 157.5 ms | 253.0 ms |
| OpenSearch | 16,987 | 77.19 ms | 135.1 ms |

The low execution budget admits fewer simultaneous batches and increases queue
tails at this concurrency. Peak throughput and low tail latency are distinct
tuning targets; this is not a recommended production saturation level.

## Search visibility

With the 256 MiB execution budget, 10 ms wait, 32 writers, and a fixed 128
operation run, the baseline exceeded its 30 second request deadline and failed
the benchmark. The candidate completed every operation at 31.91 ops/s, with
P95 and P99 both approximately 1,019 ms. The fixed operation count matters:
an adaptive short benchmark can select too few iterations to sustain the
requested concurrency when the baseline is very slow.

Visible writes wait for OpenSearch refresh. Applied-write throughput numbers
do not describe this completion mode. Changing the completion contract or
disabling refresh is not part of this optimization.

## Reproduce

Start disposable local MongoDB and OpenSearch instances, then set
`SINK_MONGODB_TEST_URI` and `SINK_SEARCH_TEST_ENDPOINT` to their loopback
addresses. MongoDB must be an initialized replica set; OpenSearch must accept
unauthenticated HTTP. The harness rejects non-loopback hosts and creates and
cleans a unique database/index for every sample.

```sh
# Existing-record Merge, default execution budget, explicit batching wait.
SINK_SYNC_BENCH_WAIT=2ms GOMAXPROCS=4 go test -tags=integration \
  ./internal/service -run '^$' \
  -bench '^BenchmarkSynchronousStorage/(mongodb|opensearch)/merge$/concurrency=(32|128|512)$' \
  -benchtime=5s -count=3

# Deployment-sized execution reservation and wait, same workload.
SINK_SYNC_BENCH_EXECUTION_MIB=4096 SINK_SYNC_BENCH_WAIT=10ms GOMAXPROCS=4 \
  go test -tags=integration ./internal/service -run '^$' \
  -bench '^BenchmarkSynchronousStorage/(mongodb|opensearch)/merge$/concurrency=(128|512)$' \
  -benchtime=5s -count=3

# Fixed iteration count for visibility; any timeout is a failed result.
GOMAXPROCS=4 go test -tags=integration ./internal/service -run '^$' \
  -bench '^BenchmarkSynchronousStorage/opensearch/merge-visible/concurrency=32$' \
  -benchtime=128x -count=1

# Deterministic adapter round-trip benchmark and real-backend chunk checks.
go test ./internal/service -run '^$' \
  -bench '^BenchmarkSynchronousMergeMicrobatch$' -benchtime=10x -count=3
go test -race -tags=integration ./internal/service \
  -run '^TestSynchronousStorageStreamsLargeRecords$' -count=1
```

To compare against v0.11.0, create a detached worktree at `026d1d1` and copy
`internal/service/sync_capacity_benchmark_test.go` and
`internal/service/sync_capacity_integration_test.go` into that worktree. Run
only the benchmarks there; the new chunk regression test intentionally checks
behavior absent from the baseline. Use identical environment variables for
both revisions. Do not run benchmarks alongside other CPU or database tests.

## Capacity tuning and limits

For an applied-write-heavy deployment, start with the existing 2 ms batching
wait and compare 32, 128, and 512 independent outstanding operations using
representative records and scripts. Keep the request, store, byte and queue
limits enabled. Returning documents, large records, mixed stores and hot keys
need separate measurements; their memory, ordering and CAS constraints remain.

The candidate used for these local measurements obtained MongoDB conditional
matched counts individually, with bounded parallelism. Subsequent Kubernetes
work adds MongoDB 8 client bulk writes with per-record results and retains the
individual path for older servers. OpenSearch conditional writes use bulk
requests. Lua execution, backend I/O, indexes, replication, network latency,
connection pools and CPU eventually determine throughput after admission stops
fragmenting batches. These short local measurements do not establish a
universal maximum or a safe production admission rate. Validate the intended
CPU quota and backend topology with a sustained workload and P95/P99/error-rate
targets before increasing production load.

## Validation evidence

The candidate passed `go test -race ./... -count=1`,
`go vet -tags=integration ./...`, and Staticcheck v0.8.1 with all checks and
integration tags enabled. Real MongoDB 8.0, OpenSearch 3.8.0 and Elasticsearch
8.19.20 storage suites passed with the race detector. The new service integration
test forced snapshot and expanded-output chunks through MongoDB/OpenSearch,
checked returned documents, and reconciled each persisted increment.

The independent `sink-production-suite` at `7917c33` passed all 28 top-level
conformance tests with no skips (771 seconds). These include crash boundaries,
lost responses, cancellation after commit, concurrent histories, hot-key folding,
successful-sibling isolation, visibility isolation, original-RPC budgets,
returned documents and slow-store saturation.

The full `make test-production` gate also passed: representative business and
multi-replica histories, backend operations/query/scan/returned documents,
restart recovery, representative load, and the three-minute fault workload.
The fault workload reconciled 3,492 logical operations in 582 cycles with eight
clients, including two recovered transient outcomes and two mutation retries.
It injected a worker SIGKILL, a roughly 45 second OpenSearch outage and a broker
restart. Final readiness, backlog/DLQ checks and repair/replay checks passed.
This short correctness gate does not establish a two-hour endurance result.

On macOS Bash 3.2, the suite's empty optional-argument array fails under
`set -u`. The successful run set `SINK_GO_DIR` to the Go module cache directory
for the suite's unchanged pinned SDK revision
`v0.4.1-0.20260909050935-91fd66561c64`, using the existing local-SDK option.
No suite or SDK source was modified.

## Read batching

Read micro-batches share one bounded snapshot buffer and one output buffer, each
limited by `service.max_read_bytes`. Admission reserves those two buffers plus
the encoded request instead of reserving two buffers for every original RPC.
Small records from independent callers can therefore share one backend read.
For 128 collected single-record reads, the default 32 MiB read limit and 256 MiB
execution budget previously caused 43 backend calls; the regression test now
requires one call for 1 KiB records. This measures adapter rounds, not production
throughput.

Every original RPC retains its byte limit, result order, and repeated-key
deduplication. If the snapshot or copied output cannot fit, affected RPCs are
read again in smaller groups before returning any of their results. Completed
callers are released immediately. A large caller can still receive per-operation
resource-limit failures under its own quota. Read batches execute concurrently
up to the existing process and per-store request limits; byte admission may
further reduce active concurrency.
