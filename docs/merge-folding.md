# Ordered merge folding

Synchronous writes fold each consecutive run of merges for the same complete
record address into one conditional backend commit. Each Lua program still runs
in order against the preceding successful in-memory result. This removes repeated
reads, writes, and visibility waits for a hot document without changing refresh
configuration or dropping any merge intent.

## Scope and ordering

The execution planner groups by store, namespace, dataset, and typed record key.
Different addresses may execute together in a backend bulk request. Interleaved
operations for other addresses do not split a run. A valid put (create, replace,
or upsert) for the same address ends the run and executes separately before the
next run. Invalid operations retain their individual validation failure.

Folding applies to explicit synchronous Write batches and to the existing
process-local micro-batcher's combined requests with the same completion mode. It does not extend the collection
window, span running batches or replicas, or buffer writes after returning success.
Read and Delete use independent execution paths. Async acceptance still publishes
each original operation to Kafka; worker per-address failure barriers remain intact.

## Commit and result contract

1. Read one document and its revision per run.
2. Run every Lua program using its own input, encoding, missing-document policy,
   and fixed observation time. A successful result becomes the next program's
   current document. A Lua or missing-document failure leaves it unchanged.
3. Commit the final successful result with the original read's revision condition,
   or a record-not-exists condition if the run creates the document.
4. After a successful commit, return APPLIED for successful programs and retain
   each failed program's failure. All successful operations in the run share the
   actual final commit revision. Preserve original operation indexes and RPC
   response boundaries. If no program succeeds, return the evaluated failures
   without issuing a backend write.

WAIT_UNTIL_VISIBLE waits for the final committed state to become searchable,
using the existing refresh=wait_for path. Synchronous micro-batches keep completion
modes separate: a request that only needs application must not inherit another
record's refresh wait. A mode change on the same address separates folding groups
and preserves submission order. No caller is acknowledged from memory.

This is a **final-state commit contract**. Intermediate documents are not written
or independently made visible. Backend schema validation, generated/default
fields, ingest processing, revision increments, change streams, and audit events
apply to the final backend write, not each in-memory step. Lua sees intermediate
program output without backend normalization. Callers requiring independent
commits or intermediate backend effects must issue sequential calls and await
each result before submitting the next. This behavior must be included in release
notes; it is not observationally equivalent to individual backend writes.

## Conflicts and failures

A definite revision conflict restarts the entire run from a fresh read, including
programs that failed on the previous speculative state. Observation times remain
fixed across attempts. Exhaustion reports a retryable conflict for every operation
in the unresolved run. Another replica or a concurrent delete can change the base;
the backend revision condition remains the concurrency boundary.

A failed final commit reports its failure for every operation in that run,
including provisional Lua failures that may depend on an uncommitted predecessor.
Other document runs retain their independent results. A transport failure, lost
acknowledgement, or cancellation can leave the commit outcome unknown; Sink does
not replay such a write internally. Existing application idempotence requirements
still apply. Folding does not provide exactly-once execution or batch transactions.

## Resource and performance bounds

The existing operation count, admission bytes, batch collection limits, request
deadline, Lua instruction/time/result limits, and aggregate read/output budgets
remain in force. Each run retains its original intents and one working document;
it does not retain every intermediate document. Conflict attempts remain bounded.

For N merges to one address in one run, without conflicts, backend document reads
and writes drop from N to one; Lua still executes N times. Visibility requires one
backend wait. Unique-document workloads retain one read and write per document.
Cross-replica contention, sparse arrivals, Lua CPU cost, and backend latency limit
the actual latency improvement. Benchmarks must report hot/mixed/unique workloads,
backend work, latency distribution, allocations, and conflicts separately.

## Local comparison (2026-09-06)

Compared against a5dadf4 on Apple M2 / darwin-arm64 with GOMAXPROCS=4, using the
same benchmark and helpers in both worktrees. Each sample submits 64 merges;
100 samples per workload, sequential batches, no injected revision conflicts.
Mixed traffic alternates 32 updates to one hot key with 32 unique keys. The
controlled I/O case sleeps 1 ms per storage Read/Write call, independently of
document count. It is not a model of production refresh timing or an SLA forecast.

| Workload | Read/write documents per batch, before → after | Visibility waits, before → after | Mean batch latency with I/O delay, before → after | P99 with I/O delay, before → after |
| --- | --- | --- | --- | --- |
| Hot | 64 → 1 | 64 → 1 | 161.63 ms → 4.76 ms | 246.77 ms → 5.32 ms |
| Mixed | 64 → 33 | 32 → 1 | 84.59 ms → 4.89 ms | 88.57 ms → 5.35 ms |
| Unique | 64 → 64 | 1 → 1 | 5.13 ms → 5.00 ms | 7.63 ms → 5.53 ms |

Without the artificial I/O delay, mean hot/mixed/unique batch times were
2.30/1.97/2.03 ms before and 1.97/1.94/2.06 ms after. Unique-key CPU latency
increased about 1.7% in this single run; do not infer statistical significance
from one run. Hot allocations dropped from 40,697 to 39,373 per batch and retained
allocation volume from about 5.34 MB to 5.24 MB. Lua execution still dominates
the memory-only workload. Quantiles from 100 samples are sensitive to scheduling.

Reproduce with `GOMAXPROCS=4 go test ./internal/service -run '^$'
-bench '^BenchmarkMergeFolding$' -benchtime=100x -count=1`, copying the same
benchmark and test helpers into a detached baseline worktree. Race tests separately
cover concurrent servers, whole-run conflict retries, cancellation, failed commits,
and lost acknowledgements. Tagged integration tests check real backend visibility,
concurrent folded updates, shared revisions, and BSON types.

### Contended commits

`BenchmarkMergeFoldingContention` runs four concurrent callers directly against
the core service, each submitting 64 merges to the same key (100 batches total,
GOMAXPROCS=4, 1 ms per storage call). Both versions use 1,000 maximum conflict
attempts to measure recomputation rather than attempt-budget exhaustion; this is
**not** the production default of three. The benchmark verifies all 6,400 increments
persist exactly once in this run. This does not change the delivery guarantee.

| Measurement | Before | After |
| --- | --- | --- |
| Amortized wall time per batch | 120.15 ms | 6.74 ms |
| Request latency P50 / P99 | 481.59 / 740.04 ms | 7.11 / 160.94 ms |
| Document reads and write attempts per batch | 197.1 | 3.75 |
| Conflicts per batch | 133.1 | 2.75 |
| Conflicts / write attempts | 67.53% | 73.33% |
| Allocated bytes per batch | 16.25 MB | 19.41 MB |

Folding greatly reduces backend work here, but a conflict re-executes the entire
Lua chain. This run increased allocated bytes and the fraction of conflicting
attempts; it does not establish a reduction in conflict probability. Production
rollout must observe attempt exhaustion, CPU/GC, and tail latency under the actual
replica count and unchanged retry budget. The synchronous store batcher serializes
its own dispatched batches; this direct-core test deliberately exercises contention
that can also occur between replicas. No global ordering or fencing is introduced.
