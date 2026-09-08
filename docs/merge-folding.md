# Record operation folding

Synchronous requests fold repeated operations for the same complete record
address within one execution batch. Each original operation still receives its
own indexed result. Folding reduces backend work and visibility waits; it never
acknowledges a mutation before its final backend result.

## Scope and ordering

Identity includes store, namespace, dataset, and typed record key. Operations
for different addresses remain independent. Folding applies to explicit core
requests and the micro-batcher's combined requests. Mutation completion modes
remain separate, and a mode change for the same address remains an ordering
barrier. Folding does not span running batches, replicas, or RPC methods.
Automatic mutation batches also stay within one namespace and dataset; explicit
multi-dataset RPCs keep their own boundary and are not combined with other RPCs.

| Operations for one address | Backend work without conflicts |
| --- | --- |
| Upsert only | Write the last document once, without a read |
| One Create or Replace | One direct conditional write, as before |
| Repeated Create/Replace/Upsert, or Put mixed with Merge | Read once, evaluate in order, commit the final successful document once |
| Merge only | Read once, execute each Lua program in order, commit once |
| Any Write in the chain requests `return_document` | Execute each operation separately in order, returning its own committed logical document and revision when requested |
| Repeated Read | Fetch once, return separate result objects from that observation |
| Repeated synchronous Delete | Delete once, return the same outcome to every operation |

A Put replaces the working document at its position in a write chain; it no
longer splits the chain. Interleaved operations for other addresses do not split
it either. Invalid addresses, payloads, actions, and Lua declarations retain
their validation failures and are excluded from the execution plan.

Async acceptance publishes every original operation. The worker's per-address
failure barriers and execution waves remain in place, so queued mutations for
one address are still applied separately. Write and Delete use independent RPCs
and queues; a Put and a Delete are not folded together.

## Write commit and result contract

The folding rules below apply when no operation in the chain requests a returned
document. `return_document` opts the entire same-address chain into separate
commits, retaining the batcher's ordering barrier. Only APPLIED operations
requesting a document receive one. Asynchronous completion rejects this option.
Returned documents are the logical Put/Merge outputs; backend-generated fields
and ingest transformations are excluded. See [returned writes](native-access.md#returned-writes).

1. A chain containing only Upserts writes its last document directly. A single
   Put retains the adapter's existing precondition handling.
2. Other chains read one document and its revision, or observe that it is absent.
3. Evaluate each operation against the preceding successful in-memory state.
   Create succeeds only if that state is absent; Replace succeeds only if it is
   present; Upsert replaces it unconditionally. A successful Put makes it present.
   Merge executes its Lua program and missing-document policy as before.
4. A failed condition or Lua program leaves the working state unchanged. Commit
   the final successful document using the original snapshot's revision, or a
   record-not-exists condition for an initially absent document.
5. After a successful commit, successful operations return APPLIED with the
   same final revision. Preserve the individually evaluated failures and original
   operation indexes/RPC boundaries. If no operation succeeds, return the
   evaluated failures without writing.

For example, on a missing document, `Create(A), Create(B), Replace(C)` commits C
once and returns APPLIED, PRECONDITION_FAILED, APPLIED. `Upsert(A), Merge(B),
Upsert(C), Merge(D)` evaluates both Lua programs in order and commits the result
of merging D into C. A failed final commit leaves the whole chain unresolved,
including conditional/Lua failures evaluated on its speculative state.

The batcher delivers each original RPC as soon as all of its operations have
final results, preserving operation indexes and independent result objects.
Finished document chains release their scheduling dependencies without waiting
for unrelated documents' reads or retries. A later execution error is returned
only to RPCs whose results are still incomplete. Backend bulk response barriers
remain: results cannot be delivered before the backend provides them. Admission
reservations and execution slots remain held until the execution finishes.

WAIT_UNTIL_VISIBLE waits for the final committed state using the existing
refresh=wait_for path. WAIT_UNTIL_APPLIED does not acquire a stronger requirement.
Repeated Deletes wait for their one backend delete and requested visibility.

This is a **final-state commit contract** for Put as well as Merge. Intermediate
documents are not independently persisted or validated by the backend. Schema
validation, generated/default fields, ingest processing, revision increments,
change streams, audit events, and visibility apply to the final backend write.
Lua sees intermediate program or Put output without backend normalization.
Successful operations may be superseded later in the same chain. Callers needing
independent commits can request returned documents or issue sequential calls and
wait for each result. Separate read observations require explicit Reads. Releases containing this
change must describe these semantics.

## Conflicts, failures, and resource bounds

A definite revision conflict restarts the whole snapshot-based chain, including
previous conditional/Lua failures. Lua observation times remain fixed. The
existing service.max_merge_attempts limit also bounds folded conditional Put
chains. Exhaustion returns a retryable CONFLICT for every operation in that
unresolved chain. An ambiguous transport failure, lost acknowledgement, or
cancellation is not replayed internally. Existing business idempotence
requirements still apply; folding does not provide exactly-once execution or
batch transactions.

Reads share one backend observation per address but produce independent payload
and revision copies. Every returned document, including repetitions, consumes
the response budget in original input order. Deduplication therefore does not
allow repeated keys to bypass max_read_bytes. The backend's unique-document
read also remains bounded. Missing/error results are returned to all matching
operations.

Snapshot-based Put chains reserve read/output admission space before execution,
just like Merge. Operation limits, byte limits, request deadlines, bounded
conflict attempts, and Lua limits remain in force. Single Puts and all-Upsert
chains retain their direct path. Existing merge metrics continue counting Lua
operations rather than inflating them with Puts; they are not total folding
metrics for Read/Put/Delete.

Write/Delete dispatchers also track record dependencies across queued and active
batches. Independent later requests can run during a previous refresh wait,
within bounded execution capacity; same-record and multi-record dependency
chains keep their order. This ordering remains local to one store/method queue.

Micro-batches preserve original RPC snapshot/output quotas. Shared observations
cannot make a healthy RPC inherit another caller's quota failure. A conditional
RPC's working state is admitted before later callers can use it; failed output
is excluded from the folded document. Worst-case reservations are charged per
RPC, sharing physical snapshot/output reservations for hot records; oversized
combined executions split at RPC boundaries. This can reduce
cross-RPC folding when large per-RPC budgets approach the process byte limit;
it does not affect folding within an explicit RPC.

## Validation

The service tests exhaust all 486 five-Put sequences across Create/Replace/Upsert
and initially present/absent records, comparing each result and final state with
sequential commits. Additional tests cover mixed Put/Merge ordering, whole-chain
conflict recomputation, failed/lost commits, output and admission bounds, read
response copies and repeated-key budgets, full address isolation, cross-RPC
folding, and preservation of asynchronous intents. Existing completion-mode and
cancellation regressions remain applicable. Tagged MongoDB/Search integration
cases exercise mixed writes, final revisions, BSON dates, and search visibility.
Additional regressions cover multiline Bulk JSON, bounded Replace revision
retries (including unknown acknowledgements and successful siblings), per-RPC
budget isolation, memory-limited splitting, and cross-batch dependencies and
shutdown. Real Search tests create an actual revision conflict and disable
automatic refresh while later applied writes/deletes complete.

The historical Merge measurements below predate folding of Puts, Reads, and
Deletes. New operation measurements use BenchmarkRecordFolding.

## Operation comparison (2026-09-07)

Compared with cee5dd5 on Apple M2 / darwin-arm64, GOMAXPROCS=4, using the same
BenchmarkRecordFolding source against both implementations. Each batch contains
64 operations. The table uses one hot record and 30 batches, with an artificial
1 ms delay per storage adapter call. Counts are operations passed from the core
to the adapter; they do not count an adapter's internal database round trips.

| Workload | Reads / writes / deletes per batch, before → after | Visibility waits, before → after | Mean milliseconds per batch, before → after |
| --- | --- | --- | --- |
| upsert | 0 / 64 / 0 → 0 / 1 / 0 | 64 → 1 | 82.23 → 1.33 |
| replace | 0 / 64 / 0 → 1 / 1 / 0 | 64 → 1 | 82.28 → 2.64 |
| put_merge | 32 / 64 / 0 → 1 / 1 / 0 | 64 → 1 | 130.15 → 4.03 |
| read | 64 / 0 / 0 → 1 / 0 / 0 | 0 → 0 | 1.32 → 1.31 |
| delete | 0 / 0 / 64 → 0 / 0 / 1 | 1 → 1 | 1.35 → 1.33 |

Read/Delete were already sent in one adapter call, so reducing their document
operation count does not remove the artificial call delay. The Delete benchmark
seeds the record before timing; later iterations also exercise missing deletes.
These controlled measurements are not production latency predictions.

Deduplication has a cost when every address is unique. A separate memory-only
comparison used 1,000 batches per run, three runs, and the median run mean. It
kept the same backend operation count but measured extra lookup/admission work:

| 64 unique records | Microseconds per batch, before → after | Allocated bytes per batch, before → after |
| --- | --- | --- |
| replace | 41.94 → 64.97 | 114,353 → 138,737 |
| read | 16.40 → 30.64 | 50,632 → 75,288 |
| delete | 11.00 → 23.88 | 35,888 → 60,520 |

The benefit therefore depends on repetition, document size, backend latency,
and contention. Do not describe folding as a speedup for every workload.

Reproduce with `GOMAXPROCS=4 go test ./internal/service -run '^$'
-bench '^BenchmarkRecordFolding$' -benchtime=30x -count=1`, running the same
benchmark and test helpers against the baseline and candidate code. For the CPU
comparison select `BenchmarkRecordFolding/(replace|read|delete)/(hot|unique)/io=0s`
with `-benchtime=1000x -count=3`.

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
