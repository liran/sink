# Reliability and recovery

Sink provides at-least-once asynchronous delivery and per-record conditional
updates. **Business idempotence belongs to the application.** Sink does not
deduplicate logical mutations. Client retries, lost backend acknowledgements,
worker restarts, and DLQ replay can repeat business effects. Use a business
operation ID with atomic check-and-apply logic when repeated effects are unsafe.

## Interpret outcomes

| Outcome | Meaning and action |
| --- | --- |
| `ACCEPTED` | Kafka acknowledged the mutation. It may still be pending, applied, or quarantined. It is not a database commit receipt. |
| `APPLIED` / visible completion | The backend acknowledged the requested operation/visibility. Durability depends on the configured backend and cluster. |
| RPC cancellation, deadline, transport failure | Some effects may already have happened. Reconcile the business state before retrying operations without idempotence. |
| Admission rejection | Core execution capacity was unavailable before execution. Back off and retry with jitter. |
| Per-operation permanent failure | Inspect and correct the operation. Retrying identical invalid input does not repair it. |
| Per-operation temporary failure | Retry only unresolved operations, respecting business idempotence and ordering. |

There is no persistent per-mutation status API. Applications that need end-to-end
reconciliation should keep their own accepted-operation ledger, compare business
IDs against stored data, and reconcile source lag and DLQ. An empty source lag
alone does not mean all accepted mutations succeeded: some may be in DLQ.

## Failure handling

Temporary backend failures are retried beyond `max_retry_attempts`; that setting
limits attempts in one processing round. Unresolved records remain in Kafka.
Workers commit only resolved contiguous partition prefixes. DLQ or commit
failure retains the affected source records for replay. Permanent failure of one
record does not suppress subsequent valid operations for the same address;
temporary failure does preserve the same-address barrier.

Processing defaults to 20 seconds. A pending rebalance cancels backend work and
allows at most five additional seconds for DLQ/offset settlement before releasing
rebalance callbacks. This improves bounded handover but is not storage fencing.
Unknown in-flight writes can complete; adapters must honor cancellation, and
business operations must tolerate replay. Keep one ordered asynchronous path
for order-sensitive records. Concurrent sync writes, multiple producers, and
late DLQ replay need application-level version/order checks.

Source retention is still a finite recovery window. The default is 72 hours;
set it longer than outage detection, repair, and backlog drain time combined.
DLQ defaults to 30 days. Source offset gaps fail visibly instead of silently
resetting. If a gap occurs, stop the affected path, preserve its positions,
recover business state from the producer ledger/backups, choose replacement
positions explicitly, and restart the worker. Do not reset to latest merely to
make lag or health green.

## Inspect and replay dead letters

Use the existing store configuration. Inspection joins no consumer group and
changes no offsets. It reports positions, payload hashes and error/source headers;
document payloads are not printed. Select a concrete range of 1–100 consecutive
records, at most 64 MiB. Missing/expired offsets fail the command.

```sh
sink dlq inspect --config /etc/sink/config.yaml --store primary \
  --partition 0 --offset 12 --count 3 > dlq-inspection.jsonl
```

Repair the cause, review the selected records' business intent, and preserve the
inspection output in the incident record. Replay validates the entire selection's
envelope/store before republishing, but publishing the selection is not atomic.
It does not remove DLQ records or advance the worker's group offsets:

```sh
sink dlq replay --config /etc/sink/config.yaml --store primary \
  --partition 0 --offset 12 --count 3 > dlq-replay.jsonl
```

Replay reports `accepted` or `failed_or_unknown` for each selected position and
returns failure if any publication fails. If the command or output file is
interrupted, outcomes may be unknown. Never repeatedly replay a whole range to
test whether the error disappeared. Reconcile its business IDs and store a
resolution record containing source/DLQ positions, hash, replay time, operator,
and observed business result. Resolve only after verifying persisted state.
Replayed older operations can overwrite newer state unless the business rule
checks its version. Malformed or wrong-store envelopes require manual correction
through the normal API rather than the replay command.

## Durability baseline

For production Kafka, start with RF=3, minimum ISR=2, all-ISR producer
acknowledgements, clean leader election, and replicas distributed across failure
domains. Loss of sufficient ISR must reject writes rather than weaken durability.
Sink reconciles the Topic policy, but rack placement, broker storage, replication
fetch limits, cluster permissions, and backup/recovery remain deployment work.
Do not configure servers and workers with conflicting policies.

Sink's MongoDB client uses `w=majority` and `journal=true`, with bounded server
selection and request deadlines. Before upgrade, verify the replica set and
journaling configuration, election behavior, and capacity. A write-concern timeout
does not roll back an already executed mutation.

For Elasticsearch/OpenSearch, verify index replicas across failure domains,
`index.translog.durability=request`, shard health, disk watermarks, and snapshot
restore. Sink does not create/manage index settings. Confirm acknowledgement and
visibility semantics against the cluster actually being deployed.

Do not change partition counts online. Pause every publisher, drain and settle
old work, record source/DLQ positions, create or explicitly migrate the target
Topic, update all clients together, and resume only after verifying routing.
Changing only a Sink config is refused to protect ordering.

## Capacity, isolation and health

All core calls, including async, batching bypass, and cross-store calls, share
process and per-store request limits plus byte reservations. Reads and synchronous
merges reserve output space before execution; MongoDB group/write limits are
shared across requests. Kafka producers have bounded byte buffers and fail fast
when full. Size these budgets alongside per-store/method waiting queues,
transport buffers, Go object/driver overhead, VM working sets, and replica count.
The admission byte gauge is not process RSS.

Read budgets include repeated keys and all stores. Search reads split a response
that exceeds their transport budget before treating an individual document as
oversized. Lua output traversal bounds alias expansion, depth and node count
before constructing Go output values. The current embedded Lua VM does not
provide a strict execution heap quota. Reviewed scripts and container memory
limits are still required; independent server/worker and store deployments limit
the impact of a process OOM. Untrusted-script isolation requires a VM with an
enforced allocation quota or a separate execution process; this is not claimed
by the current implementation.

Dependency failures do not prevent unrelated stores starting. Kafka acceptance
and consumption remain gated until their Topic policy is established. gRPC
dependency health begins `NOT_SERVING`; the default health service describes
the serving process, not all stores. With Prometheus enabled, use `/livez` for
liveness and `/readyz` for dependency readiness; a `service` query selects one
`sink.storage.<store>`, `sink.kafka.<store>`, or `sink.worker.<store>` check.
Readiness failure during an outage should not trigger liveness restart loops.

Protect gRPC and metrics with an authenticated TLS ingress/service mesh and
network policy. Restrict who can submit Lua and which storage credentials Sink
uses. The record's `store`, namespace and dataset are routing fields, not
authorization checks. Validate these deployment controls before exposing Sink.

## Monitoring and acceptance

Use [example alert rules](../deploy/reliability-alerts.yaml) and configure Kafka
exporter/group monitoring for each source and DLQ. Track source lag/oldest age,
DLQ resolution lag, last committed progress, fetch/commit recovery, admission
rejection, RSS, goroutines and P99 latency. Internal pending metrics cover the
last fetched batch only, not unpolled backlog. End-to-end business latency needs
the application ledger; the internal delivery histogram also includes quarantine.

Release evidence should include the following cases, with business outcomes and
resource usage recorded rather than only API success counts:

| Layer | Required checks |
| --- | --- |
| Every change | Race tests, cancellation/all-caller cancellation, CREATE replay continuation, permanent size errors, temporary outage beyond retry budget, DLQ failure without source commit, partition prefix progress, rebalance cancellation, expanded Lua/read output bounds, codec fuzz. |
| Sustained testing | Mixed sync/async and cross-store traffic; slow backends, saturation, worker/broker restarts, lag recovery; capture RSS, goroutines, latency, DLQ and business state. Use product retry defaults. |
| Deployment qualification | Multiple brokers/replica-set nodes/fault domains; active-consumer SIGKILL, network partitions, lost acknowledgements, election, full disk, offset retention gaps, backups and restore. |

Record the exact server/suite revisions, configuration, fault timeline, accepted
business IDs, final reconciliation, maximum resource usage and backlog drain
time. Define SLO, RPO and RTO from business requirements and validate them with
these measurements. Passing local/fake-broker tests alone is not production
reliability certification.
