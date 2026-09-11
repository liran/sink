# Sizing a synchronous Sink server

This guide targets self-hosted `mode: server` deployments using synchronous
MongoDB or OpenSearch operations. Kafka acceptance and workers are outside its
scope. The measurements apply to the candidate in
[PR #48](https://github.com/liran/sink/pull/48); use a build containing those
changes before expecting the same behavior. They are observations for a
specified workload, not a universal RPC limit.

## Starting configuration

Start with **two Sink replicas, each requesting and limiting 2 CPUs and 2 GiB**,
on different nodes. Set `GOMEMLIMIT=1536MiB`, `GOGC=400`, a 512 MiB execution
budget and a 32 MiB per-request read/response budget. For modest client
concurrency and small documents, begin with batches of 32 and a 2 ms maximum
wait. The [configuration](../examples/kubernetes/sink.yaml) and
[Deployment](../examples/kubernetes/synchronous-server.yaml) examples make
these limits explicit. These are Sink resources; size the databases separately.

For a deployment dedicated to small documents, use an **8 MiB read/response
budget** after checking the largest original RPC's aggregate snapshots and
results. The simultaneous-store test below sustained 1,750 MongoDB plus 1,500
OpenSearch RPC/s on one 2 CPU Sink with this profile. An initial combined
application budget of about **1,200 MongoDB plus 1,000 OpenSearch RPC/s** retains
roughly 30% headroom. Two replicas can use that same total budget when either
replica must handle all traffic. This profile narrows the request-size contract;
the general-purpose example retains 32 MiB.

Use one replica's validated sustained rate as the total deployment budget when
the service must tolerate losing either replica. Add about 30% application
headroom for workload variation. Concurrent MongoDB and OpenSearch traffic
shares that budget; independent backend capacity numbers cannot be added.

## Sustained production reference

The final server was also tested at a fixed arrival rate for ten minutes per
backend, using **one 2 CPU / 2 GiB Sink**, batches of 32, a 2 ms wait and 128
clients. Each RPC sends one Merge with random 1 KiB padding and 32 additional
fields, waits for applied completion, and returns status without the document.
The database clusters have three members with the replication and durability
settings described below. Every listed run has zero RPC/operation errors,
zero unissued requests, no restarts or OOMs, and a successful final reconciliation.

| Backend | Sustained RPC/s | Successful RPCs | P99 including scheduled wait | Mean Sink CPU | Sampled peak Sink RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| MongoDB 8, three members | 3,500 | 2,100,000 | 18.5 ms | 1.41 cores | 52 MiB |
| OpenSearch, three primaries + one replica per primary | 3,000 | 1,800,000 | 256.6 ms | 0.91 cores | 56 MiB |

For this MongoDB workload, an initial application budget of about **2,400 RPC/s**
leaves roughly 30% below the validated 3,500 RPC/s rate. A two-replica deployment
can use that same total budget to retain one-replica failover capacity. This is
a workload-specific starting point, not a promise for arbitrary Lua programs,
documents or database deployments.

For OpenSearch, treat throughput and low latency as separate operating targets.
The three-primary-shard configuration is a throughput reference for an
application that accepts the measured tail latency, **not a 100 ms capacity
claim**. An initial rate around 2,100 RPC/s leaves 30% below that measured
throughput; validate P99 again at the application's chosen rate. Its translog
threshold remains at the 512 MiB default, with request durability, a 1 s refresh
interval and all shard copies required to be active. More shards did not remove
flush-related pauses.
Three primary shards are a tested reference, not a universal per-index setting.
The number and size of other indexes, placement and recovery overhead also
matter. Unissued requests describe generator backlog at the end of the offered
window; they are not a count of explicit server rejections.

A separate five-minute MongoDB run used random 16 KiB padding, full incoming
documents, 64 clients and the same single-Sink resource limits. It sustained
800 RPC/s, reconciled all 240,000 writes, and had zero errors or unissued
requests. P99 was 9.2 ms, average Sink CPU was 0.74 cores and sampled peak RSS
was 49 MiB. This is an additional tested operating point, not a maximum for
16 KiB records. The incoming padding remains unchanged between updates.

The same single Sink was then tested with both stores writing concurrently for
ten minutes: MongoDB at 1,750 RPC/s and OpenSearch at 1,500 RPC/s, using the rich
document shape and three search primary shards. Both read-budget profiles below
sustained those rates with zero errors or unissued requests, reconciled all
1,950,000 writes, and had no OOMs, restarts or Pod changes.

| Per-request read/response budget | MongoDB P99 | OpenSearch P99 | Mean total Sink CPU | Simultaneous measured window |
| --- | ---: | ---: | ---: | ---: |
| 32 MiB | 249.0 ms | 213.0 ms | 1.35 cores | 595.9 s |
| 8 MiB | 12.4 ms | 26.2 ms | 1.34 cores | 596.7 s |

The 8 MiB profile meets the 100 ms reference target at this combined offered
rate. Both profiles retain a 512 MiB execution budget, batches of 32 and a 2 ms
wait. Lowering the per-RPC budget reduces the conservative working-buffer
reservation of each conditional batch, allowing more small batches to run
concurrently. It does not provide the same maximum snapshot/result size. This
is a measured operating point, not the maximum combined rate.

For the small-document profile, change this field in the complete
[configuration example](../examples/kubernetes/sink.yaml), retaining the other
limits:

```yaml
service:
  max_read_bytes: 8388608
```

Validate aggregate sizes for client batches and returned documents before
using that setting. CPU, execution slots and byte reservations are shared, so
independently measured backend maxima must not be added. Each pair of result
files observes the same Sink CPU and memory; their resource figures are not
additive.

## What determines capacity

Count **RPC/s and operations/s separately**. A unary Write containing one Merge
is one RPC and one operation; a Write containing 100 records is one RPC and up
to 100 successful operations. A batch cannot turn a slow database or expensive
Lua program into a faster one. Returning documents, reading large snapshots,
updating indexed fields, shared-key contention and search visibility each have
different costs.

The reference Merge reads an existing record, runs an idempotent Lua increment,
and conditionally writes the resulting document. Applied completion waits for
backend acknowledgement. MongoDB acknowledgement is majority and journaled;
OpenSearch visible completion additionally waits for index refresh. Do not
substitute async acceptance rates or applied rates for visible-write capacity.
The default Merge sends a small counter delta; it does not send the stored
padding on every RPC. The `--full-incoming` cases also transmit and merge the
padding and fields, making their input processing cost explicit.
Those full-input cases resend the same per-key padding and field values; the
counter and writer sequence change. They do not measure replacing a large blob
with different contents on every write, which can retain full oplog and indexing
costs.

Use two independent acceptance criteria:

1. **Correctness:** reconcile every acknowledged write, including uncertain
   outcomes after timeouts or disconnects. A timeout is not proof that a write
   did not commit. Retrying a non-idempotent increment can apply it twice.
2. **Capacity:** sustain the offered rate with zero errors, no generator backlog,
   acceptable P99, stable memory and no OOMs or unexpected restarts. Keep about
   30% throughput headroom after validating the intended workload. The reference
   small-document target is P99 below 100 ms; a visible search write with a 1 s
   refresh interval requires a different latency target.

## Measurement environment

The Kubernetes runs use Go 1.27.0, ARM64 Linux and m8g.2xlarge worker nodes
(8 vCPU, 32 GiB). Sink and the load generator are separate processes in separate
Pods on different nodes. Each test container has equal CPU/memory requests and
limits; Sink capacity is measured under its actual quota. The load generator
has 4 CPUs and 4 GiB. Nodes may also host unrelated workloads, so these are not
dedicated-host or cross-architecture comparisons.

MongoDB 8.0.30 and OpenSearch 3.8.0 each receive 4 CPUs and 8 GiB **per database
member**, plus a 20 GiB gp3 volume provisioned at 3,000 IOPS and 125 MiB/s.
MongoDB's WiredTiger cache is 3 GiB; OpenSearch uses a 4 GiB heap.
Replicated MongoDB runs use three voting data-bearing members. Replicated
OpenSearch runs use three nodes and one index replica, so the default
single-shard index has two data copies. They explicitly require all configured
shard copies to be active before starting writes and retain request-level
translog durability.
Single-member results and replicated results must
be distinguished. All test nodes were in one availability zone; replication
measurements do not include cross-zone RTT. Backend traffic and gRPC are
internal and unencrypted in this disposable environment. TLS, authentication,
different disks, indexes, data distributions and zone placement require their
own measurements.

The default individual RPC timeout is 5 seconds and begins when the call is
issued. Fixed-rate latency is measured from the scheduled arrival time and
also includes generator backlog. Consequently, scheduled latency can exceed
5 seconds even when individual calls do not time out. Use the application's
actual deadline when reproducing a latency-sensitive workload.

Default records contain 1 KiB of repeated padding, a counter and per-writer
sequence metadata. This data is highly compressible and fits in backend caches.
It is useful for measuring request processing and Lua overhead; it does not
establish storage capacity for a dataset larger than RAM. OpenSearch fields are
created before the steady-state interval and remain normally indexed, with one
primary shard and a 1 second refresh interval. Dynamic-mapping cold starts are
measured separately. The index's total-field limit accommodates the synthetic
writer-sequence fields; these fields remain indexed.

All test resources use one disposable namespace and internal endpoints.
Namespace ownership isolates resource management; it does not establish a
network security boundary. An accepted NetworkPolicy object is also insufficient
evidence of enforcement. Verify the target cluster's CNI separately before
reproducing the experiment. No production endpoint or credential is needed by the
[Kubernetes harness](../benchmarks/kubernetes/README.md).

## Observed small-document ceilings

These **single Sink Pod, single database member** saturation samples use the
simple 1 KiB stored document, one Merge per RPC, applied completion and no
returned document. All listed runs reconciled every record with zero errors,
no OOMs and no unexpected Pod changes. They lasted 30–40 seconds; they are
short ceilings, not sustained production budgets.

| Sink allocation | MongoDB RPC/s | MongoDB P99 | OpenSearch RPC/s | OpenSearch P99 |
| --- | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | 6,849 | 27.2 ms | 9,115 | 21.5 ms |
| 2 CPUs / 2 GiB | 13,605 | 65.8 ms | 18,809 | 52.0 ms |
| 4 CPUs / 4 GiB | 25,579 | 74.6 ms | 34,827 | 54.3 ms |

The 1 CPU profile uses `GOMAXPROCS=1`, 128 clients, batches of 32, an 8 MiB
read limit and a 128 MiB execution budget. The 2 CPU profile uses 512 clients,
batches of 128, a 32 MiB read limit and a 512 MiB execution budget. The 4 CPU
profile uses 1,024 clients and a 1 GiB execution budget, retaining batches of
128 and a 32 MiB read limit. All use `GOGC=400` and a 2 ms batch wait. The
profiles intentionally include the concurrency and memory needed to use the
allocated CPU; this is not a comparison that changes only CPU.
These exploratory CPU profiles use server source `f29c337`; the replicated
measurements below use the final server implementation, `63a0d55`.

For an identical 2 CPU / 2 GiB environment with `GOGC=100`, a 512 MiB
execution budget, a 32 MiB read limit, a 2 ms wait and 512 clients, v0.11.0
completed about 1,652 MongoDB Merge RPC/s at 1,571 ms P99. The bounded working
set and Lua environment changes reached 7,411 RPC/s at 426 ms P99; adding
MongoDB 8 conditional bulk writes reached 10,047 RPC/s at 157 ms P99. Further
batch and GC tuning produced the table above. This separates code changes
from configuration changes. The older OpenSearch baseline timed out at this
offered concurrency and is not a healthy capacity reference.

Neither a 2 ms wait nor a larger batch is universally faster. With 128
clients, batches of 32 produced substantially lower latency than batches of
128. With 512 clients, batches of 128 avoided the reservation competition
seen with many smaller batches. Once a batch fills before its timer expires,
changing the timer has little effect. Compare several concurrency levels at
the real document shape before selecting these settings.

## Document shape and operation type

The following single-member samples use one 2 CPU / 2 GiB Sink, `GOGC=400`,
batches of 128 and the 32 MiB read / 512 MiB execution limits. They are
40-second saturation observations. The padding size excludes counter metadata
and any additional fields; a record with 1 KiB of padding and 32 fields is
larger than 1 KiB. Each row uses one operation per RPC unless stated otherwise.
These exploratory `f29c337` results precede the returned-document admission fix;
the final returned-document comparison appears in the following section.

| Workload | Clients | MongoDB RPC/s / P99 | OpenSearch RPC/s / P99 |
| --- | ---: | ---: | ---: |
| Upsert, 1 KiB padding | 128 | 10,117 / 16.7 ms | 13,750 / 13.7 ms |
| Read, 1 KiB padding | 128 | 19,935 / 8.6 ms | 15,137 / 10.8 ms |
| 80% Merge / 20% Read, 1 KiB padding | 128 | 7,445 / 50.9 ms | 7,591 / 64.5 ms |
| Merge, random 1 KiB + 32 fields, full input | 128 | 4,139 / 52.8 ms | 4,895 / 44.7 ms |
| Merge, random 16 KiB, full input | 128 | 2,436 / 280.5 ms | 1,276 / 704.1 ms |
| Merge, random 256 KiB, small input | 32 | 154 / 572.1 ms | 45 / 1,117.4 ms |
| Merge, return the 1 KiB document | 128 | 2,174 / 327.7 ms | 2,431 / 395.7 ms |

Returning documents retains a per-caller response reservation, so its admission
cost differs from a write that only returns status. A smaller read/response
limit can help a deployment dedicated to small responses, but changes the
maximum supported request/response size. Large stored documents also increase
snapshot conversion and full replacement/indexing costs even when the incoming
Merge delta is tiny. These rows are not all within a 100 ms latency target.

For client batching, 10 operations per RPC at 32 clients reached 1,156 MongoDB
RPC/s (11,561 operations/s, P99 38 ms) and 1,717 OpenSearch RPC/s (17,169
operations/s, P99 29 ms). With 100 operations per RPC, throughput rose to
18,710 and 29,064 operations/s, while whole-RPC P99 rose to 1,192 and 733 ms.
Choose a batch size from the application's latency and response-size needs.

OpenSearch visible completion is a separate workload: with 128 clients and a
1 s refresh interval, unary visible writes reached about 128 RPC/s at 1,041 ms
P99. More Sink CPU cannot remove the requested refresh wait. Ordinary applied
completion and visible completion must not share the same latency target or
capacity budget.
The replicated two-Sink run produced the same approximately 127 RPC/s with
128 clients. This is a concurrency/refresh bound, not a universal visible-write
limit: 128 in-flight unary calls completing about once per second give about
128 RPC/s. More concurrency or client batching can raise operations/s while the
refresh wait remains.

## CPU, memory and admission

The optimized server keeps fresh Lua execution state while reusing immutable
library templates, streams conditional writes through a bounded working set,
and batches MongoDB 8 conditional updates with per-record match results.
Caller byte limits, compare-and-swap retries and durable acknowledgement remain
in force. MongoDB 7 retains the compatible individual-write path.

Large MongoDB conditional replacements use a literal replacement update
pipeline when the encoded document is at least 16 KiB and its root fields are
compatible. MongoDB can then write a delta to the oplog instead of copying an
unchanged large payload. This retains full replacement semantics and BSON
types; it does not make a large network payload or Lua snapshot free. Unusual
root fields retain the original replacement operation, and MongoDB may still
choose a full oplog record when a delta is unsuitable.
[MongoDB pipeline executor](https://github.com/mongodb/mongo/blob/r8.0.30/src/mongo/db/update/pipeline_executor.cpp).
Consumers that watch the underlying MongoDB collection must handle both
`update` and `replace` change events. The delta path can produce `update`
events where the earlier replacement path produced `replace`; configure
full-document lookup if the consumer requires an updated full document.
[MongoDB update events](https://www.mongodb.com/docs/manual/reference/change-events/update/),
[change-stream full documents](https://www.mongodb.com/docs/manual/reference/operator/aggregation/changeStream/).

With random 1 MiB stored padding, a small incoming delta and 16 clients, a
90 second single-member MongoDB comparison changed from 40.3 to 54.1 RPC/s and
814 to 318 ms P99. Sampled MongoDB block writes fell from 101.4 to 3.7 MiB/s.
Both runs had zero errors and reconciled. This shows the benefit for large
unchanged fields; a longer replicated soak is needed to size sustained storage
and replication capacity.

Admission also preserves capacity for an older runnable batch. Without this,
small arriving requests could repeatedly consume the free bytes needed by a
larger returned-document batch, causing deadline failures while CPU remained
available. Cancellation removes its reservation, and a busy store does not
block an unrelated store that can run.

An EKS regression case with one 2 CPU / 2 GiB Sink, 128 clients, 1 KiB returned
documents, an 8 MiB read limit and batches of 64 illustrates the difference.
Before this fix, a 40 second OpenSearch run had 95 deadline failures. The fixed
version ran for 120 seconds with zero errors at 6,881 RPC/s and 27.4 ms P99.
The MongoDB counterpart sustained 4,184 RPC/s with zero errors and 39.0 ms P99,
compared with 84.0 ms P99 before the fix. Both fixed runs fully reconciled.

The [server configuration template](../examples/kubernetes/sink.yaml) and
[Kubernetes example](../examples/kubernetes/synchronous-server.yaml) keep the
measured service limits explicit. The example uses two replicas with 2 CPUs and
2 GiB each, `GOMEMLIMIT=1536MiB`, and `GOGC=400`. Customize backend endpoints
through a Secret containing a `sink.yaml` key and select a tested image release
or digest. The example's hostnames and image tag are placeholders. Sink reads
the mounted configuration file directly; it does not expand environment
variables inside it. Remove unused storage entries, since readiness checks the
configured dependencies.

After editing the image and preparing a private configuration file, apply the
example to a namespace you own. The paths below are illustrative; keep the
populated configuration out of version control.

```sh
kubectl create namespace sink
kubectl -n sink create secret generic sink-server-config \
  --from-file=sink.yaml=/secure/path/sink.yaml
kubectl -n sink apply -f examples/kubernetes/synchronous-server.yaml
kubectl -n sink rollout status deployment/sink-server
```

Apply the project's [production trust-boundary guidance](reliability.md) around
this internal Service, and rerun the chosen rate with that TLS/authentication
path enabled. The sizing example is not an ingress or identity configuration.

The manifest includes readiness, liveness, a rolling-update strategy, node
anti-affinity, a preference for zone spreading and a forty-second native
pre-stop sleep. The sleep action is executed by kubelet and works without a
shell in the distroless image. Validate support in the target Kubernetes version.
Zone spreading is a preference in the example; verify actual placement before
claiming zone-level availability. Keep a third eligible node available for a
surge replica under the strict node anti-affinity rule. An HPA starting around 60–70% of requested CPU
can retain processing headroom, but include queue latency and backend saturation
in the scaling decision and test scale-down with long-lived clients.

Size CPU from a fixed-rate workload with a latency target, then verify memory
with representative maximum documents, Lua outputs and returned-document
batches. Adding CPU helps only when enough independent batches can execute.
At low concurrency, one large batch can leave extra cores idle. A smaller
`service.batching.max_operations` can increase parallel work, but at high
concurrency it can also create more batches competing for execution reservations.

`service.max_in_flight_bytes` is an execution reservation, not an RSS limit.
`service.max_read_bytes` bounds each original RPC's snapshots/results. A
conditional batch reserves up to three read-sized working buffers, plus inputs
and returned-document reservations. Queue storage, decoded objects, Lua heaps,
driver buffers and the Go runtime consume additional memory. Setting a 2 GiB
execution budget in a 2 GiB container is unsafe sizing.

Asynchronous Write/Delete publishing has a separate bounded reservation:
`service.max_publish_requests` defaults to 32 and `service.max_publish_bytes`
defaults to 256 MiB. Include this budget in container sizing alongside the
synchronous execution budget and each Kafka producer buffer. This isolates
durable enqueueing from slow synchronous `refresh=wait_for` writes and their
fair byte waiters. Monitor `sink_admission_pool_bytes` and
`sink_admission_pool_rejected_total` by pool and reason; the legacy in-flight
gauges include both pools.

Keep request-count, byte, queue and Lua limits enabled. Reducing the read limit
can admit more small-document work, but it also narrows the public request size
contract. Measure the largest batch clients actually send before changing it.
Do not increase outstanding requests merely to hide saturation: queue growth
increases latency and makes cancellation/recovery more expensive.

Go's `GOGC` trades memory for collection CPU. Larger values helped this
allocation-heavy Merge workload, but a value selected from 1 KiB documents must
also survive large-document tests. Set `GOMEMLIMIT` below the container limit
and retain room for memory outside the Go runtime. It is a soft runtime target,
not an OOM guard or a replacement for admission limits.
[Go GC guide](https://go.dev/doc/gc-guide).

With current Go versions, container CPU quotas influence the default
`GOMAXPROCS`. Avoid setting it from the host's core count. A one-CPU container
deserves a comparison with explicit `GOMAXPROCS=1`; larger containers should
also be tested at their actual quota. CPU limits can throttle an otherwise
healthy process, so inspect throttling together with P99.
[Go container-aware GOMAXPROCS](https://go.dev/doc/go1.25),
[Kubernetes resource limits](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/).

## Replicas and availability

With the final server and three-member database clusters, the following
60 second saturation runs used small incoming Merge deltas, 1 KiB stored
padding, applied completion and one operation per RPC. All reconciled with zero
errors and stable Pods. These are observed ceilings, including rows whose P99
exceeds the reference 100 ms target.

| Sink Pods | Total clients | MongoDB RPC/s / P99 | OpenSearch RPC/s / P99 |
| --- | ---: | ---: | ---: |
| One, 2 CPUs / 2 GiB | 512 | 12,710 / 62.9 ms | 15,853 / 67.5 ms |
| One, 4 CPUs / 4 GiB | 1,024 | 24,747 / 84.1 ms | 32,294 / 71.4 ms |
| Two, each 2 CPUs / 2 GiB | 1,024 | 22,646 / 106.8 ms | 27,308 / 77.3 ms |

For random 1 KiB padding plus 32 fields with the full document sent on every
Merge, one 2 CPU Sink reached 5,602 MongoDB RPC/s at 147.5 ms P99 and 7,070
OpenSearch RPC/s at 312.7 ms P99. Two 2 CPU Sinks reached 10,807 MongoDB RPC/s
at 93.2 ms P99 and 10,695 OpenSearch RPC/s at 206.6 ms P99. These full-input
runs kept 512 total clients. The two Sinks had similar measured CPU usage;
the weaker search scaling was not caused by all clients selecting one Pod.
Do not add independently observed single-Pod maxima to predict a replicated
deployment's throughput.

For production availability, start with at least two Sink replicas on different
nodes and spread them across failure domains where the cluster permits it.
Use a long-lived gRPC client with effective balancing: a Kubernetes Service
does not distribute the individual calls of an already established HTTP/2
connection. The harness uses four channels, headless DNS and `round_robin`.
Verify load distribution before attributing a weak scaling result to Sink.

Budget for losing a replica. If one replica sustains your required traffic
within the SLO, two can provide that capacity after one is lost; operating both
at their combined saturation point cannot provide the same failover headroom.
Scaling Sink does not scale database CPU, replication or storage throughput.
MongoDB 8 can batch conditional writes while retaining per-record match results;
MongoDB 7 uses the compatible individual-write path and needs its own capacity
measurement. Restart Sink after a MongoDB major-version upgrade to refresh its
cached bulk-command capability.

Shared keys need separate sizing. With 128 writers targeting only 16 shared
keys across two Sinks, the 60 second MongoDB run reported 5,592 CAS conflicts;
the OpenSearch run reported one. All acknowledged state reconciled, but neither
row qualifies as zero-error capacity. Keep conditional-write protection and
bounded retries. Reduce concurrent writers per hot key, or consistently route
a key's writers through one owner when the application supports that design.
Adding replicas does not remove contention on the same record, and longer
retry loops consume more backend work.

Use readiness at `/readyz` on the metrics port and allow graceful gRPC drain.
The server's `shutdown_timeout_seconds` must fit inside the Pod termination
grace period, including any pre-stop delay. Configure a rolling update with
`maxUnavailable: 0`, allow one surge replica, and use a PodDisruptionBudget for
voluntary eviction. A disruption budget does not protect against machine
failure or control a Deployment's rolling-update strategy.
[Kubernetes disruptions](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/),
[container lifecycle](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/).

## Rolling updates and recovery

The recovery runs use two 2 CPU / 2 GiB Sink replicas, four persistent gRPC
channels, `round_robin` and a 30-second minimum DNS re-resolution interval.
The MongoDB traffic below sends 3,500 rich-document Merge RPC/s for three
minutes, with a 5-second call timeout and a 100 ms pause after failures.
Each injected action was confirmed from the database command result or actual
Pod replacement/restart, and every acknowledged write reconciled afterward.

| Sink action | Pre-stop / termination grace | Failed RPCs | Last 30 seconds | Whole-run P99 |
| --- | --- | ---: | --- | ---: |
| Delete one Pod normally | 5 s / 60 s | 0 | 3,500 RPC/s, zero failures | 11.4 ms |
| Roll all Pods shortly after measurement starts | 5 s / 60 s | 15,695 `Unavailable` | 3,500 RPC/s, zero failures | 1,224.5 ms |
| Abort one server process | 5 s / 60 s | 42 `Unavailable` | 3,500 RPC/s, zero failures | 12.3 ms |
| Roll all Pods with longer drain preparation | 40 s / 90 s | 0 | 3,500 RPC/s, zero failures | 11.7 ms |

The short rolling-update run failed during seconds 23–29, after both old
addresses retired and before DNS could refresh again. Repeating it with a
40-second pre-stop delay replaced every original Pod without observed errors
or backlog. The production example therefore uses **40 seconds before stopping
the process and 90 seconds of total termination grace**, including the server's
30-second shutdown budget. Evaluate this together with the actual client
resolver and load balancer; a Pod becoming ready does not by itself prove a
seamless update.

The crash run demonstrates recovery from an ungraceful Sink process exit.
Successful reconciliation does not mean arbitrary retries are safe: use
idempotent application operations for uncertain outcomes. These tests do not
establish behavior during a whole-node power loss or an availability-zone
outage.

Backend failover was tested separately, with three database members and two
Sink replicas using the 40 s / 90 s drain configuration:

| Backend action and workload | Failed RPCs | Error window after workload start | Final 30 seconds |
| --- | ---: | --- | --- |
| Force MongoDB primary stepdown; 400 RPC/s, full 16 KiB input | 78 deadlines | Seconds 34–39 | 400 RPC/s, zero failures |
| Delete a search primary-holder Pod; 3,000 rich-document RPC/s, 3 primaries + 1 replica, active shards `all` | 981, mostly deadlines | Seconds 32–53 | 3,000 RPC/s, zero failures |

Both actions were confirmed, every acknowledged write reconciled, and all
scheduled calls were eventually issued within the offered window. They are
recovery results, not error-free capacity samples. Requiring all configured
search shard copies to be active reduced write availability while a copy was
being restored. Evaluate that policy against the application's durability and
availability requirements. These runs did not compare primary-only write
availability or unclean OpenSearch power loss.

## Overload and return to normal traffic

Two 2 CPU / 2 GiB Sinks were deliberately offered 4,000 RPC/s with 100 operations
per RPC for 45 seconds: 400,000 offered operations/s. Both backends rejected
excess calls with `ResourceExhausted`, accumulated unissued generator work and
reconciled every successful operation. There were no OOMs or Pod changes; the
largest sampled Sink RSS was about 247 MiB. These are overload observations,
not healthy capacity claims.

| Backend | Successful operations/s under overload | ResourceExhausted calls | Unissued scheduled calls | Following 2-minute rich-document run |
| --- | ---: | ---: | ---: | --- |
| MongoDB | 23,890 | 90,366 | 78,688 | 2,000 RPC/s, P99 10.1 ms, zero errors |
| OpenSearch, 3 primaries + 1 replica | 34,141 | 99,118 | 65,334 | 2,000 RPC/s, P99 18.8 ms, zero errors |

The recovery runs also had zero unissued calls and reconciled. Keep admission,
queue and Lua bounds enabled, and use client backoff after rejection. Increasing
the queue can hide saturation temporarily while making response time and
recovery worse.

## Backend preparation and observability

Long runs exposed a search limitation that short samples can miss. With one
primary shard and one replica, the final 2 CPU Sink offered 3,000 rich-document
RPC/s for ten minutes. It reconciled every acknowledged write without an RPC
error, but P99 reached 221 ms and 1,625 scheduled requests were not issued.
Average Sink CPU was only 0.84 cores. That row is not accepted as sustained
capacity within the 100 ms target.

A follow-up run sampled index flush and translog counters. Major throughput
dips coincided with primary flushes taking 1,453 and 1,189 ms; the translog
was near the default 512 MiB threshold before those flushes. Scheduled latency
included the resulting backlog even though ordinary RPC execution P99 was
about 29 ms. This is evidence for a backend flush contribution, not proof that
every latency spike has the same cause. A low average CPU reading alone does
not establish spare capacity.

Neither tuning comparison achieved a 100 ms P99 at 3,000 RPC/s. Three primary
shards completed the full ten-minute offered load, but P99 was 257 ms. Returning
to one primary shard and raising the translog threshold to 1 GiB also completed
the offered load, with a worse 367 ms P99. The reference keeps the default
threshold; the larger value is not recommended from these results.

Keep `index.translog.durability=request` when measuring durable applied writes.
Changing flush frequency does not remove recovery tradeoffs: a larger translog
threshold can reduce flush frequency while increasing recovery work. Compare
flush statistics and recovery time before changing an index template.
[OpenSearch index settings](https://docs.opensearch.org/latest/install-and-configure/configuring-opensearch/index-settings/),
[indexing performance guidance](https://docs.opensearch.org/latest/tuning-your-cluster/performance/).

Create expected OpenSearch mappings before a large writer burst. In this test,
introducing hundreds of new writer-field mappings during the first requests
caused timeouts that disappeared when setup established those same indexed
fields first. Increasing Sink CPU would not resolve that initialization cost.
Keep the refresh interval and replica count explicit; forced refresh per write
or disabled indexing would describe a different workload.
[OpenSearch refresh](https://docs.opensearch.org/latest/api-reference/index-apis/refresh/).

Watch RPC latency and operation statuses together. An RPC transport success can
still contain failed operations. Useful server metrics include
`sink_grpc_server_request_duration_seconds`, `sink_grpc_server_operation_results_total`,
`sink_batcher_request_queue_duration_seconds`, `sink_batcher_queued_bytes`,
`sink_batcher_rejected_total`, `sink_in_flight_bytes`,
`sink_admission_rejected_total` and `sink_write_phase_duration_seconds`.
Pair them with container CPU/throttling/RSS/OOM metrics and backend write,
replication, refresh and disk latency. An increasing queue with flat completion
throughput calls for load reduction or capacity changes, not a longer deadline.

Before adopting a capacity number, rerun the harness with the actual document
shape, indexes, key skew, script, completion mode, returned-document behavior,
client batching and database topology. Use the sustained-rate result as the
sizing input and treat the short saturation result as a ceiling observed under
those conditions.

## Reproduction and retained evidence

The [Kubernetes harness](../benchmarks/kubernetes/README.md) retains generic
plans for CPU and batch sizing, document shapes, returned documents, replicated
backends, sustained and simultaneous traffic, overload, process failure, backend
failover and rolling updates. The [measurement table](../benchmarks/kubernetes/results/eks-arm64.csv)
contains 208 runs, including explicitly excluded diagnostics and intermediate
builds. Read the [column and build explanations](../benchmarks/kubernetes/results/README.md)
before comparing rows. The [flush observations](../benchmarks/kubernetes/results/eks-search-flush.csv)
contain only relative times and numeric index statistics.

The final measured server binary was rebuilt from PR commit `2dd3f94`; its
SHA-256 exactly matched the `63a0d55` server used for the final capacity,
recovery and overload runs. The later commits changed the test tools and
documentation. The Go race, lint and integration checks passed, including
MongoDB and both OpenSearch 2.17 and 3.8 public API conformance. The example
configuration was parsed by Sink and its Kubernetes manifest passed a
server-side dry run.

Published artifacts exclude cluster identities, accounts, namespace names,
addresses, volume identifiers and raw logs. Raw ownership and reconciliation
evidence stays outside the repository.
After the final run, cleanup verified that the test namespace, its PVCs and
all six dynamically provisioned PVs were gone. No VolumeAttachment referenced
an owned PV, and the cloud API confirmed that all six test disks no longer
existed. Production application resources were not changed.
