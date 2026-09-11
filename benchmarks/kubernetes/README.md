# Disposable Kubernetes capacity tests

This opt-in harness runs real synchronous gRPC operations against fresh MongoDB
and OpenSearch datasets. Ordinary `go test ./...` does not access Kubernetes or
external databases. Use a cluster whose capacity and cloud costs you control.

All Pods, Services, ConfigMaps, policies and PVCs live in one generated namespace.
The StorageClass must use `reclaimPolicy: Delete`. The harness records the
namespace UID and a hash of the active kubectl context before provisioning, then
checks both before mutations and cleanup. API calls stay bound to that validated
context even if the caller changes their active context during a run. It never
exports kubeconfig contents.
Keep the state file and raw evidence outside the repository.

Prerequisites: Python 3.10+, Go matching `go.mod`, kubectl, a working current
context, a dynamic block StorageClass, and Linux nodes matching the selected CPU
architecture. The default databases each request and limit 4 CPUs and 8 GiB,
with a 20 GiB volume. Each additional database replica has the same allocation.
Namespace quotas cap the experiment at 48 CPUs, 96 GiB RAM and 160 GiB storage.

The NetworkPolicy permits same-namespace traffic and DNS. **It only isolates
traffic when the cluster CNI enforces NetworkPolicy.** Confirm enforcement in
your own cluster; a successfully created policy is insufficient. The test
databases use unauthenticated internal endpoints, so do not expose Services or
add an Ingress. No production database address or credentials are needed.

## Build and initialize

Run from the repository root. Select the architecture of the Kubernetes nodes,
not the architecture of your laptop. Omit `--node-type` to allow any matching
Linux nodes; specifying it makes CPU comparisons easier to reproduce.

```sh
export SINK_BENCH_DIR="$(mktemp -d /tmp/sink-kubernetes.XXXXXX)"
export SINK_BENCH_STATE="$SINK_BENCH_DIR/state.json"
export SINK_BENCH_ARCH=arm64

CGO_ENABLED=0 GOOS=linux GOARCH="$SINK_BENCH_ARCH" go build \
  -trimpath -ldflags='-s -w' -o "$SINK_BENCH_DIR/sink-candidate" ./cmd/sink
CGO_ENABLED=0 GOOS=linux GOARCH="$SINK_BENCH_ARCH" go build \
  -trimpath -ldflags='-s -w' -o "$SINK_BENCH_DIR/sink-perf" ./cmd/sink-perf

python3 benchmarks/kubernetes/cluster.py --state "$SINK_BENCH_STATE" init \
  --arch "$SINK_BENCH_ARCH" --storage-class gp3
python3 benchmarks/kubernetes/cluster.py --state "$SINK_BENCH_STATE" databases --replicas 1
python3 benchmarks/kubernetes/cluster.py --state "$SINK_BENCH_STATE" upload "$SINK_BENCH_DIR/sink-candidate"
python3 benchmarks/kubernetes/cluster.py --state "$SINK_BENCH_STATE" upload "$SINK_BENCH_DIR/sink-perf"
python3 benchmarks/kubernetes/cluster.py --state "$SINK_BENCH_STATE" server
python3 benchmarks/kubernetes/cluster.py --state "$SINK_BENCH_STATE" load
```

Wait for the Sink and load Deployments to become ready before measuring. Sink
replicas and the load generator have required node anti-affinity. Other test
Pods prefer separate nodes. CPU and memory requests equal limits, giving each
measurement a real container quota. Hosts can still run other workloads; this
does not create dedicated nodes. Test Pods carry Karpenter's
`karpenter.sh/do-not-disrupt: "true"` and
`cluster-autoscaler.kubernetes.io/safe-to-evict: "false"` annotations. A
namespace-scoped PDB also blocks voluntary eviction of active test Pods.
These safeguards do not protect against machine failure or spot interruption.
On EKS, add `--node-label eks.amazonaws.com/capacityType=ON_DEMAND` to `init`
when you want stable on-demand placement. Additional node labels are optional
and generic; they must match capacity actually available in your cluster.

The binary upload verifies SHA-256 before replacing an artifact. A changed
binary/configuration hash restarts the relevant Deployment. Artifact and load
storage are ephemeral: if those Pods are replaced, upload binaries again and
rerun interrupted cases. Avoid changing binaries during an active run.

## Measure

```sh
python3 benchmarks/kubernetes/run.py --state "$SINK_BENCH_STATE" \
  --output "$SINK_BENCH_DIR/mongo-merge.json" --label mongo-merge -- \
  --store mongo --workload merge --concurrency 128 --duration 60s

python3 benchmarks/kubernetes/run.py --state "$SINK_BENCH_STATE" \
  --output "$SINK_BENCH_DIR/search-merge.json" --label search-merge -- \
  --store search --workload merge --concurrency 128 --duration 60s
```

Each run creates a unique database/index, seeds records, warms gRPC connections,
measures, reconciles persisted counters and payloads, then deletes the dataset.
Workers use disjoint key partitions by default; `--hot-keys` introduces shared
keys. Merge increments have a per-writer sequence check, so replay after an
uncertain response is idempotent. Reconciliation accounts separately for an
unacknowledged final commit. A passed reconciliation is not equivalent to a
healthy capacity sample: RPC errors, unissued fixed-rate requests, restarts,
replaced Pods or OOMs also disqualify a healthy result.

Load options include `--workload upsert|read|mixed|heavy-merge`, `--padding`
(bytes), `--fields`, `--batch`, `--return-document`, `--visible`, `--hot-keys`,
`--rate`, `--timeout`, `--search-replicas` and `--search-shards`. Run
`go run ./cmd/sink-perf --help` for all options. `--rate 0` is closed-loop
saturation. Fixed-rate tests report latency from the scheduled arrival time,
including generator backlog, and count requests that could not be issued.
The timeline reports successful and failed RPCs per second for recovery tests.
The default 10 ms delay after failure prevents a tight retry loop during outages.

Default records preserve 1 KiB of repeated `x` padding and a small counter object.
They are deliberately easy to reconcile and highly compressible. They measure
request, Lua and backend processing costs; they do not establish disk capacity
for high-entropy documents. `--random-padding` instead generates reproducible
per-record text with normal token boundaries; combine it with `--fields 32`
to measure richer, indexed documents. OpenSearch writer fields are established during
setup. Use `--cold-mapping` to measure a burst of dynamic mappings separately.
The index keeps normal field indexing, one primary shard and a 1 second refresh
interval; applied writes do not wait for refresh, while `--visible` does.
Its total-field limit is raised when needed to fit the synthetic per-writer
sequence fields (`max(1000, concurrency + extra fields + 32)`). This prevents
the generator's bookkeeping from exceeding the mapping limit; it does not
disable indexing of those fields or of the payload.
`--search-flush-mib` optionally sets the fresh index's translog flush threshold;
zero retains the backend default. This changes flush frequency while keeping
`translog.durability=request`. Measure recovery as well as throughput before
adopting a larger threshold. The [flush comparison plan](plans/search-flush.json)
combines sampled flush counters with fixed-rate workloads.
The default Merge carries only a small counter delta. Add `--full-incoming`
to transmit and merge the padding and fields on each mutation as well.

The load process runs inside Kubernetes and persists its result before reporting
through kubectl. A local `.pending.json` records how to recover its output if the
control connection breaks. Check this record before launching a duplicate run.
Cgroup samples exclude setup and reconciliation. Memory peaks are **sampled**
peaks, not continuous high-water marks. CPU rates use node uptime deltas; quota
throttling and container restarts are recorded separately. The load Pod's main
process RSS is its idle supervisor; use its cgroup memory to assess generator
memory. Do not run correctness tests or another load against the same backends
during a capacity sample.
When available, cgroup I/O counters report backend block-device read/write
rates during the same interval. These are container observations, not a storage
service throughput guarantee. Use a multi-minute run spanning database
checkpoints before drawing conclusions about sustained storage capacity.
For search diagnostics, pass `--search-stats` before `--` to collect index flush,
translog and indexing counters alongside cgroup samples. The runner saves a
private `.flush-stats.json` next to the result; `--flush-output` on `export.py`
exports only relative times and numeric observations. Compare flush increments
with throughput dips in the workload's one-second timeline. Statistics are
sampled, so an observation time is not the exact start time of a flush.

`matrix.py` accepts a JSON list of scenarios. Each object contains `label`,
`server` flags for `cluster.py server`, and `load` flags for `sink-perf`. It
reuses completed reconciled cases when resuming; examine errors and P99 before
using any case as production capacity evidence. Give repeated trials unique
labels.
Recovery scenarios can additionally set `fault` and `fault_after_seconds`;
the same runner confirms the fault and keeps its result separate from healthy
capacity. An unconfirmed fault or another harness error is not reused on resume.
Pod failures require an observed replacement or restart; accepting a delete or
rollout command alone does not confirm the fault. A rollout must replace every
original Sink Pod. Check the final 30-second throughput as well as errors before
calling a recovery successful.

The checked-in [capacity plan](plans/capacity.json) compares 1, 2 and 4 CPU
profiles, and the [workload plan](plans/workloads.json) covers document size,
fields, full inputs, batches, returned documents, visibility and hot keys.
Run a plan with `matrix.py --state "$SINK_BENCH_STATE" --plan
benchmarks/kubernetes/plans/capacity.json --output-dir "$SINK_BENCH_DIR"`.
These are saturation sweeps; choose a lower fixed `--rate` for a long soak after
examining P99 and correctness. Neither plan provisions the namespace for you.
The [returned-document plan](plans/returned-documents.json) exercises smaller
response limits, admission fairness and full 1 MiB inputs/outputs. Reconciliation
shrinks read batches when the server's configured byte limit requires it;
these reads happen after measurement and do not change the recorded workload.

To add durable database copies:

```sh
python3 benchmarks/kubernetes/cluster.py --state "$SINK_BENCH_STATE" databases --replicas 3
python3 benchmarks/kubernetes/cluster.py --state "$SINK_BENCH_STATE" server --replicas 2
```

For OpenSearch, also pass `--search-replicas 1` to the load. The runner waits for
green index health before measuring. MongoDB uses a three-member replica set;
the Sink executable enforces majority journaled acknowledgement. Database
scale-down is intentionally unsupported: clean up and start a fresh namespace.
This topology does not ensure multiple availability zones; configure and verify
zone placement separately before making cross-zone capacity claims.
Search indexes explicitly use request-level translog durability. Add
`--active-shards all` to require all configured shard copies to be active before
a write starts; this is an availability prerequisite, not a promise that every
copy will acknowledge every write after a failure.
The [replicated plan](plans/replicated.json) then compares one 2 CPU Sink,
one 4 CPU Sink and two 2 CPU Sinks, including richer documents, search shard
count, visible completion and shared keys. It assumes both database clusters
already have three members; it does not scale the databases itself.

The [soak plan](plans/soak.json) uses one 2 CPU / 2 GiB Sink and fixed offered
rates for two ten-minute small-document runs and a five-minute 16 KiB MongoDB
run. It assumes the replicated database topology. Review zero errors, zero
unissued requests, reconciliation and P99 before accepting a rate. The plan
finishing successfully does not by itself establish a latency SLO.

After that plan, keep the same single-Sink configuration to check simultaneous
stores. This is an intentional exception to running only one load at a time:

```sh
python3 benchmarks/kubernetes/run.py --state "$SINK_BENCH_STATE" \
  --output "$SINK_BENCH_DIR/shared-mongo.json" --label shared-mongo -- \
  --store mongo --concurrency 128 --duration 600s --rate 1750 \
  --random-padding --fields 32 --full-incoming &
SINK_MONGO_RUN=$!
python3 benchmarks/kubernetes/run.py --state "$SINK_BENCH_STATE" \
  --output "$SINK_BENCH_DIR/shared-search.json" --label shared-search -- \
  --store search --concurrency 128 --duration 600s --rate 1500 \
  --random-padding --fields 32 --full-incoming \
  --search-replicas 1 --search-shards 3 --active-shards all &
SINK_SEARCH_RUN=$!
wait "$SINK_MONGO_RUN"
wait "$SINK_SEARCH_RUN"
```

Verify both results and their measurement-window overlap using the local
`started_unix_ns` and `elapsed_seconds`. The Sink and load cgroup measurements
include both workloads; do not add their duplicated CPU or memory figures.

For the small-document admission comparison, reconfigure the same single Sink
and repeat those two commands with new output names and labels:

```sh
python3 benchmarks/kubernetes/cluster.py --state "$SINK_BENCH_STATE" server \
  --cpu 2 --memory 2Gi --go-memory 1536MiB --gogc 400 \
  --execution-mib 512 --read-mib 8 --batch-operations 32 --wait-ms 2 \
  --replicas 1 --rolling true --prestop-seconds 40 --grace-seconds 90 \
  --min-ready-seconds 5
```

Wait for the Sink rollout to finish before restarting the load. Keep both load
commands at the same rates, duration and document shape. The
8 MiB value reduces each original RPC's snapshot/result budget; it is suitable
only when the largest client batch and returned result fit that contract.

For an explicit recovery test, `run.py` also accepts `--fault sink-crash`,
`--fault sink-terminate`, `--fault sink-rollout`, `--fault mongo-stepdown`, or `--fault search-terminate`
before `--`, with `--fault-after-seconds 20`. Use a workload of at least 120 s
and the replicated database topology for failover. These actions affect only
Pods discovered in the UID-checked test namespace. A Sink crash sends SIGABRT
to the Go process, bypassing its normal drain; termination deletes one Pod
normally. The per-second timeline and final reconciliation show whether traffic
recovers and acknowledged data survives. Expected fault errors must never be
reported as a healthy capacity sample. Do not run this against production Pods.
The [recovery plan](plans/recovery.json) runs these five fault types sequentially
against two Sink replicas and three-member database clusters.
For rolling-update tests, first deploy with `server --replicas 2 --rolling true
--prestop-seconds 5 --min-ready-seconds 5`. Compare `--dns-min-interval 30s` and
`--dns-min-interval 5s` on the load generator when investigating recovery from
changed Pod addresses. The latter changes a process-wide gRPC setting before
any client is created; it does not change cluster DNS configuration.
When increasing the pre-stop delay, set `--grace-seconds` to include that delay
plus the server's 30 second graceful shutdown budget; for example, a 40 second
pre-stop requires at least 70 seconds, with 90 seconds allowing extra margin.
The helper rejects a shorter grace period. Evaluate the delay together with
the client's DNS refresh behavior and the time available for rollout.
The [drain comparison](plans/drain.json) exercises the longer delay with a fresh
30-second DNS cache. The [overload plan](plans/overload.json) offers 4,000 RPC/s
with 100 operations per RPC, followed by ordinary traffic at 2,000 RPC/s.
The overload is deliberately excessive: inspect bounded rejection, memory,
reconciliation and subsequent recovery instead of treating its offered rate
as achieved capacity.

The optional `cluster.py legacy-mongo` command creates a disposable MongoDB 7
replica set at `mongodb-legacy:27017` in the same namespace. Run tagged storage
tests from an in-cluster Go toolchain Pod with
`SINK_MONGODB_TEST_URI=mongodb://mongodb-legacy:27017/?replicaSet=legacy` to check
the legacy conditional-write path. The main MongoDB 8 endpoint is
`mongodb://mongodb-0.mongodb:27017/?replicaSet=rs0`. Tests must not overlap
capacity measurements. `TestMongoDBConditionalWritesSelectCapabilityAndPreserveDurabilityErrors`
checks the actual command selected and rejects an unsatisfied write concern
even when the records were committed.

## Export and clean up

```sh
python3 benchmarks/kubernetes/export.py --input-dir "$SINK_BENCH_DIR" \
  --output "$SINK_BENCH_DIR/anonymous-results.csv" \
  --flush-output "$SINK_BENCH_DIR/anonymous-flush.csv"
python3 -m unittest discover -s benchmarks/kubernetes
python3 benchmarks/kubernetes/cluster.py --state "$SINK_BENCH_STATE" cleanup
```

Export uses an explicit column allowlist. Review scenario labels and exported
values before publishing. Never commit raw JSON, stderr, context names,
namespaces, node names, addresses, ARNs, volume IDs or state files.

Always clean up, including after a failed experiment. Cleanup first records
the namespace's bound volumes, then deletes the owned namespace and waits for
the namespace and dynamically provisioned PVs to disappear. A timeout is a
pending cleanup, not success: keep the state file and rerun the command. The
helper refuses a changed context or reused namespace UID. All test controllers,
databases, data volumes and artifact storage are removed with the namespace.
For cloud-backed storage, retain the owned PVs' CSI volume handles in the private
run directory before cleanup, then verify those disks are absent through the
cloud provider API. Confirm that no VolumeAttachment still references an owned
PV. An API permission or transport error is not evidence that a disk was deleted.
