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

`matrix.py` accepts a JSON list of scenarios. Each object contains `label`,
`server` flags for `cluster.py server`, and `load` flags for `sink-perf`. It
reuses completed reconciled cases when resuming; examine errors and P99 before
using any case as production capacity evidence. Give repeated trials unique
labels.

The checked-in [capacity plan](plans/capacity.json) compares 1, 2 and 4 CPU
profiles, and the [workload plan](plans/workloads.json) covers document size,
fields, full inputs, batches, returned documents, visibility and hot keys.
Run a plan with `matrix.py --state "$SINK_BENCH_STATE" --plan
benchmarks/kubernetes/plans/capacity.json --output-dir "$SINK_BENCH_DIR"`.
These are saturation sweeps; choose a lower fixed `--rate` for a long soak after
examining P99 and correctness. Neither plan provisions the namespace for you.

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

For an explicit recovery test, `run.py` also accepts `--fault sink-crash`,
`--fault sink-terminate`, `--fault sink-rollout`, `--fault mongo-stepdown`, or `--fault search-terminate`
before `--`, with `--fault-after-seconds 20`. Use a workload of at least 120 s
and the replicated database topology for failover. These actions affect only
Pods discovered in the UID-checked test namespace. A Sink crash sends SIGABRT
to the Go process, bypassing its normal drain; termination deletes one Pod
normally. The per-second timeline and final reconciliation show whether traffic
recovers and acknowledged data survives. Expected fault errors must never be
reported as a healthy capacity sample. Do not run this against production Pods.
For rolling-update tests, first deploy with `server --replicas 2 --rolling true
--prestop-seconds 5 --min-ready-seconds 5`. Compare `--dns-min-interval 30s` and
`--dns-min-interval 5s` on the load generator when investigating recovery from
changed Pod addresses. The latter changes a process-wide gRPC setting before
any client is created; it does not change cluster DNS configuration.

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
  --output "$SINK_BENCH_DIR/anonymous-results.csv"
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
