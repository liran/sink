# Development

Sink requires Go 1.27 or newer. Docker with Compose is required for the
quickstart and external storage integration suites.

## Validation

Build and run the normal checks from the repository root:

```shell
make build
make test
make lint
go test -race ./... -count=1
```

`make build` writes `bin/sink`. The default tests use local test doubles and an
in-process Kafka broker; they do not require external services. Go may download
module dependencies on the first run. `make lint` checks formatting, vet, and
staticcheck without changing source files. Use `make fmt` to apply formatting.

Run external backend tests explicitly:

```shell
make test-integration
```

`make test-integration` starts ephemeral MongoDB ReplicaSet, Elasticsearch, and
OpenSearch containers, runs storage lifecycle and concurrent read-modify-write
tests, and then stops the containers. `make test-search-integration` runs only
the Elasticsearch and OpenSearch suites. The Kafka path uses franz-go's
in-process broker in the normal test suite.

Use `make quickstart` for the end-to-end public API scenario and
`make quickstart-down` when finished.

### Repository checks

`make lint-workflows` runs the pinned actionlint version against GitHub Actions
workflows. ShellCheck is excluded from this target so the result does not depend
on an optional local installation. CI also checks local Markdown links,
images, and heading anchors with [lychee](https://github.com/lycheeverse/lychee).
Install the version used by the pinned lychee action (currently `v0.24.2`) to
reproduce the check:

```shell
make lint-workflows
make lint-docs
```

Link checks run offline to avoid making repository CI depend on third-party
website availability. External URLs should still be checked when editing them.
All repository checks feed the required **Sink reliability gate** along with
the existing behavior and packaging jobs.

Dependabot proposes weekly Go module, GitHub Actions, and base-image updates
with bounded open PR counts. Review them through the normal qualification gate.
The production suite's reusable workflow references are excluded: update the
workflow SHA and `suite_ref` together in CI and release configuration.

### Protobuf changes

Only regenerate protobuf files when changing the protocol or generator versions.
Install `protoc` and the Go plugins used by the generated files:

```shell
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@v0.6.1-0.20240319094008-0393e58bdf10
export PATH="$(go env GOPATH)/bin:$PATH"
make proto
```

Include changes under `gen/sink` in the PR. CI compares the public protocol with
the Go client, using a matching client branch when available and `main` otherwise.

## Synchronous capacity measurements

The [synchronous performance guide](synchronous-performance.md) records the
workload, resource limits, measurements, and commands for comparing revisions.
`BenchmarkSynchronousMergeMicrobatch` isolates batching and adapter round trips
without external services. The opt-in `BenchmarkSynchronousStorage` exercises
the actual gRPC codec, dispatcher, Lua engine, and disposable MongoDB/OpenSearch
backends, then verifies every writer's persisted counter.

## Repository layout

- `proto/sink` defines the public gRPC contract.
- `internal/service` implements validation, ordering, batching, puts, and Lua
  merge retries.
- `internal/storage` routes operations to independently configured adapters.
- `internal/storage/mongodb` implements MongoDB storage.
- `internal/storage/search` implements the shared Elasticsearch and OpenSearch
  adapter.
- `internal/queue` routes asynchronous mutations to the selected publisher.
- `internal/queue/kafka` implements durable publication and manual-offset
  consumption.
- `internal/worker` applies queued mutations through the synchronous service
  path.
- `internal/storage/memory` is the deterministic test and local-development
  adapter.
- `cmd/sink` loads configuration and assembles the server or worker process.

## Release artifacts

Publishing a GitHub Release triggers `.github/workflows/release-image.yml`.
Semantic version tags such as `v0.3.2` publish `0.3.2`, `0.3`, and `0` image
tags. A non-prerelease also publishes `latest`. Images are available for
`linux/amd64` and `linux/arm64` at `ghcr.io/liran/sink`.

The Release description is updated after publication with the complete tagged
image address, pull command, and immutable image digest. For `v0.3.2`, the
primary image is:

```text
ghcr.io/liran/sink:0.3.2
```

Every Release also includes `checksums.txt` and standalone archives for these
targets:

| Platform | Architecture | Asset format |
| --- | --- | --- |
| Linux | amd64 | `sink_v0.3.2_linux_amd64.tar.gz` |
| Linux | arm64 | `sink_v0.3.2_linux_arm64.tar.gz` |
| macOS | amd64 | `sink_v0.3.2_darwin_amd64.tar.gz` |
| macOS | arm64 | `sink_v0.3.2_darwin_arm64.tar.gz` |

`amd64` means 64-bit x86 and `arm64` means 64-bit ARM. Archives contain the
Sink binary, `LICENSE`, and `README.md`. CI builds all four targets before a
Release can rely on the packaging script. To reproduce the assets locally in
an empty output directory:

```shell
scripts/build-release-binaries.sh v0.3.2 dist
```

Windows archives are not currently published because the pinned Lua runtime
does not compile for Windows. The workflow deliberately excludes Windows
instead of attaching an untested binary.

## Reliability regression and sustained validation

Default race tests include fake-broker outage recovery beyond the retry budget,
DLQ publication failure, CREATE replay continuation, partition-prefix commits,
rebalance cancellation, admission/cancellation, and read/Lua output budgets.
CI also fuzzes mutation envelopes for 20 seconds on each change.

The public [production suite](https://github.com/liran/sink-production-suite)
owns release and sustained qualification. Sink's release workflow pins both the
reusable workflow and suite source to the same immutable commit. The manual
`.github/workflows/reliability.yml` entry point invokes that suite's two-hour
workflow against the selected Sink revision; the nightly schedule lives in the
suite repository.

The shared runner uses a unique disposable Compose project, product worker retry
defaults, an active-worker SIGKILL, a 45-second backend outage, and a broker
restart. It checks dependency readiness, API size/read/Lua-output limits, lag,
and DLQ inspection/repair/replay, retaining revisions, fault timelines, test
output, container resource samples and Prometheus samples. With a working Docker
engine and available qualification ports, run it from the suite checkout:

```sh
SINK_SERVER_DIR=/path/to/sink make test-production
SINK_SERVER_DIR=/path/to/sink make test-reliability
```

The first command includes a three-minute fault workload; the second uses two
hours. The business counter implements application idempotence and reconciles
stored results. Passing the short gate does not establish a completed two-hour
run. Real multi-node failover, disk pressure and backup restoration remain
deployment qualification; see [the reliability runbook](reliability.md).
