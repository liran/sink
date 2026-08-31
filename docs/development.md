# Development

Sink requires Go 1.27 or newer. Docker with Compose is required for the
quickstart and external storage integration suites.

## Validation

Generate protobuf code and run the normal checks from the repository root:

```shell
make proto
make test
make lint
make test-integration
```

`make test-integration` starts ephemeral MongoDB ReplicaSet, Elasticsearch, and
OpenSearch containers, runs storage lifecycle and concurrent read-modify-write
tests, and then stops the containers. `make test-search-integration` runs only
the Elasticsearch and OpenSearch suites. The Kafka path uses franz-go's
in-process broker in the normal test suite.

Use `make quickstart` for the end-to-end public API scenario and
`make quickstart-down` when finished.

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
