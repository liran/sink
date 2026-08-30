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

## Container releases

Publishing a GitHub Release triggers `.github/workflows/release-image.yml`.
Semantic version tags such as `v0.3.2` publish `0.3.2`, `0.3`, and `0` image
tags. A non-prerelease also publishes `latest`. Images are available for
`linux/amd64` and `linux/arm64` at `ghcr.io/liran/sink`.
