# Sink

Sink is a database-independent logical layer for record reads, writes, and
read-modify-write operations. It supports MongoDB, Elasticsearch, and
OpenSearch storage. One server can connect to multiple storage instances, and
the public record address selects an instance by its configured store name.

## API model

The gRPC service exposes three batch-native methods:

- `Read` reads one or more records.
- `Write` accepts either a complete-document `put` or a profile-driven `merge`.
- `Delete` permanently deletes one or more records.

A one-element request is the single-record form. Atomicity is per record; a
multi-record request is not a transaction. Operations for the same record are
executed in request order, while different records may execute concurrently.

`Write` and `Delete` support two completion modes:

- `WAIT_UNTIL_APPLIED` returns after the storage adapter completes the work.
- `RETURN_AFTER_ACCEPTED` returns after a durable asynchronous queue accepts
  the work. Queue redelivery can execute an operation more than once.

The asynchronous path stores the original merge intent. A worker reads the
latest record, runs the selected merge profile, and submits the resulting
change using a storage revision precondition. This makes each RMW attempt
atomic without exposing database-specific CAS primitives through the API.

Kafka records use a deterministic encoded record address as their key, so all
mutations for one record stay in one partition and retain submission order.
The publisher waits for Kafka acknowledgement before reporting `ACCEPTED`.
The worker disables auto-commit, applies fetched mutations in dependency waves
through the synchronous batch service path, and commits offsets only after the
fetched records finish. A crash after storage applies a mutation but before
Kafka commits its offset can therefore execute that mutation again; this is
the documented at-least-once behavior.

## Legacy documents

Documents use a content type and opaque bytes so existing BSON can pass through
without schema conversion. Storage adapters interpret fields only when a merge
profile requires it. The MongoDB adapter preserves the existing top-level
document shape and lazily adds one configurable Sink metadata field when a
record is first mutated through Sink. A legacy record without that field is
updated with a conditional “metadata still absent” filter, so its first RMW is
atomic as well.

The MongoDB adapter accepts `application/bson`. Logical string, int64, byte,
and `mongodb/object-id` keys map to MongoDB `_id` values. The record address's
namespace and dataset map directly to the MongoDB database and collection.
Batch reads use one `$in` query per collection. Unconditional puts and creates
use unordered bulk writes; revision-conditional writes run concurrently as
individual `ReplaceOne` operations because older MongoDB bulk responses do not
identify which individual CAS filters matched.

The Elasticsearch and OpenSearch adapter accepts `application/json` objects
and stores the user document unchanged in `_source`. Existing string IDs remain
unchanged; other logical key types receive a deterministic, reserved encoding.
It uses `_seq_no` and `_primary_term` as an opaque revision token, global
`_mget` for batch reads, and `_bulk` for puts and hard deletes. Sink does not
create or manage indexes, mappings, or aliases. The record address's namespace
remains a logical business namespace, while its dataset is the complete,
existing index or alias name.

## Packages

- `internal/service` implements validation, same-record ordering, batched puts,
  and CAS retry for merge profiles.
- `internal/storage` routes each operation by `address.store` and preserves
  result order for batches that span multiple storage instances.
- `internal/storage/mongodb` implements MongoDB storage.
- `internal/storage/search` implements the shared Elasticsearch and OpenSearch
  REST storage adapter.
- `internal/queue/kafka` implements durable mutation publication and manual
  offset consumption.
- `internal/worker` applies queued operations through the synchronous Sink
  service, keeping one execution path for sync and async writes.
- `internal/storage/memory` is the deterministic test and local-development
  adapter.

## Development

Generate protobuf code and run validation:

```shell
make proto
make test
make lint
make test-integration
```

`make test-integration` starts ephemeral MongoDB ReplicaSet, Elasticsearch, and
OpenSearch containers, runs storage lifecycle and concurrent-RMW tests against
each backend, and stops the containers. `make test-search-integration` runs
only the Elasticsearch and OpenSearch suites. The Kafka path is tested with
franz-go's in-process Kafka broker in the normal test suite.

## Docker Compose quickstart

Start a complete local stack and run the end-to-end example with one command:

```shell
make quickstart
```

The quickstart builds Sink locally, starts a single-node MongoDB ReplicaSet and
Apache Kafka in KRaft mode, creates the asynchronous mutation topic, then tests
batch synchronous writes and reads, asynchronous delivery, hard deletes, and
the Prometheus endpoint. The stack remains running for further testing at
`127.0.0.1:8080`, with metrics at `http://127.0.0.1:9090/metrics`; use
`make quickstart-down` to stop it.

See [`examples/quickstart`](examples/quickstart) for the exposed dependency
ports, direct Docker Compose commands, and reset instructions.

## Container

Published releases build multi-platform images for `linux/amd64` and
`linux/arm64` and push them to `ghcr.io/liran/sink`. Run the synchronous gRPC
service with a YAML configuration file:

```shell
cp config.example.yaml config.yaml
# Edit config.yaml for the target MongoDB deployment.
docker run --rm -p 8080:8080 -p 9090:9090 \
  --mount type=bind,source="$(pwd)/config.yaml",target=/etc/sink/config.yaml,readonly \
  ghcr.io/liran/sink:latest --config /etc/sink/config.yaml
```

The configuration file is required and the server does not read runtime
parameters from `SINK_*` environment variables. The image supports three modes
through the top-level `mode` field:

- `server` runs the gRPC API and is the default.
- `worker` consumes durable Kafka mutations without opening a gRPC listener.
- `all` runs both roles in one process for smaller deployments.

See [`docs/configuration.md`](docs/configuration.md) for every field's type,
function, required condition, default, validation rules, address routing, and
multi-storage examples. A ready-to-edit MongoDB file is available at
[`config.example.yaml`](config.example.yaml).

The stock image intentionally registers no application-specific merge
profiles. It supports reads, puts, deletes, and the complete async pipeline;
deployments that need field-aware RMW profiles should register their mergers
in a project-specific binary until a profile-loading mechanism is defined.

Publishing a GitHub Release triggers `.github/workflows/release-image.yml`.
Semantic version tags such as `v0.1.0` publish `0.1.0`, `0.1`, `0`, and—when
the release is not a prerelease—`latest` tags in GitHub Container Registry.
