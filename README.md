# Sink

Sink is a database-independent logical layer for record reads, writes, and
read-modify-write operations. MongoDB is the first storage adapter, while the
public API uses logical stores, namespaces, datasets, and record keys instead
of physical database terminology.

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
fetched records finish. A
crash after MongoDB applies a mutation but before Kafka commits its offset can
therefore execute that mutation again; this is the documented at-least-once
behavior.

## Legacy documents

Documents use a content type and opaque bytes so existing BSON can pass through
without schema conversion. Storage adapters interpret fields only when a merge
profile requires it. The MongoDB adapter preserves the existing top-level
document shape and lazily adds one configurable hidden revision field when a
record is first mutated through Sink. A legacy record without that field is
updated with a conditional “revision still absent” filter, so its first RMW is
atomic as well.

The MongoDB adapter currently accepts `application/bson`. Logical string,
int64, byte, and `mongodb/object-id` keys map to MongoDB `_id` values. Logical
namespace and dataset names can map directly to a database and collection, or
use explicit bindings for an existing deployment. Batch reads use one `$in`
query per collection. Unconditional puts and creates use unordered bulk writes;
revision-conditional writes run concurrently as individual `ReplaceOne`
operations because older MongoDB bulk responses do not identify which
individual CAS filters matched.

## Packages

- `internal/service` implements validation, same-record ordering, batched puts,
  and CAS retry for merge profiles.
- `internal/storage/mongodb` implements the first persistent storage adapter.
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

`make test-integration` starts an ephemeral single-node MongoDB ReplicaSet in
Docker, runs the storage and concurrent-RMW integration tests, and stops the
container. The Kafka path is tested with franz-go's in-process Kafka broker in
the normal test suite.

## Docker Compose quickstart

Start a complete local stack and run the end-to-end example with one command:

```shell
make quickstart
```

The quickstart builds Sink locally, starts a single-node MongoDB ReplicaSet and
Apache Kafka in KRaft mode, creates the asynchronous mutation topic, then tests
batch synchronous writes and reads, asynchronous delivery, and hard deletes
through the public gRPC API. The stack remains running for further testing at
`127.0.0.1:8080`; use `make quickstart-down` to stop it.

See [`examples/quickstart`](examples/quickstart) for the exposed dependency
ports, direct Docker Compose commands, and reset instructions.

## Container

Published releases build multi-platform images for `linux/amd64` and
`linux/arm64` and push them to `ghcr.io/liran/sink`. Run the synchronous gRPC
service with:

```shell
docker run --rm -p 8080:8080 \
  -e SINK_MONGODB_URI=mongodb://mongodb:27017 \
  ghcr.io/liran/sink:latest
```

The image supports three modes through `SINK_MODE`:

- `server` runs the gRPC API and is the default.
- `worker` consumes durable Kafka mutations without opening a gRPC listener.
- `all` runs both roles in one process for smaller deployments.

Runtime configuration:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SINK_MONGODB_URI` | required | MongoDB connection string. |
| `SINK_MONGODB_STORE` | `primary` | Logical store name accepted by this process. |
| `SINK_MONGODB_HIDDEN_FIELD` | `__sink` | Hidden revision metadata field. |
| `SINK_MONGODB_BINDINGS` | direct names | JSON array mapping logical namespace/dataset pairs to existing MongoDB database/collection pairs. |
| `SINK_GRPC_ADDRESS` | `:8080` | gRPC listen address in `server` and `all` modes. |
| `SINK_KAFKA_BROKERS` | empty | Comma-separated Kafka bootstrap addresses. |
| `SINK_KAFKA_TOPIC` | empty | Durable mutation topic; configure together with brokers to enable asynchronous acceptance. |
| `SINK_KAFKA_GROUP_ID` | required for worker | Kafka consumer group for `worker` and `all` modes. |
| `SINK_KAFKA_MAX_POLL_RECORDS` | `500` | Maximum mutations handled in one consumer batch. |

The stock image intentionally registers no application-specific merge
profiles. It supports reads, puts, deletes, and the complete async pipeline;
deployments that need field-aware RMW profiles should register their mergers
in a project-specific binary until a profile-loading mechanism is defined.

Publishing a GitHub Release triggers `.github/workflows/release-image.yml`.
Semantic version tags such as `v0.1.0` publish `0.1.0`, `0.1`, `0`, and—when
the release is not a prerelease—`latest` tags in GitHub Container Registry.
