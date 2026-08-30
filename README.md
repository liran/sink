# Sink

Sink is a database-independent gRPC service for reading, writing, merging, and
deleting JSON records. Applications use one record API while Sink handles
routing, connection pooling, batching, concurrency, synchronous or durable
asynchronous delivery, and storage-specific behavior for MongoDB,
Elasticsearch, and OpenSearch.

![Sink routes each record operation through a synchronous or Kafka-backed path to one matching store](docs/assets/sink-overview.svg)

## Why use Sink?

Applications that write to several databases often repeat the same
non-business work: storage drivers, batching, backpressure, retry rules,
read-modify-write conflicts, queue consumers, health checks, and metrics. That
logic becomes especially expensive in crawlers and ingestion systems with many
concurrent workers.

Sink centralizes those concerns:

| Problem | What Sink provides |
| --- | --- |
| Each backend has a different API and data model | One batch-native `Read`, `Write`, and `Delete` gRPC API for JSON records |
| Every crawler process owns a database connection pool | Backend pools move into the smaller Sink tier, so database connections scale with Sink replicas instead of crawler processes |
| Many small calls overload storage | Automatic bounded batching, concurrency limits, and backpressure per store |
| Some writes must be immediate while others can be buffered | Per-request completion modes, with optional Kafka-backed asynchronous delivery |
| Concurrent updates lose data | Atomic per-record Lua merges with storage revision checks |
| A service needs more than one cluster or engine | Named stores selected by the record address, even within one batch |
| Failures are hard to operate | Per-dependency health, bounded-cardinality metrics, bounded worker retries, and dead letters |

Sink is a good fit when several workers or services need consistent record
semantics across shared storage. It is deliberately not an ORM, a schema or
index manager, or a cross-record transaction coordinator.

### Protect storage behind crawler fleets

A crawler fleet commonly runs many processes, with many threads sharing a
database driver pool inside each process. If every process writes directly,
database connections grow with the crawler fleet while one-record writes add
round trips and storage scheduling overhead.

Sink moves that fan-in boundary in front of storage. Workers connect to Sink;
each Sink instance owns the backend clients and pools. Its short, bounded
per-store queues coalesce concurrent small RPCs into bulk operations, while
concurrency limits and backpressure keep bursts from reaching the database
without control.

![Without Sink, every crawler process owns a database pool and sends fragmented writes; with Sink, crawler processes converge on bounded connection pools and micro-batches before storage](docs/assets/sink-database-protection.svg)

Database connection demand now follows the deliberately sized Sink tier rather
than the crawler process count, and storage receives fewer, fuller requests.
This protects the backend from connection and request amplification; it does
not replace normal capacity planning. See [Architecture and behavior](docs/architecture.md#connection-fan-in-and-storage-protection)
for the exact scope and boundaries.

## How it works

Every record has a logical address:

```text
store = primary
namespace = catalog
dataset = products
key = product-42
```

`store` selects a configured backend. For MongoDB, `namespace` and `dataset`
are the database and collection. For Elasticsearch and OpenSearch, `dataset`
is the complete existing index or alias name. The application never sends a
database connection string or storage-specific query through the API.

All requests are batch-native, but a one-record request is the normal
single-record form. Sink can combine concurrent small requests into bounded
storage batches, preserves the order of operations for the same record, and
runs independent records and stores concurrently.

Mutations choose when success is returned:

| Completion mode | Success means |
| --- | --- |
| `WAIT_UNTIL_APPLIED` | The backend acknowledged the mutation |
| `WAIT_UNTIL_VISIBLE` | A following search read can observe the mutation |
| `RETURN_AFTER_ACCEPTED` | The configured Kafka queue durably accepted it |

See [Architecture and behavior](docs/architecture.md) for the full request
flow, ordering, batching, merge, and failure semantics.

## Try it locally

The quickstart requires Docker with Compose. From the repository root, run:

```shell
make quickstart
```

This builds Sink, starts MongoDB and Kafka, runs synchronous and asynchronous
record operations, checks Prometheus metrics, and leaves the stack available
at `127.0.0.1:8080`.

```shell
make quickstart-down
```

The [quickstart guide](examples/quickstart/README.md) lists the exposed ports,
direct Compose commands, and reset instructions.

## Connect an application

Go applications can use the typed, concurrency-safe
[`sink-go`](https://github.com/liran/sink-go) client:

```shell
go get github.com/liran/sink-go
```

Its [quick-start example](https://github.com/liran/sink-go#quick-start) shows
how to connect, create an address, and write a Go value. Other languages can
generate a standard gRPC client from [`proto/sink/sink.proto`](proto/sink/sink.proto).

For a real deployment, copy [`config.example.yaml`](config.example.yaml), edit
the backend connection, and start the container with the file mounted:

```shell
cp config.example.yaml config.yaml
# Edit config.yaml for the target backend.
docker run --rm -p 8080:8080 -p 9090:9090 \
  --mount type=bind,source="$(pwd)/config.yaml",target=/etc/sink/config.yaml,readonly \
  ghcr.io/liran/sink:latest --config /etc/sink/config.yaml
```

The server validates the complete configuration and connects to every required
dependency before becoming ready.

## Important guarantees and boundaries

- Atomicity is per record; a multi-record request is not a transaction.
- Operations for one record retain request order. Independent records and
  stores may execute concurrently.
- Kafka delivery is at least once. A mutation can run again after a worker
  crashes between applying it and committing its offset.
- Sink does not create Elasticsearch or OpenSearch indexes, mappings, or
  aliases.
- Every configured store must be reachable at startup. In `server` and `all`
  modes, each configured Kafka publisher is checked too. Runtime health is
  reported separately so an unrelated healthy store can continue serving.

## Documentation

- [Architecture and behavior](docs/architecture.md) — request flow, adapters,
  batching, Lua merges, asynchronous delivery, and reliability boundaries
- [Configuration reference](docs/configuration.md) — every field, default,
  allowed value, validation rule, routing rule, and deployment mode
- [Docker Compose quickstart](examples/quickstart/README.md) — local environment
  and end-to-end example
- [Development guide](docs/development.md) — repository layout, validation, and
  release images
- [Protocol definition](proto/sink/sink.proto) — authoritative gRPC contract

Sink is released under the [MIT License](LICENSE).
