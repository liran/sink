# Sink

[![CI](https://github.com/liran/sink/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/liran/sink/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/liran/sink)](https://github.com/liran/sink/releases/latest)
[![Go version](https://img.shields.io/github/go-mod/go-version/liran/sink)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**One gRPC data layer for MongoDB, Elasticsearch, and OpenSearch.**

Read, write, and atomically merge documents through a shared API. Sink handles
database connections, routing, bounded batching, and backpressure, with optional
Kafka-backed asynchronous delivery. Native queries use the same connections.

[Quickstart](#quickstart) · [Documentation](docs/README.md) ·
[Go client](https://github.com/liran/sink-go) ·
[Releases](https://github.com/liran/sink/releases) · [Contributing](CONTRIBUTING.md)

## Quickstart

With Docker Compose and Make installed:

```shell
git clone https://github.com/liran/sink.git
cd sink
make quickstart
```

This builds Sink, starts a local MongoDB ReplicaSet and Kafka, and verifies
synchronous writes, asynchronous delivery, reads, deletes, and Prometheus
metrics. The Go example runs in a container; a local Go installation is not
required.

After the checks pass, connect to **`127.0.0.1:8080`** or inspect
[metrics](http://127.0.0.1:9090/metrics). Stop the stack with
`make quickstart-down`. See the [quickstart guide](examples/quickstart/README.md)
for ports, rerunning the example, and resetting local data.

Need just the binary or container? Use the
[latest release](https://github.com/liran/sink/releases/latest) for Linux/macOS
binaries and checksums, or [run the container](#run-the-container).

## Why Sink?

![Sink routes each record operation through a synchronous or Kafka-backed path to one matching store](docs/assets/sink-overview.svg)

Applications that write to several databases often repeat the same
non-business work: storage drivers, batching, backpressure, retry rules,
read-modify-write conflicts, queue consumers, health checks, and metrics. That
logic becomes especially expensive in crawlers and ingestion systems with many
concurrent workers.

Sink centralizes those concerns:

| Problem | What Sink provides |
| --- | --- |
| Each backend has a different API and data model | One batch-native `Read`, `Write`, and `Delete` gRPC API for JSON or BSON documents |
| Native queries still need direct database clients | `Execute`, `Query`, `Count`, and paged `Scan` share one command structure and Sink's connections |
| Every crawler process opens its own database connections | Database connections move into the smaller Sink tier, so connection growth follows Sink replicas instead of crawler processes |
| Many small calls overload storage | Automatic bounded batching, concurrency limits, and backpressure per store |
| Some writes must be immediate while others can be buffered | Per-request completion modes, with optional Kafka-backed asynchronous delivery |
| Concurrent updates lose data | Atomic per-record Lua merges with storage revision checks |
| A service needs more than one cluster or engine | Named stores selected by the record address, even within one batch |
| Failures are hard to operate | Per-dependency health, bounded-cardinality metrics, recoverable worker retries, and dead letters |

Sink is a good fit when several workers or services need consistent record
semantics across shared storage. It is deliberately not an ORM, a schema or
index manager, or a cross-record transaction coordinator.

### Protect storage behind crawler fleets

A crawler fleet commonly runs many processes, with each process maintaining
reusable database connections for its worker threads. If every process writes
directly, database connections grow with the crawler fleet while one-record
writes add round trips and storage scheduling overhead.

Sink moves that fan-in boundary in front of storage. Workers connect to Sink;
each Sink instance owns the backend clients and database connections. Its
short, bounded per-store queues coalesce concurrent small RPCs into bulk
operations, while concurrency limits and backpressure keep bursts from reaching
the database without control.

![Without Sink, every crawler process opens database connections and sends fragmented writes; with Sink, crawler processes converge on controlled database connections and micro-batches before storage](docs/assets/sink-database-protection.svg)

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
database connection string. Native access uses `Execute`, `Query`, `Count`, and
`Scan` with a common `Command`; the record API remains storage-independent.

Every document declares its encoding. MongoDB stores require BSON, so clients
apply `bson` struct tags and retain native BSON values such as datetimes.
Elasticsearch and OpenSearch stores require JSON, so clients apply `json`
struct tags and send ordinary JSON without Extended JSON wrappers.

Record requests are batch-native, but a one-record request is the normal
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

## Connect an application

Go applications can use the typed, concurrency-safe
[`sink-go`](https://github.com/liran/sink-go) client:

```shell
go get github.com/liran/sink-go
```

Its [quick-start example](https://github.com/liran/sink-go#quick-start) shows
how to connect, create an address, and write a Go value. Other languages can
generate a standard gRPC client from [`proto/sink/sink.proto`](proto/sink/sink.proto).

## Run the container

For a real deployment, copy [`config.example.yaml`](config.example.yaml), edit
the backend connection, and start the container with the file mounted:

```shell
cp config.example.yaml config.yaml
# Edit config.yaml for the target backend.
docker run --rm -p 8080:8080 -p 9090:9090 \
  --mount type=bind,source="$(pwd)/config.yaml",target=/etc/sink/config.yaml,readonly \
  ghcr.io/liran/sink:latest --config /etc/sink/config.yaml
```

For repeatable deployments, replace `latest` with a version tag or image digest
from the release. See the [configuration reference](docs/configuration.md) for
store routing and server/worker modes, and [production sizing](docs/production-sizing.md)
for measured capacity and deployment guidance.

The server validates the complete configuration. Dependency readiness recovers
independently; inspect the health of each required store before sending traffic.

## Test Lua merge programs

Business applications can validate merge rules locally with the exact Lua
runtime used by Sink, without starting a server or storage backend:

```shell
sink lua test \
  --script merge/product.lua \
  --cases merge/testdata
```

The command supports JSON fixtures and type-aware BSON fixtures written as
Extended JSON. See [Testing Lua merge programs](docs/lua-testing.md) for the
case format, direct flags, BSON examples, CI usage, and coverage guidance.

## Important guarantees and boundaries

- Atomicity is per record; a multi-record request is not a transaction.
- Operations for one record retain request order. Independent records and
  stores may execute concurrently.
- Kafka delivery is at least once. A mutation can run again after a worker
  crashes between applying it and committing its offset. Applications must
  guarantee business idempotence for every retried or replayed mutation; Sink
  does not deduplicate business operations. A timeout can leave the outcome unknown.
- Kafka is disabled by default per store. When enabled, Sink owns creation and
  reconciliation of that store's source and dead-letter Topics at startup.
- Record writes do not initialize indexes, mappings, or aliases. Applications
  can explicitly initialize them through native `Execute` commands.
- Dependencies recover independently at startup and runtime. Kafka publication
  and consumption remain gated until that store's Topic policy is established.
  Temporary processing failures retain source offsets for automatic recovery.

## Documentation

Start with the [documentation index](docs/README.md), organized by task:

| I want to… | Read |
| --- | --- |
| Understand routing, batching, and completion | [Architecture](docs/architecture.md) and [document write flow](docs/document-write-flow.md) |
| Query a backend or return an atomic update result | [Native access](docs/native-access.md) |
| Write and test a Lua merge | [Lua guide](docs/lua-merge-guide.md) and [local testing](docs/lua-testing.md) |
| Deploy and operate Sink | [Configuration](docs/configuration.md), [sizing](docs/production-sizing.md), and [reliability](docs/reliability.md) |
| Build or contribute | [Development](docs/development.md) and [contributing](CONTRIBUTING.md) |

## Contributing and support

Bug reports, documentation improvements, and focused pull requests are welcome.
Use the [issue forms](https://github.com/liran/sink/issues/new/choose) for bugs
and feature proposals, and read [CONTRIBUTING.md](CONTRIBUTING.md) for local
checks and review guidance. Report vulnerabilities through the
[security policy](SECURITY.md).

Related projects: [Go SDK](https://github.com/liran/sink-go) ·
[Public production qualification suite](https://github.com/liran/sink-production-suite).

Sink is released under the [MIT License](LICENSE).
