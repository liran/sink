# Architecture and behavior

This document describes the behavior behind Sink's small public API. Start with
the [main README](../README.md) for the problem Sink solves and a local
quickstart. Deployment settings and their validation rules live in the
[configuration reference](configuration.md).

![Sink routes each record operation through a synchronous or Kafka-backed path to one matching store](assets/sink-overview.svg)

## Request path

Applications send JSON record operations over gRPC. Each operation carries a
logical address, and Sink routes it to the configured store with the matching
name. The selected adapter converts the storage-independent record into native
MongoDB, Elasticsearch, or OpenSearch work.

The public service exposes three methods:

- `Read` reads one or more records.
- `Write` performs complete-document puts or Lua-driven merges.
- `Delete` permanently deletes one or more records.

All three methods are batch-native. A one-operation request is the single-record
form. Results remain in request order and include their operation index, even
when Sink executes independent work concurrently.

The default maximum encoded gRPC request and response size is 64 MiB. Operation
counts and transport sizes are configurable independently.

## Connection fan-in and storage protection

Threads in one crawler process can reuse database connections, but separate
processes and pods normally cannot share them. In a direct-write topology,
adding crawler processes therefore adds potential database connections. Small,
independent writes also multiply network round trips and storage-side request
scheduling.

Sink moves the database client boundary into a separately sized service tier.
Crawler processes keep gRPC connections to Sink, while each Sink process owns
the backend clients and their database connections. The synchronous batcher
then coalesces concurrent small RPCs for the same store into bounded storage
batches. MongoDB can use collection-level bulk operations, and Elasticsearch or
OpenSearch can use `_mget` and `_bulk`. Queue limits, adapter concurrency limits,
and backpressure stop caller concurrency from passing through to a backend
without bounds.

This changes the scaling relationship: database connection count follows the
number of Sink replicas instead of the number of crawler processes. It is not a
global connection cap. Every Sink replica has process-local clients,
connections, queues, and batchers, so replica count and backend capacity must
still be sized deliberately. Batching also stays within one Sink process and one
store; callers that already have several records should send an explicit batch
when possible.

![Direct crawler writes multiply database connections and fragmented requests; routing through Sink consolidates backend connections and converts concurrent small RPCs into bounded bulk operations](assets/sink-database-protection.svg)

## Address routing

A record address contains `store`, `namespace`, `dataset`, and `key`. Sink does
not use a separate binding layer: the client-provided `store` must exactly match
a case-sensitive `storages[].name` in the server configuration.

| Address field | MongoDB | Elasticsearch and OpenSearch |
| --- | --- | --- |
| `store` | Configured storage name | Configured storage name |
| `namespace` | Database name | Logical business namespace |
| `dataset` | Collection name | Complete existing index or alias name |
| `key` | MongoDB `_id` | Document `_id` |

One request may target several stores. Sink routes their operations
independently, executes unrelated store groups concurrently, and restores the
original result order. An unknown store produces a failure for only the
affected operation.

## Documents and storage adapters

The public protocol carries an explicit document encoding and opaque payload.
MongoDB accepts BSON documents; Elasticsearch and OpenSearch accept JSON
documents. The client chooses the encoding before serialization, so each
serializer applies its own struct tags and native types. Adapters reject a
document whose encoding does not match the selected store.

### MongoDB

The MongoDB adapter maps a record address directly to database, collection, and
`_id`. It preserves the top-level document shape and lazily adds one configurable
metadata field for Sink's internal revision. The field is removed from reads.
Because the adapter receives native BSON, `_id`, datetimes, ObjectIDs, binary
values, and other BSON types do not pass through JSON or Extended JSON.

Batch reads use one `$in` query per collection. Unconditional puts and creates
use unordered bulk writes. Revision-conditional writes execute concurrently as
individual operations so each result can be correlated with its precondition.
Legacy documents without Sink metadata receive it through a conditional first
mutation, keeping their first read-modify-write atomic.

### Elasticsearch and OpenSearch

The search adapter stores the JSON document unchanged in `_source`. It uses
`_mget` for reads, `_bulk` for puts and hard deletes, and `_seq_no` plus
`_primary_term` as an opaque revision token. Several configured endpoints are
used for transport failover.

Sink does not create or manage indexes, mappings, or aliases. The address's
`dataset` must name a complete existing index or alias.

## Ordering, concurrency, and batching

Atomicity is per record, never per request. Operations for the same address run
in request order. Operations for different records or stores may run
concurrently.

In `server` and `all` modes, Sink automatically coalesces concurrent
one-operation RPCs into bounded, process-local batches for each store. Reads
always use this path when batching is enabled. Synchronous writes and deletes
use it as well; Kafka-backed mutations bypass it because the publisher already
batches asynchronous work.

Read, write, and delete queues are independent for each store. This keeps a
slow method or backend from consuming another queue's allowance. Each queue is
bounded by operation and byte limits. A request that would exceed a limit fails
with `RESOURCE_EXHAUSTED` before it is applied.

The collection window is intentionally short. Dispatch occurs when the window
expires or when accepting another request would cross a configured operation or
byte target. Explicit client batches remain more efficient when work is already
available because they avoid per-RPC transport overhead. Aggregation never
crosses stores or Sink processes.

See [Synchronous request batching](configuration.md#synchronous-request-batching)
for every queue and batching setting.

## Mutation completion

`Write` and `Delete` support three completion modes:

- `WAIT_UNTIL_APPLIED` returns after the storage adapter acknowledges the work.
- `WAIT_UNTIL_VISIBLE` also waits until a following search read can observe it.
  Elasticsearch and OpenSearch use `refresh=wait_for`; acknowledged MongoDB
  writes are already visible.
- `RETURN_AFTER_ACCEPTED` returns after the selected store's Kafka publisher
  durably accepts the mutation.

A store without Kafka remains fully usable for synchronous operations. An
asynchronous operation targeting it receives a retryable `UNAVAILABLE` failure
without failing unrelated operations in the same request.

## Lua read-modify-write

A merge carries a Lua program and an explicitly encoded incoming document. Sink
reads the current record, requires both documents to use the same encoding,
runs the program, and writes the result using that encoding with the adapter's
revision precondition. On a revision conflict, Sink reads the latest record and retries
up to the configured limit. Each attempt is atomic without exposing a
database-specific compare-and-swap API to applications.

Programs travel with the request so business rules can be versioned with client
code. A batch declares identical source once by SHA-256 digest. Sink caches the
compiled program but creates a fresh VM for each execution, isolating mutable
global state. Asynchronous queue records contain the full source and remain
replayable without server-side rule registration.

Lua runs inside a restricted environment with time, instruction, source, and
result limits. Merge functions receive only `current` and `incoming`; versioned
`sink.v1` helpers provide validated collection operations and a retry-stable
observation time. See the [Lua merge developer guide](lua-merge-guide.md) for
the full authoring contract and [Lua merge programs](configuration.md#lua-merge-programs)
for runtime configuration.

## Asynchronous delivery

Kafka is configured per store. Different stores may use different clusters,
topics, consumer groups, retry policies, and dead-letter topics. The publisher
uses a deterministic encoded record address as the Kafka key, keeping mutations
for one record in a single partition and in submission order.

Kafka is disabled by default. Each enabled store establishes its source and
DLQ policy independently before publication or consumption. Sink creates missing
Topics, reconciles replicas and durability settings, and refuses any partition
count mismatch. Increasing partitions requires pausing all publishers, draining
old work, and explicitly migrating the Topic; key routing can otherwise change.

Workers disable auto-commit. Temporary backend, DLQ publication, and offset
commit failures retain unresolved records and retry with bounded backoff. The
attempt count limits each processing round, not message lifetime. Permanent
failures are copied to DLQ before committing their source offsets. A permanent
CREATE failure does not prevent a later valid update to the same key; a temporary
failure blocks following work for that key. Each partition commits only its
resolved contiguous prefix.

Processing has a 20-second default deadline and is cancelled when a rebalance
callback is waiting. A separate settlement window of at most five seconds allows
DLQ acknowledgement and offset commits before releasing blocked rebalances.
Adapters must honor cancellation. This is not a storage fencing token: a write
whose acknowledgement was lost can still complete. Concurrent producers,
synchronous writes, and DLQ replay do not form a single global ordering domain.

Delivery is at least once. **Applications own business idempotence**, including
client retries, worker retries, crashes after storage commit, lost responses,
and manual replay. CAS prevents conflicting revisions from overwriting each
other; it does not identify repeated business intent. Use a business operation
identifier and an atomic check-and-apply rule where repeat effects are unsafe.
An increment without such a rule can execute more than once.

## Health and observability

Invalid configuration fails startup. Unavailable stores are connected lazily,
and each Kafka store retries its Topic setup independently. Healthy stores can
serve while another dependency is recovering. Only Kafka clients for a store
whose policy has been established can accept or consume asynchronous work.

At runtime in `server` and `all` modes, the standard gRPC health service reports
process readiness. Each storage and configured Kafka publisher also has its own
health service name. A failed dependency becomes `NOT_SERVING` without marking
unrelated stores unavailable.

The optional Prometheus listener exports build, gRPC, operation, batching,
merge, publisher, and worker metrics. Worker progress metrics label only configured store names. Labels exclude
client-provided namespaces, datasets, keys, and error text. See
[Prometheus metrics](configuration.md#prometheus-metrics) for the metric list
and network exposure guidance.

## Reliability boundaries

- Multi-record requests are not transactions.
- Async delivery can execute a mutation more than once.
- Sink does not automatically make an arbitrary complete-document write
  idempotent.
- Storage schema, indexes, mappings, aliases, backup, and replication remain
  responsibilities of the backing systems.
- Batching and queues are local to one Sink process; replicas do not share
  in-memory state.
- Dependency failures are reported separately; shared-process resource
  exhaustion still requires deployment isolation and capacity planning.

These boundaries keep the record contract portable without hiding guarantees
that only a specific database or deployment can provide.
