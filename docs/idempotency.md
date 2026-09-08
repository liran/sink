# Idempotent record writes

`WriteIdempotent` is an opt-in RPC for Put/Create/Replace/Upsert and Lua Merge.
Every operation has a stable `operation_id`. Ordinary `Write`, hard Delete and
native Execute remain available with their existing semantics; do not retry
those mutations blindly after an unknown outcome.

## Supported scope

The current implementation requires MongoDB transactions (replica set or mongos).
Search storage and standalone MongoDB reject protected requests, including before
Kafka publication. This does **not** fix duplicate non-idempotent mutations on
Elasticsearch/OpenSearch; applications using those routes still need business
idempotence. In-memory storage does not advertise durable idempotence either.

A transaction commits one business operation and its receipt in the same
MongoDB database, using snapshot reads and majority write concern. Receipts live
in the reserved `__sink_receipts_v1` collection, independent of business documents.
An ownership validator is created with this collection; an existing unowned
collection is rejected without installing a TTL index or modifying its data.
MongoDB native replacement/pipeline updates and hard deletion of business records
do not erase them. Record APIs and native commands reject direct access to that
reserved collection. Direct database administration and restores are outside the
contract: protect the receipt collection from manual modification and restore it
consistently with business data.

## IDs, retries and retention

The Go SDK's `ClientOptions.IdempotentWrites` enables automatic IDs for record
writes. Explicit `Record.OperationID` / `WriteOperation.WithOperationID` also
enables protection, with missing IDs in that call generated automatically.
IDs are allocated before request splitting. SDK transport retries reuse the
exact protected request, and uncertain outcomes retain pending operations in
`WriteTransportError`. Separate calls with no explicit ID are separate operations.

For cross-call/process retry, persist the SDK ID, or derive one with
`OperationIDFor(businessOperationKey, originalCreationTime)`. Never use the retry
time. The wire format is `v1:<Unix milliseconds>:<base64url identity>`; arbitrary
plain business keys are not valid wire IDs. The full ID is scoped to logical
store and namespace; the fingerprint also covers the complete record address,
action/mode, payload, resolved Lua digest and returned-document flag. Changing
these for a committed ID produces a permanent conflict. Equivalent JSON/BSON
with different bytes is a different payload.

Receipts expire **31 days from the operation's original creation**, not its last
retry. A TTL index reclaims expired receipts; the service rejects expired IDs
independently of whether TTL deletion has run. There is no capacity-based early
eviction. Provision disk for the retained receipts and alert on growth. Replay
windows longer than 31 days require a separate business reconciliation policy;
do not generate fresh IDs just to bypass expiry. Longer configured Kafka/DLQ
retention does not extend this deduplication window.

Only committed APPLIED outcomes receive receipts. Retrying the same committed
operation returns its original revision and optional document, even after later
writes or deletion. This is historical evidence of that commit, not current
record state. Failed/aborted attempts do not reserve an ID indefinitely and
do not promise stable failure results. Returned receipts are bounded by the
smaller of `max_read_bytes` and 15 MiB; an oversized receipt aborts the transaction.

## Queue and rollout safety

The SDK uses the new RPC, so old servers return UNIMPLEMENTED instead of silently
ignoring the ID. Protected queued writes use envelope version 2 and retain the
full ID and Lua source through worker/DLQ replay. Old workers reject version 2
before applying it. Upgrade servers and all workers before enabling SDK
idempotent writes; otherwise work may be quarantined, not silently downgraded.

ACCEPTED means only Kafka acceptance. A worker crash after transaction commit but
before offset settlement causes receipt replay rather than another mutation.
Atomicity remains per operation, not per batch. Protected operations are not
allowed to pass a failed protected predecessor on the same record: the remaining
chain returns retryable failures and must retain its IDs when retried. This
prevents a whole-RPC retry from applying an earlier missing effect after a later
committed effect.
Protected operations are not
folded, and protected original RPCs are not coalesced with other RPCs. Admission
includes MongoDB wire space and receipt copies. This costs additional storage
and transaction round trips; use appropriate batch sizes, byte budgets and
deadlines for the workload.

Native Execute and hard Delete have no operation-ID contract. The guarantees
above do not cover arbitrary external side effects or certify MongoDB's own
election, disk durability or backup implementation.
