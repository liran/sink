# Native commands, paged queries, counts, scans, and returned writes

Applications can move their database connections and runtime clients into Sink
while retaining native query languages. `Execute` sends native commands and
returns the database response; `Query` returns an independent page, `Count`
counts matching results, and `Scan` manages streaming cursor queries. Existing `Read`, `Write`, `Delete`, Lua Merge, completion modes, and
returned-document options remain available.

These capabilities are additive to the protocol, not a database wire-protocol
proxy. Native commands retain backend-specific semantics, subject to the
revision-protected MongoDB write contract below. Supported MongoDB native writes
atomically install fresh Sink revisions, so a concurrent record Merge detects
the changed document and recomputes instead of overwriting an undetected update.
Native mutations do not run Lua merges or participate in record batching or
asynchronous completion modes. They are not automatically retried or deduplicated.

## Execute

All four native RPCs share `Command`: `store`, `namespace`, `method`, `path`,
`query`, `headers`, `content_type`, and `payload`. The configured store selects
the adapter. MongoDB reads namespace and the BSON payload; HTTP search reads
method/path/query/headers and the original body. Unused fields must be empty.
There are no database-specific protobuf branches or extra payload envelopes.
Sink selects the connection and authentication;
callers cannot supply a URI, endpoint host, or credentials. Unknown stores return
`INVALID_ARGUMENT`; adapters without native support return `UNIMPLEMENTED`.

The response contains `content_type`, opaque `payload`, and `success`. Search
responses also contain the HTTP status and repeated response headers. Sink does
not rewrite the database response into the record API's failure model. A database
error with a complete response is a successful gRPC exchange with `success=false`;
the SDK returns both the response and a `NativeError` retaining it. Transport,
validation, cancellation, and capacity failures use gRPC errors without a native
response. HTTP 2xx marks search success; inspect the raw body for partial errors,
including item failures inside `_msearch` and `_bulk` responses.

MongoDB payloads are raw BSON, including BSON datetimes, numeric widths, ObjectIDs,
binary values, and database error fields. Search payloads retain the native HTTP
entity bytes and framing, including NDJSON, JSON whitespace, and non-JSON output.
HTTP transport may decode compression and normalize header names; this is not
byte-for-byte forwarding of HTTP packets. Redirects are returned without being
followed. Request headers are forwarded except transport-owned headers:
`Authorization`, `Proxy-Authorization`, `Host`, `Connection`, `Proxy-Connection`,
`Keep-Alive`, `TE`, `Trailer`, `Transfer-Encoding`, `Upgrade`, and `Content-Length`.
Set `content_type` directly; `Content-Type` in request headers is rejected.
The query field uses URL query encoding and preserves repeated parameter values.
Nonempty payloads require a valid content type; no encoding is inferred from paths.

### MongoDB commands

The database is provided as `Command.namespace`, separately from the ordered
BSON command in `Command.payload`, with `content_type=application/bson`. The first BSON field is
the command name; use a struct, `bson.D`, or `bson.Raw`, not an unordered map.

MongoDB Execute uses a command allowlist. Supported commands are `insert`,
`update`, `delete`, `findAndModify` (also `findandmodify`), `count`, `distinct`,
`explain`, `createIndexes`, `dropIndexes`, `collStats`, `dbStats`, `ping`, `hello`,
`isMaster`/`ismaster`, `buildInfo`, and `serverStatus`. Database validation and
write errors still retain their native response envelopes. Unknown commands,
`drop`, `dropDatabase`, collection renames/conversions, and commands such as
`applyOps` or `mapReduce` are rejected before execution: they cannot bypass the
revision protocol through opaque native writes. There is no unsafe passthrough
fallback. This intentionally narrows the earlier unrestricted Execute contract;
administrative operations outside this list must use a separately controlled
database administration path.

#### Revision-protected native mutations

Every inserted or replacement document receives a fresh opaque revision in the
configured `metadata_field` (default `__sink`). Operator updates add that metadata
with `$set` in the same database update as the business mutation. Update pipelines append a final
literal metadata assignment, including after `$project`, `$replaceRoot`, or
`$replaceWith` removes/replaces the original root. The database commits business
data and the new revision atomically for each document, without a second update.
`findAndModify` supports the same update forms; `upsert` and `multi` preserve
their native meanings. Each multi-update statement generates a fresh token shared
by its matched documents; revisions are compared only for the same record.

Callers cannot explicitly assign, unset, rename, or replace the reserved metadata
field or its subpaths in inserted/replacement documents, update operators, or
named pipeline paths. Dynamic root replacement cannot control the final metadata:
Sink overwrites it in the appended stage. Unknown update operators/pipeline stages,
duplicate BSON fields, writes to `system.*` collections, and unsafe members are
rejected while preparing the entire command, before any member is sent to MongoDB.
Ordinary backend errors may still
partially apply a native batch according to its `ordered` setting; inspect the
native write-error envelope. No-op updates may now report a modification because
the revision changes even when business fields remain equal.

Hard deletes remain native deletes. They invalidate a stale revision match by
removing the record; subsequent supported inserts/upserts/replacements assign a
fresh revision, so reusing an `_id` cannot revive a pre-delete revision. This does
not make an unconditional replacement or delete conditional, provide cross-record
transactions, or deduplicate retries.

All writers to the same collection must use this protocol and the same
`metadata_field`, including other Sink deployments and alternate store names.
Direct database writers, restore tools, and collection administration can still
bypass it. Isolate their privileges and coordinate such work; Sink cannot enforce
the protocol on connections it does not own. Native query/command responses can
include metadata as before; the record API continues to hide it.

Cursor commands such as `find`, `aggregate`, `listIndexes`,
`listCollections`, `getMore`, `killCursors`, `parallelCollectionScan`, and
MongoDB's cursor-returning `bulkWrite` are rejected before execution. Execute
uses the driver's ordinary command path and does not retain a session or pin a
server across RPCs. Use Scan for `find`, read-only `aggregate`, `listIndexes`,
and `listCollections`. Cursor writes such as `bulkWrite` and data-writing
aggregations are outside the supported Scan contract; collection-level
`insert`, `update`, and `delete` commands remain available through Execute.

Client-managed sessions and transactions are also unsupported. `startSession`,
`refreshSessions`, `endSessions`, `commitTransaction`, and `abortTransaction`
are rejected. Duplicate top-level fields and driver-managed `$db`, `lsid`,
`txnNumber`, `startTransaction`, `autocommit`, `apiVersion`, `apiStrict`,
`apiDeprecationErrors`, `maxTimeMS`, and `$readPreference` fields are rejected.
Use the RPC context for deadlines. Other filters, sorts, projections, hints,
write concerns, and native command options remain in BSON.

For example, `Execute` with `{find: "products"}` returns `INVALID_ARGUMENT`
without opening a database cursor. The same command passed to `Scan` streams
individual BSON documents and closes the cursor when the stream ends.
`findAndModify` returns a single response document and is accepted by Execute.

### Elasticsearch and OpenSearch endpoints

Execute forwards any valid HTTP method and endpoint path within the configured
store; there is no endpoint or action allowlist. This includes document writes,
`_bulk`, index deletion, alias `remove_index`, queries, index management, and
plugin endpoints. The backend returns its own response for unsupported requests.

Paths are absolute endpoint paths with no host, query, dot segments, or fragment.
Percent-encoded document IDs are supported. Supply query parameters separately;
`{index}` can use the backend's index and alias expressions. Authentication and
the endpoint host remain configured by Sink.

For `_msearch` and `_bulk`, set `content_type=application/x-ndjson` and retain
the final newline. For JSON bodies, use `application/json`. Other media types
are forwarded unchanged through Execute.
The body stays opaque. Callers may manage search scrolls or other native
pagination explicitly through Execute and are responsible for closing them.

## Query and Count

`QueryRequest` combines `Command`, `page`, `page_size`, ordered `sort` keys, and
an optional `projection`. Pages start at 1 (zero defaults to 1); page size defaults
to 100 and is capped at 1000. Query returns native `documents` and `has_more`,
fetching one extra result to determine whether another page exists. It never runs
Count automatically. An oversized response fails rather than shortening the page.

Sort entries contain `field` and `descending`. Projection contains `fields` and
`exclude`: include the fields by default, or exclude them when true. Empty sort
preserves native sorting; absent projection preserves native projection. An
explicit projection with no fields selects all fields. Duplicate or blank fields
are rejected. HTTP projection applies to `_source`; hit metadata is retained.
MongoDB retains its native `_id` projection rules.

MongoDB Query supports `find` and read-only `aggregate`. For find, Query replaces
native skip/limit, and explicit sort/projection replace their native counterparts.
For aggregate, explicit sort and projection follow the supplied pipeline, then
skip/limit apply to its output. HTTP Query requires `_search`, replaces from/size,
and maps explicit sort/projection to sort and `_source` selection.

Each call is independent: no cursor or session is retained between RPCs. Any
short-lived MongoDB cursor is closed before returning. Search does not open a
scroll or PIT; manual pagination inputs (`scroll`, `pit`, `search_after`) and
response-truncating `filter_path` are rejected. Use a stable native sort with a
unique tie-breaker. Concurrent changes may shift pages; this is not a snapshot.
Deep pages incur backend offset costs and result-window limits, including the
extra result needed for `has_more`. Use Scan for sustained traversal.

`CountRequest` takes the same Command and returns uint64 `count` plus `estimated`.
MongoDB find with a missing or empty document filter automatically uses
`EstimatedDocumentCount`, obtaining collection metadata without scanning the
documents. `estimated=true` identifies this fast path. Filtered finds and
aggregate pipelines remain exact. Options that metadata counts cannot honor (such as hint,
collation, readConcern, let or allowDiskUse) also retain the exact path. Comment
is forwarded by both paths; presentation options do not affect the total.
Metadata estimates follow MongoDB's accuracy limitations, including sharded
collections and recovery; use an aggregate pipeline when an exact total is required.

MongoDB counts find matches before skip/limit, or the output of a supplied
read-only aggregate pipeline. Find filters, hints, collation, read concern, let,
comment and allowDiskUse are supported; other non-pagination find options are
rejected when they cannot be translated faithfully. HTTP Count obtains exact
matching-document totals before pagination/collapse, without computing hit
presentation or aggregations (`estimated=false`). Partial, timed-out, shard-failed
and approximate HTTP results fail instead of returning a misleading count. Query
and Count report backend failures as gRPC errors; use Execute when a complete native reply is needed.

Count and Query are separate observations and can differ during concurrent writes.
Neither RPC is automatically retried. Both use the same admission, timeout and
byte limits as Execute.

## Scan

`ScanRequest` wraps the shared Command and an optional batch size (default 100,
maximum 1000). Sink streams `ScanResponse.documents` until completion and owns
opening, advancing, and closing the backend cursor. Cancellation, callback
failure, timeout, and database errors terminate the scan; cleanup uses a separate
five-second context even after the original context was canceled. Cleanup is
best effort if the database is unavailable. Server cursor expiry remains the
fallback after a lost connection or process termination.

MongoDB Scan accepts `find`, read-only `aggregate`, `listIndexes`, and
`listCollections`. Data-writing aggregation stages (`$out`, `$merge`), change
streams, tailable cursors, `awaitData`, `noCursorTimeout`, and `singleBatch` are
rejected. This check is conservative and applies to nested BSON fields as well.

MongoDB Scan streams each native cursor document as BSON. It overrides the
initial and subsequent batch sizes. Filters, sort, projection, aggregation, and
query limits remain in the command. Unlike standard Read, native documents can
include Sink's revision metadata field; use an explicit projection when it
should be excluded. Scan provides the database's read semantics, not a new
cross-page snapshot guarantee.

Search Scan accepts a `_search` request and uses a two-minute scroll keepalive.
It sets `size` from batch size, removes `from`, and defaults sort to `_doc` when
none is supplied. `source`/`filter_path` query parameters and `search_after` are rejected:
use the JSON body for Scan, or Execute for manual pagination. Every streamed JSON
document is a **complete search hit**, including `_id`, `_source`, score and sort
values when present. Aggregation envelopes and total-hit metadata belong to
Execute. Timed-out, terminated, or shard-failed pages fail the scan rather than
silently returning an incomplete result set. Execute can also manage a native
scroll explicitly; in that case the caller must close it.

The SDK visits documents one at a time while retaining one received page. It
cancels the stream when the callback returns an error. Callbacks doing blocking
work should also observe their context. Previously completed callbacks remain
completed if a later page fails. Neither Sink nor the SDK restarts a partial
scan; applications choosing to restart must handle duplicate effects.

## Returned writes

Set `WriteOperation.return_document` (SDK `WithReturnedDocument()` or
`Record.ReturnDocument`) to receive the operation's logical Put/Merge document
alongside its successful revision. Only APPLIED results include documents.
`RETURN_AFTER_ACCEPTED` with this option is rejected before publishing anything;
use `WAIT_UNTIL_APPLIED` or `WAIT_UNTIL_VISIBLE`.

Sink takes the document from the successful commit candidate, never from a later
Read. When any operation in a same-address execution chain requests a returned
document, every operation in that chain executes and commits separately in order.
Two successful counter increments therefore receive their own values and
revisions. A conflicting attempt's speculative output is not returned. A
definite CAS conflict may recompute against a fresh snapshot; transport failures
and unknown commit outcomes are not automatically replayed.

The returned document is the logical document submitted to the backend. Sink
does not re-read it to incorporate new backend-generated fields (such as a
MongoDB `_id` absent from the candidate), internal revision metadata, or search
ingest transformations. Fields already present in the candidate are retained.
Use Read for a later stored observation. Returning a document does not make a whole batch a
transaction, guarantee exactly-once increments, or prevent a subsequent writer
from changing the record.

## Limits, deadlines, and retries

Native RPCs use the existing per-process and per-store admission limits.
Execute follows `service.request_timeout_seconds`; Scan uses that duration as
the maximum interval without a successfully sent page, plus an absolute
`service.scan_timeout_seconds` limit (default 900). A shorter caller deadline
wins. A slow reader retains its admission slot until termination and cannot
cause unbounded page accumulation.

Execute responses and returned Write documents share `service.max_read_bytes`
semantics; returned-document budgets are per original RPC even after batching.
Output space is reserved before committing a returned write. A candidate that
cannot fit fails before its own write; earlier operations may already be applied.
Scan pages use at most min(`service.max_read_bytes`, 4 MiB), with count and byte
limits both enforced. A single oversized document fails with
`RESOURCE_EXHAUSTED`. Search also caps each complete backend HTTP response before
decoding it, so lower batch sizes may be necessary for large hits. MongoDB driver
wire buffers have separate conservative admission reservations; configured byte
budgets are not an exact process memory limit.

Execute and Scan do not use the record micro-batcher or asynchronous queue. The
SDK never retries either call. Native search requests use one endpoint attempt;
MongoDB command execution uses the driver's ordinary command path and its
configured retry behavior. Native mutations can have taken effect even when an
acknowledgement is lost. Callers must inspect native results and determine
whether replay is safe; a timeout does not imply that a write was unapplied.
