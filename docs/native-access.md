# Native commands, paged queries, counts, scans, and returned writes

Applications can move their database connections and runtime clients into Sink
while retaining native query languages. `Execute` sends native commands and
returns the database response; `Query` returns an independent page, `Count`
counts matching results, and `Scan` returns resumable live pages. Existing `Read`, `Write`, `Delete`, Lua Merge, completion modes, and
returned-document options remain available.

These capabilities are additive to the protocol, not a database wire-protocol
proxy. Execute validates the adapter's supported operations before forwarding
their native payloads. Rejected operations return `INVALID_ARGUMENT` without a
backend request. MongoDB writes use the revision-protected command contract
below; HTTP search permits document/query operations and rejects index lifecycle
management. Supported MongoDB native writes atomically install fresh Sink
revisions, so a concurrent record Merge detects
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
server across RPCs. Use Scan for resumable `find`, or Query for independent
`find` and read-only `aggregate` pages. `listIndexes` and `listCollections` are
not exposed by these native read RPCs. Cursor writes such as `bulkWrite` and
data-writing aggregations are outside the supported read contract; collection-level
`insert`, `update`, and `delete` commands remain available through Execute.

Client-managed sessions and transactions are also unsupported. `startSession`,
`refreshSessions`, `endSessions`, `commitTransaction`, and `abortTransaction`
are rejected. Duplicate top-level fields and driver-managed `$db`, `lsid`,
`txnNumber`, `startTransaction`, `autocommit`, `apiVersion`, `apiStrict`,
`apiDeprecationErrors`, `maxTimeMS`, and `$readPreference` fields are rejected.
Use the RPC context for deadlines. Other filters, sorts, projections, hints,
write concerns, and native command options remain in BSON.

For example, `Execute` with `{find: "products"}` returns `INVALID_ARGUMENT`
without opening a database cursor. The same command passed to `Scan` returns
one BSON page and an optional continuation cursor. Any database cursor is
exhausted or closed before the RPC returns.
`findAndModify` returns a single response document and is accepted by Execute.

### Elasticsearch and OpenSearch endpoints

Execute permits native document and query operations within the configured
store. It is not unrestricted HTTP passthrough. GET, HEAD and OPTIONS requests
can inspect native endpoints; other methods must match a supported route:

- Document `_doc`, `_create`, and `_update` writes, including individual deletes
  and IDs containing escaped slashes; POST `_doc` also supports generated IDs.
- POST `_bulk`, `_mget`, `_msearch`, `_search`, `_search_shards`, `_count`,
  `_termvectors`, `_mtermvectors`, `_analyze`, `_field_caps`, `_rank_eval`, and
  `_terms_enum`, with an optional index selector. Document `_explain`, search
  templates and `_validate/query` are also supported.
- POST `_update_by_query`, `_delete_by_query`, and cluster-level `_reindex`.
- PUT/POST `_mapping`; POST `_refresh`, `_flush`, `_forcemerge`, and `_cache/clear`.
- Native scroll and point-in-time opening, advancement and cleanup routes.

Explicit index creation/deletion, alias changes (including every `_aliases`
action), open/close, rollover, clone/split/shrink, snapshot restore/mount, data
stream management, settings/templates, lifecycle policies, and replication
management are rejected before sending an HTTP request. Unknown administrative
or plugin write routes are also rejected; there is no unsafe passthrough option.
This restriction includes policy changes that could schedule a later rollover
or deletion. Use a separate database administration path for such operations.

Search record revisions use the backend's sequence number and primary term;
they are valid within one concrete index's lifetime. External administration,
existing ILM/ISM policies and alias changes can still replace that index. They
must be coordinated with record clients, which must discard old revisions and
snapshots after an index change. Sink does not coordinate external index
lifecycle operations. Ordinary document writes retain backend behavior,
including any configured automatic index creation.

Paths are absolute endpoint paths with no host, query, dot segments, or fragment.
Percent-encoded document IDs are supported. Supply query parameters separately;
`{index}` can use the backend's index and alias expressions. Authentication and
the endpoint host remain configured by Sink.

For `_msearch` and `_bulk`, set `content_type=application/x-ndjson` and retain
the final newline. For JSON bodies, use `application/json`. Other media types
are forwarded unchanged through Execute.
For permitted requests, the body stays opaque. Callers may manage search scrolls
or other native pagination explicitly through Execute and are responsible for
closing them.

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
`allowPartialResults: true` is rejected: a page must not silently omit unavailable
shards. The lookahead document used only for `has_more` does not consume MongoDB's
returned-document byte budget; the driver wire limit still applies.
For aggregate, explicit sort and projection follow the supplied pipeline, then
skip/limit apply to its output. HTTP Query requires `_search`, replaces from/size,
and maps explicit sort/projection to sort and `_source` selection.

Each call is independent: no cursor or session is retained between RPCs. Any
short-lived MongoDB cursor is closed before returning. Search does not open a
scroll or PIT; manual pagination inputs (`scroll`, `pit`, `search_after`) and
response-truncating `filter_path` are rejected. Use a stable native sort with a
unique tie-breaker. Concurrent changes may shift pages; this is not a snapshot.
Search Query, Count and Scan require explicit timeout and shard-completion
fields. Missing or inconsistent completion evidence fails the entire RPC without
returning a page, total or continuation cursor.
When cross-cluster metadata is present, every requested cluster must have
succeeded; skipped, running, partial or failed clusters reject the response even
if the HTTP status is 200 and all reported shards succeeded.
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
read-only aggregate pipeline. Find predicates using `$where`, `$near`, or
`$nearSphere` cannot use the aggregation count path. Sink selects a native find
cursor for these predicates before execution, requests a small constant
projection, and counts matches one batch at a time without retaining documents.
The result is exact under that cursor's native read semantics (`estimated=false`).
This path transfers one small result per match and can be slower than database
aggregation; the same Count deadline and memory limits still apply. Cursor
failure or cancellation returns an error, never a partial total.

Find filters, hints, collation, read concern, let, comment and allowDiskUse are
supported; other non-pagination find options are
rejected when they cannot be translated faithfully. HTTP Count obtains exact
matching-document totals before pagination/collapse, without computing hit
presentation or aggregations (`estimated=false`). Partial, timed-out, shard-failed
and approximate HTTP results fail instead of returning a misleading count. Query
and Count report backend failures as gRPC errors; use Execute when a complete native reply is needed.

Count and Query are separate observations and can differ during concurrent writes.
Neither RPC is automatically retried. Both use the same admission, timeout and
byte limits as Execute.

## Scan

Scan is one unary RPC per page. `ScanRequest` contains the shared `Command`,
`batch_size` (default 100, maximum 1000), and an opaque bytes `cursor`.
`ScanResponse` contains native `documents` and `next_cursor`. A byte-limited
page may contain fewer than `batch_size` documents. Only an empty `next_cursor`
marks the end observed by this request; a short page alone does not.

Start with an empty cursor. For subsequent calls, resend the same Command and
pass the previous `next_cursor` as `cursor`. Batch size can change between calls.
After successfully processing a page, persist its cursor; on completion, persist
an explicit completed state rather than using an empty cursor to restart later.

Cursors carry the seek position and a fingerprint binding it to the complete
Command, including the store, namespace, body, parameters and caller headers.
They are independent of any Sink process or database session, so a request can
continue on another Pod. Treat them as opaque and preserve them byte-for-byte;
changing the Command or using a corrupt cursor returns `INVALID_ARGUMENT`.
Cursor size is limited to 64 KiB. They are continuation markers, not credentials;
backend authentication and request validation apply on every call. Checksums
protect against accidental corruption, not caller forgery.

There is no `keep_alive`, cursor expiry, create/close RPC, or absolute scan task
lifetime. No request admission slot or database cursor remains occupied between
pages. The business task owns its overall deadline and retention. Every page
still uses the ordinary request deadline and admission limits. Cursors assume
the same logical dataset and schema; after external recreation or remapping,
start a new scan explicitly.

### MongoDB

Scan supports `find`, ordered by `_id` ascending by default; an explicit
`sort: {_id: -1}` scans descending. Other sorts, skip/limit, aggregates,
`listIndexes` and `listCollections` are rejected. Query remains available for
independent find and read-only aggregation pages.

Sink adds an indexed seek predicate rather than a growing skip offset.
Expression comparison preserves BSON ordering across mixed `_id` types and
wraps the saved value as a literal. Scan enforces simple collation even when the
collection has another default; non-simple collation is rejected because it
can collapse distinct IDs into the same ordering position. Use an appropriate
index for additional filters; the seek predicate alone does not make arbitrary
filters efficient. Sharded collections must have globally unique `_id` values.

Filters, hint, let, comment, read concern, allowDiskUse and ordinary projections
remain native. `_id` may be excluded from returned documents: Sink reads it for
the cursor and removes it from the result. Computing or partially projecting
`_id` is rejected. MongoDB page cursors are exhausted or closed before returning;
cleanup uses a separate five-second context after cancellation.

### Elasticsearch and OpenSearch

Scan accepts `_search` with an explicit JSON `sort` array. Use ordinary indexed
fields with doc_values; the complete sort tuple must be unique across the
selected indices, non-null and immutable. For example, sort by a unique business
UID keyword field. `_id`, `_doc`, `_shard_doc`, `_score`, scripted sorts, missing
value substitutions and URL `sort` are rejected. Entries are field names or
single-field objects whose values are `asc` or `desc`.

Sink uses `search_after`, with no scroll or PIT. Initial size/from are replaced
by Scan's page controls, and total-hit counting is disabled. Manual pagination,
collapse, aggregation, rescore, suggest, knn, retriever and early termination are
not supported by Scan. Use Query or Execute for those native operations where
supported. Duplicate sort tuples observed within a page or its lookahead cause
an error; ensuring global uniqueness and immutability belongs to the caller.

Each result is a complete JSON hit, including `_id`, `_source` when requested,
and exact native sort values. Request headers and parameters are validated and
forwarded on every page; callers must preserve them while resuming. Incomplete,
timed-out or shard-failed backend responses return an error without a page.

### Changes, failures and retries

Scan does not provide a snapshot. New records after the saved position may
appear, records inserted before it may be missed, and updates/deletes can change
later pages. Concurrent changes may also change a retried page. Do not change
ordering fields during a scan. A final page describes the end at that request's
observation time; it does not wait for future inserts.

A failed RPC returns no page or new cursor. Reuse the last saved cursor to retry:
there is no stateful backend position that a lost response could silently
advance. Process pages idempotently and checkpoint only after successful
processing; Sink does not provide exactly-once business effects. A retry of the
initial empty cursor starts at the beginning of the current live query.

During rolling updates, the old server drains active page requests within its
shutdown deadline. If a request is interrupted, another server can accept the
same Command and saved cursor. Neither SDK Scan nor the server retries a failed
page automatically. Cancellation does not invalidate a saved cursor.

## Returned writes

Set `WriteOperation.return_document` (SDK `WithReturnedDocument()` or
`Record.ReturnDocument`) to receive the operation's logical Put/Merge document
alongside its successful revision. Only APPLIED results include documents.
`RETURN_AFTER_ACCEPTED` with this option is rejected before publishing anything;
use `WAIT_UNTIL_APPLIED` or `WAIT_UNTIL_VISIBLE`.

Sink takes the document from the successful commit candidate, never from a later
Read. Every operation requesting a returned document commits independently of
preceding and following operations. Other operations can still share a folded
commit and revision; setting the option on one operation does not promise
independent commits for the entire same-address chain. For three increments
where only the first requests a document, the first can return value 1 and
revision A while the remaining two share the commit of value 3 and revision B.
Request a document on each operation when each needs its own committed output
and revision. A conflicting attempt's speculative output is not returned. A
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
Execute, Query, Count and each Scan page use `service.request_timeout_seconds`.
A shorter caller deadline wins. Between Scan calls there is no admission
reservation and no background cursor to keep alive.

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
