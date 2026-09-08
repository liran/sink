# Native queries, streaming scans, and returned writes

Applications can move their database connections and runtime clients into Sink
while retaining native query languages. Existing `Read`, `Write`, `Delete`, Lua
Merge, completion modes, and per-record revisions remain the mutation contract.
`Execute` handles native queries and index setup; `Scan` handles cursor lifetime.
The Go SDK exposes both methods plus returned-document options on Dataset writes.

These are additive capabilities, not a database wire-protocol proxy. Native
queries remain specific to the selected backend. Multi-record transactions,
client-managed sessions, change streams, and arbitrary native document mutations
are outside this contract. Business operations confined to one document can use
a Lua Merge with compare-and-swap retries. Cross-document atomicity requires a
different data model or an additional transaction contract.

## Execute

An `ExecuteRequest` names exactly one configured `store` and either a MongoDB
command or a search HTTP command. Sink selects the connection and authentication;
callers cannot supply a URI, endpoint host, or credentials. Unknown stores return
`INVALID_ARGUMENT`; adapters without native support return `UNIMPLEMENTED`.

The response contains `content_type`, opaque `payload`, and `success`. Search
responses also contain the HTTP status and repeated response headers. Sink does
not rewrite the database response into the record API's failure model. A database
error with a complete response is a successful gRPC exchange with `success=false`;
the SDK returns both the response and a `NativeError` retaining it. Transport,
validation, cancellation, and capacity failures use gRPC errors without a native
response. HTTP 2xx marks search success; inspect the raw body for partial errors,
including failures inside `_msearch` responses.

MongoDB payloads are raw BSON, including BSON datetimes, numeric widths, ObjectIDs,
binary values, and database error fields. Search payloads retain the native HTTP
entity bytes and framing, including NDJSON, JSON whitespace, and non-JSON output.
HTTP transport may decode compression and normalize header names; this is not
byte-for-byte forwarding of HTTP packets. Redirects are returned without being
followed. Supported request headers are `Accept`, `Content-Type`, and `X-Opaque-Id`.
The query field uses URL query encoding and preserves repeated parameter values.

### MongoDB commands

The database is separate from an ordered BSON command. The first BSON field is
the command name; use a struct, `bson.D`, or `bson.Raw`, not an unordered map.

| Method | Commands |
| --- | --- |
| Execute | `count`, `distinct`, `collStats`, `dbStats`, `ping`, `createIndexes`, `dropIndexes` |
| Execute | `explain` wrapping a read-only `find`, `aggregate`, `count`, or `distinct` |
| Scan | `find`, read-only `aggregate`, `listIndexes`, `listCollections` |

Filters, sorts, projections, hints, aggregation pipelines, and supported native
options stay in BSON. For example, index setup can pass `createIndexes` with
unique, compound, or partial index definitions; the database decides whether an
existing definition is compatible and returns its original result.

Duplicate top-level fields and driver-managed `$db`, `lsid`, `txnNumber`,
`startTransaction`, `autocommit`, `apiVersion`, `apiStrict`, `apiDeprecationErrors`,
`maxTimeMS`, and `$readPreference` are rejected. Use the RPC context for deadlines.
Data-writing aggregation stages (`$out`, `$merge`), change streams, tailable
cursors, `awaitData`, `noCursorTimeout`, and `singleBatch` are rejected. This check
is deliberately conservative and applies to nested BSON fields as well.

### Elasticsearch and OpenSearch endpoints

Paths are unescaped absolute endpoint paths with no host, query, empty segments,
dot segments, or fragment. `{index}` can use the backend's index/alias expressions.

| Endpoint | Methods |
| --- | --- |
| `/` | GET, HEAD |
| `/_search`, `/_msearch`, `/_count`, and `/{index}/` equivalents | GET, POST |
| `/_search/scroll` | GET, POST, DELETE |
| `/{index}` | GET, HEAD, PUT (explicit index creation) |
| `/{index}/_mapping`, `/{index}/_settings` | GET, PUT |
| `/_aliases` | POST, with `add` and `remove` actions only |
| `/{index}/_alias` | GET |
| `/{index}/_refresh` | POST |
| `/_cat/indices`, `/_cat/indices/{index}` | GET |
| `/{index}/_doc/{id}`, `/{index}/_source/{id}` | GET, HEAD |
| `/{index}/_explain/{id}` | GET, POST |

`_msearch` defaults to `application/x-ndjson`; retain the final newline in the
body. Other requests default to JSON. The native search DSL preserves queries,
aggregations, scoring, sorting, pagination, total hits, and raw response metadata.
Document writes, Bulk writes, index deletion, and alias `remove_index` are
unsupported so they cannot bypass Sink's revision-aware mutation path.

## Scan

`ScanRequest` wraps a native request and an optional batch size (default 100,
maximum 1000). Sink streams `ScanResponse.documents` until completion and owns
opening, advancing, and closing the backend cursor. Cancellation, callback
failure, timeout, and database errors terminate the scan; cleanup uses a separate
five-second context even after the original context was canceled. Cleanup is
best effort if the database is unavailable. Server cursor expiry remains the
fallback after a lost connection or process termination.

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
MongoDB command execution uses the driver's command path. Index operations can
have taken effect even when their acknowledgement is lost. Callers should use
idempotent initialization and inspect native conflicts before retrying.
