# Lua merge script guide

Sink Merge operations use Lua to describe the read-modify-write rule for one
record. A business service sends an `incoming` document and a Lua program. Sink
reads the current document, runs the program, and uses the storage revision to
apply the result atomically.

## Minimal script

Every program must return one function that accepts exactly two arguments:

```lua
return function(current, incoming)
    current = current or json.object()
    current.stock = incoming.stock
    return current
end
```

- `current` is the stored object. It is `nil` when the record does not
  exist and the Merge uses `MISSING_DOCUMENT_MODE_CREATE`.
- `incoming` is the object carried by this Merge and is always present.
- `current` and `incoming` must have the same document encoding.
- The function must return an object. It cannot return `nil`, an array, or
  a scalar.
- The result keeps the incoming encoding: BSON for MongoDB and JSON for search.
- A storage revision conflict makes Sink read the latest `current` document and
  run the same function again, so the script must be deterministic.

Use `sink.v1.time.now()` when a script needs the execution time.

## Complete example

The following script updates non-empty fields, merges tags, retains the latest
20 history entries, and records the Merge time:

```lua
local array = sink.v1.array
local object = sink.v1.object

return function(current, incoming)
    current = current or json.object()

    object.replace_nonempty_string(current, incoming, "title")
    object.replace_nonempty_array(current, incoming, "images")
    current.tags = array.union_strings(current.tags, incoming.tags)

    local history = current.history or json.array()
    array.append_all(history, incoming.history)
    history = array.deduplicate(history, function(item)
        return item.id
    end)
    current.history = array.keep_tail(history, 20)
    current.updated_at = sink.v1.time.now()
    return current
end
```

Create local aliases for frequently used submodules at the top of the program.
This reduces both source size and repeated global table lookups.

## `sink.v1` utilities

`sink.v1` is Sink's versioned, deterministic utility API. Missing or extra
arguments, wrong types, sparse arrays, invalid callback results, and invalid
limits fail the current Merge explicitly instead of silently changing data.

### Array utilities

Every array argument must be a JSON array: a value from `current` or `incoming`,
or an array created with `json.array()`. Do not use an empty `{}` as an array.

#### `sink.v1.array.append_all(target, source)`

- `target` is a required JSON array.
- `source` is a JSON array or `nil`.
- Appends every `source` item to `target` in its original order.
- Mutates and returns the same `target`; it does not mutate `source`.
- Does nothing when `source` is `nil`.

#### `sink.v1.array.deduplicate(items, key_function)`

- `items` is a required JSON array.
- `key_function(item)` is required and must return exactly one string, number,
  or boolean.
- Keeps the first item for each key in the original order and returns a new
  JSON array.
- Does not mutate `items`. A callback error fails the Merge.
- A key cannot be `nil`, a table, or a function. Return a stable string when an
  object needs a composite key.

```lua
local unique = sink.v1.array.deduplicate(incoming.offers, function(item)
    return (item.platform or "") .. ":" .. (item.id or "")
end)
```

#### `sink.v1.array.keep_tail(items, limit)`

- `items` is a required JSON array.
- `limit` is an integer greater than or equal to `0`.
- Returns a new JSON array containing only the last `limit` items.
- A zero limit returns an empty array. The function does not mutate `items`.

#### `sink.v1.array.union_strings(left, right)`

- `left` and `right` are string JSON arrays or `nil`.
- Combines and deduplicates them in left-then-right order and returns a new JSON
  array.
- Does not mutate either input. A non-string item in either array fails the
  Merge.

### Object utilities

The `target` and `source` parameters must be JSON objects. The functions are
convenient when the same rule applies to a list of fields:

```lua
local fields = {"title", "description", "country"}
for index = 1, #fields do
    sink.v1.object.replace_nonempty_string(current, incoming, fields[index])
end
```

#### `sink.v1.object.replace_nonempty_string(target, source, field)`

- `field` must be a string.
- A missing or empty `source[field]` leaves `target` unchanged.
- A non-empty string is assigned to `target[field]`.
- Any other source type fails the Merge instead of writing an invalid value.
- The function has no return value.

#### `sink.v1.object.replace_nonempty_array(target, source, field)`

- `field` must be a string.
- A missing or empty `source[field]` leaves `target` unchanged.
- A non-empty JSON array is assigned to `target[field]`.
- Any other type or a sparse array fails the Merge.
- The function has no return value.

### Time utility

#### `sink.v1.time.now()`

- Accepts no arguments.
- Returns a UTC RFC3339Nano string such as
  `2026-08-30T09:08:07.654321Z`.
- Returns one fixed observation time for the Merge. Multiple calls in the same
  script and reruns caused by storage revision conflicts return the same value.
- A synchronous Merge uses the time at which the server starts processing the
  operation. An asynchronous Merge uses the time at which the Worker starts
  processing the Kafka record, not the client submission time.
- In a BSON merge result the value is encoded as a native BSON datetime. In a
  JSON merge result it remains an RFC3339Nano string.

Sink does not expose `os.time`, the host clock, or mutable time zones because
they could produce different results during a retry.

## Document and Lua types

JSON values and the JSON-compatible view of BSON values use this mapping:

| JSON | Lua |
| --- | --- |
| object | table |
| array | table with a JSON array marker |
| string | string |
| integer | 64-bit Lua integer |
| decimal | number |
| boolean | boolean |
| null | `json.null` |

The common JSON helpers are:

- `json.object()` creates an explicitly typed empty JSON object.
- `json.array()` creates an explicitly typed empty JSON array.
- `json.null` represents JSON null. Assigning Lua `nil` removes a table field.
- `json.is_null(value)` reports whether a value is `json.null`.

An empty Lua table `{}` is encoded as an object. Use `json.array()` whenever an
empty array is required.

## Available and restricted Lua capabilities

Sink provides deterministic base, string, table, math, and UTF-8 functions.
`utf8.upper(value)` performs Unicode-aware uppercasing; `string.upper` is only
suitable for ASCII text.

Scripts cannot use host file or network I/O, operating-system APIs,
`require`/package, dynamic code loading, coroutines, debug APIs, random numbers,
output, metatable mutation, or unbounded string repetition. A script cannot
access Sink configuration, environment variables, credentials, or another
tenant's data.

## Retries, asynchronous delivery, and idempotency

- Concurrent Merges for the same record use revision preconditions. A conflict
  reruns the script with the newest document.
- Kafka provides at-least-once delivery. If a Worker applies a write and exits
  before committing the offset, it may execute the same Merge again.
- Deduplication keys, append rules, and counters must account for replaying the
  same input. `current.count = current.count + 1` is not inherently idempotent.
- Do not depend on random values, external state, or a time value that changes
  on every call.
- `sink.v1.time.now()` is stable only within one Merge operation and its storage
  revision retries. Consuming a Kafka record again is a new execution, so the
  business rule must still provide its required idempotency.

Business idempotence is an application requirement for **all** retries and
replays. Sink performs no business-operation deduplication. An ambiguous write
acknowledgement may be followed by another execution of the same Lua, even in
the same worker. Use business identifiers and atomic document updates where
repeat effects are unsafe. CAS alone does not provide this guarantee.

## Resource limits and errors

Each execution has limits for wall-clock time, instructions, call depth, VM
stack size, source size, and result size. Native `sink.v1` array loops also
enforce the execution deadline and work limit. Script syntax, arguments, types,
callbacks, resources, and result errors fail only the corresponding Write
operation and return a structured failure.

Before rollout, business tests should cover at least:

1. Existing and missing records.
2. Missing fields, empty strings, empty arrays, and `json.null`.
3. Duplicate elements, history limits, and invalid field types.
4. Replaying the same incoming document twice.
5. Final state after concurrent updates cause a storage revision conflict.
6. Real Sink integration tests for both synchronous and Kafka modes.

Use [`sink lua test`](lua-testing.md) for fast local and CI contract tests. It
executes this production Lua engine directly, supports JSON and BSON fixtures,
fixes the observation time, and compares type-aware results without starting a
Sink server or backend.

## Using the Go client

Keep the script with the business application's source and create one reusable
`LuaProgram` in the process:

```go
source := []byte(`
return function(current, incoming)
    current = current or json.object()
    current.stock = incoming.stock
    current.updated_at = sink.v1.time.now()
    return current
end`)

program, err := sink.NewLuaProgram(source)
if err != nil {
    return err
}
options := sink.MergeOptions{
    Incoming:            incomingDocument,
    Program:             program,
    MissingDocumentMode: sink.MissingDocumentCreate,
}
operation, err := sink.NewMerge(address, options)
if err != nil {
    return err
}
```

Create `incomingDocument` with `sink.DocumentEncodingBSON` for a MongoDB
address or `sink.DocumentEncodingJSON` for an Elasticsearch or OpenSearch
address. Sink rejects mixed encodings and backend/encoding mismatches.

Identical source is declared only once within one Write RPC; Merge operations
refer to it by SHA-256. An asynchronous Kafka record still carries the complete
business source so a Worker can process it independently. The common utilities
are built into Sink and therefore are not retransmitted with every request.

## API versioning

Scripts must select `sink.v1` explicitly. The names and semantics of v1
functions remain stable; a future incompatible contract will use a new version
namespace. Do not probe or call undocumented globals, internal fields, or a
higher API version.

Result conversion first checks the expanded table graph, including each use of
a shared table. The limit includes a maximum depth of 256 and at most
`max(64, min(max_result_bytes / 8, 1000000))` visited values/keys, followed by the
encoded byte check. This prevents small alias graphs from creating exponential
Go maps, slices, or serialized output. Current/incoming payloads are also limited
by `max_result_bytes`.

The embedded VM still has no strict allocation-byte quota during execution.
These limits do not make arbitrary Lua safe to run in a shared trusted process.
Use reviewed business scripts, isolate stores/workers into containers with
memory limits, and test the largest permitted documents. See
[reliability boundaries](reliability.md) for deployment acceptance criteria.
