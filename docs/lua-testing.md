# Testing Lua merge programs

The `sink lua test` command runs merge programs locally with the same parser,
compiler, restricted Lua environment, `sink.v1` functions, JSON/BSON bridge,
fresh virtual machine, and default resource limits used by the Sink server. It
does not start a server or connect to MongoDB, Elasticsearch, OpenSearch, or
Kafka, and it does not require a Sink configuration file. Pass `--config` when
production overrides `service.lua` limits; the command validates the file and
uses those limits without opening its configured backends.

Use the same Sink binary version in development, CI, and production. This keeps
script behavior aligned when the Lua runtime or `sink.v1` gains new features.

## Run the included example

From the repository root:

```shell
go run ./cmd/sink lua test \
  --script examples/lua-test/product.lua \
  --cases examples/lua-test/cases
```

Successful output lists every case and ends with the total:

```text
PASS create JSON product
PASS update BSON product
PASS 2 cases
```

Any syntax, execution, resource, document, or expected-result error produces a
nonzero exit status. When several case files fail, Sink runs all discovered
cases and reports every failure in one invocation.

## Test one document pair

Use direct flags while developing a case:

```shell
sink lua test \
  --script merge/product.lua \
  --encoding json \
  --current testdata/current.json \
  --incoming testdata/incoming.json \
  --expected testdata/expected.json \
  --observed-at 2026-08-31T10:20:30.456Z
```

`--current` is optional and represents a missing stored document when omitted.
`--expected` is also optional. Without it, the command prints the merged
document instead of comparing a result. JSON output is formatted JSON. BSON
output is canonical Extended JSON so BSON-specific types remain visible.

### Direct flags

| Flag | Required | Value and behavior |
| --- | --- | --- |
| `--script` | Yes | Lua source file. The chunk must return one merge function. |
| `--config` | No | Sink YAML configuration whose `service.lua` limits are applied. No backend is opened. |
| `--cases` | No | One `.yaml`/`.yml` case or a directory of cases. Cannot be combined with the document flags below. |
| `--encoding` | Without `--cases` | `json` or `bson`. |
| `--current` | No | Existing JSON or Extended JSON object. Omit to pass `nil` as `current`. |
| `--incoming` | Without `--cases` | Incoming JSON or Extended JSON object. |
| `--expected` | No | Expected JSON or Extended JSON object. Omit to print the actual result. |
| `--observed-at` | Without `--cases` | Fixed RFC3339/RFC3339Nano time returned by `sink.v1.time.now()`. |

## Run a case suite

Point `--cases` at one YAML file or a directory. Directory discovery is
non-recursive and runs `.yaml` and `.yml` files in filename order. A useful
layout is:

```text
merge/
├── product.lua
└── testdata/
    ├── 01-create.yaml
    ├── 02-update.yaml
    ├── create-incoming.json
    ├── create-expected.json
    ├── update-current.extjson
    ├── update-incoming.extjson
    └── update-expected.extjson
```

One case file describes one execution:

```yaml
name: update BSON product
encoding: bson
current: update-current.extjson
incoming: update-incoming.extjson
expected: update-expected.extjson
observed_at: 2026-08-31T10:20:30.456Z
```

Document paths are resolved relative to the YAML case file. Unknown fields and
multiple YAML documents are rejected.

| Field | Required | Allowed value and behavior |
| --- | --- | --- |
| `name` | Yes | Non-empty name printed in test results. |
| `encoding` | Yes | `json` or `bson`; current, incoming, expected, and result all use it. |
| `current` | No | Existing document path. Omission passes `nil` to the script. |
| `incoming` | Yes | Incoming document path. |
| `expected` | Yes | Expected result path. |
| `observed_at` | Yes | RFC3339/RFC3339Nano time used for every `sink.v1.time.now()` call and retry-equivalent execution. |

Comparison ignores object-member order and insignificant JSON whitespace. It
does not ignore values or types. BSON documents are compared through canonical
Extended JSON, so a BSON datetime is different from a string containing the
same timestamp.

## JSON and BSON fixture formats

For `encoding: json`, every document file must contain one JSON object.

For `encoding: bson`, document files use MongoDB Extended JSON. Sink converts
them to real BSON before invoking the production bridge. Relaxed input is
accepted:

```json
{
  "_id": {"$oid": "64f000000000000000000001"},
  "created_at": {"$date": "2026-08-31T10:20:30.456Z"},
  "payload": {"$binary": {"base64": "AQID", "subType": "00"}}
}
```

Do not represent BSON datetimes as ordinary strings in expected fixtures. The
type-aware comparison is intended to catch accidental BSON-to-JSON conversion.

## CI usage

With a downloaded standalone binary:

```shell
./sink lua test --script merge/product.lua --cases merge/testdata
```

With the same tagged image used in production:

```shell
docker run --rm \
  --mount type=bind,source="$(pwd)",target=/workspace,readonly \
  ghcr.io/liran/sink:0.8.0 \
  lua test \
  --script /workspace/merge/product.lua \
  --cases /workspace/merge/testdata
```

Pin an exact image tag or binary release in CI. Do not use `latest` for merge
contract tests.

## Recommended coverage

At minimum, cover:

1. Existing and missing current documents.
2. Missing fields, empty strings, empty arrays, and `json.null`.
3. Invalid field types and malformed arrays.
4. Large integers and BSON-specific values used by the business model.
5. Replaying the same incoming document against the previous result.
6. Long sequences with deterministic random seeds or Go fuzz tests.

The command tests one deterministic merge execution. Keep a smaller real Sink
integration suite for storage revision conflicts, concurrent retry behavior,
Kafka at-least-once delivery, backend visibility, MongoDB `_id`, and search
index mappings.
