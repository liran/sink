# Lua product-merge benchmark

This module measures a Lua port and a self-contained Go snapshot of the current
`product-search-engine/types.Product.Merge` implementation. It is isolated in
a nested Go module so the experiment does not add either Lua runtime to the
Sink production binary or depend on a sibling checkout.

## Scope

- The benchmark includes JSON decoding of the current and incoming documents,
  conversion to Lua tables, one merge call, conversion back to Go, and JSON
  encoding.
- The native control decodes the same inputs into the benchmark's Product
  snapshot, calls its Go merge implementation, and encodes the result.
- The OpenSearch read/write, gRPC, Kafka, and network paths are intentionally
  excluded. This isolates the CPU and allocation cost of the merge engine.
- The script is parsed once and the VM is reused unless the benchmark name
  contains `FreshVM`.

The fixtures contain product scalar fields plus prices, offers, sales, stock,
comments, images, and classifier data. Their combined current and incoming
document sizes are:

| Fixture | Current | Incoming | Total input |
| --- | ---: | ---: | ---: |
| small | 2,933 B | 2,073 B | 5,006 B |
| medium | 41,721 B | 23,041 B | 64,762 B |
| large | 191,260 B | 98,123 B | 289,383 B |

## Results

Environment: Apple M2, 8 logical CPUs, 24 GiB memory, macOS arm64, Go 1.27.0.
Numbers below are the median of five runs with a 500 ms benchmark time.

| Fixture | JSON only | Native Go | Lua 5.4 with limits | Lua / Go | Lua heap/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| small | 0.084 ms | 0.042 ms | 0.230 ms | 5.48x | 161 KB |
| medium | 0.955 ms | 0.584 ms | 3.326 ms | 5.70x | 2.34 MB |
| large | 4.203 ms | 2.283 ms | 14.839 ms | 6.50x | 10.12 MB |

The bounded Lua configuration uses a one-second context deadline plus call
depth, stack slot, and instruction-checkpoint limits. On the medium fixture,
the bounded median was 3.330 ms versus 3.304 ms without these limits, a 0.8%
difference in this run.

Creating, parsing, and compiling a new Lua 5.4 VM took 0.216 ms median. A full
merge with a fresh VM on every call measured 0.467 ms, 3.579 ms, and 15.057 ms
for the small, medium, and large fixtures. Compared with VM reuse, the fresh-VM
penalty was approximately 103%, 7.6%, and 1.5%, respectively.

The medium fixture with one independent VM per benchmark worker measured:

| `GOMAXPROCS` | Median | Approximate process throughput |
| ---: | ---: | ---: |
| 1 | 4.335 ms/op | 231 merges/s |
| 8 | 1.112 ms/op | 900 merges/s |

Parallel scaling is limited by allocation and garbage collection: the 8-CPU
case allocates about 2.38 MB and 35,000 objects per merge.

A targeted fixture with a 5,787 B current document and a 5,937 B incoming
document measured 0.594 ms with a reused VM and 0.842 ms with a completely
fresh, reparsed, and recompiled VM. A production path that caches the compiled
program but creates a clean VM should fall between these two measurements on
comparable hardware.

## Runtime decision

`github.com/iceisfun/golua` v1.1.1 is the viable runtime from this experiment.
It is pure Go, implements Lua 5.4 integers, works with Go 1.27, and exposes
context and execution limits. The benchmark's Lua 5.4 path preserves the
integer `9007199254740993`.

`github.com/yuin/gopher-lua` v1.1.2 is not safe for a schema-neutral Sink. Its
Lua 5.1 number bridge uses `float64`; the same integer is changed to
`9007199254740992` in the included regression test. It is retained only as a
comparison benchmark.

Before using the Lua 5.4 adapter in a multi-tenant service, production code
still needs a restricted standard library, source/result size limits, a
content-addressed compilation cache, deterministic host inputs, and an
explicit JSON table contract for distinguishing empty arrays from empty
objects. An in-process VM also has no hard per-script heap quota, so strict
multi-tenant memory isolation requires a process or container boundary.

## Reproduce

```sh
go test ./... -count=1
go test -race ./... -count=1
go test -run '^$' -bench '^BenchmarkProductMerge$' -benchmem -benchtime=500ms -count=5
go test -run '^$' -bench '^BenchmarkProductMergeParallelMedium$' -benchmem -benchtime=1s -count=5 -cpu=1,8
go test -run '^$' -bench '^BenchmarkLuaEngineColdStart$' -benchmem -benchtime=1s -count=5
go test -run '^$' -bench '^BenchmarkProductMergeFreshVM$' -benchmem -benchtime=500ms -count=5
go test -run '^$' -bench '^BenchmarkFiveKilobyteDocuments$' -benchmem -benchtime=1s -count=5
```
