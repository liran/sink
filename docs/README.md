# Sink documentation

[Back to the project](../README.md)

## Get started

| Guide | What you will learn |
| --- | --- |
| [Docker Compose quickstart](../examples/quickstart/README.md) | Start Sink, MongoDB, and Kafka and verify the public API |
| [Go client](https://github.com/liran/sink-go#quick-start) | Connect an application and read or write typed documents |
| [Architecture and behavior](architecture.md) | Understand addresses, encoding, batching, ordering, and completion modes |
| [Document write flow](document-write-flow.md) | Follow one document from an RPC through storage or a Kafka worker |

## Build an application

| Guide | What you will learn |
| --- | --- |
| [Native access](native-access.md) | Execute backend commands, query, count, scan, and return atomic update results |
| [Lua merge developer guide](lua-merge-guide.md) | Write merge programs using the `sink.v1` tools |
| [Testing Lua merge programs](lua-testing.md) | Test JSON and BSON fixtures with Sink's production Lua runtime |
| [Merge folding](merge-folding.md) | Understand how compatible operations share one storage write |
| [Protocol definition](../proto/sink/sink.proto) | Inspect the authoritative gRPC messages and services |

## Deploy and operate

| Guide | What you will learn |
| --- | --- |
| [Configuration reference](configuration.md) | Configure named stores, resource limits, Kafka, and process modes |
| [Reliability and recovery](reliability.md) | Plan idempotence, monitor dependencies, handle failures, and replay dead letters |
| [Production sizing](production-sizing.md) | Use measured workload limits to size a replicated deployment |
| [Synchronous performance](synchronous-performance.md) | Reproduce benchmarks and interpret their scope |
| [Kubernetes capacity harness](../benchmarks/kubernetes/README.md) | Measure disposable test deployments with explicit safeguards |

## Contribute and qualify changes

| Guide | What you will learn |
| --- | --- |
| [Contributing](../CONTRIBUTING.md) | Report an issue and prepare a focused pull request |
| [Development](development.md) | Build, lint, generate protobuf code, run tests, and package releases |
| [Lua benchmarks](../benchmarks/lua/README.md) | Compare Lua runtime workloads |
| [Public production suite](https://github.com/liran/sink-production-suite) | Run public API conformance and sustained fault qualification |
| [Security policy](../SECURITY.md) | Report a vulnerability privately |
