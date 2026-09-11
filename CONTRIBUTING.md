# Contributing to Sink

Focused bug fixes, documentation improvements, tests, and feature proposals are
welcome. For changes to the public protocol or storage semantics, open an
[issue](https://github.com/liran/sink/issues/new/choose) first so the server,
[Go client](https://github.com/liran/sink-go), and
[production suite](https://github.com/liran/sink-production-suite) can evolve
together.

## Report a problem

Use the issue forms and include the Sink version, storage engine/version,
completion mode, expected result, and a minimal reproduction. Include relevant
logs and configuration with credentials and private data removed. For capacity
reports, describe the workload, replica count, and resource limits.

For vulnerabilities, follow [SECURITY.md](SECURITY.md).

## Work locally

Install the Go version declared in [go.mod](go.mod), Git, and Make. Docker with
Compose is needed only for the quickstart and external integration suites.

```shell
git clone https://github.com/liran/sink.git
cd sink
make build
make test
make lint
```

`make test` uses local test doubles, including an in-process Kafka broker.
It does not require a database or Docker; the first run may download Go
dependencies. `make lint` checks without rewriting files. Use `make fmt` to
apply formatting.

Run `make test-integration` for adapter changes and `make quickstart` for
end-to-end behavior. See the [development guide](docs/development.md) for
workflow/link checks, protobuf generation, race tests, and qualification.

## Prepare a pull request

- Keep the change focused and explain the observed problem and resulting behavior.
- Add a regression test when fixing behavior; use disposable backends for
  integration tests and keep external services out of the default test suite.
- Update affected documentation and examples when configuration or APIs change.
- Keep generated protobuf files consistent with the protocol. Coordinate
  public contract changes with the Go client and public suite.
- Preserve per-record atomicity, explicit document encoding, bounded resource
  use, and application responsibility for replay idempotence.
- Describe the checks you ran and any validation still outstanding.

The required **Sink reliability gate** combines repository checks, race tests,
client compatibility, storage integration, public conformance, the quickstart,
and packaging. A passing short CI run does not replace sustained or deployment
qualification.

Contributions are distributed under the repository's [MIT License](LICENSE).
