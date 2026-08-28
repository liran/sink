# Docker Compose quickstart

The quickstart runs a complete local Sink environment and verifies it through
the public gRPC API. It starts MongoDB as a single-node ReplicaSet, Apache Kafka
in KRaft mode, Sink in combined server/worker mode, and a disposable Go client.
The Sink container loads
[`sink.yaml`](sink.yaml) through `--config /etc/sink/config.yaml`; edit that file
to try other server settings.

From the repository root, run:

```shell
./examples/quickstart/run.sh
```

The command builds the local Sink image, starts the dependencies, creates the
Kafka mutation topic, and runs these checks:

1. Wait for the Sink gRPC health service.
2. Batch-write and batch-read two BSON documents synchronously.
3. Submit one asynchronous write and wait for the Kafka worker to apply it.
4. Hard-delete all three records and confirm they are absent.

The stack remains running after the checks pass so it can be inspected or used
for additional requests. The exposed endpoints are:

- Sink gRPC: `127.0.0.1:8080`
- Prometheus metrics: `http://127.0.0.1:9090/metrics`
- MongoDB: `mongodb://127.0.0.1:27017/?directConnection=true`
- Kafka: `127.0.0.1:9092`

Re-run only the scenario against the active stack with:

```shell
docker compose \
  --file examples/quickstart/compose.yaml \
  --profile test \
  run --build --rm --no-deps example
```

Stop the services while preserving their local volumes:

```shell
docker compose --file examples/quickstart/compose.yaml down
```

Add `--volumes` to the down command when a completely clean MongoDB and Kafka
state is required.
