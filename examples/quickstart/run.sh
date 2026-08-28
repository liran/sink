#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compose=(docker compose --project-directory "${script_dir}" --file "${script_dir}/compose.yaml")

"${compose[@]}" up --build --detach --wait
"${compose[@]}" --profile test run --build --rm --no-deps example

echo
echo "Sink quickstart is running."
echo "gRPC endpoint: 127.0.0.1:8080"
echo "MongoDB: mongodb://127.0.0.1:27017/?directConnection=true"
echo "Kafka: 127.0.0.1:9092"
echo "Stop it with: docker compose -f ${script_dir}/compose.yaml down"
