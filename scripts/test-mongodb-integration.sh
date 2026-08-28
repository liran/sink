#!/usr/bin/env bash
set -euo pipefail

container_name="sink-mongodb-test-$$"
test_count="${SINK_INTEGRATION_COUNT:-1}"

if [[ ! "${test_count}" =~ ^[1-9][0-9]*$ ]]; then
	echo "SINK_INTEGRATION_COUNT must be a positive integer" >&2
	exit 1
fi

cleanup() {
	docker stop "${container_name}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --rm --detach \
	--name "${container_name}" \
	--publish 127.0.0.1::27017 \
	--env GLIBC_TUNABLES=glibc.pthread.rseq=1 \
	mongo:8.0 \
	--replSet rs0 \
	--bind_ip_all >/dev/null

for _ in $(seq 1 60); do
	if docker exec "${container_name}" mongosh --quiet --eval 'db.adminCommand({ping: 1}).ok' 2>/dev/null | grep -q '^1$'; then
		break
	fi
	sleep 1
done

docker exec "${container_name}" mongosh --quiet --eval \
	'rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27017"}]})' >/dev/null

for _ in $(seq 1 60); do
	if docker exec "${container_name}" mongosh --quiet --eval 'db.hello().isWritablePrimary' 2>/dev/null | grep -q '^true$'; then
		break
	fi
	sleep 1
done

published_port="$(docker port "${container_name}" 27017/tcp | sed -n 's/.*://p' | head -n 1)"
if [[ -z "${published_port}" ]]; then
	echo "MongoDB test port was not published" >&2
	exit 1
fi

SINK_MONGODB_TEST_URI="mongodb://127.0.0.1:${published_port}/?directConnection=true" \
	go test -tags=integration ./internal/storage/mongodb -count="${test_count}" -timeout=3m
