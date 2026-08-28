#!/usr/bin/env bash
set -euo pipefail

backend="${1:-${SINK_SEARCH_BACKEND:-}}"
test_count="${SINK_INTEGRATION_COUNT:-1}"

if [[ "${backend}" != "elasticsearch" && "${backend}" != "opensearch" ]]; then
	echo "usage: $0 <elasticsearch|opensearch>" >&2
	exit 1
fi
if [[ ! "${test_count}" =~ ^[1-9][0-9]*$ ]]; then
	echo "SINK_INTEGRATION_COUNT must be a positive integer" >&2
	exit 1
fi

container_name="sink-${backend}-test-$$"

cleanup() {
	docker stop "${container_name}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ "${backend}" == "elasticsearch" ]]; then
	docker run --rm --detach \
		--name "${container_name}" \
		--publish 127.0.0.1::9200 \
		--env discovery.type=single-node \
		--env xpack.security.enabled=false \
		--env 'ES_JAVA_OPTS=-Xms512m -Xmx512m' \
		docker.elastic.co/elasticsearch/elasticsearch:8.19.20 >/dev/null
else
	docker run --rm --detach \
		--name "${container_name}" \
		--publish 127.0.0.1::9200 \
		--env discovery.type=single-node \
		--env DISABLE_INSTALL_DEMO_CONFIG=true \
		--env DISABLE_SECURITY_PLUGIN=true \
		--env 'OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m' \
		opensearchproject/opensearch:3.8.0 >/dev/null
fi

published_port="$(docker port "${container_name}" 9200/tcp | sed -n 's/.*://p' | head -n 1)"
if [[ -z "${published_port}" ]]; then
	echo "${backend} test port was not published" >&2
	exit 1
fi
endpoint="http://127.0.0.1:${published_port}"

ready="false"
for _ in $(seq 1 120); do
	if curl --fail --silent --max-time 2 "${endpoint}/" >/dev/null; then
		ready="true"
		break
	fi
	sleep 1
done
if [[ "${ready}" != "true" ]]; then
	docker logs "${container_name}" >&2
	echo "${backend} did not become ready" >&2
	exit 1
fi

SINK_SEARCH_TEST_DRIVER="${backend}" \
	SINK_SEARCH_TEST_ENDPOINT="${endpoint}" \
	go test -tags=integration ./internal/storage/search -count="${test_count}" -timeout=5m
