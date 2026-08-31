#!/usr/bin/env bash
set -euo pipefail

test_root=$(mktemp -d "${TMPDIR:-/tmp}/sink-release-notes-test.XXXXXX")

cleanup() {
  rm -rf -- "${test_root}"
}
trap cleanup EXIT

current_body="${test_root}/current.md"
first_output="${test_root}/first.md"
second_output="${test_root}/second.md"
printf '%s\n' '# Release notes' '' 'Existing details.' > "${current_body}"

scripts/render-release-notes.sh \
  "${current_body}" \
  "${first_output}" \
  'ghcr.io/liran/sink:1.2.3' \
  'sha256:abc123'
scripts/render-release-notes.sh \
  "${first_output}" \
  "${second_output}" \
  'ghcr.io/liran/sink:1.2.3' \
  'sha256:abc123'

diff -u "${first_output}" "${second_output}"
grep -Fq '`ghcr.io/liran/sink:1.2.3`' "${second_output}"
grep -Fq 'docker pull ghcr.io/liran/sink:1.2.3' "${second_output}"
grep -Fq '`ghcr.io/liran/sink@sha256:abc123`' "${second_output}"
marker_count=$(grep -Fc '<!-- sink-release-artifacts:start -->' "${second_output}")
if [[ "${marker_count}" -ne 1 ]]; then
  echo "managed release-note block count = ${marker_count}, want 1" >&2
  exit 1
fi
