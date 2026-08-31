#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: $0 <current-body-file> <output-file> <tagged-image> <image-digest>" >&2
  exit 2
fi

current_body_file=$1
output_file=$2
tagged_image=$3
image_digest=$4
start_marker='<!-- sink-release-artifacts:start -->'
end_marker='<!-- sink-release-artifacts:end -->'
base_file=$(mktemp "${TMPDIR:-/tmp}/sink-release-body.XXXXXX")

cleanup() {
  rm -f -- "${base_file}"
}
trap cleanup EXIT

awk -v start="${start_marker}" -v end="${end_marker}" '
  $0 == start { skipping = 1; next }
  $0 == end { skipping = 0; next }
  !skipping { print }
' "${current_body_file}" > "${base_file}"

awk 'NF { last = NR } { lines[NR] = $0 } END { for (line_number = 1; line_number <= last; line_number++) print lines[line_number] }' \
  "${base_file}" > "${output_file}"

if [[ -s "${output_file}" ]]; then
  printf '\n\n' >> "${output_file}"
fi
printf '%s\n' "${start_marker}" >> "${output_file}"
printf '%s\n\n' '## Installation artifacts' >> "${output_file}"
printf '%s\n\n' 'Docker image:' >> "${output_file}"
printf '`%s`\n\n' "${tagged_image}" >> "${output_file}"
printf '%s\n' '```shell' >> "${output_file}"
printf 'docker pull %s\n' "${tagged_image}" >> "${output_file}"
printf '%s\n\n' '```' >> "${output_file}"
printf 'Immutable image: `%s@%s`\n\n' "${tagged_image%%:*}" "${image_digest}" >> "${output_file}"
printf '%s\n' 'Standalone binaries for Linux and macOS on amd64 and arm64 are attached to this release.' >> "${output_file}"
printf '%s\n' 'Verify an archive with `checksums.txt` before installation.' >> "${output_file}"
printf '%s\n' "${end_marker}" >> "${output_file}"
