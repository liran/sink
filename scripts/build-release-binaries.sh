#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <version> <output-directory>" >&2
  exit 2
fi

release_version=$1
output_argument=$2
if [[ ! "${release_version}" =~ ^[0-9A-Za-z._+-]+$ ]]; then
  echo "version contains unsupported characters: ${release_version}" >&2
  exit 2
fi

mkdir -p "${output_argument}"
output_directory=$(cd "${output_argument}" && pwd)
staging_root=$(mktemp -d "${TMPDIR:-/tmp}/sink-release.XXXXXX")

cleanup() {
  rm -rf -- "${staging_root}"
}
trap cleanup EXIT

targets=(
  "linux amd64"
  "linux arm64"
  "darwin amd64"
  "darwin arm64"
)
archive_names=()

for target in "${targets[@]}"; do
  read -r target_os target_arch <<<"${target}"
  asset_name="sink_${release_version}_${target_os}_${target_arch}"
  asset_directory="${staging_root}/${asset_name}"
  mkdir -p "${asset_directory}"

  binary_name=sink

  CGO_ENABLED=0 GOOS="${target_os}" GOARCH="${target_arch}" \
    go build -trimpath -ldflags="-s -w -X main.version=${release_version}" \
    -o "${asset_directory}/${binary_name}" ./cmd/sink
  cp LICENSE README.md "${asset_directory}/"

  archive_name="${asset_name}.tar.gz"
  if [[ -e "${output_directory}/${archive_name}" ]]; then
    echo "release asset already exists: ${output_directory}/${archive_name}" >&2
    exit 1
  fi
  tar -C "${asset_directory}" -czf "${output_directory}/${archive_name}" "${binary_name}" LICENSE README.md
  archive_names+=("${archive_name}")
done

if [[ -e "${output_directory}/checksums.txt" ]]; then
  echo "release checksum file already exists: ${output_directory}/checksums.txt" >&2
  exit 1
fi

(
  cd "${output_directory}"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${archive_names[@]}" > checksums.txt
  else
    shasum -a 256 "${archive_names[@]}" > checksums.txt
  fi
)
