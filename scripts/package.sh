#!/usr/bin/env bash
# Build portable, self-contained release archives without installing dependencies.
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
version=${1:?Usage: scripts/package.sh VERSION [OUTPUT_DIRECTORY]}
if [[ ! "$version" =~ ^(v[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?|snapshot-[0-9a-f]{7,40})$ ]]; then
  echo 'Invalid version: expected vMAJOR.MINOR.PATCH or snapshot-COMMIT.' >&2
  exit 2
fi
out=${2:-dist}
if [[ -L "$out" || ( -e "$out" && ! -d "$out" ) ]]; then
  echo 'Output must be a new or empty directory, not a symlink.' >&2
  exit 2
fi
mkdir -p "$out"
if [[ -n "$(ls -A "$out")" ]]; then
  echo 'Output directory is not empty; existing artifacts will not be overwritten.' >&2
  exit 2
fi
out=$(cd "$out" && pwd)
revision=$(git rev-parse HEAD)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
for os in linux darwin; do
  for arch in amd64 arm64; do
    stage="$work/$os-$arch"
    mkdir -p "$stage/transport"
    GOOS="$os" GOARCH="$arch" CGO_ENABLED=0 \
      go build -trimpath -buildvcs=true \
        -ldflags="-s -w -X github.com/Version2beta/urifan/internal/cli.Version=$version -X github.com/Version2beta/urifan/internal/cli.Revision=$revision" \
        -o "$stage/urifan" ./cmd/urifan
    chmod 0755 "$stage/urifan"
    cp README.md LICENSE "$stage/"
    cp transport/LICENSE transport/ORIGIN.md transport/UPSTREAM.sha256 "$stage/transport/"
    COPYFILE_DISABLE=1 tar -C "$stage" -czf "$out/urifan_${version}_${os}_${arch}.tar.gz" .
  done
done
(
  cd "$out"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum ./*.tar.gz > SHA256SUMS
  else
    shasum -a 256 ./*.tar.gz > SHA256SUMS
  fi
)
printf 'Built %s archives in %s\n' "$version" "$out"
