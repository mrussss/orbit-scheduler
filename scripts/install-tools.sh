#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tool_dir="$root_dir/bin"
mkdir -p "$tool_dir"

GOBIN="$tool_dir" GOTOOLCHAIN=local go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.1
GOBIN="$tool_dir" GOTOOLCHAIN=local go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

if [[ ! -x "$tool_dir/protoc" ]]; then
  archive=$(mktemp /tmp/orbit-protoc-XXXXXX.zip)
  extract_dir=$(mktemp -d /tmp/orbit-protoc-XXXXXX)
  trap 'rm -f "$archive"; rm -rf "$extract_dir"' EXIT
  curl -fL --retry 3 https://github.com/protocolbuffers/protobuf/releases/download/v28.3/protoc-28.3-linux-x86_64.zip -o "$archive"
  if command -v unzip >/dev/null 2>&1; then
    unzip -q "$archive" bin/protoc -d "$extract_dir"
  else
    busybox unzip -q "$archive" bin/protoc -d "$extract_dir"
  fi
  install -m 0755 "$extract_dir/bin/protoc" "$tool_dir/protoc"
fi

"$tool_dir/protoc" --version

