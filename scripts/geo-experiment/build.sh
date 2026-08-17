#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
target="x86_64-unknown-linux-gnu"
binary_path="$root_dir/target/$target/release/orset_bench_replica"

require_command() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

require_command cargo
require_command rustc
require_command rustup
require_command python3
require_command ssh
require_command scp
require_command file

python3 -c 'import sys; assert sys.version_info >= (3, 10), "Python 3.10 or newer is required"'
rustup target list --installed | grep -qx "$target" || {
  echo "Missing Rust target: $target" >&2
  echo "Install it with: rustup target add $target" >&2
  exit 1
}

cd "$root_dir"
if [[ "$(uname -s)" == "Linux" && "$(uname -m)" == "x86_64" ]]; then
  echo "Building native Linux target: $target"
  cargo build --release --target "$target" --bin orset_bench_replica
else
  require_command zig
  require_command cargo-zigbuild
  echo "Cross-compiling Linux target with zig: $target"
  cargo zigbuild --release --target "$target" --bin orset_bench_replica
fi

file_description=$(file -b "$binary_path")
case "$file_description" in
  *"ELF 64-bit"*"x86-64"*) ;;
  *) echo "Expected a 64-bit x86_64 Linux ELF binary, got: $file_description" >&2; exit 1 ;;
esac

sha256=$(python3 -c 'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "$binary_path")
commit=$(git rev-parse HEAD 2>/dev/null || echo unknown)

echo "Built: $binary_path"
echo "Format: $file_description"
echo "SHA-256: $sha256"
echo "Git commit: $commit"
echo "Use this SHA-256 as expected_binary_sha256 (or let preflight.sh pass it automatically)."
