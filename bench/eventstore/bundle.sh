#!/usr/bin/env bash
# Builds a bench bundle for run.sh: the committed builds to compare, the load
# generator, the recipe, and run.sh.
#
#   bench/eventstore/bundle.sh [-o dir] [--local] [label=gitref ...]
#
# With no label=gitref, the bundle holds the working tree as "head". Each
# label=gitref builds that commit (git archive, so the working tree is left
# alone). By default the nodes are linux/arm64 and the load generator
# linux/amd64; --local builds both for this machine.
set -euo pipefail

root=$(git rev-parse --show-toplevel)
out=$root/bench-bundle
local_build=
refs=()
while [ $# -gt 0 ]; do
  case $1 in
  -o) out=$2; shift 2 ;;
  --local) local_build=1; shift ;;
  *=*) refs+=("$1"); shift ;;
  *) echo "bundle.sh: unknown argument $1" >&2; exit 2 ;;
  esac
done

node_env=(GOOS=linux GOARCH=arm64)
load_env=(GOOS=linux GOARCH=amd64)
if [ -n "$local_build" ]; then
  node_env=(GOOS="$(go env GOOS)" GOARCH="$(go env GOARCH)")
  load_env=("${node_env[@]}")
fi
export CGO_ENABLED=0

rm -rf "$out"
mkdir -p "$out/bin" "$out/recipe"
out=$(cd "$out" && pwd)

build_committed() { # build_committed label dir commit
  echo "building committed $1 ($3) for ${node_env[*]}"
  mkdir -p "$out/bin/$1"
  local v=github.com/committeddb/committed/internal/version
  (cd "$2" && env "${node_env[@]}" go build -trimpath \
    -ldflags "-s -w -X $v.Version=bench-$1 -X $v.Commit=$3" -o "$out/bin/$1/committed" .)
}

if [ ${#refs[@]} -eq 0 ]; then
  build_committed head "$root" "$(git -C "$root" describe --always --dirty)"
else
  tmp=$(mktemp -d "$root/.bench-src.XXXXXX")
  trap 'rm -rf "$tmp"' EXIT
  for r in "${refs[@]}"; do
    label=${r%%=*} ref=${r#*=}
    mkdir -p "$tmp/$label"
    git -C "$root" archive "$ref" | tar -x -C "$tmp/$label"
    build_committed "$label" "$tmp/$label" "$(git -C "$root" rev-parse --short "$ref")"
  done
fi

echo "building eventstore-load for ${load_env[*]}"
(cd "$root" && env "${load_env[@]}" go build -trimpath -o "$out/bin/eventstore-load" ./bench/eventstore)
cp "$root/docs/recipes/event-store-read-models.md" "$out/recipe/"
cp "$root/bench/eventstore/run.sh" "$out/run.sh"
chmod +x "$out/run.sh"
echo "bundle: $out"
