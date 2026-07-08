#!/usr/bin/env bash
# Regenerate THIRD_PARTY_NOTICES.md from the third-party Go modules linked into
# the committed binary (the non-test dependencies of the main package). Test-only
# modules — sqlmock, go-mysql-server, testcontainers — are excluded because they
# are not in the shipped binary. Run via `make third-party-notices`.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

out="THIRD_PARTY_NOTICES.md"
{
	cat <<'HDR'
# Third-Party Notices

The `committed` binary is distributed under the Apache License 2.0 (see
`LICENSE`). It links the third-party Go modules listed below; each module's
license text is reproduced verbatim from its source. Regenerate this file with
`make third-party-notices`.

## Mozilla Public License 2.0 — source availability

These modules are licensed under the MPL-2.0, which requires their source remain
available. committed links them unmodified from their public upstreams, where
the corresponding source is published:

- `github.com/go-sql-driver/mysql` — https://github.com/go-sql-driver/mysql
- `github.com/hashicorp/golang-lru` — https://github.com/hashicorp/golang-lru

---
HDR
	go list -deps -f '{{with .Module}}{{.Path}}	{{.Version}}{{end}}' . |
		sort -u | grep -v '^github.com/committeddb/committed' | grep . |
		while IFS=$'\t' read -r path version; do
			dir=$(go list -m -f '{{.Dir}}' "$path" 2>/dev/null || true)
			lic=""
			if [ -n "$dir" ]; then
				lic=$(ls "$dir"/LICENSE* "$dir"/LICENCE* "$dir"/COPYING* "$dir"/License* 2>/dev/null | head -1 || true)
			fi
			printf '\n## %s %s\n\n' "$path" "$version"
			if [ -n "$lic" ]; then
				printf '```\n'
				cat "$lic"
				printf '\n```\n'
			else
				printf '_License file not found in the module cache; see https://%s_\n' "$path"
			fi
		done
} >"$out"
echo "wrote $out ($(grep -c '^## ' "$out") modules)"
