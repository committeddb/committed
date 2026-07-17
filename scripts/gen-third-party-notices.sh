#!/usr/bin/env bash
# Regenerate THIRD_PARTY_NOTICES.md from the third-party Go modules linked into
# the committed binary (the non-test dependencies of the main package). Test-only
# modules — sqlmock, go-mysql-server, testcontainers — are excluded because they
# are not in the shipped binary. Run via `make third-party-notices`.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Deterministic + hermetic output regardless of the caller's environment:
#   LC_ALL=C pins sort order to bytes (not the locale's collation), so the module
#   list and per-module license files order identically on every machine.
#   `go mod download` guarantees every module's source is in the cache, so the
#   per-module license lookup below never comes up empty.
# Without these, a fresh tag checkout can reorder entries or drop license blocks,
# failing `make release`'s diff-guard on a file that is in fact correct.
export LC_ALL=C
go mod download

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
				# Secondary license texts beyond the primary file — e.g. go-mysql's
				# `vitess_license` (BSD-3-Clause code it embeds), which the single
				# primary grab above would miss. A binary that links such a module
				# must reproduce these too. Match license texts only, skip the
				# already-emitted primary, and skip .go sources.
				if [ -n "$dir" ]; then
					find "$dir" -maxdepth 1 -type f 2>/dev/null | sort | while read -r extra; do
						[ "$extra" = "$lic" ] && continue
						base=$(basename "$extra")
						case "$(printf '%s' "$base" | tr 'A-Z' 'a-z')" in
							*.go|*.sh|*.py|*.pl|*.rb|*.js|*.ts|*.bat|*.ps1|*.yml|*.yaml|*.json|*.mod|*.sum) continue ;;
							*license*|*licence*|copying*)
								printf '\n### %s\n\n```\n' "$base"
								cat "$extra"
								printf '\n```\n'
								;;
						esac
					done
				fi
			else
				printf '_License file not found in the module cache; see https://%s_\n' "$path"
			fi
		done
} >"$out"
echo "wrote $out ($(grep -c '^## ' "$out") modules)"
