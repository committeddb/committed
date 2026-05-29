# syntax=docker/dockerfile:1

# ---- build stage ----
# golang:1.26-alpine tracks the latest 1.26.x, satisfying the go.mod
# toolchain floor. The builder is throwaway, so its size doesn't matter;
# only the static binary it produces is copied into the runtime image.
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Copy module manifests first so `go mod download` is cached on its own
# layer and only re-runs when go.mod/go.sum change, not on every source
# edit.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build identity is injected at link time so `committed --version` and
# GET /version report the real version/commit/date. The defaults keep an
# un-stamped `docker build` honest ("dev"/"unknown") rather than letting
# it silently claim a version. Pass real values with
# --build-arg VERSION=$(git describe ...) (see the docker/build Make target).
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

# CGO_ENABLED=0 yields a fully static binary that runs on distroless
# static (no libc). -trimpath strips local filesystem paths; -s -w drops
# the symbol table and DWARF for a smaller artifact.
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w \
      -X github.com/philborlin/committed/internal/version.Version=${VERSION} \
      -X github.com/philborlin/committed/internal/version.Commit=${COMMIT} \
      -X github.com/philborlin/committed/internal/version.BuildDate=${BUILD_DATE}" \
    -o /out/committed .

# ---- runtime stage ----
# distroless/static-debian12:nonroot ships only CA certs, tzdata, and a
# nonroot user (uid 65532) — no shell, package manager, or toolchain, so
# the runtime attack surface is the binary itself.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/committed /usr/local/bin/committed

# 8080: HTTP API (COMMITTED_API_ADDR default). 9022: raft peer transport
# (COMMITTED_PEER_URL default). EXPOSE is documentation; publish with -p.
EXPOSE 8080 9022

# The base image already defaults to the nonroot user; stated explicitly
# so the runtime identity survives a base-image change.
USER nonroot:nonroot

# Run from the nonroot user's home, which it owns and can write to. This
# makes the default COMMITTED_DATA_DIR ("./data") resolve to a writable
# /home/nonroot/data — a non-root process can't create ./data under / .
# Mount a volume here (or set COMMITTED_DATA_DIR) to persist WAL/state.
WORKDIR /home/nonroot

# No shell or curl in distroless, so the binary self-probes /ready.
# start-period covers first-boot raft election + WAL replay before a
# failing probe counts against the container's retry budget.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
  CMD ["/usr/local/bin/committed", "healthcheck"]

# `committed node` reads all of its configuration from COMMITTED_* env
# vars, so the default CMD needs no flags. Override CMD to run other
# subcommands (e.g. `--version`, `healthcheck`).
ENTRYPOINT ["/usr/local/bin/committed"]
CMD ["node"]
