# syntax=docker/dockerfile:1

# ---- build stage ----
# golang:1.26-alpine tracks the latest 1.26.x, satisfying the go.mod
# toolchain floor. The builder is throwaway, so its size doesn't matter;
# only the static binary it produces is copied into the runtime image.
#
# --platform=$BUILDPLATFORM pins the builder to the machine doing the
# build (not the target), so a multi-arch `buildx` build cross-compiles
# with GOOS/GOARCH at native speed instead of running an emulated
# (QEMU) builder per target arch. Safe here because the binary is pure
# Go with CGO disabled.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

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

# TARGETOS/TARGETARCH are provided automatically by buildx from the
# --platform value; defaulting unset they fall back to the builder's own
# GOOS/GOARCH for a plain `docker build`. CGO_ENABLED=0 yields a fully
# static binary that runs on distroless static (no libc). -trimpath
# strips local filesystem paths; -s -w drops the symbol table and DWARF
# for a smaller artifact.
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags="-s -w \
      -X github.com/committeddb/committed/internal/version.Version=${VERSION} \
      -X github.com/committeddb/committed/internal/version.Commit=${COMMIT} \
      -X github.com/committeddb/committed/internal/version.BuildDate=${BUILD_DATE}" \
    -o /out/committed .

# ---- runtime stage ----
# distroless/static-debian12:nonroot ships only CA certs, tzdata, and a
# nonroot user (uid 65532) — no shell, package manager, or toolchain, so
# the runtime attack surface is the binary itself.
FROM gcr.io/distroless/static-debian12:nonroot

# OCI image labels advertise provenance on the registry and in
# `docker inspect`. version/revision/created mirror the same build args
# the binary is stamped with, so the image metadata and
# `committed --version` agree. ARGs are scoped per build stage, so the
# three build-identity args are re-declared here to be visible to LABEL.
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="committed" \
      org.opencontainers.image.description="A single-binary, Raft-backed CDC pipeline with the log as its own source of truth." \
      org.opencontainers.image.source="https://github.com/committeddb/committed" \
      org.opencontainers.image.url="https://github.com/committeddb/committed" \
      org.opencontainers.image.documentation="https://github.com/committeddb/committed/blob/main/README.md" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}"

COPY --from=builder /out/committed /usr/local/bin/committed

# The Apache-2.0 LICENSE, project NOTICE, and third-party attributions travel
# with the image — Apache/MIT/MPL all require the license texts to accompany a
# distribution. /licenses is the conventional path license scanners look for.
COPY --from=builder /src/LICENSE /src/NOTICE /src/THIRD_PARTY_NOTICES.md /licenses/

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
