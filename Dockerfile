# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Build stage: compiles the Mix project and produces a self-contained OTP
# release (`mix release`, bundling its own ERTS runtime) so the runtime image
# does not need an Elixir/Mix toolchain.
# ---------------------------------------------------------------------------
FROM elixir:1.20.4-otp-29-slim AS build

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /openrelik

ENV MIX_ENV=prod
# Install Hex and Rebar
RUN mix local.hex --force && mix local.rebar --force

# Copy dependency definitions and fetch/compile production dependencies for layer caching
COPY mix.exs mix.lock ./
RUN mix deps.get --only prod && mix deps.compile

# Copy application source and release configuration, then compile
COPY .formatter.exs ./
COPY lib ./lib
COPY rel ./rel
RUN mix compile

# Produce the self-contained OTP release in _build/prod/rel/openrelik_worker_strings
RUN mix release

# ---------------------------------------------------------------------------
# Runtime stage: OpenRelik worker on a self-contained OTP release.
#
# The release bundles its own ERTS runtime, so no Elixir/Erlang toolchain is
# required here. This stage uses the *same Debian release (trixie) as the
# build stage* so the bundled BEAM links against a compatible glibc; it only
# adds the minimal runtime bits:
#   - binutils        : GNU `strings` (with --encoding support) used by the worker
#   - ca-certificates : TLS trust store for Redis connections
#   - libstdc++6, libtinfo6 : shared libraries linked by the bundled beam.smp
#   - libssl3         : OpenSSL runtime required by OTP's `crypto` application
# ---------------------------------------------------------------------------
FROM debian:trixie-slim AS runtime

ENV DEBIAN_FRONTEND=noninteractive

# UTF-8 locale (built into glibc, no extra package needed); the build base
# image sets it too, and the BEAM prints a latin1-encoding warning without it.
ENV LANG=C.UTF-8

RUN apt-get update && apt-get install -y --no-install-recommends \
    binutils \
    ca-certificates \
    libstdc++6 \
    libtinfo6 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the self-contained release into /opt/openrelik, owned directly by the
# non-root `openrelik` user (a `RUN chown -R` after COPY would duplicate the
# whole release into an extra layer).
COPY --from=build --chown=10001:10001 \
    /openrelik/_build/prod/rel/openrelik_worker_strings /opt/openrelik

# Non-root `openrelik` user and writable OpenRelik artifact volume directory.
# When mounting a host volume over /usr/share/openrelik/data, ensure it is
# writable by this user (uid/gid 10001) or run the container with
# `user: "<uid>:<gid>"` matching the host directory ownership.
RUN groupadd --gid 10001 openrelik \
    && useradd --uid 10001 --gid 10001 --create-home --home-dir /home/openrelik openrelik \
    && mkdir -p /usr/share/openrelik/data \
    && chown openrelik:openrelik /usr/share/openrelik/data

# Default Redis URL (override at runtime with `-e REDIS_URL=...`)
ENV REDIS_URL="redis://localhost:6379/0"

# Release mode starts the Celery worker supervised by the OTP application
# (concurrency and queues configurable via WORKER_CONCURRENCY / WORKER_QUEUES;
# extraction parallelism via EXTRACTION_CONCURRENCY; graceful shutdown via
# WORKER_SHUTDOWN_TIMEOUT_MS).
ENV START_WORKER="true"
ENV WORKER_CONCURRENCY="1"
ENV WORKER_SHUTDOWN_TIMEOUT_MS="30000"

USER openrelik
WORKDIR /opt/openrelik

# The release startup script execs the BEAM process, so the Erlang runtime is
# PID 1 and receives `docker stop`'s SIGTERM directly (graceful runtime-system
# termination on OTP >= 26 stops all applications, letting the supervised
# Celery.Worker emit `worker-offline` and clean up within
# WORKER_SHUTDOWN_TIMEOUT_MS).
ENTRYPOINT ["/opt/openrelik/bin/openrelik_worker_strings"]
CMD ["start"]

# Basic liveness probe: `rpc "IO.puts(:pong)"` connects to the running node via
# Erlang distribution (enabled for `start`/`rpc` in rel/env.sh.eex) and exits 0
# while the release VM — and therefore the supervised Celery worker — is alive.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD ["/opt/openrelik/bin/openrelik_worker_strings", "rpc", "IO.puts(:pong)"]
