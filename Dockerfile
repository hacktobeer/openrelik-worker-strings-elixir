# Use the official Elixir image matching mise.toml (elixir 1.20.4-otp-29)
FROM elixir:1.20.4-otp-29-slim

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install binutils (provides GNU `strings` with `--encoding` support) and ca-certificates
RUN apt-get update && apt-get install -y --no-install-recommends \
    binutils \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /openrelik

# Run in production Mix environment
ENV MIX_ENV=prod

# Install Hex and Rebar
RUN mix local.hex --force && mix local.rebar --force

# Copy dependency definitions and fetch/compile production dependencies for layer caching
COPY mix.exs mix.lock ./
RUN mix deps.get --only prod && mix deps.compile

# Copy application source files and compile
COPY .formatter.exs ./
COPY lib ./lib
RUN mix compile

# Default Redis URL (override at runtime with `-e REDIS_URL=...`)
ENV REDIS_URL="redis://localhost:6379/0"

# Default command to start the Elixir Celery worker
CMD ["mix", "celery.worker", "-l", "info", "-Q", "openrelik-worker-strings"]
