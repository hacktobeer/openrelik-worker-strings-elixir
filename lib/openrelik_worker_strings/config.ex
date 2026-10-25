defmodule OpenrelikWorkerStrings.Config do
  @moduledoc """
  Centralized environment variable and application config resolution for
  `openrelik-worker-strings`.

  Every runtime knob of the worker is resolved in this single module so that
  env → config parsing is not duplicated across modules:

  - **Broker / result backend** — `REDIS_URL`
  - **Graceful shutdown** — `WORKER_SHUTDOWN_TIMEOUT_MS`
  - **OTP-supervised worker startup** — `START_WORKER`, `WORKER_CONCURRENCY`, `WORKER_QUEUES`
  - **Task runtime** — `strings(1)` executable location and `"task-progress"` interval

  Precedence for each setting: **environment variable > application config >
  documented default**. Environment variables are intended for container
  orchestration (docker-compose), while application config
  (`Application.put_env(:openrelik_worker_strings, ...)`) is intended for
  tests and programmatic overrides.
  """

  require Logger

  @app :openrelik_worker_strings

  @default_redis_url "redis://localhost:6379/0"
  @default_shutdown_timeout_ms 30_000
  @default_update_interval_ms 3_000

  # --- Broker & Result Backend ---

  @doc """
  Returns the Redis URL from the `REDIS_URL` environment variable (or the
  `:redis_url` application config), falling back to
  `"redis://localhost:6379/0"` when unset or empty.
  """
  @spec redis_url() :: String.t()
  def redis_url do
    case System.get_env("REDIS_URL") do
      url when is_binary(url) and url != "" ->
        Logger.debug("Resolved REDIS_URL from environment: #{url}")
        url

      _ ->
        url = app_env(:redis_url, @default_redis_url)

        Logger.debug("REDIS_URL environment variable not set; using default #{url}")

        url
    end
  end

  # --- Graceful Shutdown (`WORKER_SHUTDOWN_TIMEOUT_MS`) ---

  @doc """
  Returns the documented default graceful shutdown timeout (`30_000` ms).
  """
  @spec default_shutdown_timeout_ms() :: pos_integer()
  def default_shutdown_timeout_ms, do: @default_shutdown_timeout_ms

  @doc """
  Resolves the graceful shutdown timeout from the `WORKER_SHUTDOWN_TIMEOUT_MS`
  environment variable (or `:shutdown_timeout_ms` application config),
  defaulting to `#{@default_shutdown_timeout_ms}` ms (30 seconds).

  Also accepts `"infinity"` (returning `:infinity`).
  """
  @spec shutdown_timeout_ms() :: pos_integer() | :infinity
  def shutdown_timeout_ms do
    case System.get_env("WORKER_SHUTDOWN_TIMEOUT_MS") do
      nil -> parse_shutdown_timeout(app_env(:shutdown_timeout_ms, @default_shutdown_timeout_ms))
      raw -> parse_shutdown_timeout(raw)
    end
  end

  @doc """
  Parses a shutdown timeout value (integer milliseconds, numeric string, or
  `"infinity"`), falling back to `30_000` ms on invalid input.
  """
  @spec parse_shutdown_timeout(term()) :: pos_integer() | :infinity
  def parse_shutdown_timeout(:infinity), do: :infinity
  def parse_shutdown_timeout(ms) when is_integer(ms) and ms > 0, do: ms

  def parse_shutdown_timeout(raw) when is_binary(raw) do
    trimmed = String.trim(raw)

    if String.downcase(trimmed) == "infinity" do
      :infinity
    else
      case Integer.parse(trimmed) do
        {ms, ""} when ms > 0 ->
          ms

        _ ->
          Logger.warning(
            "Invalid WORKER_SHUTDOWN_TIMEOUT_MS=#{inspect(raw)}; falling back to #{@default_shutdown_timeout_ms}ms"
          )

          @default_shutdown_timeout_ms
      end
    end
  end

  def parse_shutdown_timeout(_), do: @default_shutdown_timeout_ms

  # --- OTP-Supervised Worker Startup (`START_WORKER`, `WORKER_CONCURRENCY`, `WORKER_QUEUES`) ---

  @doc """
  Whether a `Celery.Worker` should be started automatically under the
  application supervisor. Enabled via the `START_WORKER` environment variable
  (`"1"` or `"true"`, case-sensitive) or the `:start_worker` application config.
  """
  @spec start_worker?() :: boolean()
  def start_worker? do
    app_env(:start_worker, false) == true or
      System.get_env("START_WORKER") in ["1", "true", "TRUE"]
  end

  @doc """
  Resolves the number of concurrent task processes from the
  `WORKER_CONCURRENCY` environment variable (or `:worker_concurrency`
  application config), defaulting to `1` on unset or invalid input.
  """
  @spec worker_concurrency() :: pos_integer()
  def worker_concurrency do
    case System.get_env("WORKER_CONCURRENCY") do
      nil -> parse_worker_concurrency(app_env(:worker_concurrency, 1))
      raw -> parse_worker_concurrency(raw)
    end
  end

  @doc """
  Parses a worker concurrency value (positive integer, numeric string, or
  `nil`), falling back to `1` on unset or invalid input.
  """
  @spec parse_worker_concurrency(term()) :: pos_integer()
  def parse_worker_concurrency(nil), do: 1
  def parse_worker_concurrency(value) when is_integer(value) and value > 0, do: value

  def parse_worker_concurrency(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {n, ""} when n > 0 ->
        n

      _ ->
        Logger.warning("Invalid WORKER_CONCURRENCY=#{inspect(value)}; falling back to 1")

        1
    end
  end

  def parse_worker_concurrency(_), do: 1

  @doc """
  Resolves additional queue names to consume from from the
  `WORKER_QUEUES` environment variable (comma-separated, or `:worker_queues`
  application config as a list). The default OpenRelik queue is always
  prepended by `Src.App.resolve_queues/1` and should not be included here.
  """
  @spec worker_queues() :: [String.t()]
  def worker_queues do
    case System.get_env("WORKER_QUEUES") do
      raw when is_binary(raw) ->
        String.split(raw, ",", trim: true)

      _ ->
        app_env(:worker_queues, [])
    end
  end

  @doc """
  Whether the application was started via the `mix celery.worker` CLI task (set
  by `Mix.Tasks.Celery.Worker` so a duplicate supervised worker is not started
  when `START_WORKER=true` is also present in the container environment).
  """
  @spec cli_worker_mode?() :: boolean()
  def cli_worker_mode? do
    app_env(:cli_worker_mode, false) == true
  end

  @doc """
  Marks the application as running under the `mix celery.worker` CLI task.
  """
  @spec put_cli_worker_mode(boolean()) :: :ok
  def put_cli_worker_mode(value) do
    Application.put_env(@app, :cli_worker_mode, value)
    :ok
  end

  # --- Task Runtime ---

  @doc """
  Resolves the `strings(1)` executable from the `:strings_executable`
  application config (used by tests to point at a shim), defaulting to
  `"strings"` resolved from `PATH`.
  """
  @spec strings_executable() :: String.t()
  def strings_executable, do: app_env(:strings_executable, "strings")

  @doc """
  Resolves the `"task-progress"` event interval in milliseconds from the
  `:update_interval_ms` application config, defaulting to `3_000` ms.
  """
  @spec update_interval_ms() :: pos_integer()
  def update_interval_ms, do: app_env(:update_interval_ms, @default_update_interval_ms)

  @doc """
  Returns an optional local event listener registered via the `:event_handler`
  application config (a `pid` or 1/2-arity function; used by tests).
  """
  @spec event_handler() :: pid() | (term() -> term()) | (term(), term() -> term()) | nil
  def event_handler, do: app_env(:event_handler, nil)

  # --- Helpers ---

  defp app_env(key, default) do
    Application.get_env(@app, key, default)
  end
end
