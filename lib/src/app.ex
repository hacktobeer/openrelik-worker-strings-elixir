defmodule Src.App do
  @moduledoc """
  Celery application and worker configuration for `openrelik-worker-strings`
  (Elixir port of `src/app.py`).

  ## OpenRelik Broker & Queue Conventions

  1. **Redis Broker & Result Backend**:
     OpenRelik passes the Redis connection string via the `REDIS_URL` environment
     variable (for example, `redis://openrelik-redis:6379/0`). Both the Celery
     message broker and the result backend use this Redis instance.

  2. **Queue Naming (`"openrelik-worker-strings"`)**:
     OpenRelik Server discovers registered tasks via Celery's `inspect.registered`
     control command and automatically routes each task to the queue matching the
     first segment of its task name (`task_name.split(".")[0]`).
     Because the task name in `Src.Tasks` is `"openrelik-worker-strings.tasks.strings"`,
     OpenRelik Server routes jobs to the `"openrelik-worker-strings"` queue.

  3. **Graceful Shutdown Timeout (`WORKER_SHUTDOWN_TIMEOUT_MS`)**:
     When the OTP supervisor stops `Celery.Worker` (for example, on `SIGTERM` from
     `docker stop`), it waits up to `shutdown_timeout_ms/0` (default `30_000` ms /
     30 seconds, or `:infinity` when set to `"infinity"`) for the worker to emit
     `"worker-offline"`, cancel active queue consumers, and close broker connections
     cleanly before forcibly terminating the process.
  """

  require Logger

  @default_redis_url "redis://localhost:6379/0"
  @default_queue "openrelik-worker-strings"
  @default_shutdown_timeout_ms 30_000
  @task_modules ["src.tasks"]

  @doc """
  Returns the default OpenRelik queue name (`"openrelik-worker-strings"`).

  In OpenRelik, this must match the prefix of `Src.Tasks.task_name/0` before the
  first dot (`"openrelik-worker-strings.tasks.strings"` -> `"openrelik-worker-strings"`).
  """
  @spec default_queue() :: String.t()
  def default_queue, do: @default_queue

  @doc """
  Returns the default OTP supervisor shutdown timeout in milliseconds (`30_000`).
  """
  @spec default_shutdown_timeout_ms() :: pos_integer()
  def default_shutdown_timeout_ms, do: @default_shutdown_timeout_ms

  @doc """
  Resolves the OTP supervisor graceful shutdown timeout from the
  `WORKER_SHUTDOWN_TIMEOUT_MS` environment variable (or `:shutdown_timeout_ms`
  application config), defaulting to `30_000` ms (30 seconds).

  Also accepts `"infinity"` (returning `:infinity`).
  """
  @spec shutdown_timeout_ms() :: pos_integer() | :infinity
  def shutdown_timeout_ms do
    case System.get_env("WORKER_SHUTDOWN_TIMEOUT_MS") do
      nil ->
        Application.get_env(
          :openrelik_worker_strings,
          :shutdown_timeout_ms,
          @default_shutdown_timeout_ms
        )

      raw ->
        parse_shutdown_timeout(raw)
    end
  end

  @doc """
  Parses a shutdown timeout value (integer milliseconds, numeric string, or `"infinity"`),
  falling back to `30_000` ms on invalid input.
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

  @doc """
  Returns the Redis URL from the `REDIS_URL` environment variable, falling back
  to `"redis://localhost:6379/0"` when unset or empty.
  """
  @spec redis_url() :: String.t()
  def redis_url do
    case System.get_env("REDIS_URL") do
      url when is_binary(url) and url != "" ->
        Logger.debug("Resolved REDIS_URL from environment: #{url}")
        url

      _ ->
        Logger.debug(
          "REDIS_URL environment variable not set; falling back to default #{@default_redis_url}"
        )

        @default_redis_url
    end
  end

  @doc """
  Normalizes a queue specification (nil, list of queue names, or comma-separated
  string) into a deduplicated list of queue names that always includes
  `default_queue/0`.
  """
  @spec resolve_queues(nil | String.t() | [String.t()]) :: [String.t()]
  def resolve_queues(nil), do: [@default_queue]
  def resolve_queues([]), do: [@default_queue]

  def resolve_queues(queues) when is_binary(queues) do
    queues
    |> String.split(",", trim: true)
    |> resolve_queues()
  end

  def resolve_queues(queues) when is_list(queues) do
    Enum.uniq([@default_queue | queues])
  end

  @doc """
  Builds a `%Celery{}` application instance configured with `REDIS_URL` as both
  broker and result backend, and `include: ["src.tasks"]`.

  ## Options
  - `:redis_url` - Explicit Redis URL (defaults to `redis_url/0`).
  - `:broker` - Overrides the broker URL (e.g. `"memory://"` in unit tests).
  - `:backend` - Overrides the result backend URL.
  - Any additional `Celery.new/1` configuration options.
  """
  @spec celery(keyword()) :: Celery.t()
  def celery(opts \\ []) do
    url = Keyword.get_lazy(opts, :redis_url, &redis_url/0)

    base_opts = [
      broker: url,
      backend: url,
      include: @task_modules,
      task_default_queue: @default_queue,
      task_default_exchange: @default_queue,
      task_default_routing_key: @default_queue,
      task_queues: [@default_queue]
    ]

    merged_opts =
      base_opts
      |> Keyword.merge(Keyword.delete(opts, :redis_url))

    app = Celery.new(merged_opts)

    Logger.debug(
      "Initialized Celery app (broker=#{inspect(app.config.broker_url)}, " <>
        "backend=#{inspect(app.config.result_backend)}, " <>
        "include=#{inspect(app.config.include)})"
    )

    app
  end

  @doc """
  Builds an OTP `Supervisor` child specification for `Celery.Worker` with a
  configured graceful `:shutdown` timeout (defaulting to `shutdown_timeout_ms/0`).

  ## Options
  - `:shutdown` - Explicit supervisor shutdown grace period in milliseconds or `:infinity`
    (defaults to `shutdown_timeout_ms/0`).
  - Any `celery/1` or `Celery.Worker.start_link/1` options.
  """
  @spec worker_child_spec(keyword()) :: Supervisor.child_spec()
  def worker_child_spec(opts \\ []) do
    {shutdown_opt, rest_opts} = Keyword.pop(opts, :shutdown)
    shutdown_timeout = parse_shutdown_timeout(shutdown_opt || shutdown_timeout_ms())
    worker_opts = build_worker_opts(rest_opts)

    Supervisor.child_spec({Celery.Worker, worker_opts}, shutdown: shutdown_timeout)
  end

  @doc """
  Starts a linked `Celery.Worker` process for this application.

  Accepts both `celery/1` application options (`:broker`, `:backend`, `:redis_url`, etc.)
  and `Celery.Worker.start_link/1` worker options (`:concurrency`, `:queues`,
  `:send_events`, `:hostname`, `:poll_interval`, `:notify`, etc.).
  """
  @spec start_worker(keyword()) :: GenServer.on_start()
  def start_worker(opts \\ []) do
    worker_opts = build_worker_opts(opts)
    app = Keyword.fetch!(worker_opts, :app)
    queues = Keyword.fetch!(worker_opts, :queues)

    Logger.info(
      "Starting Celery.Worker (broker=#{inspect(app.config.broker_url)}, " <>
        "concurrency=#{inspect(Keyword.get(worker_opts, :concurrency))}, " <>
        "queues=#{inspect(queues)})"
    )

    Celery.Worker.start_link(worker_opts)
  end

  defp build_worker_opts(opts) do
    {app_opts, worker_opts} =
      Keyword.split(opts, [
        :broker,
        :backend,
        :broker_url,
        :result_backend,
        :redis_url,
        :redis_conn,
        :include
      ])

    app = Keyword.get_lazy(worker_opts, :app, fn -> celery(app_opts) end)
    queues = resolve_queues(Keyword.get(worker_opts, :queues))

    worker_opts
    |> Keyword.put(:app, app)
    |> Keyword.put(:queues, queues)
    |> Keyword.put_new(:concurrency, 1)
  end
end
