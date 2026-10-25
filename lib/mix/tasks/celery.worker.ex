defmodule Mix.Tasks.Celery.Worker do
  @shortdoc "Starts the OpenRelik Strings Celery worker"
  @moduledoc """
  Starts the OpenRelik Strings Celery worker process under `OpenrelikWorkerStrings.Supervisor`,
  providing CLI flag compatibility with Python's `celery worker` command used in
  OpenRelik `docker-compose.yml` definitions.

  ## Examples

      # Standard OpenRelik docker-compose invocation:
      REDIS_URL=redis://localhost:6379/0 mix celery.worker \\
        --app=OpenrelikWorkerStrings.Tasks worker --task-events --concurrency=4 --loglevel=INFO -Q openrelik-worker-strings

      # Short flags with custom 60-second graceful shutdown timeout:
      REDIS_URL=redis://localhost:6379/0 mix celery.worker -c 4 -l info -Q openrelik-worker-strings --shutdown-timeout=60000

  ## Supported Options

  - `--app`, `-A` - Task module to load (default: `"OpenrelikWorkerStrings.Tasks"`).
  - `--concurrency`, `-c` - Number of concurrent task worker processes (default: `1`).
  - `--queues`, `-Q` - Comma-separated list of queues to consume from (always includes `"openrelik-worker-strings"`).
  - `--loglevel`, `-l` - Logger level (`debug`, `info`, `warning`, `error`; default: `info`).
  - `--task-events`, `-E` - Enable emission of `celeryev` task/worker events for Flower & OpenRelik Mediator (default: `true`).
  - `--hostname`, `-n` - Custom worker node hostname (default: `"celery@<host>"`).
  - `--shutdown-timeout` - Graceful OTP supervisor shutdown timeout in milliseconds or `"infinity"` (default: `WORKER_SHUTDOWN_TIMEOUT_MS` or `30000`).
  - `--broker` - Explicit broker URL override (defaults to `REDIS_URL`).
  - `--backend` - Explicit result backend URL override (defaults to `REDIS_URL`).
  """

  use Mix.Task
  require Logger

  @impl true
  def run(args) do
    {opts, _positional, _invalid} =
      OptionParser.parse(args,
        switches: [
          app: :string,
          concurrency: :integer,
          hostname: :string,
          loglevel: :string,
          queues: :string,
          task_events: :boolean,
          shutdown_timeout: :string,
          broker: :string,
          backend: :string
        ],
        aliases: [
          A: :app,
          c: :concurrency,
          E: :task_events,
          l: :loglevel,
          n: :hostname,
          Q: :queues
        ]
      )

    maybe_configure_loglevel(Keyword.get(opts, :loglevel))

    # Signal to OpenrelikWorkerStrings.Application that the CLI task will attach
    # the configured worker child spec to OpenrelikWorkerStrings.Supervisor after
    # parsing CLI arguments, preventing a duplicate worker if `START_WORKER=true`
    # is also set in the environment.
    OpenrelikWorkerStrings.Config.put_cli_worker_mode(true)
    Mix.Task.run("app.start")

    app_mod = Keyword.get(opts, :app, OpenrelikWorkerStrings.Tasks)
    Celery.Registry.load_include(app_mod)

    concurrency = Keyword.get(opts, :concurrency, 1)
    queues = OpenrelikWorkerStrings.CeleryApp.resolve_queues(Keyword.get(opts, :queues))
    send_events = Keyword.get(opts, :task_events, true)

    shutdown_timeout =
      case Keyword.get(opts, :shutdown_timeout) do
        nil -> OpenrelikWorkerStrings.CeleryApp.shutdown_timeout_ms()
        raw -> OpenrelikWorkerStrings.CeleryApp.parse_shutdown_timeout(raw)
      end

    app_opts =
      []
      |> maybe_put(:broker, Keyword.get(opts, :broker))
      |> maybe_put(:backend, Keyword.get(opts, :backend))

    app = OpenrelikWorkerStrings.CeleryApp.celery(app_opts)

    worker_opts =
      [
        app: app,
        concurrency: concurrency,
        queues: queues,
        send_events: send_events,
        shutdown: shutdown_timeout
      ]
      |> maybe_put(:hostname, Keyword.get(opts, :hostname))

    child_spec = OpenrelikWorkerStrings.CeleryApp.worker_child_spec(worker_opts)

    Logger.info(
      "Starting openrelik-worker-strings Celery worker " <>
        "(broker=#{app.config.broker_url}, concurrency=#{concurrency}, " <>
        "queues=#{inspect(queues)}, shutdown=#{inspect(shutdown_timeout)}, " <>
        "tasks=#{inspect(Celery.Registry.keys(app))})"
    )

    {:ok, _worker} = Supervisor.start_child(OpenrelikWorkerStrings.Supervisor, child_spec)
    Process.sleep(:infinity)
  end

  defp maybe_configure_loglevel(nil), do: :ok

  defp maybe_configure_loglevel(level_str) when is_binary(level_str) do
    level =
      case String.downcase(String.trim(level_str)) do
        "debug" -> :debug
        "info" -> :info
        "warn" -> :warning
        "warning" -> :warning
        "error" -> :error
        "critical" -> :critical
        "fatal" -> :emergency
        _ -> :info
      end

    Logger.configure(level: level)
  end

  defp maybe_put(kw, _k, nil), do: kw
  defp maybe_put(kw, k, v), do: Keyword.put(kw, k, v)
end
