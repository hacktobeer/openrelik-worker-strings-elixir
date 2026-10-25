defmodule OpenrelikWorkerStrings.Application do
  @moduledoc """
  OTP Application callback module for `openrelik-worker-strings`.

  On startup this application:
  1. Loads and registers the Celery task module (`Src.Tasks`) in `Celery.Registry`
     so task definitions and OpenRelik metadata are immediately available.
  2. Starts the top-level `OpenrelikWorkerStrings.Supervisor`.
  3. Optionally starts a supervised `Celery.Worker` child process with a configurable
     graceful `:shutdown` timeout (`WORKER_SHUTDOWN_TIMEOUT_MS`, default `30_000` ms)
     when `START_WORKER=true` (or `:start_worker` application config) is enabled and
     the application is not already being launched via the `mix celery.worker` CLI task.

  ## Starting the Worker

  There are two supported ways to run the worker in production or development:

  - **CLI Task (recommended / Docker default)**:
    ```bash
    mix celery.worker --app=src.tasks worker --task-events --concurrency=1 --loglevel=INFO -Q openrelik-worker-strings
    ```
  - **OTP-Supervised Worker (`mix run --no-halt` or Elixir release)**:
    ```bash
    START_WORKER=true WORKER_CONCURRENCY=4 WORKER_SHUTDOWN_TIMEOUT_MS=30000 mix run --no-halt
    ```
  """

  use Application
  require Logger

  alias OpenrelikWorkerStrings.Config

  @impl true
  def start(_type, _args) do
    Logger.info("Starting OpenrelikWorkerStrings.Application")

    # Ensure the task module is loaded and registered in Celery.Registry on startup
    Celery.Registry.load_include(Src.Tasks)

    registered_tasks = Map.keys(Celery.Registry.all())
    Logger.info("Registered Celery tasks: #{inspect(registered_tasks)}")

    children =
      if Config.start_worker?() and not Config.cli_worker_mode?() do
        concurrency = Config.worker_concurrency()
        queues = Src.App.resolve_queues(Config.worker_queues())
        shutdown_timeout = Config.shutdown_timeout_ms()

        Logger.info(
          "Starting supervised Celery.Worker " <>
            "(concurrency=#{concurrency}, queues=#{inspect(queues)}, shutdown=#{inspect(shutdown_timeout)})"
        )

        [
          {Task.Supervisor, name: OpenrelikWorkerStrings.TaskSupervisor},
          Src.App.worker_child_spec(
            concurrency: concurrency,
            queues: queues,
            shutdown: shutdown_timeout
          )
        ]
      else
        Logger.debug(
          "Supervised Celery.Worker not enabled (using `mix celery.worker` CLI or START_WORKER is unset)"
        )

        [{Task.Supervisor, name: OpenrelikWorkerStrings.TaskSupervisor}]
      end

    opts = [strategy: :one_for_one, name: OpenrelikWorkerStrings.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # A supervised worker is started only when `Config.start_worker?/0` is enabled
  # and the application was not launched via `mix celery.worker`. When launched
  # via `mix celery.worker` (`Config.put_cli_worker_mode(true)`), `Mix.Tasks.Celery.Worker`
  # attaches the worker child spec to `OpenrelikWorkerStrings.Supervisor` using
  # its parsed CLI flags instead, preventing a duplicate worker from being
  # started if `START_WORKER=true` is also present in the container environment.
end
