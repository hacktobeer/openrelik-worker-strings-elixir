defmodule OpenrelikWorkerStrings do
  @moduledoc """
  Reference OpenRelik Celery worker written in Elixir — extracts printable ASCII and
  UTF-16LE strings from forensic artifacts using GNU `strings(1)`.

  ## Architecture & Module Overview

  - `OpenrelikWorkerStrings.Config` — Centralized environment variable and
    application config resolution (`REDIS_URL`, `WORKER_SHUTDOWN_TIMEOUT_MS`,
    `START_WORKER`/`WORKER_CONCURRENCY`/`WORKER_QUEUES`, `strings(1)` executable
    and progress interval settings).
  - `Src.App` — Configures the `%Celery{}` application (`REDIS_URL` broker/backend,
    `"openrelik-worker-strings"` queue routing, and `WORKER_SHUTDOWN_TIMEOUT_MS`
    graceful shutdown configuration) and builds/starts `Celery.Worker`.
  - `Src.Tasks` — Defines the `"openrelik-worker-strings.tasks.strings"` Celery task,
    its OpenRelik UI metadata (`@task_metadata`), subprocess execution, and real-time
    `"task-progress"` event reporting.
  - `Src.Events` — Celery event emission plumbing (`send_event/3`, matching
    `self.send_event(...)` in Python Celery) publishing to the `celeryev` exchange
    for OpenRelik Mediator and Flower.
  - `OpenrelikWorkerCommon` (`:openrelik_worker_common` Hex package) — Shared OpenRelik
    worker utilities (`OpenrelikWorkerCommon.FileUtils` and `OpenrelikWorkerCommon.TaskUtils`)
    for resolving chained (`pipe_result`) or direct (`input_files`) workflow inputs,
    generating UUID-named `%OutputFile{}` artifacts, counting extracted lines, and encoding
    task results as Base64 JSON strings for `openrelik-mediator`.
  - `Mix.Tasks.Celery.Worker` — Provides the `mix celery.worker` CLI entry point compatible
    with OpenRelik's `docker-compose.yml` worker command format.
  """

  defdelegate default_queue(), to: Src.App
  defdelegate default_shutdown_timeout_ms(), to: Src.App
  defdelegate shutdown_timeout_ms(), to: Src.App
  defdelegate redis_url(), to: Src.App
  defdelegate celery(opts \\ []), to: Src.App
  defdelegate worker_child_spec(opts \\ []), to: Src.App
  defdelegate start_worker(opts \\ []), to: Src.App
  defdelegate task_name(), to: Src.Tasks
  defdelegate task_metadata(), to: Src.Tasks
end
