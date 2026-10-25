# Changelog

All notable changes to `openrelik-worker-strings` (Elixir) are documented in
this file. The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to semantic versioning when release artifacts are
published.

## Unreleased

### Changed
- Unified module naming under the canonical `OpenrelikWorkerStrings.*` namespace
  (`OpenrelikWorkerStrings.CeleryApp`, `OpenrelikWorkerStrings.Tasks`,
  `OpenrelikWorkerStrings.Tasks.StringsEncoding`, `OpenrelikWorkerStrings.Events`).
- Output artifacts now declare `data_type: "text/plain"` so the OpenRelik UI and
  downstream tasks receive correct MIME metadata.
- Failing `strings(1)` subprocesses (non-zero exit status, unexpected runner
  termination) now fail the task and delete partial output files instead of
  silently registering truncated artifacts.
- Progress reporting counts extracted strings incrementally inside the
  subprocess runner instead of re-reading the whole output file per event;
  `task-progress` events are tagged with `file` and `encoding`.
- The misleading "No strings extracted from the provided files." error raised
  when no input files were received now reads
  "No input files provided for the strings task."

### Added
- Bounded parallel extraction within a task (`EXTRACTION_CONCURRENCY`
  environment variable / `:extraction_concurrency` application config,
  defaulting to `1`).
- `OpenrelikWorkerStrings.Events` (`send_event/3`, `celeryev` publishing) and
  `OpenrelikWorkerStrings.Config` (centralized environment variable and
  application config resolution: `REDIS_URL`, `WORKER_SHUTDOWN_TIMEOUT_MS`,
  `START_WORKER`, `WORKER_CONCURRENCY`, `WORKER_QUEUES`, `EXTRACTION_CONCURRENCY`,
  task runtime settings).
- Production Dockerfile on a self-contained `mix release` OTP release: multi
  stage build, non-root `openrelik` user, BEAM as PID 1 with graceful SIGTERM
  handling, and an `rpc`-based HEALTHCHECK.

## 0.2.1

- Elixir reference implementation of `openrelik-worker-strings`: extracts
  printable ASCII (`7-bit`) and Unicode (`UTF-16LE`) strings from forensic
  artifacts using GNU `strings(1)`, reports real-time extraction progress, and
  registers output artifacts in OpenRelik.
