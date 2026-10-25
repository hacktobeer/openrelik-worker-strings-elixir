defmodule Src.Tasks.StringsEncoding do
  @moduledoc """
  Supported string encodings and their corresponding GNU `strings(1)` `--encoding`
  flag values (ported from `StringsEncoding` in `src/tasks.py`).

  Per `strings(1)`:
  - `s` = single-7-bit-byte characters (ASCII, default)
  - `S` = single-8-bit-byte characters
  - `b` = 16-bit bigendian
  - `l` = 16-bit littleendian (UTF-16LE)
  - `B` = 32-bit bigendian
  - `L` = 32-bit littleendian
  """

  require Logger

  @members %{
    "ASCII" => "s",
    "UTF16LE" => "l"
  }

  @doc "Returns the `strings(1)` flag value for ASCII (`\"s\"`)."
  @spec ascii() :: String.t()
  def ascii, do: "s"

  @doc "Returns the `strings(1)` flag value for UTF16LE (`\"l\"`)."
  @spec utf16le() :: String.t()
  def utf16le, do: "l"

  @doc "Returns the map of valid encoding member names to their `strings(1)` flag values."
  @spec members() :: %{String.t() => String.t()}
  def members, do: @members

  @doc "Checks whether `name` is a valid `StringsEncoding` member name (`\"ASCII\"` or `\"UTF16LE\"`)."
  @spec valid?(String.t()) :: boolean()
  def valid?(name) when is_binary(name), do: Map.has_key?(@members, name)
  def valid?(_), do: false

  @doc "Fetches the `strings(1)` encoding value for `name` or raises `RuntimeError`."
  @spec fetch!(String.t()) :: String.t()
  def fetch!(name) when is_binary(name) do
    case Map.fetch(@members, name) do
      {:ok, value} ->
        value

      :error ->
        Logger.error("Invalid StringsEncoding requested: #{inspect(name)}")
        raise RuntimeError, "#{name} is not a valid StringsEncoding name"
    end
  end
end

defmodule Src.Tasks do
  @moduledoc """
  OpenRelik Celery task implementation for `openrelik-worker-strings`
  (Elixir port of `src/tasks.py`).

  ## Anatomy of an OpenRelik Task in Elixir

  This module serves as the reference template for building an OpenRelik worker in Elixir:

  1. **Task Name (`@task_name`)**:
     Must follow the `<queue_name>.tasks.<task_fn>` convention
     (here `"openrelik-worker-strings.tasks.strings"`). OpenRelik Server splits the
     task name on `"."` and routes jobs to the queue matching the first segment
     (`"openrelik-worker-strings"`).

  2. **Task Registration Metadata (`@task_metadata`)**:
     Attached to the task via `@task bind: true, name: @task_name, metadata: @task_metadata`.
     When OpenRelik Server runs Celery's `inspect.registered` control command, `Celery.Worker`
     serializes this metadata map as a Python literal so OpenRelik Server can display the
     worker and its `task_config` form controls in the OpenRelik web UI.

  3. **Input File Resolution (`TaskUtils.get_input_files/3`)**:
     The first task in a workflow receives `input_files` (a list of file maps), while
     downstream tasks in a chained workflow receive `pipe_result` (the Base64-encoded JSON
     output of the preceding task). `TaskUtils.get_input_files/3` transparently resolves both.

  4. **Output File Creation (`FileUtils.create_output_file/2`)**:
     Generates a UUID-named file path inside `output_path` on the shared OpenRelik artifact
     volume while recording the human-readable `display_name` and extension for database
     registration.

  5. **Real-Time Progress Reporting (`Src.Events.send_event/3`) & Result Encoding** (`TaskUtils.create_task_result/1`):
     Long-running commands emit `"task-progress"` events over Celery's `celeryev` fanout
     exchange for `openrelik-mediator` and Flower, and return a Base64-encoded JSON result
     string when finished.
  """

  use Celery.Task
  require Logger

  alias OpenrelikWorkerCommon.FileUtils
  alias OpenrelikWorkerCommon.FileUtils.OutputFile
  alias OpenrelikWorkerCommon.TaskUtils
  alias OpenrelikWorkerStrings.Config
  alias Src.Events
  alias Src.Tasks.StringsEncoding

  # Task name used to register and route the task to the "openrelik-worker-strings" queue.
  @task_name "openrelik-worker-strings.tasks.strings"

  # Task metadata exposed via Celery `inspect.registered` for the OpenRelik Server & UI.
  @task_metadata %{
    "display_name" => "Strings",
    "description" => "Extract strings from files",
    "task_config" => [
      %{
        "name" => "UTF16LE",
        "label" => "Extract Unicode strings",
        "description" =>
          "This will tell the strings command to extract UTF-16LE (little endian) encoded strings",
        "type" => "checkbox"
      },
      %{
        "name" => "ASCII",
        "label" => "Extract ASCII strings",
        "description" =>
          "This will tell the strings command to extract ASCII (single-7-bit-byte) encoded strings",
        "type" => "checkbox"
      }
    ]
  }

  @doc "Returns the registered Celery task name (`\"openrelik-worker-strings.tasks.strings\"`)."
  @spec task_name() :: String.t()
  def task_name, do: @task_name

  @doc "Returns the OpenRelik task registration metadata map."
  @spec task_metadata() :: map()
  def task_metadata, do: @task_metadata

  @doc """
  Extracts printable strings from input files for each encoding specified in `task_config`.

  ## Arguments
  - `self` - Bound `%Celery.Task{}` execution context (`bind: true`).
  - `pipe_result` - Base64-encoded result from the previous Celery task in the workflow, if any.
  - `input_files` - List of input file maps (used when `pipe_result` is `nil`).
  - `output_path` - Directory path on the shared OpenRelik volume where output artifacts are written.
  - `workflow_id` - ID of the OpenRelik workflow executing this task.
  - `task_config` - Map of user-selected task configuration options (e.g. `%{"ASCII" => true, "UTF16LE" => true}`).

  ## Returns
  A Base64-encoded JSON string created by `TaskUtils.create_task_result/1` describing
  all generated `output_files` and the `workflow_id`.
  """
  @task bind: true, name: @task_name, metadata: @task_metadata
  def strings(
        self,
        pipe_result \\ nil,
        input_files \\ nil,
        output_path \\ nil,
        workflow_id \\ nil,
        task_config \\ nil
      ) do
    task_id = extract_task_id(self)

    Logger.info(
      "Starting task #{@task_name} (task_id=#{inspect(task_id)}, " <>
        "workflow_id=#{inspect(workflow_id)}, output_path=#{inspect(output_path)}, " <>
        "task_config=#{inspect(task_config)})"
    )

    resolved_input_files = TaskUtils.get_input_files(pipe_result, input_files || [])

    Logger.info(
      "Resolved #{length(resolved_input_files)} input file(s) for task #{inspect(task_id)}: " <>
        inspect(Enum.map(resolved_input_files, &TaskUtils.get_field(&1, "display_name")))
    )

    if resolved_input_files == [] do
      Logger.error("Task #{@task_name} (task_id=#{inspect(task_id)}) received no input files")

      raise RuntimeError, "No strings extracted from the provided files."
    end

    config_entries = normalize_task_config(task_config)

    Logger.debug("Task #{inspect(task_id)} normalized config entries: #{inspect(config_entries)}")

    # Run extraction for the selected encodings (or default ASCII with `.strings` naming
    # if no encoding options were selected in the UI), and only register output files
    # where `strings` extracted data (`size > 0`).
    output_files = run_extractions(self, resolved_input_files, output_path, config_entries)

    if output_files == [] do
      Logger.info(
        "Task #{@task_name} (task_id=#{inspect(task_id)}, workflow_id=#{inspect(workflow_id)}) " <>
          "extracted no strings for the selected encoding(s); returning empty output_files"
      )
    else
      Logger.info(
        "Completed task #{@task_name} (task_id=#{inspect(task_id)}, " <>
          "workflow_id=#{inspect(workflow_id)}) with #{length(output_files)} output file(s)"
      )
    end

    TaskUtils.create_task_result(
      output_files: output_files,
      workflow_id: workflow_id,
      meta: %{}
    )
  end

  # --- Internal Helpers ---

  # Runs extraction for all (encoding, input file) pairs via
  # `Task.Supervisor.async_stream_nolink` with bounded concurrency
  # (`Config.extraction_concurrency/0`, default `1` for parity with the serial
  # Python worker), keeping `"task-progress"` events tagged per output file.
  # Results are collected in deterministic job order, and a failing extraction
  # (non-zero `strings` exit status or runner crash) re-raises in the task
  # process after the stream completes, aborting the task so no partial
  # outputs are registered.
  defp run_extractions(self_task, input_files, output_path, config_entries) do
    jobs =
      for {encoding_name, encoding_flag} <- config_entries,
          input_file <- input_files do
        {input_file, encoding_name, encoding_flag}
      end

    max_concurrency = min(Config.extraction_concurrency(), length(jobs))

    Logger.debug(
      "Running #{length(jobs)} strings extraction job(s) " <>
        "(max_concurrency=#{max_concurrency})"
    )

    results =
      Task.Supervisor.async_stream_nolink(
        OpenrelikWorkerStrings.TaskSupervisor,
        jobs,
        fn {input_file, encoding_name, encoding_flag} ->
          extract_strings_for_file(
            self_task,
            input_file,
            output_path,
            encoding_name,
            encoding_flag
          )
        end,
        max_concurrency: max_concurrency,
        ordered: true,
        timeout: :infinity
      )

    Enum.flat_map(results, &collect_extraction_result/1)
  end

  # Flattens one bounded-concurrency stream result: skips `nil` (no strings
  # extracted / skipped), keeps successful serialized output file maps, and
  # re-raises extraction worker crashes in the task process so the task fails
  # (matching the serial failure semantics of the pre-parallel implementation).
  defp collect_extraction_result({ok_result, nil}) when ok_result in [:ok, :exit] do
    []
  end

  defp collect_extraction_result({:ok, output_file}) do
    [output_file]
  end

  defp collect_extraction_result({:exit, {exception, stacktrace}})
       when is_exception(exception) and is_list(stacktrace) do
    reraise exception, stacktrace
  end

  defp collect_extraction_result({:exit, {reason, stacktrace}}) when is_list(stacktrace) do
    raise RuntimeError,
          "strings extraction worker exited unexpectedly: #{inspect({reason, stacktrace})}"
  end

  defp collect_extraction_result({:exit, reason}) do
    raise RuntimeError, "strings extraction worker exited unexpectedly: #{inspect(reason)}"
  end

  # Runs GNU `strings` for a single input file + encoding and returns its serialized
  # `OutputFile.to_dict/1` map.
  defp extract_strings_for_file(self_task, input_file, output_path, encoding_name, encoding_flag) do
    display_name = TaskUtils.get_field(input_file, "display_name")
    input_path = TaskUtils.get_field(input_file, "path")

    output_display_name =
      case encoding_name do
        nil -> "#{display_name}.strings"
        name -> "#{display_name}.#{name}_strings"
      end

    encoding_label = encoding_name || "default"

    output_file =
      FileUtils.create_output_file(
        output_path || "",
        display_name: output_display_name
      )

    # Flags passed to GNU `strings`:
    #   -a               : Scan the entire file, not only initialized/loaded data sections
    #   -t d             : Prefix each string with its byte offset in decimal
    #   --encoding <flag>: Character encoding ("s" for 7-bit ASCII, "l" for 16-bit UTF-16LE)
    command = [
      Config.strings_executable(),
      "-a",
      "-t",
      "d",
      "--encoding",
      encoding_flag,
      to_string(input_path)
    ]

    Logger.info(
      "Running strings extraction for #{inspect(display_name)} " <>
        "(encoding=#{encoding_label}/#{encoding_flag}, output=#{output_file.path}, " <>
        "command=#{inspect(Enum.join(command, " "))})"
    )

    run_command_with_progress(
      self_task,
      command,
      output_file.path,
      progress_fields(display_name, encoding_label)
    )

    case File.stat(output_file.path) do
      {:ok, %File.Stat{size: size}} when size > 0 ->
        OutputFile.to_dict(output_file)

      {:ok, %File.Stat{size: 0}} ->
        Logger.info(
          "No #{encoding_label} strings extracted from #{inspect(display_name)}; " <>
            "removing empty output file #{output_file.path}"
        )

        _ = File.rm(output_file.path)
        nil

      {:error, _reason} ->
        Logger.info(
          "No #{encoding_label} strings extracted from #{inspect(display_name)}; " <>
            "skipping output file registration"
        )

        nil
    end
  end

  # Spawns the external `strings` executable, streams its standard output directly to
  # `output_file_path` (creating the file on disk only when output bytes are produced),
  # and periodically emits `"task-progress"` events until the command exits. Events
  # are tagged with `progress_fields` (input file display name and encoding label)
  # so progress is unambiguous when extractions run in parallel.
  #
  # Raises `RuntimeError` when the `strings` subprocess exits with a non-zero status
  # or when the runner process crashes, so partially written output is never
  # registered as a successful artifact.
  defp run_command_with_progress(
         self_task,
         [executable | args],
         output_file_path,
         progress_fields
       ) do
    output_file_path |> Path.dirname() |> File.mkdir_p!()

    exec_path =
      case System.find_executable(executable) do
        nil ->
          Logger.error("Strings executable not found in PATH: #{inspect(executable)}")
          raise RuntimeError, "Executable not found: #{executable}"

        path ->
          path
      end

    Logger.debug(
      "Spawning strings subprocess: #{exec_path} #{Enum.join(args, " ")} -> #{output_file_path}"
    )

    parent = self()

    {runner_pid, mon_ref} =
      spawn_monitor(fn ->
        stream_port_to_file(parent, exec_path, args, output_file_path)
      end)

    start_time_us = System.monotonic_time(:microsecond)
    update_interval_ms = Config.update_interval_ms()

    emit_progress(self_task, runner_pid, output_file_path, start_time_us, progress_fields)

    await_command_completion(
      self_task,
      runner_pid,
      mon_ref,
      output_file_path,
      start_time_us,
      update_interval_ms,
      progress_fields
    )
  end

  # Runner process entry point: opens the port, streams its data to
  # `output_file_path` while counting lines incrementally, and reports the
  # final `{:strings_exited, {:ok, line_count} | {:error, reason}}` result to
  # `parent` before exiting cleanly.
  defp stream_port_to_file(parent, exec_path, args, output_file_path) do
    port =
      Port.open(
        {:spawn_executable, exec_path},
        [:binary, :exit_status, {:args, args}]
      )

    {exit_status, line_count} = collect_port_output(port, parent, nil, output_file_path, 0)

    result =
      if exit_status == 0 do
        {:ok, line_count}
      else
        {:error, {:exit_status, exit_status}}
      end

    Logger.debug(
      "Strings subprocess exited with status #{exit_status} and #{line_count} line(s) " <>
        "(output=#{output_file_path})"
    )

    send(parent, {self(), {:strings_exited, result}})
  end

  # Streams port data to `output_file_path`, counting newline occurrences
  # incrementally (matching `wc -l` semantics used by
  # `FileUtils.count_file_lines/1`) so progress events do not need to re-read the
  # output file, and answers `{:get_counts, reply_ref}` requests from the
  # polling (task) process with the current line count.
  defp collect_port_output(port, parent, file, output_file_path, line_count) do
    receive do
      {^port, {:data, ""}} ->
        collect_port_output(port, parent, file, output_file_path, line_count)

      {^port, {:data, chunk}} ->
        file = file || File.open!(output_file_path, [:write, :binary])
        IO.binwrite(file, chunk)

        collect_port_output(
          port,
          parent,
          file,
          output_file_path,
          line_count + count_newlines(chunk)
        )

      {^port, {:exit_status, status}} ->
        if file, do: File.close(file)
        {status, line_count}

      {^parent, {:get_counts, reply_ref}} ->
        send(parent, {reply_ref, line_count})
        collect_port_output(port, parent, file, output_file_path, line_count)
    end
  end

  defp count_newlines(chunk) when is_binary(chunk) do
    byte_size(chunk) - byte_size(:binary.replace(chunk, "\n", "", [:global]))
  end

  # Awaits the completion of the monitored subprocess runner while emitting
  # `"task-progress"` events every `update_interval_ms`, waking up immediately
  # when the runner reports completion or exits (`{:DOWN, ...}`).
  defp await_command_completion(
         self_task,
         runner_pid,
         mon_ref,
         output_file_path,
         start_time_us,
         update_interval_ms,
         progress_fields
       ) do
    receive do
      {^runner_pid, {:strings_exited, {:ok, _line_count}}} ->
        ensure_runner_down(mon_ref, runner_pid)

      {^runner_pid, {:strings_exited, {:error, reason}}} ->
        ensure_runner_down(mon_ref, runner_pid)
        cleanup_partial_output(output_file_path)

        raise RuntimeError, strings_failure_message(reason, output_file_path)

      {:DOWN, ^mon_ref, :process, ^runner_pid, :normal} ->
        # Safeguard in case the runner exits before its result message is
        # observed; peek briefly for a queued result before assuming success.
        receive do
          {^runner_pid, {:strings_exited, {:ok, _line_count}}} ->
            :ok

          {^runner_pid, {:strings_exited, {:error, reason}}} ->
            cleanup_partial_output(output_file_path)
            raise RuntimeError, strings_failure_message(reason, output_file_path)
        after
          5_000 ->
            :ok
        end

      {:DOWN, ^mon_ref, :process, ^runner_pid, reason} ->
        cleanup_partial_output(output_file_path)

        raise RuntimeError,
              "strings runner process exited unexpectedly " <>
                "(#{format_exit_reason(reason)}; output=#{output_file_path})"
    after
      update_interval_ms ->
        emit_progress(self_task, runner_pid, output_file_path, start_time_us, progress_fields)

        await_command_completion(
          self_task,
          runner_pid,
          mon_ref,
          output_file_path,
          start_time_us,
          update_interval_ms,
          progress_fields
        )
    end
  end

  # Waits for the monitored runner process to terminate after it reported its
  # final result, so no stale `{:DOWN, ...}` message is left in the caller's
  # mailbox.
  defp ensure_runner_down(mon_ref, runner_pid) do
    receive do
      {:DOWN, ^mon_ref, :process, ^runner_pid, _reason} ->
        :ok
    after
      5_000 ->
        :ok
    end
  end

  # Deletes a truncated output file produced by a failed `strings` subprocess so
  # partial data is never left on disk or registered downstream.
  defp cleanup_partial_output(output_file_path) do
    case File.rm(output_file_path) do
      :ok ->
        Logger.info(
          "Removed partial strings output file after subprocess failure: #{output_file_path}"
        )

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "Failed to remove partial strings output file #{output_file_path}: #{inspect(reason)}"
        )

        :ok
    end
  end

  defp strings_failure_message({:exit_status, status}, output_file_path) do
    "strings command exited with non-zero status #{status} (output=#{output_file_path})"
  end

  defp format_exit_reason({exception, stacktrace})
       when is_exception(exception) and is_list(stacktrace) do
    Exception.format_banner(:error, exception, stacktrace)
  rescue
    _ -> inspect({exception, stacktrace})
  end

  defp format_exit_reason(reason), do: inspect(reason)

  # Emits a `"task-progress"` event reporting the incremental line count tracked
  # by the running subprocess (no full file re-read), the extraction rate, and
  # the `progress_fields` (input file display name and encoding label) that
  # identify this extraction when several run in parallel.
  defp emit_progress(self_task, runner_pid, output_file_path, start_time_us, progress_fields) do
    extracted_strings = get_line_count(runner_pid)

    duration_s = (System.monotonic_time(:microsecond) - start_time_us) / 1_000_000.0

    rate =
      if duration_s > 0 do
        trunc(extracted_strings / duration_s)
      else
        0
      end

    Logger.debug(
      "Progress poll for #{output_file_path}: extracted_strings=#{extracted_strings}, rate=#{rate}/s"
    )

    Events.send_event(
      self_task,
      "task-progress",
      data:
        Map.merge(progress_fields, %{
          "extracted_strings" => extracted_strings,
          "rate" => rate
        })
    )
  end

  defp progress_fields(display_name, encoding_label) do
    %{"file" => display_name, "encoding" => encoding_label}
  end

  # Requests the current incremental line count from the runner process.
  # Falls back to `0` (with a debug log) when the runner is already gone —
  # completion is handled via the `{:DOWN, ...}`/result messages instead.
  defp get_line_count(runner_pid) do
    reply_ref = make_ref()
    send(runner_pid, {self(), {:get_counts, reply_ref}})

    receive do
      {^reply_ref, count} when is_integer(count) ->
        count
    after
      1_000 ->
        Logger.debug("Timed out requesting line count from runner #{inspect(runner_pid)}")
        0
    end
  end

  defp extract_task_id(%{request: %{id: id}}), do: id
  defp extract_task_id(_), do: nil

  defp normalize_task_config(nil), do: [{nil, StringsEncoding.ascii()}]

  defp normalize_task_config(config) when is_map(config) or is_list(config) do
    entries =
      Enum.map(config, fn {k, v} ->
        encoding_name = to_string(k)
        encoding_flag = StringsEncoding.fetch!(encoding_name)
        {encoding_name, encoding_flag, v}
      end)

    enabled_entries =
      for {encoding_name, encoding_flag, true} <- entries,
          do: {encoding_name, encoding_flag}

    if enabled_entries == [] do
      [{nil, StringsEncoding.ascii()}]
    else
      enabled_entries
    end
  end
end
