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

  5. **Real-Time Progress Reporting (`send_event/3`) & Result Encoding (`TaskUtils.create_task_result/1`)**:
     Long-running commands emit `"task-progress"` events over Celery's `celeryev` fanout
     exchange for `openrelik-mediator` and Flower, and return a Base64-encoded JSON result
     string when finished.
  """

  use Celery.Task
  require Logger

  alias OpenrelikWorkerCommon.FileUtils
  alias OpenrelikWorkerCommon.FileUtils.OutputFile
  alias OpenrelikWorkerCommon.TaskUtils
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

  @default_progress_interval_ms 3_000

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
    output_files =
      for {encoding_name, encoding_flag} <- config_entries,
          input_file <- resolved_input_files,
          output_file =
            extract_strings_for_file(self, input_file, output_path, encoding_name, encoding_flag),
          not is_nil(output_file) do
        output_file
      end

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

  @doc """
  Emits a Celery task event (such as `"task-progress"`), matching `self.send_event(...)`
  in Python Celery.

  On Redis brokers, `Celery.Events.exchange/1` publishes the event over the `celeryev`
  fanout exchange (`/0.celeryev/task.progress`) so both OpenRelik Mediator and Flower
  receive real-time task progress updates.
  """
  @spec send_event(Celery.Task.t() | nil, String.t(), keyword() | map()) :: {:ok, map()}
  def send_event(self_task, event_type, fields \\ []) when is_binary(event_type) do
    fields_map =
      fields
      |> Enum.into(%{})
      |> Map.new(fn {k, v} -> {to_string(k), v} end)

    task_id = extract_task_id(self_task)

    hostname =
      case self_task do
        %{request: %{hostname: h}} when is_binary(h) and h != "" -> h
        _ -> default_hostname()
      end

    event =
      %{
        "type" => event_type,
        "uuid" => task_id,
        "hostname" => hostname,
        "timestamp" => System.system_time(:microsecond) / 1_000_000.0,
        "utcoffset" => 0,
        "pid" => System.pid() |> String.to_integer()
      }
      |> Map.merge(fields_map)

    Logger.debug(
      "Sending Celery event #{inspect(event_type)} (task_id=#{inspect(task_id)}, fields=#{inspect(fields_map)})"
    )

    notify_event_handler(event_type, event)
    maybe_publish_celery_event(self_task, event_type, event)

    {:ok, event}
  end

  # --- Internal Helpers ---

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
      strings_executable(),
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

    run_command_with_progress(self_task, command, output_file.path)

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
  # and periodically emits `"task-progress"` events until the command exits.
  defp run_command_with_progress(self_task, [executable | args], output_file_path) do
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

    {runner_pid, mon_ref} =
      spawn_monitor(fn ->
        stream_port_to_file(exec_path, args, output_file_path)
      end)

    start_time_us = System.monotonic_time(:microsecond)
    update_interval_ms = update_interval_ms()

    poll_until_finished(
      self_task,
      runner_pid,
      mon_ref,
      output_file_path,
      start_time_us,
      update_interval_ms
    )
  end

  defp stream_port_to_file(exec_path, args, output_file_path) do
    port =
      Port.open(
        {:spawn_executable, exec_path},
        [:binary, :exit_status, {:args, args}]
      )

    collect_port_output(port, nil, output_file_path)
  end

  defp collect_port_output(port, file, output_file_path) do
    receive do
      {^port, {:data, ""}} ->
        collect_port_output(port, file, output_file_path)

      {^port, {:data, chunk}} ->
        file = file || File.open!(output_file_path, [:write, :binary])
        IO.binwrite(file, chunk)
        collect_port_output(port, file, output_file_path)

      {^port, {:exit_status, 0}} ->
        if file, do: File.close(file)
        Logger.debug("Strings subprocess exited with status 0 (output=#{output_file_path})")
        :ok

      {^port, {:exit_status, status}} ->
        if file, do: File.close(file)

        Logger.warning(
          "Strings subprocess exited with non-zero status #{status} (output=#{output_file_path})"
        )

        :ok
    end
  end

  # Emits `"task-progress"` events every `update_interval_ms` while the subprocess is
  # alive, waking up immediately when the monitored subprocess exits (`{:DOWN, ...}`)
  # instead of blocking for the remainder of the sleep interval.
  defp poll_until_finished(
         self_task,
         runner_pid,
         mon_ref,
         output_file_path,
         start_time_us,
         update_interval_ms
       ) do
    receive do
      {:DOWN, ^mon_ref, :process, ^runner_pid, _reason} ->
        :ok
    after
      0 ->
        emit_progress(self_task, output_file_path, start_time_us)

        receive do
          {:DOWN, ^mon_ref, :process, ^runner_pid, _reason} ->
            :ok
        after
          update_interval_ms ->
            poll_until_finished(
              self_task,
              runner_pid,
              mon_ref,
              output_file_path,
              start_time_us,
              update_interval_ms
            )
        end
    end
  end

  defp emit_progress(self_task, output_file_path, start_time_us) do
    extracted_strings =
      if File.exists?(output_file_path) do
        FileUtils.count_file_lines(output_file_path)
      else
        0
      end

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

    send_event(
      self_task,
      "task-progress",
      data: %{"extracted_strings" => extracted_strings, "rate" => rate}
    )
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

  defp strings_executable do
    Application.get_env(:openrelik_worker_strings, :strings_executable, "strings")
  end

  defp update_interval_ms do
    Application.get_env(
      :openrelik_worker_strings,
      :update_interval_ms,
      @default_progress_interval_ms
    )
  end

  # Notifies any local test listener registered via `:event_handler` application env.
  defp notify_event_handler(event_type, event) do
    case Application.get_env(:openrelik_worker_strings, :event_handler) do
      pid when is_pid(pid) ->
        send(pid, {:celery_event, event_type, event})

      fun when is_function(fun, 2) ->
        fun.(event_type, event)

      fun when is_function(fun, 1) ->
        fun.(event)

      _ ->
        :ok
    end
  end

  # Publishes custom task events (e.g. `"task-progress"`) to the Celery event exchange
  # when running against a live broker (`is_eager: false`).
  defp maybe_publish_celery_event(
         %Celery.Task{app: %Celery{} = app, request: %{is_eager: false}},
         event_type,
         event
       ) do
    try do
      conn = Celery.AMQP.build_connection(app)

      try do
        exchange = Celery.Events.exchange(conn)
        routing_key = String.replace(event_type, "-", ".")
        producer = Kombu.Producer.new(conn, exchange: exchange, serializer: "json")

        Kombu.Producer.publish(producer, event,
          exchange: exchange,
          routing_key: routing_key,
          declare: [exchange],
          serializer: "json"
        )
      after
        Kombu.Connection.release(conn)
      end
    rescue
      error ->
        Logger.warning(
          "Failed to publish Celery event #{inspect(event_type)} to broker: #{Exception.message(error)}"
        )

        :ok
    end
  end

  defp maybe_publish_celery_event(_self_task, _event_type, _event), do: :ok

  defp default_hostname do
    {:ok, host} = :inet.gethostname()
    "celery@#{List.to_string(host)}"
  end
end
