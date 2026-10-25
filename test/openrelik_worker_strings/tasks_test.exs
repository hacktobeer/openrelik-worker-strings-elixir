defmodule OpenrelikWorkerStrings.TasksTest do
  use ExUnit.Case, async: false

  alias OpenrelikWorkerCommon.TaskUtils
  alias OpenrelikWorkerStrings.CeleryApp
  alias OpenrelikWorkerStrings.Tasks
  alias OpenrelikWorkerStrings.Tasks.StringsEncoding

  setup %{tmp_dir: tmp_dir} do
    # Create a portable GNU `strings` shim executable in tmp_dir that validates
    # `-a -t d --encoding <s|l> <file>` and outputs offset + extracted string lines.
    shim_path = Path.join(tmp_dir, "strings_shim.sh")

    shim_script = """
    #!/bin/sh
    if [ "$1" != "-a" ] || [ "$2" != "-t" ] || [ "$3" != "d" ] || [ "$4" != "--encoding" ]; then
      echo "unexpected arguments: $@" >&2
      exit 1
    fi
    ENC="$5"
    FILE="$6"
    if [ -n "$STRINGS_SHIM_SLEEP" ]; then
      sleep "$STRINGS_SHIM_SLEEP"
    fi
    if [ "$STRINGS_SHIM_EMPTY_ENC" = "$ENC" ] || [ "$STRINGS_SHIM_EMPTY_ENC" = "all" ]; then
      exit 0
    fi
    echo "0 extracted_${ENC}_line_1 from $(basename "$FILE")"
    echo "16 extracted_${ENC}_line_2 from $(basename "$FILE")"
    exit "${STRINGS_SHIM_EXIT:-0}"
    """

    File.write!(shim_path, shim_script)
    File.chmod!(shim_path, 0o755)

    Application.put_env(:openrelik_worker_strings, :strings_executable, shim_path)
    Application.put_env(:openrelik_worker_strings, :update_interval_ms, 20)
    Application.put_env(:openrelik_worker_strings, :event_handler, self())

    on_exit(fn ->
      Application.delete_env(:openrelik_worker_strings, :strings_executable)
      Application.delete_env(:openrelik_worker_strings, :update_interval_ms)
      Application.delete_env(:openrelik_worker_strings, :event_handler)
      System.delete_env("STRINGS_SHIM_SLEEP")
      System.delete_env("STRINGS_SHIM_EMPTY_ENC")
      System.delete_env("STRINGS_SHIM_EXIT")
    end)

    :ok
  end

  @tag :tmp_dir
  test "StringsEncoding maps ASCII and UTF16LE to strings(1) flags" do
    assert StringsEncoding.ascii() == "s"
    assert StringsEncoding.utf16le() == "l"
    assert StringsEncoding.fetch!("ASCII") == "s"
    assert StringsEncoding.fetch!("UTF16LE") == "l"

    assert_raise RuntimeError, "UTF32 is not a valid StringsEncoding name", fn ->
      StringsEncoding.fetch!("UTF32")
    end
  end

  @tag :tmp_dir
  test "task registration and metadata match Python openrelik-worker-strings" do
    app = CeleryApp.celery()
    assert Tasks.task_name() == "openrelik-worker-strings.tasks.strings"

    {:ok, defn} = Celery.Registry.lookup("openrelik-worker-strings.tasks.strings", app)
    assert defn.bind == true
    assert defn.name == "openrelik-worker-strings.tasks.strings"
    assert defn.metadata["display_name"] == "Strings"
    assert defn.metadata["description"] == "Extract strings from files"

    assert [
             %{"name" => "UTF16LE", "type" => "checkbox"},
             %{"name" => "ASCII", "type" => "checkbox"}
           ] = defn.metadata["task_config"]
  end

  @tag :tmp_dir
  test "strings task extracts ASCII and UTF16LE strings and emits task-progress events", %{
    tmp_dir: tmp_dir
  } do
    System.put_env("STRINGS_SHIM_SLEEP", "0.05")

    # Run both (encoding × input file) extraction jobs in parallel so the
    # bounded-concurrency `Task.async_stream` path is exercised. Progress
    # events are tagged per output file to stay unambiguous under parallelism.
    Application.put_env(:openrelik_worker_strings, :extraction_concurrency, 2)

    on_exit(fn ->
      Application.delete_env(:openrelik_worker_strings, :extraction_concurrency)
    end)

    input_path = Path.join(tmp_dir, "sample.bin")
    File.write!(input_path, "hello world\n")

    output_dir = Path.join(tmp_dir, "out")
    File.mkdir_p!(output_dir)

    input_files = [
      %{
        "display_name" => "sample.bin",
        "path" => input_path
      }
    ]

    task_config = %{
      "ASCII" => true,
      "UTF16LE" => true
    }

    app = CeleryApp.celery(task_always_eager: true, task_eager_propagates: true)

    {:ok, async_res} =
      Celery.send_task(app, Tasks.task_name(),
        kwargs: %{
          "pipe_result" => nil,
          "input_files" => input_files,
          "output_path" => output_dir,
          "workflow_id" => "wf-001",
          "task_config" => task_config
        }
      )

    encoded_result = Celery.Result.get(async_res)
    decoded = TaskUtils.decode_base64_to_dict(encoded_result)

    assert decoded["workflow_id"] == "wf-001"
    assert decoded["meta"] == %{}
    assert length(decoded["output_files"]) == 2

    by_display_name = Map.new(decoded["output_files"], &{&1["display_name"], &1})

    ascii_out = Map.fetch!(by_display_name, "sample.bin.ASCII_strings")
    assert ascii_out["extension"] == ".ASCII_strings"
    assert ascii_out["data_type"] == "text/plain"
    assert File.read!(ascii_out["path"]) =~ "extracted_s_line_1 from sample.bin"

    utf16_out = Map.fetch!(by_display_name, "sample.bin.UTF16LE_strings")
    assert utf16_out["extension"] == ".UTF16LE_strings"
    assert File.read!(utf16_out["path"]) =~ "extracted_l_line_1 from sample.bin"

    assert_receive {:celery_event, "task-progress",
                    %{
                      "type" => "task-progress",
                      "data" => %{
                        "extracted_strings" => _,
                        "rate" => _,
                        "file" => "sample.bin",
                        "encoding" => _
                      }
                    }}
  end

  @tag :tmp_dir
  test "strings task uses pipe_result when provided", %{tmp_dir: tmp_dir} do
    piped_file = Path.join(tmp_dir, "piped.bin")
    File.write!(piped_file, "piped content\n")

    output_dir = Path.join(tmp_dir, "out_pipe")
    File.mkdir_p!(output_dir)

    pipe_result =
      TaskUtils.encode_dict_to_base64(%{
        "output_files" => [
          %{
            "display_name" => "piped.bin",
            "path" => piped_file
          }
        ]
      })

    app = CeleryApp.celery(task_always_eager: true, task_eager_propagates: true)

    {:ok, async_res} =
      Tasks.apply_async(app, :strings,
        kwargs: %{
          "pipe_result" => pipe_result,
          "input_files" => [],
          "output_path" => output_dir,
          "workflow_id" => "wf-pipe",
          "task_config" => %{"ASCII" => true}
        }
      )

    decoded = async_res |> Celery.Result.get() |> TaskUtils.decode_base64_to_dict()
    assert [out_file] = decoded["output_files"]
    assert out_file["display_name"] == "piped.bin.ASCII_strings"
    assert File.read!(out_file["path"]) =~ "extracted_s_line_1 from piped.bin"
  end

  @tag :tmp_dir
  test "strings task raises RuntimeError on invalid StringsEncoding name", %{tmp_dir: tmp_dir} do
    app = CeleryApp.celery(task_always_eager: true, task_eager_propagates: true)

    assert_raise RuntimeError, "BAD_ENC is not a valid StringsEncoding name", fn ->
      Celery.send_task(app, Tasks.task_name(),
        kwargs: %{
          "input_files" => [%{"display_name" => "a.bin", "path" => Path.join(tmp_dir, "a.bin")}],
          "output_path" => tmp_dir,
          "workflow_id" => "wf-err",
          "task_config" => %{"BAD_ENC" => true}
        }
      )
    end
  end

  @tag :tmp_dir
  test "strings task runs default strings without encoding in filename when none are checked, and includes encoding when checked",
       %{
         tmp_dir: tmp_dir
       } do
    input_path = Path.join(tmp_dir, "ascii_only.txt")
    File.write!(input_path, "plain ascii text\n")

    output_dir = Path.join(tmp_dir, "out_selective")
    File.mkdir_p!(output_dir)

    app = CeleryApp.celery(task_always_eager: true, task_eager_propagates: true)

    # 1. When both checkboxes are nil (default OpenRelik workflow config with no encoding selected),
    #    default strings ("s") runs and output filename does NOT contain ASCII or UTF16LE.
    {:ok, res_default_nil} =
      Celery.send_task(app, Tasks.task_name(),
        kwargs: %{
          "input_files" => [%{"display_name" => "ascii_only.txt", "path" => input_path}],
          "output_path" => output_dir,
          "workflow_id" => "wf-default-nil",
          "task_config" => %{"ASCII" => nil, "UTF16LE" => nil}
        }
      )

    decoded_nil = res_default_nil |> Celery.Result.get() |> TaskUtils.decode_base64_to_dict()
    assert [default_file] = decoded_nil["output_files"]
    assert default_file["display_name"] == "ascii_only.txt.strings"
    refute String.contains?(default_file["display_name"], "ASCII")
    refute String.contains?(default_file["display_name"], "UTF16LE")
    assert File.exists?(default_file["path"])

    # 2. When ASCII is false/nil and UTF16LE is true, only UTF16LE is executed and registered
    output_dir_only_utf16 = Path.join(tmp_dir, "out_only_utf16")
    File.mkdir_p!(output_dir_only_utf16)

    {:ok, res_only_utf16} =
      Celery.send_task(app, Tasks.task_name(),
        kwargs: %{
          "input_files" => [%{"display_name" => "ascii_only.txt", "path" => input_path}],
          "output_path" => output_dir_only_utf16,
          "workflow_id" => "wf-selective-1",
          "task_config" => %{"ASCII" => false, "UTF16LE" => true}
        }
      )

    decoded_1 = res_only_utf16 |> Celery.Result.get() |> TaskUtils.decode_base64_to_dict()
    assert [utf16_file] = decoded_1["output_files"]
    assert utf16_file["display_name"] == "ascii_only.txt.UTF16LE_strings"
    assert File.exists?(utf16_file["path"])

    # 3. When both are checked, but UTF16LE ("l") produces no output, only ASCII is registered
    #    and no empty UTF16LE file exists on disk
    output_dir_2 = Path.join(tmp_dir, "out_no_utf16")
    File.mkdir_p!(output_dir_2)
    System.put_env("STRINGS_SHIM_EMPTY_ENC", "l")

    {:ok, res_no_utf16} =
      Celery.send_task(app, Tasks.task_name(),
        kwargs: %{
          "input_files" => [%{"display_name" => "ascii_only.txt", "path" => input_path}],
          "output_path" => output_dir_2,
          "workflow_id" => "wf-selective-2",
          "task_config" => %{"ASCII" => true, "UTF16LE" => true}
        }
      )

    decoded_2 = res_no_utf16 |> Celery.Result.get() |> TaskUtils.decode_base64_to_dict()
    assert [ascii_file] = decoded_2["output_files"]
    assert ascii_file["display_name"] == "ascii_only.txt.ASCII_strings"
    assert File.exists?(ascii_file["path"])
    assert File.ls!(output_dir_2) == [Path.basename(ascii_file["path"])]
  end

  @tag :tmp_dir
  test "strings task raises RuntimeError when no input files are provided and returns empty output_files when 0 strings match",
       %{tmp_dir: tmp_dir} do
    app = CeleryApp.celery(task_always_eager: true, task_eager_propagates: true)

    assert_raise RuntimeError, "No input files provided for the strings task.", fn ->
      Celery.send_task(app, Tasks.task_name(),
        kwargs: %{
          "input_files" => [],
          "output_path" => tmp_dir,
          "workflow_id" => "wf-empty",
          "task_config" => %{"ASCII" => true}
        }
      )
    end

    # When strings runs on an input file (e.g. UTF16LE on an ASCII file) and outputs 0 bytes,
    # the task succeeds with output_files: [] (triggering OpenRelik UI's "No output" badge)
    # and leaves no 0-byte files on disk.
    input_path = Path.join(tmp_dir, "no_matches.bin")
    File.write!(input_path, <<0, 1, 2>>)
    empty_out_dir = Path.join(tmp_dir, "empty_out")
    File.mkdir_p!(empty_out_dir)
    System.put_env("STRINGS_SHIM_EMPTY_ENC", "all")

    {:ok, res_empty} =
      Celery.send_task(app, Tasks.task_name(),
        kwargs: %{
          "input_files" => [%{"display_name" => "no_matches.bin", "path" => input_path}],
          "output_path" => empty_out_dir,
          "workflow_id" => "wf-empty-2",
          "task_config" => %{"ASCII" => nil, "UTF16LE" => true}
        }
      )

    decoded_empty = res_empty |> Celery.Result.get() |> TaskUtils.decode_base64_to_dict()
    assert decoded_empty["output_files"] == []
    assert decoded_empty["workflow_id"] == "wf-empty-2"
    assert File.ls!(empty_out_dir) == []
  end

  @tag :tmp_dir
  test "strings task raises RuntimeError and removes partial output when the subprocess exits non-zero",
       %{tmp_dir: tmp_dir} do
    System.put_env("STRINGS_SHIM_EXIT", "3")

    input_path = Path.join(tmp_dir, "failing.bin")
    File.write!(input_path, "partially extractable data\n")

    output_dir = Path.join(tmp_dir, "out_fail")
    File.mkdir_p!(output_dir)

    app = CeleryApp.celery(task_always_eager: true, task_eager_propagates: true)

    assert_raise RuntimeError, ~r/exited with non-zero status 3/, fn ->
      Celery.send_task(app, Tasks.task_name(),
        kwargs: %{
          "input_files" => [%{"display_name" => "failing.bin", "path" => input_path}],
          "output_path" => output_dir,
          "workflow_id" => "wf-fail",
          "task_config" => %{"ASCII" => true}
        }
      )
    end

    # The truncated output file written before the failure must have been removed.
    assert File.ls!(output_dir) == []
  end

  @tag :tmp_dir
  test "Celery.Worker consumes and executes strings task over memory broker", %{tmp_dir: tmp_dir} do
    input_path = Path.join(tmp_dir, "worker_input.bin")
    File.write!(input_path, "worker test data\n")
    output_dir = Path.join(tmp_dir, "worker_out")
    File.mkdir_p!(output_dir)

    app =
      CeleryApp.celery(
        broker: "memory://",
        backend: nil
      )

    {:ok, worker} =
      CeleryApp.start_worker(
        app: app,
        concurrency: 1,
        poll_interval: 10,
        notify: self()
      )

    on_exit(fn ->
      if Process.alive?(worker), do: Celery.Worker.stop(worker)
    end)

    {:ok, sent} =
      Celery.send_task(app, Tasks.task_name(),
        kwargs: %{
          "pipe_result" => nil,
          "input_files" => [%{"display_name" => "worker_input.bin", "path" => input_path}],
          "output_path" => output_dir,
          "workflow_id" => "wf-worker-1",
          "task_config" => %{"ASCII" => true}
        }
      )

    task_id = sent.id
    assert_receive {:celery_task_done, ^task_id, {:ok, encoded_result}}, 5_000

    decoded = TaskUtils.decode_base64_to_dict(encoded_result)
    assert decoded["workflow_id"] == "wf-worker-1"
    assert [out_file] = decoded["output_files"]
    assert out_file["display_name"] == "worker_input.bin.ASCII_strings"
    assert File.exists?(out_file["path"])
  end

  @tag :tmp_dir
  test "worker_child_spec configures graceful shutdown timeout from WORKER_SHUTDOWN_TIMEOUT_MS" do
    on_exit(fn ->
      System.delete_env("WORKER_SHUTDOWN_TIMEOUT_MS")
    end)

    System.delete_env("WORKER_SHUTDOWN_TIMEOUT_MS")
    assert CeleryApp.shutdown_timeout_ms() == 30_000
    assert CeleryApp.worker_child_spec().shutdown == 30_000

    System.put_env("WORKER_SHUTDOWN_TIMEOUT_MS", "60000")
    assert CeleryApp.shutdown_timeout_ms() == 60_000
    assert CeleryApp.worker_child_spec().shutdown == 60_000

    System.put_env("WORKER_SHUTDOWN_TIMEOUT_MS", "infinity")
    assert CeleryApp.shutdown_timeout_ms() == :infinity
    assert CeleryApp.worker_child_spec().shutdown == :infinity

    assert CeleryApp.worker_child_spec(shutdown: 15_000).shutdown == 15_000
  end
end
