defmodule OpenrelikWorkerStrings.Events do
  @moduledoc """
  Celery event emission plumbing for OpenRelik workers, extracted from
  `OpenrelikWorkerStrings.Tasks` so task modules only call `send_event/3` (matching
  `self.send_event(...)` in Python Celery).

  ## Event Flow

  1. `send_event/3` builds a Celery event payload (`type`, `uuid`, `hostname`,
     `timestamp`, `utcoffset`, `pid`) merged with the caller's `fields`.
  2. `notify_event_handler/2` notifies an optional local listener registered
     via the `:event_handler` application config (pid or function; used by
     tests).
  3. `maybe_publish_celery_event/3` publishes the event to the Celery event
     exchange when running against a live broker (`is_eager: false`).

  On Redis brokers, `Celery.Events.exchange/1` publishes over the `celeryev`
  fanout exchange (`/0.celeryev/task.progress`) so both OpenRelik Mediator and
  Flower receive real-time task progress updates.
  """

  require Logger

  alias OpenrelikWorkerStrings.Config

  @doc """
  Emits a Celery task event (such as `"task-progress"`) matching
  `self.send_event(...)` in Python Celery.
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

  defp extract_task_id(%{request: %{id: id}}), do: id
  defp extract_task_id(_), do: nil

  # Notifies any local test listener registered via `:event_handler` application env.
  defp notify_event_handler(event_type, event) do
    case Config.event_handler() do
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
