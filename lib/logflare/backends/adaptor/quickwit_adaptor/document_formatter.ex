defmodule Logflare.Backends.Adaptor.QuickwitAdaptor.DocumentFormatter do
  @moduledoc """
  Middleware encoding `Logflare.LogEvent`s as the NDJSON documents Quickwit ingests.

  All sources of a backend share one index, so every document carries the token
  of its source in `#{Logflare.Backends.Adaptor.QuickwitAdaptor.Query.source_field()}`;
  queries filter on it.

  Sets the `content-type` header, so it must be used with `json: false` in
  `Logflare.Backends.Adaptor.HttpBased.Client.new/1` options.
  """

  @behaviour Tesla.Middleware

  require Logger

  alias Logflare.Backends.Adaptor.QuickwitAdaptor.Query
  alias Logflare.LogEvent

  @content_type "application/x-ndjson"

  @impl Tesla.Middleware
  def call(env, next, _opts) do
    case encode(env.body) do
      [] when env.body != [] ->
        {:error, :all_events_dropped}

      encoded ->
        body = if is_list(encoded), do: IO.iodata_to_binary(encoded), else: encoded

        headers =
          Enum.reject(env.headers, fn {key, _value} -> String.downcase(key) == "content-type" end)

        %{env | headers: headers}
        |> Tesla.put_header("content-type", @content_type)
        |> Tesla.put_body(body)
        |> Tesla.run(next)
    end
  end

  @spec reserved_headers() :: [String.t()]
  def reserved_headers, do: ["content-type"]

  @doc """
  Encodes log events as newline-delimited JSON documents. Events whose body
  cannot be JSON-encoded are dropped. Any other term passes through unchanged.
  """
  @spec encode([LogEvent.t()] | term()) :: iodata() | term()
  def encode([%LogEvent{} | _] = events) do
    {encoded, dropped} =
      Enum.reduce(events, {[], 0}, fn %LogEvent{} = event, {acc, dropped} ->
        case Jason.encode_to_iodata(document(event)) do
          {:ok, iodata} -> {[iodata | acc], dropped}
          {:error, _reason} -> {acc, dropped + 1}
        end
      end)

    if dropped > 0 do
      Logger.warning("Dropped #{dropped} log events from a Quickwit batch: JSON encoding failed")
    end

    encoded |> Enum.reverse() |> Enum.intersperse("\n")
  end

  def encode(term), do: term

  @spec document(LogEvent.t()) :: map()
  def document(%LogEvent{body: body, source_uuid: source_uuid}) do
    Map.put(body, Query.source_field(), to_string(source_uuid))
  end
end
