defmodule Logflare.Backends.Adaptor.QuickwitAdaptor do
  @moduledoc """
  An adaptor storing and querying logs in [Quickwit](https://quickwit.io).

  ## Ingestion

  Log events are sent as NDJSON documents to the Quickwit ingest API
  (`POST /api/v1/{index_id}/ingest`) through the shared `HttpBased.Pipeline`.
  All sources attached to the backend write to the same index; each document
  carries its source token in `lf_source`. The index is created on first use
  with a dynamic mapping, so any event shape is accepted.

  ## Querying

  Quickwit has no SQL interface. Endpoint queries are translated into requests
  for its Elasticsearch-compatible search API
  (`POST /api/v1/_elastic/{index_id}/_search`), see
  `Logflare.Backends.Adaptor.QuickwitAdaptor.Query` for the supported SQL.

  ## Configuration

  - `:endpoint` - Base URL of the Quickwit REST API, e.g. `http://quickwit:7280`.
  - `:index_id` - Id of the Quickwit index used for ingest and search.
  - `:api_key` - Optional bearer token when Quickwit sits behind authentication.
  """

  @behaviour Logflare.Backends.Adaptor
  @behaviour Logflare.Backends.Adaptor.HttpBased.Client

  require Logger

  alias Ecto.Changeset
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.Adaptor.QuickwitAdaptor.DocumentFormatter
  alias Logflare.Backends.Adaptor.QuickwitAdaptor.Query
  alias Logflare.Backends.Backend
  alias Logflare.Backends.QueryError
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.Utils

  @default_endpoint "http://localhost:7280"
  @api_base "/api/v1"
  @histogram "buckets"
  @terms "values"
  @max_concurrent_searches 4
  @search_timeout 30_000

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Adaptor
  def start_link({source, backend}) do
    with {:error, reason} <- ensure_index(backend) do
      Logger.warning("Quickwit index #{backend.config.index_id} is not ready: #{inspect(reason)}",
        backend_id: backend.id
      )
    end

    HttpBased.Pipeline.start_link(source, backend, __MODULE__)
  end

  @impl Adaptor
  def cast_config(params, existing_config \\ %{}) do
    types = %{endpoint: :string, index_id: :string, api_key: :string}

    {existing_config, types}
    |> Changeset.cast(params, Map.keys(types))
    |> Utils.default_field_value(:endpoint, @default_endpoint)
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Changeset.validate_required([:endpoint, :index_id])
    |> Changeset.validate_format(:endpoint, ~r/^https?:\/\//,
      message: "must be a valid http(s) URL, e.g. http://quickwit:7280"
    )
    |> Changeset.validate_format(:index_id, ~r/^[a-zA-Z][a-zA-Z0-9_\-]{2,254}$/,
      message:
        "must start with a letter and contain only letters, digits, - and _ (3 to 255 characters)"
    )
  end

  @impl Adaptor
  def redact_config(config) do
    if Map.get(config, :api_key), do: Map.put(config, :api_key, "REDACTED"), else: config
  end

  @impl Adaptor
  def sanitize_config_for_display(config) do
    Adaptor.mask_config_values(config, except: [:endpoint, :index_id])
  end

  @impl Adaptor
  def supports_default_ingest?, do: true

  @impl HttpBased.Client
  def client_opts(%Backend{config: config}) do
    [
      url: base_url(config) <> "#{@api_base}/#{config.index_id}/ingest",
      formatter: DocumentFormatter,
      json: false,
      gzip: true,
      http2: false,
      token: config[:api_key]
    ]
  end

  @impl Adaptor
  @spec test_connection(Backend.t()) :: :ok | {:error, String.t()}
  def test_connection(%Backend{} = backend), do: ensure_index(backend)

  @doc """
  Creates the backend's index when it does not exist yet.
  """
  @spec ensure_index(Backend.t()) :: :ok | {:error, String.t()}
  def ensure_index(%Backend{config: config}) do
    case Tesla.get(api_client(config), "/indexes/#{config.index_id}") do
      {:ok, %Tesla.Env{status: 200}} -> :ok
      {:ok, %Tesla.Env{status: 404}} -> create_index(config)
      other -> request_error(other)
    end
  end

  defp create_index(config) do
    case Tesla.post(api_client(config), "/indexes", index_config(config.index_id)) do
      {:ok, %Tesla.Env{status: 200}} ->
        :ok

      # lost a race against another pipeline of the same backend
      {:ok, %Tesla.Env{status: 400, body: %{"message" => message}}} when is_binary(message) ->
        if message =~ "already exist", do: :ok, else: {:error, message}

      other ->
        request_error(other)
    end
  end

  defp request_error({:ok, %Tesla.Env{status: status, body: body}}) when status in [401, 403],
    do: {:error, "Unauthorized: #{inspect(body)}"}

  defp request_error({:ok, %Tesla.Env{status: status, body: body}}),
    do: {:error, "Unexpected response: #{status} #{inspect(body)}"}

  defp request_error({:error, reason}), do: {:error, "Request error: #{inspect(reason)}"}

  @doc """
  The Quickwit index configuration for Logflare events.

  Unknown fields are indexed as raw tokens (exact match) and stored as fast
  fields, so they can be filtered, sorted and aggregated on. `event_message`
  is tokenized for phrase search.
  """
  @spec index_config(String.t()) :: map()
  def index_config(index_id) do
    %{
      version: "0.8",
      index_id: index_id,
      doc_mapping: %{
        mode: "dynamic",
        dynamic_mapping: %{
          indexed: true,
          stored: true,
          tokenizer: "raw",
          record: "basic",
          expand_dots: true,
          fast: true
        },
        field_mappings: [
          %{
            name: "timestamp",
            type: "datetime",
            input_formats: ["unix_timestamp", "rfc3339"],
            output_format: "unix_timestamp_micros",
            fast_precision: "microseconds",
            fast: true
          },
          %{name: Query.source_field(), type: "text", tokenizer: "raw", fast: true},
          %{name: "id", type: "text", tokenizer: "raw"},
          %{name: "event_message", type: "text", tokenizer: "default", record: "position"}
        ],
        timestamp_field: "timestamp"
      },
      search_settings: %{default_search_fields: ["event_message"]},
      indexing_settings: %{commit_timeout_secs: 5}
    }
  end

  @impl Adaptor
  def execute_query(%Backend{} = backend, query, opts) when is_list(opts) do
    {sql, params, language} = query_args(query)

    with {:ok, plan} <- to_plan(language, sql, params),
         {:ok, rows} <- run(plan, backend.config) do
      {:ok, QueryResult.new(rows, %{total_rows: length(rows)})}
    end
    |> log_query_error(backend)
  end

  defp query_args({sql, _declared, params, %EndpointQuery{language: language}})
       when is_map(params),
       do: {sql, params, language || :bq_sql}

  defp query_args({sql, _declared, params}) when is_map(params), do: {sql, params, :bq_sql}
  defp query_args({sql, params}) when is_map(params), do: {sql, params, :bq_sql}
  defp query_args({sql, _params}), do: {sql, %{}, :bq_sql}
  defp query_args(sql) when is_binary(sql), do: {sql, %{}, :bq_sql}

  defp to_plan(language, sql, params) do
    with {:error, reason} <- Query.to_plan(language, sql, params) do
      {:error, query_error(:invalid_query, reason)}
    end
  end

  @spec run(Query.plan(), map()) :: {:ok, [map()]} | {:error, QueryError.t()}
  defp run(%{shape: :rows} = plan, config) do
    body =
      %{query: plan.query, size: plan.size, from: plan.from}
      |> then(&if plan.sort == [], do: &1, else: Map.put(&1, :sort, plan.sort))

    with {:ok, %{"hits" => %{"hits" => hits}}} <- search(config, body) do
      {:ok, Enum.map(hits, &project(Map.get(&1, "_source", %{}), plan.columns))}
    end
  end

  defp run(%{shape: :counts, counters: counters}, config) do
    with {:ok, totals} <-
           each_query(counters, config, fn query ->
             %{query: query, size: 0, track_total_hits: true}
           end) do
      row =
        Map.new(counters, fn {name, query} ->
          {name, get_in(totals, [query, "hits", "total", "value"]) || 0}
        end)

      {:ok, [row]}
    end
  end

  defp run(%{shape: :histogram} = plan, config) do
    aggregation = %{
      @histogram => %{date_histogram: %{field: "timestamp", fixed_interval: plan.interval}}
    }

    with {:ok, results} <-
           aggregate([{plan.column, plan.query} | plan.counters], config, aggregation) do
      counts = &bucket_counts(results[&1], @histogram, fn key -> trunc(key) end)
      keys = for {key, total} <- Enum.sort(counts.(plan.query)), total > 0, do: key
      rows = counter_rows(plan, counts, keys, &(&1 * 1_000))

      {:ok, if(plan.order == :desc, do: Enum.reverse(rows), else: rows)}
    end
  end

  defp run(%{shape: :terms} = plan, config) do
    aggregation = %{@terms => %{terms: %{field: plan.field, size: plan.size}}}

    with {:ok, results} <- aggregate(plan.counters, config, aggregation) do
      counts = &bucket_counts(results[&1], @terms, fn key -> key end)

      keys =
        plan.counters
        |> Enum.flat_map(fn {_name, query} -> Map.keys(counts.(query)) end)
        |> Enum.uniq()

      {:ok, counter_rows(plan, counts, keys, & &1)}
    end
  end

  # One search per distinct filter, each with the same nested bucket aggregations. The base
  # query decides which groups exist; filtered metrics default to 0 (counts) or null.
  defp run(%{shape: :aggregate} = plan, config) do
    queries = Enum.uniq([plan.query | Enum.map(plan.metrics, fn {_, _, _, query} -> query end)])

    with {:ok, results} <-
           each_query(Enum.map(queries, &{nil, &1}), config, &aggregate_body(plan, &1)) do
      groups = Map.new(queries, fn query -> {query, groups(results[query], plan.keys)} end)

      rows = for {key, _base} <- groups[plan.query], do: aggregate_row(plan, groups, key)

      {:ok, rows |> order_rows(plan.order) |> Enum.take(plan.limit)}
    end
  end

  defp aggregate_row(plan, groups, key) do
    values = Map.new(plan.metrics, &metric_value(&1, groups, key))
    Map.new(plan.columns, fn {name, expr} -> {name, evaluate(expr, key, values, plan.keys)} end)
  end

  defp aggregate_body(plan, query) do
    leaves =
      for {id, kind, field, ^query} <- plan.metrics, kind != :count, into: %{} do
        {id, metric_aggregation(kind, field, plan.size)}
      end

    aggregations =
      plan.keys
      |> Enum.with_index()
      |> Enum.reverse()
      |> Enum.reduce(leaves, fn {{_name, key}, index}, inner ->
        bucket = key_aggregation(key, plan.size)
        bucket = if inner == %{}, do: bucket, else: Map.put(bucket, :aggs, inner)
        %{"k#{index}" => bucket}
      end)

    body = %{query: query, size: 0, track_total_hits: true}
    if aggregations == %{}, do: body, else: Map.put(body, :aggs, aggregations)
  end

  defp key_aggregation({:bucket, interval, _format}, _size),
    do: %{date_histogram: %{field: "timestamp", fixed_interval: interval}}

  defp key_aggregation({:term, field}, size), do: %{terms: %{field: field, size: size}}

  # Quickwit has no cardinality aggregation: distinct values are counted as term buckets.
  defp metric_aggregation(:distinct, field, size), do: %{terms: %{field: field, size: size}}
  defp metric_aggregation(kind, field, _size), do: %{kind => %{field: field}}

  # Flattens nested buckets into `%{[key values] => leaf}`; empty buckets are not groups.
  @spec groups(map(), [term()]) :: %{[term()] => map()}
  defp groups(response, []) do
    count = get_in(response, ["hits", "total", "value"]) || 0
    %{[] => Map.put(response["aggregations"] || %{}, "doc_count", count)}
  end

  defp groups(response, keys), do: flatten(response["aggregations"] || %{}, 0, length(keys), [])

  defp flatten(leaf, depth, depth, path), do: %{Enum.reverse(path) => leaf}

  defp flatten(node, index, depth, path) do
    for %{"key" => key, "doc_count" => count} = bucket <-
          get_in(node, ["k#{index}", "buckets"]) || [],
        count > 0,
        reduce: %{} do
      acc -> Map.merge(acc, flatten(bucket, index + 1, depth, [key | path]))
    end
  end

  defp metric_value({id, :count, _field, query}, groups, key),
    do: {id, get_in(groups, [query, key, "doc_count"]) || 0}

  defp metric_value({id, :distinct, _field, query}, groups, key),
    do: {id, length(get_in(groups, [query, key, id, "buckets"]) || [])}

  defp metric_value({id, _kind, _field, query}, groups, key),
    do: {id, get_in(groups, [query, key, id, "value"])}

  defp evaluate({:metric, id}, _key, values, _keys), do: values[id]
  defp evaluate({:const, value}, _key, _values, _keys), do: value

  defp evaluate({:key, index}, key, _values, keys) do
    {_name, kind} = Enum.at(keys, index)
    key_value(kind, Enum.at(key, index))
  end

  defp evaluate({:call, fun, args}, key, values, keys),
    do: call(fun, Enum.map(args, &evaluate(&1, key, values, keys)))

  defp key_value({:bucket, _interval, :datetime}, millis),
    do: (trunc(millis) * 1_000) |> to_datetime() |> String.trim_trailing("Z")

  defp key_value({:bucket, _interval, _format}, millis), do: trunc(millis) * 1_000
  # numeric terms come back as floats
  defp key_value({:term, _field}, value) when is_float(value) do
    whole = trunc(value)
    if whole == value, do: whole, else: value
  end

  defp key_value({:term, _field}, value), do: value

  defp call(:coalesce, values), do: Enum.find(values, &(not is_nil(&1)))
  defp call(:round, [value]) when is_number(value), do: round(value)

  defp call(:round, [value, digits]) when is_float(value) and is_integer(digits),
    do: Float.round(value, digits)

  defp call(:round, [value | _]), do: value
  defp call(:divide, [a, b]) when is_number(a) and is_number(b) and b != 0, do: a / b
  defp call(:multiply, [a, b]) when is_number(a) and is_number(b), do: a * b
  defp call(:add, [a, b]) when is_number(a) and is_number(b), do: a + b
  defp call(:subtract, [a, b]) when is_number(a) and is_number(b), do: a - b
  defp call(_fun, _values), do: nil

  defp order_rows(rows, []), do: rows
  defp order_rows(rows, order), do: Enum.sort(rows, &ordered?(&1, &2, order))

  defp ordered?(_a, _b, []), do: true

  defp ordered?(a, b, [{name, direction} | rest]) do
    case {a[name], b[name]} do
      {same, same} -> ordered?(a, b, rest)
      {x, y} when direction == :asc -> x <= y
      {x, y} -> x >= y
    end
  end

  defp aggregate(counters, config, aggregation) do
    each_query(counters, config, &%{query: &1, size: 0, aggs: aggregation})
  end

  @spec bucket_counts(map() | nil, String.t(), (term() -> term())) :: %{term() => integer()}
  defp bucket_counts(response, name, key_fun) do
    for %{"key" => key, "doc_count" => count} <- buckets(response, name),
        into: %{},
        do: {key_fun.(key), count}
  end

  # One row per bucket key: the key under the plan's column, plus every counter's count for it.
  defp counter_rows(plan, counts, keys, value_fun) do
    per_counter = Enum.map(plan.counters, fn {name, query} -> {name, counts.(query)} end)

    for key <- keys do
      per_counter
      |> Map.new(fn {name, by_key} -> {name, Map.get(by_key, key, 0)} end)
      |> Map.put(plan.column, value_fun.(key))
    end
  end

  defp buckets(%{"aggregations" => aggregations}, name),
    do: get_in(aggregations, [name, "buckets"]) || []

  defp buckets(_response, _name), do: []

  # One search per distinct query, a few at a time.
  defp each_query(counters, config, body_fun) do
    counters
    |> Enum.map(fn {_name, query} -> query end)
    |> Enum.uniq()
    |> Task.async_stream(fn query -> {query, search(config, body_fun.(query))} end,
      max_concurrency: @max_concurrent_searches,
      timeout: @search_timeout,
      on_timeout: :kill_task
    )
    |> Enum.reduce_while({:ok, %{}}, fn
      {:ok, {query, {:ok, response}}}, {:ok, acc} -> {:cont, {:ok, Map.put(acc, query, response)}}
      {:ok, {_query, {:error, _} = error}}, _acc -> {:halt, error}
      {:exit, reason}, _acc -> {:halt, {:error, query_error(:timeout, inspect(reason))}}
    end)
  end

  defp search(config, body) do
    case Tesla.post(api_client(config), "/_elastic/#{config.index_id}/_search", body) do
      {:ok, %Tesla.Env{status: 200, body: %{} = response}} ->
        {:ok, response}

      {:ok, %Tesla.Env{status: status, body: response}} ->
        {:error,
         query_error(
           :backend_error,
           "Quickwit search failed with status #{status}: #{inspect(response)}"
         )}

      {:error, reason} ->
        {:error, query_error(:connection_error, "Request error: #{inspect(reason)}")}
    end
  end

  defp project(document, :all), do: Map.delete(document, Query.source_field())

  defp project(document, columns) do
    Map.new(columns, fn
      {name, {:path, path}} -> {name, get_in(document, path)}
      {name, {:datetime, path}} -> {name, document |> get_in(path) |> to_datetime()}
    end)
  end

  defp to_datetime(micros) when is_integer(micros),
    do: micros |> DateTime.from_unix!(:microsecond) |> DateTime.to_iso8601()

  defp to_datetime(other), do: other

  defp api_client(config) do
    HttpBased.Client.new(
      url: base_url(config) <> @api_base,
      token: config[:api_key],
      json: true,
      http2: false
    )
  end

  defp base_url(config), do: String.trim_trailing(config.endpoint, "/")

  @spec query_error(QueryError.kind(), String.t()) :: QueryError.t()
  defp query_error(kind, description) do
    %QueryError{kind: kind, raw_error: description, backend: __MODULE__, description: description}
  end

  defp log_query_error({:error, %QueryError{} = error} = result, %Backend{} = backend) do
    QueryError.log(error,
      user_id: backend.user_id,
      backend_id: backend.id,
      backend_token: backend.token
    )

    result
  end

  defp log_query_error(result, _backend), do: result
end
