defmodule Logflare.Backends.Adaptor.QuickwitAdaptor do
  @moduledoc """
  An adaptor storing logs in [Quickwit](https://quickwit.io), the scalable self-hosted option.

  ## Ingestion

  Log events are sent as NDJSON documents to the Quickwit ingest API
  (`POST /api/v1/{index_id}/ingest`) using the shared `HttpBased.Pipeline`.

  ## Querying

  SQL endpoint queries are translated to the
  [Quickwit query language](https://quickwit.io/docs/query-language/query-language) and executed
  against the search API (`POST /api/v1/{index_id}/search`). See
  `Logflare.Backends.Adaptor.QuickwitAdaptor.Query` for the supported SQL subset.

  ## Configuration

  - `:endpoint` - Base URL of the Quickwit REST API, e.g. `http://quickwit:7280`.
  Defaults to `http://localhost:7280`.
  - `:index_id` - Id of the Quickwit index used for log ingest and search.
  - `:api_key` - Optional bearer token when the Quickwit instance requires authentication.
  """

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.Adaptor.QuickwitAdaptor.Query
  alias Logflare.Backends.Backend
  alias Logflare.Backends.QueryError
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.Utils
  alias Ecto.Changeset

  require Logger

  @behaviour Adaptor
  @behaviour HttpBased.Client

  @default_endpoint "http://localhost:7280"
  @api_base "/api/v1"

  @impl Adaptor
  def child_spec(init_arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [init_arg]}
    }
  end

  @impl Adaptor
  def start_link({source, backend}) do
    HttpBased.Pipeline.start_link(source, backend, __MODULE__)
  end

  @impl Adaptor
  def cast_config(params, existing_config \\ %{}) do
    types = %{
      endpoint: :string,
      index_id: :string,
      api_key: :string
    }

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
  end

  @impl Adaptor
  def redact_config(config) do
    if Map.get(config, :api_key) do
      Map.put(config, :api_key, "REDACTED")
    else
      config
    end
  end

  @impl Adaptor
  def sanitize_config_for_display(config) do
    Adaptor.mask_config_values(config, except: [:endpoint, :index_id])
  end

  @impl HttpBased.Client
  def client_opts(%Backend{config: config}) do
    url = base_url(config) <> ingest_path(config)

    formatter = [
      {HttpBased.LogEventTransformer, transform_fn: &transform_timestamp/1},
      {HttpBased.NdjsonFormatter, content_type: "application/x-ndjson"}
    ]

    [
      formatter: formatter,
      json: false,
      gzip: true,
      url: url,
      token: config[:api_key]
    ]
  end

  # Logflare timestamps are unix microseconds; Quickwit expects RFC3339 datetimes
  # on its timestamp field by default.
  defp transform_timestamp(%{"timestamp" => timestamp} = body) when is_integer(timestamp) do
    Map.put(body, "timestamp", DateTime.from_unix!(timestamp, :microsecond) |> DateTime.to_iso8601())
  end

  defp transform_timestamp(body), do: body

  @impl Adaptor
  @spec test_connection(Backend.t()) :: :ok | {:error, atom() | String.t()}
  def test_connection(%Backend{} = backend) do
    url = base_url(backend.config) <> index_path(backend.config)

    client = HttpBased.Client.new(url: url, token: backend.config[:api_key])

    case Tesla.get(client, "") do
      {:ok, %Tesla.Env{status: 200}} ->
        :ok

      {:ok, %Tesla.Env{status: 404}} ->
        {:error, "Index #{backend.config.index_id} not found"}

      {:ok, %Tesla.Env{status: status, body: body}} when status in [401, 403] ->
        {:error, "Unauthorized: #{inspect(body)}"}

      {:ok, %Tesla.Env{status: status, body: body}} ->
        {:error, "Unexpected response: #{status} #{inspect(body)}"}

      {:error, reason} ->
        {:error, "Request error: #{inspect(reason)}"}
    end
  end

  @doc """
  Executes a Quickwit search for an endpoint query.

  The SQL query is translated to the Quickwit query language, then posted to the
  search API. Rows are the returned hit documents; when the query selects specific
  fields, hits are projected down to those fields.
  """
  @impl Adaptor
  def execute_query(%Backend{} = backend, query_string, opts)
      when is_binary(query_string) and is_list(opts) do
    search(backend, query_string, %{})
  end

  def execute_query(%Backend{} = backend, {query_string, params}, opts)
      when is_list(params) and is_list(opts) do
    search(backend, query_string, params)
  end

  def execute_query(%Backend{} = backend, {query_string, params}, opts)
      when is_map(params) and is_list(opts) do
    search(backend, query_string, params)
  end

  def execute_query(%Backend{} = backend, {query_string, _declared_params, input_params}, opts)
      when is_map(input_params) and is_list(opts) do
    search(backend, query_string, input_params)
  end

  def execute_query(
        %Backend{} = backend,
        {query_string, _declared_params, input_params, endpoint_query},
        opts
      )
      when is_map(input_params) and is_list(opts) do
    search(backend, query_string, input_params, endpoint_query)
  end

  @spec search(Backend.t(), String.t(), map() | list(), EndpointQuery.t() | nil) ::
          {:ok, QueryResult.t()} | {:error, QueryError.t()}
  defp search(backend, sql, params, endpoint_query \\ nil) do
    language =
      case endpoint_query do
        %Logflare.Endpoints.EndpointQuery{language: language} when language != nil ->
          language

        _ ->
          :bq_sql
      end

    with {:ok, {query_string, search_opts}} <- Query.to_search(language, sql, params) do
      body =
        %{
          query: query_string,
          max_hits: Keyword.fetch!(search_opts, :max_hits)
        }
        |> maybe_put_sort_by(search_opts)
        |> maybe_put_aggs(search_opts)

      do_search(backend, body, search_opts)
    else
      {:error, reason} ->
        {:error, query_error(:invalid_query, reason)}
    end
  end

  defp maybe_put_sort_by(body, search_opts) do
    case Keyword.get(search_opts, :sort_by) do
      nil -> body
      sort_by -> Map.put(body, :sort_by, sort_by)
    end
  end

  defp maybe_put_aggs(body, search_opts) do
    case Keyword.get(search_opts, :aggs) do
      nil -> body
      aggs -> Map.put(body, "aggs", aggs)
    end
  end

  defp do_search(backend, body, search_opts) do
    url = base_url(backend.config) <> search_path(backend.config)
    client = HttpBased.Client.new(url: url, token: backend.config[:api_key], json: true)

    case Tesla.post(client, body) do
      {:ok, %Tesla.Env{status: 200, body: response}} ->
        shape_result(response, body, search_opts)

      {:ok, %Tesla.Env{status: status, body: body}} ->
        {:error,
         query_error(:backend_error, "Quickwit search failed with status #{status}: #{inspect(body)}")}

      {:error, reason} ->
        {:error, query_error(:connection_error, "Request error: #{inspect(reason)}")}
    end
    |> log_query_error(backend)
  end

  defp shape_result(response, body, search_opts) do
    case Keyword.get(search_opts, :result_shape, :rows) do
      :rows -> rows_result(response, body, search_opts)
      :total -> total_result(response, search_opts)
      :chart -> chart_result(response, search_opts)
    end
  end

  defp rows_result(%{"hits" => hits} = response, body, search_opts) do
    rows = project_rows(hits, Keyword.get(search_opts, :fields, :all))

    {:ok,
     QueryResult.new(rows, %{
       total_rows: Map.get(response, "num_hits", length(rows)),
       query_string: body[:query]
     })}
  end

  defp rows_result(response, _body, _search_opts),
    do: {:error, query_error(:backend_error, "Unexpected search response: #{inspect(response)}")}

  defp total_result(%{"aggregations" => %{"total_count" => %{"value" => value}}}, search_opts) do
    column = Keyword.get(search_opts, :total_alias, "count")
    rows = [%{column => value}]

    {:ok, QueryResult.new(rows, %{total_rows: length(rows), query_string: "count(*)"})}
  end

  defp total_result(response, _search_opts),
    do: {:error, query_error(:backend_error, "Unexpected aggregation response: #{inspect(response)}")}

  defp chart_result(%{"aggregations" => aggs}, search_opts) do
    counters = Keyword.get(search_opts, :counters, [])

    case aggs do
      %{"buckets" => %{"buckets" => buckets}} when is_list(buckets) ->
        rows = Enum.map(buckets, &bucket_row(&1, counters))

        rows =
          if Keyword.get(search_opts, :bucket_order, :asc) == :desc do
            Enum.reverse(rows)
          else
            rows
          end

        {:ok,
         QueryResult.new(rows, %{total_rows: length(rows), query_string: "date_histogram"})}

      _ ->
        {:error,
         query_error(:backend_error, "Unexpected aggregation response: #{inspect(aggs)}")}
    end
  end

  defp chart_result(response, _search_opts),
    do: {:error, query_error(:backend_error, "Unexpected aggregation response: #{inspect(response)}")}

  defp bucket_row(bucket, counters) do
    row = %{"timestamp" => bucket_key_to_micros(Map.get(bucket, "key"))}

    Enum.reduce(counters, row, fn {column, spec}, row ->
      Map.put(row, column, counter_value(spec, bucket))
    end)
  end

  # date_histogram buckets are keyed in epoch milliseconds with the
  # "epoch_millis" format; Logflare rows carry unix microseconds.
  defp bucket_key_to_micros(key) when is_integer(key), do: key * 1_000

  defp bucket_key_to_micros(key) when is_binary(key) do
    case Integer.parse(key) do
      {millis, ""} -> millis * 1_000
      _ -> key
    end
  end

  defp counter_value({:static, boolean}, bucket) do
    if boolean, do: Map.get(bucket, "doc_count", 0), else: 0
  end

  defp counter_value({:field, term_name, field_path, condition}, bucket) do
    case Map.get(bucket, term_name) do
      %{"buckets" => sub_buckets} when is_list(sub_buckets) ->
        Enum.sum(
          for %{"key" => key, "doc_count" => count} <- sub_buckets,
              Query.condition_matches?(condition, field_path, key),
              do: count
        )

      _ ->
        0
    end
  end

  # SELECT projection is applied client-side since Quickwit returns full documents.
  defp project_rows(hits, :all), do: Enum.map(hits, &convert_timestamp_value/1)

  defp project_rows(hits, fields) when is_list(fields) do
    for hit <- hits do
      Map.new(fields, fn {column, path} ->
        if path == ["timestamp"] do
          {column, convert_timestamp_value(hit_value(hit, path))}
        else
          {column, hit_value(hit, path)}
        end
      end)
    end
  end

  defp hit_value(hit, [single]) when is_map(hit), do: Map.get(hit, single)
  defp hit_value(hit, path) when is_map(hit), do: get_in(hit, path)
  defp hit_value(_hit, _path), do: nil

  # Quickwit returns RFC3339 datetimes on the timestamp field; Logflare rows
  # carry unix microseconds.
  defp convert_timestamp_value(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> DateTime.to_unix(datetime, :microsecond)
      _ -> value
    end
  end

  defp convert_timestamp_value(value), do: value

  defp project_rows(hits, fields) when is_list(fields) do
    for hit <- hits do
      Map.new(fields, fn
        {field, alias} -> {alias, hit[field]}
        field -> {field, hit[field]}
      end)
    end
  end

  @spec query_error(QueryError.kind(), String.t()) :: QueryError.t()
  defp query_error(kind, raw_error) do
    %QueryError{
      kind: kind,
      raw_error: raw_error,
      backend: __MODULE__,
      description: if(is_binary(raw_error), do: raw_error)
    }
  end

  @spec log_query_error({:ok, QueryResult.t()} | {:error, QueryError.t()}, Backend.t()) ::
          {:ok, QueryResult.t()} | {:error, QueryError.t()}
  defp log_query_error({:error, %QueryError{} = error} = result, %Backend{} = backend) do
    QueryError.log(error,
      user_id: backend.user_id,
      backend_id: backend.id,
      backend_token: backend.token
    )

    result
  end

  defp log_query_error(result, _backend), do: result

  defp base_url(config), do: String.trim_trailing(config.endpoint, "/")
  defp ingest_path(config), do: "#{@api_base}/#{config.index_id}/ingest"
  defp index_path(config), do: "#{@api_base}/#{config.index_id}"
  defp search_path(config), do: "#{@api_base}/#{config.index_id}/search"
end