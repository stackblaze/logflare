defmodule Logflare.Backends.Adaptor.QuickwitAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.QueryError
  alias Logflare.Backends.SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Tesla.MockAdapter

  @subject Adaptor.QuickwitAdaptor
  @tesla_adapter Tesla.Adapter.Finch

  @valid_config %{endpoint: "http://quickwit.local:7280", index_id: "logflare"}
  @valid_config_input Map.new(@valid_config, fn {k, v} -> {Atom.to_string(k), v} end)

  defp backend_data(_ctx) do
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        type: :quickwit,
        sources: [source],
        config: @valid_config
      )

    table = "`project.dataset.#{String.replace(to_string(source.token), "-", "_")}`"

    [backend: backend, source: source, table: table]
  end

  setup do
    # SourceSup's rate counter reads BigQuery at startup; stub it before starting the tree.
    stub(Logflare.Google.BigQuery, :get_table, fn _ -> {:error, :not_found} end)
    start_supervised!(AllLogsLogged)
    insert(:plan)
    :ok
  end

  describe "config typecast and validation" do
    test "enforces required options" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})
      refute changeset.valid?
      assert errors_on(changeset).index_id == ["can't be blank"]
    end

    test "sets default options" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, Map.delete(@valid_config_input, "endpoint"))

      assert changeset.valid?
      assert %{endpoint: "http://localhost:7280"} = Ecto.Changeset.apply_changes(changeset)
    end

    test "rejects non-http endpoints" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          @valid_config_input
          | "endpoint" => "ftp://quickwit.local"
        })

      refute changeset.valid?
      assert errors_on(changeset).endpoint
    end
  end

  describe "test_connection/1 and ensure_index/1" do
    setup :backend_data

    test "succeeds when the index exists", ctx do
      mock_adapter(fn env ->
        assert env.method == :get
        assert env.url == "http://quickwit.local:7280/api/v1/indexes/logflare"

        {:ok, %Tesla.Env{status: 200, body: %{"index_config" => %{"index_id" => "logflare"}}}}
      end)

      assert :ok = @subject.test_connection(ctx.backend)
    end

    test "creates the index when it is missing", ctx do
      this = self()

      mock_adapter(fn
        %{method: :get} ->
          {:ok, %Tesla.Env{status: 404, body: %{"message" => "index `logflare` not found"}}}

        %{method: :post} = env ->
          assert env.url == "http://quickwit.local:7280/api/v1/indexes"
          send(this, {:created, Jason.decode!(env.body)})
          {:ok, %Tesla.Env{status: 200, body: %{}}}
      end)

      assert :ok = @subject.ensure_index(ctx.backend)

      assert_receive {:created, %{"index_id" => "logflare", "doc_mapping" => mapping}}
      assert mapping["mode"] == "dynamic"
      assert mapping["timestamp_field"] == "timestamp"

      assert %{"tokenizer" => "raw", "fast" => true} =
               Enum.find(mapping["field_mappings"], &(&1["name"] == "lf_source"))
    end

    test "tolerates another pipeline creating the index first", ctx do
      mock_adapter(fn
        %{method: :get} ->
          {:ok, %Tesla.Env{status: 404, body: %{"message" => "not found"}}}

        %{method: :post} ->
          {:ok, %Tesla.Env{status: 400, body: %{"message" => "index `logflare` already exists"}}}
      end)

      assert :ok = @subject.ensure_index(ctx.backend)
    end

    test "returns the error when the index cannot be created", ctx do
      mock_adapter(fn
        %{method: :get} ->
          {:ok, %Tesla.Env{status: 404, body: %{"message" => "not found"}}}

        %{method: :post} ->
          {:ok, %Tesla.Env{status: 400, body: %{"message" => "invalid mapping"}}}
      end)

      assert {:error, "invalid mapping"} = @subject.test_connection(ctx.backend)
    end

    test "returns error when unauthorized", ctx do
      mock_adapter(fn _env -> {:ok, %Tesla.Env{status: 401, body: "denied"}} end)

      assert {:error, "Unauthorized" <> _} = @subject.test_connection(ctx.backend)
    end

    test "returns error on request failure", ctx do
      mock_adapter(fn _env -> {:error, :nxdomain} end)

      assert {:error, "Request error: :nxdomain"} = @subject.test_connection(ctx.backend)
    end
  end

  describe "logs ingestion" do
    setup :backend_data

    setup %{source: source} do
      this = self()

      mock_adapter(fn
        %{method: :get} ->
          {:ok, %Tesla.Env{status: 200, body: %{}}}

        %{method: :post} = env ->
          send(this, {:ingest, env})
          {:ok, %Tesla.Env{status: 200, body: ~s({"num_docs_for_processing":1})}}
      end)

      start_supervised!({SourceSup, source})
      :ok
    end

    test "sends logs as NDJSON documents tagged with their source", %{source: source} do
      log_event =
        build(:log_event,
          source: source,
          event_message: "Test log message",
          random_attribute: "nothing",
          timestamp: System.system_time(:microsecond)
        )

      assert {:ok, _} = Backends.ingest_logs([log_event], source)
      assert_receive {:ingest, env}, 5000

      assert Tesla.build_url(env) == "http://quickwit.local:7280/api/v1/logflare/ingest"
      assert Tesla.get_header(env, "content-type") == "application/x-ndjson"
      assert Tesla.get_header(env, "content-encoding") == "gzip"

      assert [document] =
               env.body
               |> :zlib.gunzip()
               |> String.split("\n", trim: true)
               |> Enum.map(&Jason.decode!/1)

      assert document["event_message"] == "Test log message"
      assert document["random_attribute"] == "nothing"
      assert document["timestamp"] == log_event.body["timestamp"]
      assert document["lf_source"] == to_string(source.token)
    end

    test "sends multiple log events as separate NDJSON lines", %{source: source} do
      log_events =
        build_list(3, :log_event, source: source, timestamp: System.system_time(:microsecond))

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {:ingest, env}, 5000
      assert [_, _, _] = env.body |> :zlib.gunzip() |> String.split("\n", trim: true)
    end
  end

  describe "execute_query/3" do
    setup :backend_data

    test "searches documents and projects the selected columns", ctx do
      source_token = to_string(ctx.source.token)

      mock_adapter(fn env ->
        assert env.method == :post
        assert env.url == "http://quickwit.local:7280/api/v1/_elastic/logflare/_search"

        assert %{
                 "size" => 2,
                 "from" => 0,
                 "sort" => [%{"timestamp" => %{"order" => "desc"}}],
                 "query" => %{
                   "bool" => %{
                     "must" => [
                       %{"term" => %{"lf_source" => %{"value" => ^source_token}}},
                       %{"term" => %{"metadata.level" => %{"value" => "error"}}}
                     ]
                   }
                 }
               } = Jason.decode!(env.body)

        hit = fn id, timestamp ->
          %{
            "_source" => %{
              "id" => id,
              "timestamp" => timestamp,
              "event_message" => "boom",
              "lf_source" => source_token,
              "metadata" => %{"level" => "error"}
            }
          }
        end

        {:ok,
         %Tesla.Env{
           status: 200,
           body: %{
             "hits" => %{
               "hits" => [hit.("b", 1_790_000_060_000_000), hit.("a", 1_790_000_000_000_000)]
             }
           }
         }}
      end)

      sql = """
      SELECT t.id, CAST(t.timestamp AS DATETIME) AS time, m.level AS level
      FROM #{ctx.table} t CROSS JOIN UNNEST(t.metadata) AS m
      WHERE m.level = @level
      ORDER BY t.timestamp DESC
      LIMIT 2
      """

      assert {:ok, %QueryResult{rows: rows, total_rows: 2}} =
               @subject.execute_query(ctx.backend, {sql, [], %{"level" => "error"}}, [])

      assert rows == [
               %{"id" => "b", "time" => "2026-09-21T14:14:20.000000Z", "level" => "error"},
               %{"id" => "a", "time" => "2026-09-21T14:13:20.000000Z", "level" => "error"}
             ]
    end

    test "SELECT * returns whole documents without the source tag", ctx do
      mock_adapter(fn _env ->
        {:ok,
         %Tesla.Env{
           status: 200,
           body: %{"hits" => %{"hits" => [%{"_source" => %{"id" => "a", "lf_source" => "x"}}]}}
         }}
      end)

      assert {:ok, %QueryResult{rows: [%{"id" => "a"}]}} =
               @subject.execute_query(ctx.backend, "SELECT * FROM #{ctx.table}", [])
    end

    test "counts with one search per distinct filter", ctx do
      mock_adapter(fn env ->
        assert %{"size" => 0, "track_total_hits" => true, "query" => query} =
                 Jason.decode!(env.body)

        total = if match?(%{"term" => _}, query), do: 7, else: 3

        {:ok,
         %Tesla.Env{
           status: 200,
           body: %{"hits" => %{"total" => %{"value" => total}, "hits" => []}}
         }}
      end)

      sql = """
      SELECT count(*) AS total, count(CASE WHEN level = 'error' THEN 1 END) AS errors
      FROM #{ctx.table}
      """

      assert {:ok, %QueryResult{rows: [%{"total" => 7, "errors" => 3}]}} =
               @subject.execute_query(ctx.backend, {sql, [], %{}}, [])
    end

    test "builds time buckets from a date histogram", ctx do
      mock_adapter(fn env ->
        assert %{
                 "size" => 0,
                 "aggs" => %{
                   "buckets" => %{
                     "date_histogram" => %{"field" => "timestamp", "fixed_interval" => "1m"}
                   }
                 }
               } = Jason.decode!(env.body)

        buckets = [
          %{"key" => 1_790_000_040_000.0, "doc_count" => 2},
          %{"key" => 1_790_000_100_000.0, "doc_count" => 0},
          %{"key" => 1_790_000_160_000.0, "doc_count" => 5}
        ]

        {:ok,
         %Tesla.Env{
           status: 200,
           body: %{"aggregations" => %{"buckets" => %{"buckets" => buckets}}}
         }}
      end)

      sql = """
      SELECT timestamp_trunc(t.timestamp, minute) AS timestamp, count(t.timestamp) AS count
      FROM #{ctx.table} t
      GROUP BY timestamp
      ORDER BY timestamp DESC
      """

      assert {:ok, %QueryResult{rows: rows}} =
               @subject.execute_query(ctx.backend, {sql, [], %{}}, [])

      assert rows == [
               %{"timestamp" => 1_790_000_160_000_000, "count" => 5},
               %{"timestamp" => 1_790_000_040_000_000, "count" => 2}
             ]
    end

    test "counts per field value from a terms aggregation", ctx do
      mock_adapter(fn env ->
        assert %{"aggs" => %{"values" => %{"terms" => %{"field" => "level"}}}} =
                 Jason.decode!(env.body)

        buckets = [%{"key" => "error", "doc_count" => 4}, %{"key" => "info", "doc_count" => 9}]

        {:ok,
         %Tesla.Env{
           status: 200,
           body: %{"aggregations" => %{"values" => %{"buckets" => buckets}}}
         }}
      end)

      sql = "SELECT level, count(*) AS count FROM #{ctx.table} GROUP BY level"

      assert {:ok, %QueryResult{rows: rows}} =
               @subject.execute_query(ctx.backend, {sql, [], %{}}, [])

      assert Enum.sort_by(rows, & &1["level"]) == [
               %{"level" => "error", "count" => 4},
               %{"level" => "info", "count" => 9}
             ]
    end

    test "runs grouped aggregates as nested bucket aggregations", ctx do
      mock_adapter(fn env ->
        body = Jason.decode!(env.body)

        assert %{
                 "k0" => %{
                   "date_histogram" => %{"fixed_interval" => "1h"},
                   "aggs" => %{"k1" => %{"terms" => %{"field" => "metadata.method"}} = inner}
                 }
               } = body["aggs"]

        filtered? = match?(%{"bool" => %{"must" => [_, %{"range" => _}]}}, body["query"])
        if not filtered?, do: assert(%{"aggs" => %{"m2" => %{"avg" => _}}} = inner)

        method = fn key, count, avg ->
          %{"key" => key, "doc_count" => count, "m2" => %{"value" => avg}}
        end

        methods =
          if filtered?,
            do: [method.("GET", 1, nil)],
            else: [method.("GET", 4, 1500.0), method.("POST", 2, nil)]

        buckets = [
          %{"key" => 1_790_000_000_000.0, "doc_count" => 6, "k1" => %{"buckets" => methods}},
          %{"key" => 1_790_003_600_000.0, "doc_count" => 0, "k1" => %{"buckets" => []}}
        ]

        {:ok,
         %Tesla.Env{status: 200, body: %{"aggregations" => %{"k0" => %{"buckets" => buckets}}}}}
      end)

      sql = """
      SELECT cast(timestamp_trunc(t.timestamp, hour) AS datetime) AS timestamp, m.method AS method,
             count(*) AS count, countif(m.status >= 500) AS errors, round(avg(m.duration) / 1000, 1) AS avg_s
      FROM #{ctx.table} t CROSS JOIN UNNEST(t.metadata) AS m
      GROUP BY timestamp, method
      ORDER BY count DESC
      """

      assert {:ok, %QueryResult{rows: rows}} =
               @subject.execute_query(ctx.backend, {sql, [], %{}}, [])

      assert rows == [
               %{
                 "timestamp" => "2026-09-21T14:13:20.000000",
                 "method" => "GET",
                 "count" => 4,
                 "errors" => 1,
                 "avg_s" => 1.5
               },
               %{
                 "timestamp" => "2026-09-21T14:13:20.000000",
                 "method" => "POST",
                 "count" => 2,
                 "errors" => 0,
                 "avg_s" => nil
               }
             ]
    end

    test "evaluates a computed key per group and merges equal values", ctx do
      mock_adapter(fn env ->
        assert %{"k0" => %{"terms" => %{"field" => "provider", "missing" => ""}}} =
                 Jason.decode!(env.body)["aggs"]

        methods = fn pairs ->
          %{"buckets" => for({key, count} <- pairs, do: %{"key" => key, "doc_count" => count})}
        end

        buckets = [
          %{"key" => "", "doc_count" => 5, "k1" => methods.([{"password", 3}, {"otp", 2}])},
          %{"key" => "github", "doc_count" => 4, "k1" => methods.([{"oauth", 4}])},
          %{"key" => "email", "doc_count" => 1, "k1" => methods.([{"password", 1}])}
        ]

        {:ok,
         %Tesla.Env{status: 200, body: %{"aggregations" => %{"k0" => %{"buckets" => buckets}}}}}
      end)

      sql = """
      SELECT CASE WHEN provider IS NOT NULL AND provider != '' AND provider != 'email'
                  THEN concat(method, ' (', provider, ')') ELSE method END AS label,
             count(*) AS count
      FROM #{ctx.table}
      GROUP BY label
      ORDER BY count DESC
      """

      assert {:ok, %QueryResult{rows: rows}} =
               @subject.execute_query(ctx.backend, {sql, [], %{}}, [])

      assert rows == [
               %{"label" => "oauth (github)", "count" => 4},
               %{"label" => "password", "count" => 4},
               %{"label" => "otp", "count" => 2}
             ]
    end

    test "returns invalid_query error for unsupported SQL", ctx do
      reject(HttpBased.Client, :new, 1)

      assert {:error,
              %QueryError{kind: :invalid_query, backend: @subject, description: description}} =
               @subject.execute_query(
                 ctx.backend,
                 "SELECT row_number() OVER () FROM #{ctx.table}",
                 []
               )

      assert description =~ "Unsupported"
    end

    test "returns backend_error on a failed search", ctx do
      mock_adapter(fn _env ->
        {:ok, %Tesla.Env{status: 400, body: %{"message" => "bad query"}}}
      end)

      assert {:error, %QueryError{kind: :backend_error, description: description}} =
               @subject.execute_query(ctx.backend, "SELECT id FROM #{ctx.table}", [])

      assert description =~ "status 400"
    end

    test "returns connection_error when Quickwit is unreachable", ctx do
      mock_adapter(fn _env -> {:error, :econnrefused} end)

      assert {:error, %QueryError{kind: :connection_error}} =
               @subject.execute_query(ctx.backend, "SELECT id FROM #{ctx.table}", [])
    end
  end

  describe "sanitize_config_for_display/1" do
    test "masks api_key while preserving displayable keys" do
      config = Map.put(@valid_config, :api_key, "secret-key-123")

      assert %{endpoint: "http://quickwit.local:7280", index_id: "logflare", api_key: masked} =
               @subject.sanitize_config_for_display(config)

      refute masked == "secret-key-123"
    end
  end

  describe "redact_config/1" do
    test "redacts API key" do
      assert %{api_key: "REDACTED"} = @subject.redact_config(%{api_key: "secret-key-123"})
      assert @subject.redact_config(@valid_config) == @valid_config
    end
  end

  defp mock_adapter(function) do
    stub(@tesla_adapter)

    stub(HttpBased.Client, :new, fn opts ->
      HttpBased.Client
      |> Mimic.call_original(:new, [opts])
      |> MockAdapter.replace(function)
    end)
  end
end
