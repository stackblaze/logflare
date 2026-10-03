defmodule Logflare.Backends.Adaptor.QuickwitAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.QuickwitAdaptor.Query
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

    [backend: backend, source: source]
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

  describe "test_connection/1" do
    setup :backend_data

    test "succeeds on 200 response", ctx do
      mock_adapter(fn env ->
        assert env.method == :get
        assert env.url == "http://quickwit.local:7280/api/v1/logflare"

        {:ok, %Tesla.Env{status: 200, body: ~s({"index_id":"logflare"})}}
      end)

      assert :ok = @subject.test_connection(ctx.backend)
    end

    test "returns error when the index is missing", ctx do
      mock_adapter(fn env ->
        assert env.url == "http://quickwit.local:7280/api/v1/logflare"
        {:ok, %Tesla.Env{status: 404, body: ~s({"message":"index not found"})}}
      end)

      assert {:error, reason} = @subject.test_connection(ctx.backend)
      assert reason =~ "not found"
    end

    test "returns error on request failure", ctx do
      mock_adapter(fn _env -> {:error, :nxdomain} end)

      assert {:error, reason} = @subject.test_connection(ctx.backend)
      assert is_binary(reason)
    end
  end

  describe "logs ingestion" do
    setup :backend_data

    setup %{source: source} do
      start_supervised!({SourceSup, source})
      :ok
    end

    test "sends logs as NDJSON to the ingest API", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        assert Tesla.build_url(env) == "http://quickwit.local:7280/api/v1/logflare/ingest"
        assert env.method == :post
        assert Tesla.get_header(env, "content-type") == "application/x-ndjson"
        assert Tesla.get_header(env, "content-encoding") == "gzip"

        send(this, {ref, env.body})
        {:ok, %Tesla.Env{status: 200, body: ~s({"num_docs_processed":1})}}
      end)

      log_event =
        build(:log_event,
          source: source,
          event_message: "Test log message",
          random_attribute: "nothing",
          timestamp: System.system_time(:microsecond)
        )

      assert {:ok, _} = Backends.ingest_logs([log_event], source)
      assert_receive {^ref, gzipped}, 5000
      assert json = :zlib.gunzip(gzipped)

      assert [log] =
               json
               |> String.split("\n", trim: true)
               |> Enum.map(&Jason.decode!/1)

      assert log["event_message"] == log_event.body["event_message"]
      assert log["random_attribute"] == "nothing"
      assert String.contains?(log["timestamp"], "T")
    end

    test "sends multiple log events as separate NDJSON lines", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        send(this, {ref, env.body})
        {:ok, %Tesla.Env{status: 200, body: ~s({"num_docs_processed":3})}}
      end)

      log_events =
        build_list(3, :log_event,
          source: source,
          timestamp: System.system_time(:microsecond)
        )

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, gzipped}, 5000
      assert json = :zlib.gunzip(gzipped)
      assert [_, _, _] = String.split(json, "\n", trim: true)
    end
  end

  describe "execute_query/3" do
    setup :backend_data

    test "translates SQL to QuickwitQL and returns hit rows", ctx do
      mock_adapter(fn env ->
        assert env.method == :post
        assert Tesla.build_url(env) == "http://quickwit.local:7280/api/v1/logflare/search"
        assert Tesla.get_header(env, "content-type") == "application/json"

        assert %{"query" => "level:\"error\"", "max_hits" => 100} =
                 env.body |> IO.iodata_to_binary() |> Jason.decode!()

        {:ok,
         %Tesla.Env{
           status: 200,
           body: ~s({"hits":[{"level":"error","event_message":"boom"}],"num_hits":1})
         }}
      end)

      assert {:ok, %Adaptor.QueryResult{rows: [row]}} =
               @subject.execute_query(ctx.backend, "select * where level = 'error'", [])

      assert row["level"] == "error"
    end

    test "applies LIMIT, ORDER BY and projections", ctx do
      mock_adapter(fn env ->
        assert %{
                 "query" => "level:\"error\" AND source:\"api\"",
                 "max_hits" => 10,
                 "sort_by" => "-timestamp"
               } = env.body |> IO.iodata_to_binary() |> Jason.decode!()

        {:ok,
         %Tesla.Env{
           status: 200,
           body:
             ~s({"hits":[{"level":"error","event_message":"boom","source":"api","timestamp":"2026-01-01T00:00:00Z"}],"num_hits":1})
         }}
      end)

      sql = """
      select event_message, level
      where level = 'error' and source = 'api'
      order by timestamp desc
      limit 10
      """

      assert {:ok, %Adaptor.QueryResult{rows: [row], total_rows: 1}} =
               @subject.execute_query(ctx.backend, sql, [])

      assert row == %{"event_message" => "boom", "level" => "error"}
    end

    test "substitutes @params from input_params", ctx do
      mock_adapter(fn env ->
        assert %{"query" => "level:\"error\""} = env.body |> IO.iodata_to_binary() |> Jason.decode!()
        {:ok, %Tesla.Env{status: 200, body: ~s({"hits":[],"num_hits":0})}}
      end)

      assert {:ok, %Adaptor.QueryResult{rows: []}} =
               @subject.execute_query(
                 ctx.backend,
                 {"select * where level = @lvl", %{"lvl" => "error"}},
                 []
               )
    end

    test "returns invalid_query error for unsupported SQL", ctx do
      assert {:error, %Logflare.Backends.QueryError{kind: :invalid_query}} =
               @subject.execute_query(ctx.backend, "select count(*) from logs", [])
    end

    test "returns backend_error on HTTP failure", ctx do
      mock_adapter(fn _env ->
        {:ok, %Tesla.Env{status: 500, body: ~s({"message":"internal error"})}}
      end)

      assert {:error, %Logflare.Backends.QueryError{kind: :backend_error}} =
               @subject.execute_query(ctx.backend, "select * where level = 'error'", [])
    end
  end

  describe "sanitize_config_for_display/1" do
    test "masks api_key while preserving displayable keys" do
      config = %{endpoint: "http://quickwit:7280", index_id: "logflare", api_key: "SECRET"}

      assert %{endpoint: "http://quickwit:7280", index_id: "logflare", api_key: "**********"} ==
               @subject.sanitize_config_for_display(config)
    end
  end

  describe "redact_config/1" do
    test "redacts API key" do
      config = %{endpoint: "http://quickwit:7280", index_id: "logflare", api_key: "SECRET"}

      assert %{api_key: "REDACTED"} = @subject.redact_config(config)
    end
  end

  describe "Query.to_search/3" do
    test "translates comparison operators" do
      assert {:ok, {"level:\"error\"", _opts}} =
               Query.to_search(:bq_sql, "select * where level = 'error'", %{})
    end

    test "translates NOT EQUAL" do
      assert {:ok, {"NOT level:\"error\"", _opts}} =
               Query.to_search(:bq_sql, "select * where level != 'error'", %{})
    end

    test "translates LIKE with wildcards" do
      assert {:ok, {"event_message:*timed*", _opts}} =
               Query.to_search(:bq_sql, "select * where event_message like '%timed%'", %{})
    end

    test "translates IN lists" do
      assert {:ok, {"(level:\"error\" OR level:\"warn\")", _opts}} =
               Query.to_search(:bq_sql, "select * where level in ('error', 'warn')", %{})
    end

    test "translates timestamp ranges" do
      assert {:ok, {"timestamp:[1700000000000000 TO *]", _opts}} =
               Query.to_search(:bq_sql, "select * where timestamp >= 1700000000000000", %{})
    end

    test "maps ORDER BY to sort_by and LIMIT to max_hits" do
      assert {:ok, {"*", [max_hits: 25, fields: :all, sort_by: "-timestamp"]}} =
               Query.to_search(:bq_sql, "select * order by timestamp desc limit 25", %{})
    end

    test "rejects unsupported SQL" do
      assert {:error, reason} = Query.to_search(:bq_sql, "select count(*) from logs", %{})
      assert is_binary(reason)
    end
  end

  defp mock_adapter(calls_num \\ 1, function) do
    stub(@tesla_adapter)

    HttpBased.Client
    |> expect(:new, calls_num, fn opts ->
      HttpBased.Client
      |> Mimic.call_original(:new, [opts])
      |> MockAdapter.replace(function)
    end)
  end
end