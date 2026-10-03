defmodule Logflare.Backends.Adaptor.QuickwitAdaptor.QueryTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.QuickwitAdaptor.Query

  @token "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
  @table "`project.dataset.aaaaaaaa_bbbb_cccc_dddd_eeeeeeeeeeee`"
  @source %{"term" => %{"lf_source" => %{"value" => @token}}}
  @nothing %{"bool" => %{"must_not" => [%{"match_all" => %{}}]}}

  defp plan!(sql, params \\ %{}) do
    assert {:ok, plan} = Query.to_plan(:bq_sql, sql, params)
    plan
  end

  defp filters(where, params \\ %{}) do
    case plan!("SELECT id FROM #{@table} WHERE #{where}", params).query do
      %{"bool" => %{"must" => [@source | filters]}} -> filters
      @source -> []
      other -> other
    end
  end

  defp term(field, value), do: %{"term" => %{field => %{"value" => value}}}
  defp exists(field), do: %{"exists" => %{"field" => field}}
  defp phrase(text), do: %{"match_phrase_prefix" => %{"event_message" => %{"query" => text}}}

  describe "rows" do
    test "selects columns, sorts and limits" do
      plan =
        plan!(
          "SELECT t.timestamp, t.id, t.event_message FROM #{@table} t ORDER BY t.timestamp DESC LIMIT 5"
        )

      assert plan == %{
               shape: :rows,
               query: @source,
               size: 5,
               from: 0,
               sort: [%{"timestamp" => %{"order" => "desc"}}],
               columns: [
                 {"timestamp", {:path, ["timestamp"]}},
                 {"id", {:path, ["id"]}},
                 {"event_message", {:path, ["event_message"]}}
               ]
             }
    end

    test "restricts every query to the documents of its source" do
      assert %{query: @source} = plan!("SELECT * FROM #{@table}")
    end

    test "caps the limit and passes the offset" do
      assert %{columns: :all, size: 1000, from: 10} =
               plan!("SELECT * FROM #{@table} LIMIT 2000 OFFSET 10")
    end

    test "applies the default limit" do
      assert %{size: 1000, from: 0, sort: []} = plan!("SELECT id FROM #{@table}")
    end

    test "aliases columns and formats cast timestamps" do
      assert %{columns: [{"ts", {:datetime, ["timestamp"]}}]} =
               plan!("SELECT CAST(timestamp AS DATETIME) AS ts FROM #{@table}")
    end

    test "resolves CTEs and UNNEST aliases to document paths" do
      plan =
        plan!("""
        WITH logs AS (SELECT * FROM #{@table} WHERE level = 'error')
        SELECT l.id, m.request.path AS path
        FROM logs l CROSS JOIN UNNEST(l.metadata) AS m
        WHERE m.request.method = 'GET'
        LIMIT 3
        """)

      assert plan.size == 3

      assert plan.columns == [
               {"id", {:path, ["id"]}},
               {"path", {:path, ["metadata", "request", "path"]}}
             ]

      assert plan.query == %{
               "bool" => %{
                 "must" => [
                   @source,
                   term("level", "error"),
                   term("metadata.request.method", "GET")
                 ]
               }
             }
    end
  end

  describe "unqualified columns" do
    test "resolve to the unnested record that owns them" do
      plan =
        plan!("""
        SELECT id, path, function_id
        FROM #{@table} t CROSS JOIN UNNEST(t.metadata) AS m CROSS JOIN UNNEST(m.request) AS request
        WHERE status_code >= 500
        """)

      assert plan.columns == [
               {"id", {:path, ["id"]}},
               {"path", {:path, ["metadata", "request", "path"]}},
               {"function_id", {:path, ["metadata", "function_id"]}}
             ]

      assert plan.query == %{
               "bool" => %{
                 "must" => [@source, %{"range" => %{"metadata.status_code" => %{"gte" => 500}}}]
               }
             }
    end

    test "stay top-level without an UNNEST" do
      assert filters("level = 'error'") == [term("level", "error")]
    end
  end

  describe "conditions" do
    test "comparisons" do
      assert filters("level = 'error' AND status >= 500") == [
               term("level", "error"),
               %{"range" => %{"status" => %{"gte" => 500}}}
             ]

      assert filters("timestamp > '2026-10-01T00:00:00Z'") == [
               %{"range" => %{"timestamp" => %{"gt" => "2026-10-01T00:00:00Z"}}}
             ]
    end

    test "inequality only matches documents that have the field" do
      assert filters("level != 'error'") == [
               exists("level"),
               %{"bool" => %{"must_not" => [term("level", "error")]}}
             ]
    end

    test "OR, IN and NOT" do
      either = %{"bool" => %{"should" => [term("level", "a"), term("level", "b")]}}

      assert filters("level = 'a' OR level = 'b'") == [either]
      assert filters("level IN ('a', 'b')") == [either]

      assert filters("NOT (level = 'a' OR level = 'b')") == [
               %{"bool" => %{"must" => [exists("level")], "must_not" => [either]}}
             ]
    end

    test "BETWEEN" do
      assert filters("status BETWEEN 400 AND 499") == [
               %{"range" => %{"status" => %{"gte" => 400}}},
               %{"range" => %{"status" => %{"lte" => 499}}}
             ]
    end

    test "IS NULL and IS NOT NULL" do
      assert filters("level IS NOT NULL") == [exists("level")]
      assert filters("level IS NULL") == [%{"bool" => %{"must_not" => [exists("level")]}}]
    end

    test "IFNULL default also matches documents without the field" do
      assert filters("IFNULL(level, 'info') = 'info'") == [
               %{
                 "bool" => %{
                   "should" => [
                     term("level", "info"),
                     %{"bool" => %{"must_not" => [exists("level")]}}
                   ]
                 }
               }
             ]
    end

    test "LIKE and regexp_contains become phrase searches" do
      assert filters("event_message LIKE '%time out%'") == [phrase("time out")]

      assert filters("regexp_contains(event_message, 'foo|bar baz')") == [
               %{"bool" => %{"should" => [phrase("foo"), phrase("bar baz")]}}
             ]
    end

    test "substitutes parameters" do
      assert filters("level = @level", %{"level" => "error"}) == [term("level", "error")]
    end

    test "a comparison with a missing parameter matches nothing" do
      assert filters("level = @level") == @nothing
    end

    test "constant conditions are evaluated up front" do
      assert filters("1 = 1") == []
      assert filters("1 = 2") == @nothing
    end

    test "CASE only translates the branch its parameters select" do
      where =
        "CASE WHEN COALESCE(@start, '') = '' THEN true ELSE timestamp >= CAST(@start AS TIMESTAMP) END"

      assert filters(where, %{"start" => ""}) == []

      assert filters(where, %{"start" => "2026-10-01T00:00:00Z"}) == [
               %{"range" => %{"timestamp" => %{"gte" => "2026-10-01T00:00:00Z"}}}
             ]
    end
  end

  describe "aggregations" do
    test "count(*) and conditional counts" do
      plan =
        plan!("""
        SELECT count(*) AS total, count(CASE WHEN level = 'error' THEN 1 END) AS errors
        FROM #{@table}
        """)

      assert plan.shape == :counts
      assert [{"total", @source}, {"errors", errors}] = plan.counters
      assert errors == %{"bool" => %{"must" => [@source, term("level", "error")]}}
    end

    test "counts per time bucket" do
      plan =
        plan!("""
        SELECT timestamp_trunc(t.timestamp, minute) AS timestamp, count(t.timestamp) AS count
        FROM #{@table} t
        GROUP BY timestamp
        ORDER BY timestamp DESC
        """)

      assert %{
               shape: :histogram,
               column: "timestamp",
               interval: "1m",
               order: :desc,
               query: @source
             } = plan

      assert plan.counters == [
               {"count", %{"bool" => %{"must" => [@source, exists("timestamp")]}}}
             ]
    end

    test "counts per field value" do
      assert %{
               shape: :terms,
               column: "level",
               field: "level",
               query: @source,
               counters: [{"count", @source}]
             } = plan!("SELECT level, count(*) AS count FROM #{@table} GROUP BY level")
    end
  end

  describe "general aggregation" do
    test "groups by several columns with metrics and computed columns" do
      plan =
        plan!("""
        SELECT r.path AS path, r.method AS method, count(t.id) AS count,
               round(avg(m.duration) / 1000, 2) AS avg_ms
        FROM #{@table} t CROSS JOIN UNNEST(t.metadata) AS m CROSS JOIN UNNEST(m.request) AS r
        GROUP BY r.path, r.method
        ORDER BY count DESC
        LIMIT 10
        """)

      assert %{shape: :aggregate, query: @source, limit: 10, order: [{"count", :desc}]} = plan

      assert plan.keys == [
               {"path", {:term, "metadata.request.path"}},
               {"method", {:term, "metadata.request.method"}}
             ]

      assert plan.metrics == [
               {"m0", :count, nil, @source},
               {"m1", :avg, "metadata.duration", @source}
             ]

      assert plan.columns == [
               {"path", {:key, 0}},
               {"method", {:key, 1}},
               {"count", {:metric, "m0"}},
               {"avg_ms",
                {:call, :round,
                 [{:call, :divide, [{:metric, "m1"}, {:const, 1000}]}, {:const, 2}]}}
             ]
    end

    test "time buckets combine with other keys, countif and count distinct" do
      plan =
        plan!("""
        SELECT cast(timestamp_trunc(timestamp, hour) AS datetime) AS timestamp, level,
               countif(status >= 500) AS errors, count(DISTINCT user_id) AS users
        FROM #{@table}
        GROUP BY timestamp, level
        """)

      assert plan.keys == [
               {"timestamp", {:bucket, "1h", :datetime}},
               {"level", {:term, "level"}}
             ]

      assert [{"m0", :count, nil, errors}, {"m1", :distinct, "user_id", @source}] = plan.metrics

      assert errors == %{
               "bool" => %{"must" => [@source, %{"range" => %{"status" => %{"gte" => 500}}}]}
             }
    end

    test "aggregates without GROUP BY" do
      assert %{shape: :aggregate, keys: [], columns: [{"slowest", {:metric, "m0"}}]} =
               plan!("SELECT max(duration) AS slowest FROM #{@table}")
    end

    test "json_value on the message reads the parsed copy under metadata" do
      assert filters(~s|json_value(event_message, "$.auth_event.action") = 'login'|) == [
               term("metadata.auth_event.action", "login")
             ]
    end

    test "substring LIKE on an exact-match field matches a prefix, with or without a slash" do
      assert [%{"bool" => %{"should" => [first, second]}}] = filters("path LIKE '%auth/v1%'")
      assert first == %{"query_string" => %{"query" => "path:auth\\/v1*"}}
      assert second == %{"query_string" => %{"query" => "path:\\/auth\\/v1*"}}
    end

    test "regexp_contains on an exact-match field matches a prefix too" do
      assert filters("regexp_contains(path, '/rest')") ==
               filters("path LIKE '%/rest%'")
    end

    test "a key computed with CASE is grouped by its fields and merged afterwards" do
      plan =
        plan!("""
        SELECT CASE WHEN provider IS NOT NULL AND provider != '' THEN concat(method, ' (', provider, ')')
               ELSE method END AS label, count(*) AS count
        FROM #{@table}
        GROUP BY label
        """)

      assert plan.merge

      assert plan.keys == [
               {nil, {:term, "provider", :missing}},
               {nil, {:term, "method", :missing}}
             ]

      assert [{"label", {:computed, {:case, [{condition, concat}], {:key, 1}}}}, {"count", _}] =
               plan.columns

      assert condition == {:and, {:present, {:key, 0}}, {:neq, {:key, 0}, {:const, ""}}}
      assert concat == {:concat, [{:key, 1}, {:const, " ("}, {:key, 0}, {:const, ")"}]}
    end

    test "a computed key cannot be combined with avg" do
      sql = "SELECT CASE WHEN a = 'x' THEN b ELSE a END AS k, avg(n) FROM #{@table} GROUP BY k"
      assert {:error, message} = Query.to_plan(:bq_sql, sql, %{})
      assert message =~ "only supports count()"
    end

    test "rejects columns that are neither grouped nor aggregated" do
      assert {:error, message} =
               Query.to_plan(:bq_sql, "SELECT level, avg(status) FROM #{@table}", %{})

      assert message =~ "must appear in GROUP BY"
    end
  end

  describe "unsupported SQL" do
    for {name, sql, message} <- [
          {"joins", "SELECT a.id FROM t a JOIN t b ON a.id = b.id", "CROSS JOIN UNNEST"},
          {"unions", "SELECT id FROM t UNION ALL SELECT id FROM t", "no UNION"},
          {"window functions", "SELECT row_number() OVER () FROM t", "Unsupported"},
          {"statements other than SELECT", "DELETE FROM t", "Only SELECT"},
          {"multiple statements", "SELECT 1; SELECT 2", "single SQL statement"},
          {"invalid SQL", "SELEKT", "SQL parse error"}
        ] do
      test "rejects #{name}" do
        sql = String.replace(unquote(sql), " t", " #{@table}")
        assert {:error, message} = Query.to_plan(:bq_sql, sql, %{})
        assert message =~ unquote(message)
      end
    end

    test "rejects languages without a SQL dialect" do
      assert {:error, "Unsupported query language :lql"} =
               Query.to_plan(:lql, "m.level:error", %{})

      refute Query.supported_language?(:lql)
      assert Query.supported_language?(:bq_sql)
    end
  end
end
