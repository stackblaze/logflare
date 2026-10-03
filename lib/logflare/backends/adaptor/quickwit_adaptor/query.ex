defmodule Logflare.Backends.Adaptor.QuickwitAdaptor.Query do
  @moduledoc """
  Translates Logflare endpoint SQL into a Quickwit search plan.

  Quickwit has no SQL interface. Its Elasticsearch-compatible search API
  (`POST /api/v1/_elastic/{index}/_search`) covers what endpoint queries over log
  events need: boolean filters, ranges, exact terms, phrase matching, date
  histograms and terms aggregations. `to_plan/3` turns the transformed endpoint
  SQL into a plan the adaptor executes against that API:

  - `:rows` - a document search (`SELECT a, b ... ORDER BY ... LIMIT n`)
  - `:counts` - one row of counts (`SELECT count(*) ...`), one search per column
  - `:histogram` - counts per time bucket
    (`SELECT timestamp_trunc(timestamp, minute), count(...) ... GROUP BY 1`)
  - `:terms` - counts per value of a field (`SELECT field, count(*) ... GROUP BY field`)

  ## Supported SQL

  - One level of `FROM`: a source table, or a CTE that selects from a source
    table (CTEs over CTEs are followed). The `WHERE` of every level is ANDed.
    All documents of a backend live in one index, so the source table becomes
    a filter on the `lf_source` field (the source token).
  - `CROSS JOIN UNNEST(metadata) AS m` and friends are not joins in Quickwit:
    aliases resolve to dotted document paths (`m.request.path` is
    `metadata.request.path`).
  - Conditions: `AND`, `OR`, `NOT`, `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`,
    `BETWEEN`, `IS [NOT] NULL`, `LIKE`, `regexp_contains`, `contains`,
    `starts_with`, boolean `CASE`, and `IFNULL`/`COALESCE`/`CAST`/`SAFE_CAST`
    around a field. Expressions over parameters and literals only are
    evaluated up front, so `CASE WHEN COALESCE(@start, '') = '' THEN true ELSE
    ... END` costs nothing when the parameter is empty.
  - `count(*)`, `count(field)` and `count(CASE WHEN <condition> THEN 1 END)`.
  - `ORDER BY` a field, `LIMIT` (capped at #{1_000}), `OFFSET`.

  Text matching is token based: `regexp_contains(event_message, 'time out')`
  and `LIKE '%time out%'` match the phrase, with the last word as a prefix.
  Regular expression syntax beyond alternation (`a|b`) is not available in
  Quickwit and is reduced to its literal words.
  """

  import Logflare.Utils.Guards

  alias Logflare.Sql.Parser

  @default_limit 1_000
  @max_limit 1_000
  @terms_size 1_000
  @source_field "lf_source"
  @timestamp_field "timestamp"
  @text_fields ["event_message"]
  @event_columns ~w(timestamp id event_message metadata project identifier)
  @record_fields %{
    "request" => ~w(path method search host protocol url headers cf),
    "response" => ~w(status_code origin_time headers)
  }
  @dialects %{bq_sql: "bigquery", ch_sql: "clickhouse", pg_sql: "postgres"}
  @intervals %{
    "second" => "1s",
    "minute" => "1m",
    "hour" => "1h",
    "day" => "1d",
    "week" => "7d"
  }

  @typedoc "An Elasticsearch query DSL fragment."
  @type es_query :: map()

  @type plan ::
          %{
            shape: :rows,
            query: es_query(),
            size: non_neg_integer(),
            from: non_neg_integer(),
            sort: [map()],
            columns: :all | [{String.t(), column()}]
          }
          | %{shape: :counts, counters: [{String.t(), es_query()}]}
          | %{
              shape: :histogram,
              column: String.t(),
              interval: String.t(),
              order: :asc | :desc,
              query: es_query(),
              counters: [{String.t(), es_query()}]
            }
          | %{
              shape: :terms,
              column: String.t(),
              field: String.t(),
              query: es_query(),
              counters: [{String.t(), es_query()}],
              size: pos_integer()
            }
          | %{
              shape: :aggregate,
              query: es_query(),
              keys: [{String.t() | nil, key()}],
              metrics: [
                {String.t(), :count | :distinct | :sum | :avg | :min | :max, String.t() | nil,
                 es_query()}
              ],
              columns: [{String.t(), expression()}],
              order: [{String.t(), :asc | :desc}],
              limit: pos_integer(),
              size: pos_integer(),
              merge: boolean()
            }

  @type column :: {:path, [String.t()]} | {:datetime, [String.t()]}
  @type key ::
          {:bucket, String.t(), :micros | :datetime}
          | {:term, String.t()}
          | {:term, String.t(), :missing}
  @type expression ::
          {:key, non_neg_integer()}
          | {:metric, String.t()}
          | {:const, term()}
          | {:call, atom(), [expression()]}
          | {:computed, tuple()}

  @spec source_field() :: String.t()
  def source_field, do: @source_field

  @spec supported_language?(atom()) :: boolean()
  def supported_language?(language), do: Map.has_key?(@dialects, language)

  @doc """
  Builds the search plan for a transformed endpoint query.

  `params` are the endpoint's input parameters keyed by name, without the `@`.
  """
  @spec to_plan(atom(), String.t(), map()) :: {:ok, plan()} | {:error, String.t()}
  def to_plan(language, sql, params)
      when is_atom_value(language) and is_non_empty_binary(sql) and is_map(params) do
    with {:ok, dialect} <- dialect(language),
         {:ok, ast} <- parse(dialect, sql),
         {:ok, query} <- single_query(ast) do
      ctes = cte_map(query)

      with {:ok, select} <- select_of(query),
           {:ok, base, scope} <- base_filter(select, ctes, params, 0) do
        ctx = %{scope: scope, params: params}
        classify(query, select, base, ctx)
      end
    end
  catch
    {:unsupported, message} -> {:error, message}
  end

  defp dialect(language) do
    case Map.fetch(@dialects, language) do
      {:ok, dialect} -> {:ok, dialect}
      :error -> {:error, "Unsupported query language #{inspect(language)}"}
    end
  end

  defp parse(dialect, sql) do
    case Parser.parse(dialect, sql) do
      {:ok, [ast]} -> {:ok, ast}
      {:ok, _} -> {:error, "Only a single SQL statement is supported"}
      {:error, reason} -> {:error, "SQL parse error: #{inspect(reason)}"}
    end
  end

  defp single_query(%{"Query" => query}), do: {:ok, query}
  defp single_query(_), do: {:error, "Only SELECT queries are supported"}

  defp select_of(%{"body" => %{"Select" => select}}), do: {:ok, select}
  defp select_of(_), do: {:error, "Only plain SELECT queries are supported (no UNION)"}

  defp cte_map(%{"with" => %{"cte_tables" => ctes}}) when is_list(ctes) do
    for %{"alias" => %{"name" => %{"value" => name}}, "query" => query} <- ctes, into: %{} do
      {name, query}
    end
  end

  defp cte_map(_), do: %{}

  ## FROM: source filter, inherited WHERE clauses, alias scope

  # Returns the filter every result must satisfy (source + the WHERE of this
  # level and of the CTEs below it) and the alias scope of this level.
  defp base_filter(_select, _ctes, _params, depth) when depth > 8,
    do: {:error, "CTEs are nested too deeply"}

  defp base_filter(select, ctes, params, depth) do
    with {:ok, table, scope} <- from_scope(select) do
      ctx = %{scope: scope, params: params}
      own = if select["selection"], do: condition(select["selection"], ctx), else: true

      with {:ok, inherited} <- table_filter(table, ctes, params, depth) do
        {:ok, all_of([inherited, own]), scope}
      end
    end
  end

  defp table_filter(table, ctes, params, depth) do
    with {:ok, cte_query} <- Map.fetch(ctes, table),
         {:ok, cte_select} <- select_of(cte_query),
         {:ok, inherited, _scope} <-
           base_filter(cte_select, Map.delete(ctes, table), params, depth + 1) do
      {:ok, inherited}
    else
      :error -> {:ok, term(@source_field, source_token(table))}
      {:error, _reason} = error -> error
    end
  end

  defp from_scope(%{"from" => [%{"relation" => %{"Table" => table}} = from]}) do
    name = table["name"] |> Enum.map_join(".", &get_in(&1, ["Identifier", "value"]))

    aliases =
      [alias_name(table["alias"]), name, name |> String.split(".") |> List.last()]
      |> Enum.reject(&is_nil/1)

    scope = %{tables: MapSet.new(aliases), unnests: %{}}

    scope =
      Enum.reduce(from["joins"] || [], scope, fn
        %{"relation" => %{"UNNEST" => %{"alias" => alias, "array_exprs" => [expr]}}}, scope ->
          case {alias_name(alias), identifier(expr)} do
            {name, segments} when is_binary(name) and is_list(segments) ->
              put_in(scope, [:unnests, name], resolve(segments, scope))

            _ ->
              unsupported("Unsupported UNNEST, expected `UNNEST(field) AS alias`")
          end

        _join, _scope ->
          unsupported("Only CROSS JOIN UNNEST(...) joins are supported")
      end)

    {:ok, name, scope}
  end

  defp from_scope(%{"from" => []}), do: {:error, "Query has no FROM clause"}
  defp from_scope(_), do: {:error, "Only a single table in FROM is supported"}

  defp alias_name(%{"name" => %{"value" => name}}), do: name
  defp alias_name(_), do: nil

  # BigQuery tables are `project.dataset.token_with_underscores`, Postgres ones
  # `log_events_token`; the source token is the trailing uuid.
  defp source_token(table) do
    name = table |> String.split(".") |> List.last() |> String.replace("`", "")

    case Regex.run(
           ~r/([0-9a-f]{8})_([0-9a-f]{4})_([0-9a-f]{4})_([0-9a-f]{4})_([0-9a-f]{12})$/i,
           name
         ) do
      [_ | groups] -> groups |> Enum.join("-") |> String.downcase()
      nil -> name
    end
  end

  defp identifier(%{"Identifier" => %{"value" => value}}), do: [value]

  defp identifier(%{"CompoundIdentifier" => parts}) when is_list(parts),
    do: Enum.map(parts, & &1["value"])

  defp identifier(_), do: nil

  # Document path of an identifier: unnest aliases expand, table aliases drop.
  defp resolve([first | rest] = segments, scope) do
    cond do
      path = scope.unnests[first] -> path ++ rest
      rest != [] and MapSet.member?(scope.tables, first) -> rest
      rest == [] -> unqualified(first, scope)
      true -> segments
    end
  end

  # A bare column that is not an event column belongs to one of the unnested records, as in
  # BigQuery: `path` after `CROSS JOIN UNNEST(m.request)` is `metadata.request.path`. Without
  # a schema, the record is picked by its usual fields, else the outermost one (`metadata`).
  defp unqualified(name, scope) do
    paths = scope.unnests |> Map.values() |> Enum.sort_by(&length/1)

    owner =
      Enum.find(paths, fn path -> name in Map.get(@record_fields, List.last(path), []) end) ||
        List.first(paths)

    if name in @event_columns or is_nil(owner), do: [name], else: owner ++ [name]
  end

  ## Result shape

  # The simple shapes cover Studio's Logs pages with one search each. Anything else that
  # aggregates (several GROUP BY columns, avg/sum/min/max, computed columns) becomes the
  # general `:aggregate` shape.
  defp classify(query, select, base, ctx) do
    classify_simple(query, select, base, ctx)
  catch
    {:unsupported, _message} = reason ->
      if aggregate?(select), do: aggregate(query, select, base, ctx), else: throw(reason)
  else
    {:error, _message} = error ->
      if aggregate?(select), do: aggregate(query, select, base, ctx), else: error

    plan ->
      plan
  end

  defp classify_simple(query, select, base, ctx) do
    items = Enum.map(select["projection"] || [], &projection_item(&1, ctx))
    group_by = group_by(select, ctx)
    aggregates = for {:count, name, filter} <- items, do: {name, all_of([base, filter])}
    others = Enum.reject(items, &match?({:count, _, _}, &1))

    cond do
      group_by == [] and aggregates != [] and others == [] ->
        {:ok, %{shape: :counts, counters: for({name, q} <- aggregates, do: {name, to_query(q)})}}

      group_by == [] and aggregates != [] ->
        {:error, "Mixing count() with plain columns needs a GROUP BY"}

      group_by != [] ->
        grouped(query, others, aggregates, group_by, base, ctx)

      true ->
        rows(query, others, base, ctx)
    end
  end

  defp projection_item(%{"UnnamedExpr" => expr}, ctx), do: projection_expr(expr, nil, ctx)

  defp projection_item(
         %{"ExprWithAlias" => %{"expr" => expr, "alias" => %{"value" => name}}},
         ctx
       ),
       do: projection_expr(expr, name, ctx)

  defp projection_item(%{"Wildcard" => _}, _ctx), do: :all
  defp projection_item(%{"QualifiedWildcard" => _}, _ctx), do: :all
  defp projection_item(_, _ctx), do: unsupported("Unsupported SELECT item")

  defp projection_expr(%{"Function" => function} = expr, name, ctx) do
    case {function_name(function), function_args(function)} do
      {"count", args} ->
        {:count, name || "count", count_filter(args, ctx)}

      {trunc, [field, %{"Identifier" => %{"value" => unit}}]}
      when trunc in ["timestamp_trunc", "datetime_trunc", "date_trunc"] ->
        {:bucket, name || "timestamp", field_path!(field, ctx), interval!(unit)}

      # Postgres argument order: date_trunc('minute', timestamp)
      {"date_trunc", [%{"Value" => _} = unit, field]} ->
        {:bucket, name || "timestamp", field_path!(field, ctx), interval!(literal!(unit))}

      _ ->
        column(expr, name, ctx)
    end
  end

  defp projection_expr(expr, name, ctx), do: column(expr, name, ctx)

  defp column(expr, name, ctx) do
    case operand(expr, ctx) do
      {:field, path, _default, cast} ->
        kind = if cast == :datetime, do: :datetime, else: :path
        {:column, name || List.last(path), {kind, path}}

      _ ->
        unsupported("Only fields, count() and timestamp_trunc() are supported in SELECT")
    end
  end

  defp count_filter(["Wildcard"], _ctx), do: true
  defp count_filter([%{"Value" => %{"value" => %{"Null" => _}}}], _ctx), do: false
  defp count_filter([%{"Value" => _}], _ctx), do: true

  # count(CASE WHEN c THEN 1 END) counts the rows where the CASE is not null.
  defp count_filter([%{"Case" => %{"operand" => nil, "conditions" => whens} = kase}], ctx) do
    else_counts? = not null_literal?(kase["else_result"])

    {filters, _} =
      Enum.reduce(whens, {[], true}, fn %{"condition" => condition, "result" => result},
                                        {acc, remaining} ->
        matches = all_of([remaining, condition(condition, ctx)])
        acc = if null_literal?(result), do: acc, else: [matches | acc]
        {acc, all_of([remaining, negate(condition(condition, ctx), condition, ctx)])}
      end)

    if else_counts?, do: true, else: any_of(filters)
  end

  defp count_filter([expr], ctx) do
    case operand(expr, ctx) do
      # every event has an id; it is not a fast field, so `exists` cannot see it
      {:field, ["id"], _default, _cast} -> true
      {:field, path, :none, _cast} -> exists(path)
      {:field, _path, _default, _cast} -> true
      _ -> unsupported("Unsupported count() argument")
    end
  end

  defp count_filter(_, _ctx), do: unsupported("Unsupported count() arguments")

  defp null_literal?(nil), do: true
  defp null_literal?(%{"Value" => %{"value" => %{"Null" => _}}}), do: true
  defp null_literal?(%{"Value" => %{"value" => "Null"}}), do: true
  defp null_literal?(_), do: false

  defp interval!(unit) do
    case Map.fetch(@intervals, String.downcase(to_string(unit))) do
      {:ok, interval} ->
        interval

      :error ->
        unsupported(
          "Unsupported time bucket #{inspect(unit)}, use second, minute, hour, day or week"
        )
    end
  end

  defp group_by(%{"group_by" => %{"Expressions" => [exprs | _]}}, _ctx) when is_list(exprs),
    do: exprs

  defp group_by(_, _ctx), do: []

  # GROUP BY refers to a SELECT item by alias, position or expression.
  defp grouped(query, others, aggregates, [key], base, ctx) do
    counters =
      case aggregates do
        [] -> unsupported("GROUP BY needs at least one count() column")
        aggregates -> for {name, q} <- aggregates, do: {name, to_query(q)}
      end

    case group_target(key, others, ctx) do
      {:bucket, name, [@timestamp_field], interval} when length(others) == 1 ->
        {:ok,
         %{
           shape: :histogram,
           column: name,
           interval: interval,
           order: order_direction(query, name),
           query: to_query(base),
           counters: counters
         }}

      {:bucket, _name, _path, _interval} ->
        {:error,
         "Time buckets are only supported on the timestamp field, as the only non-count column"}

      {:column, name, {_kind, path}} when length(others) == 1 ->
        {:ok,
         %{
           shape: :terms,
           column: name,
           field: Enum.join(path, "."),
           query: to_query(base),
           counters: counters,
           size: min(limit(query) || @terms_size, @terms_size)
         }}

      _ ->
        {:error, "GROUP BY must name the single non-count column of the SELECT"}
    end
  end

  defp grouped(_query, _others, _aggregates, _keys, _base, _ctx),
    do: {:error, "GROUP BY supports a single expression"}

  defp group_target(%{"Value" => %{"value" => %{"Number" => [position, _]}}}, others, _ctx) do
    Enum.at(others, String.to_integer(position) - 1)
  end

  defp group_target(key, others, ctx) do
    by_alias =
      case identifier(key) do
        [name] -> Enum.find(others, &(is_tuple(&1) and elem(&1, 1) == name))
        _ -> nil
      end

    by_alias ||
      case identifier(key) && operand(key, ctx) do
        {:field, path, _, _} ->
          Enum.find(others, fn
            {:column, _name, {_kind, ^path}} -> true
            {:bucket, _name, ^path, _interval} -> true
            _ -> false
          end)

        _ ->
          nil
      end
  end

  defp rows(query, items, base, ctx) do
    columns =
      if items == [] or :all in items do
        :all
      else
        for {:column, name, column} <- items, do: {name, column}
      end

    if Enum.any?(items, &match?({:bucket, _, _, _}, &1)) do
      {:error, "timestamp_trunc() needs a GROUP BY"}
    else
      {:ok,
       %{
         shape: :rows,
         query: to_query(base),
         size: min(limit(query) || @default_limit, @max_limit),
         from: offset(query),
         sort: sort(query, items, ctx),
         columns: columns
       }}
    end
  end

  ## General aggregation

  @aggregate_functions ~w(count countif sum avg min max)
  @metric_functions %{"sum" => :sum, "avg" => :avg, "min" => :min, "max" => :max}
  @arithmetic %{
    "Divide" => :divide,
    "Multiply" => :multiply,
    "Plus" => :add,
    "Minus" => :subtract
  }
  @scalar_functions %{
    "round" => :round,
    "coalesce" => :coalesce,
    "ifnull" => :coalesce,
    "safe_divide" => :divide
  }

  defp aggregate?(select) do
    group_by(select, nil) != [] or Enum.any?(select["projection"] || [], &aggregate_expr?/1)
  end

  defp aggregate_expr?(%{"Function" => function} = expr) do
    function_name(function) in @aggregate_functions or
      Enum.any?(expr, fn {_key, value} -> aggregate_expr?(value) end)
  end

  defp aggregate_expr?(%{} = expr),
    do: Enum.any?(expr, fn {_key, value} -> aggregate_expr?(value) end)

  defp aggregate_expr?(list) when is_list(list), do: Enum.any?(list, &aggregate_expr?/1)
  defp aggregate_expr?(_), do: false

  defp aggregate(query, select, base, ctx) do
    {items, metrics} =
      Enum.map_reduce(select["projection"] || [], [], fn item, metrics ->
        {expr, name} = aliased(item)
        {compiled, metrics} = aggregate_expr(expr, ctx, metrics)
        {{name || default_name(expr, compiled), compiled}, metrics}
      end)

    grouped = Enum.map(group_by(select, ctx), &group_key(&1, items, ctx))
    key_values = Enum.map(grouped, fn {_name, key} -> key end)
    computed? = Enum.any?(key_values, &match?({:computed, _}, &1))
    keys = grouped |> Enum.flat_map(&key_fields/1) |> Enum.uniq()

    if computed? and Enum.any?(metrics, fn {_id, kind, _field, _filter} -> kind != :count end),
      do: unsupported("Grouping by an expression only supports count()")

    for {name, {:key, key}} <- items, key not in key_values do
      unsupported("Column #{name} must appear in GROUP BY or be aggregated")
    end

    {buckets, terms} = Enum.split_with(keys, &match?({_name, {:bucket, _, _}}, &1))
    if length(buckets) > 1, do: unsupported("Only one time bucket per query is supported")

    {:ok,
     %{
       shape: :aggregate,
       query: to_query(base),
       keys: buckets ++ terms,
       metrics:
         for {id, kind, field, filter} <- Enum.reverse(metrics) do
           {id, kind, field, to_query(all_of([base, filter]))}
         end,
       columns: for({name, compiled} <- items, do: {name, column_expr(compiled, keys)}),
       order: aggregate_order(query, items),
       limit: min(limit(query) || @max_limit, @max_limit),
       size: @terms_size,
       merge: computed?
     }}
  end

  # A key computed from fields (CASE, CONCAT) is grouped by those fields; the adaptor
  # evaluates it per group and merges the groups that end up with the same value.
  defp key_fields({_name, {:computed, expr}}),
    do: for(field <- value_fields(expr), do: {nil, {:term, field, :missing}})

  defp key_fields(key), do: [key]

  defp value_fields({:field, field}), do: [field]
  defp value_fields(tuple) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> value_fields()
  defp value_fields(list) when is_list(list), do: Enum.flat_map(list, &value_fields/1)
  defp value_fields(_), do: []

  defp index_fields({:field, field}, keys),
    do: {:key, Enum.find_index(keys, fn {_name, key} -> key == {:term, field, :missing} end)}

  defp index_fields(tuple, keys) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> index_fields(keys) |> List.to_tuple()

  defp index_fields(list, keys) when is_list(list), do: Enum.map(list, &index_fields(&1, keys))
  defp index_fields(other, _keys), do: other

  defp aliased(%{"UnnamedExpr" => expr}), do: {expr, nil}

  defp aliased(%{"ExprWithAlias" => %{"expr" => expr, "alias" => %{"value" => name}}}),
    do: {expr, name}

  defp aliased(_), do: unsupported("SELECT * cannot be combined with aggregates")

  defp default_name(_expr, {:key, {:term, field}}), do: field |> String.split(".") |> List.last()
  defp default_name(_expr, {:key, {:bucket, _, _}}), do: @timestamp_field
  defp default_name(_expr, {:key, {:computed, _}}), do: "f0_"
  defp default_name(%{"Function" => function}, _compiled), do: function_name(function)
  defp default_name(_expr, _compiled), do: "f0_"

  # Keys become references by position among the plan's keys, so the adaptor can read them
  # from a bucket path.
  defp column_expr({:key, {:computed, expr}}, keys), do: {:computed, index_fields(expr, keys)}

  defp column_expr({:key, key}, keys),
    do: {:key, Enum.find_index(keys, fn {_name, k} -> k == key end)}

  defp column_expr({:call, fun, args}, keys),
    do: {:call, fun, Enum.map(args, &column_expr(&1, keys))}

  defp column_expr(other, _keys), do: other

  # Compiles a SELECT expression into `{:key, key} | {:metric, id} | {:const, value} |
  # {:call, fun, args}`, collecting the metrics it needs.
  defp aggregate_expr(%{"Nested" => expr}, ctx, metrics), do: aggregate_expr(expr, ctx, metrics)

  defp aggregate_expr(%{"Cast" => %{"expr" => expr, "data_type" => type}} = cast, ctx, metrics) do
    if aggregate_expr?(expr) or bucket_expr?(expr) do
      {compiled, metrics} = aggregate_expr(expr, ctx, metrics)
      {cast_bucket(compiled, cast_kind(type)), metrics}
    else
      aggregate_operand(cast, ctx, metrics)
    end
  end

  defp aggregate_expr(%{"BinaryOp" => %{"left" => left, "op" => op, "right" => right}}, ctx, m)
       when is_map_key(@arithmetic, op) do
    {left, m} = aggregate_expr(left, ctx, m)
    {right, m} = aggregate_expr(right, ctx, m)
    {{:call, @arithmetic[op], [left, right]}, m}
  end

  defp aggregate_expr(%{"Function" => function} = expr, ctx, metrics) do
    name = function_name(function)
    args = function_args(function)

    cond do
      name == "count" and distinct?(function) ->
        metric(:distinct, field_name!(args, ctx), true, metrics)

      name == "count" ->
        metric(:count, nil, count_filter(args, ctx), metrics)

      name == "countif" ->
        metric(:count, nil, condition(single!(args, name), ctx), metrics)

      is_map_key(@metric_functions, name) ->
        metric(@metric_functions[name], field_name!(args, ctx), true, metrics)

      bucket_expr?(expr) ->
        {:bucket, _name, path, interval} = projection_expr(expr, nil, ctx)
        if path != [@timestamp_field], do: unsupported("Time buckets need the timestamp field")
        {{:key, {:bucket, interval, :micros}}, metrics}

      is_map_key(@scalar_functions, name) and aggregate_expr?(expr) ->
        {args, metrics} = Enum.map_reduce(args, metrics, &aggregate_expr(&1, ctx, &2))
        {{:call, @scalar_functions[name], args}, metrics}

      true ->
        aggregate_operand(expr, ctx, metrics)
    end
  end

  defp aggregate_expr(expr, ctx, metrics), do: aggregate_operand(expr, ctx, metrics)

  defp cast_bucket({:key, {:bucket, interval, _format}}, :datetime),
    do: {:key, {:bucket, interval, :datetime}}

  defp cast_bucket(compiled, _kind), do: compiled

  defp aggregate_operand(%{"Case" => _} = expr, ctx, metrics),
    do: {{:key, {:computed, value_expr(expr, ctx)}}, metrics}

  defp aggregate_operand(expr, ctx, metrics) do
    case operand(expr, ctx) do
      {:field, path, _default, _cast} -> {{:key, {:term, Enum.join(path, ".")}}, metrics}
      {:value, value} -> {{:const, value}, metrics}
    end
  end

  # A per-row expression over fields, evaluated by the adaptor on group keys.
  defp value_expr(%{"Nested" => expr}, ctx), do: value_expr(expr, ctx)
  defp value_expr(nil, _ctx), do: {:const, nil}

  defp value_expr(%{"Case" => %{"operand" => nil, "conditions" => whens} = kase}, ctx) do
    branches =
      for %{"condition" => condition, "result" => result} <- whens,
          do: {value_condition(condition, ctx), value_expr(result, ctx)}

    {:case, branches, value_expr(kase["else_result"], ctx)}
  end

  defp value_expr(%{"Function" => function} = expr, ctx) do
    case function_name(function) do
      "concat" -> {:concat, Enum.map(function_args(function), &value_expr(&1, ctx))}
      _ -> value_operand(expr, ctx)
    end
  end

  defp value_expr(expr, ctx), do: value_operand(expr, ctx)

  defp value_operand(expr, ctx) do
    case operand(expr, ctx) do
      {:field, path, _default, _cast} -> {:field, Enum.join(path, ".")}
      {:value, value} -> {:const, value}
    end
  end

  @value_operators %{"And" => :and, "Or" => :or, "Eq" => :eq, "NotEq" => :neq}

  defp value_condition(%{"Nested" => expr}, ctx), do: value_condition(expr, ctx)
  defp value_condition(%{"IsNotNull" => expr}, ctx), do: {:present, value_expr(expr, ctx)}
  defp value_condition(%{"IsNull" => expr}, ctx), do: {:absent, value_expr(expr, ctx)}

  defp value_condition(%{"BinaryOp" => %{"left" => left, "op" => op, "right" => right}}, ctx)
       when op in ["And", "Or"],
       do: {@value_operators[op], value_condition(left, ctx), value_condition(right, ctx)}

  defp value_condition(%{"BinaryOp" => %{"left" => left, "op" => op, "right" => right}}, ctx)
       when op in ["Eq", "NotEq"],
       do: {@value_operators[op], value_expr(left, ctx), value_expr(right, ctx)}

  defp value_condition(_expr, _ctx),
    do: unsupported("Unsupported condition in a grouped CASE expression")

  defp bucket_expr?(%{"Function" => function}),
    do: function_name(function) in ~w(timestamp_trunc datetime_trunc date_trunc)

  defp bucket_expr?(_), do: false

  defp distinct?(%{"args" => %{"List" => %{"duplicate_treatment" => "Distinct"}}}), do: true
  defp distinct?(_), do: false

  defp single!([arg], _name), do: arg
  defp single!(_args, name), do: unsupported("#{name}() takes one argument")

  defp field_name!(args, ctx) do
    case args do
      [arg] -> arg |> field_path!(ctx) |> Enum.join(".")
      _ -> unsupported("Aggregates take one field")
    end
  end

  defp metric(kind, field, filter, metrics) do
    id = "m#{length(metrics)}"
    {{:metric, id}, [{id, kind, field, filter} | metrics]}
  end

  # GROUP BY names a SELECT item by position or alias, or a field directly.
  defp group_key(%{"Value" => %{"value" => %{"Number" => [position, _]}}}, items, _ctx) do
    case Enum.at(items, String.to_integer(position) - 1) do
      {name, {:key, key}} -> {name, key}
      _ -> unsupported("GROUP BY #{position} does not name a groupable column")
    end
  end

  defp group_key(expr, items, ctx) do
    by_alias =
      case identifier(expr) do
        [name] -> Enum.find(items, &match?({^name, {:key, _}}, &1))
        _ -> nil
      end

    case by_alias do
      {name, {:key, key}} ->
        {name, key}

      nil ->
        {{:key, key}, _metrics} = aggregate_expr(expr, ctx, [])
        {name, _} = Enum.find(items, {nil, nil}, &match?({_, {:key, ^key}}, &1))
        {name, key}
    end
  end

  defp aggregate_order(query, items) do
    names = Enum.map(items, fn {name, _compiled} -> name end)

    for %{"expr" => expr} = order <- order_exprs(query),
        name = order_name(expr, names),
        do: {name, direction(order)}
  end

  defp order_name(%{"Value" => %{"value" => %{"Number" => [position, _]}}}, names),
    do: Enum.at(names, String.to_integer(position) - 1)

  defp order_name(expr, names) do
    case identifier(expr) do
      nil -> nil
      segments -> Enum.find(names, &(&1 == List.last(segments)))
    end
  end

  defp order_exprs(%{"order_by" => %{"kind" => %{"Expressions" => exprs}}}) when is_list(exprs),
    do: exprs

  defp order_exprs(_), do: []

  defp direction(%{"options" => %{"sort" => "Desc"}}), do: :desc
  defp direction(_), do: :asc

  defp order_direction(query, name) do
    case order_exprs(query) do
      [%{"expr" => expr} = order | _] ->
        if identifier(expr) in [[name], [@timestamp_field]], do: direction(order), else: :asc

      _ ->
        :asc
    end
  end

  defp sort(query, items, ctx) do
    for %{"expr" => expr} = order <- order_exprs(query) do
      aliased =
        case identifier(expr) do
          [name] -> Enum.find(items, &match?({:column, ^name, _}, &1))
          _ -> nil
        end

      path =
        case aliased do
          {:column, _name, {_kind, path}} -> path
          nil -> field_path!(expr, ctx)
        end

      %{Enum.join(path, ".") => %{"order" => to_string(direction(order))}}
    end
  end

  defp limit(%{
         "limit_clause" => %{
           "LimitOffset" => %{"limit" => %{"Value" => %{"value" => %{"Number" => [n, _]}}}}
         }
       }),
       do: String.to_integer(n)

  defp limit(_), do: nil

  defp offset(%{
         "limit_clause" => %{
           "LimitOffset" => %{
             "offset" => %{"value" => %{"Value" => %{"value" => %{"Number" => [n, _]}}}}
           }
         }
       }),
       do: String.to_integer(n)

  defp offset(_), do: 0

  ## Conditions
  #
  # `condition/2` returns `true`, `false` or an Elasticsearch query map.
  # Booleans come from expressions over parameters and literals only.

  defp condition(%{"Nested" => expr}, ctx), do: condition(expr, ctx)

  defp condition(%{"BinaryOp" => %{"op" => "And", "left" => left, "right" => right}}, ctx),
    do: all_of([condition(left, ctx), condition(right, ctx)])

  defp condition(%{"BinaryOp" => %{"op" => "Or", "left" => left, "right" => right}}, ctx),
    do: any_of([condition(left, ctx), condition(right, ctx)])

  defp condition(%{"UnaryOp" => %{"op" => "Not", "expr" => expr}}, ctx),
    do: negate(condition(expr, ctx), expr, ctx)

  defp condition(%{"BinaryOp" => %{"op" => op, "left" => left, "right" => right}}, ctx)
       when op in ["Eq", "NotEq", "Gt", "GtEq", "Lt", "LtEq"] do
    compare(operand(left, ctx), op, operand(right, ctx))
  end

  defp condition(
         %{"InList" => %{"expr" => expr, "list" => list, "negated" => negated}} = node,
         ctx
       ) do
    left = operand(expr, ctx)
    result = any_of(for item <- list, do: compare(left, "Eq", operand(item, ctx)))

    if negated,
      do: negate(result, %{node | "InList" => %{node["InList"] | "negated" => false}}, ctx),
      else: result
  end

  defp condition(
         %{"Between" => %{"expr" => expr, "low" => low, "high" => high, "negated" => negated}} =
           node,
         ctx
       ) do
    left = operand(expr, ctx)

    result =
      all_of([
        compare(left, "GtEq", operand(low, ctx)),
        compare(left, "LtEq", operand(high, ctx))
      ])

    if negated,
      do: negate(result, %{node | "Between" => %{node["Between"] | "negated" => false}}, ctx),
      else: result
  end

  defp condition(%{"IsNull" => expr}, ctx) do
    case operand(expr, ctx) do
      {:field, path, :none, _} -> none_of(exists(path))
      {:field, _path, _default, _} -> false
      {:value, value} -> is_nil(value)
    end
  end

  defp condition(%{"IsNotNull" => expr}, ctx) do
    case operand(expr, ctx) do
      {:field, path, :none, _} -> exists(path)
      {:field, _path, _default, _} -> true
      {:value, value} -> not is_nil(value)
    end
  end

  defp condition(%{"Like" => like}, ctx), do: like_condition(like, ctx)
  defp condition(%{"ILike" => like}, ctx), do: like_condition(like, ctx)

  defp condition(
         %{"Case" => %{"operand" => nil, "conditions" => whens, "else_result" => else_result}},
         ctx
       ),
       do: case_condition(whens, else_result, ctx)

  defp condition(%{"Function" => function} = expr, ctx) do
    case {function_name(function), function_args(function)} do
      {"regexp_contains", [field, pattern]} -> text_search(field, pattern, :regexp, ctx)
      {"contains", [field, pattern]} -> text_search(field, pattern, :contains, ctx)
      {"contains_substr", [field, pattern]} -> text_search(field, pattern, :contains, ctx)
      {"starts_with", [field, pattern]} -> text_search(field, pattern, :prefix, ctx)
      _ -> truthy(operand(expr, ctx))
    end
  end

  defp condition(expr, ctx), do: truthy(operand(expr, ctx))

  # Branches are only translated when they can be reached, so a branch that is
  # ruled out by a parameter may hold expressions that would not translate.
  defp case_condition([], nil, _ctx), do: false
  defp case_condition([], else_result, ctx), do: condition(else_result, ctx)

  defp case_condition([%{"condition" => test_expr, "result" => result} | rest], else_result, ctx) do
    case condition(test_expr, ctx) do
      true ->
        condition(result, ctx)

      false ->
        case_condition(rest, else_result, ctx)

      test ->
        any_of([
          all_of([test, condition(result, ctx)]),
          all_of([negate(test, test_expr, ctx), case_condition(rest, else_result, ctx)])
        ])
    end
  end

  defp like_condition(%{"expr" => expr, "pattern" => pattern, "negated" => negated}, ctx) do
    result =
      case {operand(expr, ctx), operand(pattern, ctx)} do
        {{:field, path, _, _}, {:value, pattern}} when is_binary(pattern) ->
          like(path, pattern)

        {{:value, value}, {:value, pattern}} when is_binary(value) and is_binary(pattern) ->
          static_like(value, pattern)

        _ ->
          unsupported("LIKE needs a field and a string pattern")
      end

    if negated, do: negate(result, expr, ctx), else: result
  end

  # A value used as a condition: `WHERE flag`, `THEN true`, `THEN 1`.
  defp truthy({:value, value}), do: value not in [nil, false, 0, ""]
  defp truthy({:field, path, _default, _}), do: term(Enum.join(path, "."), true)

  defp text_search(field, pattern, mode, ctx) do
    case {operand(field, ctx), operand(pattern, ctx)} do
      {{:field, path, _, _}, {:value, pattern}} when is_binary(pattern) ->
        field = Enum.join(path, ".")

        case mode do
          :regexp -> regexp(field, pattern)
          :contains -> phrase(field, pattern, :contains)
          :prefix -> phrase(field, pattern, :prefix)
        end

      {{:value, value}, {:value, pattern}} when is_binary(value) and is_binary(pattern) ->
        String.contains?(value, pattern)

      _ ->
        unsupported("Text search needs a field and a string pattern")
    end
  end

  # NOT in SQL is false for NULL operands, while a must_not would match
  # documents that lack the field. Require the fields the condition reads.
  defp negate(true, _expr, _ctx), do: false
  defp negate(false, _expr, _ctx), do: true

  defp negate(query, expr, ctx) when is_map(query) do
    required = expr |> required_fields(ctx) |> Enum.uniq() |> Enum.map(&exists/1)
    %{"bool" => %{"must" => required, "must_not" => [query]}} |> prune_bool()
  end

  defp required_fields(expr, ctx) when is_map(expr) do
    case expr do
      %{"Identifier" => _} ->
        field_or_nothing(expr, ctx)

      %{"CompoundIdentifier" => _} ->
        field_or_nothing(expr, ctx)

      %{"Function" => function} ->
        if function_name(function) in ["ifnull", "coalesce"] do
          []
        else
          function |> function_args() |> Enum.flat_map(&required_fields(&1, ctx))
        end

      _ ->
        expr |> Map.values() |> Enum.flat_map(&required_fields(&1, ctx))
    end
  end

  defp required_fields(list, ctx) when is_list(list),
    do: Enum.flat_map(list, &required_fields(&1, ctx))

  defp required_fields(_other, _ctx), do: []

  defp field_or_nothing(expr, ctx) do
    case operand(expr, ctx) do
      {:field, path, :none, _} -> [path]
      _ -> []
    end
  end

  ## Operands

  # `{:value, term}` for literals and parameters, `{:field, path, default, cast}`
  # for document fields; `default` is what IFNULL/COALESCE substitute for a
  # missing field, or `:none`.
  defp operand(%{"Nested" => expr}, ctx), do: operand(expr, ctx)

  defp operand(%{"Identifier" => %{"value" => "@" <> name}}, ctx), do: {:value, ctx.params[name]}

  defp operand(%{"Value" => %{"value" => value}}, ctx), do: {:value, literal(value, ctx)}

  defp operand(%{"Cast" => %{"expr" => expr, "data_type" => type}}, ctx) do
    case operand(expr, ctx) do
      {:field, path, default, _} -> {:field, path, default, cast_kind(type)}
      {:value, value} -> {:value, cast_value(value, cast_kind(type))}
    end
  end

  defp operand(%{"Function" => function}, ctx) do
    case {function_name(function), function_args(function)} do
      # Structured log lines are stored parsed under `metadata`, so a JSON path into the
      # message is the same path into `metadata`.
      {name, [message, %{"Value" => %{"value" => path}}]}
      when name in ["json_value", "json_extract_scalar"] ->
        if List.last(field_path!(message, ctx)) not in @text_fields,
          do: unsupported("#{name}() is only supported on #{Enum.join(@text_fields, ", ")}")

        {:field, ["metadata" | json_path!(literal(path, ctx))], :none, nil}

      {name, [first, fallback]} when name in ["ifnull", "coalesce", "nvl"] ->
        case {operand(first, ctx), operand(fallback, ctx)} do
          {{:field, path, :none, cast}, {:value, default}} ->
            {:field, path, {:default, default}, cast}

          {{:field, _, _, _} = field, _} ->
            field

          {{:value, nil}, fallback} ->
            fallback

          {{:value, _} = value, _} ->
            value
        end

      {name, [arg]} when name in ["lower", "upper", "trim", "string", "to_json_string"] ->
        operand(arg, ctx)

      _ ->
        unsupported("Unsupported function #{function_name(function)}()")
    end
  end

  defp operand(expr, ctx) do
    case identifier(expr) do
      nil -> unsupported("Unsupported expression #{expr |> Map.keys() |> Enum.join(", ")}")
      segments -> {:field, resolve(segments, ctx.scope), :none, nil}
    end
  end

  defp json_path!("$." <> path) when path != "", do: String.split(path, ".")
  defp json_path!(path), do: unsupported("Unsupported JSON path #{inspect(path)}")

  defp field_path!(expr, ctx) do
    case operand(expr, ctx) do
      {:field, path, _, _} -> path
      _ -> unsupported("Expected a field")
    end
  end

  defp literal!(%{"Value" => %{"value" => value}}), do: literal(value, %{params: %{}})

  defp literal(%{"Number" => [number, _]}, _ctx) do
    case Integer.parse(number) do
      {integer, ""} -> integer
      _ -> String.to_float(number)
    end
  end

  defp literal(%{"SingleQuotedString" => string}, _ctx), do: string
  defp literal(%{"DoubleQuotedString" => string}, _ctx), do: string
  defp literal(%{"Boolean" => boolean}, _ctx), do: boolean
  defp literal(%{"Placeholder" => "@" <> name}, ctx), do: ctx.params[name]
  defp literal(%{"Null" => _}, _ctx), do: nil
  defp literal("Null", _ctx), do: nil
  defp literal(other, _ctx), do: unsupported("Unsupported literal #{inspect(other)}")

  defp cast_kind(type) when is_binary(type) do
    cond do
      type in ~w(Timestamp Datetime Date) -> :datetime
      String.starts_with?(type, "Int") or type in ~w(BigInt Integer SmallInt TinyInt) -> :integer
      String.starts_with?(type, "Float") or type in ~w(Numeric Decimal Double Real) -> :float
      true -> nil
    end
  end

  defp cast_kind(type) when is_map(type), do: type |> Map.keys() |> List.first() |> cast_kind()
  defp cast_kind(_), do: nil

  defp cast_value(value, :integer) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _ -> nil
    end
  end

  defp cast_value(value, :float) when is_binary(value) do
    case Float.parse(value) do
      {float, ""} -> float
      _ -> nil
    end
  end

  defp cast_value(value, _kind), do: value

  defp function_name(%{"name" => name}) when is_list(name) do
    name |> List.last() |> get_in(["Identifier", "value"]) |> to_string() |> String.downcase()
  end

  defp function_args(%{"args" => %{"List" => %{"args" => args}}}) do
    Enum.map(args, fn
      %{"Unnamed" => %{"Expr" => expr}} -> expr
      %{"Unnamed" => "Wildcard"} -> "Wildcard"
      _ -> unsupported("Named function arguments are not supported")
    end)
  end

  defp function_args(_), do: []

  ## Comparisons

  @flip %{
    "Eq" => "Eq",
    "NotEq" => "NotEq",
    "Gt" => "Lt",
    "GtEq" => "LtEq",
    "Lt" => "Gt",
    "LtEq" => "GtEq"
  }

  defp compare({:value, left}, op, {:value, right}), do: static_compare(left, op, right)

  defp compare({:value, _} = value, op, {:field, _, _, _} = field),
    do: compare(field, Map.fetch!(@flip, op), value)

  defp compare({:field, _, _, _}, _op, {:field, _, _, _}),
    do: unsupported("Comparing two fields is not supported")

  # SQL comparisons with NULL are never true.
  defp compare({:field, _path, :none, _cast}, _op, {:value, nil}), do: false

  defp compare({:field, path, :none, cast}, op, {:value, value}) do
    field_compare(Enum.join(path, "."), op, coerce(value, cast))
  end

  # IFNULL(field, default) <op> value: the field's own comparison when it is
  # present, otherwise whatever the default compares to.
  defp compare({:field, path, {:default, default}, cast}, op, {:value, value} = right) do
    present = compare({:field, path, :none, cast}, op, right)

    if static_compare(coerce(default, cast), op, coerce(value, cast)) do
      any_of([present, none_of(exists(path))])
    else
      present
    end
  end

  defp coerce(value, :integer) when is_binary(value), do: cast_value(value, :integer)
  defp coerce(value, :float) when is_binary(value), do: cast_value(value, :float)
  defp coerce(value, _cast), do: value

  defp field_compare(_field, _op, nil), do: false

  defp field_compare(@timestamp_field = field, "Eq", value),
    do: range(field, %{"gte" => timestamp(value), "lte" => timestamp(value)})

  defp field_compare(field, "Eq", value) when field in @text_fields and is_binary(value),
    do: %{"match_phrase" => %{field => %{"query" => value}}}

  defp field_compare(field, "Eq", value), do: term(field, value)

  defp field_compare(field, "NotEq", value),
    do: all_of([exists(String.split(field, ".")), none_of(field_compare(field, "Eq", value))])

  defp field_compare(field, "Gt", value), do: range(field, %{"gt" => range_value(field, value)})

  defp field_compare(field, "GtEq", value),
    do: range(field, %{"gte" => range_value(field, value)})

  defp field_compare(field, "Lt", value), do: range(field, %{"lt" => range_value(field, value)})

  defp field_compare(field, "LtEq", value),
    do: range(field, %{"lte" => range_value(field, value)})

  defp range_value(@timestamp_field, value), do: timestamp(value)
  defp range_value(_field, value), do: value

  # Quickwit takes RFC 3339 on datetime ranges. Logflare timestamps are unix
  # microseconds, and SQL literals may come without a zone.
  defp timestamp(value) when is_integer(value) do
    unit =
      cond do
        value > 100_000_000_000_000_000 -> :nanosecond
        value > 100_000_000_000_000 -> :microsecond
        value > 100_000_000_000 -> :millisecond
        true -> :second
      end

    value |> DateTime.from_unix!(unit) |> DateTime.to_iso8601()
  end

  defp timestamp(value) when is_binary(value) do
    normalized = String.replace(value, " ", "T", global: false)

    with {:error, _} <- DateTime.from_iso8601(normalized),
         {:error, _} <- DateTime.from_iso8601(normalized <> "Z"),
         {:error, _} <- Date.from_iso8601(normalized) do
      unsupported("Invalid timestamp #{inspect(value)}")
    else
      {:ok, %DateTime{} = datetime, _offset} -> DateTime.to_iso8601(datetime)
      {:ok, %Date{} = date} -> Date.to_iso8601(date) <> "T00:00:00Z"
    end
  end

  defp timestamp(value), do: unsupported("Invalid timestamp #{inspect(value)}")

  defp static_compare(left, _op, right) when is_nil(left) or is_nil(right), do: false
  defp static_compare(left, "Eq", right), do: left == right
  defp static_compare(left, "NotEq", right), do: left != right
  defp static_compare(left, "Gt", right), do: comparable(left, right) and left > right
  defp static_compare(left, "GtEq", right), do: comparable(left, right) and left >= right
  defp static_compare(left, "Lt", right), do: comparable(left, right) and left < right
  defp static_compare(left, "LtEq", right), do: comparable(left, right) and left <= right

  defp comparable(left, right),
    do: (is_number(left) and is_number(right)) or (is_binary(left) and is_binary(right))

  ## Text matching

  defp like(path, pattern) do
    field = Enum.join(path, ".")
    core = pattern |> String.trim("%") |> String.replace(["%", "_"], " ") |> String.trim()

    cond do
      core == "" ->
        exists(path)

      not String.contains?(pattern, ["%", "_"]) ->
        field_compare(field, "Eq", pattern)

      field in @text_fields ->
        phrase(field, core, :contains)

      String.ends_with?(pattern, "%") and not String.starts_with?(pattern, "%") ->
        prefix(field, core)

      String.starts_with?(pattern, "%") and String.ends_with?(pattern, "%") ->
        substring(field, core)

      true ->
        unsupported(
          "LIKE with only a leading wildcard is only supported on #{Enum.join(@text_fields, ", ")}"
        )
    end
  end

  defp static_like(value, pattern) do
    regex = pattern |> Regex.escape() |> String.replace("%", ".*") |> String.replace("_", ".")
    Regex.match?(Regex.compile!("^" <> regex <> "$", "s"), value)
  end

  # Quickwit has no regular expression query. Alternation becomes an OR of
  # phrases, everything else is reduced to the words in the pattern.
  defp regexp(field, pattern) do
    pattern
    |> String.replace(~r/^\(\?[a-z]+\)/, "")
    |> String.split("|")
    |> Enum.map(fn alternative ->
      words =
        alternative
        |> String.replace(~r/\\[a-zA-Z]|[\^\$\.\*\+\?\(\)\[\]\{\}\\]/, " ")
        |> String.trim()

      if words == "", do: true, else: phrase(field, words, :contains)
    end)
    |> any_of()
  end

  defp phrase(field, text, mode) when field in @text_fields do
    text = String.trim(text)

    cond do
      text == "" -> true
      mode == :contains -> %{"match_phrase_prefix" => %{field => %{"query" => text}}}
      mode == :prefix -> %{"match_phrase_prefix" => %{field => %{"query" => text}}}
    end
  end

  # Other fields are indexed as single raw tokens: exact or prefix only.
  defp phrase(field, text, :prefix), do: prefix(field, text)
  defp phrase(field, text, :contains), do: substring(field, text)

  # An exact-match field cannot be searched for a substring. Values that start with the
  # text, with or without a leading slash, cover filtering by path (`/rest`, `auth/v1`).
  defp substring(field, text) do
    bare = String.trim_leading(text, "/")
    any_of([prefix(field, bare), prefix(field, "/" <> bare)])
  end

  defp prefix(field, text) do
    escaped = String.replace(text, ~r/([+\-!(){}\[\]^"~*?:\\\/ ])/, "\\\\\\1")
    %{"query_string" => %{"query" => "#{field}:#{escaped}*"}}
  end

  ## Elasticsearch query helpers

  defp term(field, value), do: %{"term" => %{field => %{"value" => value}}}
  defp range(field, bounds), do: %{"range" => %{field => bounds}}
  defp exists(path) when is_list(path), do: %{"exists" => %{"field" => Enum.join(path, ".")}}
  defp none, do: %{"bool" => %{"must_not" => [%{"match_all" => %{}}]}}

  defp none_of(true), do: false
  defp none_of(false), do: true
  defp none_of(query), do: %{"bool" => %{"must_not" => [query]}}

  defp all_of(conditions) do
    conditions = Enum.reject(conditions, &(&1 == true))

    cond do
      false in conditions -> false
      conditions == [] -> true
      match?([_], conditions) -> hd(conditions)
      true -> %{"bool" => %{"must" => Enum.flat_map(conditions, &flatten(&1, "must"))}}
    end
  end

  defp any_of(conditions) do
    conditions = Enum.reject(conditions, &(&1 == false))

    cond do
      true in conditions -> true
      conditions == [] -> false
      match?([_], conditions) -> hd(conditions)
      true -> %{"bool" => %{"should" => Enum.flat_map(conditions, &flatten(&1, "should"))}}
    end
  end

  # Merges nested bools of the same kind: (a AND b) AND c is one must list.
  # A bool holding only `should` clauses matches when at least one of them does.
  defp flatten(%{"bool" => %{"must" => items} = bool}, "must") when map_size(bool) == 1, do: items

  defp flatten(%{"bool" => %{"should" => items} = bool}, "should") when map_size(bool) == 1,
    do: items

  defp flatten(query, _kind), do: [query]

  defp prune_bool(%{"bool" => bool}),
    do: %{"bool" => Map.reject(bool, fn {_key, value} -> value == [] end)}

  defp to_query(true), do: %{"match_all" => %{}}
  defp to_query(false), do: none()
  defp to_query(query) when is_map(query), do: query

  defp unsupported(message), do: throw({:unsupported, message})
end
