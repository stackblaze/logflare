defmodule Logflare.Backends.Adaptor.QuickwitAdaptor.Query do
  @moduledoc """
  Translates Logflare endpoint SQL (BigQuery, ClickHouse or Postgres dialect) into a
  Quickwit search request, and describes how to shape the Quickwit response back
  into Logflare rows.

  `to_search/3` returns a `{query_string, opts}` pair. The `query_string` is a
  [Quickwit query language](https://quickwit.io/docs/query-language/query-language)
  expression and `opts` carries everything the adaptor needs to build the search
  body and interpret the response:

  - `:result_shape` - `:rows` (document search), `:chart` (date histogram) or
    `:total` (single count)
  - `:max_hits` - `max_hits` for the search body (0 for aggregations)
  - `:sort_by` - optional sort for rows queries (Quickwit `-` prefix = descending)
  - `:fields` - for rows queries, `[{column, path}]` where `path` is the full
    dotted document path to project client-side
  - `:aggs` - aggregation definitions for chart/total queries
  - `:counters` - for chart queries, `[{column, {:static, boolean} |
    {:field, term_name, ast}}]` describing how each `count(CASE ...)` column is
    classified per bucket
  - `:histogram_field` / `:bucket_order` - chart histogram details
  - `:total_alias` - column name for a `SELECT count(*)` result

  ## Supported SQL subset

  The Studio endpoint queries all follow three shapes, which is what this module
  targets:

  - **Rows** - `SELECT ... FROM <cte> CROSS JOIN UNNEST(metadata) ... WHERE ...
    ORDER BY timestamp DESC LIMIT 100`
  - **Chart** - `SELECT timestamp_trunc(t.timestamp, minute) AS timestamp,
    count(CASE WHEN ... THEN 1 END) AS ok_count, ... GROUP BY timestamp`
  - **Total** - `SELECT count(*) AS count FROM <cte> ...`

  Specifically:

  - Common table expressions are collected and the CTE referenced by the outer
    `FROM` is resolved; its `WHERE` is merged (ANDed) with the outer `WHERE`.
  - `CROSS JOIN UNNEST(...)` is flattened into dotted document paths, e.g.
    `UNNEST(m.request) AS request` makes `request.method` address
    `metadata.request.method`.
  - Table aliases (`edge_logs AS t`) are stripped from column references.
  - `CASE WHEN <static> THEN true ELSE <expr> END` is statically evaluated: when
    the condition is known (empty timestamp params) the branch is dropped.
  - `CAST(x AS TIMESTAMP)` unwraps to `x`; timestamp comparisons use RFC3339
    literals.
  - `regexp_contains(field, 'pattern')` maps to token wildcard/phrase search.
  - `count(*)` maps to a Quickwit metric count aggregation, and
    `count(CASE WHEN <cond> THEN 1 END)` over a single field maps to a date
    histogram with a sub `term` aggregation classified client-side.
  - `LIMIT n` maps to `max_hits` (default `#{inspect(@default_max_hits)}`).
  """

  import Logflare.Utils.Guards

  alias Logflare.Sql.AstUtils
  alias Logflare.Sql.Parser

  @default_max_hits 100
  @max_max_hits 1_000
  @dialects %{
    bq_sql: "bigquery",
    ch_sql: "clickhouse",
    pg_sql: "postgres"
  }

  @granularities ~w(second minute hour day week month year)
  @histogram_name "buckets"
  @timestamp_field "timestamp"
  @match_none_query "-#{@timestamp_field}:[* TO *]"
  @term_size 1_000

  @spec supported_language?(atom()) :: boolean()
  def supported_language?(language), do: Map.has_key?(@dialects, language)

  @doc """
  Translates an endpoint SQL query into a Quickwit search request.

  `params` substitutes `@param` placeholders: either a map of endpoint input
  parameters keyed by parameter name (without the `@` prefix), or a list of
  values matched to parameters in order of first appearance in the SQL.
  """
  @spec to_search(atom(), String.t(), map() | list()) ::
          {:ok, {String.t(), keyword()}} | {:error, String.t()}
  def to_search(language, sql, params)
      when is_atom_value(language) and is_non_empty_binary(sql) and
             (is_map(params) or is_list(params)) do
    dialect = Map.fetch!(@dialects, language)

    with {:ok, params} <- resolve_params(sql, params),
         {:ok, [ast]} <- Parser.parse(dialect, sql),
         {:ok, ctes, final_query} <- split_query(ast),
         {:ok, final_select} <- final_select(final_query),
         {:ok, cte_name} <- resolve_from_table(final_select, ctes),
         {:ok, cte_select} <- cte_select(Map.fetch!(ctes, cte_name)),
         {tables, unnests} = scope = build_scope(cte_select, final_select),
         {:ok, query_string} <- where_ql(cte_select, final_select, scope, params),
         {:ok, opts} <- classify(final_query, final_select, {tables, unnests}, params) do
      {:ok, {query_string, opts}}
    end
    |> normalize_error()
  end

  defp resolve_params(sql, params) when is_map(params), do: {:ok, params}

  defp resolve_params(sql, params) when is_list(params) do
    with {:ok, [ast]} <- Parser.parse(Map.fetch!(@dialects, :bq_sql), sql) do
      names = ast |> AstUtils.extract_parameters() |> Enum.uniq()

      if length(names) == length(params) do
        {:ok, Map.new(Enum.zip(names, params))}
      else
        {:error,
         "Expected #{length(names)} parameter(s) (#{Enum.join(names, ", ")}), got #{length(params)}"}
      end
    end
  end

  defp normalize_error({:ok, _} = ok), do: ok
  defp normalize_error({:error, reason}) when is_binary(reason), do: {:error, reason}

  defp normalize_error({:error, {:parse_error, reason}}),
    do: {:error, "SQL parse error: #{reason}"}

  defp normalize_error(:error), do: {:error, "Query is not a supported Quickwit search query"}
  defp normalize_error({:error, reason}), do: {:error, "Unsupported query: #{inspect(reason)}"}

  ## Query structure

  defp split_query(%{"Query" => %{"with" => with_clause} = query}) do
    ctes = cte_tables(with_clause)
    {:ok, ctes, query}
  end

  defp split_query(ast) when is_map(ast), do: {:error, "Only simple SELECT queries are supported"}

  defp cte_tables(%{"cte_tables" => ctes}) when is_list(ctes) do
    Map.new(ctes, fn cte ->
      name = get_in(cte, ["alias", "name", "value"]) || get_in(cte, ["cte_alias", "value"])

      if is_binary(name) do
        {name, cte}
      else
        raise ArgumentError, "unsupported CTE definition: #{inspect(cte)}"
      end
    end)
  end

  defp cte_tables(_), do: %{}

  defp final_select(%{"body" => %{"Select" => select}}), do: {:ok, select}

  defp final_select(%{"body" => %{"SetOperation" => _}}),
    do: {:error, "UNION/INTERSECT/EXCEPT queries are not supported"}

  defp final_select(_), do: {:error, "Only simple SELECT queries are supported"}

  defp cte_select(%{"query" => %{"body" => %{"Select" => select}}}), do: {:ok, select}
  defp cte_select(cte), do: {:error, "Unsupported CTE definition: #{inspect(cte)}"}

  # The outer FROM references one of the CTEs by name.
  defp resolve_from_table(final_select, ctes) do
    with [first | _] <- final_select["from"] || [],
         {:ok, name} <- table_name(Map.get(first, "relation")) do
      cond do
        Map.has_key?(ctes, name) ->
          {:ok, name}

        String.contains?(name, ".") and
            Map.has_key?(ctes, List.last(String.split(name, "."))) ->
          {:ok, List.last(String.split(name, "."))}

        true ->
          {:error, "Query does not select from a known CTE, got table #{name}"}
      end
    else
      [] -> {:error, "Query has no FROM clause"}
      {:error, _} = error -> error
    end
  end

  defp table_name(%{"Table" => %{"name" => name}}) when is_list(name) do
    {:ok, Enum.map_join(name, ".", & &1["Identifier"]["value"])}
  end

  defp table_name(_), do: {:error, "Unsupported FROM relation"}

  ## Identifier scopes: table aliases are dropped, unnest aliases expand to paths

  defp build_scope(cte_select, final_select) do
    [cte_select["from"] || [], final_select["from"] || []]
    |> Enum.reduce({%{}, %{}}, &add_from_list/2)
  end

  defp add_from_list(from_list, acc) do
    Enum.reduce(from_list, acc, fn twj, acc ->
      acc = add_relation(Map.get(twj, "relation"), acc)

      Enum.reduce(Map.get(twj, "joins", []), acc, fn join, acc ->
        add_relation(Map.get(join, "relation"), acc)
      end)
    end)
  end

  defp add_relation(%{"Table" => %{"name" => name, "alias" => alias}}, {tables, unnests}) do
    table = Enum.map_join(name, ".", & &1["Identifier"]["value"])
    tables = Map.put(tables, table, table)

    tables =
      case alias_name(alias) do
        nil -> tables
        alias -> Map.put(tables, alias, table)
      end

    {tables, unnests}
  end

  defp add_relation(%{"UNNEST" => %{"alias" => alias, "array_exprs" => exprs}}, {tables, unnests}) do
    with alias when is_binary(alias) <- alias_name(alias),
         [expr | _] when is_map(expr) <- exprs,
         {:ok, segments} <- identifier_segments(expr),
         {:ok, path} <- resolve_segments(segments, tables, unnests) do
      {tables, Map.put(unnests, alias, path)}
    else
      _ -> {tables, unnests}
    end
  end

  defp add_relation(_relation, acc), do: acc

  defp alias_name(%{"name" => %{"value" => name}}) when is_binary(name), do: name
  defp alias_name(_), do: nil

  defp identifier_segments(%{"Identifier" => %{"value" => value}}), do: {:ok, [value]}

  defp identifier_segments(%{"CompoundIdentifier" => ident}) when is_list(ident) do
    {:ok, Enum.map(ident, & &1["Identifier"]["value"])}
  end

  defp identifier_segments(other), do: {:error, "Unsupported identifier: #{inspect(other)}"}

  # sqlparser serializes Expr::Nested directly: {"Nested" => <expr>}
  defp unwrap_nested(%{"Nested" => %{"expr" => inner}}), do: unwrap_nested(inner)
  defp unwrap_nested(%{"Nested" => inner}) when is_map(inner), do: unwrap_nested(inner)
  defp unwrap_nested(other), do: other

  defp resolve_segments([first | rest], tables, unnests) do
    cond do
      Map.has_key?(unnests, first) ->
        {:ok, Enum.join([Map.fetch!(unnests, first) | rest], ".")}

      Map.has_key?(tables, first) and rest != [] ->
        {:ok, Enum.join(rest, ".")}

      Map.has_key?(tables, first) ->
        {:ok, first}

      true ->
        {:ok, Enum.join([first | rest], ".")}
    end
  end

  defp resolve_scope(segments, {tables, unnests}) do
    case resolve_segments(segments, tables, unnests) do
      {:ok, path} -> {:ok, path}
      _ -> {:ok, Enum.join(segments, ".")}
    end
  end

  ## WHERE translation

  # The resolved CTE's WHERE (project + timestamp gating) is ANDed with the
  # outer query's WHERE (search box, status filters, chart windows).
  defp where_ql(cte_select, final_select, scope, params) do
    [cte_select["selection"], final_select["selection"]]
    |> Enum.reject(&is_nil/1)
    |> fold_conditions(scope, params)
  end

  defp fold_conditions(conditions, scope, params) do
    Enum.reduce_while(conditions, {:ok, []}, fn condition, {:ok, acc} ->
      case expr_to_ql(condition, scope, params) do
        {:ok, :match_all} -> {:cont, {:ok, acc}}
        {:ok, :match_none} -> {:halt, {:ok, :match_none}}
        {:ok, ql} when is_binary(ql) -> {:cont, {:ok, [ql | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, :match_none} -> {:ok, @match_none_query}
      {:ok, []} -> {:ok, "*"}
      {:ok, parts} -> {:ok, Enum.join(Enum.reverse(parts), " AND ")}
      {:error, _} = error -> error
    end
  end

  # Statically known expressions collapse to :match_all / :match_none, so empty
  # timestamp params contribute nothing and `CASE WHEN '' = '' THEN true ELSE ...`
  # resolves to its else branch.
  defp expr_to_ql(expr, scope, params) do
    case static_eval(expr, params) do
      {:static, true} -> {:ok, :match_all}
      {:static, false} -> {:ok, :match_none}
      :dynamic -> dynamic_expr_to_ql(expr, scope, params)
    end
  end

  defp dynamic_expr_to_ql(
         %{"BinaryOp" => %{"left" => left, "op" => op, "right" => right}},
         scope,
         params
       )
       when op in ["And", "Or", "Eq", "NotEq", "Gt", "GtEq", "Lt", "LtEq"] do
    if op in ["And", "Or"] do
      with {:ok, l} <- expr_to_ql(left, scope, params),
           {:ok, r} <- expr_to_ql(right, scope, params) do
        binary_join(l, r, op)
      end
    else
      comparison_to_ql(left, op, right, scope, params)
    end
  end

  defp dynamic_expr_to_ql(%{"Nested" => _} = expr, scope, params) do
    expr_to_ql(unwrap_nested(expr), scope, params)
  end

  defp dynamic_expr_to_ql(%{"UnaryOp" => %{"op" => "Not", "expr" => expr}}, scope, params) do
    with {:ok, ql} <- expr_to_ql(expr, scope, params) do
      ql_not(ql)
    end
  end

  defp dynamic_expr_to_ql(
         %{"Case" => %{"conditions" => conditions, "else_result" => else_result}},
         scope,
         params
       ) do
    match_case(conditions, else_result, scope, params)
  end

  defp dynamic_expr_to_ql(%{"Function" => %{"name" => name} = function}, scope, params) do
    function_to_ql(function_name(name), function, scope, params)
  end

  defp dynamic_expr_to_ql(
         %{"Between" => %{"expr" => expr, "low" => low, "high" => high, "negated" => negated}},
         scope,
         params
       ) do
    with {:ok, {:field, path}} <- operand(expr, scope, params),
         {:ok, {:value, low_value}} <- operand(low, scope, params),
         {:ok, {:value, high_value}} <- operand(high, scope, params) do
      ql = "#{path}:[#{range_value(path, low_value)} TO #{range_value(path, high_value)}]"

      if negated do
        {:ok, "NOT (#{ql})"}
      else
        {:ok, ql}
      end
    end
  end

  defp dynamic_expr_to_ql(
         %{"Like" => %{"negated" => negated, "expr" => expr, "pattern" => pattern}},
         scope,
         params
       ) do
    with {:ok, {:field, path}} <- operand(expr, scope, params),
         {:ok, pattern} <- literal_string(pattern) do
      ql = like_ql(path, pattern)

      if negated do
        {:ok, "NOT (#{ql})"}
      else
        {:ok, ql}
      end
    end
  end

  defp dynamic_expr_to_ql(%{"ILike" => _}, _scope, _params),
    do: {:error, "ILIKE is not supported, use LIKE"}

  defp dynamic_expr_to_ql(%{"InList" => %{"expr" => expr, "list" => list, "negated" => negated}}, scope, params) do
    items =
      Enum.reduce_while(list, {:ok, []}, fn item, {:ok, acc} ->
        case comparison_to_ql(expr, "Eq", item, scope, params) do
          {:ok, ql} -> {:cont, {:ok, [ql | acc]}}
          {:error, _} = error -> {:halt, error}
        end
      end)

    with {:ok, parts} <- items do
      ql = "(" <> Enum.join(Enum.reverse(parts), " OR ") <> ")"

      if negated do
        {:ok, "NOT #{ql}"}
      else
        {:ok, ql}
      end
    end
  end

  # `WHERE field` style boolean columns
  defp dynamic_expr_to_ql(expr, scope, _params) do
    with {:ok, {:field, path}} <- operand(expr, scope, %{}) do
      {:ok, "#{path}:true"}
    end
  end

  defp match_case(conditions, else_result, scope, params) do
    Enum.reduce_while(conditions, nil, fn
      %{"condition" => condition, "result" => result}, _ ->
        case static_eval(condition, params) do
          {:static, true} -> {:halt, {:ok, case_bool_ql(result)}}
          {:static, false} -> {:cont, nil}
          :dynamic -> {:halt, {:error, "Unsupported dynamic CASE WHEN condition"}}
        end

      _, _ ->
        {:cont, nil}
    end)
    |> case do
      nil ->
        if else_result do
          dynamic_expr_to_ql(else_result, scope, params)
        else
          {:ok, :match_none}
        end

      other ->
        other
    end
  end

  # CASE THEN branches in boolean position: `THEN 1` and `THEN true` both match.
  defp case_bool_ql(%{"Value" => %{"value" => %{"Boolean" => false}}}), do: :match_none
  defp case_bool_ql(_), do: :match_all

  defp binary_join(l, r, op) do
    case {l, r, op} do
      {_, :match_none, "And"} -> {:ok, :match_none}
      {:match_none, _, "And"} -> {:ok, :match_none}
      {_, :match_all, "And"} -> {:ok, r}
      {:match_all, _, "And"} -> {:ok, l}
      {_, :match_all, "Or"} -> {:ok, :match_all}
      {:match_all, _, "Or"} -> {:ok, :match_all}
      {_, :match_none, "Or"} -> {:ok, r}
      {:match_none, _, "Or"} -> {:ok, l}
      {l, r, "And"} -> {:ok, "(#{l} AND #{r})"}
      {l, r, "Or"} -> {:ok, "(#{l} OR #{r})"}
    end
  end

  defp ql_not(:match_all), do: {:ok, :match_none}
  defp ql_not(:match_none), do: {:ok, :match_all}
  defp ql_not(ql) when is_binary(ql), do: {:ok, "NOT (#{ql})"}

  defp function_to_ql("regexp_contains", function, scope, _params) do
    with {:ok, [field_expr, pattern_expr]} <- function_args(function, 2),
         {:ok, {:field, path}} <- operand(field_expr, scope, %{}),
         {:ok, pattern} <- literal_string(pattern_expr) do
      regexp_to_ql(path, pattern)
    end
  end

  defp function_to_ql(name, function, scope, params)
       when name in ["contains", "starts_with", "ends_with"] do
    with {:ok, [field_expr, pattern_expr]} <- function_args(function, 2),
         {:ok, {:field, path}} <- operand(field_expr, scope, %{}),
         {:ok, pattern} <- literal_string(pattern_expr) do
      core = String.replace(pattern, ["*", "?"], "")

      case name do
        "contains" -> {:ok, "#{path}:*#{core}*"}
        "starts_with" -> {:ok, "#{path}:#{core}*"}
        "ends_with" -> {:ok, "#{path}:*#{core}"}
      end
    end
  end

  defp function_to_ql(name, _function, _scope, _params),
    do: {:error, "Unsupported function: #{name}()"}

  defp function_args(%{"args" => %{"List" => %{"args" => args}}}, expected)
       when is_list(args) do
    exprs = Enum.map(args, &arg_expr/1)

    count_ok? =
      if is_integer(expected) do
        length(exprs) == expected
      else
        length(exprs) in expected
      end

    if count_ok? do
      {:ok, exprs}
    else
      {:error, "Unsupported function arguments"}
    end
  end

  defp function_args(_, _), do: {:error, "Unsupported function arguments"}

  defp arg_expr(%{"Unnamed" => %{"Expr" => expr}}), do: expr
  defp arg_expr(%{"Named" => %{"expr" => expr}}), do: expr
  defp arg_expr(_), do: nil

  defp function_name([%{"Identifier" => %{"value" => value}} | _]), do: String.downcase(value)
  defp function_name(%{"Identifier" => %{"value" => value}}), do: String.downcase(value)
  defp function_name(name) when is_binary(name), do: String.downcase(name)
  defp function_name(other), do: inspect(other)

  # regexp_contains maps to token wildcard search, which is the closest
  # Quickwit equivalent of a substring match on a text field.
  defp regexp_to_ql(path, pattern) do
    core =
      pattern
      |> String.replace_prefix(".*", "")
      |> String.replace_suffix(".*", "")
      |> String.replace_prefix("^", "")
      |> String.replace_suffix("$", "")

    cond do
      core == "" ->
        {:ok, :match_all}

      regex_meta?(core) ->
        {:error,
         "regexp_contains pattern #{inspect(pattern)} contains unsupported regular expression syntax"}

      String.contains?(core, " ") ->
        {:ok, "#{path}:#{quote_string(core)}"}

      true ->
        {:ok, "#{path}:*#{core}*"}
    end
  end

  defp regex_meta?(string) do
    String.contains?(string, ["[", "]", "(", ")", "+", "{", "}", "|", "\\", "^", "$", ".", "?", "*"])
  end

  defp literal_string(%{"Value" => %{"value" => %{"SingleQuotedString" => string}}}),
    do: {:ok, string}

  defp literal_string(%{"Value" => %{"value" => %{"DoubleQuotedString" => string}}}),
    do: {:ok, string}

  defp literal_string(other), do: {:error, "Expected a string literal, got #{inspect(other)}"}

  ## Static evaluation

  defp static_eval(expr, params) when is_map(expr) do
    case expr do
      %{"Value" => %{"value" => %{"Boolean" => boolean}}} ->
        {:static, boolean}

      %{"Nested" => _} ->
        static_eval(unwrap_nested(expr), params)

      %{"UnaryOp" => %{"op" => "Not", "expr" => inner}} ->
        case static_eval(inner, params) do
          {:static, boolean} -> {:static, not boolean}
          :dynamic -> :dynamic
        end

      %{"BinaryOp" => %{"left" => left, "op" => op, "right" => right}} when op in ["And", "Or"] ->
        case {static_eval(left, params), static_eval(right, params)} do
          {{:static, a}, {:static, b}} ->
            {:static, if(op == "And", do: a and b, else: a or b)}

          _ ->
            :dynamic
        end

      %{"BinaryOp" => %{"left" => left, "op" => "Eq", "right" => right}} ->
        case {coalesce_value(left, params), coalesce_value(right, params)} do
          {{:value, a}, {:value, b}} -> {:static, a == b}
          _ -> :dynamic
        end

      %{"Case" => %{"conditions" => conditions, "else_result" => else_result}} ->
        static_case(conditions, else_result, params)

      _ ->
        :dynamic
    end
  end

  defp static_eval(_expr, _params), do: :dynamic

  defp static_case(conditions, else_result, params) do
    Enum.reduce_while(conditions, nil, fn
      %{"condition" => condition, "result" => result}, _ ->
        case static_eval(condition, params) do
          {:static, true} -> {:halt, case_result_static(result)}
          {:static, false} -> {:cont, nil}
          :dynamic -> {:halt, :dynamic}
        end

      _, _ ->
        {:cont, nil}
    end)
    |> case do
      nil ->
        if else_result do
          static_eval(else_result, params)
        else
          {:static, false}
        end

      other ->
        other
    end
  end

  # `THEN 1` / `THEN true` are truthy in boolean position.
  defp case_result_static(%{"Value" => %{"value" => %{"Boolean" => false}}}), do: {:static, false}
  defp case_result_static(_), do: {:static, true}

  # Resolves `COALESCE(@param, '')` chains down to a concrete value when every
  # argument is a literal or a present parameter.
  defp coalesce_value(%{"Function" => %{"name" => name, "args" => %{"List" => %{"args" => list}}}}, params)
       when is_list(list) do
    if function_name(name) == "coalesce" do
      exprs = Enum.map(list, &arg_expr/1)

      if Enum.any?(exprs, &is_nil/1) do
        :dynamic
      else
        coalesce_args(exprs, params)
      end
    else
      :dynamic
    end
  end

  defp coalesce_value(%{"Function" => %{}}, _params), do: :dynamic

  defp coalesce_value(%{"Identifier" => %{"value" => "@" <> name}}, params) do
    case Map.get(params, name) do
      nil -> :dynamic
      value -> {:value, coerce_param(value)}
    end
  end

  defp coalesce_value(%{"Value" => %{"value" => %{"SingleQuotedString" => string}}}, _params),
    do: {:value, string}

  defp coalesce_value(%{"Value" => %{"value" => %{"DoubleQuotedString" => string}}}, _params),
    do: {:value, string}

  defp coalesce_value(_expr, _params), do: :dynamic

  defp coalesce_args(exprs, params) do
    Enum.reduce_while(exprs, :dynamic, fn expr, :dynamic ->
      case coalesce_value(expr, params) do
        {:value, _} = value -> {:halt, value}
        :dynamic -> {:cont, :dynamic}
      end
    end)
  end

  ## Comparisons

  defp operand(%{"Identifier" => %{"value" => "@" <> name}}, _scope, params) do
    case Map.get(params, name) do
      nil -> {:error, "Missing value for parameter @#{name}"}
      value -> {:ok, {:value, coerce_param(value)}}
    end
  end

  defp operand(%{"Identifier" => %{"value" => value}} = expr, scope, params) do
    with {:ok, segments} <- identifier_segments(expr) do
      {:ok, {:field, resolve_scope(segments, scope, params)}}
    end
  end

  defp operand(%{"CompoundIdentifier" => ident} = expr, scope, params) when is_list(ident) do
    with {:ok, segments} <- identifier_segments(expr) do
      {:ok, {:field, resolve_scope(segments, scope, params)}}
    end
  end

  defp operand(%{"Cast" => %{"expr" => inner}}, scope, params), do: operand(inner, scope, params)
  defp operand(%{"Nested" => _} = expr, scope, params), do: operand(unwrap_nested(expr), scope, params)

  defp operand(%{"Value" => %{"value" => value}}, _scope, _params) do
    with {:ok, term} <- value_to_term(value) do
      {:ok, {:value, term}}
    end
  end

  defp operand(other, _scope, _params),
    do: {:error, "Unsupported comparison operand: #{inspect(other)}"}

  defp resolve_scope(segments, scope, _params) do
    case resolve_segments(segments, elem(scope, 0), elem(scope, 1)) do
      {:ok, path} -> path
      _ -> Enum.join(segments, ".")
    end
  end

  defp value_to_term(%{"Number" => [string, _]}), do: {:ok, parse_number(string)}
  defp value_to_term(%{"SingleQuotedString" => string}), do: {:ok, string}
  defp value_to_term(%{"DoubleQuotedString" => string}), do: {:ok, string}
  defp value_to_term(%{"Boolean" => boolean}), do: {:ok, boolean}
  defp value_to_term(%{"Null" => _}), do: {:error, "NULL comparisons are not supported"}
  defp value_to_term(other), do: {:error, "Unsupported literal: #{inspect(other)}"}

  defp coerce_param(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp coerce_param(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp coerce_param(value) when is_binary(value), do: value
  defp coerce_param(value) when is_number(value), do: value
  defp coerce_param(value) when is_boolean(value), do: value
  defp coerce_param(value), do: to_string(value)

  defp parse_number(string) when is_binary(string) do
    case Integer.parse(string) do
      {integer, ""} -> integer
      {_, _} -> elem(Float.parse(string), 0)
      :error -> string
    end
  end

  defp parse_number(number) when is_number(number), do: number

  # Comparison translation: one side must be a field, the other a literal.
  defp comparison_to_ql(left, op, right, scope, params) do
    with {:ok, l} <- operand(left, scope, params),
         {:ok, r} <- operand(right, scope, params) do
      case {l, r} do
        {{:field, path}, {:value, value}} ->
          field_comparison(path, op, value)

        {{:value, value}, {:field, path}} ->
          field_comparison(path, flip_op(op), value)

        {{:field, path}, {:field, other}} ->
          {:error, "Comparing two fields (#{path} #{op} #{other}) is not supported"}

        _ ->
          {:error, "Unsupported comparison"}
      end
    end
  end

  defp flip_op("Eq"), do: "Eq"
  defp flip_op("NotEq"), do: "NotEq"
  defp flip_op("Gt"), do: "Lt"
  defp flip_op("GtEq"), do: "LtEq"
  defp flip_op("Lt"), do: "Gt"
  defp flip_op("LtEq"), do: "GtEq"

  defp field_comparison(path, "Eq", value), do: {:ok, equality(path, value)}
  defp field_comparison(path, "NotEq", value), do: {:ok, "NOT (#{equality(path, value)})"}

  defp field_comparison(path, op, value) when op in ["Gt", "GtEq", "Lt", "LtEq"],
    do: {:ok, range(path, op, value)}

  defp field_comparison(_path, op, _value),
    do: {:error, "Unsupported SQL operator: #{op}"}

  defp equality(path, value) do
    cond do
      is_boolean(value) -> "#{path}:#{to_string(value)}"
      is_number(value) -> "#{path}:#{value}"
      true -> "#{path}:#{quote_string(to_string(value))}"
    end
  end

  defp range(path, op, value) do
    bounds =
      case op do
        "Gt" -> "{#{range_value(path, value)} TO *]"
        "GtEq" -> "[#{range_value(path, value)} TO *]"
        "Lt" -> "[* TO #{range_value(path, value)}}"
        "LtEq" -> "[* TO #{range_value(path, value)}]"
      end

    "#{path}:#{bounds}"
  end

  # The Quickwit timestamp field stores RFC3339 datetimes; numeric comparisons
  # against it are treated as unix microseconds.
  defp range_value(path, value) when is_number(value) do
    if path == @timestamp_field and value > 1_000_000_000_000 do
      DateTime.from_unix!(trunc(value), :microsecond) |> DateTime.to_iso8601()
    else
      to_string(value)
    end
  end

  defp range_value(_path, value) when is_binary(value), do: value
  defp range_value(_path, value) when is_boolean(value), do: to_string(value)

  defp like_ql(path, pattern) do
    if String.contains?(pattern, ["%", "_"]) do
      core = pattern |> String.replace("%", "*") |> String.replace("_", "?")

      if String.contains?(core, " ") do
        "#{path}:#{quote_string(core)}"
      else
        "#{path}:#{core}"
      end
    else
      "#{path}:#{quote_string(pattern)}"
    end
  end

  defp quote_string(string) do
    "\"" <> String.replace(to_string(string), "\"", "\\\"") <> "\""
  end

  ## Projection classification: rows / chart / total

  defp classify(final_query, final_select, scope, params) do
    items = projection_items(final_select)

    functions =
      items
      |> Enum.filter(fn {_kind, _alias, expr} -> match?(%{"Function" => _}, expr) end)

    cond do
      functions == [] ->
        rows_plan(final_query, items, scope)

      true ->
        names =
          functions
          |> Enum.map(fn {_kind, _alias, expr} ->
            function_name(get_in(expr, ["Function", "name"]))
          end)
          |> Enum.uniq()

        cond do
          names -- ["count", "timestamp_trunc"] != [] ->
            {:error,
             "Unsupported aggregation functions: #{inspect(names -- ["count", "timestamp_trunc"])}"}

          Enum.any?(functions, fn {_kind, _alias, expr} ->
            function_name(get_in(expr, ["Function", "name"])) == "timestamp_trunc"
          end) ->
            chart_plan(final_query, items, scope, params)

          true ->
            total_plan(items)
        end
    end
  end

  defp projection_items(select) do
    select
    |> Map.get("projection", [])
    |> Enum.map(fn
      %{"UnnamedExpr" => expr} ->
        {:unnamed, nil, expr}

      %{"ExprWithAlias" => %{"alias" => alias, "expr" => expr}} ->
        {:named, alias_value(alias), expr}

      other ->
        {:error, other}
    end)
  end

  defp alias_value(%{"value" => value}) when is_binary(value), do: value
  defp alias_value(%{"Identifier" => %{"value" => value}}) when is_binary(value), do: value
  defp alias_value(_), do: nil

  ## Rows shape

  defp rows_plan(final_query, items, scope) do
    with {:ok, fields} <- rows_fields(items, scope),
         {:ok, sort_by} <- order_by_ql(final_query, scope) do
      max_hits =
        final_query
        |> extract_max_hits()
        |> max(0)
        |> min(@max_max_hits)

      opts =
        [
          result_shape: :rows,
          max_hits: max_hits,
          fields: fields
        ] ++ if(sort_by, do: [sort_by: sort_by], else: [])

      {:ok, opts}
    end
  end

  defp rows_fields(items, scope) do
    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      case item do
        {_, _, %{"Wildcard" => _}} ->
          {:cont, {:ok, [:all]}}

        {:error, other} ->
          {:halt, {:error, "Unsupported SELECT projection: #{inspect(other)}"}}

        {kind, alias, expr} ->
          with {:ok, segments} <- projection_segments(expr) do
            path = resolve_scope(segments, scope, %{})
            column = column_name(kind, alias, path)
            {:cont, {:ok, acc ++ [{column, String.split(path, ".")}]}}
          else
            {:error, _} = error -> {:halt, error}
          end
      end
    end)
    |> case do
      {:ok, fields} ->
        if :all in fields do
          {:ok, :all}
        else
          {:ok, fields}
        end

      error ->
        error
    end
  end

  defp projection_segments(%{"Identifier" => _} = expr), do: identifier_segments(expr)
  defp projection_segments(%{"CompoundIdentifier" => _} = expr), do: identifier_segments(expr)
  defp projection_segments(%{"Cast" => %{"expr" => inner}}), do: projection_segments(inner)

  defp projection_segments(other),
    do: {:error, "Unsupported SELECT expression: #{inspect(other)}"}

  defp column_name(:named, alias, _path) when is_binary(alias), do: alias
  defp column_name(_kind, _alias, path), do: List.last(String.split(path, "."))

  ## Total shape: a single count(*)

  defp total_plan(items) do
    case items do
      [{kind, alias, %{"Function" => %{"name" => name, "args" => args}}}] ->
        if function_name(name) == "count" and count_args_wildcard?(args) do
          column = column_name(kind, alias, "count")

          agg = %{
            "type" => "metric",
            "name" => "total_count",
            "field" => @timestamp_field,
            "metric" => "count"
          }

          {:ok,
           [
             result_shape: :total,
             max_hits: 0,
             aggs: [agg],
             total_alias: column
           ]}
        else
          {:error, "Only count(*) is supported without timestamp_trunc"}
        end

      _ ->
        {:error, "SELECT count(*) must be the only projection"}
    end
  end

  defp count_args_wildcard?(%{"List" => %{"args" => []}}), do: true
  defp count_args_wildcard?(%{"List" => %{"args" => [%{"Unnamed" => "Wildcard"}]}}), do: true
  defp count_args_wildcard?(_), do: false

  ## Chart shape: timestamp_trunc histogram + count(CASE ...) columns

  defp chart_plan(final_query, items, scope, params) do
    with {:ok, {hist_column, hist_field, granularity}} <- histogram(items, scope),
         {:ok, counters} <- counters(items, scope, params),
         {:ok, bucket_order} <- chart_order(final_query, hist_column, hist_field) do
      term_names =
        counters
        |> Enum.flat_map(fn {_column, {:field, path, _ast}} -> [path] end)
        |> Enum.uniq()
        |> Enum.with_index()
        |> Map.new(fn {path, index} -> {path, "field#{index}"} end)

      term_aggs =
        Enum.map(term_names, fn {path, name} ->
          %{"type" => "term", "name" => name, "field" => path, "size" => @term_size}
        end)

      counters =
        Enum.map(counters, fn
          {column, {:static, boolean}} ->
            {column, {:static, boolean}}

          {column, {:field, path, ast}} ->
            {column, {:field, Map.fetch!(term_names, path), path, ast}}
        end)

      histogram_agg =
        %{
          "type" => "date_histogram",
          "name" => @histogram_name,
          "field" => hist_field,
          "interval" => "1 #{granularity}",
          "format" => "epoch_millis"
        }
        |> then(&if(term_aggs == [], do: &1, else: Map.put(&1, "aggs", term_aggs)))

      {:ok,
       [
         result_shape: :chart,
         max_hits: 0,
         aggs: [histogram_agg],
         counters: counters,
         histogram_field: hist_field,
         bucket_order: bucket_order
       ]}
    end
  end

  defp histogram(items, scope) do
    items
    |> Enum.find(fn
      {_kind, _alias, %{"Function" => %{"name" => name}}} ->
        function_name(name) == "timestamp_trunc"

      _ ->
        false
    end)
    |> case do
      nil ->
        {:error, "Chart queries require timestamp_trunc()"}

      {kind, alias, function} ->
        with {:ok, [field_expr, gran_expr]} <- function_args(function, 2),
             {:ok, {:field, path}} <- operand(field_expr, scope, %{}),
             {:ok, granularity} <- granularity(gran_expr) do
          column =
            case kind do
              :named when is_binary(alias) -> alias
              _ -> "timestamp"
            end

          {:ok, {column, path, granularity}}
        end
    end
  end

  defp granularity(%{"Identifier" => %{"value" => value}}) when value in @granularities,
    do: {:ok, value}

  defp granularity(%{"Value" => %{"value" => value}}) do
    case value do
      %{"Keyword" => [name, _]} when name in @granularities -> {:ok, name}
      name when is_binary(name) and name in @granularities -> {:ok, name}
      other -> {:error, "Unsupported timestamp_trunc granularity: #{inspect(other)}"}
    end
  end

  defp granularity(other),
    do: {:error, "Unsupported timestamp_trunc granularity: #{inspect(other)}"}

  defp counters(items, scope, params) do
    items
    |> Enum.filter(fn
      {_kind, _alias, %{"Function" => %{"name" => name}}} -> function_name(name) == "count"
      _ -> false
    end)
    |> Enum.reduce_while({:ok, []}, fn {kind, alias, function}, {:ok, acc} ->
      with {:ok, args} <- function_args(function, 0..1),
           {:ok, spec} <- counter_spec(args, scope, params) do
        column = column_name(kind, alias, "count")
        {:cont, {:ok, acc ++ [{column, spec}]}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp counter_spec([], _scope, _params), do: {:ok, {:static, true}}

  defp counter_spec([%{"Unnamed" => "Wildcard"}], _scope, _params), do: {:ok, {:static, true}}

  defp counter_spec([condition], scope, params) do
    case static_eval(condition, params) do
      {:static, boolean} ->
        {:ok, {:static, boolean}}

      :dynamic ->
        with {:ok, [path]} <- condition_fields(condition, scope, params) do
          ast = compile_condition(condition, scope)
          {:ok, {:field, path, ast}}
        end
    end
  end

  # Collect the distinct document fields referenced by a count(CASE ...) condition.
  defp condition_fields(expr, scope, params) do
    {paths, params?} =
      expr
      |> walk_exprs()
      |> Enum.flat_map(fn
        %{"Identifier" => %{"value" => "@" <> _}} -> [{:param, nil}]
        %{"Identifier" => %{"value" => value}} -> [{:field, [value]}]

        %{"CompoundIdentifier" => ident} when is_list(ident) ->
          [{:field, Enum.map(ident, & &1["Identifier"]["value"])}]

        _ ->
          []
      end)
      |> Enum.split_with(&match?({:field, _}, &1))

    if params? != [] do
      {:error, "count(CASE ...) conditions cannot reference @params"}
    else
      paths =
        paths
        |> Enum.map(fn {:field, segments} -> resolve_scope(segments, scope, %{}) end)
        |> Enum.uniq()

      {:ok, paths}
    end
  end

  # Rewrite identifier references in a count(CASE ...) condition to their
  # resolved document paths, so the adaptor can evaluate bucket keys directly.
  defp compile_condition(expr, scope) when is_map(expr) do
    case identifier_segments(expr) do
      {:ok, segments} ->
        path = resolve_scope(segments, scope, %{})
        %{"Identifier" => %{"value" => path, "quote_style" => nil}}

      {:error, _} ->
        Map.new(expr, fn
          {key, value} when is_map(value) -> {key, compile_condition(value, scope)}
          {key, values} when is_list(values) -> {key, Enum.map(values, &compile_condition(&1, scope))}
          {key, value} -> {key, value}
        end)
    end
  end

  defp compile_condition(list, scope) when is_list(list),
    do: Enum.map(list, &compile_condition(&1, scope))

  defp compile_condition(other, _scope), do: other

  defp walk_exprs(expr) when is_map(expr) do
    [expr] ++
      Enum.flat_map(expr, fn
        {_key, value} when is_map(value) -> walk_exprs(value)
        {_key, values} when is_list(values) -> Enum.flat_map(values, &walk_exprs/1)
        _ -> []
      end)
  end

  defp walk_exprs(_), do: []

  defp chart_order(final_query, hist_column, hist_field) do
    case order_expressions(final_query) do
      [%{"expr" => expr, "options" => %{"sort" => sort}}] ->
        path =
          case expr do
            %{"Identifier" => %{"value" => value}} -> value

            %{"CompoundIdentifier" => ident} when is_list(ident) ->
              Enum.map_join(ident, ".", & &1["Identifier"]["value"])

            _ ->
              nil
          end

        cond do
          is_nil(path) ->
            {:ok, :asc}

          path == hist_column or path == hist_field or
              List.last(String.split(path, ".")) == List.last(String.split(hist_field, ".")) ->
            if sort == "Desc", do: {:ok, :desc}, else: {:ok, :asc}

          true ->
            {:error, "Chart queries can only ORDER BY the histogram timestamp column"}
        end

      order when order in [[], nil] ->
        {:ok, :asc}

      _ ->
        {:error, "Chart queries support at most one ORDER BY expression"}
    end
  end

  ## LIMIT / ORDER BY (rows)

  defp extract_max_hits(final_query) do
    case get_in(final_query, ["limit_clause", "LimitOffset", "limit"]) do
      %{"Value" => %{"value" => %{"Number" => [string, _]}}} -> String.to_integer(string)
      _ -> @default_max_hits
    end
  end

  defp order_by_ql(final_query, scope) do
    case order_expressions(final_query) do
      [%{"expr" => expr, "options" => %{"sort" => sort}}] ->
        with {:ok, segments} <- identifier_segments(expr) do
          path = resolve_scope(segments, scope, %{})
          if sort == "Desc", do: {:ok, "-#{path}"}, else: {:ok, path}
        end

      order when order in [[], nil] ->
        {:ok, nil}

      _ ->
        {:error, "ORDER BY supports at most one expression"}
    end
  end

  # ORDER BY items nest their expression under "kind" => {"Expressions" => [...]}
  defp order_expressions(final_query) do
    final_query
    |> Map.get("order_by", [])
    |> Enum.flat_map(fn
      %{"kind" => %{"Expressions" => exprs}} when is_list(exprs) -> exprs
      %{"expr" => _} = item -> [item]
      _ -> []
    end)
  end

  ## Result shaping helpers used by the adaptor

  @doc """
  Classifies a term aggregation bucket value against a count(CASE ...) condition.

  `raw` is the bucket key from the Quickwit term aggregation response (a string
  or number); the comparison falls back to string equality when either side is
  not numeric.
  """
  @spec condition_matches?(map(), String.t(), term()) :: boolean()
  def condition_matches?(condition, field_path, raw) do
    eval_condition(condition, field_path, raw)
  end

  defp eval_condition(%{"BinaryOp" => %{"left" => left, "op" => op, "right" => right}}, path, raw) do
    case op do
      "And" -> eval_condition(left, path, raw) and eval_condition(right, path, raw)
      "Or" -> eval_condition(left, path, raw) or eval_condition(right, path, raw)
      "Eq" -> compare_condition(left, right, path, raw)
      "NotEq" -> not compare_condition(left, right, path, raw)
      comparison_op when comparison_op in ["Gt", "GtEq", "Lt", "LtEq"] ->
        compare_condition(left, right, path, raw, comparison_op)
      _ ->
        false
    end
  end

  defp eval_condition(%{"Nested" => _} = expr, path, raw),
    do: eval_condition(unwrap_nested(expr), path, raw)

  defp eval_condition(%{"UnaryOp" => %{"op" => "Not", "expr" => expr}}, path, raw),
    do: not eval_condition(expr, path, raw)

  defp eval_condition(%{"InList" => %{"expr" => expr, "list" => list}}, path, raw) do
    Enum.any?(list, fn item -> compare_condition(expr, item, path, raw) end)
  end

  defp eval_condition(%{"Value" => %{"value" => %{"Boolean" => boolean}}}, _path, _raw),
    do: boolean

  defp eval_condition(_expr, _path, _raw), do: false

  defp compare_condition(left, right, path, raw, op \\ "Eq") do
    with {:ok, {lhs, rhs}} <- condition_sides(left, right, path, raw) do
      numeric_compare?(lhs, rhs, op)
    else
      _ -> false
    end
  end

  # One side must be the aggregated field, the other a literal.
  defp condition_sides(left, right, path, raw) do
    lhs = condition_operand(left, path, raw)
    rhs = condition_operand(right, path, raw)

    case {lhs, rhs} do
      {{:field, ^path}, {:value, value}} -> {:ok, {raw, value}}
      {{:value, value}, {:field, ^path}} -> {:ok, {raw, value}}
      {{:field, _}, _} -> {:error, :field_mismatch}
      _ -> {:error, :unsupported}
    end
  end

  defp condition_operand(%{"Identifier" => %{"value" => value}}, path, _raw) do
    if String.starts_with?(value, "@") do
      :error
    else
      {:field, value}
    end
  end

  defp condition_operand(%{"Cast" => %{"expr" => inner}}, path, raw),
    do: condition_operand(inner, path, raw)

  defp condition_operand(%{"Value" => %{"value" => value}}, _path, _raw) do
    case value_to_term(value) do
      {:ok, term} -> {:value, term}
      _ -> :error
    end
  end

  defp condition_operand(_expr, _path, _raw), do: :error

  defp numeric_compare?(lhs, rhs, op) do
    l = number_or_string(lhs)
    r = number_or_string(rhs)

    case {l, r} do
      {l, r} when is_number(l) and is_number(r) ->
        order(op, l, r)

      {l, r} ->
        order(op, to_string(l), to_string(r))
    end
  end

  defp order("Eq", l, r), do: l == r
  defp order("NotEq", l, r), do: l != r
  defp order("Gt", l, r), do: l > r
  defp order("GtEq", l, r), do: l >= r
  defp order("Lt", l, r), do: l < r
  defp order("LtEq", l, r), do: l <= r

  defp number_or_string(value) when is_number(value), do: value

  defp number_or_string(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      {_, _} -> elem(Float.parse(value), 0)
      :error -> value
    end
  end

  defp number_or_string(value) when is_boolean(value), do: to_string(value)
  defp number_or_string(value), do: to_string(value)
end