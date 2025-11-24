from __future__ import annotations

import jq as jqm
import jsonschema
import polars as pl

from apscheduler.triggers.cron import CronTrigger
from datetime import timezone
from enum import StrEnum
from loguru import logger
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError
from typing import Any, Callable, cast

from context.context import (
    CommandContext,
    CreateHTTPLookupTableContext,
    CreateHTTPTableContext,
    CreateWSSourceContext,
    CreateWSTableContext,
    CreateHTTPSourceContext,
    CreateSinkContext,
    CreateViewContext,
    CreateSecretContext,
    InvalidContext,
    ValidContext,
    SelectContext,
    SetContext,
    ShowContext,
    DropContext,
    DropSimpleContext,
    DropCascadeContext,
)
from store.db import (
    METADATA_TABLE_TABLE_NAME,
    METADATA_VIEW_TABLE_NAME,
    METADATA_SINK_TABLE_NAME,
    METADATA_SECRET_TABLE_NAME,
    METADATA_SOURCE_TABLE_NAME,
)
from sql.dialect import OmlspDialect, GENERATED_COLUMN_FUNCTION_DISPATCH
from sql.types import (
    SourceHttpProperties,
    SourceWSProperties,
    HttpMethod,
    JQ,
)
from utils import unnest_dict


class CreateKind(StrEnum):
    SINK = "SINK"
    TABLE = "TABLE"
    SOURCE = "SOURCE"
    VIEW = "VIEW"
    SECRET = "SECRET"


class SourceConnectorKind(StrEnum):
    HTTP = "http"
    LOOKUP_HTTP = "lookup-http"
    WS = "ws"


def build_generated_column_func(
    expr: exp.Expression, dtype: exp.DataType
) -> Callable[[dict[str, Any]], pl.Expr]:
    """
    Pre-compile column constraint kind.
    This recursively build nested lambda function.

    Example with pure function expr:
    Paren(
        this=Func(this=START_TIME)
    )

    Example with pure column expression:
    this=Paren(
        this=Column(
                this=Identifier(this=symbol, quoted=False)
        )
    )

    Example with composed expression:
    Paren(
      this=Sub(
        this=Func(this=TRIGGER_TIME),
        expression=Interval(
          this=Literal(this='5', is_string=True),
          unit=Var(this=MINUTES)
        )
      )
    )
    """

    # Recursively build a closure tree that *calls context only when needed*
    def _compile(node: exp.Expression) -> Callable[[dict[str, Any]], pl.Expr]:
        if isinstance(node, exp.Column):
            return lambda ctx: pl.col(node.name)

        elif isinstance(node, exp.Literal):
            val = node.this
            if node.is_string:
                return lambda ctx, v=str(val): pl.lit(v)
            elif str(val).isdigit():
                return lambda ctx, v=int(val): pl.lit(v)
            return lambda ctx, v=val: pl.lit(v)

        elif isinstance(node, exp.Func):
            # TODO: improve, function should have a "name" parameter
            # instead of serializing by node value (generated)
            name = str(node).upper()
            if name not in GENERATED_COLUMN_FUNCTION_DISPATCH:
                raise NotImplementedError(f"Unknown function `{name}`")
            func = GENERATED_COLUMN_FUNCTION_DISPATCH[name]
            # Inject dtype in the compiled lambda to be used at runtime
            return lambda ctx, f=func: f(ctx, dtype)

        elif isinstance(node, exp.Add):
            left = _compile(node.this)
            right = _compile(node.expression)
            return lambda ctx: left(ctx) + right(ctx)

        elif isinstance(node, exp.Sub):
            left = _compile(node.this)
            right = _compile(node.expression)
            return lambda ctx: left(ctx) - right(ctx)

        elif isinstance(node, exp.Mul):
            left = _compile(node.this)
            right = _compile(node.expression)
            return lambda ctx: left(ctx) * right(ctx)

        elif isinstance(node, exp.Div):
            left = _compile(node.this)
            right = _compile(node.expression)
            return lambda ctx: left(ctx) / right(ctx)

        elif isinstance(node, exp.Interval):
            # Example: INTERVAL '5' MINUTE
            # node.this.this → literal value, node.args["unit"] → unit token
            amount_node = node.this
            amount = (
                int(amount_node.this) if isinstance(amount_node, exp.Literal) else 0
            )
            unit_expr = node.args.get("unit")
            unit_name = unit_expr.name if unit_expr else "seconds"

            # Normalize to seconds
            seconds = amount * {
                "SECOND": 1,
                "SECONDS": 1,
                "MINUTE": 60,
                "MINUTES": 60,
                "HOUR": 3600,
                "HOURS": 3600,
                "DAY": 86400,
                "DAYS": 86400,
            }.get(unit_name.upper(), 1)

            return lambda ctx, s=seconds: pl.duration(seconds=s)

        elif isinstance(node, exp.Paren):
            inner = _compile(node.this)
            return lambda ctx: inner(ctx)

        else:
            logger.error("Unsupported: {}", type(node))
            raise NotImplementedError(f"Unsupported: {type(node)}")

    # compile once:
    compiled = _compile(expr)

    return compiled


def get_name(expression: exp.Expression) -> str:
    return getattr(getattr(expression, "this", expression), "name", "")


def get_property(expression: exp.Property) -> exp.Expression | None:
    return expression.args.get("value")


def rename_column_transform(node, old_name, new_name):
    if isinstance(node, exp.Column) and node.name == old_name:
        # If it's a direct column reference
        node.this.set("this", new_name)
    elif isinstance(node, exp.Alias) and node.this.name == old_name:
        # If it's an aliased column, rename the alias
        node.set("alias", new_name)
    return node


def parse_table_schema(
    table: exp.Schema,
) -> tuple[exp.Schema, str, list[str], dict[str, str], dict[str, Callable]]:
    """Parse table schema expression into required metadata"""
    table_name = table.this.name
    table = table.copy()

    column_types: dict[str, str] = {}
    columns = []
    generated_columns: dict[str, Callable] = {}
    for col in table.expressions:
        # Parse Columns name + type (and optionally generated columns)
        col_name, column_type, generated_column = parse_column_def(col)
        column_types[col_name] = column_type

        # If generated column exist, remove it from sql
        # for duckdb sql generation
        if generated_column:
            generated_columns[col_name] = generated_column
            col.set("constraints", None)

    return table, table_name, columns, column_types, generated_columns


def parse_lookup_table_schema(
    table: exp.Schema,
) -> tuple[exp.Schema, str, dict[str, str], list[str], dict[str, Callable]]:
    # Parse temporary table schema (lookup).
    # Extract parameters for dynamic eval
    table_name = table.this.name
    table = table.copy()

    column_types: dict[str, str] = {}
    lookup_fields = []
    generated_columns: dict[str, Callable] = {}

    for col in table.expressions:
        # Parse Columns name + type (and optionally generated columns)
        col_name, column_type, generated_column = parse_column_def(col)

        new_col_name = str(col_name).replace("$", "")

        if col_name.startswith("$"):
            lookup_fields.append(new_col_name)

        col.set("this", exp.to_identifier(new_col_name))
        column_types[new_col_name] = column_type

        # If generated column exist, remove it from sql
        # for duckdb sql generation
        if generated_column:
            generated_columns[new_col_name] = generated_column
            col.set("constraints", None)

    return table, table_name, column_types, lookup_fields, generated_columns


def parse_column_def(
    col: exp.ColumnDef,
) -> tuple[str, str, Callable[[dict[str, Any]], pl.Expr] | None]:
    col_name = str(col.name)
    col_type: exp.DataType = col.args.get("kind")  # type: ignore
    column_type = str(col_type).upper()
    generated_column: None | Callable[[dict[str, Any]], pl.Expr] = None

    # Parse Generated columns referred as "constraints" in sqlglot
    if col.constraints:
        # A ColumnDef can have multiple constraints, we only support the
        # first one for now, then we extract it's "this" expression
        generated_column = build_generated_column_func(
            col.constraints[0].kind.this, col_type
        )

    return col_name, column_type, generated_column


def parse_ws_table_schema(
    table: exp.Schema,
) -> tuple[exp.Schema, str, dict[str, str], dict[str, Callable]]:
    # Parse web socket table schema.
    table_name = table.this.name
    table = table.copy()

    column_types: dict[str, str] = {}
    generated_columns: dict[str, Callable] = {}
    for col in table.expressions:
        # Parse Columns name + type (and optionally generated columns)
        col_name, column_type, generated_column = parse_column_def(col)
        column_types[col_name] = column_type

        # If generated column exist, remove it from sql
        # for duckdb sql generation
        if generated_column:
            generated_columns[col_name] = generated_column
            col.set("constraints", None)

    return table, table_name, column_types, generated_columns


def validate_create_properties(
    properties: dict[str, str], properties_schema: dict
) -> str | None:
    # validate properties according to json schema
    try:
        return jsonschema.validate(instance=properties, schema=properties_schema)
    except Exception as e:
        return f"JSON schema validation on properties failed with error: {e}"


def get_duckdb_sql(statement: exp.Create | exp.Select | exp.Set | exp.Command) -> str:
    statement = statement.copy()

    # remove known "non duckdb" compliant sql
    if statement.args.get("properties"):
        del statement.args["properties"]

    return statement.sql("duckdb")


def extract_create_properties(statement: exp.Create) -> dict[str, str]:
    properties = {}
    for prop in statement.args.get("properties", []):
        # sqlglot pushes "temporary" predicate
        # in properties, needs to ignore it
        if not prop or str(prop) == "TEMPORARY":
            continue

        # regular properties are divided in "this" and "value"
        properties[prop.this.this] = prop.args.get("value").this

    return properties


def build_create_http_properties(properties: dict[str, str]) -> SourceHttpProperties:
    unnested_properties = unnest_dict(properties)

    return SourceHttpProperties(
        url=str(unnested_properties["url"]),
        method=cast(HttpMethod, unnested_properties["method"]),
        signer_class=str(unnested_properties.get("signer_class", "")),
        jq=cast(JQ, jqm.compile(unnested_properties["jq"])),
        pagination=cast(dict, unnested_properties.get("pagination", {})),
        headers=cast(dict, unnested_properties.get("headers", {})),
        json=cast(dict, unnested_properties.get("json", {})),
        params=cast(dict, unnested_properties.get("params", {})),
    )


def build_create_http_table_or_source_context(
    statement: exp.Create, properties: dict[str, str]
) -> CreateHTTPTableContext | CreateHTTPSourceContext:
    # avoid side effect, leverage sqlglot ast copy
    statement = statement.copy()

    # parse table schema and update exp.Schema
    new_expr_schema, table_name, _, column_types, generated_columns = (
        parse_table_schema(statement.this)
    )

    # overwrite modified table statement
    statement.set("this", new_expr_schema)
    # extract source for correct typing
    is_source = str(statement.kind) == "SOURCE"
    # restore table for state creation
    statement.set("kind", "TABLE")

    # cron schedule for http
    trigger = CronTrigger.from_crontab(
        str(properties.pop("schedule")), timezone=timezone.utc
    )

    # get query purged of it's omlsp specific syntax
    duckdb_query = get_duckdb_sql(statement)

    if is_source:
        return CreateHTTPSourceContext(
            name=table_name,
            properties=build_create_http_properties(properties),
            query=duckdb_query,
            column_types=column_types,
            generated_columns=generated_columns,
            trigger=trigger,
        )
    return CreateHTTPTableContext(
        name=table_name,
        properties=build_create_http_properties(properties),
        query=duckdb_query,
        column_types=column_types,
        generated_columns=generated_columns,
        trigger=trigger,
    )


def build_create_http_lookup_table_context(
    statement: exp.Create, properties: dict[str, str]
) -> CreateHTTPLookupTableContext:
    # avoid side effect, leverage sqlglot ast copy
    statement = statement.copy()

    # get updated exp.Schema (to extract sql later)
    # columns and dynamic columns are extracted for
    # duckdb scalar function & macros
    new_expr_schema, table_name, column_types, dynamic_columns, generated_columns = (
        parse_lookup_table_schema(statement.this)
    )

    # get updated exp.Create (to extract sql later)
    statement.set("this", new_expr_schema)

    return CreateHTTPLookupTableContext(
        name=table_name,
        properties=build_create_http_properties(properties),
        query=get_duckdb_sql(statement),
        column_types=column_types,
        lookup_fields=dynamic_columns,
        generated_columns=generated_columns,
    )


def build_create_ws_properties(properties: dict[str, str]) -> SourceWSProperties:
    unnested_properties = unnest_dict(properties)

    return SourceWSProperties(
        url=str(unnested_properties["url"]),
        jq=cast(JQ, jqm.compile(unnested_properties["jq"])),
        on_start_query=str(unnested_properties.get("on_start_query", "")),
    )


def build_create_ws_table_context(
    statement: exp.Create, properties: dict[str, str]
) -> CreateWSSourceContext | CreateWSTableContext:
    # avoid side effect, leverage sqlglot ast copy
    statement = statement.copy()

    new_expr_schema, table_name, column_types, generated_columns = (
        parse_ws_table_schema(statement.this)
    )

    # overwrite modified table statement
    statement.set("this", new_expr_schema)
    # extract source for correct typing
    is_source = str(statement.kind) == "SOURCE"
    # restore table for state creation
    statement.set("kind", "TABLE")

    # get query purged of it's omlsp specific syntax
    duckdb_query = get_duckdb_sql(statement)

    on_start_query = properties.pop("on_start_query", "")

    if is_source:
        return CreateWSSourceContext(
            name=table_name,
            properties=build_create_ws_properties(properties),
            column_types=column_types,
            generated_columns=generated_columns,
            query=duckdb_query,
            on_start_query=on_start_query,
        )

    return CreateWSTableContext(
        name=table_name,
        properties=build_create_ws_properties(properties),
        column_types=column_types,
        generated_columns=generated_columns,
        query=duckdb_query,
        on_start_query=on_start_query,
    )


def build_create_sink_context(
    statement: exp.Create, properties_schema: dict[str, Any]
) -> CreateSinkContext | InvalidContext:
    # extract subquery
    ctx = extract_select_context(statement.expression)
    if isinstance(ctx, InvalidContext):
        return ctx

    # extract list of exp.Property
    # declared behind sql WITH statement
    properties = extract_create_properties(statement)

    # validate properties against schema
    # if not ok, return invalid context
    nok = validate_create_properties(properties, properties_schema)
    if nok:
        return InvalidContext(reason=nok)

    # avoid side effect, leverage sqlglot ast copy
    statement = statement.copy()
    expr = statement.expression

    # TODO: support multiple upstreams merged/unioned
    ctx = extract_select_context(expr)
    if isinstance(ctx, SelectContext):
        upstreams = [ctx.table]
    else:
        return InvalidContext(reason=f"Unsupported sink query: {expr}")

    return CreateSinkContext(
        name=statement.this.name,
        upstreams=upstreams,
        properties=properties,
        subquery=ctx.query,
    )


def build_create_view_context(
    statement: exp.Create,
) -> CreateViewContext | InvalidContext:
    name = statement.this.name

    ctx = extract_select_context(statement.expression)
    if isinstance(ctx, InvalidContext):
        return ctx

    # TODO: support multiple upstreams merged/unioned
    # TODO: add where clause
    upstreams = [ctx.table]

    statement.args["kind"] = "TABLE"
    query = get_duckdb_sql(statement)

    # sqlglot captures MATERIALIZED/NOT MATERIALIZED as a property
    properties = statement.args.get("properties")
    if properties:
        for prop in properties.expressions:
            if isinstance(prop, exp.MaterializedProperty):
                return CreateViewContext(
                    name=name,
                    upstreams=upstreams,
                    query=query,
                    transform_ctx=ctx,
                    materialized=True,
                )

    return CreateViewContext(
        name=name,
        upstreams=upstreams,
        query=query,
        transform_ctx=ctx,
        materialized=False,
    )


def build_generic_create_table_or_source_context(
    statement: exp.Create, properties_schema: dict
) -> (
    CreateHTTPTableContext
    | CreateHTTPSourceContext
    | CreateHTTPLookupTableContext
    | CreateWSSourceContext
    | CreateWSTableContext
    | InvalidContext
):
    # extract list of exp.Property
    # declared behind sql WITH statement
    properties = extract_create_properties(statement)

    # validate properties against schema
    # if not ok, return invalid context
    nok = validate_create_properties(properties, properties_schema)
    if nok:
        return InvalidContext(reason=f"Failed to validate properties: {nok}")

    connector = properties.pop("connector")

    if connector == SourceConnectorKind.LOOKUP_HTTP.value:
        return build_create_http_lookup_table_context(statement, properties)

    if connector == SourceConnectorKind.HTTP.value:
        return build_create_http_table_or_source_context(statement, properties)

    if connector == SourceConnectorKind.WS.value:
        return build_create_ws_table_context(statement, properties)

    return InvalidContext(
        reason=f"Connector from properties '{connector}' is unknown: {properties}"
    )


def build_create_secret_context(
    statement: exp.Create, properties_schema: dict
) -> CreateSecretContext | InvalidContext:
    properties = extract_create_properties(statement)

    # validate properties against schema
    # if not ok, return invalid context
    nok = validate_create_properties(properties, properties_schema)
    if nok:
        return InvalidContext(reason=nok)

    value = str(statement.expression)

    return CreateSecretContext(
        name=statement.name, properties=properties, value=str(value)
    )


def extract_create_context(
    statement: exp.Create, properties_schema: dict
) -> (
    CreateHTTPTableContext
    | CreateHTTPSourceContext
    | CreateHTTPLookupTableContext
    | CreateWSSourceContext
    | CreateWSTableContext
    | CreateSinkContext
    | CreateViewContext
    | CreateSecretContext
    | InvalidContext
):
    if (
        str(statement.kind) == CreateKind.TABLE.value
        or str(statement.kind) == CreateKind.SOURCE.value
    ):
        return build_generic_create_table_or_source_context(
            statement, properties_schema
        )

    elif str(statement.kind) == CreateKind.SINK.value:
        return build_create_sink_context(statement, properties_schema)

    elif str(statement.kind) == CreateKind.VIEW.value:
        return build_create_view_context(statement)

    elif str(statement.kind) == CreateKind.SECRET.value:
        return build_create_secret_context(statement, properties_schema)

    return InvalidContext(
        reason=f"Query provided is not a valid statement: {statement.kind}"
    )


def get_table_name_placeholder(table_name: str):
    return "$" + table_name


def parse_select(
    statement: exp.Select,
) -> tuple[exp.Select, list[str], str, str, str, dict[str, str]]:
    """
    Select parsing function. Extract necessary attributes of the query and expose.
    """
    columns = []
    table_name = ""
    table_alias = ""
    where = ""
    join_tables = {}  # can be unique

    statement = statement.copy()

    for projection in statement.expressions:
        col_name = get_name(projection)
        columns.append(str(col_name))

    from_clause = statement.args.get("from")
    if from_clause:
        table_expr = from_clause.this
        table_name = str(table_expr.name)
        table_alias = str(table_expr.alias_or_name)

    where_clause = statement.args.get("where")
    if where_clause:
        where = where_clause.sql(dialect=None)

    # Deal with lookup tables
    for table in statement.find_all(exp.Table):
        if isinstance(table.parent, exp.Join):
            join_tables[str(table.name)] = str(table.alias_or_name)

        if table.name:  # filter away scalar functions / macros
            table.set(
                "this",
                exp.to_identifier(get_table_name_placeholder(table.name), False),
            )

    return statement, columns, table_name, table_alias, where, join_tables


def extract_select_context(statement: exp.Select) -> SelectContext | InvalidContext:
    """
    Parse a SELECT query into a dictionary
    """
    has_cte = statement.args.get("with") is not None
    has_subquery = any(
        isinstance(node, exp.Subquery) for node in statement.find_all(exp.Subquery)
    )
    if has_cte or has_subquery:
        return InvalidContext(
            reason="Detected invalid WITH statement or subquery usage."
        )

    updated_select_statement, columns, table, alias, where, joins = parse_select(
        statement
    )

    return SelectContext(
        columns=columns,
        table=table,
        alias=alias,
        where=where,
        joins=joins,
        query=get_duckdb_sql(updated_select_statement),
    )


def extract_set_context(statement: exp.Set) -> SetContext:
    assert isinstance(statement, exp.Set), (
        f"Unexpected statement of type {type(statement)}, expected exp.Set statement"
    )

    return SetContext(query=get_duckdb_sql(statement))


def extract_show_statement(statement: exp.Show) -> ShowContext:
    query = "SELECT * FROM "
    sql = statement.sql(dialect=OmlspDialect)
    if "TABLES" in sql:
        query += METADATA_TABLE_TABLE_NAME

    elif "VIEWS" in sql:
        query += METADATA_VIEW_TABLE_NAME

    elif "SECRETS" in sql:
        query += METADATA_SECRET_TABLE_NAME

    elif "SINKS" in sql:
        query += METADATA_SINK_TABLE_NAME

    elif "SOURCES" in sql:
        query += METADATA_SOURCE_TABLE_NAME

    return ShowContext(user_query=sql, query=query)


def extract_drop_statement(statement: exp.Drop) -> DropContext | InvalidContext:
    sql = statement.sql(dialect=OmlspDialect)
    drop_type = statement.kind
    if drop_type is None:
        return InvalidContext(reason=f"Unknown drop type - {drop_type}")

    sql_parts = sql.replace(";", "").split()
    # validate enough tokens exist
    if len(sql_parts) < 3:
        return InvalidContext(reason=f"Malformed DROP statement - {sql}")

    cascade = bool(statement.args.get("cascade", False))

    if cascade:
        return DropCascadeContext(
            drop_type=drop_type,
            name=sql_parts[-2],
        )

    return DropSimpleContext(
        drop_type=drop_type,
        name=sql_parts[-1],
    )


def extract_command_context(
    statement: exp.Command,
) -> CommandContext | InvalidContext:
    sql_string = get_duckdb_sql(statement).strip().upper()

    return CommandContext(query=sql_string)


def parse_with_sqlglot(query: str) -> exp.Expression | ParseError:
    try:
        parsed_statement = parse_one(query, dialect=OmlspDialect)
        return parsed_statement
    except ParseError as e:
        logger.error(e)
        return e


def extract_one_query_context(
    query: str, properties_schema: dict
) -> ValidContext | InvalidContext:
    parsed_statement = parse_with_sqlglot(query)

    if isinstance(parsed_statement, exp.Command):
        return extract_command_context(parsed_statement)

    elif isinstance(parsed_statement, exp.Create):
        return extract_create_context(parsed_statement, properties_schema)

    elif isinstance(parsed_statement, exp.Select):
        return extract_select_context(parsed_statement)

    elif isinstance(parsed_statement, exp.Set):
        return extract_set_context(parsed_statement)

    elif isinstance(parsed_statement, exp.Show):
        return extract_show_statement(parsed_statement)

    elif isinstance(parsed_statement, exp.Drop):
        return extract_drop_statement(parsed_statement)

    elif isinstance(parsed_statement, exp.With):
        return InvalidContext(reason="CTE statement (i.e WITH ...) is not accepted")

    elif isinstance(parsed_statement, ParseError):
        return InvalidContext(reason=f"Parsing error: {parsed_statement}")

    return InvalidContext(
        reason=f"Unknown statement {type(parsed_statement)} - {parsed_statement}"
    )
