from __future__ import annotations

from enum import StrEnum
from typing import Any
from datetime import timezone

import jsonschema
from apscheduler.triggers.cron import CronTrigger
from sqlglot import exp, parse_one

from context.context import (
    CommandContext,
    CreateHTTPLookupTableContext,
    CreateHTTPTableContext,
    CreateWSTableContext,
    CreateSinkContext,
    CreateViewContext,
    CreateMaterializedViewContext,
    CreateSecretContext,
    InvalidContext,
    QueryContext,
    NonQueryContext,
    SelectContext,
    SetContext,
    ShowContext,
)
from metadata.db import (
    METADATA_TABLE_TABLE_NAME,
    METADATA_VIEW_TABLE_NAME,
    METADATA_VIEW_MATERIALIZED_TABLE_NAME,
    METADATA_SINK_TABLE_NAME,
    METADATA_SECRET_TABLE_NAME,
)
from sql.dialect import OmlspDialect


def get_start_time():
    pass


GENERATED_COLUMN_DISPATCH = {"START_TIME": lambda: get_start_time}


class CreateKind(StrEnum):
    SINK = "SINK"
    TABLE = "TABLE"
    VIEW = "VIEW"
    SECRET = "SECRET"


class TableConnectorKind(StrEnum):
    HTTP = "http"
    LOOKUP_HTTP = "lookup-http"
    WS = "ws"


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
) -> tuple[exp.Schema, str, list[str], dict[str, str], dict[str, str]]:
    """Parse table schema into name and columns"""
    table_name = get_name(table)
    table = table.copy()

    column_types: dict[str, str] = {}
    columns = []
    generated_columns: dict[str, str] = {}
    for col in table.expressions:
        # Parse Columns name + type (and optionally generated columns)
        if isinstance(col, exp.ColumnDef):
            col_name = str(col.name)
            col_type = col.args.get("kind")  # data type
            column_types[col_name] = str(col_type).upper()

            # Parse Generated columns referred as
            # "constraints" in sqlglot
            if col.constraints:
                # Example with function:
                # [
                #     ColumnConstraint(
                #         kind=ComputedColumnConstraint(
                #             this=Paren(
                #                 this=Func(this=START_TIME)
                #             )
                #         )
                #     )
                # ]

                # Example with column expression
                # [
                #   ColumnConstraint(
                #       kind=ComputedColumnConstraint(
                #           this=Paren(
                #               this=Column(
                #                       this=Identifier(this=symbol, quoted=False)
                #               )
                #           )
                #       )
                #   )
                # ]

                # We only support generated columns now as column
                # constrants, so we are fine getting first argument
                # `kind` refers to after dtype 'TEXT AS ...'
                # `this` refers to after alias 'AS ...'
                first_constraint = col.constraints[0].kind.this.this
                if isinstance(first_constraint, exp.Func):
                    value = GENERATED_COLUMN_DISPATCH[first_constraint.name]
                else:
                    # TODO: make a function directly (expr ?)
                    value = first_constraint.sql(OmlspDialect)
                generated_columns[col_name] = value

                # Deleting constraints the shady way
                col.set("constraints", None)

    return table, table_name, columns, column_types, generated_columns


def parse_lookup_table_schema(
    table: exp.Schema,
) -> tuple[exp.Schema, str, dict[str, str], list[str]]:
    # Parse temporary table schema (lookup).
    # Extract parameters for dynamic eval
    table_name = get_name(table)
    table = table.copy()

    columns = {}
    dynamic_columns = []

    for column in table.expressions:
        if isinstance(column, exp.ColumnDef) and column.name.startswith("$"):
            new_col_name = str(column.name).replace("$", "")
            column.set("this", exp.to_identifier(new_col_name))
            dynamic_columns.append(new_col_name)
        columns[column.name] = str(column.kind)

    return table, table_name, columns, dynamic_columns


def parse_ws_table_schema(table: exp.Schema) -> tuple[str, dict[str, str]]:
    # Parse web socket table schema.
    table_name = table.this.name

    column_types = {}
    for col in table.expressions:
        if isinstance(col, exp.ColumnDef):
            col_name = str(col.name)
            col_type = col.args.get("kind")  # data type
            column_types[col_name] = str(col_type).upper()

    return table_name, column_types


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


def build_create_http_table_context(
    statement: exp.Create, properties: dict[str, str]
) -> CreateHTTPTableContext:
    # avoid side effect, leverage sqlglot ast copy
    statement = statement.copy()

    # parse table schema and update exp.Schema
    updated_table_statement, table_name, _, column_types, generated_columns = (
        parse_table_schema(statement.this)
    )

    # overwrite modified table statement
    statement.set("this", updated_table_statement)

    # cron schedule for http
    trigger = CronTrigger.from_crontab(
        str(properties.pop("schedule")), timezone=timezone.utc
    )

    # get query purged of it's omlsp specific syntax
    clean_query = get_duckdb_sql(statement)

    return CreateHTTPTableContext(
        name=table_name,
        properties=properties,
        query=clean_query,
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
    new_expr_schema, table_name, columns, dynamic_columns = parse_lookup_table_schema(
        statement.this
    )

    # get updated exp.Create (to extract sql later)
    statement.set("this", new_expr_schema)

    return CreateHTTPLookupTableContext(
        name=table_name,
        properties=properties,
        query=get_duckdb_sql(statement),
        dynamic_columns=dynamic_columns,
        columns=columns,
    )


def build_create_ws_table_context(
    statement: exp.Create, properties: dict[str, str]
) -> CreateWSTableContext:
    # avoid side effect, leverage sqlglot ast copy
    statement = statement.copy()

    table_name, column_types = parse_ws_table_schema(statement.this)

    # get query purged of omlsp-specific syntax
    clean_query = get_duckdb_sql(statement)

    on_start_query = properties.pop("on_start_query", "")

    return CreateWSTableContext(
        name=table_name,
        properties=properties,
        column_types=column_types,
        query=clean_query,
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
        name=statement.this,
        upstreams=upstreams,
        properties=properties,
        subquery=ctx.query,
    )


def build_create_view_context(
    statement: exp.Create,
) -> CreateViewContext | CreateMaterializedViewContext | InvalidContext:
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
                return CreateMaterializedViewContext(
                    name=name,
                    upstreams=upstreams,
                    query=query,
                    transform_ctx=ctx,
                )

    return CreateViewContext(
        name=name,
        upstreams=upstreams,
        query=query,
        transform_ctx=ctx,
    )


def build_generic_create_table_context(
    statement: exp.Create, properties_schema: dict
) -> (
    CreateHTTPTableContext
    | CreateHTTPLookupTableContext
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

    if connector == TableConnectorKind.LOOKUP_HTTP.value:
        return build_create_http_lookup_table_context(statement, properties)

    if connector == TableConnectorKind.HTTP.value:
        return build_create_http_table_context(statement, properties)

    if connector == TableConnectorKind.WS.value:
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
    | CreateHTTPLookupTableContext
    | CreateWSTableContext
    | CreateSinkContext
    | CreateViewContext
    | CreateMaterializedViewContext
    | CreateSecretContext
    | InvalidContext
):
    if str(statement.kind) == CreateKind.TABLE.value:
        return build_generic_create_table_context(statement, properties_schema)

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

    elif "MATERIALIZED VIEWS" in sql:
        query += METADATA_VIEW_MATERIALIZED_TABLE_NAME

    elif "VIEWS" in sql:
        query += METADATA_VIEW_TABLE_NAME

    elif "SECRETS" in sql:
        query += METADATA_SECRET_TABLE_NAME

    elif "SINKS" in sql:
        query += METADATA_SINK_TABLE_NAME

    return ShowContext(user_query=sql, query=query)


def extract_command_context(
    statement: exp.Command,
) -> CommandContext | InvalidContext:
    sql_string = get_duckdb_sql(statement).strip().upper()

    return CommandContext(query=sql_string)


def extract_one_query_context(
    query: str, properties_schema: dict
) -> QueryContext | NonQueryContext:
    parsed_statement = parse_one(query, dialect=OmlspDialect)

    if isinstance(parsed_statement, exp.Create):
        return extract_create_context(parsed_statement, properties_schema)

    elif isinstance(parsed_statement, exp.Select):
        return extract_select_context(parsed_statement)

    elif isinstance(parsed_statement, exp.Set):
        return extract_set_context(parsed_statement)

    elif isinstance(parsed_statement, exp.Show):
        return extract_show_statement(parsed_statement)

    elif isinstance(parsed_statement, exp.With):
        return InvalidContext(reason="CTE statement (i.e WITH ...) is not accepted")

    elif isinstance(parsed_statement, exp.Command):
        return extract_command_context(parsed_statement)

    return InvalidContext(
        reason=f"Unknown statement {type(parsed_statement)} - {parsed_statement}"
    )
