import jsonschema

from apscheduler.triggers.cron import CronTrigger
from datetime import timezone

from loguru import logger
from sqlglot import parse_one, exp
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType
from sqlglot.dialects import Dialect

from context.context import (
    CreateTableContext,
    CreateLookupTableContext,
    CreateSinkContext,
    CommandContext,
    SelectContext,
    SetContext,
    InvalidContext,
    QueryContext,
)


class OmlspTokenizer(Tokenizer):
    KEYWORDS = {
        **Tokenizer.KEYWORDS,
        "SINK": TokenType.SINK,
    }


class OmlspParser(Parser):
    def _parse_create(self):
        if self._match(TokenType.SINK):
            # Parse the name of the sink
            name = self._parse_id_var()

            if not self._match(TokenType.FROM):
                self.raise_error("Expected FROM in CREATE SINK statement")

            if self._curr and self._curr.token_type == TokenType.L_PAREN:
                # Parse a subquery: (SELECT ...)
                source_expr = self._parse_wrapped(self._parse_select)
            else:
                # Just a table identifier
                source_expr = self._parse_id_var()

            properties = None
            if self._match(TokenType.WITH):
                self._match(TokenType.L_PAREN)
                properties = self._parse_properties()
                self._match(TokenType.R_PAREN)

            return self.expression(
                exp.Create,
                this=name,
                kind="SINK",
                expression=source_expr,
                properties=properties,
            )

        return super()._parse_create()


class OmlspDialect(Dialect):
    parser = OmlspParser
    tokenizer = OmlspTokenizer


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


def parse_table_schema(table: exp.Schema) -> tuple[exp.Schema, str, list[str]]:
    """Parse table schema into name and columns"""
    assert isinstance(table, (exp.Schema,)), (
        f"Expression of type {type(table)} is not accepted"
    )
    table_name = get_name(table)
    table = table.copy()

    columns = [
        str(column.name)
        for column in table.expressions
        if isinstance(column, exp.ColumnDef)
    ]

    return table, table_name, columns


def parse_lookup_table_schema(
    table: exp.Schema,
) -> tuple[exp.Schema, str, dict[str, str], list[str]]:
    """Parse temp table schema. Handle dynamic parameters of lookup / temp tables."""
    assert isinstance(table, (exp.Schema,)), (
        f"Expression of type {type(table)} is not accepted"
    )
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


def validate_create_properties(
    properties: dict[str, str], properties_schema: dict
) -> None:
    """JSON Schema validation of properties extracted from WITH statement"""
    jsonschema.validate(instance=properties, schema=properties_schema)


def parse_create_properties(
    statement: exp.Create, properties_schema: dict
) -> tuple[exp.Create, dict[str, str]]:
    """Parse properties of Create statement"""
    assert isinstance(statement, (exp.Create,)), (
        f"Expression of type {type(statement)} is not accepted"
    )
    custom_properties = {}
    table_kind = "TABLE"
    statement = statement.copy()
    for prop in get_properties_from_create(statement):
        # sqlglot extract temporary statement and put it in properties
        if isinstance(prop, exp.TemporaryProperty):
            # TODO: find better way than overwrite table "kind"
            table_kind = "TEMPORARY TABLE"
            continue
        elif isinstance(prop, exp.Property):
            key = prop.name
            val_exp = prop.args["value"]
            if isinstance(val_exp, exp.Literal):
                value = val_exp.this
            else:  # fallback to raw SQL
                value = val_exp.sql(dialect=None)
        else:
            logger.warning(f"Unknown property: {type(prop)} - {prop}")
        custom_properties[key] = value

    statement.set("kind", table_kind)
    validate_create_properties(custom_properties, properties_schema)
    return statement, custom_properties


def get_duckdb_sql(statement: exp.Create | exp.Select | exp.Set | exp.Command) -> str:
    statement = statement.copy()
    if statement.args.get("properties"):
        del statement.args["properties"]

    return statement.sql("duckdb")


def get_properties_from_create(statement: exp.Create) -> list[exp.Property]:
    return [prop for prop in statement.args.get("properties", [])]


def extract_create_context(
    statement: exp.Create, properties_schema: dict
) -> CreateTableContext | CreateLookupTableContext | CreateSinkContext | InvalidContext:
    # TODO: assert only tables here and make this split by kind of create: table, function, etc...
    is_temp = isinstance(
        get_properties_from_create(statement)[0], exp.TemporaryProperty
    )

    if str(statement.kind) == "TABLE" and not is_temp:
        updated_create_statement, properties = parse_create_properties(
            statement, properties_schema
        )
        updated_table_statement, table_name, _ = parse_table_schema(
            updated_create_statement.this
        )
        updated_create_statement.set(
            "this", updated_table_statement
        )  # overwrite modified table statement
        cron_expr = str(properties.pop("schedule"))
        return CreateTableContext(
            name=table_name,
            properties=properties,
            query=get_duckdb_sql(updated_create_statement),
            trigger=CronTrigger.from_crontab(cron_expr, timezone=timezone.utc),
        )

    # process separately to handle the udft logic here
    elif str(statement.kind) == "TABLE" and is_temp:
        updated_create_statement, properties = parse_create_properties(
            statement, properties_schema
        )
        updated_table_statement, table_name, columns, dynamic_columns = (
            parse_lookup_table_schema(updated_create_statement.this)
        )
        updated_create_statement.set(
            "this", updated_table_statement
        )  # overwrite modified table statement

        return CreateLookupTableContext(
            name=table_name,
            properties=properties,
            query=get_duckdb_sql(updated_create_statement),
            dynamic_columns=dynamic_columns,
            columns=columns,
        )
    elif str(statement.kind) == "SINK":
        updated_create_statement, properties = parse_create_properties(
            statement, properties_schema
        )
        expr = updated_create_statement.expression

        # TODO: support multiple upstreams merged/unioned
        if isinstance(expr, exp.Select):
            upstreams = [extract_select_context(expr)]
        elif isinstance(expr, exp.Table):
            upstreams = [expr.copy()] if expr else []
        else:
            return InvalidContext(reason=f"Unsupported upstream expression: {expr}")

        return CreateSinkContext(
            name=updated_create_statement.this,
            upstreams=upstreams,
            properties=properties,
        )

    # TODO: Process views, materialized views, sinks and functions here
    return InvalidContext(reason=f"Not known statement kind: {statement.kind}")


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
    assert isinstance(statement, exp.Select), (
        f"Unexpected statement of type: {type(statement)}, expected exp.Select statement"
    )
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


def extract_command_context(
    statement: exp.Command,
) -> CommandContext | InvalidContext:
    sql_string = get_duckdb_sql(statement).strip().upper()

    if sql_string == "SHOW TABLES":
        return CommandContext(query=sql_string)

    return InvalidContext("Unsupported command, only 'SHOW TABLES' is supported")


def extract_one_query_context(
    query: str, properties_schema: dict
) -> QueryContext | InvalidContext:
    parsed_statement = parse_one(query, dialect=OmlspDialect)

    if isinstance(parsed_statement, exp.Create):
        return extract_create_context(parsed_statement, properties_schema)

    elif isinstance(parsed_statement, exp.Select):
        return extract_select_context(parsed_statement)

    elif isinstance(parsed_statement, exp.Set):
        return extract_set_context(parsed_statement)

    elif isinstance(parsed_statement, exp.With):
        return InvalidContext(reason="CTE statement (i.e WITH ...) is not accepted")

    elif isinstance(parsed_statement, exp.Command):
        return extract_command_context(parsed_statement)

    return InvalidContext(
        reason=f"Unknown statement {type(parsed_statement)} - {parsed_statement}"
    )
