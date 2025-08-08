import jsonschema

from sqlglot import parse, exp
from typing import Any
from loguru import logger

TYPE_MAPPING = {
    "TEXT": ("str", "VARCHAR"),
    "VARCHAR": ("str", "VARCHAR"),
    "CHAR": ("str", "CHAR"),
    "INT": ("int", "INTEGER"),
    "BIGINT": ("int", "BIGINT"),
    "SMALLINT": ("int", "SMALLINT"),
    "FLOAT": ("float", "FLOAT"),
    "DOUBLE": ("float", "DOUBLE"),
    "BOOLEAN": ("bool", "BOOLEAN"),
}

VALID_STATEMENTS = (
    exp.Create,
    exp.Select,
)


def get_name(expression: exp.Expression) -> str:
    return getattr(getattr(expression, "this", expression), "name", "")


def get_type(expression: exp.ColumnDef) -> tuple[str, str, str]:
    type_name = getattr(
        getattr(getattr(expression, "kind", None), "this", None), "name", ""
    )
    python_type, duckdb_type = TYPE_MAPPING.get(type_name, ("", ""))
    return python_type, duckdb_type, type_name


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


def parse_temp_table_schema(
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


def get_duckdb_sql(statement: exp.Create | exp.Select) -> str:
    assert isinstance(statement, (exp.Create, exp.Select)), (
        f"Expected {type(exp.Create)} not {type(statement)}"
    )

    statement = statement.copy()
    if statement.args.get("properties"):
        del statement.args["properties"]

    return statement.sql("duckdb")


def get_properties_from_create(statement: exp.Create) -> list[exp.Property]:
    return [prop for prop in statement.args.get("properties", [])]


def process_create_statement(statement: exp.Create, properties_schema: dict) -> dict:
    # TODO: assert only tables here and make this split by kind of create: table, function, etc...
    is_temp = isinstance(
        get_properties_from_create(statement)[0], exp.TemporaryProperty
    )
    table_dict = {
        "type": "table",
        "name": "",
        "properties": {},
    }

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

        table_dict["name"] = table_name
        table_dict["properties"] = properties
        table_dict["query"] = get_duckdb_sql(updated_create_statement)
        return table_dict

    # process separately to handle the udft logic here
    elif str(statement.kind) == "TABLE" and is_temp:
        updated_create_statement, properties = parse_create_properties(
            statement, properties_schema
        )
        updated_table_statement, table_name, columns, dynamic_columns = (
            parse_temp_table_schema(updated_create_statement.this)
        )
        updated_create_statement.set(
            "this", updated_table_statement
        )  # overwrite modified table statement

        table_dict["name"] = table_name
        table_dict["properties"] = properties
        table_dict["query"] = get_duckdb_sql(updated_create_statement)

        # Keep to build dynamic macro call in engine
        table_dict["dynamic_columns"] = dynamic_columns
        table_dict["columns"] = columns

        return table_dict

    # TODO: Process views, materialized views, sinks and functions here
    return {}


def parse_select(statement: exp.Select) -> dict[str, Any]:
    select_dict = {"columns": [], "table": "", "where": None, "joins": []}

    for projection in statement.expressions:
        col_name = get_name(projection)
        select_dict["columns"].append(col_name)

    from_clause = statement.args.get("from")
    if from_clause:
        select_dict["table"] = get_name(from_clause.this)

    where_clause = statement.args.get("where")
    if where_clause:
        select_dict["where"] = where_clause.sql(dialect=None)

    joins = statement.args.get("joins", [])
    for join in joins:
        join_dict = {
            "table": get_name(join.this),
            "type": join.args.get("kind", "").upper() or "INNER",
            "on": join.args.get("on").sql(dialect=None)
            if join.args.get("on")
            else None,
        }
        select_dict["joins"].append(join_dict)

    select_dict["query"] = get_duckdb_sql(statement)

    return select_dict


def process_select_statement(statement: exp.Select) -> dict[str, Any]:
    """
    Parse a SELECT query into a dictionary
    Args:
        query (str): SQL query string containing one SELECT statement
    Returns:
        dict: dictionary
    """
    assert isinstance(statement, exp.Select), (
        f"Unexpected statement of type: {type(statement)}, expected exp.Select statement"
    )

    return parse_select(statement)


def parse_sql_statements(
    query: str, properties_schema: dict
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Parse a SQL file containing multiple CREATE TABLE queries into a list of dictionaries

    Args:
        query (str): SQL query string containing one or more CREATE TABLE statements

    Returns:
        list[dict]: list of dictionaries, each with table name, columns, and properties

    Raises:
        sqlglot.errors.ParseError: If any query cannot be parsed
        ValueError: If any query structure is invalid
    """
    tables = []
    selects = []

    parsed_statements = parse(query)

    for parsed_statement in parsed_statements:
        assert isinstance(parsed_statement, VALID_STATEMENTS), (
            f"Invalid statement {type(parsed_statement)}, expected: {VALID_STATEMENTS}"
        )

        if isinstance(parsed_statement, exp.Create):
            tables.append(process_create_statement(parsed_statement, properties_schema))

        elif isinstance(parsed_statement, exp.Select):
            selects.append(process_select_statement(parsed_statement))

    return tables, selects


if __name__ == "__main__":
    from pathlib import Path
    import json

    file = "examples/basic.sql"
    prop_schema_file = "src/properties.schema.json"

    sql_filepath = Path(file)
    prop_schema_filepath = Path(prop_schema_file)
    sql_content: str

    with open(sql_filepath, "rb") as fo:
        sql_content = fo.read().decode("utf-8")
    with open(prop_schema_filepath, "rb") as fo:
        properties_schema = json.loads(fo.read().decode("utf-8"))

    tables, selects = parse_sql_statements(sql_content, properties_schema)
    for query in tables:
        logger.warning(query)

    for select in selects:
        logger.warning(select)
