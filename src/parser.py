import sqlglot.errors
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


def parse_table(
    table: exp.Schema | exp.Table,
) -> tuple[str, list[dict[str, str]]]:
    """Parse table schema or table into name and columns"""
    assert isinstance(table, (exp.Schema,)), (
        f"Expression of type {type(table)} is not accepted"
    )

    table_name = get_name(table)
    columns = []

    for column in table.expressions:
        col_name = get_name(column)
        python_type, duckdb_type, _ = get_type(column)
        columns.append(
            {
                "name": col_name,
                "python_type": python_type,
                "duckdb_type": duckdb_type,
            }
        )
    return table_name, columns


def parse_with_properties(
    with_properties: exp.Create,
) -> tuple[dict[str, str], str]:
    """Parse properties from a WITH statement"""
    custom_properties = {}
    table_kind = ""
    for prop in with_properties.args.get("properties", []):
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
    return custom_properties, table_kind


def validate_with_properties(
    properties: dict[str, str], properties_schema: dict
) -> None:
    """JSON Schema validation of properties extracted from WITH statement"""
    jsonschema.validate(instance=properties, schema=properties_schema)


def parse_query_to_dict(query: str, properties_schema: dict) -> list[dict[str, Any]]:
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
    result = []

    try:
        parsed_statements = parse(query, dialect=None)
    except sqlglot.errors.ParseError as e:
        logger.error(f"Failed to parse query: {e}")
        raise
    except ValueError as e:
        logger.error(f"Invalid query structure: {e}")
        raise

    for parsed in parsed_statements:
        if not isinstance(parsed, exp.Create):
            logger.warning(f"Skipping non-CREATE TABLE statement: {parsed}")
            continue

        table_dict = {
            "type": "table",
            "name": None,
            "columns": [],
            "properties": {},
        }
        table_name, columns = parse_table(parsed.this)

        properties, table_kind = parse_with_properties(parsed)
        validate_with_properties(properties, properties_schema)
        logger.warning(table_kind)
        table_dict["name"] = table_name
        table_dict["columns"] = columns
        table_dict["properties"] = properties
        table_dict["query"] = get_duckdb_sql(parsed, table_kind)

        result.append(table_dict)

    if not result:
        raise ValueError("No valid CREATE TABLE statements found in the query")

    return result


def get_duckdb_sql(statement: exp.Expression, table_kind: str = "") -> str:
    if not isinstance(statement, exp.Create):
        return ""
    statement = statement.copy()
    if statement.args.get("properties"):
        del statement.args["properties"]
    if table_kind:
        statement.set("kind", table_kind)

    logger.warning(statement)
    return statement.sql("duckdb")


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

    parsed_queries = parse_query_to_dict(sql_content, properties_schema)
    print(parsed_queries)
