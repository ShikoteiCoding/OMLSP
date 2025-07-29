from sqlglot import parse, exp
from typing import Any
import sqlglot.errors
import logging
import jsonschema

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
    columns = []
    table_name = get_name(table)

    if isinstance(table, exp.Schema):
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
) -> dict[str, str | None]:
    """Parse properties from a WITH statement"""
    properties = {}
    for prop in with_properties.args.get("properties", []):
        key = get_name(prop)
        value = get_property(prop)
        properties[key] = value.this if isinstance(value, exp.Literal) else None
    return properties


def validate_table(
    table: exp.Schema | exp.Table, name: str, parsed_columns: list[dict[str, str]]
) -> None:
    """Validate the table structure and its columns"""
    table_name = get_name(table)
    if not table_name:
        raise ValueError("Missing or empty table name")
    if table_name != name:
        raise ValueError(f"Table name mismatch: {table_name} != {name}")
    if not isinstance(table, (exp.Schema, exp.Table)):
        raise ValueError(f"Expected exp.Schema or exp.Table, got {type(table)}")

    if isinstance(table, exp.Schema):
        if len(table.expressions) != len(parsed_columns):
            raise ValueError(
                f"Mismatch: {len(table.expressions)} expressions but {len(parsed_columns)} parsed columns"
            )

        for i, (raw_column, parsed_column) in enumerate(
            zip(table.expressions, parsed_columns)
        ):
            if not isinstance(raw_column, exp.ColumnDef):
                raise ValueError(f"Expected exp.ColumnDef, got {type(raw_column)}")
            col_name = get_name(raw_column)
            if not col_name:
                raise ValueError(f"Missing column name in {raw_column}")
            if col_name != parsed_column["name"]:
                raise ValueError(
                    f"Column name mismatch at index {i}: {col_name} vs {parsed_column['name']}"
                )
            python_type, duckdb_type, type_name = get_type(raw_column)
            if python_type == "" or duckdb_type == "":
                raise ValueError(f"Unsupported or missing type {type_name}")
            if (
                python_type != parsed_column["python_type"]
                or duckdb_type != parsed_column["duckdb_type"]
            ):
                raise ValueError(
                    f"Type mismatch for column {col_name}: {python_type}/{duckdb_type} vs {parsed_column['python_type']}/{parsed_column['duckdb_type']}"
                )


def validate_with_properties(
    properties: dict[str, str | None], properties_schema: dict
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
        logging.error(f"Failed to parse query: {e}")
        raise
    except ValueError as e:
        logging.error(f"Invalid query structure: {e}")
        raise

    for parsed in parsed_statements:
        if not isinstance(parsed, exp.Create):
            logging.warning(f"Skipping non-CREATE TABLE statement: {parsed}")
            continue

        table_dict = {
            "table": {
                "name": None,
                "columns": [],
                "properties": {},
                "query": get_duckdb_sql(parsed),
            }
        }
        table_name, columns = parse_table(parsed.this)
        validate_table(parsed.this, table_name, columns)

        properties = parse_with_properties(parsed)
        validate_with_properties(properties, properties_schema)

        table_dict["table"]["name"] = table_name
        table_dict["table"]["columns"] = columns
        table_dict["table"]["properties"] = properties

        result.append(table_dict)

    if not result:
        raise ValueError("No valid CREATE TABLE statements found in the query")

    return result


def get_duckdb_sql(statement: exp.Expression) -> str:
    if not isinstance(statement, exp.Create):
        return ""
    statement = statement.copy()
    if statement.args.get("properties"):
        del statement.args["properties"]
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
