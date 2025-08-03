import jsonschema

from sqlglot import parse, exp, parse_one
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


def parse_table_schema(
    table: exp.Schema,
) -> tuple[str, list[dict[str, str]], list[str]]:
    """Parse table schema or table into name and columns"""
    assert isinstance(table, (exp.Schema,)), (
        f"Expression of type {type(table)} is not accepted"
    )
    exp.Identifier
    exp.ColumnDef

    table_name = get_name(table)
    columns = []
    dynamic_columns = []

    for column in table.expressions:
        col_name = get_name(column)
        python_type, duckdb_type, _ = get_type(column)
        if col_name.startswith("$"):
            new_col_name = col_name.replace("$", "")
            dynamic_columns.append(new_col_name)
        columns.append(
            {
                "name": col_name,
                "python_type": python_type,
                "duckdb_type": duckdb_type,
            }
        )
    return table_name, columns, dynamic_columns


def parse_table_properties(
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


def get_duckdb_sql(
    statement: exp.Create, table_kind: str = "", dynamic_columns: list[str] = []
) -> str:
    assert isinstance(statement, exp.Create), (
        f"Expected {type(exp.Create)} not {type(statement)}"
    )

    statement = statement.copy()
    if statement.args.get("properties"):
        del statement.args["properties"]

    if table_kind:
        statement.set("kind", table_kind)

    sql = statement.sql()
    stmt = parse_one(sql)
    stmt = stmt.copy()

    # TODO: find a way to remove $ column names
    for col in stmt.expressions:
        if isinstance(col, exp.ColumnDef) and col.name.startswith("$"):
            col.set("this", exp.to_identifier(str(col.name).replace("$", "")))

    logger.debug(stmt)
    return stmt.sql("duckdb")


def process_create_statement(statement: exp.Create, properties_schema: dict) -> dict:
    # TODO: assert only tables here and make this split by kind of create: table, function, etc...
    table_dict = {
        "type": "table",
        "name": None,
        "columns": [],
        "dynamic_columns": [],  # TODO: for lookup table, maybe move elsewhere
        "properties": {},
    }
    table_name, columns, dynamic_columns = parse_table_schema(statement.this)

    properties, table_kind = parse_table_properties(statement)
    validate_with_properties(properties, properties_schema)
    table_dict["name"] = table_name
    table_dict["columns"] = columns
    table_dict["dynamic_columns"] = dynamic_columns
    table_dict["properties"] = properties
    table_dict["query"] = get_duckdb_sql(statement, table_kind, dynamic_columns)
    return table_dict


def parse_select(select: exp.Select) -> dict[str, Any]:
    select_dict = {"columns": [], "table": "", "where": None, "joins": []}

    for projection in select.expressions:
        col_name = get_name(projection)
        select_dict["columns"].append(col_name)

    from_clause = select.args.get("from")
    if from_clause:
        select_dict["table"] = get_name(from_clause.this)

    where_clause = select.args.get("where")
    if where_clause:
        select_dict["where"] = where_clause.sql(dialect=None)

    joins = select.args.get("joins", [])
    for join in joins:
        join_dict = {
            "table": get_name(join.this),
            "type": join.args.get("kind", "").upper() or "INNER",
            "on": join.args.get("on").sql(dialect=None)
            if join.args.get("on")
            else None,
        }
        select_dict["joins"].append(join_dict)

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

    parsed_statements = parse(query, dialect=None)

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
