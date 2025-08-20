import jsonschema

from collections import namedtuple
from loguru import logger
from sqlglot import parse, exp


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

VALID_STATEMENTS = (exp.Create, exp.Select, exp.Set)

CreateTableParams = namedtuple("CreateParams", ["name", "properties", "query"])
CreateLookupTableParams = namedtuple(
    "CreateLookupTableParams",
    ["name", "properties", "query", "dynamic_columns", "columns"],
)
SelectParams = namedtuple(
    "SelectParams", ["columns", "table", "alias", "where", "joins", "query"]
)
SetParams = namedtuple("SetParams", ["query"])


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


def get_duckdb_sql(statement: exp.Create | exp.Select | exp.Set) -> str:
    assert isinstance(statement, VALID_STATEMENTS), (
        f"Expected {VALID_STATEMENTS} not {type(statement)}"
    )

    statement = statement.copy()
    if statement.args.get("properties"):
        del statement.args["properties"]

    return statement.sql("duckdb")


def get_properties_from_create(statement: exp.Create) -> list[exp.Property]:
    return [prop for prop in statement.args.get("properties", [])]


def extract_create_params(
    statement: exp.Create, properties_schema: dict
) -> CreateTableParams | CreateLookupTableParams | None:
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

        return CreateTableParams(
            name=table_name,
            properties=properties,
            query=get_duckdb_sql(updated_create_statement),
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

        table_dict["name"] = table_name
        table_dict["properties"] = properties
        table_dict["query"] = get_duckdb_sql(updated_create_statement)

        # Keep to build dynamic macro call in engine
        table_dict["dynamic_columns"] = dynamic_columns
        table_dict["columns"] = columns

        return CreateLookupTableParams(
            name=table_name,
            properties=properties,
            query=get_duckdb_sql(updated_create_statement),
            dynamic_columns=dynamic_columns,
            columns=columns,
        )

    # TODO: Process views, materialized views, sinks and functions here
    return None


def parse_select(
    statement: exp.Select,
) -> tuple[exp.Select, list[str], str, str, str, list[dict[str, str]]]:
    columns = []
    table_name = ""
    table_alias = ""
    where = ""
    join_tables = []

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

    # TODO: change find all with more robust ?
    for table in statement.find_all(exp.Table):
        if isinstance(table.parent, exp.Join):
            table_exp = table
            join_tables.append({str(table_exp.name): str(table_exp.alias_or_name)})
            table.set("this", exp.to_identifier(f"${str(table.name)}"))

    return statement, columns, table_name, table_alias, where, join_tables


def extract_select_params(statement: exp.Select) -> SelectParams:
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
    updated_select_statement, columns, table, alias, where, joins = parse_select(
        statement
    )

    return SelectParams(
        columns=columns,
        table=table,
        alias=alias,
        where=where,
        joins=joins,
        query=get_duckdb_sql(updated_select_statement),
    )


def extract_set_params(statement: exp.Set) -> SetParams:
    assert isinstance(statement, exp.Set), (
        f"Unexpected statement of type {type(statement)}, expected exp.Set statement"
    )

    return SetParams(query=get_duckdb_sql(statement))


def extract_sql_params(
    query: str, properties_schema: dict
) -> list[CreateTableParams | CreateLookupTableParams | SelectParams | SetParams]:
    """
    Parse a SQL file containing multiple CREATE TABLE queries into a list of dictionaries.
    This keeps ordering

    Args:
        query (str): SQL query string containing one or more CREATE TABLE statements

    Returns:
        list[dict]: list of parameters for each sql statement

    Raises:
        sqlglot.errors.ParseError: If any query cannot be parsed
        ValueError: If any query structure is invalid
    """
    param_list = []

    parsed_statements = parse(query)

    for parsed_statement in parsed_statements:
        assert isinstance(parsed_statement, VALID_STATEMENTS), (
            f"Invalid statement {type(parsed_statement)}, expected: {VALID_STATEMENTS}"
        )

        if isinstance(parsed_statement, exp.Create):
            param_list.append(
                extract_create_params(parsed_statement, properties_schema)
            )

        elif isinstance(parsed_statement, exp.Select):
            param_list.append(extract_select_params(parsed_statement))

        elif isinstance(parsed_statement, exp.Set):
            param_list.append(extract_set_params(parsed_statement))

    return param_list


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

    ordered_statements = extract_sql_params(sql_content, properties_schema)
    for params in ordered_statements:
        logger.info(ordered_statements)
