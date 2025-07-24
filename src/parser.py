from sqlglot import parse_one, exp
from typing import Dict, List, Union, Optional
import sqlglot.errors
import logging

TYPE_MAPPING  = {
    "TEXT": ("str", "VARCHAR"),
    "VARCHAR": ("str", "VARCHAR"),
    "CHAR": ("str", "CHAR"),
    "INT": ("int", "INTEGER"),
    "BIGINT": ("int", "BIGINT"),
    "SMALLINT": ("int", "SMALLINT"),
    "FLOAT": ("float", "FLOAT"),
    "DOUBLE": ("float", "DOUBLE"),
    "BOOLEAN": ("bool", "BOOLEAN")
}

def get_name(expression: exp.Expression) -> str:
    return getattr(expression, 'this', expression).name

def get_type(expression: exp.ColumnDef) -> Union[str, str, str]:
    type_name = getattr(getattr(expression, 'kind', None), 'this', None).name
    python_type, duckdb_type = TYPE_MAPPING[type_name]
    return python_type, duckdb_type, type_name
    
def get_property(expression: exp.Property) -> Optional[exp.Expression]:
    return expression.args.get('value')

def parse_table(table: Union[exp.Schema, exp.Table]) -> tuple[str, List[Dict[str, str]]]:
    columns = []
    if isinstance(table, exp.Schema):
        table_name = get_name(table)
        for column in table.expressions:
            if not isinstance(column, exp.ColumnDef):
                raise ValueError(f"Expected exp.ColumnDef, got {type(column)}")
            col_name = get_name(column)
            if not col_name:
                raise ValueError(f"Missing column name in {column}")
            python_type, duckdb_type, type_name = get_type(column)
            if python_type == "" or duckdb_type == "":
                raise ValueError(f"Unsupported or missing type {type_name}")
            columns.append({'name': col_name, 'python_type': python_type, 'duckdb_type': duckdb_type})
    elif isinstance(table, exp.Table):
        table_name = get_name(table)
    else:
        raise ValueError(f"Expected exp.Schema or exp.Table, got {type(table)}")
    return table_name, columns

def parse_with_properties(with_properties: exp.Create) -> Dict[str, Optional[Union[str, None]]]:
    properties = {}
    for prop in with_properties.args.get('properties', []):
        if not isinstance(prop, exp.Property):
            raise ValueError(f"Expected exp.Property, got {type(prop)}")
        key = get_name(prop)
        if not key:
            raise ValueError(f"Missing key in {prop}")
        value = get_property(prop)
        if not value:
            raise ValueError(f"Missing value in {prop}")
        if not isinstance(value, exp.Literal):
            raise ValueError(f"Expected exp.Literal, got {type(value)}: {value}")
        properties[key] = value.this
    return properties

def parse_query_to_dict(query: str) -> Dict[str, Union[str, List[Dict[str, str]], Dict[str, Optional[Union[str, None]]]]]:
    """
    Parse a SQL CREATE TABLE query into a dictionary.

    Args:
        query (str): SQL query string (expected to be a CREATE TABLE statement)

    Returns:
        dict: Dictionary with table name, columns, and properties

    Raises:
        sqlglot.errors.ParseError: If the query cannot be parsed
        ValueError: If the query structure is invalid
    """
    result = {'table_name': None, 'columns': [], 'properties': {}}
    
    try:
        parsed = parse_one(query, dialect=None)
        if not isinstance(parsed, exp.Create):
            ValueError(f"Expected CREATE TABLE query, got {type(parsed)}")
        
        result['table_name'], result['columns'] = parse_table(parsed.this)
        result['properties'] = parse_with_properties(parsed)
        return result
    
    except sqlglot.errors.ParseError as e:
        logging.error(f"Failed to parse query: {e}")
        raise
    except ValueError as e:
        logging.error(f"Invalid query structure: {e}")
        raise

query = """
CREATE TABLE example (
    url STRING
)
WITH (
    'connector' = 'http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'scan.interval' = '60s',
    'json.jsonpath' = '$.url'
);
"""

result = """
{'table_name': 'example', 
    'columns': [{'name': 'url', 'python_type': 'str', 'duckdb_type': 'VARCHAR'}], 
        'properties': {'connector': 'http', 'url': 'https://httpbin.org/get', 
        'method': 'GET', 'scan.interval': '60s', 'json.jsonpath': '$.url'
    }
}
"""

invalid_query = """
SELECT * FROM example;
"""

invalid_query_2 = """
CREATE TABLE example (
    url STRING
)
WITH (
    'connector' = 'http',
    'timestamp' = CURRENT_TIMESTAMP
);
"""

invalid_query_3 = """
CREATE TABLE example (
    url STRING
)
WITH (
    'connector' = 'http',
    'novalue' = NULL,
    'time' = CURRENT_TIMESTAMP
);
"""

invalid_query_4 = """
CREATE TABLE example (
    STRING
)
WITH (
    'connector' = 'http'
);
"""