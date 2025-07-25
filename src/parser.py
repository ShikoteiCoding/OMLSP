from sqlglot import parse_one, exp
from typing import Dict, List, Union, Optional, Tuple
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

def get_type(expression: exp.ColumnDef) -> Tuple[str, str, str]:
    type_name = getattr(getattr(expression, 'kind', None), 'this', None).name
    python_type, duckdb_type = TYPE_MAPPING[type_name]
    return python_type, duckdb_type, type_name

def get_property(expression: exp.Property) -> Optional[exp.Expression]:
    return expression.args.get('value')

def parse_table(table: Union[exp.Schema, exp.Table]) -> Tuple[str, List[Dict[str, str]]]:
    """Parse table schema or table into name and columns"""
    columns = []
    table_name = get_name(table)
    
    if isinstance(table, exp.Schema):
        for column in table.expressions:
            col_name = get_name(column)
            python_type, duckdb_type, _ = get_type(column)
            columns.append({'name': col_name, 'python_type': python_type, 'duckdb_type': duckdb_type})
    
    return table_name, columns

def parse_with_properties(with_properties: exp.Create) -> Dict[str, Optional[Union[str, None]]]:
    """Parse properties from a WITH statement"""
    properties = {}
    for prop in with_properties.args.get('properties', []):
        key = get_name(prop)
        value = get_property(prop)
        properties[key] = value.this if isinstance(value, exp.Literal) else None
    return properties

def validate_table(table: Union[exp.Schema, exp.Table], name: str, parsed_columns: List[Dict[str, str]]) -> None:
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
            raise ValueError(f"Mismatch: {len(table.expressions)} expressions but {len(parsed_columns)} parsed columns")
        
        for i, (raw_column, parsed_column) in enumerate(zip(table.expressions, parsed_columns)):
            if not isinstance(raw_column, exp.ColumnDef):
                raise ValueError(f"Expected exp.ColumnDef, got {type(raw_column)}")
            col_name = get_name(raw_column)
            if not col_name:
                raise ValueError(f"Missing column name in {raw_column}")
            if col_name != parsed_column['name']:
                raise ValueError(f"Column name mismatch at index {i}: {col_name} vs {parsed_column['name']}")
            python_type, duckdb_type, type_name = get_type(raw_column)
            if python_type == "" or duckdb_type == "":
                raise ValueError(f"Unsupported or missing type {type_name}")
            if python_type != parsed_column['python_type'] or duckdb_type != parsed_column['duckdb_type']:
                raise ValueError(f"Type mismatch for column {col_name}: {python_type}/{duckdb_type} vs {parsed_column['python_type']}/{parsed_column['duckdb_type']}")

def validate_properties(with_properties: exp.Create, properties: Dict[str, Optional[Union[str, None]]]) -> None:
    """Validate properties from a WITH statement, cross-checking with parsed properties"""
    raw_properties = with_properties.args.get('properties', []).expressions
    parsed_properties = list(properties.items())  # Convert dict to list of (key, value)
    if len(raw_properties) != len(parsed_properties):
        raise ValueError(f"Mismatch: {len(raw_properties)} raw properties but {len(parsed_properties)} parsed properties")
    
    for i, (prop, (parsed_key, parsed_value)) in enumerate(zip(raw_properties, parsed_properties)):
        if not isinstance(prop, exp.Property):
            raise ValueError(f"Expected exp.Property at index {i}, got {type(prop)}")
        
        key = get_name(prop)
        if not key:
            raise ValueError(f"Missing key at index {i} in {prop}")
        if key != parsed_key:
            raise ValueError(f"Property key mismatch at index {i}: {key} vs {parsed_key}")
        
        value = get_property(prop)
        if not value:
            raise ValueError(f"Missing value at index {i} in {prop}")
        if not isinstance(value, exp.Literal):
            raise ValueError(f"Expected exp.Literal at index {i}, got {type(value)}: {value}")
        
        raw_value = value.this
        if raw_value != parsed_value:
            raise ValueError(f"Property value mismatch at index {i} for key {key}: {raw_value} vs {parsed_value}")

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
    result = {'table': {'name': None, 'columns': [], 'properties': {}}}
    
    try:
        parsed = parse_one(query, dialect=None)
        
        if not isinstance(parsed, exp.Create):
            raise ValueError(f"Expected CREATE TABLE query, got {type(parsed)}")
        
        table_name, columns = parse_table(parsed.this)
        validate_table(parsed.this, table_name, columns)
        
        properties = parse_with_properties(parsed)
        validate_properties(parsed, properties)
        
        result['table']['name'] = table_name
        result['table']['columns'] = columns
        result['table']['properties'] = properties
        
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
{'table': 
    {'name': 'example', 
    'columns': [
        {'name': 'url', 'python_type': 'str', 'duckdb_type': 'VARCHAR'}], 
    'properties': {
        'connector': 'http', 'url': 'https://httpbin.org/get', 
        'method': 'GET', 'scan.interval': '60s', 'json.jsonpath': '$.url'}
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