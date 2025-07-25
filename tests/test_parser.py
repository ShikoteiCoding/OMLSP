import pytest
from src.parser import parse_query_to_dict

VALID_QUERY = """
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

EXPECTED_RESULT = {
    'table': {
        'name': 'example',
        'columns': [
            {'name': 'url', 'python_type': 'str', 'duckdb_type': 'VARCHAR'}
        ],
        'properties': {
            'connector': 'http',
            'url': 'https://httpbin.org/get',
            'method': 'GET',
            'scan.interval': '60s',
            'json.jsonpath': '$.url'
        }
    }
}

INVALID_QUERY = """
SELECT * FROM example;
"""

INVALID_QUERY_2 = """
CREATE TABLE example (
    url STRING
)
WITH (
    'connector' = 'http',
    'timestamp' = CURRENT_TIMESTAMP
);
"""

INVALID_QUERY_3 = """
CREATE TABLE example (
    url STRING
)
WITH (
    'connector' = 'http',
    'novalue' = NULL,
    'time' = CURRENT_TIMESTAMP
);
"""

INVALID_QUERY_4 = """
CREATE TABLE example (
    STRING
)
WITH (
    'connector' = 'http'
);
"""

def test_valid_create_table_with_columns_and_properties():
    result = parse_query_to_dict(VALID_QUERY)
    assert result == EXPECTED_RESULT

def test_invalid_non_create_query():
    with pytest.raises(ValueError, match="Expected CREATE TABLE query"):
        parse_query_to_dict(INVALID_QUERY)

def test_invalid_property_not_literal_timestamp():
    with pytest.raises(ValueError, match="Expected exp.Literal at index 1"):
        parse_query_to_dict(INVALID_QUERY_2)

def test_invalid_property_null_value():
    with pytest.raises(ValueError, match="Expected exp.Literal at index 1"):
        parse_query_to_dict(INVALID_QUERY_3)

def test_invalid_expression():
    with pytest.raises(ValueError, match="Expected exp.ColumnDef, got <class 'sqlglot.expressions.Identifier'>"):
        parse_query_to_dict(INVALID_QUERY_4)
