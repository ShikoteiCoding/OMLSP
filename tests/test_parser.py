import json
import pytest

from jsonschema.exceptions import ValidationError
from src.parser import parse_query_to_dict

prop_schema_filepath = "schemas/properties.schema.json"


@pytest.fixture
def properties_schema() -> dict:
    with open(prop_schema_filepath, "rb") as fo:
        properties_schema = json.loads(fo.read().decode("utf-8"))
    return properties_schema


VALID_QUERY = """
CREATE TABLE example (
    url STRING
)
WITH (
    'connector' = 'http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'schedule' = '*/5 * * * *',
    'json.jsonpath' = '$.url'
);

CREATE TABLE example_2 (
    url STRING
)
WITH (
    'connector' = 'http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'schedule' = '*/1 * * * *',
    'json.jsonpath' = '$.url'
);
"""

EXPECTED_RESULT = [
    {
        "table": {
            "name": "example",
            "columns": [
                {"name": "url", "python_type": "str", "duckdb_type": "VARCHAR"}
            ],
            "properties": {
                "connector": "http",
                "url": "https://httpbin.org/get",
                "method": "GET",
                "schedule": "*/5 * * * *",
                "json.jsonpath": "$.url",
            },
            "query": "CREATE TABLE example (url TEXT)",
        }
    },
    {
        "table": {
            "name": "example_2",
            "columns": [
                {"name": "url", "python_type": "str", "duckdb_type": "VARCHAR"}
            ],
            "properties": {
                "connector": "http",
                "url": "https://httpbin.org/get",
                "method": "GET",
                "schedule": "*/1 * * * *",
                "json.jsonpath": "$.url",
            },
            "query": "CREATE TABLE example_2 (url TEXT)",
        }
    },
]

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


def test_valid_create_table_with_columns_and_properties(properties_schema: dict):
    result = parse_query_to_dict(VALID_QUERY, properties_schema)
    assert result == EXPECTED_RESULT


def test_invalid_non_create_query(properties_schema: dict):
    with pytest.raises(ValueError):
        parse_query_to_dict(INVALID_QUERY, properties_schema)


def test_invalid_property_not_literal_timestamp(properties_schema: dict):
    with pytest.raises(ValidationError):
        parse_query_to_dict(INVALID_QUERY_2, properties_schema)


def test_invalid_property_null_value(properties_schema: dict):
    with pytest.raises(ValidationError):
        parse_query_to_dict(INVALID_QUERY_3, properties_schema)


def test_invalid_expression(properties_schema: dict):
    with pytest.raises(ValueError):
        parse_query_to_dict(INVALID_QUERY_4, properties_schema)
