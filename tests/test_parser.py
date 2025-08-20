import json
import pytest

from src.parser import (
    extract_query_contexts,
    CreateTableContext,
    CreateLookupTableContext,
    SelectContext,
)

prop_schema_filepath = "src/properties.schema.json"


@pytest.fixture
def properties_schema() -> dict:
    with open(prop_schema_filepath, "rb") as fo:
        properties_schema = json.loads(fo.read().decode("utf-8"))
    return properties_schema


VALID_CREATE_QUERY = """
CREATE TABLE example (
    url STRING
)
WITH (
    'connector' = 'http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'schedule' = '*/5 * * * *',
    'jq' = '.'
);

CREATE TABLE example_2 (
    url STRING
)
WITH (
    'connector' = 'http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'schedule' = '*/1 * * * *',
    'jq' = '.'
);

CREATE TEMP TABLE lookup_example (
    $url STRING
)
WITH (
    'connector' = 'lookup-http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'schedule' = '*/1 * * * *',
    'jq' = '.'
);
"""

VALID_CREATE_RESULT = [
    CreateTableContext(
        name="example",
        properties={
            "connector": "http",
            "url": "https://httpbin.org/get",
            "method": "GET",
            "schedule": "*/5 * * * *",
            "jq": ".",
        },
        query="CREATE TABLE example (url TEXT)",
    ),
    CreateTableContext(
        name="example_2",
        properties={
            "connector": "http",
            "url": "https://httpbin.org/get",
            "method": "GET",
            "schedule": "*/1 * * * *",
            "jq": ".",
        },
        query="CREATE TABLE example_2 (url TEXT)",
    ),
    CreateLookupTableContext(
        name="lookup_example",
        properties={
            "connector": "lookup-http",
            "url": "https://httpbin.org/get",
            "method": "GET",
            "schedule": "*/1 * * * *",
            "jq": ".",
        },
        query="CREATE TEMPORARY TABLE lookup_example (url TEXT)",
        dynamic_columns=["url"],
        columns={"url": "TEXT"},
    ),
]

VALID_SELECT_QUERY = """
SELECT * FROM example;
"""

VALID_SELECT_RESULT = [
    SelectContext(
        columns=[""],
        joins=[],
        table="example",
        where="",
        alias="example",
        query="SELECT * FROM example",
    )
]

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
    results = extract_query_contexts(VALID_CREATE_QUERY, properties_schema)
    assert results == VALID_CREATE_RESULT


def test_valid_select_statement(properties_schema: dict):
    results = extract_query_contexts(VALID_SELECT_QUERY, properties_schema)
    assert results == VALID_SELECT_RESULT


def test_invalid_property_not_literal_timestamp(properties_schema: dict):
    with pytest.raises(Exception):
        extract_query_contexts(INVALID_QUERY_2, properties_schema)


def test_invalid_property_null_value(properties_schema: dict):
    with pytest.raises(Exception):
        extract_query_contexts(INVALID_QUERY_3, properties_schema)


def test_invalid_expression(properties_schema: dict):
    with pytest.raises(Exception):
        extract_query_contexts(INVALID_QUERY_4, properties_schema)
