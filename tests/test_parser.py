import json
import pytest

from apscheduler.triggers.cron import CronTrigger
from datetime import timezone
from src.sql.sqlparser.parser import extract_one_query_context
from src.sql.file import iter_sql_statements
from src.context.context import (
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
            "jq": ".",
        },
        query="CREATE TABLE example (url TEXT)",
        trigger=CronTrigger.from_crontab("*/5 * * * *", timezone=timezone.utc),
    ),
    CreateTableContext(
        name="example_2",
        properties={
            "connector": "http",
            "url": "https://httpbin.org/get",
            "method": "GET",
            "jq": ".",
        },
        query="CREATE TABLE example_2 (url TEXT)",
        trigger=CronTrigger.from_crontab("*/1 * * * *", timezone=timezone.utc),
    ),
    CreateLookupTableContext(
        name="lookup_example",
        properties={
            "connector": "lookup-http",
            "url": "https://httpbin.org/get",
            "method": "GET",
            "jq": ".",
        },
        query="CREATE TEMPORARY TABLE lookup_example (url TEXT)",
        dynamic_columns=["url"],
        columns={"url": "TEXT"},
    ),
]

VALID_SELECT_QUERY = "SELECT * FROM example;"

VALID_SELECT_RESULT = [
    SelectContext(
        columns=[""],
        joins={},
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


@pytest.mark.skip(reason="No equality check between crontab, needs to mock")
def test_valid_create_table_with_columns_and_properties(properties_schema: dict):
    with open("example.sql", "w", encoding="utf-8") as f:
        f.write(VALID_CREATE_QUERY)

    for i, sql_statement in enumerate(iter_sql_statements("example.sql")):
        result = extract_one_query_context(sql_statement, properties_schema)
        assert result == VALID_CREATE_RESULT[i]


def test_valid_select_statement(properties_schema: dict):
    with open("example.sql", "w", encoding="utf-8") as f:
        f.write(VALID_SELECT_QUERY)

    for i, sql_statement in enumerate(iter_sql_statements("example.sql")):
        result = extract_one_query_context(sql_statement, properties_schema)
        assert result == VALID_SELECT_RESULT[i]


def test_invalid_property_not_literal_timestamp(properties_schema: dict):
    with open("example.sql", "w", encoding="utf-8") as f:
        f.write(INVALID_QUERY_2)

    for i, sql_statement in enumerate(iter_sql_statements("example.sql")):
        with pytest.raises(Exception):
            result = extract_one_query_context(sql_statement, properties_schema)


def test_invalid_property_null_value(properties_schema: dict):
    with open("example.sql", "w", encoding="utf-8") as f:
        f.write(INVALID_QUERY_3)
    for i, sql_statement in enumerate(iter_sql_statements("example.sql")):
        with pytest.raises(Exception):
            extract_one_query_context(sql_statement, properties_schema)


def test_invalid_expression(properties_schema: dict):
    with open("example.sql", "w", encoding="utf-8") as f:
        f.write(INVALID_QUERY_4)
    for i, sql_statement in enumerate(iter_sql_statements("example.sql")):
        with pytest.raises(Exception):
            extract_one_query_context(sql_statement, properties_schema)
