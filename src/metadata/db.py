from typing import Any

from duckdb import DuckDBPyConnection
from loguru import logger

from context.context import (
    CreateSinkContext,
    CreateSourceContext,
    CreateTableContext,
    CreateViewContext,
    CreateSecretContext,
    SelectContext,
)


METADATA_TABLE_TABLE_NAME = "__table_metadata"
METADATA_SOURCE_TABLE_NAME = "__source_metadata"
METADATA_VIEW_TABLE_NAME = "__view_metadata"
METADATA_MACRO_TABLE_NAME = "__macro_metadata"
METADATA_SINK_TABLE_NAME = "__sink_metadata"
METADATA_SECRET_TABLE_NAME = "__secret_metadata"


def init_metadata_store(conn: DuckDBPyConnection) -> None:
    # Create table for lookup macro definition
    macro_table_to_def = f"""
    CREATE TABLE {METADATA_MACRO_TABLE_NAME} (
        macro_name STRING,
        fields STRING[]
    );
    """
    conn.execute(macro_table_to_def)

    table_metadata = f"""
    CREATE TABLE {METADATA_TABLE_TABLE_NAME} (
        table_name STRING,
        last_batch_id INTEGER,
        lookup BOOL
    )
    """
    conn.execute(table_metadata)

    source_metadata = f"""
    CREATE TABLE {METADATA_SOURCE_TABLE_NAME} (
        source_name STRING,
        last_batch_id INTEGER
    )
    """
    conn.execute(source_metadata)

    view_metadata = f"""
    CREATE TABLE {METADATA_VIEW_TABLE_NAME} (
        view_name STRING,
        materialized BOOL
    )
    """
    conn.execute(view_metadata)

    sink_metadata = f"""
    CREATE TABLE {METADATA_SINK_TABLE_NAME} (
        sink_name STRING,
    )
    """
    conn.execute(sink_metadata)

    secret_metadata = f"""
    CREATE TABLE {METADATA_SECRET_TABLE_NAME} (
        secret_name STRING,
        secret_value STRING
    )
    """
    conn.execute(secret_metadata)


def insert_table_metadata(
    conn: DuckDBPyConnection, context: CreateTableContext
) -> None:
    table_name = context.name
    insert = f"""
    INSERT INTO {METADATA_TABLE_TABLE_NAME} (table_name, last_batch_id, lookup)
    VALUES ('{table_name}', 0, {context.lookup});
    """
    conn.execute(insert)


def insert_source_metadata(
    conn: DuckDBPyConnection, context: CreateSourceContext
) -> None:
    source_name = context.name
    insert = f"""
    INSERT INTO {METADATA_SOURCE_TABLE_NAME} (source_name, last_batch_id)
    VALUES ('{source_name}', 0);
    """
    conn.execute(insert)


def insert_view_metadata(conn: DuckDBPyConnection, context: CreateViewContext) -> None:
    view_name = context.name
    insert = f"""
    INSERT INTO {METADATA_VIEW_TABLE_NAME} (view_name, materialized)
    VALUES ('{view_name}', {context.materialized});
    """
    conn.execute(insert)


def insert_sink_metadata(conn: DuckDBPyConnection, context: CreateSinkContext) -> None:
    sink_name = context.name
    insert = f"""
    INSERT INTO {METADATA_SINK_TABLE_NAME} (sink_name)
    VALUES ('{sink_name}');
    """
    conn.execute(insert)


def insert_secret_metadata(
    conn: DuckDBPyConnection, context: CreateSecretContext
) -> None:
    secret_name = context.name
    secret_value = context.value
    insert = f"""
    INSERT INTO {METADATA_SECRET_TABLE_NAME} (secret_name, secret_value)
    VALUES ('{secret_name}', '{secret_value}');
    """
    conn.execute(insert)


def get_secret_value_by_name(conn: DuckDBPyConnection, secret_name: str) -> str:
    query = f"""
    SELECT secret_value
    FROM {METADATA_SECRET_TABLE_NAME}
    WHERE secret_name = '{secret_name}';
    """
    res: list[tuple] = conn.execute(query).fetchall()

    return res[0][0]


def get_macro_definition_by_name(
    conn: DuckDBPyConnection, macro_name: str
) -> tuple[str, list[str]]:
    query = f"""
    SELECT 
        macro_name,
        fields
    FROM {METADATA_MACRO_TABLE_NAME}
    WHERE macro_name = '{macro_name}';
    """
    res = conn.execute(query).fetchall()

    assert len(res) > 0, (
        f"no result for macro definition {macro_name}, has it been registered ?"
    )

    return res[0]


def create_macro_definition(
    conn: DuckDBPyConnection, macro_name: str, fields: list[str]
) -> None:
    query = f"""
    INSERT INTO {METADATA_MACRO_TABLE_NAME} (macro_name, fields, macro_sql)
    VALUES ('{macro_name}', {fields});
    """
    conn.execute(query)


def get_lookup_tables(conn: DuckDBPyConnection) -> list[str]:
    query = f"""
        SELECT table_name FROM {METADATA_TABLE_TABLE_NAME} WHERE lookup IS TRUE;
        """
    temp_tables = [str(table_name[0]) for table_name in conn.execute(query).fetchall()]

    return temp_tables


def get_tables(conn: DuckDBPyConnection) -> list[str]:
    query = f"""
        SELECT table_name FROM {METADATA_TABLE_TABLE_NAME} WHERE lookup IS FALSE;
        """
    tables = [str(table_name[0]) for table_name in conn.execute(query).fetchall()]

    return tables


def get_views(conn: DuckDBPyConnection) -> list[str]:
    query = f"""
    SELECT view_name FROM {METADATA_VIEW_TABLE_NAME}
    """
    views = [str(table_name[0]) for table_name in conn.execute(query).fetchall()]

    return views


def get_duckdb_tables(conn: DuckDBPyConnection) -> list[str]:
    """
    Get all duckdb table names, this is because both backend and cache are currently
    relying on duckdb tables. (i.e views / mviews are in fact ... tables !)
    """
    query = """
    SELECT table_name FROM duckdb_tables;
    """
    tables = [str(table_name[0]) for table_name in conn.execute(query).fetchall()]

    return tables


def create_source(
    conn: DuckDBPyConnection,
    context: CreateSourceContext,
) -> str:
    query = context.query
    conn.execute(query)
    insert_source_metadata(conn, context)
    logger.info("[db] Registered source: {}", context.name)
    return "CREATE SOURCE"


def create_table(
    conn: DuckDBPyConnection,
    context: CreateTableContext,
) -> str:
    query = context.query
    conn.execute(query)
    insert_table_metadata(conn, context)
    logger.info("[db] Registered table: {}", context.name)
    return "CREATE TABLE"


def create_view(
    conn: DuckDBPyConnection,
    context: CreateViewContext,
) -> str:
    query = context.query
    conn.execute(query)
    insert_view_metadata(conn, context)
    logger.info("[db] Registered view: {}", context.name)
    return f"CREATE {'MATERIALIZED' if context.materialized else ''}VIEW"


def create_sink(
    conn: DuckDBPyConnection,
    context: CreateSinkContext,
) -> str:
    insert_sink_metadata(conn, context)
    logger.info("[db] Registered sink: {}", context.name)
    return "CREATE SINK"


def create_secret(
    conn: DuckDBPyConnection,
    context: CreateSecretContext,
) -> str:
    insert_secret_metadata(conn, context)
    logger.info("[db] Registerd secret: {}", context.name)
    return "CREATE SECRET"


def get_batch_id_from_table_metadata(conn: DuckDBPyConnection, table_name: str) -> int:
    query = f"""
        SELECT *
        FROM {METADATA_TABLE_TABLE_NAME}
        WHERE table_name = '{table_name}';
    """
    res = conn.execute(query).fetchall()
    return res[0][1]


def get_batch_id_from_source_metadata(
    conn: DuckDBPyConnection, source_name: str
) -> int:
    query = f"""
        SELECT *
        FROM {METADATA_SOURCE_TABLE_NAME}
        WHERE source_name = '{source_name}';
    """
    res = conn.execute(query).fetchall()
    return res[0][1]


def update_batch_id_in_table_metadata(
    conn: DuckDBPyConnection, table_name: str, batch_id: int
) -> None:
    query = f"""
    UPDATE {METADATA_TABLE_TABLE_NAME}
    SET last_batch_id={batch_id}
    WHERE table_name = '{table_name}';
    """
    conn.execute(query)


def update_batch_id_in_source_metadata(
    conn: DuckDBPyConnection, source_name: str, batch_id: int
) -> None:
    query = f"""
    UPDATE {METADATA_SOURCE_TABLE_NAME}
    SET last_batch_id={batch_id}
    WHERE source_name = '{source_name}';
    """
    conn.execute(query)


def get_table_schema(conn: DuckDBPyConnection, table_name: str) -> list[dict[str, Any]]:
    result = conn.execute(f"DESCRIBE {table_name}").fetchall()
    return [{"name": row[0], "type": row[1]} for row in result]


def get_select_schema(conn: DuckDBPyConnection, context: SelectContext):
    result = conn.execute(f"{context.query} LIMIT 0")
    return [{"name": col[0], "type": col[1]} for col in result.description or []]


# TODO: change method to resolve subquery
def resolve_schema(con, relation: str | SelectContext):
    if isinstance(relation, SelectContext):
        sql = relation.query
        schema = get_select_schema(con, relation)
    else:
        sql = f"SELECT * FROM {relation}"
        schema = get_table_schema(con, relation)
    return sql, schema
