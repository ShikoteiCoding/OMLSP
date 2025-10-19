from typing import Any

from duckdb import DuckDBPyConnection
from loguru import logger

from context.context import (
    CreateTableContext,
    CreateSinkContext,
    CreateViewContext,
    CreateMaterializedViewContext,
    CreateSecretContext,
    SelectContext,
)

import re

METADATA_TABLE_TABLE_NAME = "__table_metadata"
METADATA_VIEW_TABLE_NAME = "__view_metadata"
METADATA_VIEW_MATERIALIZED_TABLE_NAME = "__view_materialized_metadata"
METADATA_MACRO_TABLE_NAME = "__macro_metadata"
METADATA_SINK_TABLE_NAME = "__sink_metadata"
METADATA_SECRET_TABLE_NAME = "__secret_metadata"


def init_metadata_store(conn: DuckDBPyConnection) -> None:
    # Create table for lookup macro definition
    macro_table_to_def = f"""
    CREATE TABLE {METADATA_MACRO_TABLE_NAME} (
        macro_name STRING,
        fields STRING[],
        macro_sql STRING
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

    view_metadata = f"""
    CREATE TABLE {METADATA_VIEW_TABLE_NAME} (
        view_name STRING,
        last_batch_id INTEGER
    )
    """
    conn.execute(view_metadata)

    view_materialized_metadata = f"""
    CREATE TABLE {METADATA_VIEW_MATERIALIZED_TABLE_NAME} (
        view_name STRING,
        last_batch_id INTEGER
    )
    """
    conn.execute(view_materialized_metadata)

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


def insert_view_metadata(conn: DuckDBPyConnection, context: CreateViewContext) -> None:
    view_name = context.name
    insert = f"""
    INSERT INTO {METADATA_VIEW_TABLE_NAME} (view_name, last_batch_id)
    VALUES ('{view_name}', 0);
    """
    conn.execute(insert)


def insert_view_materialized_metadata(
    conn: DuckDBPyConnection, context: CreateMaterializedViewContext
) -> None:
    view_name = context.name
    insert = f"""
    INSERT INTO {METADATA_VIEW_MATERIALIZED_TABLE_NAME} (view_name, last_batch_id)
    VALUES ('{view_name}', 0);
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
    conn: DuckDBPyConnection, macro_name: str, fields: list[str], macro_sql: str
) -> None:
    query = f"""
    INSERT INTO {METADATA_MACRO_TABLE_NAME} (macro_name, fields, macro_sql)
    VALUES ('{macro_name}', {fields}, '{macro_sql}');
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
    UNION ALL
    SELECT view_name FROM {METADATA_VIEW_MATERIALIZED_TABLE_NAME}
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
    return "CREATE VIEW"


def create_view_materialized(
    conn: DuckDBPyConnection,
    context: CreateMaterializedViewContext,
) -> str:
    query = context.query
    conn.execute(query)
    insert_view_materialized_metadata(conn, context)
    logger.info("[db] Registered view materialized: {}", context.name)
    return "CREATE MATERIALIZED VIEW"


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


def update_batch_id_in_table_metadata(
    conn: DuckDBPyConnection, table_name: str, batch_id: int
) -> None:
    query = f"""
    UPDATE {METADATA_TABLE_TABLE_NAME}
    SET last_batch_id={batch_id}
    WHERE table_name = '{table_name}';
    """
    conn.execute(query)


def get_batch_id_from_view_metadata(
    conn: DuckDBPyConnection, view_name: str, is_materialized: bool
) -> int:
    if is_materialized:
        query = f"""
            SELECT *
            FROM {METADATA_VIEW_MATERIALIZED_TABLE_NAME}
            WHERE view_name = '{view_name}';
        """
        res = conn.execute(query).fetchall()
    else:
        query = f"""
            SELECT *
            FROM {METADATA_VIEW_TABLE_NAME}
            WHERE view_name = '{view_name}';
        """
        res = conn.execute(query).fetchall()
    return res[0][1]


def update_batch_id_in_view_metadata(
    conn: DuckDBPyConnection, view_name: str, batch_id: int, is_materialized: bool
) -> None:
    if is_materialized:
        query = f"""
        UPDATE {METADATA_VIEW_MATERIALIZED_TABLE_NAME}
        SET last_batch_id={batch_id}
        WHERE view_name = '{view_name}';
        """
    else:
        query = f"""
        UPDATE {METADATA_VIEW_TABLE_NAME}
        SET last_batch_id={batch_id}
        WHERE view_name = '{view_name}';
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


def get_known_macros(registry_conn: DuckDBPyConnection) -> set[str]:
    rows = registry_conn.execute(
        f"SELECT macro_name FROM {METADATA_MACRO_TABLE_NAME}"
    ).fetchall()
    return {row[0] for row in rows}


def detect_referenced_macros_optimized(
    transform_query: str, known_macros: set[str]
) -> set[str]:
    if not known_macros:
        return set()

    # Sort macros descending to ensure longer names are processed first in the pattern
    sorted_macros = sorted(known_macros, key=len, reverse=True)

    # Build the pattern: \b(MACRO_A|MACRO_B)\b
    pattern_string = "|".join(re.escape(name) for name in sorted_macros)
    full_pattern = rf"\b({pattern_string})\b"

    # Run the single, fast search
    found_matches = re.findall(full_pattern, transform_query, re.IGNORECASE)

    # Map results back to the original casing
    found_macros_lower = {name.lower() for name in found_matches}

    original_casing_macros = {
        original_name
        for original_name in known_macros
        if original_name.lower() in found_macros_lower
    }

    return original_casing_macros


def lazy_sync_macros(
    registry_conn: DuckDBPyConnection,
    exec_conn: DuckDBPyConnection,
    transform_query: str,
    loaded_cache: set[str],
) -> None:
    """
    Synchronizes only the necessary, missing macros from persistent registry_conn 
    into the runtime execution exec_conn

    This uses a 'lazy, on-demand' strategy, prioritizing efficiency by:
    1. Detecting dependencies quickly using a single optimized regex (O(M))
    2. Using a fast Python Set (loaded_cache) to track already registered macros (O(1))
    """
    known_macros = get_known_macros(registry_conn)
    needed_macros = detect_referenced_macros_optimized(transform_query, known_macros)

    macros_to_sync_sql = []
    macros_to_sync_names = []

    for macro_name in needed_macros:
        if macro_name not in loaded_cache:
            sql_row = registry_conn.execute(
                f"SELECT macro_sql FROM {METADATA_MACRO_TABLE_NAME} WHERE macro_name = '{macro_name}'"
            ).fetchone()

            if sql_row:
                macros_to_sync_sql.append(sql_row[0])
                macros_to_sync_names.append(macro_name)

    if macros_to_sync_sql:
        for sql in macros_to_sync_sql:
            exec_conn.execute(sql)
        loaded_cache.update(macros_to_sync_names)
