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
        fields STRING[]
    );
    """
    conn.sql(macro_table_to_def)

    table_metadata = f"""
    CREATE TABLE {METADATA_TABLE_TABLE_NAME} (
        table_name STRING,
        last_batch_id INTEGER
    )
    """
    conn.sql(table_metadata)

    view_metadata = f"""
    CREATE TABLE {METADATA_VIEW_TABLE_NAME} (
        view_name STRING,
        last_batch_id INTEGER
    )
    """
    conn.sql(view_metadata)

    view_materialized_metadata = f"""
    CREATE TABLE {METADATA_VIEW_MATERIALIZED_TABLE_NAME} (
        view_materialized_name STRING,
        last_batch_id INTEGER
    )
    """
    conn.sql(view_materialized_metadata)

    sink_metadata = f"""
    CREATE TABLE {METADATA_SINK_TABLE_NAME} (
        sink_name STRING,
    )
    """
    conn.sql(sink_metadata)

    secret_metadata = f"""
    CREATE TABLE {METADATA_SECRET_TABLE_NAME} (
        secret_name STRING,
        secret_value STRING
    )
    """
    conn.sql(secret_metadata)


def insert_table_metadata(
    conn: DuckDBPyConnection, context: CreateTableContext
) -> None:
    table_name = context.name
    insert = f"""
    INSERT INTO {METADATA_TABLE_TABLE_NAME} (table_name, last_batch_id)
    VALUES ('{table_name}', 0);
    """
    conn.sql(insert)


def insert_view_metadata(conn: DuckDBPyConnection, context: CreateViewContext) -> None:
    view_name = context.name
    insert = f"""
    INSERT INTO {METADATA_VIEW_TABLE_NAME} (view_name, last_batch_id)
    VALUES ('{view_name}', 0);
    """
    conn.sql(insert)


def insert_view_materialized_metadata(
    conn: DuckDBPyConnection, context: CreateMaterializedViewContext
) -> None:
    view_materialized_name = context.name
    insert = f"""
    INSERT INTO {METADATA_VIEW_MATERIALIZED_TABLE_NAME} (view_materialized_name, last_batch_id)
    VALUES ('{view_materialized_name}', 0);
    """
    conn.sql(insert)


def insert_sink_metadata(conn: DuckDBPyConnection, context: CreateSinkContext) -> None:
    sink_name = context.name
    insert = f"""
    INSERT INTO {METADATA_SINK_TABLE_NAME} (sink_name)
    VALUES ('{sink_name}');
    """
    conn.sql(insert)


def insert_secret_metadata(
    conn: DuckDBPyConnection, context: CreateSecretContext
) -> None:
    secret_name = context.name
    secret_value = context.value
    insert = f"""
    INSERT INTO {METADATA_SECRET_TABLE_NAME} (secret_name, secret_value)
    VALUES ('{secret_name}', '{secret_value}');
    """
    conn.sql(insert)


def get_secret_value_by_name(conn: DuckDBPyConnection, secret_name: str) -> str:
    query = f"""
    SELECT secret_value
    FROM {METADATA_SECRET_TABLE_NAME}
    WHERE secret_name = '{secret_name}';
    """
    res = conn.sql(query).fetchall()

    return res[0]


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
    res = conn.sql(query).fetchall()

    assert len(res) > 0, (
        f"no result for macro definition {macro_name}, has it been registered ?"
    )

    return res[0]


def create_macro_definition(
    conn: DuckDBPyConnection, macro_name: str, fields: list[str]
) -> None:
    query = f"""
    INSERT INTO {METADATA_MACRO_TABLE_NAME} (macro_name, fields)
    VALUES ('{macro_name}', {fields});
    """
    conn.sql(query)


def get_lookup_tables(conn: DuckDBPyConnection) -> list:
    query = """
        SELECT table_name FROM duckdb_tables WHERE temporary IS TRUE;
        """
    temp_tables = [str(table_name[0]) for table_name in conn.sql(query).fetchall()]

    return temp_tables


def get_tables(conn: DuckDBPyConnection) -> list:
    query = """
        SELECT table_name FROM duckdb_tables WHERE temporary IS FALSE;
        """
    tables = [str(table_name[0]) for table_name in conn.sql(query).fetchall()]

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
    res = conn.sql(query).fetchall()
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
            WHERE view_materialized_name = '{view_name}';
        """
        res = conn.sql(query).fetchall()
    else:
        query = f"""
            SELECT *
            FROM {METADATA_VIEW_TABLE_NAME}
            WHERE view_name = '{view_name}';
        """
        res = conn.sql(query).fetchall()
    return res[0][1]


def update_batch_id_in_view_metadata(
    conn: DuckDBPyConnection, view_name: str, batch_id: int, is_materialized: bool
) -> None:
    if is_materialized:
        query = f"""
        UPDATE {METADATA_VIEW_MATERIALIZED_TABLE_NAME}
        SET last_batch_id={batch_id}
        WHERE view_materialized_name = '{view_name}';
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
