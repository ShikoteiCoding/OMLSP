from typing import Any

from duckdb import DuckDBPyConnection
from loguru import logger

from context.context import (
    CreateLookupTableContext,
    CreateSinkContext,
    CreateTableContext,
    CreateViewContext,
    SelectContext,
)

METADATA_TABLE_NAME = "__table_metadata"
METADATA_VIEW_NAME = "__view_metadata"
METADATA_MACRO_NAME = "__macro_metadata"
METADATA_SINK_NAME = "__sink_metadata"


def init_metadata_store(con: DuckDBPyConnection) -> None:
    # Create table for lookup macro definition
    macro_table_to_def = f"""
    CREATE TABLE {METADATA_MACRO_NAME} (
        macro_name STRING,
        fields STRING[]
    );
    """
    con.sql(macro_table_to_def)

    table_metadata = f"""
    CREATE TABLE {METADATA_TABLE_NAME} (
        table_name STRING,
        last_batch_id INTEGER,
        is_lookup BOOLEAN
    )
    """
    con.sql(table_metadata)

    view_metadata = f"""
    CREATE TABLE {METADATA_VIEW_NAME} (
        view_name STRING,
    )
    """
    con.sql(view_metadata)

    sink_metadata = f"""
    CREATE TABLE {METADATA_SINK_NAME} (
        sink_name STRING,
    )
    """
    con.sql(sink_metadata)


def insert_table_metadata(
    con: DuckDBPyConnection, context: CreateTableContext | CreateLookupTableContext
) -> None:
    table_name = context.name
    if isinstance(context, CreateTableContext):
        insert = f"""
        INSERT INTO {METADATA_TABLE_NAME} (table_name, last_batch_id, is_lookup)
        VALUES ('{table_name}', 0, false);
        """
    elif isinstance(context, CreateLookupTableContext):
        insert = f"""
        INSERT INTO {METADATA_TABLE_NAME} (table_name, last_batch_id, is_lookup)
        VALUES ('{table_name}', 0, true);
        """
    con.sql(insert)


def insert_view_metadata(con: DuckDBPyConnection, context: CreateViewContext) -> None:
    view_name = context.name
    insert = f"""
    INSERT INTO {METADATA_VIEW_NAME} (view_name)
    VALUES ('{view_name}');
    """
    con.sql(insert)

def insert_sink_metadata(con: DuckDBPyConnection, context: CreateSinkContext) -> None:
    sink_name = context.name
    insert = f"""
    INSERT INTO {METADATA_SINK_NAME} (sink_name)
    VALUES ('{sink_name}');
    """
    con.sql(insert)


def get_macro_definition_by_name(
    con: DuckDBPyConnection, macro_name: str
) -> tuple[str, list[str]]:
    query = f"""
    SELECT 
        macro_name,
        fields
    FROM {METADATA_MACRO_NAME}
    WHERE macro_name = '{macro_name}';
    """
    res = con.sql(query).fetchall()

    assert len(res) > 0, (
        f"no result for macro definition {macro_name}, has it been registered ?"
    )

    return res[0]


def create_macro_definition(
    con: DuckDBPyConnection, macro_name: str, fields: list[str]
) -> None:
    query = f"""
    INSERT INTO {METADATA_MACRO_NAME} (macro_name, fields)
    VALUES ('{macro_name}', {fields});
    """
    con.sql(query)


def get_lookup_tables(con: DuckDBPyConnection) -> list:
    query = """
        SELECT table_name FROM duckdb_tables WHERE temporary IS TRUE;
        """
    temp_tables = [str(table_name[0]) for table_name in con.sql(query).fetchall()]

    return temp_tables


def get_tables(con: DuckDBPyConnection) -> list:
    query = """
        SELECT table_name FROM duckdb_tables WHERE temporary IS FALSE;
        """
    tables = [str(table_name[0]) for table_name in con.sql(query).fetchall()]

    return tables


def create_table(
    con: DuckDBPyConnection,
    context: CreateTableContext | CreateLookupTableContext,
) -> str:
    query = context.query
    con.execute(query)
    insert_table_metadata(con, context)
    logger.info(f"[db] Registered table: {context.name}")
    return "CREATE TABLE"


def create_view(
    con: DuckDBPyConnection,
    context: CreateViewContext,
) -> str:
    query = context.query
    con.execute(query)
    insert_view_metadata(con, context)
    logger.info(f"[db] Registered view: {context.name}")
    return "CREATE VIEW"


def create_sink(
    con: DuckDBPyConnection,
    context: CreateSinkContext,
) -> str:
    insert_sink_metadata(con, context)
    logger.info(f"[db] Registered sink: {context.name}")
    return "CREATE VIEW"


def get_batch_id_from_table_metadata(con: DuckDBPyConnection, table_name: str) -> int:
    query = f"""
        SELECT *
        FROM {METADATA_TABLE_NAME}
        WHERE table_name = '{table_name}';
    """
    res = con.sql(query).fetchall()
    return res[0][1]


def update_batch_id_in_table_metadata(
    con: DuckDBPyConnection, table_name: str, batch_id: int
) -> None:
    query = f"""
    UPDATE {METADATA_TABLE_NAME}
    SET last_batch_id={batch_id}
    WHERE table_name = '{table_name}';
    """
    con.execute(query)


def get_table_schema(con: DuckDBPyConnection, table_name: str) -> list[dict[str, Any]]:
    result = con.execute(f"DESCRIBE {table_name}").fetchall()
    return [{"name": row[0], "type": row[1]} for row in result]


def get_select_schema(con: DuckDBPyConnection, context: SelectContext):
    result = con.execute(f"{context.query} LIMIT 0")
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
