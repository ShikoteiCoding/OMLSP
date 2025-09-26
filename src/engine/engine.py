import time
import json
import trio

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection
from duckdb.functional import FunctionNullHandling, PythonUDFType
from duckdb.typing import VARCHAR, DuckDBPyType
from functools import partial
from string import Template
from types import FunctionType, CoroutineType
from typing import Any, Iterable, Callable, AsyncGenerator

import polars as pl
import pyarrow as pa
from duckdb import struct_type
from loguru import logger

from context.context import (
    CreateHTTPLookupTableContext,
    CreateHTTPTableContext,
    CreateWSTableContext,
    SelectContext,
    ScheduledTaskContext,
    ContinousTaskContext,
    TransformTaskContext,
    SinkTaskContext,
)
from inout import persist
from metadata import (
    create_macro_definition,
    get_batch_id_from_table_metadata,
    get_lookup_tables,
    get_macro_definition_by_name,
    update_batch_id_in_table_metadata,
    get_batch_id_from_view_materialized_metadata,
    update_batch_id_i_view_materialized_metadata,
)
from external import build_http_requester, build_ws_generator
from confluent_kafka import Producer


DUCKDB_TO_PYARROW_PYTYPE = {
    "VARCHAR": pa.string(),
    "TEXT": pa.string(),
    "TIMESTAMP_S": pa.timestamp("s"),
    "TIMESTAMP_MS": pa.timestamp("ms"),
    "TIMESTAMP": pa.timestamp("us"),
    "TIMESTAMP_NS": pa.timestamp("ns"),
    "DATETIME": pa.timestamp("us"),
    "FLOAT": pa.float32(),
    "DOUBLE": pa.float64(),
    "INTEGER": pa.int32(),
    "INT": pa.int32(),
    "BIGINT": pa.int64(),
    "SMALLINT": pa.int16(),
    "TINYINT": pa.int8(),
    "BOOLEAN": pa.bool_(),
    "DATE": pa.date32(),
    "DECIMAL": pa.decimal128(18, 2),
}


def build_lookup_properties(
    properties: dict[str, str], context: dict[str, Any]
) -> dict[str, str]:
    new_props = {}

    for key, value in properties.items():
        if key in ["method"]:  # TODO: ignore jq for now
            new_props[key] = value
            continue
        template = Template(value)
        new_props[key] = template.substitute(context)

    return new_props


def build_scalar_udf(
    properties: dict[str, str],
    dynamic_columns: list[str],
    return_type: DuckDBPyType,
    **kwargs,
) -> dict[str, Any]:
    arity = len(dynamic_columns)

    child_types = []
    for subtype in return_type.children:
        child_types.append(
            pa.field(subtype[0], DUCKDB_TO_PYARROW_PYTYPE[str(subtype[1])])
        )
    return_type_arrow = pa.struct(child_types)

    arg_names = [f"a{i}" for i in range(1, arity + 1)]

    def core_udf(*arrays: pa.ChunkedArray) -> pa.Array:
        results = []
        for chunks in zip(*(arr.chunks for arr in arrays)):
            py_chunks = [chunk.to_pylist() for chunk in chunks]
            chunk_rows = zip(*py_chunks)
            chunk_results = process_elements(chunk_rows, properties)
            results.extend(chunk_results)
        return pa.array(results, type=return_type_arrow)

    udf = eval(
        f"lambda {', '.join(arg_names)}: core_udf({', '.join(arg_names)})",
        {"core_udf": core_udf},
    )

    def process_elements(
        rows: Iterable[tuple[Any, ...]],
        properties: dict[str, str],
    ) -> pa.Array:
        def _inner(row: tuple[Any, ...]) -> dict[str, Any]:
            context = dict(zip(dynamic_columns, row))
            lookup_properties = build_lookup_properties(properties, context)
            default_response = context.copy()
            requester = build_http_requester(lookup_properties, is_async=False)

            try:
                response = requester()
                if isinstance(response, dict):
                    return context | response
                if isinstance(response, list) and len(response) >= 1:
                    default_response |= response[-1]
            except Exception as e:
                logger.exception(f"HTTP request failed: {e}")

            return default_response

        with ThreadPoolExecutor() as executor:
            results = list(executor.map(_inner, rows))

        return results

    return {
        "name": kwargs.get("name"),
        "function": udf,
        "parameters": [VARCHAR for _ in range(arity)],
        "return_type": return_type,
        "type": PythonUDFType.ARROW,
        "null_handling": FunctionNullHandling.SPECIAL,
        **kwargs,
    }


async def http_source_executable(
    task_id: str,
    conn: DuckDBPyConnection,
    table_name: str,
    start_time: datetime,
    http_requester: FunctionType,
    *args,
    **kwargs,
) -> pl.DataFrame:
    # TODO: no provided api execution_time
    # using trigger.get_next_fire_time is costly (see code)
    execution_time = start_time
    batch_id = get_batch_id_from_table_metadata(conn, table_name)
    logger.info(f"[{table_name}{{{batch_id}}}] / @ {execution_time}")

    records = await http_requester()
    logger.debug(
        f"[{table_name}{{{batch_id}}}] - http number of responses: {len(records)} - batch {batch_id}"
    )

    if len(records) > 0:
        epoch = int(time.time() * 1_000)
        # TODO: type polars with duckdb table catalog
        df = pl.from_records(records)
        await persist(df, batch_id, epoch, table_name, conn)
    else:
        df = pl.DataFrame()

    update_batch_id_in_table_metadata(conn, table_name, batch_id + 1)
    return df


def build_http_source_executable(
    ctx: CreateHTTPTableContext,
) -> Callable[[str, DuckDBPyConnection], CoroutineType[Any, Any, pl.DataFrame]]:
    start_time = datetime.now(timezone.utc)
    properties = ctx.properties
    return partial(
        http_source_executable,
        table_name=ctx.name,
        start_time=start_time,
        http_requester=build_http_requester(properties, is_async=True),
    )


async def ws_source_executable(
    task_id: str,
    conn: DuckDBPyConnection,
    table_name: str,
    ws_generator: Callable[[], AsyncGenerator[Any, list[dict]]],
    *args,
    **kwargs,
) -> AsyncGenerator[Any, pl.DataFrame]:
    logger.info(f"[{task_id}] - starting ws executable")
    batch_id = get_batch_id_from_table_metadata(conn, table_name)
    while True:
        async for records in ws_generator():
            if len(records) > 0:
                epoch = int(time.time() * 1_000)
                # TODO: type polars with duckdb table catalog
                df = pl.from_records(records)
                await persist(df, batch_id, epoch, table_name, conn)
            else:
                df = pl.DataFrame()

            yield df
            update_batch_id_in_table_metadata(conn, table_name, batch_id + 1)


def build_ws_source_executable(
    ctx: CreateWSTableContext,
) -> Callable[[str, DuckDBPyConnection], AsyncGenerator[Any, pl.DataFrame]]:
    properties = ctx.properties
    table_name = ctx.name
    return partial(
        ws_source_executable,
        table_name=table_name,
        ws_generator=build_ws_generator(properties),
    )


def build_scheduled_source_executable(
    ctx: ScheduledTaskContext,
) -> Callable[[str, DuckDBPyConnection], CoroutineType[Any, Any, pl.DataFrame]]:
    if isinstance(ctx, CreateHTTPTableContext):
        return build_http_source_executable(ctx)

    else:
        raise NotImplementedError()


def build_continuous_source_executable(
    ctx: ContinousTaskContext,
) -> Callable[[str, DuckDBPyConnection], AsyncGenerator[Any, pl.DataFrame]]:
    if isinstance(ctx, CreateWSTableContext):
        return build_ws_source_executable(ctx)

    else:
        raise NotImplementedError()


def build_lookup_table_prehook(
    create_table_context: CreateHTTPLookupTableContext, connection: DuckDBPyConnection
) -> str:
    properties = create_table_context.properties
    table_name = create_table_context.name
    dynamic_columns = create_table_context.dynamic_columns
    columns = create_table_context.columns

    func_name = f"{table_name}_func"
    macro_name = f"{table_name}_macro"
    # TODO: handle other return than dict (for instance array http responses)
    return_type = struct_type(columns)  # typed struct from sql statement
    udf_params = build_scalar_udf(
        properties, dynamic_columns, return_type, name=func_name
    )
    # register scalar for row to row http call
    connection.create_function(**udf_params)
    logger.debug(f"registered function: {func_name}")

    # TODO: wrap SQL in function
    # register macro (to be injected in place of sql)
    __inner_tbl = "__inner_tbl"
    __deriv_tbl = "__deriv_tbl"
    func_def = (
        f"{func_name}({','.join([f'{__inner_tbl}.{col}' for col in dynamic_columns])})"
    )
    macro_def = f"{macro_name}(table_name, {','.join(dynamic_columns)})"
    output_cols = ", ".join(
        [f"struct.{col_name} AS {col_name}" for col_name, _ in columns.items()]
    )

    # TODO: move to metadata func
    connection.sql(f"""
        CREATE OR REPLACE MACRO {macro_def} AS TABLE
        SELECT
            {output_cols}
        FROM (
            SELECT
                {func_def} AS struct
            FROM query_table(table_name) AS {__inner_tbl}
        ) AS {__deriv_tbl};
    """)
    logger.debug(f"registered macro: {macro_name}")
    create_macro_definition(connection, macro_name, dynamic_columns)

    return macro_name


def build_substitute_macro_definition(
    con: DuckDBPyConnection,
    join_table: str,
    from_table: str,
    from_table_or_alias: str,
    join_table_or_alias: str,
) -> str:
    macro_name, fields = get_macro_definition_by_name(con, f"{join_table}_macro")
    scalar_func_fields = ",".join(
        [f"{from_table_or_alias}.{field}" for field in fields]
    )
    macro_definition = f'{macro_name}("{from_table}", {scalar_func_fields})'
    if join_table_or_alias == join_table:
        # TODO: fix bug, if table_name == alias
        # JOIN ohlc AS ohlc
        # -> this writes the AS statements and fails
        macro_definition += f"AS {join_table_or_alias}"
    return macro_definition


def pre_hook_select_statements(
    con: DuckDBPyConnection,
    ctx: SelectContext,
    tables: list[str],
) -> str:
    """Substitutes select statement query with lookup references to macro references."""
    original_query = ctx.query
    join_tables = ctx.joins
    lookup_tables = get_lookup_tables(con)

    # join query
    substitute_mapping = dict(zip(tables, tables))
    from_table = ctx.table
    from_table_or_alias = ctx.alias

    # for table in join and in lookup,
    # build the placeholder template mapping
    for join_table, join_table_or_alias in join_tables.items():
        if join_table in lookup_tables:
            substitute_mapping[join_table] = build_substitute_macro_definition(
                con, join_table, from_table, from_table_or_alias, join_table_or_alias
            )
        else:
            substitute_mapping[join_table] = join_table

    # Substritute lookup table placeholder with template
    query = Template(original_query).substitute(substitute_mapping)

    logger.debug(f"New overwritten select statement: {query}")
    return query


def duckdb_to_pl(con: DuckDBPyConnection, duckdb_sql: str) -> pl.DataFrame:
    query = duckdb_sql.strip()
    cursor = con.execute(query)
    rows = cursor.fetchall()
    # TODO: get schema from metadata
    if cursor.description:
        columns = [desc[0] for desc in cursor.description]
        df = pl.DataFrame(rows, orient="row", schema=columns)
        return df

    return pl.DataFrame()


def build_sink_executable(
        ctx: SinkTaskContext
    ) -> CoroutineType[Any, Any, None]:
    properties = ctx.properties
    producer = Producer({"bootstrap.servers": properties["server"]})
    topic = properties["topic"]
    return partial(
        kafka_sink,
        producer=producer,
        topic=topic,
    )


async def kafka_sink(
    task_id: str,
    conn: DuckDBPyConnection,
    producer: Producer,
    topic: str,
    df: pl.DataFrame,
) -> None:
    records = df.to_dicts()

    def _produce_all():
        for record in records:
            payload = json.dumps(record).encode("utf-8")
            producer.produce(topic, value=payload)
            producer.poll(0)
        producer.flush()

    await trio.to_thread.run_sync(_produce_all)


def build_transform_executable(
        ctx: TransformTaskContext, 
        is_materialized: bool
    ) -> Callable[[str, DuckDBPyConnection], CoroutineType[Any, Any, pl.DataFrame]]:
    return partial(
        transform_executable,
        ctx.name,
        ctx.columns,
        is_materialized,
    )


async def transform_executable(
    view_name: str,
    columns: list[str],
    is_materialized: bool,
    conn: DuckDBPyConnection,
    df: pl.DataFrame,
) -> pl.DataFrame:
    if columns == [""]:
        columns = df.columns
    transform_df = df.select(columns)

    if is_materialized:
        batch_id = get_batch_id_from_view_materialized_metadata(conn, view_name)
        epoch = int(time.time() * 1_000)
        await persist(transform_df, batch_id, epoch, view_name, conn)
        update_batch_id_i_view_materialized_metadata(conn, view_name, batch_id + 1)

    return transform_df


if __name__ == "__main__":
    from duckdb import connect

    con: DuckDBPyConnection = connect(database=":memory:")

    example_fields = ["symbol"]
    example_table = "all_tickers"
    macro_name = "ohlc_macro"
    result = con.sql(
        f"SELECT * FROM {macro_name}({example_table}, {','.join(example_fields)})"
    )
    print(result.df())
