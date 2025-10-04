import time
import json
import trio
import polars as pl
import pyarrow as pa

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection
from duckdb.functional import FunctionNullHandling, PythonUDFType
from duckdb.typing import VARCHAR, DuckDBPyType
from functools import partial
from string import Template
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterable

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
from inout import cache
from metadata import (
    create_macro_definition,
    get_batch_id_from_table_metadata,
    get_lookup_tables,
    get_macro_definition_by_name,
    update_batch_id_in_table_metadata,
    get_batch_id_from_view_metadata,
    update_batch_id_in_view_metadata,
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

DUCKDB_TO_POLARS: dict[str, Any] = {
    "VARCHAR": pl.Utf8,
    "TEXT": pl.Utf8,
    "TIMESTAMP": pl.Datetime,
    "TIMESTAMP_MS": pl.Datetime("ms"),
    "TIMESTAMP_NS": pl.Datetime("ns"),
    "DATETIME": pl.Datetime,
    "FLOAT": pl.Float32,
    "DOUBLE": pl.Float64,
    "INTEGER": pl.Int32,
    "INT": pl.Int32,
    "BIGINT": pl.Int64,
    "SMALLINT": pl.Int16,
    "TINYINT": pl.Int8,
    "BOOLEAN": pl.Boolean,
    "DATE": pl.Date,
    "DECIMAL": pl.Float64,
}


def build_lookup_properties(
    properties: dict[str, str], context: dict[str, Any]
) -> dict[str, str]:
    new_props = {}

    for key, value in properties.items():
        if key in ["method"]:  # TODO: ignore jq for now
            new_props[key] = value
            continue
        new_props[key] = Template(value).substitute(context)

    return new_props


def build_scalar_udf(
    properties: dict[str, str],
    dynamic_columns: list[str],
    return_type: DuckDBPyType,
    conn: DuckDBPyConnection,
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
                response = requester(conn)
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


def records_to_polars(
    records: list[dict[str, Any]], column_types: dict[str, str]
) -> pl.DataFrame:
    df = pl.DataFrame(records)
    # Cast each column to the right dtype
    for col, duck_type in column_types.items():
        dtype = DUCKDB_TO_POLARS.get(duck_type, pl.Utf8)
        df = df.with_columns(pl.col(col).cast(dtype))
    return df


async def http_source_executable(
    task_id: str,
    conn: DuckDBPyConnection,
    table_name: str,
    start_time: datetime,
    column_types: dict[str, str],
    http_requester: Callable,
    *args,
    **kwargs,
) -> pl.DataFrame:
    # TODO: no provided api execution_time
    # using trigger.get_next_fire_time is costly (see code)
    execution_time = start_time
    batch_id = get_batch_id_from_table_metadata(conn, table_name)
    logger.info(f"[{table_name}{{{batch_id}}}] Starting @ {execution_time}")

    records = await http_requester(conn)
    logger.debug(
        f"[{table_name}{{{batch_id}}}] - http number of responses: {len(records)} - batch {batch_id}"
    )

    if len(records) > 0:
        epoch = int(time.time() * 1_000)
        df = records_to_polars(records, column_types)
        await cache(df, batch_id, epoch, table_name, conn, False)
    else:
        df = pl.DataFrame()

    update_batch_id_in_table_metadata(conn, table_name, batch_id + 1)
    return df


def build_http_source_executable(
    ctx: CreateHTTPTableContext,
) -> Callable[[str, DuckDBPyConnection], Coroutine[Any, Any, pl.DataFrame]]:
    start_time = datetime.now(timezone.utc)
    properties = ctx.properties
    return partial(
        http_source_executable,
        table_name=ctx.name,
        start_time=start_time,
        column_types=ctx.column_types,
        http_requester=build_http_requester(properties, is_async=True),
    )


async def ws_source_executable(
    task_id: str,
    conn: DuckDBPyConnection,
    table_name: str,
    column_types: dict[str, str],
    ws_generator_func: Callable[
        [trio.Nursery], AsyncGenerator[Any, list[dict[str, Any]]]
    ],
    nursery: trio.Nursery,
    *args,
    **kwargs,
) -> AsyncGenerator[Any, pl.DataFrame]:
    logger.info(f"[{task_id}] - starting ws executable")
    while True:
        async for records in ws_generator_func(nursery):
            if len(records) > 0:
                epoch = int(time.time() * 1_000)
                df = records_to_polars(records, column_types)
                await cache(df, 0, epoch, table_name, conn, False)
            else:
                df = pl.DataFrame()

            yield df


def build_ws_source_executable(
    ctx: CreateWSTableContext, conn: DuckDBPyConnection
) -> Callable[
    [str, DuckDBPyConnection, trio.Nursery], AsyncGenerator[Any, pl.DataFrame]
]:
    properties = ctx.properties
    table_name = ctx.name
    on_start_results = []

    if ctx.on_start_query != "":
        # If we go till there, we already evaled on_start_query
        # consider passing it from App to TaskManager for some
        # more efficiency
        on_start_results = duckdb_to_dicts(conn, ctx.on_start_query)

    return partial(
        ws_source_executable,
        table_name=table_name,
        column_types=ctx.column_types,
        ws_generator_func=build_ws_generator(properties, on_start_results),
    )


def build_scheduled_source_executable(
    ctx: ScheduledTaskContext,
) -> Callable[[str, DuckDBPyConnection], Coroutine[Any, Any, pl.DataFrame]]:
    if isinstance(ctx, CreateHTTPTableContext):
        return build_http_source_executable(ctx)

    else:
        raise NotImplementedError()


def build_continuous_source_executable(
    ctx: ContinousTaskContext, conn: DuckDBPyConnection
) -> Callable[
    [str, DuckDBPyConnection, trio.Nursery], AsyncGenerator[Any, pl.DataFrame]
]:
    if isinstance(ctx, CreateWSTableContext):
        return build_ws_source_executable(ctx, conn)

    else:
        raise NotImplementedError()


def build_lookup_table_prehook(
    create_table_context: CreateHTTPLookupTableContext, conn: DuckDBPyConnection
) -> str:
    properties = create_table_context.properties
    table_name = create_table_context.name
    dynamic_columns = create_table_context.dynamic_columns
    columns = create_table_context.columns

    func_name = f"{table_name}_func"
    macro_name = f"{table_name}_macro"

    # TODO: handle other return than dict (for instance array http responses)
    return_type = struct_type(columns)  # type: ignore
    udf_params = build_scalar_udf(properties, dynamic_columns, return_type, conn)
    # register scalar for row to row http call
    conn.create_function(**udf_params)
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
    conn.sql(f"""
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
    create_macro_definition(conn, macro_name, dynamic_columns)

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


def duckdb_to_pl(conn: DuckDBPyConnection, duckdb_sql: str) -> pl.DataFrame:
    """
    Execute duckdb-compatible query and return as a typed polar dataframe.
    """
    query = duckdb_sql.strip()
    cursor = conn.execute(query)
    if cursor.description:
        return cursor.pl()

    return pl.DataFrame()


def duckdb_to_dicts(conn: DuckDBPyConnection, duckdb_sql: str) -> list[Any]:
    """
    Execute duckdb-compatible query and return as a list of tuple.
    """
    query = duckdb_sql.strip()
    rel = conn.sql(query)

    # rel can be None and description can be empty list
    if rel and rel.description:
        columns = rel.columns
        rows = rel.fetchall()
        list_of_dicts = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            list_of_dicts.append(row_dict)
        return list_of_dicts
    return []


def build_sink_executable(
    ctx: SinkTaskContext,
) -> Callable[[str, DuckDBPyConnection, pl.DataFrame], Coroutine[Any, Any, None]]:
    properties = ctx.properties
    producer = Producer({"bootstrap.servers": properties["server"]})
    topic = properties["topic"]
    return partial(
        kafka_sink,
        first_upstream=ctx.upstreams[0],
        transform_query=ctx.subquery,
        pl_ctx=pl.SQLContext(register_globals=False, eager=True),
        producer=producer,
        topic=topic,
    )


async def kafka_sink(
    task_id: str,
    conn: DuckDBPyConnection,
    df: pl.DataFrame,
    first_upstream: str,
    transform_query: str,
    pl_ctx: pl.SQLContext,
    *,
    producer: Producer,
    topic: str,
) -> None:
    pl_ctx.register(first_upstream, df)
    transform_df = pl_ctx.execute(transform_query)
    records = transform_df.to_dicts()

    def _produce_all():
        for record in records:
            payload = json.dumps(record).encode("utf-8")
            producer.produce(topic, value=payload)
            producer.poll(0)
        producer.flush()

    await trio.to_thread.run_sync(_produce_all)


def build_transform_executable(
    ctx: TransformTaskContext, is_materialized: bool
) -> Callable[
    [str, DuckDBPyConnection, pl.DataFrame], Coroutine[Any, Any, pl.DataFrame]
]:
    return partial(
        transform_executable,
        name=ctx.name,
        first_upstream=ctx.upstreams[0],  # TODO add joins
        transform_query=ctx.subquery,
        is_materialized=is_materialized,
        pl_ctx=pl.SQLContext(register_globals=False, eager=True),
    )


async def transform_executable(
    task_id: str,
    conn: DuckDBPyConnection,
    df: pl.DataFrame,
    name: str,
    first_upstream: str,
    transform_query: str,
    is_materialized: bool,
    pl_ctx: pl.SQLContext,
) -> pl.DataFrame:
    pl_ctx.register(first_upstream, df)
    transform_df = pl_ctx.execute(transform_query)

    batch_id = get_batch_id_from_view_metadata(conn, name, is_materialized)
    epoch = int(time.time() * 1_000)
    # invert is_materialized for truncate
    # if is_materialized -> truncate = False -> append mode
    # if not is_materialized -> truncate = True -> truncate mode (overwrite)
    await cache(transform_df, batch_id, epoch, name, conn, not is_materialized)
    update_batch_id_in_view_metadata(conn, name, batch_id + 1, is_materialized)

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
