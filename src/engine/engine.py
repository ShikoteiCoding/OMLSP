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
    SelectContext,
    ScheduledTaskContext,
    ContinousTaskContext,
    TransformTaskContext,
    SinkTaskContext,
)
from inout import cache
from metadata import (
    create_macro_definition,
    get_batch_id_from_source_metadata,
    get_batch_id_from_table_metadata,
    get_lookup_tables,
    get_tables,
    get_macro_definition_by_name,
    update_batch_id_in_table_metadata,
    lazy_sync_macros,
)
from transport import build_http_requester, build_ws_generator
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
    # match duckdb, default is microseconds
    "TIMESTAMP": pl.Datetime("us"),
    "TIMESTAMP_MS": pl.Datetime("ms"),
    "TIMESTAMP_US": pl.Datetime("us"),
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

EXEC_CONN_LOADED_MACROS: set[str] = set()


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


def _coerce_result_dict(
    d: dict[str, Any], field_types: dict[str, pa.DataType]
) -> dict[str, Any]:
    """
    Convert the values in ``d`` so that they match the Arrow ``schema``.

    Code is AI generated. This is because it's a temporary solution to use
    duckdb for lookup logic.

    TODO: move towards type mapping and pre compilation as we know the
    python type from the jq, we can directly build response to result
    converter. If it is pre compiled we could also detect "if we need"
    coercion, making overall performance greatly improved.
    """
    out = {}

    # Loop through actual keys in the dict (sparse-friendly)
    for name, raw in d.items():
        # Skip if this field isn't in the schema
        if name not in field_types:
            logger.warning(
                "Field `{}` in dict isn't in provided schema: `{}`", name, field_types
            )
            continue

        typ = field_types[name]

        if pa.types.is_int64(typ):
            if isinstance(raw, (int)):
                out[name] = int(raw)
            elif isinstance(raw, str):
                out[name] = int(raw.strip())
            else:
                raise TypeError(f"Field '{name}' expected int64, got {type(raw)}")

        elif pa.types.is_float64(typ) or pa.types.is_float32(typ):
            if isinstance(raw, (float, int)):
                out[name] = float(raw)
            elif isinstance(raw, str):
                out[name] = float(raw.strip())
            else:
                raise TypeError(f"Field '{name}' expected float64, got {type(raw)}")

        # TODO: improve timestamp coercion to be stop being unit sensitive
        elif pa.types.is_timestamp(typ):
            if isinstance(raw, datetime):
                out[name] = raw
            elif isinstance(raw, (int)):
                out[name] = datetime.fromtimestamp(float(raw), timezone.utc)
            elif isinstance(raw, str):
                try:
                    out[name] = datetime.fromisoformat(raw)
                except ValueError:
                    out[name] = datetime.fromtimestamp(float(raw.strip()), timezone.utc)
            else:
                raise TypeError(f"Field '{name}' expected timestamp, got {type(raw)}")

        elif pa.types.is_string(typ):
            out[name] = str(raw)

        else:
            raise NotImplementedError(
                f"Coercion for Arrow type {typ} (field '{name}') not implemented"
            )

    return out


def build_scalar_udf(
    properties: dict[str, str],
    dynamic_columns: list[str],
    return_type: DuckDBPyType,
    conn: DuckDBPyConnection,
    name: str,
) -> dict[str, Any]:
    # Number of parameters (input) of the scalar func
    arity = len(dynamic_columns)

    # Types of parameters (input) of the scalar func
    # they are hardcoded VARCHAR because we are dealing
    # only with property values as string (for now ?)
    parameters = [VARCHAR for _ in range(arity)]

    child_types = []
    for subtype in return_type.children:
        child_types.append(
            pa.field(subtype[0], DUCKDB_TO_PYARROW_PYTYPE[str(subtype[1])])
        )

    # Return type of the scalar func as pyarrow (internal)
    return_type_arrow: pa.Schema = pa.struct(child_types)
    field_types = {field.name: field.type for field in return_type_arrow}

    # Create named parameters for the udf, using arity as identifier
    # Dyckdb pyarrow udf does't accept args or kwargs
    arg_names = [f"a{i}" for i in range(1, arity + 1)]

    # Threadpool http wrapper which process elements
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
                logger.exception("HTTP request failed: {}", e)

            coerced_response = _coerce_result_dict(default_response, field_types)
            return coerced_response

        with ThreadPoolExecutor() as executor:
            results = list(executor.map(_inner, rows))

        return results

    def core_udf(*arrays: pa.ChunkedArray) -> pa.Array:
        coerced_results = []
        for chunks in zip(*(arr.chunks for arr in arrays)):
            py_chunks = [chunk.to_pylist() for chunk in chunks]
            chunk_rows = zip(*py_chunks)
            chunk_results = process_elements(chunk_rows, properties)
            coerced_results.extend(chunk_results)

        return pa.array(coerced_results, type=return_type_arrow)

    udf = eval(
        f"lambda {', '.join(arg_names)}: core_udf({', '.join(arg_names)})",
        {"core_udf": core_udf},
    )

    return {
        "name": name,
        "function": udf,
        "parameters": parameters,
        "return_type": return_type,
        "type": PythonUDFType.ARROW,
        "null_handling": FunctionNullHandling.SPECIAL,
    }


def records_to_polars(
    records: list[dict[str, Any]],
    output_dtypes: dict[str, str],
    dynamic_columns: dict[str, Callable[[dict[str, Any]], pl.Expr]],
    func_context: dict[str, Any],
) -> pl.DataFrame:
    df = pl.DataFrame(records)
    static_columns_operations = []
    dynamic_columns_operations = []

    # Handle each expected output columns with dtype
    for output_col, dtype in output_dtypes.items():
        target_dtype = DUCKDB_TO_POLARS.get(dtype, pl.Utf8)

        # Generated columns are generated here
        if output_col in dynamic_columns:
            dynamic_columns_operations.append(
                dynamic_columns[output_col](func_context)
                .cast(target_dtype)
                .alias(output_col)
            )
        # Static columns simply get a cast to match output df return types
        else:
            static_columns_operations.append(pl.col(output_col).cast(target_dtype))

    return df.with_columns(
        # Start casting static columns
        *static_columns_operations
    ).with_columns(
        # Apply dynamic columns in different step
        # polars.exceptions.InvalidOperationError: sub operation not supported
        # Issue: https://github.com/pola-rs/polars/issues/9062
        *dynamic_columns_operations
    )


async def http_source_executable(
    task_id: str,
    conn: DuckDBPyConnection,
    name: str,
    column_types: dict[str, str],
    dynamic_columns: dict[str, Callable],
    is_source: bool,
    http_requester: Callable,
) -> pl.DataFrame:
    # Create generated column context (to be applied upon at exec time)
    # Some SQL functions in generated columns might use or not use this context
    # This context is dynamic by essence i.e is created at exec time
    func_context = {
        # Start time is a context for TRIGGER_TIME() and TRIGGER_TIME_EPOCH() functions
        "trigger_time": datetime.now(timezone.utc).replace(microsecond=0)
    }

    # This is slow and kept synchrone for now but making it async
    # Through a metadata manager would risk concurrency conflicts
    if is_source:
        batch_id = get_batch_id_from_source_metadata(conn, name)
    else:
        batch_id = get_batch_id_from_table_metadata(conn, name)

    logger.info(
        "[{}{{{}}}] Starting @ {}", task_id, batch_id, func_context["trigger_time"]
    )

    records = await http_requester(conn)
    logger.debug(
        "[{}{{{}}}] - http number of responses: {} - batch {}",
        task_id,
        batch_id,
        len(records),
        batch_id,
    )

    if len(records) > 0:
        epoch = int(time.time() * 1_000)
        df = records_to_polars(records, column_types, dynamic_columns, func_context)
        await cache(df, batch_id, epoch, name, conn, is_source)
    else:
        df = pl.DataFrame()

    # This is slow and kept synchrone for now but making it async
    # Through a metadata manager would risk concurrency conflicts
    if is_source:
        update_batch_id_in_table_metadata(conn, name, batch_id + 1)
    else:
        update_batch_id_in_table_metadata(conn, name, batch_id + 1)
    return df


async def ws_source_executable(
    task_id: str,
    conn: DuckDBPyConnection,
    table_name: str,
    column_types: dict[str, str],
    is_source: bool,
    ws_generator_func: Callable[
        [trio.Nursery], AsyncGenerator[list[dict[str, Any]], None]
    ],
    nursery: trio.Nursery,
    *args,
    **kwargs,
) -> AsyncGenerator[pl.DataFrame, None]:
    logger.info("[{}] - starting ws executable", task_id)

    # 'ws_generator_func' is supposed to be never ending (while True)
    # if an issue happens, the source task should be entirely
    # restarted (not a feature yet)
    async for records in ws_generator_func(nursery):
        if len(records) > 0:
            df = records_to_polars(records, column_types, {}, {})
            # Do not truncate the cache, this is a Table
            await cache(
                df, 0, int(time.time() * 1_000), table_name, conn, is_source
            )
        else:
            # This should not happen, just in case
            df = pl.DataFrame()

        yield df


def build_scheduled_source_executable(
    ctx: ScheduledTaskContext,
) -> Callable[[str, DuckDBPyConnection], Coroutine[Any, Any, pl.DataFrame]]:
    return partial(
        http_source_executable,
        name=ctx.name,
        column_types=ctx.column_types,
        dynamic_columns=ctx.generated_columns,
        is_source=ctx.source,
        http_requester=build_http_requester(ctx.properties, is_async=True),
    )


def build_continuous_source_executable(
    ctx: ContinousTaskContext, conn: DuckDBPyConnection
) -> Callable[
    [str, DuckDBPyConnection, trio.Nursery], AsyncGenerator[pl.DataFrame, None]
]:
    properties = ctx.properties
    table_name = ctx.name
    on_start_results = []

    if ctx.on_start_query != "":
        # If we come all the way here, we already evaled on_start_query
        # consider passing it from App to TaskManager for improved design
        on_start_results = duckdb_to_dicts(conn, ctx.on_start_query)

    return partial(
        ws_source_executable,
        table_name=table_name,
        column_types=ctx.column_types,
        is_source=ctx.source,
        ws_generator_func=build_ws_generator(properties, on_start_results),
    )


def build_lookup_table_prehook(
    create_table_context: CreateHTTPLookupTableContext,
    conn: DuckDBPyConnection,
) -> str:
    properties = create_table_context.properties
    table_name = create_table_context.name
    dynamic_columns = create_table_context.dynamic_columns
    columns = create_table_context.columns

    func_name = f"{table_name}_func"
    macro_name = f"{table_name}_macro"

    # TODO: handle other return than dict (for instance array http responses)
    return_type = struct_type(columns)  # type: ignore
    udf_params = build_scalar_udf(
        properties, dynamic_columns, return_type, conn, func_name
    )
    # register scalar for row to row http call
    conn.create_function(**udf_params)
    logger.debug("registered function: {}", func_name)

    # TODO: wrap SQL in function
    # register macro (to be injected in place of sql)
    __inner_tbl = "__inner_tbl"
    __deriv_tbl = "__deriv_tbl"

    # This inner macro function is used to substitute create properties
    # They are fine to be all casted to VARCHAR by default as we should
    # not need other types (all properties are string values)
    func_def = f"{func_name}({', '.join([f'CAST({__inner_tbl}.{col} AS VARCHAR)' for col in dynamic_columns])})"
    macro_def = f"{macro_name}(table_name, {', '.join(dynamic_columns)})"
    output_cols = ", ".join(
        [
            f"CAST(struct.{col_name} AS {duckdb_dtype}) AS {col_name}"
            for col_name, duckdb_dtype in columns.items()
        ]
    )

    # TODO: move to metadata func
    create_macro_sql = f"""
        CREATE OR REPLACE MACRO {macro_def} AS TABLE
        SELECT
            {output_cols}
        FROM (
            SELECT
                {func_def} AS struct
            FROM query_table(table_name) AS {__inner_tbl}
        ) AS {__deriv_tbl};
    """
    conn.execute(create_macro_sql)
    logger.debug(
        "registered macro: {} with definition: {}", macro_name, create_macro_sql
    )
    create_macro_definition(
        conn, macro_name, dynamic_columns, create_macro_sql
    )

    return macro_name


def get_substitute_macro_definition(
    conn: DuckDBPyConnection,
    join_table: str,
    from_table: str,
    from_table_or_alias: str,
    join_table_or_alias: str,
) -> str:
    # Lookup tables should go through a macro definition.
    #
    # This function returns macro definition which can be used in SQL statement.
    #
    # Example:
    #     - table: ohlc
    #     - returns: ohlc_macro("all_tickers", ALT.field1, ALT.field2)

    macro_name, fields = get_macro_definition_by_name(conn, f"{join_table}_macro")
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


def substitute_sql_template(
    conn: DuckDBPyConnection,
    ctx: SelectContext,
    substitute_mapping: dict[str, str],
) -> str:
    """
    Substitutes select statement query with lookup references to macro references.

    TODO: should it be moved somewhere better ? This function is using ctx.
    Too much coupling.
    """
    original_query = ctx.query
    join_tables = ctx.joins
    lookup_tables = get_lookup_tables(conn)

    # join query
    from_table = ctx.table
    from_table_or_alias = ctx.alias

    # for table in join and in lookup
    # build the substitute mapping
    for join_table, join_table_or_alias in join_tables.items():
        if join_table in lookup_tables:
            substitute_mapping[join_table] = get_substitute_macro_definition(
                conn,
                join_table,
                from_table,
                from_table_or_alias,
                join_table_or_alias,
            )
        else:
            substitute_mapping[join_table] = join_table

    # Substritute lookup table placeholder with template
    query = Template(original_query).substitute(substitute_mapping)

    logger.debug("New overwritten select statement: {}", query)
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
    ctx: SinkTaskContext, backend_conn: DuckDBPyConnection
) -> Callable[[str, DuckDBPyConnection, pl.DataFrame], Coroutine[Any, Any, None]]:
    properties = ctx.properties
    producer = Producer({"bootstrap.servers": properties["server"]})
    topic = properties["topic"]
    return partial(
        kafka_sink,
        backend_conn=backend_conn,
        first_upstream=ctx.upstreams[0],
        transform_query=ctx.subquery,
        pl_ctx=pl.SQLContext(register_globals=False, eager=True),
        producer=producer,
        topic=topic,
    )


async def kafka_sink(
    task_id: str,
    transform_conn: DuckDBPyConnection,
    df: pl.DataFrame,
    backend_conn: DuckDBPyConnection,
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
    ctx: TransformTaskContext, backend_conn: DuckDBPyConnection
) -> Callable[
    [str, DuckDBPyConnection, pl.DataFrame], Coroutine[Any, Any, pl.DataFrame]
]:
    from_table = ctx.upstreams[0]
    duckdb_tables = get_tables(backend_conn)
    substitute_mapping = {}

    for duckdb_table in duckdb_tables:
        val = duckdb_table if duckdb_table != from_table else "df"
        substitute_mapping[duckdb_table] = val

    transform_sql = substitute_sql_template(
        backend_conn, ctx.transform_ctx, substitute_mapping
    )

    return partial(
        transform_executable,
        backend_conn=backend_conn,
        name=ctx.name,
        first_upstream=from_table,  # TODO add joins
        transform_query=transform_sql,
        is_materialized=ctx.materialized,
        # force eager to enforce read over write consistency
        pl_ctx=pl.SQLContext(register_globals=False, eager=True),
    )


async def transform_executable(
    task_id: str,
    transform_conn: DuckDBPyConnection,
    df: pl.DataFrame,
    backend_conn: DuckDBPyConnection,
    name: str,
    first_upstream: str,
    transform_query: str,
    is_materialized: bool,
    pl_ctx: pl.SQLContext,
) -> pl.DataFrame:
    # TODO: migrate away from duckdb
    # pl_ctx.register(first_upstream, df)
    # transform_df = pl_ctx.execute(transform_query)
    logger.info("[{}] Starting @ {}", task_id, datetime.now(timezone.utc))
    # sync macro
    lazy_sync_macros(backend_conn, transform_conn, transform_query, EXEC_CONN_LOADED_MACROS)

    # explicitely mock df registration of incoming df
    # in case of lookup, this should also work when the
    # registed df is called inside / through a macro !
    # don't use conn.register() as duckdb only support
    # global registration and would make it not thread-safe anymore
    transform_query = transform_query.replace(f'"{first_upstream}"', "df")
    transform_df = transform_conn.execute(transform_query).pl()

    epoch = int(time.time() * 1_000)
    # invert is_materialized for truncate
    # if is_materialized -> truncate = False -> append mode
    # if not is_materialized -> truncate = True -> truncate mode (overwrite)
    await cache(transform_df, -1, epoch, name, backend_conn, not is_materialized)
    # update_batch_id_in_view_metadata(conn, name, batch_id + 1, is_materialized)

    return transform_df


if __name__ == "__main__":
    import jq as jqm
    from duckdb import connect
    from transport import ws_generator

    conn: DuckDBPyConnection = connect(database=":memory:")

    async def main():
        async with trio.open_nursery() as nursery:
            properties = {
                "url": "wss://fstream.binance.com/ws/!ticker@arr",
                "jq": jqm.compile(""".[:2][] | {
                    event_type: .e,
                    event_time: .E,
                    symbol: .s,
                    close: .c,
                    open: .o,
                    high: .h,
                    low: .l,
                    base_volume: .v,
                    quote_volume: .q
                 }"""),
            }

            async for msg in ws_source_executable(
                "0", conn, "abcd", {}, False, partial(ws_generator, properties), nursery
            ):
                logger.info(msg)

    trio.run(main)
