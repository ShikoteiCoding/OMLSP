import numpy as np
import polars as pl
import pyarrow as pa
import time
import trio

from confluent_kafka import Producer
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection
from functools import partial
from string import Template
from typing import Any, AsyncGenerator, Awaitable, Callable, Iterable, Type

from duckdb import struct_type
from loguru import logger


from context.context import (
    CommandContext,
    CreateHTTPTableContext,
    CreateWSTableContext,
    CreateHTTPLookupTableContext,
    CreateSecretContext,
    CreateSinkContext,
    CreateHTTPSourceContext,
    CreateWSSourceContext,
    CreateViewContext,
    EvaluableContext,
    SelectContext,
    SetContext,
    ShowContext,
)
from inout import cache
from store import (
    get_batch_id_from_source_metadata,
    get_batch_id_from_table_metadata,
    get_duckdb_tables,
    get_lookup_tables,
    update_batch_id_in_table_metadata,
    create_secret,
    create_sink,
    create_source,
    create_table,
    create_view,
)
from store.lookup import callback_store
from sql.types import SourceHttpProperties
from transport.builder import TransportBuilder

from serializer.serializer import SERIALIZER_DISPATCH, JsonSerializer, BaseSerializer

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


def substitute_http_properties(
    properties: SourceHttpProperties, context: dict[str, Any]
) -> SourceHttpProperties:
    return SourceHttpProperties(
        url=Template(properties.url).substitute(context),
        method=properties.method,
        jq=properties.jq,
        signer_class=properties.signer_class,
        pagination=properties.pagination,
        headers=properties.headers,
        json=properties.json,
        params=properties.params,
        secrets=properties.secrets,
    )


def _coerce_result_dict(
    d: dict[str, Any],
    field_types: dict[str, pa.DataType],
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

    # fast local helpers
    np_int = (np.integer,)
    np_float = (np.floating,)
    np_str = (np.str_,)
    np_datetime = (np.datetime64,)

    for name, raw in d.items():
        if name not in field_types:
            logger.warning(
                "Field `%s` in dict isn't in provided schema: `%s`",
                name,
                field_types,
            )
            continue

        typ = field_types[name]

        if pa.types.is_int64(typ):
            if isinstance(raw, (int,)) or isinstance(raw, np_int):
                out[name] = int(raw)
            elif isinstance(raw, str):
                out[name] = int(raw.strip())
            elif isinstance(raw, np_str):
                out[name] = int(raw.item().strip())
            else:
                raise TypeError(f"Field '{name}' expected int64, got {type(raw)}")

        elif pa.types.is_float64(typ) or pa.types.is_float32(typ):
            if (
                isinstance(raw, (float, int))
                or isinstance(raw, np_float)
                or isinstance(raw, np_int)
            ):
                out[name] = float(raw)
            elif isinstance(raw, str):
                out[name] = float(raw.strip())
            elif isinstance(raw, np_str):
                out[name] = float(raw.item().strip())
            else:
                raise TypeError(f"Field '{name}' expected float, got {type(raw)}")

        elif pa.types.is_timestamp(typ):
            # python datetime → pass-through
            if isinstance(raw, datetime):
                out[name] = raw

            # numpy datetime64 (nano or ms or s)
            elif isinstance(raw, np_datetime):
                # Convert to python datetime in UTC
                ts = raw.astype("datetime64[ns]").astype("int64") / 1e9
                out[name] = datetime.fromtimestamp(ts, timezone.utc)

            # python int or numpy int → timestamp
            elif isinstance(raw, (int, np_int)):
                out[name] = datetime.fromtimestamp(float(raw), timezone.utc)

            # string timestamp
            elif isinstance(raw, str) or isinstance(raw, np_str):
                s = raw if isinstance(raw, str) else raw.item()
                try:
                    out[name] = datetime.fromisoformat(s)
                except ValueError:
                    out[name] = datetime.fromtimestamp(float(s.strip()), timezone.utc)

            else:
                raise TypeError(f"Field '{name}' expected timestamp, got {type(raw)}")

        elif pa.types.is_string(typ):
            if isinstance(raw, np_str):
                out[name] = raw.item()
            else:
                out[name] = str(raw)

        else:
            raise NotImplementedError(
                f"Coercion for Arrow type {typ} (field '{name}') not implemented"
            )

    return out


def records_to_polars(
    records: list[dict[str, Any]],
    output_dtypes: dict[str, str],
    generated_columns: dict[str, Callable[[dict[str, Any]], pl.Expr]],
    func_context: dict[str, Any],
) -> pl.DataFrame:
    df = pl.DataFrame(records)
    static_columns_operations = []
    dynamic_columns_operations = []

    # Handle each expected output columns with dtype
    for output_col, dtype in output_dtypes.items():
        target_dtype = DUCKDB_TO_POLARS.get(dtype, pl.Utf8)

        # Generated columns are generated here
        if output_col in generated_columns:
            dynamic_columns_operations.append(
                generated_columns[output_col](func_context)
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
    generated_columns: dict[str, Callable],
    is_source: bool,
    http_requester_func: Callable[[DuckDBPyConnection], Awaitable[list[dict]]],
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

    records = await http_requester_func(conn)
    logger.debug(
        "[{}{{{}}}] - http number of responses: {} - batch {}",
        task_id,
        batch_id,
        len(records),
        batch_id,
    )

    if len(records) > 0:
        epoch = int(time.time() * 1_000)
        df = records_to_polars(records, column_types, generated_columns, func_context)
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
    nursery: trio.Nursery,
    table_name: str,
    column_types: dict[str, str],
    is_source: bool,
    ws_generator_func: Callable[
        [trio.Nursery, trio.Event], AsyncGenerator[list[dict[str, Any]], None]
    ],
    cancel_event: trio.Event,
    generated_columns: dict[str, Callable],
    *args,
    **kwargs,
) -> AsyncGenerator[pl.DataFrame, None]:
    logger.info("[{}] - starting ws executable", task_id)

    # 'ws_generator_func' is supposed to be never ending (while True)
    # if an issue happens, the source task should be entirely
    # restarted (not a feature yet)
    try:
        async for records in ws_generator_func(nursery, cancel_event):
            if len(records) > 0:
                df = records_to_polars(records, column_types, generated_columns, {})
                # Do not truncate the cache, this is a Table
                await cache(
                    df, 0, int(time.time() * 1_000), table_name, conn, is_source
                )
            else:
                # This should not happen, just in case
                df = pl.DataFrame()

            yield df
    except Exception as e:
        # Custom capture as async generator escapes SC control
        # https://github.com/python-trio/trio/issues/265
        logger.error(e)
        raise e


def build_scheduled_source_executable(
    ctx: CreateHTTPSourceContext | CreateHTTPTableContext,
) -> Callable[[str, DuckDBPyConnection], Awaitable[pl.DataFrame]]:
    requester = (
        TransportBuilder(ctx.properties)
        .option("mode", "async")
        .build("http")
        .configure()
        .finalize()
    )

    return partial(
        http_source_executable,
        name=ctx.name,
        column_types=ctx.column_types,
        generated_columns=ctx.generated_columns,
        is_source=ctx.source,
        http_requester_func=requester,
    )


def build_continuous_source_executable(
    ctx: CreateWSSourceContext | CreateWSTableContext, conn: DuckDBPyConnection
) -> Callable[
    [str, DuckDBPyConnection, trio.Nursery, trio.Event],
    AsyncGenerator[pl.DataFrame, None],
]:
    properties = ctx.properties
    table_name = ctx.name
    generated_columns = ctx.generated_columns
    on_start_results = []

    if ctx.on_start_query:
        # If we come all the way here, we already evaled on_start_query
        # consider passing it from App to TaskManager for improved design
        on_start_results = duckdb_to_dicts(conn, ctx.on_start_query)

    ws_generator = (
        TransportBuilder(properties)
        .option("templates_list", on_start_results)
        .build("ws")
        .configure()
        .finalize()
    )

    return partial(
        ws_source_executable,
        table_name=table_name,
        column_types=ctx.column_types,
        is_source=ctx.source,
        ws_generator_func=ws_generator,
        generated_columns=generated_columns,
    )


# NOTE: Deprecate pyarrow schema for direct SQL to polars typing
def build_lookup_functions(
    properties: SourceHttpProperties,
    lookup_fields: list[str],
    column_dtypes: dict[str, str],
    backend_conn: DuckDBPyConnection,
    generated_columns: dict[str, Callable],
) -> Callable[[pl.DataFrame], Awaitable[pl.DataFrame]]:
    # Create pyarrow type from sql DDL
    # {"symbol": "TEXT"} -> {"symbol": pa.string()}
    return_type = struct_type(column_dtypes)  # type: ignore
    child_types = []
    for subtype in return_type.children:
        child_types.append(
            pa.field(subtype[0], DUCKDB_TO_PYARROW_PYTYPE[str(subtype[1])])
        )
    pa_field_types = {field.name: field.type for field in pa.struct(child_types)}

    async def process_elements(
        rows: Iterable[tuple[Any, ...]],
        properties: SourceHttpProperties,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []

        async def _process_one(row: tuple[Any, ...]):
            context = dict(zip(lookup_fields, row))
            lookup_properties = substitute_http_properties(properties, context)
            default_response = context.copy()
            result = []

            requester = (
                TransportBuilder(lookup_properties)
                .option("mode", "async")
                .build("http")
                .configure()
                .finalize()
            )

            try:
                # requester is an awaitable
                response = await requester(backend_conn)

                # single dict reply means only 1 record
                if isinstance(response, dict):
                    result = [default_response | response]
                # list assumes multiple records
                elif isinstance(response, list) and len(response) >= 1:
                    for row in response:
                        default_response |= row
                        result.append(
                            _coerce_result_dict(default_response, pa_field_types)
                        )
                # assume empty list or None. cannot extract anything
                # from response, just create an "empty" dict
                else:
                    coerced = _coerce_result_dict(default_response, pa_field_types)
                    result.append(
                        coerced
                        | {
                            k: None
                            for k in column_dtypes
                            if k not in set(coerced.keys())
                        }
                    )

            except Exception as e:
                logger.exception("HTTP request failed: {}", e)
                result.append(_coerce_result_dict(default_response, pa_field_types))

            results.extend(result)

        async with trio.open_nursery() as nursery:
            for row in rows:
                nursery.start_soon(_process_one, row)

        return results

    async def core_udf(df: pl.DataFrame) -> pl.DataFrame:
        np_cols = [df[col].to_numpy() for col in lookup_fields]
        row_iter = zip(*np_cols)
        results = await process_elements(row_iter, properties)
        return records_to_polars(results, column_dtypes, generated_columns, {})

    return core_udf


def build_lookup_callback(
    create_table_context: CreateHTTPLookupTableContext,
    backend_conn: DuckDBPyConnection,
) -> tuple[str, Callable[[pl.DataFrame], Awaitable[pl.DataFrame]]]:
    properties = create_table_context.properties
    table_name = create_table_context.name
    lookup_fields = create_table_context.lookup_fields
    column_types = create_table_context.column_types
    generated_columns = create_table_context.generated_columns

    return (
        table_name,
        build_lookup_functions(
            properties, lookup_fields, column_types, backend_conn, generated_columns
        ),
    )


# NOTE: To be deprecated once confirmed that we do not need
# parsing-level substitution of table names
# This was created to handle dynamic sql alteration
# to transform lookup into macro
def substitute_sql_template(
    ctx: SelectContext,
    substitute_mapping: dict[str, str],
) -> str:
    """
    Substitutes select statement query with lookup references to macro references.
    """
    original_query = ctx.query
    join_tables = ctx.joins

    # for table in join and in lookup
    # build the substitute mapping
    for join_table_name, _ in join_tables.items():
        substitute_mapping[join_table_name] = join_table_name

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
    ctx: CreateSinkContext, backend_conn: DuckDBPyConnection
) -> Callable[[str, DuckDBPyConnection, pl.DataFrame], Awaitable[None]]:
    properties = ctx.properties
    from_table = ctx.upstreams[0]
    producer = Producer({"bootstrap.servers": properties.server})
    topic = properties.topic
    serializer = SERIALIZER_DISPATCH.get(type(properties.encode), JsonSerializer).init(
        properties.encode, topic
    )
    return partial(
        kafka_sink,
        from_table=from_table,
        transform_query=ctx.transform_ctx.query,
        pl_ctx=pl.SQLContext(register_globals=False, eager=True),
        producer=producer,
        topic=topic,
        serializer=serializer,
        lookup_callbacks=callback_store.get_by_names(
            list(ctx.transform_ctx.joins.keys())
        ),
    )


async def kafka_sink(
    task_id: str,
    _: DuckDBPyConnection,
    df: pl.DataFrame,
    from_table: str,
    transform_query: str,
    pl_ctx: pl.SQLContext,
    producer: Producer,
    topic: str,
    serializer: BaseSerializer,
    lookup_callbacks: dict[str, Callable[[pl.DataFrame], Awaitable[pl.DataFrame]]],
) -> None:
    logger.info("[{}] - starting sink executable", task_id)
    pl_ctx.register(from_table, df)

    # Register lookup if exist
    for lookup_table_name, func in lookup_callbacks.items():
        lookup_df = await func(df)
        pl_ctx.register(lookup_table_name, lookup_df)

    transform_df = pl_ctx.execute(transform_query)

    records = transform_df.to_dicts()

    def _produce_all():
        for record in records:
            payload = serializer.serialize(record)
            producer.produce(topic, value=payload)
            producer.poll(0)
        producer.flush()

    await trio.to_thread.run_sync(_produce_all)


def build_transform_executable(
    ctx: CreateViewContext, backend_conn: DuckDBPyConnection
) -> Callable[[str, DuckDBPyConnection, pl.DataFrame], Awaitable[pl.DataFrame]]:
    # TODO: handle more than one upstream when dealing with
    # stateful entities
    from_table = ctx.upstreams[0]

    return partial(
        transform_executable,
        backend_conn=backend_conn,
        name=ctx.name,
        from_table=from_table,
        transform_query=ctx.transform_ctx.query,
        is_materialized=ctx.materialized,
        # force eager to enforce read over write consistency
        pl_ctx=pl.SQLContext(register_globals=False, eager=True),
        lookup_callbacks=callback_store.get_by_names(
            list(ctx.transform_ctx.joins.keys())
        ),
    )


async def transform_executable(
    task_id: str,
    _: DuckDBPyConnection,
    df: pl.DataFrame,
    backend_conn: DuckDBPyConnection,
    name: str,
    from_table: str,
    transform_query: str,
    is_materialized: bool,
    pl_ctx: pl.SQLContext,
    lookup_callbacks: dict[str, Callable[[pl.DataFrame], Awaitable[pl.DataFrame]]],
) -> pl.DataFrame:
    logger.info("[{}] Starting @ {}", task_id, datetime.now(timezone.utc))

    # Register input df (from upstream)
    pl_ctx.register(from_table, df)

    # Register lookup if exist
    for lookup_table_name, func in lookup_callbacks.items():
        lookup_df = await func(df)
        pl_ctx.register(lookup_table_name, lookup_df)

    transform_df = pl_ctx.execute(transform_query)

    epoch = int(time.time() * 1_000)
    # invert is_materialized for truncate
    # if is_materialized -> truncate = False -> append mode
    # if not is_materialized -> truncate = True -> truncate mode (overwrite)
    await cache(transform_df, -1, epoch, name, backend_conn, not is_materialized)
    # update_batch_id_in_view_metadata(conn, name, batch_id + 1, is_materialized)

    return transform_df


async def eval_select(conn: DuckDBPyConnection, ctx: SelectContext) -> str:
    """
    Evaluate select statement against queryable cached layer.

    This also check the select is valid to ensure success execution.
    """
    table_name = ctx.table
    lookup_tables = get_lookup_tables(conn)
    duckdb_tables = get_duckdb_tables(conn)

    # add internal tables here for easier dev time
    duckdb_tables.append("duckdb_tables")

    if table_name in lookup_tables:
        return f"'{table_name}' is a lookup table, there is nothing to query."

    if table_name not in duckdb_tables:
        return f"'{table_name}' doesn't exist"

    pl_ctx = pl.SQLContext(register_globals=False, eager=True)

    # NOTE: to be deprecated
    duckdb_sql = ctx.query

    # If lookup join, we use the whole table for the lookup
    # might be a non-wanted behavior to be removed later
    if ctx.joins:
        df = duckdb_to_pl(conn, f"SELECT * FROM {ctx.table}")
        pl_ctx.register(table_name, df)

        # Register lookup if exist
        lookup_callbacks = callback_store.get_by_names(list(ctx.joins.keys()))
        for lookup_table_name, func in lookup_callbacks.items():
            lookup_df = await func(df)
            pl_ctx.register(lookup_table_name, lookup_df)

        return str(pl_ctx.execute(duckdb_sql))

    # Return requested table stored un duckdb
    return str(duckdb_to_pl(conn, duckdb_sql))


async def eval_set(conn: DuckDBPyConnection, ctx: SetContext):
    conn.sql(ctx.query)
    return "SET"


async def async_duckdb_to_pl(conn: DuckDBPyConnection, ctx: EvaluableContext):
    return str(duckdb_to_pl(conn, ctx.query))


# Static registration of EvaluableContext to
# their respective evaluable function
# NOTE: add type hint to callable 2nd argument. Seems to be linter
# failing to type with more than 1 level inheritance
EVALUABLE_QUERY_DISPATCH: dict[
    Type[EvaluableContext], Callable[[DuckDBPyConnection, Any], Awaitable[str]]
] = {
    CreateHTTPLookupTableContext: create_table,
    CreateHTTPTableContext: create_table,
    CreateWSTableContext: create_table,
    CreateHTTPSourceContext: create_source,
    CreateWSSourceContext: create_source,
    CreateViewContext: create_view,
    CreateSinkContext: create_sink,
    CreateSecretContext: create_secret,
    CommandContext: async_duckdb_to_pl,
    SetContext: eval_set,
    ShowContext: async_duckdb_to_pl,
    SelectContext: eval_select,
}
