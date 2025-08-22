import asyncio
import polars as pl
import time
import re
import pyarrow as pa

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.job import Job
from apscheduler.triggers.cron import CronTrigger
from duckdb import DuckDBPyConnection, struct_type
from duckdb.functional import FunctionNullHandling, PythonUDFType
from duckdb.typing import VARCHAR, DuckDBPyType
from datetime import datetime, timezone
from loguru import logger
from typing import Any, Coroutine, Callable
from types import FunctionType

from string import Template

from inout import persist
from metadata import (
    create_macro_definition,
    get_macro_definition_by_name,
    get_lookup_tables,
    get_tables,
    create_table,
    get_batch_id_from_table_metadata,
    update_batch_id_in_table_metadata,
)
from requester import build_http_requester
from parser import CreateTableParams, CreateLookupTableParams, SelectParams, SetParams
from concurrent.futures import ThreadPoolExecutor

DUCKDB_TO_PYARROW_PYTYPE = {
    "VARCHAR": pa.string(),
    "TEXT": pa.string(),
    "TIMESTAMP": pa.timestamp("us"),
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
    properties: dict[str, str], context: dict[str, str]
) -> dict[str, str]:
    new_props = {}

    for key, value in properties.items():
        if key in ["jq", "method"]:  # TODO: ignore jq for now
            new_props[key] = value
            continue
        template = Template(value)
        new_props[key] = template.substitute(context)

    return new_props


def build_scalar_udf(
    properties: dict[str, str],
    dynamic_columns: list[str],
    return_type: DuckDBPyType = VARCHAR,
) -> dict[str, Any]:
    arity = len(dynamic_columns)

    child_types = []
    for subtype in return_type.children:
        logger.info(subtype)
        child_types.append(
            pa.field(subtype[0], DUCKDB_TO_PYARROW_PYTYPE[str(subtype[1])])
        )
    return_type_arrow = pa.struct(child_types)
    logger.info(child_types)

    if arity == 1:
        def udf(arg1):
            els = []
            for chunk in arg1.chunks:
                els.extend(chunk.to_pylist())
            context = {dynamic_columns[0]: els}
            return process_elements(context, properties, return_type_arrow)
    elif arity == 2:
        def udf(arg1, arg2):
            arrays = [arg.to_pylist() for arg in [arg1, arg2]]
            els = list(zip(*arrays))
            context = [dict(zip(dynamic_columns, el)) for el in els]
            return process_elements(context, properties, return_type_arrow)
    elif arity == 3:
        def udf(arg1, arg2, arg3):
            arrays = [arg.to_pylist() for arg in [arg1, arg2, arg3]]
            els = list(zip(*arrays))
            context = [dict(zip(dynamic_columns, el)) for el in els]
            return process_elements(context, properties, return_type_arrow)
    elif arity == 4:
        def udf(arg1, arg2, arg3, arg4):
            arrays = [arg.to_pylist() for arg in [arg1, arg2, arg3, arg4]]
            els = list(zip(*arrays))
            context = [dict(zip(dynamic_columns, el)) for el in els]
            return process_elements(context, properties, return_type_arrow)
    elif arity == 5:
        def udf(arg1, arg2, arg3, arg4, arg5):
            arrays = [arg.to_pylist() for arg in [arg1, arg2, arg3, arg4, arg5]]
            els = list(zip(*arrays))
            context = [dict(zip(dynamic_columns, el)) for el in els]
            return process_elements(context, properties, return_type_arrow)
    else:
        raise ValueError("Too many dynamic columns (max 5 supported)")

    def process_elements(context_list, properties, return_type_arrow):
        def _inner(context):
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
            results = list(executor.map(_inner, context_list))

        return pa.array(results, type=return_type_arrow)

    return {
        "function": udf,
        "parameters": [VARCHAR for _ in range(arity)],
        "return_type": return_type,
        "type": PythonUDFType.ARROW,
        "null_handling": FunctionNullHandling.SPECIAL,
    }


async def processor(
    table_name: str,
    start_time: datetime,
    http_requester: FunctionType,
    con: DuckDBPyConnection,
) -> None:
    # TODO: no provided api execution_time
    # using trigger.get_next_fire_time is costly (see code)
    execution_time = start_time
    batch_id = get_batch_id_from_table_metadata(con, table_name)
    logger.info(f"[{table_name}{{{batch_id}}}] @ {execution_time}")

    records = await http_requester()
    logger.debug(
        f"[{table_name}{{{batch_id}}}] - http number of responses: {len(records)} - batch {batch_id}"
    )

    if len(records) > 0:
        epoch = int(time.time() * 1_000)
        # TODO: type polars with duckdb table catalog
        df = pl.from_records(records)
        await persist(df, batch_id, epoch, table_name, con)

    update_batch_id_in_table_metadata(con, table_name, batch_id + 1)


async def execute(scheduler: AsyncIOScheduler, job: Job):
    scheduler.start()
    logger.info(f"[{job.name}] - next schedule: {job.next_run_time}")

    # TODO: Dirty
    while True:
        await asyncio.sleep(3600)


def build_one_runner(
    create_table_params: CreateTableParams, con: DuckDBPyConnection
) -> Coroutine[Any, Any, None]:
    properties = create_table_params.properties
    table_name = create_table_params.name
    cron_expr = str(properties["schedule"])
    scheduler = AsyncIOScheduler()

    trigger = CronTrigger.from_crontab(cron_expr, timezone=timezone.utc)
    start_time = datetime.now(timezone.utc)
    http_requester = build_http_requester(properties)

    job = scheduler.add_job(
        processor,
        trigger,
        name=table_name,
        kwargs={
            "table_name": table_name,
            "start_time": start_time,
            "http_requester": http_requester,
            "con": con,
        },
    )

    return execute(scheduler, job)


def register_lookup_table_executable(
    create_table_params: CreateLookupTableParams, connection: DuckDBPyConnection
) -> str:
    properties = create_table_params.properties
    table_name = create_table_params.name
    dynamic_columns = create_table_params.dynamic_columns
    columns = create_table_params.columns

    func_name = f"{table_name}_func"
    macro_name = f"{table_name}_macro"
    # TODO: handle other return than dict (for instance array http responses)
    return_type = struct_type(columns)  # typed struct from sql statement

    udf_params = build_scalar_udf(properties, dynamic_columns, return_type)
    # register scalar for row to row http call
    connection.create_function(
        name=func_name,
        function=udf_params["function"], # type: ignore
        parameters=udf_params["parameters"],
        return_type=udf_params["return_type"],
        type=udf_params["type"],
        null_handling=udf_params["null_handling"],
    )
    logger.debug(f"registered function: {func_name}")

    # TODO: wrap SQL in function
    # register macro (to be injected in place of sql)
    func_def = f"{func_name}({','.join(dynamic_columns)})"
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
            FROM query_table(table_name)
        );
    """)
    logger.debug(f"registered macro: {macro_name}")
    create_macro_definition(connection, macro_name, dynamic_columns)

    return macro_name


async def start_background_runnners_or_register(
    table_params: CreateTableParams | CreateLookupTableParams,
    connection: DuckDBPyConnection,
):
    task: asyncio.Task | None = None
    task: asyncio.Task | None = None

    name = table_params.name
    create_table(connection, table_params)

    # register table, temp tables (TODO: views / materialized views / sink)
    if isinstance(table_params, CreateTableParams):
        task = asyncio.create_task(
            build_one_runner(table_params, connection), name=f"{name}_runner"
        )

    # handle lookup table
    if isinstance(table_params, CreateLookupTableParams):
        register_lookup_table_executable(table_params, connection)

    if task:
        _, _ = await asyncio.wait([task], return_when=asyncio.ALL_COMPLETED)


def select_query_to_duckdb(
    con: DuckDBPyConnection,
    select_query: SelectParams,
    lookup_tables: list[str],
    tables: list[str],
) -> str:
    original_query = select_query.query
    join_tables = select_query.joins

    # no lookup query case
    if len(join_tables) == 0:
        return original_query

    # lookup query case
    mapping = dict(zip(tables, tables))
    table_name = select_query.table
    table_alias = select_query.alias
    for lookup_table in lookup_tables:
        macro_name, fields = get_macro_definition_by_name(con, f"{lookup_table}_macro")
        # TODO: move to func ?
        dynamic_macro_stmt = f'{macro_name}("{table_name}", {",".join([f"{table_alias}.{field}" for field in fields])})'
        mapping[lookup_table] = dynamic_macro_stmt

    # Substritute lookup table name with query
    query = Template(original_query).substitute(mapping)

    # Remove surrounding quote from parse_select
    # And add AS statement in join from placeholder name
    # TODO: AS statement in join should be table OR alias
    for lookup_table in lookup_tables:
        subst_string = mapping[lookup_table]
        matches = [
            (m.start(), m.end()) for m in re.finditer(re.escape(subst_string), query)
        ][0]
        query = (
            query[0 : matches[0] - 1]
            + query[matches[0] : matches[1]]
            + f" AS {lookup_table}"
            + query[matches[1] + 1 : len(query)]
        )

    logger.debug(f"new overwritten select statement: {query}")
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


def handle_select_or_set(
    con: DuckDBPyConnection, params: SelectParams | SetParams
) -> str | pl.DataFrame:
    if isinstance(params, SelectParams):
        table_name = params.table
        lookup_tables = get_lookup_tables(con)
        tables = get_tables(con)

        if table_name in lookup_tables:
            msg = f"{table_name} is a lookup table, you cannot use it in FROM."
            logger.error(msg)
            return msg

        duckdb_sql = select_query_to_duckdb(con, params, lookup_tables, tables)
        return duckdb_to_pl(con, duckdb_sql)
    else:
        try:
            con.sql(params.query)
        except Exception as e:
            return str(e)  # TODO: handle duckdb configs and omlsp custom configs
        return "SET"  # psql syntax


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
