import asyncio
import polars as pl
import time
import re
import pyarrow as pa

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.job import Job
from apscheduler.triggers.cron import CronTrigger
from duckdb import DuckDBPyConnection, struct_type
from duckdb.typing import VARCHAR, DuckDBPyType
from duckdb.functional import ARROW, SPECIAL
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
)
from utils import MutableInteger
from requester import build_http_requester
from parser import CreateTableParams, CreateLookupTableParams, SelectParams
from concurrent.futures import ThreadPoolExecutor

# TODO: enrich with more type
DUCKDB_TO_PYARROW_PYTYPE = {
    "VARCHAR": pa.string(),
    "TIMESTAMP": pa.timestamp("us"),
    "FLOAT": pa.float32(),  # TODO: verify duckdb internal floating point
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
    pyarrow: bool = False,
    return_type: DuckDBPyType = VARCHAR,
) -> Callable[..., pa.Array]:
    arity = len(dynamic_columns)

    if pyarrow:
        pyarrow_child_types = []
        for subtype in return_type.children:
            pyarrow_child_types.append(
                pa.field(subtype[0], DUCKDB_TO_PYARROW_PYTYPE[str(subtype[1])])
            )
        pyarrow_return_type = pa.struct(pyarrow_child_types)

    def udf1(a1):
        # TODO: build default response from lookup table schema
        context = dict(zip(dynamic_columns, [a1]))
        lookup_properties = build_lookup_properties(properties, context)
        default_response = context.copy()

        requester = build_http_requester(lookup_properties, is_async=False)

        try:
            response = requester()

            if isinstance(response, dict):
                return context | response

            # TODO: handle array jq responses
            # need different scalar udf return type to handle
            # and most likely an explode in macro
            if isinstance(response, list):
                if len(response) >= 1:
                    default_response |= response[-1]

        except Exception as e:
            logger.exception(f"HTTP request failed: {e}")

        return default_response

    def udf1pyarrow(a1: Any) -> pa.Array:
        els = []
        for chunk in a1.chunks:
            els.extend(chunk.to_pylist())

        def _inner(el):
            context = dict(zip(dynamic_columns, [el]))
            lookup_properties = build_lookup_properties(properties, context)
            default_response = context.copy()

            requester = build_http_requester(lookup_properties, is_async=False)

            try:
                response = requester()

                if isinstance(response, dict):
                    return context | response

                # TODO: handle array jq responses
                # need different scalar udf return type to handle
                # and most likely an explode in macro
                if isinstance(response, list):
                    if len(response) >= 1:
                        default_response |= response[-1]

            except Exception as e:
                logger.exception(f"HTTP request failed: {e}")

            return default_response

        with ThreadPoolExecutor() as executor:
            results = list(executor.map(_inner, els))

        return pa.array(results, type=pyarrow_return_type)

    if arity == 1:
        if pyarrow:
            return udf1pyarrow
        return udf1
    else:
        raise ValueError("Too many dynamic columns (max 1 supported)")


async def processor(
    table_name: str,
    batch_id: MutableInteger,
    start_time: datetime,
    http_requester: FunctionType,
    connection: Any,
) -> None:
    # TODO: no provided api execution_time
    # using trigger.get_next_fire_time is costly (see code)
    execution_time = start_time
    logger.info(f"[{table_name}{{{batch_id}}}] @ {execution_time}")

    records = await http_requester()
    logger.debug(
        f"[{table_name}{{{batch_id}}}] - http number of responses: {len(records)} - batch {batch_id}"
    )

    if len(records) > 0:
        epoch = int(time.time() * 1_000)
        # TODO: type polars with duckdb table catalog
        df = pl.from_records(records)
        await persist(df, batch_id, epoch, table_name, connection)

    batch_id.increment()


async def execute(scheduler: AsyncIOScheduler, job: Job):
    scheduler.start()
    logger.info(f"[{job.name}] - next schedule: {job.next_run_time}")

    # TODO: Dirty
    while True:
        await asyncio.sleep(3600)


def register_table(
    create_table_params: CreateTableParams | CreateLookupTableParams,
    connection: DuckDBPyConnection,
) -> None:
    query = create_table_params.query
    connection.execute(query)
    logger.debug(f"running query: {query}")


def build_one_runner(
    create_table_params: CreateTableParams, connection: DuckDBPyConnection
) -> Coroutine[Any, Any, None]:
    properties = create_table_params.properties
    table_name = create_table_params.name
    cron_expr = str(properties["schedule"])
    scheduler = AsyncIOScheduler()

    # TODO:  keep batch_id in metastore
    batch_id = MutableInteger(0)
    trigger = CronTrigger.from_crontab(cron_expr, timezone=timezone.utc)
    start_time = datetime.now(timezone.utc)
    http_requester = build_http_requester(properties)

    job = scheduler.add_job(
        processor,
        trigger,
        name=table_name,
        kwargs={
            "table_name": table_name,
            "batch_id": batch_id,
            "start_time": start_time,
            "http_requester": http_requester,
            "connection": connection,
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

    # TODO: move all "create_function" arguments inside build_scalar_udf
    # to avoid verbose function here
    func = build_scalar_udf(properties, dynamic_columns, True, return_type)

    # register scalar for row to row http call
    connection.create_function(
        name=func_name,
        function=func, # type: ignore
        parameters=[VARCHAR for _ in range(len(dynamic_columns))],
        return_type=return_type,
        type=ARROW,
        null_handling=SPECIAL,
    )
    logger.debug(f"registered function: {func_name}")

    # TODO: wrap SQL in function
    # register macro (to be injected in place of sql)
    func_def = f"{func_name}({','.join(dynamic_columns)})"
    macro_def = f"{macro_name}(table_name, {','.join(dynamic_columns)})"
    output_cols = ", ".join(
        [f"struct.{col_name} AS {col_name}" for col_name, _ in columns.items()]
    )

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


async def run_executables(
    statement_params: list[CreateTableParams | CreateLookupTableParams],
    connection: DuckDBPyConnection,
):
    tasks = []

    for table_params in statement_params:
        name = table_params.name
        register_table(table_params, connection)

        # register table, temp tables (TODO: views / materialized views / sink)
        if isinstance(table_params, CreateTableParams):
            tasks.append(
                asyncio.create_task(
                    build_one_runner(table_params, connection), name=f"{name}_runner"
                )
            )

        # handle lookup table
        if isinstance(table_params, CreateLookupTableParams):
            register_lookup_table_executable(table_params, connection)

    _, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)


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


def handle_select(
    con: DuckDBPyConnection, select_query: SelectParams
) -> str | pl.DataFrame:
    table_name = select_query.table
    lookup_tables = get_lookup_tables(con)
    tables = get_tables(con)

    if table_name in lookup_tables:
        msg = f"{table_name} is a lookup table, you cannot use it in FROM."
        logger.error(msg)
        return msg

    duckdb_sql = select_query_to_duckdb(con, select_query, lookup_tables, tables)
    return duckdb_to_pl(con, duckdb_sql)


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
