import asyncio
import polars as pl
import pyarrow as pa
import time

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.job import Job
from apscheduler.triggers.cron import CronTrigger
from duckdb import DuckDBPyConnection, struct_type
from duckdb.typing import VARCHAR
from datetime import datetime, timezone
from loguru import logger
from typing import Any, Callable, Coroutine

from inout import persist
from utils import MutableInteger
from requester import build_http_requester
from string import Template


def infer_properties(
    properties: dict[str, str], context: dict[str, str]
) -> dict[str, str]:
    new_props = {}

    for key, value in properties.items():
        if key in ["jsonpath", "method"]:  # TODO: ignore jsonpath for now
            new_props[key] = value
            continue
        template = Template(value)
        new_props[key] = template.substitute(context)

    return new_props


def build_scalar_udf(
    properties: dict[str, str], dynamic_columns: list[str]
) -> Callable:
    arity = len(dynamic_columns)

    def udf1(a1):
        context = dict(zip(dynamic_columns, a1))
        inferred_properties = infer_properties(properties, context)
        requester = build_http_requester(inferred_properties)
        return pa.array(requester(), type=pa.string())

    def udf2(a1, a2):
        context = dict(zip(dynamic_columns, a1 + a2))
        inferred_properties = infer_properties(properties, context)
        requester = build_http_requester(inferred_properties)
        return pa.array(requester(), type=pa.string())

    def udf3(a1, a2, a3):
        context = dict(zip(dynamic_columns, a1 + a2 + a3))
        inferred_properties = infer_properties(properties, context)
        requester = build_http_requester(inferred_properties)
        return pa.array(requester(), type=pa.string())

    def udf4(a1, a2, a3, a4):
        context = dict(zip(dynamic_columns, a1 + a2 + a3 + a4))
        inferred_properties = infer_properties(properties, context)
        requester = build_http_requester(inferred_properties)
        return pa.array(requester(), type=pa.string())

    def udf5(a1, a2, a3, a4, a5):
        context = dict(zip(dynamic_columns, a1 + a2 + a3 + a4 + a5))
        inferred_properties = infer_properties(properties, context)
        requester = build_http_requester(inferred_properties)
        return pa.array(requester(), type=pa.string())

    if arity == 1:
        return udf1
    elif arity == 2:
        return udf2
    elif arity == 3:
        return udf3
    elif arity == 4:
        return udf4
    elif arity == 5:
        return udf5
    else:
        raise ValueError("Too many dynamic columns (max 5 supported)")


async def processor(
    table_name: str,
    batch_id: MutableInteger,
    start_time: datetime,
    http_requester: Callable,
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
        print(df)
        await persist(df, batch_id, epoch, table_name, connection)

    batch_id.increment()


async def execute(scheduler: AsyncIOScheduler, job: Job):
    scheduler.start()
    logger.info(f"[{job.name}] - next schedule: {job.next_run_time}")

    # TODO: Dirty
    while True:
        await asyncio.sleep(3600)


def register_table(query_config: dict, connection: DuckDBPyConnection) -> None:
    query = query_config["query"]
    connection.execute(query_config["query"])
    logger.debug(query)


def build_one_runner(
    query_as_dict: dict, connection: DuckDBPyConnection
) -> Coroutine[Any, Any, None]:
    properties = query_as_dict["properties"]
    table_name = query_as_dict["name"]
    cron_expr = str(properties.get("schedule"))
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
    query_as_dict: dict, connection: DuckDBPyConnection
) -> str:
    properties = query_as_dict["properties"]
    table_name = query_as_dict["name"]
    dynamic_columns = query_as_dict["dynamic_columns"]
    columns = query_as_dict["columns"]

    func_name = f"{table_name}_func"
    macro_name = f"{table_name}_macro"

    func = build_scalar_udf(properties, dynamic_columns)

    return_type = struct_type(columns)  # typed struct from sql statement
    output_cols = ", ".join(
        [f"struct.{col_name} AS {col_name}" for col_name, _ in columns.items()]
    )

    # register scalar for row to row http call
    connection.create_function(
        name=func_name,
        function=func,  # type: ignore
        parameters=[VARCHAR for _ in range(len(dynamic_columns))],  # type: ignore
        return_type=return_type,  # type: ignore
        type="native",  # type: ignore
    )
    logger.debug(f"registered function: {func_name}")

    # register macro (to be injected in place of sql)
    connection.sql(f"""
        CREATE OR REPLACE MACRO {macro_name}(table_name, {",".join(dynamic_columns)}) AS TABLE
        SELECT
            {output_cols}
        FROM (
        SELECT
            {func_name}({",".join(dynamic_columns)}) AS struct
        FROM query_table(table_name)
        );
    """)
    logger.debug(f"registered macro: {macro_name}")

    return macro_name


async def run_executables(parsed_queries: list[dict], connection: DuckDBPyConnection):
    tasks = []

    for table_config in parsed_queries:
        properties = table_config["properties"]
        name = table_config["name"]

        # register table, temp tables (TODO: views / materialized views / sink)
        register_table(table_config, connection)

        # start runner for non lookup tables
        if properties["connector"] == "http":
            tasks.append(
                asyncio.create_task(
                    build_one_runner(table_config, connection), name=f"{name}_runner"
                )
            )

        # handle lookup table
        if properties["connector"] == "lookup-http":
            macro_name = register_lookup_table_executable(table_config, connection)
            logger.debug(macro_name)

    _, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)


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
