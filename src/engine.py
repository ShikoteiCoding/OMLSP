import asyncio
import time
import polars as pl

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.job import Job
from apscheduler.triggers.cron import CronTrigger
from duckdb import DuckDBPyConnection
from datetime import datetime, timezone
from loguru import logger
from typing import Any, Callable, Coroutine

from inout import persist
from utils import MutableInteger
from requester import build_http_requester


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
        # TODO: type polars with ducbdb table catalog
        df = pl.from_records(records)
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


def build_one_executable(
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


async def run_executables(parsed_queries: list[dict], connection: DuckDBPyConnection):
    tasks = []
    for table_config in parsed_queries:
        register_table(table_config, connection)

    tasks = [
        asyncio.create_task(
            build_one_executable(table_config, connection),
            name=table_config["name"],
        )
        for table_config in parsed_queries
        if table_config["properties"]["connector"] == "http"
    ]
    _, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
