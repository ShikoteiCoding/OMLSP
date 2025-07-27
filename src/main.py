import argparse
import asyncio
import time


from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Coroutine
from loguru import logger

from parser import parse_query_to_dict
from requester import http_requester_builder
from inout import persist

STORE_LOCATION = "local-store"


def build_executable(properties: dict, table_name: str) -> Coroutine[Any, Any, None]:
    cron_expr = str(properties.get("schedule"))
    trigger = CronTrigger.from_crontab(cron_expr)
    requester = http_requester_builder(properties)
    scheduler = AsyncIOScheduler()
    fire_time = trigger.get_next_fire_time(
        datetime.now(tz=timezone.utc), datetime.now(tz=timezone.utc)
    )

    # TODO:  keep batch_id in metastore
    batch_id = 0

    async def job_func():
        nonlocal batch_id
        nonlocal trigger
        nonlocal fire_time

        start_interval_dt = trigger.get_next_fire_time(
            fire_time, datetime.now(timezone.utc)
        )
        if start_interval_dt:
            start_interval_dt = start_interval_dt.astimezone(timezone.utc)
            fire_time = start_interval_dt
        logger.info(
            f"running scheduled batch for table: {table_name} - {start_interval_dt}"
        )

        res = await requester()
        logger.info(f"http response: {res}")

        epoch = int(time.time() * 1_000)
        res = await persist(res, batch_id, epoch, STORE_LOCATION, table_name)
        batch_id += 1
        return res

    job = scheduler.add_job(job_func, trigger, name=table_name)

    async def _execute():
        scheduler.start()
        logger.info(f"next run time for schedule: {job.next_run_time}")

        # TODO: Dirty
        while True:
            await asyncio.sleep(5)

    return _execute()


async def run_all(parsed_queries):
    tasks = [
        asyncio.create_task(
            build_executable(query["table"]["properties"], query["table"]["name"]),
            name=query["table"]["name"],
        )
        for query in parsed_queries
        if "table" in query
        and "properties" in query["table"]
        and "name" in query["table"]
    ]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)


if __name__ == "__main__":
    # TODO: decipher entrypoint
    parser = argparse.ArgumentParser("Run a SQL file")
    parser.add_argument("file")

    args = parser.parse_args()
    filepath = Path(args.file)
    sql_content: str

    with open(filepath, "rb") as fo:
        sql_content = fo.read().decode("utf-8")

    parsed_queries = parse_query_to_dict(sql_content)

    asyncio.run(run_all(parsed_queries))
