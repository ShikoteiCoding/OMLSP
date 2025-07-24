import argparse
import asyncio
import time


from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Coroutine, NoReturn
from loguru import logger

from parser import parse_query_to_dict
from requester import http_requester_builder
from inout import persist

STORE_LOCATION = "local-store"


def build_executable(
    properties: dict, table_name: str
) -> Callable[[], Coroutine[Any, Any, NoReturn]]:
    cron_expr = properties.get("schedule")
    trigger = CronTrigger.from_crontab(cron_expr)
    requester = http_requester_builder(properties)

    # TODO:  keep batch_id in metastore
    batch_id = 0

    async def job_func():
        logger.info(f"Running scheduled batch for table: {table_name}")
        logger.info(f"{datetime.now(timezone.utc)}")
        nonlocal batch_id
        res = await requester()
        logger.info(f"http response: {res}")
        epoch = int(time.time() * 1_000)
        res = await persist(res, batch_id, epoch, STORE_LOCATION, table_name)
        batch_id += 1
        return res

    async def _execute():
        scheduler = AsyncIOScheduler()
        scheduler.add_job(job_func, trigger)
        scheduler.start()

        # TODO: Dirty
        while True:
            await asyncio.sleep(3600)

    return _execute


if __name__ == "__main__":
    # TODO: decipher entrypoint
    parser = argparse.ArgumentParser("Run a SQL file")
    parser.add_argument("file")

    args = parser.parse_args()
    filepath = Path(args.file)
    sql_content: str

    with open(filepath, "rb") as fo:
        sql_content = fo.read().decode("utf-8")

    parsed_query = parse_query_to_dict(sql_content)

    fn = build_executable(parsed_query["properties"], parsed_query["table_name"])
    asyncio.run(fn())
