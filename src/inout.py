# file cannot be named io
import asyncio
import duckdb
import polars as pl
from typing import Any

from loguru import logger
from utils import MutableInteger


async def persist(
    df: pl.DataFrame,
    batch_id: MutableInteger,
    epoch: int,
    table_name: str,
    connection: Any,
) -> None:
    connection.execute(f"INSERT INTO {table_name} SELECT * FROM df")
    logger.info(f"[{table_name}{{{batch_id}}}] {len(df)} records inserted @ {epoch}")


async def read_all(store_location: str, table_name: str) -> pl.DataFrame:
    rel = con.execute(f"SELECT * FROM {table_name}")
    data = rel.pl()
    return data


if __name__ == "__main__":
    import time

    store_location = "local-store"
    table_name = "example"
    records = [{"url": "https://httpbin.org/get"} for _ in range(10)]
    df = pl.from_records(records)
    epoch = int(time.time() * 1_000)
    batch_id = MutableInteger()
    con = duckdb.connect(database=":memory:")
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (url TEXT)")

    async def _test():
        _ = await persist(df, batch_id, epoch, table_name, con)
        data = await read_all(store_location, table_name)
        print(data)

    asyncio.run(_test())
