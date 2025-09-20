# file cannot be named io
import asyncio
from typing import Any

import duckdb
import polars as pl
from loguru import logger


async def persist(
    df: pl.DataFrame,
    batch_id: int,
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

    from metadata import get_batch_id_from_table_metadata

    store_location = "local-store"
    table_name = "example"
    records = [{"url": "https://httpbin.org/get"} for _ in range(10)]
    df = pl.from_records(records)
    epoch = int(time.time() * 1_000)
    con = duckdb.connect(database=":memory:")
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (url TEXT)")
    batch_id = get_batch_id_from_table_metadata(con, table_name)

    async def _test():
        _ = await persist(df, batch_id, epoch, table_name, con)
        data = await read_all(store_location, table_name)
        print(data)

    asyncio.run(_test())
