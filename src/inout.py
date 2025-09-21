# file cannot be named io
import polars as pl

from typing import Any
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
