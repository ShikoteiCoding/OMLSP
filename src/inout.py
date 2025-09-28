# file cannot be named io
import polars as pl

from loguru import logger
from duckdb import DuckDBPyConnection


async def cache(
    df: pl.DataFrame,
    batch_id: int,
    epoch: int,
    table_name: str,
    conn: DuckDBPyConnection,
) -> None:
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
    logger.debug(f"[{table_name}{{{batch_id}}}] {len(df)} records inserted @ {epoch}")
