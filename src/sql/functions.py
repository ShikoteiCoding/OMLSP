import polars as pl

from datetime import datetime
from loguru import logger
from sqlglot import exp
from typing import Any

__all__ = ["get_trigger_time", "get_trigger_time_epoch"]


def get_trigger_time(sql_context: dict[str, Any], output_type: exp.DataType) -> pl.Expr:
    """
    Custom OMLSP generated column function, to be used for scheduled sources.

    Example:
    CREATE TABLE (
        my_trigger_time TIMESTAMP AS (TRIGGER_TIME())
    )

    This expect output_type of TIMESTAMP but still tries best attempt.
    In worst case, timestamp can be interpreted from ISO formatted str.
    """
    trigger_time: datetime = sql_context["trigger_time"]
    output_type_str = str(output_type)

    if output_type_str == "TIMESTAMP_MS":
        millis = int(trigger_time.timestamp() * 1_000)
        return pl.lit(millis, dtype=pl.Datetime("ms"))

    if output_type_str in {"TIMESTAMP", "TIMESTAMP_US"}:
        micros = int(trigger_time.timestamp() * 1_000_000)
        return pl.lit(micros, dtype=pl.Datetime("us"))

    if output_type_str == "TIMESTAMP_NS":
        micros = int(trigger_time.timestamp() * 1_000_000_000)
        return pl.lit(micros, dtype=pl.Datetime("ns"))

    # Return best attempt
    logger.warning(
        "Failed to coerce timestamp `{}` with output type `{}`",
        trigger_time,
        output_type,
    )
    return pl.lit(trigger_time)


def get_trigger_time_epoch(
    sql_context: dict[str, Any], output_type: exp.DataType
) -> pl.Expr:
    """
    Custom OMLSP generated column function, to be used for scheduled sources.

    Example:
    CREATE TABLE (
        my_trigger_time TIMESTAMP AS (TRIGGER_EPOCH_TIME())
    )
    """
    trigger_time: datetime = sql_context["trigger_time"]
    epoch = int(trigger_time.timestamp() * 1_000)
    return pl.lit(epoch)
