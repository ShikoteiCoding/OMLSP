import polars as pl

from datetime import datetime
from loguru import logger
from sqlglot import exp
from typing import Any

__all__ = ["get_trigger_time", "get_trigger_time_epoch"]


def get_trigger_time(sql_context: dict[str, Any], output_type: exp.DataType) -> pl.Expr:
    trigger_time: datetime = sql_context["trigger_time"]
    logger.error(sql_context)
    logger.error(output_type)

    if str(output_type) == "TIMESTAMP_MS":
        millis = int(trigger_time.timestamp() * 1_000)
        return pl.lit(millis, dtype=pl.Datetime("ms"))

    if str(output_type) in {"TIMESTAMP", "TIMESTAMP_US"}:
        micros = int(trigger_time.timestamp() * 1_000_000)
        return pl.lit(micros, dtype=pl.Datetime("us"))

    if str(output_type) == "TIMESTAMP_NS":
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
    trigger_time: datetime = sql_context["trigger_time"]
    epoch = int(trigger_time.timestamp() * 1_000)
    return pl.lit(epoch)
