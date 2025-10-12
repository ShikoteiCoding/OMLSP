import polars as pl

from datetime import datetime
from typing import Any

__all__ = ["get_trigger_time", "get_trigger_time_epoch", "OMLSP_FUNCTIONS"]

OMLSP_FUNCTIONS = ["TRIGGER_TIME", "TRIGGER_TIME_EPOCH"]


def get_trigger_time(sql_context: dict[str, Any]) -> pl.Expr:
    trigger_time: datetime = sql_context["trigger_time"]
    return pl.lit(trigger_time)


def get_trigger_time_epoch(sql_context: dict[str, Any]) -> pl.Expr:
    trigger_time: datetime = sql_context["trigger_time"]
    epoch = int(trigger_time.timestamp() * 1000 * 1000)
    return pl.lit(epoch)
