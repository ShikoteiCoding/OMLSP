import trio
from datetime import datetime, timezone
from croniter import croniter


async def minimal_scheduler(cron_expr: str):
    """
    cron-based scheduler
    yields next_fire_time whenever the cron expression fires
    """
    base_time = datetime.now(timezone.utc)
    cron = croniter(cron_expr, base_time)

    while True:
        next_fire = cron.get_next(datetime)
        delay = (next_fire - datetime.now(timezone.utc)).total_seconds()
        if delay > 0:
            await trio.sleep(delay)
        yield next_fire


if __name__ == "__main__":

    async def main():
        async for ts in minimal_scheduler("*/1 * * * *"):
            print(f"tick at {ts}")

    trio.run(main)
