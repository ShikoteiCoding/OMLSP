import trio
import duckdb
import json
from confluent_kafka import Producer
from context.context import (
    SelectContext,
)
from metadata import (
    resolve_schema
)
from loguru import logger
from typing import Any
from datetime import datetime, timedelta


def kafka_config(server):
    return {"bootstrap.servers": server}


class Deduplicator:
    def __init__(self, key_index: int | None = None):
        self.key_index = key_index
        self.last_seen = set()

    def is_new(self, row) -> bool:
        key = row if self.key_index is None else row[self.key_index]
        if key in self.last_seen:
            return False
        self.last_seen.add(key)
        return True


class KafkaSink:
    def __init__(self, server: str, topic: str):
        self.producer = Producer(kafka_config(server))
        self.topic = topic

    def send(self, messages: list[dict[str, Any]]):
        for msg in messages:
            self.producer.produce(
                self.topic, value=json.dumps(msg, default=str).encode("utf-8")
            )
        self.producer.poll(0)

    def close(self, timeout: float = 10.0):
        deadline = datetime.now() + timedelta(seconds=timeout)
        while True:
            remaining = self.producer.flush(timeout=1)
            if remaining == 0:
                break
            if datetime.now() > deadline:
                logger.error(
                    f"KafkaSink failed to flush {remaining} messages before timeout"
                )
                break


async def run_kafka_sink(
    con: duckdb.DuckDBPyConnection,
    upstream: list[str | SelectContext],
    topic: str,
    server: str,
    task_id: str,
    poll_interval: float = 1.0,
):
    # TODO: in future: support multiple upstreams merged/unioned
    relation = upstream[0]

    sql, schema = resolve_schema(con, relation)
    column_names = [col["name"] for col in schema]

    sink = KafkaSink(server, topic)
    dedup = Deduplicator()  # TODO: use batch id to deduplicate

    try:
        while True:
            rows = await trio.to_thread.run_sync(lambda: con.execute(sql).fetchall())
            new_messages = [
                {column_names[i]: row[i] for i in range(len(column_names))}
                for row in rows
                if dedup.is_new(row)
            ]
            if new_messages:
                logger.info(
                    f"Sending {len(new_messages)} rows from {task_id} → Kafka topic '{topic}'"
                )
                sink.send(new_messages)

            await trio.sleep(poll_interval)

    except Exception as e:
        logger.error(f"[{task_id}] Sink failed: {e}")
    finally:
        sink.close()
