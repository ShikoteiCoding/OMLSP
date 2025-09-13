import asyncio
import duckdb
import json
from confluent_kafka import Producer, KafkaException
from context.context import (
    SelectContext,
)
from loguru import logger
from typing import Any


def kafka_config(server, acks):
    return {"bootstrap.servers": server, "acks": acks}


def get_table_schema(
    con: duckdb.DuckDBPyConnection, table_name: str
) -> list[dict[str, Any]]:
    result = con.execute(f"DESCRIBE {table_name}").fetchall()
    return [{"name": row[0], "type": row[1]} for row in result]


def get_select_schema(con: duckdb.DuckDBPyConnection, context: SelectContext):
    result = con.execute(f"{context.query} LIMIT 0")
    return [{"name": col[0], "type": col[1]} for col in result.description]


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
    def __init__(self, server: str, acks: str, topic: str):
        self.producer = Producer(kafka_config(server, acks))
        self.topic = topic

    def send(self, messages: list[dict[str, Any]]):
        for msg in messages:
            self.producer.produce(
                self.topic, value=json.dumps(msg, default=str).encode("utf-8")
            )
        self.producer.poll(0)

    def close(self):
        self.producer.flush()


def resolve_source(con, source: str | SelectContext):
    if isinstance(source, SelectContext):
        sql = source.query
        schema = get_select_schema(con, source)
    else:
        sql = f"SELECT * FROM {source}"
        schema = get_table_schema(con, source)
    return sql, schema


async def run_kafka_sink(
    con: duckdb.DuckDBPyConnection,
    source: str | SelectContext,
    topic: str,
    server: str,
    acks: str,
    task_id: str,
    poll_interval: float = 1.0,
):
    sql, schema = resolve_source(con, source)
    column_names = [col["name"] for col in schema]

    sink = KafkaSink(server, acks, topic)
    dedup = Deduplicator()  # TODO: use primary key

    try:
        while True:
            rows = con.execute(sql).fetchall()
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

            await asyncio.sleep(poll_interval)

    except Exception as e:
        logger.error(f"[{task_id}] Sink failed: {e}")
    finally:
        sink.close()
