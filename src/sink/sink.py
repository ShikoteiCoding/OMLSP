import asyncio
import duckdb
import json
from confluent_kafka import Producer, KafkaException
from loguru import logger
from typing import Any


def kafka_config(server, acks):
    return {"bootstrap.servers": server, "acks": acks}


def send_to_kafka(producer: Producer, topic: str, messages: list[dict[str, Any]]):
    for msg in messages:
        value = json.dumps(msg, default=str)
        producer.produce(topic, value=value.encode("utf-8"))

    remaining_messages = producer.flush(timeout=5)
    if remaining_messages > 0:
        raise KafkaException(
            f"Failed to deliver {remaining_messages} messages to Kafka"
        )


def get_table_schema(
    con: duckdb.DuckDBPyConnection, table_name: str
) -> list[dict[str, Any]]:
    result = con.execute(f"DESCRIBE {table_name}").fetchall()
    return [{"name": row[0], "type": row[1]} for row in result]


# TODO: add key_columns
# TODO: add more information
async def stream_to_kafka(con: Any,
                          table_name: str,
                          topic: str,
                          server,
                          acks,
                          task_id: str,
                          *args,
    **kwargs):
    producer = Producer(kafka_config(server, acks))
    last_processed = set()
    schema = get_table_schema(con, table_name)
    column_names = [col["name"] for col in schema]

    while True:
        try:
            rows = con.execute(f"SELECT * FROM {table_name}").fetchall()
            messages_to_send = []
            for row in rows:
                if row not in last_processed:
                    msg = {column_names[i]: row[i] for i in range(len(column_names))}
                    messages_to_send.append(msg)
                    last_processed.add(row)

            if messages_to_send:
                logger.info(
                    f"Found {len(messages_to_send)} new rows in '{table_name}', sending to Kafka"
                )
                send_to_kafka(producer, topic, messages_to_send)

        except Exception as e:
            logger.error(f"Kafka not found: {e}")
            break

        await asyncio.sleep(1)
