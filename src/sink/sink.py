import json
import polars as pl

from functools import partial
from confluent_kafka import Producer


def build_sink_connector(properties: dict[str, str]):
    producer = Producer({"bootstrap.servers": properties["server"]})
    topic = properties["topic"]
    return partial(
        kafka_sink,
        producer,
        topic,
    )


async def kafka_sink(producer: Producer, topic: str, df: pl.DataFrame):
    records = df.to_dicts()

    for record in records:
        payload = json.dumps(record).encode("utf-8")
        producer.produce(topic, value=payload)
        producer.poll(0)

    # handle timeout
    producer.flush()
