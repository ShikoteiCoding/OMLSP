import json
import polars as pl
import trio

from functools import partial
from confluent_kafka import Producer


def build_sink_connector(properties: dict[str, str]):
    if properties and properties.get("connector") == "kafka":
        producer = Producer({"bootstrap.servers": properties["server"]})
        topic = properties["topic"]
        return partial(
            kafka_sink,
            producer,
            topic,
        )
    else:
        return None


async def kafka_sink(producer: Producer, topic: str, df: pl.DataFrame):
    records = df.to_dicts()

    def _produce_all():
        for record in records:
            payload = json.dumps(record).encode("utf-8")
            producer.produce(topic, value=payload)
            producer.poll(0)
        producer.flush()

    await trio.to_thread.run_sync(_produce_all)
