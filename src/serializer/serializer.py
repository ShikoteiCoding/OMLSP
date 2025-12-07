from __future__ import annotations
from typing import Any, TypeVar, Generic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import (
    AvroSerializer as ConfluentAvroSerializer,
)
from confluent_kafka.serialization import SerializationContext, MessageField
import json
from sql.types import (
    EncodeJSON,
    EncodeAvro,
)

T = TypeVar("T")


class BaseSerializer(Generic[T]):
    @classmethod
    def init(cls, config: T, topic: str):
        raise NotImplementedError

    def serialize(self, record: dict[str, Any]) -> bytes:
        raise NotImplementedError


class JsonSerializer(BaseSerializer):
    @classmethod
    def init(cls, config: EncodeJSON, topic: str) -> JsonSerializer:
        return cls.__new__(cls)

    def serialize(self, record: dict[str, Any]) -> bytes:
        return json.dumps(record).encode("utf-8")


class AvroSerializer(BaseSerializer):
    # We hold the Confluent serializer instance
    _serializer: ConfluentAvroSerializer
    _ctx: SerializationContext

    @classmethod
    def init(cls, config: EncodeAvro, topic: str) -> "AvroSerializer":
        new = cls.__new__(cls)

        registry_client = SchemaRegistryClient({"url": config.registry})

        expected_subject = f"{topic}-value"
        registered_version = registry_client.get_latest_version(expected_subject)

        # Extract the actual schema
        fetched_schema_str = registered_version.schema.schema_str  # type: ignore

        new._serializer = ConfluentAvroSerializer(
            schema_registry_client=registry_client,  # type: ignore
            schema_str=fetched_schema_str,  # type: ignore
            conf={"auto.register.schemas": False},  # type: ignore
        )

        new._ctx = SerializationContext(topic, MessageField.VALUE)
        return new

    def serialize(self, record: dict[str, Any]) -> bytes:
        # ID + Data
        payload = self._serializer(record, self._ctx)
        return payload  # type: ignore


SERIALIZER_DISPATCH: dict[type, type[BaseSerializer]] = {
    EncodeJSON: JsonSerializer,
    EncodeAvro: AvroSerializer,
}
