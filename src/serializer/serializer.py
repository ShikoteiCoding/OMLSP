from __future__ import annotations
from typing import Any
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


class BaseSerializer:
    @classmethod
    def init(cls, config):
        raise NotImplementedError

    def serialize(self, record: dict[str, Any]) -> bytes | None:
        raise NotImplementedError


class JsonSerializer(BaseSerializer):
    @classmethod
    def init(cls, config: EncodeJSON) -> JsonSerializer:
        return cls.__new__(cls)

    def serialize(self, record: dict[str, Any]) -> bytes | None:
        return json.dumps(record).encode("utf-8")


class AvroSerializer(BaseSerializer):
    # We hold the Confluent serializer instance
    _serializer: ConfluentAvroSerializer
    _ctx: SerializationContext

    @classmethod
    def init(cls, config: EncodeAvro) -> "AvroSerializer":
        new = cls.__new__(cls)
        # Setup the Registry Client
        registry_client = SchemaRegistryClient({"url": config.registry})
        new._serializer = ConfluentAvroSerializer(
            schema_registry_client=registry_client,  # type: ignore
            conf={"auto.register.schemas": True},  # type: ignore
        )
        # Tell the serializer which Subject to look up
        new._ctx = SerializationContext(config.subject, MessageField.VALUE)
        return new

    def serialize(self, record: dict[str, Any]) -> bytes | None:
        # ID + Data
        payload = self._serializer(record, self._ctx)
        return payload


SERIALIZER_DISPATCH: dict[type, type[BaseSerializer]] = {
    EncodeJSON: JsonSerializer,
    EncodeAvro: AvroSerializer,
}
