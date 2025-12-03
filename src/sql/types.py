from typing import Literal, Protocol
from dataclasses import dataclass


type HttpMethod = Literal["GET", "POST"]
type SourceConnectorName = Literal["http", "lookup-http", "ws"]


class JQ(Protocol):
    def input(self, data: dict): ...


class Properties:
    """
    Base Properties class, can be freely used as an untyped dictionnary.

    To be instanciated to cast subclasses (typed).
    """

    ...


@dataclass
class EncodeJSON:
    pass


@dataclass
class EncodeAvro:
    registry: str
    subject: str


EncodeType = EncodeJSON | EncodeAvro


@dataclass
class SinkProperties(Properties):
    topic: str
    server: str
    encode: EncodeType


@dataclass
class SourceHttpProperties(Properties):
    url: str
    method: HttpMethod
    jq: JQ
    signer_class: str

    # TODO: type those properties
    pagination: dict
    headers: dict
    json: dict
    params: dict


@dataclass
class SourceWSProperties(Properties):
    url: str
    jq: JQ
    on_start_query: str


@dataclass
class SecretProperties(Properties):
    backend: Literal["meta"]
