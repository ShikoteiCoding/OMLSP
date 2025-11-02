from typing import Literal, TypedDict

type HttpMethod = Literal["GET", "POST"]
type SourceConnectorName = Literal["http", "lookup-http", "ws"]


class Properties(TypedDict):
    """
    Base Properties class, can be freely used as an untyped dictionnary.

    To be instanciated to cast subclasses (typed).
    """

    ...


class SinkProperties(Properties, TypedDict):
    connector: SourceConnectorName
    topic: str
    server: str
    format: str


class SourceHttpProperties(Properties, TypedDict):
    connector: SourceConnectorName
    url: str
    method: HttpMethod
    schedule: str
    jq: str

    # TODO: type those properties
    pagination: dict
    headers: dict
    json: dict
    params: dict


class SourceWSProperties(Properties, TypedDict):
    connector: SourceConnectorName
    url: str
    jq: str
    on_start_query: str


class SecretProperties(Properties, TypedDict):
    backend: Literal["meta"]
