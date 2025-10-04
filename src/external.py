import trio
import jq as jqm
import httpx
import json

from duckdb import DuckDBPyConnection
from functools import partial
from trio_websocket import open_websocket_url
from typing import Any, Callable, Coroutine, AsyncGenerator

from auth import AUTH_DISPATCHER, BaseSignerT, SecretsHandler

from loguru import logger

MAX_RETRIES = 5


def parse_http_properties(
    properties: dict[str, str],
) -> tuple[dict[str, Any], BaseSignerT, dict[str, Any]]:
    """
    Parse property dict provided by context and get required http parameters.
    """
    # Build kwargs dict compatible with both httpx or requests
    requests_kwargs = {}
    requests_kwargs["headers"] = {}
    requests_kwargs["json"] = {}

    # Manually extract standard values
    requests_kwargs["url"] = properties["url"]
    requests_kwargs["method"] = properties["method"]
    jq = jqm.compile(properties["jq"])
    secrets_handler = SecretsHandler()
    signer_class = AUTH_DISPATCHER[requests_kwargs.get("signer_class", "NoSigner")](
        secrets_handler
    )

    # Build "dict" kwargs dynamically
    # TODO: improve with defaultdict(dict)
    for key, value in properties.items():
        # Handle headers, will always be a dict
        if key.startswith("headers."):
            subkey = key.split(".")[1]

            # TODO: Implement more robust link between executable and SECRET.
            if "SECRET" in value:
                # Get secret_name from value which should respect format:
                # SECRET secret_name
                value = value.split(" ")[1]

                secrets_handler.add(subkey, value)

            requests_kwargs["headers"][subkey] = value

        # Handle json, will always be a dict
        elif key.startswith("json."):
            requests_kwargs["json"][key.split(".")[1]] = value
        # TODO: handle bytes, form and url params

    # Some API don't like empty json / headers fields
    if requests_kwargs["headers"] == {}:
        requests_kwargs.pop("headers")
    if requests_kwargs["json"] == {}:
        requests_kwargs.pop("json")

    return jq, signer_class, requests_kwargs


def parse_ws_properties(params: dict[str, str]) -> dict[str, Any]:
    parsed_params = {}

    for key, value in params.items():
        if key == "jq":
            parsed_params["jq"] = jqm.compile(value)
        else:
            parsed_params[key] = value

    return parsed_params


async def async_request(
    client: httpx.AsyncClient,
    jq: Any,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    attempt = 0
    while attempt < MAX_RETRIES:
        response = await client.request(**signer.sign(conn, request_kwargs))

        try:
            if response.is_success:
                logger.debug(f"{response.status_code}: response for {request_kwargs}.")
                data = response.json()
                return parse_response(data, jq)
        except Exception:
            pass

        logger.error(
            f"{response.status_code}: request failed {request_kwargs} with {response.text}."
        )

        attempt += 1
        if attempt < MAX_RETRIES:
            delay = attempt
            await trio.sleep(delay)

    logger.error(f"request to {request_kwargs} failed after {MAX_RETRIES} attempts")
    return []


def sync_request(
    client: httpx.Client,
    jq: Any,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    try:
        response = client.request(**signer.sign(conn, request_kwargs))
        logger.debug(f"response for {request_kwargs}: {response.status_code}")

        if response.is_success:
            return parse_response(response.json(), jq)

        try:
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"unable to request: {e}")

    except Exception as e:
        logger.error(f"HTTP request failed: {e}")

    return []


def parse_response(data: dict[str, Any], jq: Any = None) -> list[dict[str, Any]]:
    res = jq.input(data).all()
    return res


async def async_http_requester(
    jq,
    base_signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    async with httpx.AsyncClient() as client:
        logger.debug(f"running request with properties: {request_kwargs}")
        res = await async_request(client, jq, base_signer, request_kwargs, conn)
        return res


async def ws_generator(
    properties: dict[str, Any],
) -> AsyncGenerator[Any, list[dict[str, Any]]]:
    async with open_websocket_url(properties["url"]) as ws:
        message = await ws.get_message()
        res = json.loads(message)
        yield parse_response(res, properties["jq"])


def sync_http_requester(
    jq,
    base_signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    client = httpx.Client()
    logger.debug(f"running request with properties: {request_kwargs}")
    return sync_request(client, jq, base_signer, request_kwargs, conn)


def build_http_requester(
    properties: dict[str, Any], is_async: bool = True
) -> (
    Callable[[DuckDBPyConnection], Coroutine[Any, Any, list[dict]]]
    | Callable[[DuckDBPyConnection], list[dict]]
):
    jq, base_signer, request_kwargs = parse_http_properties(properties)

    if is_async:

        def _async_inner(conn):
            return async_http_requester(jq, base_signer, request_kwargs, conn)

        return _async_inner

    def _sync_inner(conn):
        return sync_http_requester(jq, base_signer, request_kwargs, conn)

    return _sync_inner


def build_ws_generator(
    properties: dict[str, Any],
) -> Callable[[], AsyncGenerator[Any, list[dict[str, Any]]]]:
    return partial(ws_generator, properties=parse_ws_properties(properties))


if __name__ == "__main__":
    print("OMLSP starting")
    from duckdb import connect

    conn: DuckDBPyConnection = connect(database=":memory:")

    async def main():
        properties = {
            "connector": "http",
            "url": "https://httpbin.org/get",
            "method": "GET",
            "scan.interval": "60s",
            "jq": ".url",
        }

        _http_requester = build_http_requester(properties, is_async=True)

        res = await _http_requester()  # type: ignore
        logger.info(res)

    trio.run(main)
