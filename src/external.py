import trio
import jq
import httpx
import json
from trio_websocket import open_websocket_url

from functools import partial
from typing import Any, Callable, Coroutine, AsyncGenerator

from loguru import logger

MAX_RETRIES = 5


def parse_http_properties(params: dict[str, str]) -> dict:
    parsed_params = {}
    parsed_params["headers"] = {}
    parsed_params["json"] = {}
    for key, value in params.items():
        if key.startswith("headers."):
            parsed_params["headers"][key.split(".")[1]] = value
        elif key == "jq":
            parsed_params["jq"] = jq.compile(value)
        elif key.startswith("json."):
            parsed_params["json"][key.split(".")[1]] = value
        else:
            parsed_params[key] = value
    return parsed_params


def parse_ws_properties(params: dict[str, str]) -> dict[str, Any]:
    parsed_params = {}

    for key, value in params.items():
        if key == "jq":
            parsed_params["jq"] = jq.compile(value)
        else:
            parsed_params[key] = value

    return parsed_params


async def async_request(
    client: httpx.AsyncClient,
    url: str,
    jq: Any = None,
    method: str = "GET",
    headers={"Content-Type": "application/json"},
    json={},
    **kwarg,
) -> list[dict[str, Any]]:
    attempt = 0
    while attempt < MAX_RETRIES:
        response = await client.request(method, url, headers=headers, json=json)

        logger.debug(f"response for {url}: {response.status_code}")

        if response.is_success:
            data = response.json()
            return parse_response(data, jq)

        logger.error(f"request failed {url}: {response.status_code}")

    attempt += 1
    if attempt < MAX_RETRIES:
        delay = attempt
        await trio.sleep(delay)

    logger.error(f"request to {url} failed after {MAX_RETRIES} attempts")
    return []


def sync_request(
    client: httpx.Client,
    url: str,
    jq: Any = None,
    method: str = "GET",
    headers: dict = {"Content-Type": "application/json"},
    json: dict = {},
    **kwargs,
) -> list[dict[str, Any]]:
    try:
        response = client.request(method=method, url=url, headers=headers, json=json)
        logger.debug(f"response for {url}: {response.status_code}")

        if response.is_success:
            return parse_response(response.json(), jq)

        # TODO: add retry on fail here
        try:
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"unable to request: {e}")

    except Exception as e:
        logger.error(f"HTTP request failed: {e}")

    # TODO: handle failure of sync task
    return []


def parse_response(data: dict[str, Any], jq: Any = None) -> list[dict[str, Any]]:
    res = jq.input(data).all()
    return res


async def http_requester(properties: dict[str, Any]) -> list[dict[str, Any]]:
    async with httpx.AsyncClient() as client:
        logger.debug(f"running request with properties: {properties}")
        res = await async_request(client, **properties)
        return res


async def ws_generator(
    properties: dict[str, Any],
) -> AsyncGenerator[Any, list[dict[str, Any]]]:
    async with open_websocket_url(properties["url"]) as ws:
        message = await ws.get_message()
        res = json.loads(message)
        yield parse_response(res, properties["jq"])


def sync_http_requester(properties: dict) -> list[dict[str, Any]]:
    client = httpx.Client()
    logger.debug(f"running request with properties: {properties}")
    return sync_request(client, **properties)


def build_http_requester(
    properties: dict[str, Any], is_async: bool = True
) -> (
    Callable[[], Coroutine[Any, Any, list[dict[str, Any]]]]
    | Callable[[], list[dict[str, Any]]]
):
    http_properties = parse_http_properties(properties)

    if is_async:

        def _async_inner():
            return http_requester(http_properties)

        return _async_inner

    def _sync_inner():
        return sync_http_requester(http_properties)

    return _sync_inner


def build_ws_generator(
    properties: dict[str, Any],
) -> Callable[[], AsyncGenerator[Any, list[dict[str, Any]]]]:
    return partial(ws_generator, properties=parse_ws_properties(properties))


if __name__ == "__main__":
    print("OMLSP starting")

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
