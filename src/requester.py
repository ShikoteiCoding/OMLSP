import asyncio
import jq
import httpx

from aiohttp import ClientSession
from typing import Any, Callable, Coroutine

from loguru import logger
from jsonpath_ng import parse


def parse_http_properties(params: dict[str, str]) -> dict:
    parsed_params = {}
    parsed_params["headers"] = {}
    parsed_params["json"] = {}
    for key, value in params.items():
        if key.startswith("headers."):
            parsed_params["headers"][key.split(".")[1]] = value
        elif key == "jsonpath":
            if value.startswith("$"):
                parsed_params["jsonpath"] = parse(value)
            else:
                parsed_params["jq"] = jq.compile(value)
        elif key.startswith("json."):
            parsed_params["json"][key.split(".")[1]] = value
        else:
            parsed_params[key] = value
    return parsed_params


async def async_request(
    client: ClientSession,
    url: str,
    jsonpath: Any = None,
    jq: Any = None,
    method: str = "GET",
    headers={"Content-Type": "application/json"},
    json={},
    **kwarg,
) -> list[dict]:
    async with client as session:
        async with session.request(
            method=method,
            url=url,
            headers=headers,
            json=json,
        ) as response:
            logger.debug(f"response for {url}: {response.status}")

            if response.ok:
                data = await response.json()
                return parse_response(data, jsonpath, jq)

            # TODO: add retry on fail here
            try:
                response.raise_for_status()
            except Exception as e:
                logger.warning(f"unable to request: {e}")
            return []

        # TODO: handle failure of async task
        return []


def sync_request(
    client: httpx.Client,
    url: str,
    jsonpath: Any = None,
    jq: Any = None,
    method: str = "GET",
    headers: dict = {"Content-Type": "application/json"},
    json: dict = {},
    **kwargs,
) -> list[dict]:
    try:
        response = client.request(method=method, url=url, headers=headers, json=json)
        logger.debug(f"response for {url}: {response.status_code}")

        if response.is_success:
            return parse_response(response.json(), jsonpath, jq)

        # TODO: add retry on fail here
        try:
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"unable to request: {e}")

    except Exception as e:
        logger.error(f"HTTP request failed: {e}")

    # TODO: handle failure of sync task
    return []


def parse_response(data: dict, jsonpath: Any = None, jq: Any = None) -> list[dict]:
    if jsonpath:
        matches = jsonpath.find(data)
        return [{str(match.path): match.value} for match in matches]

    # TODO: new jq parsing, might be deprecating jsonpath
    elif jq:
        res = jq.input(data).all()
        return res

    elif isinstance(data, dict):
        return [data]
    elif isinstance(data, list):
        return data
    else:
        logger.warning(f"No jsonpath provided or invalid response: {data}")
        return []


async def http_requester(properties: dict) -> list[dict]:
    client = ClientSession()
    logger.debug(f"running request with properties: {properties}")
    res = await async_request(client, **properties)
    await client.close()
    return res


def sync_http_requester(properties: dict) -> list[dict]:
    client = httpx.Client()
    logger.debug(f"running request with properties: {properties}")
    return sync_request(client, **properties)


def build_http_requester(
    properties: dict, is_async: bool = True
) -> Callable[[], Coroutine[Any, Any, list[dict]]] | Callable[[], list[dict]]:
    http_properties = parse_http_properties(properties)

    if is_async:

        def _async_inner():
            return http_requester(http_properties)

        return _async_inner

    def _sync_inner():
        return sync_http_requester(http_properties)

    return _sync_inner


if __name__ == "__main__":
    print("OMLSP starting")
    properties = {
        "connector": "http",
        "url": "https://httpbin.org/get",
        "method": "GET",
        "scan.interval": "60s",
        "jsonpath": "$.url",
    }

    _http_requester = build_http_requester(properties, is_async=True)

    res = asyncio.run(_http_requester())  # type: ignore
    print(res)
