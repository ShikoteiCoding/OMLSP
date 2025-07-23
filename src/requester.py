from aiohttp import ClientSession, ClientResponse
from typing import Any, Callable, Coroutine

from loguru import logger

import asyncio
from jsonpath_ng import parse


def get_jsonpath_expression(jsonpath: str):
    return parse(jsonpath)


def parse_http_properties(params: dict) -> dict:
    return {
        "url": params["url"],
        "method": params["method"],
        "jsonpath_expr": get_jsonpath_expression(params["json.jsonpath"]),
    }


async def request(
    client: ClientSession, url: str, jsonpath_expr: str, method: str = "GET", **kwarg
) -> list[dict]:
    async with client as session:
        async with session.request(
            method=method, url=url, headers={"Content-Type": "application/json"}
        ) as response:
            logger.debug(f"response for {url}: {response.status}")

            if response.ok:
                return await parse_request(response, jsonpath_expr)

            # TODO: add retry on fail here
            try:
                response.raise_for_status()
            except Exception as e:
                logger.warning(f"unable to request: {e}")
            return []

        # TODO: handle failure of async task
        return []


async def parse_request(
    response: ClientResponse, jsonpath_expr: Any | None
) -> list[dict]:
    data = await response.json()
    if jsonpath_expr:
        matches = jsonpath_expr.find(data)
        return [{str(match.path): match.value} for match in matches]
    elif isinstance(data, dict):
        return [data]
    elif isinstance(data, list):
        return data
    else:
        logger.warning(f"No jsonpath provided or invalid response: {data}")
        return []


async def http_requester(properties: dict) -> list[dict]:
    client = ClientSession()
    res = await request(client, **properties)
    await client.close()
    return res


def http_requester_builder(
    properties: dict,
) -> Callable[[], Coroutine[Any, Any, list[dict]]]:
    http_properties = parse_http_properties(properties)

    def _inner():
        return http_requester(http_properties)

    return _inner


if __name__ == "__main__":
    print("OMLSP starting")
    properties = {
        "connector": "http",
        "url": "https://httpbin.org/get",
        "method": "GET",
        "scan.interval": "60s",
        "json.jsonpath": "$.url",
    }

    _http_requester = http_requester_builder(properties)

    res = asyncio.run(_http_requester())
    print(res)
