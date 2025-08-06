import asyncio
import jq

from aiohttp import ClientSession, ClientResponse
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
            if value.startswith("."):
                parsed_params["jq"] = jq.compile(value)
        elif key.startswith("json."):
            parsed_params["json"][key.split(".")[1]] = value
        else:
            parsed_params[key] = value
    return parsed_params


async def request(
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
                return await parse_response(response, jsonpath, jq)

            # TODO: add retry on fail here
            try:
                response.raise_for_status()
            except Exception as e:
                logger.warning(f"unable to request: {e}")
            return []

        # TODO: handle failure of async task
        return []


async def parse_response(
    response: ClientResponse, jsonpath: Any = None, jq: Any = None
) -> list[dict]:
    data = await response.json()

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
    res = await request(client, **properties)
    await client.close()
    return res


def build_http_requester(
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
        "jsonpath": "$.url",
    }

    _http_requester = build_http_requester(properties)

    res = asyncio.run(_http_requester())
    print(res)
