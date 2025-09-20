import trio
import jq
import httpx

from typing import Any, Callable, Coroutine

from loguru import logger

MAX_RETRIES = 3


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


async def async_request(
    url: str,
    jq: Any = None,
    method: str = "GET",
    headers={"Content-Type": "application/json"},
    json={},
    **kwarg,
) -> list[dict]:
    attempt = 0
    while attempt < MAX_RETRIES:
        async with httpx.AsyncClient() as client:
            response = await client.request(method, url, headers=headers, json=json)

            logger.debug(f"response for {url}: {response.status_code}")

            if response.is_success:
                data = response.json()
                return parse_response(data, jq)

            logger.warning(f"request failed {url}: {response.status_code}")

        attempt += 1
        if attempt < MAX_RETRIES:
            delay = 2**attempt
            await trio.sleep(delay)

    logger.error(f"request to {url} failed after {retries} attempts")
    return []


def sync_request(
    client: httpx.Client,
    url: str,
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


def parse_response(data: dict, jq: Any = None) -> list[dict]:
    res = jq.input(data).all()
    return res


async def http_requester(properties: dict) -> list[dict]:
    logger.debug(f"running request with properties: {properties}")
    res = await async_request(**properties)
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

    else:

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
        "jq": ".url",
    }

    _http_requester = build_http_requester(properties, is_async=True)

    res = asyncio.run(_http_requester())  # type: ignore
    print(res)
