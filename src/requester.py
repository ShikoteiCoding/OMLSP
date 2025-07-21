
from aiohttp import ClientSession, ClientResponse
from typing import Any, Coroutine

from loguru import logger

async def request(client: ClientSession, url: str, jsonpath: str, method: str = 'GET', **kwarg) -> Coroutine[Any, Any, Any] | None:
    async with client as session:
        async with session.request(
            method=method,
            url=url,
            headers={"Content-Type": "application/json"}
        ) as response:
            logger.debug(f"Status: {response.status}")
            logger.debug("Content-type", {response.headers['content-type']})

            if response.ok:
                return await parse_request(response, jsonpath)
            else: # TODO: add retry on fail here
                ...

            # TODO: handle failure of async task


async def parse_request(response: ClientResponse, jsonpath: str) -> Coroutine[Any, Any, Any]:
    return await response.json()