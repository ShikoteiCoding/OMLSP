from aiohttp import ClientSession, ClientResponse
from typing import Any, Coroutine

from loguru import logger

import asyncio


async def request(
    client: ClientSession, url: str, jsonpath: str, method: str = "GET", **kwarg
) -> Coroutine[Any, Any, Any] | None:
    async with client as session:
        async with session.request(
            method=method, url=url, headers={"Content-Type": "application/json"}
        ) as response:
            logger.debug(f"Response for {url}: {response.status}")

            if response.ok:
                return await parse_request(response, jsonpath)
            else:  # TODO: add retry on fail here
                ...

            # TODO: handle failure of async task


async def parse_request(
    response: ClientResponse, jsonpath: str
) -> Coroutine[Any, Any, Any]:
    return await response.json()


if __name__ == "__main__":
    print("OMLSP starting")

    def parse_request_params(params: dict) -> dict:
        return {
            "url": params["url"],
            "method": params["method"],
            "jsonpath": params["json.jsonpath"],
        }

    async def main():
        params = {
            "connector": "http",
            "url": "https://httpbin.org/get",
            "method": "GET",
            "scan.interval": "60s",
            "json.jsonpath": "$.url",
        }

        client = ClientSession()

        res = await request(client, **parse_request_params(params))

        await client.close()
        return res

    res = asyncio.run(main())
    print(res)
