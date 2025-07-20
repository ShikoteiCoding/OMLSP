from requester import request

from aiohttp import ClientSession

import asyncio

def parse_request_params(params: dict) -> dict:
    """
    Parse sql parameters to function parameters
    """
    return {
        "url": params["url"],
        "method": params["method"],
        "jsonpath": params["json.jsonpath"]
    }


async def main():

    params = {
        "connector": "http",
        'url': 'https://httpbin.org/get',
        'method': 'GET',
        'scan.interval': '60s',
        'json.jsonpath': '$.url'
    }

    client = ClientSession()

    res = await request(client, **parse_request_params(params))

    await client.close()

    return res

if __name__ == "__main__":
    print("OMLSP starting")
    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(main())
    print(res)