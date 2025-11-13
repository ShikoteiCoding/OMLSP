from __future__ import annotations

import trio
import httpx
import time
from duckdb import DuckDBPyConnection
from typing import Any

from auth.types import BaseSignerT
from transport.utils import jq_dict
from transport.pagination import BasePagination
from loguru import logger

from sql.types import SourceHttpProperties, JQ


MAX_RETRIES = 5


def extract_http_properties(
    properties: SourceHttpProperties,
) -> tuple[JQ, dict[str, Any], dict[str, Any]]:
    """Parse property dict provided by context and get required http parameters."""
    # Build kwargs dict compatible with both httpx or requests
    # leverage dict update to avoid empty values
    requests_kwargs: dict[str, Any] = (
        {
            "url": properties.url,
            "method": properties.method,
        }
        | ({"headers": properties.headers} if properties.headers else {})
        | ({"json": properties.json} if properties.json else {})
        | ({"params": properties.params} if properties.params else {})
    )

    # Handle pagination parameters
    pagination_kwargs = {"type": "no-pagination"} | properties.pagination

    return properties.jq, requests_kwargs, pagination_kwargs


async def async_request(
    client: httpx.AsyncClient,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> httpx.Response | None:
    """Retry async HTTP request with exponential backoff."""
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            response = await client.request(**signer.sign(conn, request_kwargs))
            if response.is_success:
                logger.debug(f"{response.status_code}: response for {request_kwargs}.")
                return response
        except Exception as e:
            logger.warning(f"Async request failed (attempt {attempt + 1}): {e}")

        attempt += 1
        await trio.sleep(2**attempt)

    logger.error(f"request to {request_kwargs} failed after {MAX_RETRIES} attempts")
    return None


def sync_request(
    client: httpx.Client,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> httpx.Response | None:
    """Retry sync HTTP request with exponential backoff."""
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            response = client.request(**signer.sign(conn, request_kwargs))
            if response.is_success:
                logger.debug(f"{response.status_code}: response for {request_kwargs}.")
                return response
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"Sync request failed (attempt {attempt + 1}): {e}")

        attempt += 1
        time.sleep(2**attempt)

    logger.error(f"request to {request_kwargs} failed after {MAX_RETRIES} attempts")
    return None


async def async_http_requester(
    jq,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    pagination_strategy: BasePagination,
    conn: DuckDBPyConnection,
) -> list[dict]:
    async with httpx.AsyncClient() as client:
        logger.debug(f"running async request with {request_kwargs}")
        results, cursor = [], None
        base_params: dict = request_kwargs.get("params", {}).copy()

        while True:
            params = pagination_strategy.update_params(cursor, base_params.copy())
            req = {**request_kwargs, "params": params}
            if req["params"] == {}:
                req.pop("params")

            response = await async_request(client, signer, req, conn)
            if not response:
                break

            batch = jq_dict(response.json(), jq)
            if not batch:
                break

            results.extend(batch)
            if len(batch) == 0:
                break

            if pagination_strategy.has_cursor():
                cursor = pagination_strategy.extract_next_cursor(batch, response)
                if cursor is None:
                    break

        return results


def sync_http_requester(
    jq,
    base_signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    pagination_strategy: BasePagination,
    conn: DuckDBPyConnection,
) -> list[dict]:
    with httpx.Client() as client:
        logger.debug(f"running sync request with {request_kwargs}")
        results, cursor = [], None
        base_params = request_kwargs.get("params", {}).copy()
        limit = int(base_params.get("limit", 100))

        while True:
            params = pagination_strategy.update_params(cursor, base_params.copy())
            req = {**request_kwargs, "params": params}
            if req["params"] == {}:
                req.pop("params")

            response = sync_request(client, base_signer, req, conn)
            if not response:
                break

            batch = jq_dict(response.json(), jq)
            if not batch:
                break

            results.extend(batch)
            if len(batch) < limit:
                break

            if pagination_strategy.has_cursor():
                cursor = pagination_strategy.extract_next_cursor(batch, response)
                if cursor is None:
                    break

        return results
