# file cannot be named io
import asyncio
import os
import pickle

from pathlib import Path
from loguru import logger


async def persist(
    records: list[dict], txn_id: int, epoch: int, store_location: str, table_name: str
) -> str:
    path = Path(f"{store_location}/{table_name}/")

    # TODO: move initialisation when creating table
    if not path.exists():
        path.mkdir()

    file_name = f"{epoch}-{txn_id}.pkl"
    with open(path / file_name, "wb") as f:
        pickle.dump(records, f, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info(
            f"{epoch} - {table_name} - batch {txn_id} - persisted at {path / file_name}"
        )
    return str(path / file_name)


async def read_all(store_location: str, table_name: str) -> list[dict] | None:
    path = Path(f"{store_location}/{table_name}/")
    files = os.listdir(path)
    pickle_files = [f for f in files if f.endswith(".pkl")]
    data = []
    for filename in pickle_files:
        with open(path / filename, "rb") as fo:
            data.extend(pickle.load(fo))
    return data


if __name__ == "__main__":
    import time

    store_location = "local-store"
    table_name = "example"
    records = [{"url": "https://httpbin.org/get"} for _ in range(10)]
    epoch = int(time.time() * 1_000)

    async def _test():
        _ = await persist(records, 0, epoch, store_location, table_name)
        data = await read_all(store_location, table_name)
        print(data)

    asyncio.run(_test())
