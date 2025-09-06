import argparse
import asyncio
import json
import time

from duckdb import connect, DuckDBPyConnection
from pathlib import Path
from parser import (
    extract_query_contexts,
    CreateLookupTableContext,
    CreateTableContext,
    SelectContext,
)
from engine import start_background_runnners_or_register
from entrypoint import start_server
from metadata import init_metadata_store

from loguru import logger


async def main():
    # TODO: decipher entrypoint
    parser = argparse.ArgumentParser("Run a SQL file")
    parser.add_argument("file")

    args = parser.parse_args()
    sql_filepath = Path(args.file)
    prop_schema_filepath = Path("src/properties.schema.json")
    sql_content: str

    # TODO: persist on disk
    con: DuckDBPyConnection = connect(database=":memory:")
    init_metadata_store(con)

    with open(sql_filepath, "rb") as fo:
        sql_content = fo.read().decode("utf-8")
    with open(prop_schema_filepath, "rb") as fo:
        properties_schema = json.loads(fo.read().decode("utf-8"))

    contexts = extract_query_contexts(sql_content, properties_schema)

    async with asyncio.TaskGroup() as tg:
        for query_context in contexts:
            if isinstance(
                query_context, (CreateTableContext, CreateLookupTableContext)
            ):
                tg.create_task(
                    start_background_runnners_or_register(query_context, con)
                )

            if isinstance(query_context, SelectContext):
                logger.warning("Ignoring select statement at startup")

        tg.create_task(start_server(con, properties_schema))


if __name__ == "__main__":
    asyncio.run(main())
