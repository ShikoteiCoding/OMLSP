import argparse
import json
import trio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from duckdb import connect, DuckDBPyConnection
from pathlib import Path
from runner import Runner
from context import ContextManager
from task import TaskManager
from sql.file import iter_sql_statements


PROPERTIES_SCHEMA = json.loads(
    open(Path("src/properties.schema.json"), "rb").read().decode("utf-8")
)


async def main():
    parser = argparse.ArgumentParser("Run a SQL file")
    parser.add_argument("file")
    args = parser.parse_args()
    sql_filepath = Path(args.file)

    conn: DuckDBPyConnection = connect(database=":memory:")
    scheduler = AsyncIOScheduler()
    context_manager = ContextManager(PROPERTIES_SCHEMA)
    task_manager = TaskManager(conn, scheduler)
    runner = Runner(conn, context_manager, task_manager)

    await runner.build()
    async with trio.open_nursery() as nursery:
        nursery.start_soon(runner.run)
        for sql in iter_sql_statements(sql_filepath):
            await runner.submit(sql)


# Keep before full migration
# async def main_deprec():
#     # TODO: decipher entrypoint
#     parser = argparse.ArgumentParser("Run a SQL file")
#     parser.add_argument("file")
#
#     args = parser.parse_args()
#     sql_filepath = Path(args.file)
#     prop_schema_filepath = Path("src/properties.schema.json")
#     sql_content: str
#
#     # TODO: persist on disk
#     con: DuckDBPyConnection = connect(database=":memory:")
#     init_metadata_store(con)
#
#     with open(sql_filepath, "rb") as fo:
#         sql_content = fo.read().decode("utf-8")
#     with open(prop_schema_filepath, "rb") as fo:
#         properties_schema = json.loads(fo.read().decode("utf-8"))
#
#     contexts = extract_query_contexts(sql_content, properties_schema)
#
#     async with asyncio.TaskGroup() as tg:
#         for query_context in contexts:
#             if isinstance(
#                 query_context, (CreateTableContext, CreateLookupTableContext)
#             ):
#                 tg.create_task(
#                     start_background_runnners_or_register(query_context, con)
#                 )
#
#             if isinstance(query_context, SelectContext):
#                 logger.warning("Ignoring select statement at startup")
#
#         tg.create_task(start_server(con, properties_schema))


if __name__ == "__main__":
    trio.run(main)
