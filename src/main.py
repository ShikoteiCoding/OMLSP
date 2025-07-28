import argparse
import asyncio
import duckdb

from pathlib import Path
from parser import parse_query_to_dict
from engine import run_executables

if __name__ == "__main__":
    # TODO: decipher entrypoint
    parser = argparse.ArgumentParser("Run a SQL file")
    parser.add_argument("file")

    args = parser.parse_args()
    filepath = Path(args.file)
    sql_content: str

    # TODO: persist on disk
    con = duckdb.connect(database=":memory:")

    with open(filepath, "rb") as fo:
        sql_content = fo.read().decode("utf-8")

    parsed_queries = parse_query_to_dict(sql_content)

    asyncio.run(run_executables(parsed_queries, con))
