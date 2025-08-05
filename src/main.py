import argparse
import asyncio
import json

from duckdb import connect, DuckDBPyConnection
from pathlib import Path
from parser import parse_sql_statements
from engine import run_executables

if __name__ == "__main__":
    # TODO: decipher entrypoint
    parser = argparse.ArgumentParser("Run a SQL file")
    parser.add_argument("file")

    args = parser.parse_args()
    sql_filepath = Path(args.file)
    prop_schema_filepath = Path("src/properties.schema.json")
    sql_content: str

    # TODO: persist on disk
    con: DuckDBPyConnection = connect(database=":memory:")

    with open(sql_filepath, "rb") as fo:
        sql_content = fo.read().decode("utf-8")
    with open(prop_schema_filepath, "rb") as fo:
        properties_schema = json.loads(fo.read().decode("utf-8"))

    tables, select = parse_sql_statements(sql_content, properties_schema)

    asyncio.run(run_executables(tables, con))
