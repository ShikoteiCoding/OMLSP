import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from functools import partial
from string import Template
from types import FunctionType
from typing import Any, Iterable

import polars as pl
import pyarrow as pa
from duckdb import DuckDBPyConnection, struct_type
from duckdb.functional import FunctionNullHandling, PythonUDFType
from duckdb.typing import VARCHAR, DuckDBPyType
from loguru import logger

from context.context import (
    CreateLookupTableContext,
    SelectContext,
    SourceTaskContext,
)
from inout import persist
from metadata import (
    create_macro_definition,
    get_batch_id_from_table_metadata,
    get_lookup_tables,
    get_macro_definition_by_name,
    update_batch_id_in_table_metadata,
)
from requester import build_http_requester

DUCKDB_TO_PYARROW_PYTYPE = {
    "VARCHAR": pa.string(),
    "TEXT": pa.string(),
    "TIMESTAMP_S": pa.timestamp("s"),
    "TIMESTAMP_MS": pa.timestamp("ms"),
    "TIMESTAMP": pa.timestamp("us"),
    "TIMESTAMP_NS": pa.timestamp("ns"),
    "DATETIME": pa.timestamp("us"),
    "FLOAT": pa.float32(),
    "DOUBLE": pa.float64(),
    "INTEGER": pa.int32(),
    "INT": pa.int32(),
    "BIGINT": pa.int64(),
    "SMALLINT": pa.int16(),
    "TINYINT": pa.int8(),
    "BOOLEAN": pa.bool_(),
    "DATE": pa.date32(),
    "DECIMAL": pa.decimal128(18, 2),
}


def build_lookup_properties(
    properties: dict[str, str], context: dict[str, Any]
) -> dict[str, str]:
    new_props = {}

    for key, value in properties.items():
        if key in ["method"]:  # TODO: ignore jq for now
            new_props[key] = value
            continue
        template = Template(value)
        new_props[key] = template.substitute(context)

    return new_props


def build_scalar_udf(
    properties: dict[str, str],
    dynamic_columns: list[str],
    return_type: DuckDBPyType,
    **kwargs,
) -> dict[str, Any]:
    arity = len(dynamic_columns)

    child_types = []
    for subtype in return_type.children:
        child_types.append(
            pa.field(subtype[0], DUCKDB_TO_PYARROW_PYTYPE[str(subtype[1])])
        )
    return_type_arrow = pa.struct(child_types)

    arg_names = [f"a{i}" for i in range(1, arity + 1)]

    def core_udf(*arrays: pa.ChunkedArray) -> pa.Array:
        results = []
        for chunks in zip(*(arr.chunks for arr in arrays)):
            py_chunks = [chunk.to_pylist() for chunk in chunks]
            chunk_rows = zip(*py_chunks)
            chunk_results = process_elements(chunk_rows, properties)
            results.extend(chunk_results)
        return pa.array(results, type=return_type_arrow)

    udf = eval(
        f"lambda {', '.join(arg_names)}: core_udf({', '.join(arg_names)})",
        {"core_udf": core_udf},
    )

    def process_elements(
        rows: Iterable[tuple[Any, ...]],
        properties: dict[str, str],
    ) -> pa.Array:
        def _inner(row: tuple[Any, ...]) -> dict[str, Any]:
            context = dict(zip(dynamic_columns, row))
            lookup_properties = build_lookup_properties(properties, context)
            default_response = context.copy()
            requester = build_http_requester(lookup_properties, is_async=False)

            try:
                response = requester()
                if isinstance(response, dict):
                    return context | response
                if isinstance(response, list) and len(response) >= 1:
                    default_response |= response[-1]
            except Exception as e:
                logger.exception(f"HTTP request failed: {e}")

            return default_response

        with ThreadPoolExecutor() as executor:
            results = list(executor.map(_inner, rows))

        return results

    return {
        "name": kwargs.get("name"),
        "function": udf,
        "parameters": [VARCHAR for _ in range(arity)],
        "return_type": return_type,
        "type": PythonUDFType.ARROW,
        "null_handling": FunctionNullHandling.SPECIAL,
        **kwargs,
    }


async def source_executable(
    task_id: str,
    conn: DuckDBPyConnection,
    table_name: str,
    start_time: datetime,
    http_requester: FunctionType,
    *args,
    **kwargs,
) -> pl.DataFrame:
    # TODO: no provided api execution_time
    # using trigger.get_next_fire_time is costly (see code)
    execution_time = start_time
    batch_id = get_batch_id_from_table_metadata(conn, table_name)
    logger.info(f"[{table_name}{{{batch_id}}}] / @ {execution_time}")

    records = await http_requester()
    logger.debug(
        f"[{table_name}{{{batch_id}}}] - http number of responses: {len(records)} - batch {batch_id}"
    )

    if len(records) > 0:
        epoch = int(time.time() * 1_000)
        # TODO: type polars with duckdb table catalog
        df = pl.from_records(records)
        await persist(df, batch_id, epoch, table_name, conn)
    else:
        df = pl.DataFrame()

    update_batch_id_in_table_metadata(conn, table_name, batch_id + 1)
    return df


def build_source_executable(ctx: SourceTaskContext):
    start_time = datetime.now(timezone.utc)
    properties = ctx.properties
    return partial(
        source_executable,
        table_name=ctx.name,
        start_time=start_time,
        http_requester=build_http_requester(properties, is_async=True),
    )


def build_lookup_table_prehook(
    create_table_context: CreateLookupTableContext, connection: DuckDBPyConnection
) -> str:
    properties = create_table_context.properties
    table_name = create_table_context.name
    dynamic_columns = create_table_context.dynamic_columns
    columns = create_table_context.columns

    func_name = f"{table_name}_func"
    macro_name = f"{table_name}_macro"
    # TODO: handle other return than dict (for instance array http responses)
    return_type = struct_type(columns)  # typed struct from sql statement
    udf_params = build_scalar_udf(
        properties, dynamic_columns, return_type, name=func_name
    )
    # register scalar for row to row http call
    connection.create_function(**udf_params)
    logger.debug(f"registered function: {func_name}")

    # TODO: wrap SQL in function
    # register macro (to be injected in place of sql)
    __inner_tbl = "__inner_tbl"
    __deriv_tbl = "__deriv_tbl"
    func_def = (
        f"{func_name}({','.join([f'{__inner_tbl}.{col}' for col in dynamic_columns])})"
    )
    macro_def = f"{macro_name}(table_name, {','.join(dynamic_columns)})"
    output_cols = ", ".join(
        [f"struct.{col_name} AS {col_name}" for col_name, _ in columns.items()]
    )

    # TODO: move to metadata func
    connection.sql(f"""
        CREATE OR REPLACE MACRO {macro_def} AS TABLE
        SELECT
            {output_cols}
        FROM (
            SELECT
                {func_def} AS struct
            FROM query_table(table_name) AS {__inner_tbl}
        ) AS {__deriv_tbl};
    """)
    logger.debug(f"registered macro: {macro_name}")
    create_macro_definition(connection, macro_name, dynamic_columns)

    return macro_name


def build_substitute_macro_definition(
    con: DuckDBPyConnection,
    join_table: str,
    from_table: str,
    from_table_or_alias: str,
    join_table_or_alias: str,
) -> str:
    macro_name, fields = get_macro_definition_by_name(con, f"{join_table}_macro")
    scalar_func_fields = ",".join(
        [f"{from_table_or_alias}.{field}" for field in fields]
    )
    macro_definition = f'{macro_name}("{from_table}", {scalar_func_fields})'
    if join_table_or_alias == join_table:
        # TODO: fix bug, if table_name == alias
        # JOIN ohlc AS ohlc
        # -> this writes the AS statements and fails
        macro_definition += f"AS {join_table_or_alias}"
    return macro_definition


def pre_hook_select_statements(
    con: DuckDBPyConnection,
    ctx: SelectContext,
    tables: list[str],
) -> str:
    """Substitutes select statement query with lookup references to macro references."""
    original_query = ctx.query
    join_tables = ctx.joins
    lookup_tables = get_lookup_tables(con)

    # join query
    substitute_mapping = dict(zip(tables, tables))
    from_table = ctx.table
    from_table_or_alias = ctx.alias

    # for table in join and in lookup,
    # build the placeholder template mapping
    for join_table, join_table_or_alias in join_tables.items():
        if join_table in lookup_tables:
            substitute_mapping[join_table] = build_substitute_macro_definition(
                con, join_table, from_table, from_table_or_alias, join_table_or_alias
            )
        else:
            substitute_mapping[join_table] = join_table

    # Substritute lookup table placeholder with template
    query = Template(original_query).substitute(substitute_mapping)

    logger.debug(f"New overwritten select statement: {query}")
    return query


def duckdb_to_pl(con: DuckDBPyConnection, duckdb_sql: str) -> pl.DataFrame:
    query = duckdb_sql.strip()
    cursor = con.execute(query)
    rows = cursor.fetchall()
    # TODO: get schema from metadata
    if cursor.description:
        columns = [desc[0] for desc in cursor.description]
        df = pl.DataFrame(rows, orient="row", schema=columns)
        return df

    return pl.DataFrame()


if __name__ == "__main__":
    from duckdb import connect

    con: DuckDBPyConnection = connect(database=":memory:")

    example_fields = ["symbol"]
    example_table = "all_tickers"
    macro_name = "ohlc_macro"
    result = con.sql(
        f"SELECT * FROM {macro_name}({example_table}, {','.join(example_fields)})"
    )
    print(result.df())
