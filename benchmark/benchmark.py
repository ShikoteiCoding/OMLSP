import time
import random
import pyarrow as pa
import duckdb
import polars as pl
import psutil, os
from collections import defaultdict


def get_mem_mb():
    """Return current process memory (RSS) in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1e6


def run_benchmark(n_batches, batch_size, warmup=True):
    con = duckdb.connect()

    # accumulate timings (s) + memory (MB)
    build_arrow, query_arrow, mem_arrow = 0.0, 0.0, 0.0
    build_polars, query_polars, mem_polars = 0.0, 0.0, 0.0
    build_polars_duck, query_polars_duck, mem_polars_duck = 0.0, 0.0, 0.0
    build_python, query_python, mem_python = 0.0, 0.0, 0.0

    # transforms
    def transform_pl(df: pl.DataFrame | pl.LazyFrame):
        if isinstance(df, pl.LazyFrame):
            # LazyFrame: compute with collect()
            return df.group_by("group").agg(pl.col("value").mean()).collect()
        else:
            # Eager DataFrame: agg() already computes eagerly
            return df.group_by("group").agg(pl.col("value").mean())


    def transform_duck(con, name: str):
        return con.execute(
            f'SELECT "group", avg(value) FROM {name} GROUP BY "group"'
        ).df()


    def transform_python(records: list[dict]):
        sums = defaultdict(float)
        counts = defaultdict(int)
        for rec in records:
            g = rec["group"]
            v = rec["value"]
            sums[g] += v
            counts[g] += 1
        return {g: sums[g] / counts[g] for g in sums}

    # warm-up
    if warmup:
        arr = list(range(batch_size))
        grp = [random.randint(0, 5) for _ in range(batch_size)]
        val = [random.random() for _ in range(batch_size)]

        arrow_table = pa.table({"id": arr, "group": grp, "value": val})
        pl_df = pl.DataFrame({"id": arr, "group": grp, "value": val})

        con.register("arrow_table", arrow_table)
        transform_duck(con, "arrow_table")
        transform_pl(pl_df)
        con.register("pl_df", pl_df.to_arrow())
        transform_duck(con, "pl_df")

    # benchmark loop
    for _ in range(n_batches):
        arr = list(range(batch_size))
        grp = [random.randint(0, 5) for _ in range(batch_size)]
        val = [random.random() for _ in range(batch_size)]

        # Arrow + DuckDB
        mem_before = get_mem_mb()
        start = time.perf_counter()
        arrow_table = pa.table({"id": arr, "group": grp, "value": val})
        build_arrow += time.perf_counter() - start
        con.register("arrow_table", arrow_table)
        start = time.perf_counter()
        transform_duck(con, "arrow_table")
        query_arrow += time.perf_counter() - start
        mem_arrow += (get_mem_mb() - mem_before)

        # Polars
        mem_before = get_mem_mb()
        start = time.perf_counter()
        pl_df = pl.DataFrame({"id": arr, "group": grp, "value": val})
        build_polars += time.perf_counter() - start
        start = time.perf_counter()
        transform_pl(pl_df)
        query_polars += time.perf_counter() - start
        mem_polars += (get_mem_mb() - mem_before)

        # Polars + DuckDB
        mem_before = get_mem_mb()
        start = time.perf_counter()
        arrow_from_pl = pl_df.to_arrow()
        con.register("pl_df", arrow_from_pl)
        build_polars_duck += time.perf_counter() - start
        start = time.perf_counter()
        transform_duck(con, "pl_df")
        query_polars_duck += time.perf_counter() - start
        mem_polars_duck += (get_mem_mb() - mem_before)

        # Plain Python
        mem_before = get_mem_mb()
        start = time.perf_counter()
        # Convert Polars → list of dicts (this is the "build" step)
        py_records = pl_df.to_dicts()
        build_python += time.perf_counter() - start

        start = time.perf_counter()
        transform_python(py_records)
        query_python += time.perf_counter() - start
        mem_python += (get_mem_mb() - mem_before)


    total_records = n_batches * batch_size

    return {
        "batches": n_batches,
        "batch_size": batch_size,
        "records": total_records,

        "build_arrow_ms": (build_arrow / n_batches) * 1000,
        "query_arrow_ms": (query_arrow / n_batches) * 1000,
        "arrow_rps": total_records / (build_arrow + query_arrow),
        "arrow_mem_mb": mem_arrow / n_batches,

        "build_polars_ms": (build_polars / n_batches) * 1000,
        "query_polars_ms": (query_polars / n_batches) * 1000,
        "polars_rps": total_records / (build_polars + query_polars),
        "polars_mem_mb": mem_polars / n_batches,

        "build_polars_duck_ms": (build_polars_duck / n_batches) * 1000,
        "query_polars_duck_ms": (query_polars_duck / n_batches) * 1000,
        "polars_duck_rps": total_records / (build_polars_duck + query_polars_duck),
        "polars_duck_mem_mb": mem_polars_duck / n_batches,

        "build_python_ms": (build_python / n_batches) * 1000,
        "query_python_ms": (query_python / n_batches) * 1000,
        "python_rps": total_records / (build_python + query_python),
        "python_mem_mb": mem_python / n_batches,
    }


if __name__ == "__main__":
    res1 = run_benchmark(10_000, 1)
    res2 = run_benchmark(1_000, 10)
    res3 = run_benchmark(100, 100)
    res4 = run_benchmark(10_000, 10)

    import pandas as pd
    df_results = pd.DataFrame([res1, res2, res3, res4])
    for i, row in df_results.iterrows():
        print(f"\n--- Result {i+1} ---")
        print(row.to_string())
