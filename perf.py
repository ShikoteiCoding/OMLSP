import timeit

N = 10_000
CHUNK_SIZE = 1

setup_common = f"""
import msgspec
import pyarrow as pa
import polars as pl
import numpy as np
import io

N = {N}
CHUNK_SIZE = {CHUNK_SIZE}

class User(msgspec.Struct):
    id: int
    name: str
    active: bool
"""

# --- Benchmark 1: msgspec.Struct instantiation only ---
code_struct_create = "users = [User(i, 'John', True) for i in range(N)]"

# --- Benchmark 2: dict instantiation only ---
code_dict_create = "users = [{'id': i, 'name': 'John', 'active': True} for i in range(N)]"

# --- Benchmark 3: msgspec.Struct → PyArrow (list-based, chunked) ---
code_struct_to_arrow_list_chunked = """
tables = []
for start in range(0, N, CHUNK_SIZE):
    end = min(start + CHUNK_SIZE, N)
    users = [User(i, 'John', True) for i in range(start, end)]
    table = pa.table({
        'id': [u.id for u in users],
        'name': [u.name for u in users],
        'active': [u.active for u in users],
    })
    tables.append(table)
table = pa.concat_tables(tables)
"""

# --- Benchmark 4: dict → PyArrow (list-based, chunked) ---
code_dict_to_arrow_list_chunked = """
tables = []
for start in range(0, N, CHUNK_SIZE):
    end = min(start + CHUNK_SIZE, N)
    users = [{'id': i, 'name': 'John', 'active': True} for i in range(start, end)]
    table = pa.table({
        'id': [u['id'] for u in users],
        'name': [u['name'] for u in users],
        'active': [u['active'] for u in users],
    })
    tables.append(table)
table = pa.concat_tables(tables)
"""

# --- Benchmark 5: msgspec.Struct → PyArrow (NumPy-based, chunked) ---
code_struct_to_arrow_numpy_chunked = """
tables = []
for start in range(0, N, CHUNK_SIZE):
    end = min(start + CHUNK_SIZE, N)
    users = [User(i, 'John', True) for i in range(start, end)]
    ids = np.fromiter((u.id for u in users), dtype=np.int64, count=len(users))
    actives = np.fromiter((u.active for u in users), dtype=bool, count=len(users))
    names = [u.name for u in users]
    table = pa.table({
        'id': pa.array(ids),
        'name': pa.array(names, type=pa.string()),
        'active': pa.array(actives),
    })
    tables.append(table)
table = pa.concat_tables(tables)
"""

# --- Benchmark 6: msgspec.Struct → Polars (list-based, chunked) ---
code_struct_to_polars_chunked = """
dfs = []
for start in range(0, N, CHUNK_SIZE):
    end = min(start + CHUNK_SIZE, N)
    users = [User(i, 'John', True) for i in range(start, end)]
    df = pl.DataFrame({
        'id': [u.id for u in users],
        'name': [u.name for u in users],
        'active': [u.active for u in users],
    })
    dfs.append(df)
df = pl.concat(dfs)
"""

# --- Benchmark 7: msgspec.Struct → Polars (NumPy-based, chunked) ---
code_struct_to_polars_numpy_chunked = """
dfs = []
for start in range(0, N, CHUNK_SIZE):
    end = min(start + CHUNK_SIZE, N)
    users = [User(i, 'John', True) for i in range(start, end)]
    ids = np.fromiter((u.id for u in users), dtype=np.int64, count=len(users))
    actives = np.fromiter((u.active for u in users), dtype=bool, count=len(users))
    names = [u.name for u in users]
    df = pl.DataFrame({
        'id': ids,
        'name': names,
        'active': actives,
    })
    dfs.append(df)
df = pl.concat(dfs)
"""

# --- Benchmark 8: msgspec.Struct → PyArrow StreamWriter (chunked) ---
code_struct_to_arrow_stream_chunked = """
sink = io.BytesIO()
schema = pa.schema([
    ('id', pa.int64()),
    ('name', pa.string()),
    ('active', pa.bool_()),
])

with pa.ipc.RecordBatchStreamWriter(sink, schema) as writer:
    for start in range(0, N, CHUNK_SIZE):
        end = min(start + CHUNK_SIZE, N)
        users = [User(i, 'John', True) for i in range(start, end)]
        ids = np.fromiter((u.id for u in users), dtype=np.int64, count=len(users))
        actives = np.fromiter((u.active for u in users), dtype=bool, count=len(users))
        names = [u.name for u in users]
        batch = pa.record_batch({
            'id': pa.array(ids),
            'name': pa.array(names, type=pa.string()),
            'active': pa.array(actives),
        }, schema=schema)
        writer.write_batch(batch)

sink.seek(0)
reader = pa.ipc.RecordBatchStreamReader(sink)
table = reader.read_all()
"""

# Benchmark runner
def run_benchmark(label, code):
    t = timeit.timeit(code, setup=setup_common, number=1)
    print(f"{label:<60} {t:.4f} sec")

print("=== msgspec.Struct vs dict (chunked conversion) ===")
run_benchmark("Struct instantiation only", code_struct_create)
run_benchmark("Dict instantiation only", code_dict_create)
run_benchmark("Struct → PyArrow (list-based, chunked)", code_struct_to_arrow_list_chunked)
run_benchmark("Dict → PyArrow (list-based, chunked)", code_dict_to_arrow_list_chunked)
run_benchmark("Struct → PyArrow (Numpy-based, chunked)", code_struct_to_arrow_numpy_chunked)
run_benchmark("Struct → Polars (list-based, chunked)", code_struct_to_polars_chunked)
run_benchmark("Struct → Polars (NumPy-based, chunked)", code_struct_to_polars_numpy_chunked)
run_benchmark("Struct → PyArrow StreamWriter (chunked)", code_struct_to_arrow_stream_chunked)
