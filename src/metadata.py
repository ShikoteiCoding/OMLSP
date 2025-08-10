from duckdb import DuckDBPyConnection


def init_metadata(con: DuckDBPyConnection) -> None:
    # Create table for lookup macro definition
    macro_table_to_def = """
    CREATE TABLE macro_metadata (
        macro_name STRING,
        fields STRING[]
    );
    """
    con.sql(macro_table_to_def)


def get_macro_definition_by_name(
    con: DuckDBPyConnection, macro_name: str
) -> tuple[str, list[str]]:
    assert macro_name, "no macro name provided"

    query = f"""
    SELECT 
        macro_name,
        fields
    FROM macro_metadata
    WHERE macro_name = '{macro_name}';
    """
    res = con.sql(query).fetchall()

    assert len(res) > 0

    return res[0]


def create_macro_definition(
    con: DuckDBPyConnection, macro_name: str, fields: list[str]
) -> None:
    assert macro_name and fields, "no macro name or definition provided"

    query = f"""
    INSERT INTO macro_metadata (macro_name, fields)
    VALUES ('{macro_name}', {fields});
    """
    con.sql(query)
