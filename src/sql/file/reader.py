import sqlglot
from typing import Generator


def iter_sql_statements(filepath: str) -> Generator[str, None, None]:
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    statements = [stmt.lstrip() for stmt in content.split(";") if stmt != ""]

    for statement in statements:
        yield statement


if __name__ == "__main__":
    with open("example.sql", "w", encoding="utf-8") as f:
        f.write("""
        -- This is a comment.
        SELECT * FROM users;
            
        INSERT INTO products (name, price) VALUES ('Laptop', 1200.00); /* Another comment */

        -- Another query with a semicolon inside a string
        UPDATE settings SET value = 'This is a test; with a semicolon' WHERE id = 1;
        """)

    try:
        for i, sql_statement in enumerate(iter_sql_statements("example.sql")):
            print(sql_statement)
    except (FileNotFoundError, IOError) as e:
        print(e)
