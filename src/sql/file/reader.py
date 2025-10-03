from pathlib import Path
from typing import Generator

from loguru import logger


def iter_sql_statements(path: Path | str) -> Generator[str, None, None]:
    content = ""

    with open(path, "r", encoding="utf-8") as f:
        while line := f.readline():
            # remove comments
            if line.startswith("--"):
                continue
            content += line

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
            logger.success(sql_statement)
    except (FileNotFoundError, IOError) as e:
        logger.error(e)
