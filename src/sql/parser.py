from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError

from sql.dialect import OmlspDialect
from sql.errors import SqlSyntaxError

if TYPE_CHECKING:
    from sql.types import SQL


class Parser:
    """
    Parser wrapper for parse method
    """

    @staticmethod
    def parse(sql: SQL) -> exp.Expression:
        """
        Parse SQL string into AST.

        Raises:
            SqlSyntaxError: When SQL cannot be parsed.
        """
        try:
            parsed_statement = parse_one(sql, dialect=OmlspDialect)
            if parsed_statement:
                Parser._validate(parsed_statement)
            return parsed_statement
        except (ParseError, ValueError) as e:
            logger.error("[Parser] Not able to parse sql {} - Error {}", sql, e)
            raise SqlSyntaxError(f"Syntax Error: {e}")

    @staticmethod
    def _validate(expression: exp.Expression) -> None:
        if isinstance(expression, exp.Create):
            kind = str(expression.args.get("kind")).upper()
            query = expression.expression

            if not query:
                return

            # Common validation: No CTE (nesting)
            if expression.find(exp.With) or query.find(exp.Group):
                raise ValueError(f"CTE or GROUP BY is not allowed in CREATE {kind}")

            if kind in ("SOURCE", "TABLE"):
                if query.find(exp.Join):
                    raise ValueError(f"JOIN is not allowed in CREATE {kind}")

            elif kind in ("VIEW", "SINK"):
                if len(list(query.find_all(exp.Join))) > 1:
                    raise ValueError(f"Max 1 lookup JOIN allowed in CREATE {kind}")
