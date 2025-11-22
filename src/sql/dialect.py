from __future__ import annotations

import polars as pl

from typing import Any, Callable
from sqlglot import exp, parse_one, parser, Dialect, TokenType, generator, tokens

from sql.functions import get_trigger_time, get_trigger_time_epoch


# OMLSP specific functions available for generated columns
class TriggerTime(exp.Func):
    arg_types = {}


class TriggerEpochTime(exp.Func):
    arg_types = {}


# OLMSP mapping of st(node) to function
# mainly used in mod:`sql.sqlparser.parser`
GENERATED_COLUMN_FUNCTION_DISPATCH: dict[
    str, Callable[[dict[str, Any], exp.DataType], pl.Expr]
] = {
    "TRIGGER_TIME()": get_trigger_time,
    "TRIGGER_EPOCH_TIME()": get_trigger_time_epoch,
}


def generate_functions_parser():
    return {
        "TRIGGER_TIME": lambda args: TriggerTime(),
        "TRIGGER_EPOCH_TIME": lambda args: TriggerEpochTime(),
    }


def _show_parser(
    *args: Any, **kwargs: Any
) -> Callable[[OmlspDialect.Parser], exp.Show]:
    def _parse(self: OmlspDialect.Parser) -> exp.Show:
        return self._parse_show_omlsp(*args, **kwargs)

    return _parse


class OmlspDialect(Dialect):
    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "SHOW": TokenType.SHOW,
            "SINK": TokenType.SINK,
            "SOURCE": TokenType.SOURCE,
            "WITH": TokenType.WITH,
            # Generic token type for "non existing" tokens
            # Can only be delt dynamically if self._match
            "SECRET": TokenType.COMMAND,
        }

        COMMANDS = tokens.Tokenizer.COMMANDS - {TokenType.SHOW}

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            **generate_functions_parser(),
        }

        SHOW_PARSERS = {
            "TABLES": _show_parser("TABLES"),
            "VIEWS": _show_parser("VIEWS"),
            "MATERIALIZED VIEWS": _show_parser("MATERIALIZED VIEWS"),
            "SECRETS": _show_parser("SECRETS"),
            "SINKS": _show_parser("SINKS"),
            "SOURCES": _show_parser("SOURCES"),
        }

        STATEMENT_PARSERS = {
            **parser.Parser.STATEMENT_PARSERS,
            TokenType.SHOW: lambda self: self._parse_show(),
        }

        def _parse_table_hints(self) -> list[exp.Expression] | None:
            # Keep SINK behavior unchanged
            return None

        def _parse_show_omlsp(self, this: str) -> exp.Show:
            # Example:
            # SHOW TABLES

            return self.expression(exp.Show, this=this)

        def _parse_generated_as_identity(
            self,
        ) -> (
            exp.GeneratedAsIdentityColumnConstraint
            | exp.ComputedColumnConstraint
            | exp.GeneratedAsRowColumnConstraint
        ):
            this = super()._parse_generated_as_identity()
            return this

        def _parse_create(self):
            # Example:
            # CREATE SECRET secret_name WITH (backend = 'meta') AS 'secret_value'

            # Detect CREATE SECRET as TokenType COMMAND
            if self._match(TokenType.COMMAND) and not self._match(TokenType.TABLE):
                if self._prev:
                    # Extract SECRET
                    kind = self._prev.text.upper()

                    # Parse the secret name
                    this = self._parse_id_var()

                    # Optionally parse WITH (...)
                    properties = self._parse_properties()

                    # Require AS '...'
                    if not self._match(TokenType.ALIAS):
                        self.raise_error("Expected AS after CREATE SECRET ...")

                    # This should get parsed as Literal
                    secret_value = self._parse_string()

                    # Handle removal of single quotes
                    secret_value_str = str(secret_value)
                    if secret_value_str.startswith("'") and secret_value_str.startswith(
                        "'"
                    ):
                        secret_value = secret_value_str[1:-1]

                    # Return a standard Create expression
                    return self.expression(
                        exp.Create,
                        kind=kind,
                        this=this,
                        properties=properties,
                        expression=secret_value,
                    )

            # Fallback to the default Postgres CREATE behavior
            return super()._parse_create()

    class Generator(generator.Generator):
        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            TriggerTime: lambda self, e: "TRIGGER_TIME()",
            TriggerEpochTime: lambda self, e: "TRIGGER_EPOCH_TIME()",
        }

        def show_sql(self, expression: exp.Show) -> str:
            return f"SHOW {expression.name}"


if __name__ == "__main__":
    sql = """
    CREATE SOURCE all_tickers (
        symbol STRING,
        trigger_time_ms TIMESTAMP_MS AS (TRIGGER_TIME()),
        end_at BIGINT AS (TRIGGER_EPOCH_TIME() / 1000)
    )
    WITH (
        'key1' = 'val1'
    )
    """

    expr = parse_one(sql, read=OmlspDialect)
    if expr:
        print(expr)
