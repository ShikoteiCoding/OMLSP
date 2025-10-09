from __future__ import annotations

from typing import Any, Callable
from sqlglot import exp, parse_one, parser, Dialect, TokenType, generator, tokens


def _show_parser(
    *args: Any, **kwargs: Any
) -> Callable[[OmlspDialect.Parser], exp.Show]:
    def _parse(self: OmlspDialect.Parser) -> exp.Show:
        return self._parse_show_omlsp(*args, **kwargs)

    return _parse


def func_sql(self, expression: exp.Func) -> str:
    return expression.name


class OmlspDialect(Dialect):
    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {
            # TODO: ADD SOURCE HERE
            **tokens.Tokenizer.KEYWORDS,
            "SINK": TokenType.SINK,
            "WITH": TokenType.WITH,
            "SHOW": TokenType.SHOW,
            # Generic token type for "non existing" tokens
            # Can only be delt dynamically if self._match
            "SECRET": TokenType.COMMAND,
        }

        COMMANDS = tokens.Tokenizer.COMMANDS - {TokenType.SHOW}

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "START_TIME": lambda args: exp.Func(this="START_TIME"),
        }
        SHOW_PARSERS = {
            "TABLES": _show_parser("TABLES"),
            "VIEWS": _show_parser("VIEWS"),
            "MATERIALIZED VIEWS": _show_parser("MATERIALIZED VIEWS"),
            "SECRETS": _show_parser("SECRETS"),
            "SINKS": _show_parser("SINKS"),
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
            exp.Func: lambda self, e: self.func(
                "START_TIME"
                # TODO: check, but we might be able to attach function
                # here so we actually get it from SQLGlot directly
            ),
        }

        def show_sql(self, expression: exp.Show) -> str:
            return f"SHOW {expression.name}"


if __name__ == "__main__":
    stmt = """
    CREATE TABLE all_tickers (
        symbol STRING,
        symbolName STRING,
        buy FLOAT,
        sell FLOAT,
        end_interval_time TIMESTAMP AS (start_time()),
        start_interval_time TIMESTAMP AS (end_time())
    )
    WITH (
        connector = 'http',
        url = 'https://api.kucoin.com/api/v1/market/allTickers',
        method = 'GET',
        schedule = '*/1 * * * *',
        jq = '.data.ticker[:2][] | {symbol, symbolName, buy, sell}',
        'headers.Content-Type' = 'application/json'
    );
    """
    tokens = list(OmlspDialect.Tokenizer().tokenize(stmt))
    for t in tokens:
        print(t.token_type, t.text)
    res = parse_one(stmt, dialect=OmlspDialect).sql(dialect=OmlspDialect)
