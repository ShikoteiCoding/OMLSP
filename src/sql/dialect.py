from __future__ import annotations

from typing import Any, Callable
from sqlglot import exp, parse_one, Tokenizer, Dialect, TokenType, Parser, Generator


def _show_parser(
    *args: Any, **kwargs: Any
) -> Callable[[OmlspDialect.Parser], exp.Show]:
    def _parse(self: OmlspDialect.Parser) -> exp.Show:
        return self._parse_show_omlsp(*args, **kwargs)

    return _parse


class OmlspDialect(Dialect):
    class Tokenizer(Tokenizer):
        KEYWORDS = {
            # TODO: ADD SOURCE HERE
            **Tokenizer.KEYWORDS,
            "SINK": TokenType.SINK,
            "WITH": TokenType.WITH,
            "SHOW": TokenType.SHOW,
            # Generic token type for "non existing" tokens
            # Can only be delt dynamically if self._match
            "SECRET": TokenType.COMMAND,
        }

        COMMANDS = Tokenizer.COMMANDS - {TokenType.SHOW}

    class Parser(Parser):
        SHOW_PARSERS = {
            "TABLES": _show_parser("TABLES"),
            "VIEWS": _show_parser("VIEWS"),
            "MATERIALIZED VIEWS": _show_parser("MATERIALIZED VIEWS"),
            "SECRETS": _show_parser("SECRETS"),
            "SINKS": _show_parser("SINKS"),
        }

        STATEMENT_PARSERS = {
            **Parser.STATEMENT_PARSERS,
            TokenType.SHOW: lambda self: self._parse_show(),
        }

        def _parse_table_hints(self) -> list[exp.Expression] | None:
            # Keep SINK behavior unchanged
            return None

        def _parse_show_omlsp(self, this: str) -> exp.Show:
            # Example:
            # SHOW TABLES
            
            return self.expression(exp.Show, this=this)

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

    class Generator(Generator):
        def show_sql(self, expression: exp.Show) -> str:
            return f"SHOW {expression.name}"


if __name__ == "__main__":
    stmt = "SHOW TABLES;"
    tokens = list(OmlspDialect.Tokenizer().tokenize(stmt))
    for t in tokens:
        print(t.token_type, t.text)
    res = parse_one(stmt, dialect=OmlspDialect).sql(dialect=OmlspDialect)
