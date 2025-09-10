from sqlglot import exp, parse_one
from sqlglot.dialects.dialect import Dialect
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType

class CustomTokenizer(Tokenizer):
    """
    Custom tokenizer to recognize 'SINK' as a keyword.
    """
    KEYWORDS = {
        **Tokenizer.KEYWORDS,
        'SINK': TokenType.SINK,
    }

class CustomParser(Parser):
    """
    Custom parser to handle the 'CREATE SINK' syntax.
    """
    def _parse_create(self):
        """
        Overrides the default _parse_create to add logic for 'SINK'.
        CREATE SINK <sink_name> FROM <source_table> WITH (...)
        """
        # We check if the token immediately after CREATE is SINK
        if self._match(TokenType.SINK):
            # Parse the name of the sink (e.g., 'all_tickers')
            name = self._parse_id_var()

            # Expect the FROM keyword, otherwise raise a parse error.
            if not self._match(TokenType.FROM):
                self.raise_error("Expected FROM in CREATE SINK statement")

            # Parse the source table name
            source_table = self._parse_id_var()

            # The WITH clause is optional. The built-in _parse_properties() helper
            # seems to have issues with keys that are string literals, so we'll parse them manually.
            properties = None
            if self._match(TokenType.WITH):
                self._match(TokenType.L_PAREN)
                properties = self._parse_properties()
                self._match(TokenType.R_PAREN)

            # Return a standard sqlglot Create expression, but with our custom kind.
            # This makes it easy to work with the parsed structure.
            return self.expression(
                exp.Create,
                this=name,
                kind='SINK',
                expression=source_table, # We can use the 'expression' field to hold the source table
                properties=properties
            )

        # If it's not 'CREATE SINK', fall back to the default parser behavior
        # so it can still parse things like 'CREATE TABLE', 'CREATE VIEW', etc.
        return super()._parse_create()


class CustomDialect(Dialect):
    """
    The custom dialect that ties our custom tokenizer and parser together.
    """
    parser = CustomParser
    tokenizer = CustomTokenizer


# --- DEMONSTRATION ---
if __name__ == "__main__":
    sql = """
    CREATE SINK all_tickers FROM source_stream
    WITH (
        connector = 'kafka',
        topic = 'crypto_tickers_topic',
    'bootstrap.servers' = 'localhost:9092'
    )
    """

    print("--- Parsing Custom SQL ---")
    # Use parse_one with our custom dialect, 'read' is the correct argument
    parsed_expression = parse_one(sql, read=CustomDialect)

    # Print the parsed structure
    print("\nParsed Expression:")
    print(parsed_expression)

    # You can access the different parts of the statement easily
    print("\nAccessing Parsed Components:")
    print(f"  Kind: {parsed_expression.kind}")
    print(f"  Sink Name: {parsed_expression.this.name}")
    print(f"  Source Table: {parsed_expression.expression.name}")

    # The properties are also parsed into a list of equality expressions
    with_properties = parsed_expression.args['properties'].expressions
    print("\nWITH Properties:")
    for prop in with_properties:
        print(prop)

    # You can also generate the SQL back from the expression
    print("\n--- Regenerating SQL ---")
    regenerated_sql = parsed_expression.sql(dialect=CustomDialect, pretty=True)
    print(regenerated_sql)

    # Verification check
    assert parsed_expression.kind == 'SINK'
    print("\nVerification successful: parsed.kind is 'SINK'")




