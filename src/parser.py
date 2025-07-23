from sqlglot import parse_one, exp
import sqlglot.errors
import logging


def parse_query_to_dict(query) -> dict:
    """
    Parse a SQL query into a dictionary

    Returns:
        dict: Dictionary containing table schema and properties
    """

    result = {"table_name": None, "columns": [], "properties": {}}
    try:
        parsed = parse_one(query, dialect=None)
        # Extract Query
        if isinstance(parsed, exp.Create):
            table = parsed.this
            if isinstance(parsed.this, exp.Schema):
                if isinstance(table.this, exp.Table):
                    result["table_name"] = table.this.name
                else:
                    raise ValueError(f"Table name not found in query: {query}")
                for column in parsed.this.expressions:
                    if isinstance(column, exp.ColumnDef):
                        # Check if column has a name
                        if hasattr(column, "this") and hasattr(column.this, "name"):
                            col_name = column.this.name
                        else:
                            raise ValueError(f"Column name not found in {column}")
                        # Check if column has a type
                        if (
                            column.kind
                            and hasattr(column.kind, "this")
                            and hasattr(column.kind.this, "name")
                        ):
                            col_type = column.kind.this.name
                        else:
                            raise ValueError(
                                f"Column type not found for column {col_name}"
                            )

                        result["columns"].append({"name": col_name, "type": col_type})
            elif isinstance(table, exp.Table):
                result["table_name"] = table.name
            else:
                raise ValueError(
                    f"Expected exp.Schema or exp.Table, got {type(table)}: {table}"
                )
            # Extract WITH properties
            for prop in parsed.args.get("properties", []):
                if not isinstance(prop, exp.Property):
                    raise ValueError(f"Expected exp.Property, got {type(prop)}: {prop}")
                try:
                    key = (
                        prop.this.name
                        if isinstance(prop.this, exp.Identifier)
                        else str(prop.this)
                    )
                    value = prop.args.get("value")
                    if value is None:
                        logging.error(f"Property {key} has no value")
                        raise ValueError(f"Invalid property: {key} has no value")
                    elif isinstance(value, exp.Literal):
                        result["properties"][key.replace("'", "")] = value.this
                    else:
                        raise ValueError(f"Invalid property: {value} is not a lieral")
                except AttributeError as e:
                    logging.error(f"Error processing property {prop}: {e}")
                    continue
        else:
            logging.error(
                f"Expected CREATE TABLE query, got {type(parsed)}: {str(parsed)}"
            )
            print(f"Error: Query is not a CREATE TABLE statement: {str(parsed)}")
            return result

        return result

    except sqlglot.errors.ParseError as e:
        logging.error(f"Failed to parse SQL query: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error while parsing query: {e}")
        raise


query = """
CREATE TABLE example (
    url STRING
)
WITH (
    'connector' = 'http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'scan.interval' = '60s',
    'json.jsonpath' = '$.url'
);
"""

result = """
{'table_name': 'example', 
    'columns': [{'name': 'url', 'type': 'TEXT'}], 
        'properties': {'connector': 'http', 'url': 'https://httpbin.org/get', 
        'method': 'GET', 'scan.interval': '60s', 'json.jsonpath': '$.url'
    }
}
"""

invalid_query = """
SELECT * FROM example;
"""

invalid_query_2 = """
CREATE TABLE example (
    url STRING
)
WITH (
    'connector' = 'http',
    'timestamp' = CURRENT_TIMESTAMP
);
"""

invalid_query_3 = """
CREATE TABLE example (
    url STRING
)
WITH (
    'connector' = 'http',
    'novalue' = NULL,
    'time' = CURRENT_TIMESTAMP
);
"""

invalid_query_4 = """
CREATE TABLE example (
    STRING
)
WITH (
    'connector' = 'http'
);
"""
