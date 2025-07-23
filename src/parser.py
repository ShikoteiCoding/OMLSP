from sqlglot import parse_one, exp
import sqlglot.errors
import logging

MAPPING_TYPES = {
    'BOOLEAN': bool,
    'SMALLINT': int,
    'INTEGER': int,
    'BIGINT': int,
    'FLOAT': float,
    'DOUBLE': float,
    'VARCHAR': str,
}

RESULT = {
        'table_name': None,
        'columns': [],
        'properties': {}
    }

def parse_table(table):
    columns = []
    if isinstance(table.this, exp.Table):
        table_name = table.this.name
        for column in table.this.expressions:
            if isinstance(column, exp.ColumnDef):
                col_name = column.this.name if column.this else None
                col_type = column.kind.this.name if column.kind and column.kind.this else None
                
                if not col_name:
                    raise ValueError(f"Column name not found in {column}")
                if not col_type:
                    raise ValueError(f"Column type not found for column {col_name}")
                
                columns.append({'name': col_name, 'type': col_type})
    elif isinstance(table, exp.Table):
        table_name = table.name
    else:
        raise ValueError(f"Expected exp.Schema or exp.Table, got {type(table)}")
    return table_name, columns

def parse_with_properties(with_properties):
    properties = {}
    for prop in with_properties.args.get('properties', []):
        if not isinstance(prop, exp.Property):
            raise ValueError(f"Expected exp.Property, got {type(prop)}")
        
        key = prop.this.name if isinstance(prop.this, exp.Identifier) else str(prop.this)
        value = prop.args.get('value')
        
        if value is None:
            raise ValueError(f"Property {key} has no value")
        if not isinstance(value, exp.Literal):
            raise ValueError(f"Property {key} value is not a literal: {value}")
        
        properties[key] = value.this
    
    return properties

def parse_query_to_dict(query):
    """
    Parse a SQL query into a dictionary
    
    Returns:
        dict: Dictionary containing table schema and properties
    """

    try:
        parsed = parse_one(query, dialect=None)
        # Extract Query
        if isinstance(parsed, exp.Create):
            table = parsed.this
            RESULT['table_name'], RESULT['columns'] = parse_table(table)
            # Extract WITH properties
            RESULT['properties'] = parse_with_properties(parsed)
        else:
            logging.error(f"Expected CREATE TABLE query, got {type(parsed)}: {str(parsed)}")
            print(f"Error: Query is not a CREATE TABLE statement: {str(parsed)}")
            return RESULT
        return RESULT
    
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