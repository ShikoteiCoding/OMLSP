from sqlglot import parse_one, exp
import sqlglot.errors
import logging

def parse_query_to_dict(query):
    """
    Parse a SQL query into a dictionary containing table schema and connector properties.
    
    Returns:
        dict: Dictionary containing table schema and properties
    """
    try:
        parsed = parse_one(query, dialect=None)
        
        result = {
            'table_name': None,
            'columns': [],
            'properties': {}
        }
        
        # Extract Query
        if isinstance(parsed, exp.Create):
            table = parsed.this
            parsed = parse_one(query, dialect=None)

            if isinstance(parsed.this, exp.Schema):
                result['table_name'] = table.this.name if isinstance(table.this, exp.Table) else str(table.this)
                for column in parsed.this.expressions:
                    if isinstance(column, exp.ColumnDef):
                        col_name = column.this.name
                        col_type = column.kind.this.name if column.kind and hasattr(column.kind, 'this') and hasattr(column.kind.this, 'name') else str(column.kind) if column.kind else None
                        result['columns'].append({
                            'name': col_name,
                            'type': col_type
                        })
            else:
                result['table_name'] = table.name if isinstance(table, exp.Table) else str(table)
            
            # Extract WITH properties
            for prop in parsed.args.get('properties', []):
                if not isinstance(prop, exp.Property):
                    logging.error(f"Expected exp.Property, got {type(prop)}: {prop}")
                    continue
                try:
                    key = prop.this.name if isinstance(prop.this, exp.Identifier) else str(prop.this)
                    value = prop.args.get('value')
                    if value is None:
                        logging.warning(f"Property {key} has no value")
                        result['properties'][key] = None
                        continue
                    if isinstance(value, exp.Literal):
                        result['properties'][key] = value.this
                    else:
                        result['properties'][key] = str(value)
                except AttributeError as e:
                    logging.error(f"Error processing property {prop}: {e}")
                    continue
        else:
            logging.error(f"Expected CREATE TABLE query, got {type(parsed)}: {str(parsed)}")
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