# OMLSP
One More Little SQL Parser

# Development

## Dependencies
uv>=0.7.0

## Examples
Basic example, this table will call every 60s the endpoint of httbin for GET method and store the url inside a 'url' column.
```sql
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
```