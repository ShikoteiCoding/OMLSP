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