CREATE TABLE example (
    url STRING
)
WITH (
    'connector' = 'http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'schedule' = '*/1 * * * *',
    'json.jsonpath' = '$.url'
);

CREATE TABLE example_2 (
    url STRING
)
WITH (
    'connector' = 'http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'schedule' = '*/5 * * * *',
    'json.jsonpath' = '$.url'
);


CREATE TABLE example_3 (
    url STRING
)
WITH (
    'connector' = 'lookup_http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'schedule' = '*/5 * * * *',
    'json.jsonpath' = '$.url'
);