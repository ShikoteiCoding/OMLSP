CREATE TABLE example (
    url STRING
)
WITH (
    'connector' = 'http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'schedule' = '*/1 * * * *',
    'jsonpath' = '$.url',
    'headers.Content-Type' = 'application/json'
);

CREATE TABLE example_2 (
    url STRING
)
WITH (
    'connector' = 'http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'schedule' = '*/5 * * * *',
    'jsonpath' = '$.url',
    'headers.Content-Type' = 'application/json'
);


CREATE TEMP TABLE example_3 (
    url STRING
)
WITH (
    'connector' = 'lookup-http',
    'url' = 'https://httpbin.org/get',
    'method' = 'GET',
    'jsonpath' = '$.url',
    'headers.Content-Type' = 'application/json'
);