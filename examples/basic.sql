CREATE TABLE all_tickers (
    symbol STRING,
    symbolName STRING,
    buy FLOAT,
    sell FLOAT
)
WITH (
    'connector' = 'http',
    'url' = 'https://api.kucoin.com/api/v1/market/allTickers',
    'method' = 'GET',
    'schedule' = '*/1 * * * *',
    'jq' = '.data.ticker[:10][] | {symbol, symbolName, buy, sell}',
    'headers.Content-Type' = 'application/json'
);

CREATE TEMP TABLE ohlc (
    $symbol STRING,
    $symbolName STRING,
    start_time TIMESTAMP,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT,
    amount FLOAT
)
WITH (
    'connector' = 'lookup-http',
    'url' = 'https://api.kucoin.com/api/v1/market/candles?type=1min&symbol=$symbol&startAt=1753977000&endAt=1753977300',
    'method' = 'GET',
    'jq' = '.data[] | {
        start_time: (.[0] | tonumber),
        open: (.[1] | tonumber),
        high: (.[2] | tonumber),
        low: (.[3] | tonumber),
        close: (.[4] | tonumber),
        volume: (.[5] | tonumber),
        amount: (.[6] | tonumber)
    }',
    'headers.Content-Type' = 'application/json'
);

-- Actual query we want to make it work
SELECT
    ALT.symbol,
    start_time,
    open,
    high,
    low,
    close,
    volume,
    amount
FROM all_tickers AS ALT
LEFT JOIN ohlc AS oh
    ON ALT.symbol = oh.symbol;

-- Invalid example: CTE
WITH test AS (
    SELECT * 
    FROM all_tickers
)
SELECT * FROM test;

-- Invalid example: subquery
SELECT *
FROM (SELECT * FROM all_tickers);

-- Test function registered from lookup
SELECT ohlc_func('MNDE-USDT', 'MNDE-USDT');
-- Test macro wrapping the udf
SELECT * FROM ohlc_macro("all_tickers", symbol, symbolName);
-- Test sink from table
CREATE SINK all_tickers_sink FROM all_tickers
WITH (
    connector = 'kafka',
    topic = 'tickers_topic',
    server = 'localhost:9092',
    acks = 'all',
);

-- Test sink from select
CREATE SINK all_tickers_sink FROM (SELECT symbolName, buy FROM all_tickers)
WITH (
    connector = 'kafka',
    topic = 'tickers_topic_2',
    server = 'localhost:9092',
    acks = 'all',
);