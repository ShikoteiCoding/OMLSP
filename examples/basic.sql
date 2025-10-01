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
    'jq' = '.data.ticker[:2][] | {symbol, symbolName, buy, sell}',
    'headers.Content-Type' = 'application/json'
);

CREATE TABLE binance_mini_tickers (
    event_type STRING,
    event_time BIGINT,
    symbol STRING,
    close FLOAT,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    base_volume FLOAT,
    quote_volume FLOAT
)
WITH (
    'connector' = 'ws',
    'url' = 'wss://fstream.binance.com/ws/!ticker@arr',
    'jq' = '.[:2][] | {
        event_type: .e,
        event_time: .E,
        symbol: .s,
        close: .c,
        open: .o,
        high: .h,
        low: .l,
        base_volume: .v,
        quote_volume: .q
    }'
);

CREATE TEMPORARY TABLE ohlc (
    $symbol STRING,
    start_time TIMESTAMP_S,
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
SELECT ohlc_func('MNDE-USDT');

-- Test macro wrapping the udf
SELECT *
FROM ohlc_macro("all_tickers", symbol);

-- Test subquery in macro
SELECT
    ohlc_func(ALT.symbol) AS struct
FROM "all_tickers" AS ALT;

SELECT struct.* FROM (
SELECT
    ohlc_func(ALT.symbol) AS struct
FROM query_table("all_tickers") AS ALT);

SELECT
    ohlc_func(ALT.symbol)
FROM query_table("all_tickers") AS ALT;

-- Test pre hook
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
LEFT JOIN ohlc_macro("all_tickers", ALT.symbol, ALT.symbolName) AS oh 
    ON ALT.symbol = oh.symbol;

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
LEFT JOIN ohlc_macro("all_tickers", ALT.symbol) AS oh 
    ON ALT.symbol = oh.symbol;

SELECT ALT.symbol 
FROM all_tickers AS ALT 
LEFT JOIN all_tickers AS ALT2 
    on ALT.symbol = ALT2.symbol;

-- Test sink from table
-- CREATE SINK all_tickers_sink FROM all_tickers
-- WITH (
--     connector = 'kafka',
--     topic = 'tickers_topic',
--     server = 'localhost:9092',
-- );

-- Test view
CREATE MATERIALIZED VIEW my_first_view AS
SELECT
    symbol,
    buy
FROM all_tickers
WHERE buy IS NOT NULL AND buy < 20;

CREATE VIEW my_second_view FROM all_tickers;