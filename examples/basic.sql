CREATE TABLE all_tickers (
    symbol STRING,
    symbolName STRING,
    buy FLOAT,
    sell FLOAT,
    date STRING,
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
    $date STRING,
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
    'url' = 'https://api.kucoin.com/api/v1/market/candles?type=1min&symbol=$symbol&startAt=$date&endAt=$date',
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

-- Simple query on non lookup table
SELECT * 
FROM all_tickers;

-- Test function registered from lookup
SELECT ohlc_func('MNDE-USDT', '1697059200');
-- Test macro wrapping the udf
SELECT * FROM ohlc_macro("all_tickers", symbol, date);