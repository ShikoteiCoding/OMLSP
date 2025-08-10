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
    'jsonpath' = '.data.ticker[] | {symbol, symbolName, buy, sell}',
    'headers.Content-Type' = 'application/json'
);

CREATE TEMP TABLE ohlc (
    $symbol STRING, -- from calling table
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
    'jsonpath' = '.data[] | {
        start_time: (.[0] | tonumber | . as $ts | ($ts | todate)),
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
LEFT JOIN ohlc ON
    ALT.symbol = ohlc.symbol
WHERE ALT.symbol LIKE '%MNDE';

-- Simple query on non lookup table
SELECT * 
FROM all_tickers;

-- Test function registered from lookup
SELECT ohlc_func('MNDE-USDT');
-- Test macro wrapping the udf
SELECT * FROM ohlc_macro("all_tickers", symbol);