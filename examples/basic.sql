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
    amounnt FLOAT
)
WITH (
    'connector' = 'lookup-http',
    'url' = 'https://api.kucoin.com/api/v1/market/candles?type=1min&symbol=$symbol&startAt=1753977000&endAt=1753977300',
    'method' = 'GET',
    'jsonpath' = '$.url',
    'headers.Content-Type' = 'application/json'
);

-- TODO
SELECT 
    *
FROM all_tickers AS ALT
LEFT JOIN ohlc ON
    ALT.symbol = ohlc.symbol;