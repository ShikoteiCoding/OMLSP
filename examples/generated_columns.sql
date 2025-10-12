-- Get all tickers from kucoin
CREATE TABLE all_tickers (
    symbol STRING,
    symbolName STRING,
    buy FLOAT,
    sell FLOAT,
    -- Defining symbol_copy from symbol (for the sake of example)
    symbol_copy STRING AS (symbol),
    -- Defining spread from buy and sell
    spread FLOAT AS (sell - buy),
    -- Defining trigger time as timestamp default is us
    trigger_time TIMESTAMP AS (TRIGGER_TIME()),
    -- Defining start_at and end_at for ohlc lookup
    -- TRIGGER_TIME_EPOCH returns epoch as us
    start_at BIGINT AS (TRIGGER_TIME_EPOCH() / 1000000 - (5 * 60)),
    end_at BIGINT AS (TRIGGER_TIME_EPOCH() / 1000000)
)
WITH (
    connector = 'http',
    url = 'https://api.kucoin.com/api/v1/market/allTickers',
    method = 'GET',
    schedule = '*/1 * * * *',
    jq = '.data.ticker[:10][] | {symbol, symbolName, buy, sell}',
    'headers.Content-Type' = 'application/json'
);

-- Get ohlc data provided symbols
CREATE TEMPORARY TABLE ohlc (
    $symbol STRING,
    -- TODO: fix to same type as upstream table
    $start_at STRING,
    $end_at STRING,
    -- TODO: fix TIMESTAMP_S, it should be integer from url response
    -- Consider doing aumatic timestamp convertion (aka coercion)
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
    'url' = 'https://api.kucoin.com/api/v1/market/candles?type=1min&symbol=$symbol&startAt=$start_at&endAt=$end_at',
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

CREATE VIEW ohlc_all_spot_tickers AS
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