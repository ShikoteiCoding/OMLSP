-- Get all tickers from kucoin
CREATE TABLE all_tickers (
    symbol STRING,
    symbolName STRING,
    buy FLOAT,
    sell FLOAT,
    start_at BIGINT AS (TRIGGER_EPOCH_TIME() / 1000 - (5 * 60)),
    end_at BIGINT AS (TRIGGER_EPOCH_TIME() / 1000)
)
WITH (
    connector = 'http',
    url = 'https://api.kucoin.com/api/v1/market/allTickers',
    method = 'GET',
    schedule = '*/1 * * * *',
    jq = '.data.ticker[:2][] | {symbol, symbolName, buy, sell}',
    'headers.Content-Type' = 'application/json'
);

-- Get ohlc data provided symbols
CREATE TEMPORARY TABLE ohlc (
    $symbol STRING,
    $start_at BIGINT,
    $end_at BIGINT,
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

-- Materialized view leveraging all tickers + ohlc on lookup (request)
CREATE VIEW ohlc_all_spot_tickers AS
SELECT
    ALT.symbol,
    oh.start_at,
    oh.end_at,
    oh.start_time,
    open,
    high,
    low,
    close,
    volume,
    amount
FROM all_tickers AS ALT
LEFT JOIN ohlc AS oh
    ON ALT.symbol = oh.symbol;