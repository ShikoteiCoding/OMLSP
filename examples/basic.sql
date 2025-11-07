-- Get all tickers from kucoin
CREATE TABLE all_tickers (
    symbol STRING,
    symbolName STRING,
    buy FLOAT,
    sell FLOAT
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
-- TODO: handle dynamic arguments
CREATE TEMPORARY TABLE ohlc (
    $symbol STRING,
    start_time TIMESTAMP_MS,
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

-- Materialized view leveraging all tickers + ohlc on lookup (request)
CREATE MATERIALIZED VIEW ohlc_all_spot_tickers AS
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

CREATE SOURCE binance_mini_tickers_source (
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
    connector = 'ws',
    url = 'wss://fstream.binance.com/ws/!ticker@arr',
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

CREATE SINK all_tickers_sink_bis FROM all_tickers
WITH (
    connector = 'kafka',
    topic = 'tickers_topic_2',
    server = 'localhost:9092',
);