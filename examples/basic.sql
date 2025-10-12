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

CREATE TABLE coinlore_market_tickers (
    symbol STRING,
    name STRING,
    price_usd FLOAT,
)
WITH (
    connector = 'http',
    url = 'https://api.coinlore.net/api/tickers/',
    method = 'GET',
    'pagination.type' = 'limit_offset',
    'pagination.limit_param' = 'limit',
    'pagination.page_param' = 'start',
    'pagination.page_start' = '0',
    'pagination.limit' = '100',
    'pagination.max' = '1000',
    schedule = '*/1 * * * *',
    jq = '.data[] | {symbol, name, price_usd}',
    'headers.Content-Type' = 'application/json'
);

CREATE TABLE binance_agg_trades (
    a BIGINT,
    p FLOAT,
    T BIGINT,
)
WITH (
    connector = 'http',
    url = 'https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT', 
    method = 'GET',
    'pagination.type' = 'cursor',
    'pagination.cursor_id' = 'a',
    'pagination.cursor_param' = 'fromId',
    'pagination.limit' = '500',
    'pagination.max' = '1000',
    schedule = '*/1 * * * *',
    jq = '.[] | {a, p, T}',
    'headers.Content-Type' = 'application/json'
);

CREATE TABLE coinbase_trades (
    trade_id BIGINT,
    side STRING,
    size FLOAT,
    price FLOAT,
    time TIMESTAMP
)
WITH (
    connector = 'http',
    url = 'https://api.exchange.coinbase.com/products/BTC-USD/trades',
    method = 'GET',
    'pagination.type' = 'header',
    'pagination.limit_param' = 'limit',
    'pagination.limit' = '40',
    'pagination.cursor_param' = 'after',
    'pagination.next_header' = 'cb-after',
    'pagination.max' = '100',
    schedule = '*/1 * * * *',
    jq = '.[] | {trade_id, side, size, price, time}',
    'headers.Accept' = 'application/json'
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