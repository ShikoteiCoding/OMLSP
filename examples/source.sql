-- Get all tickers from kucoin
-- A source doesn't retain it's data once consumed by downstream
CREATE SOURCE all_tickers_source (
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

-- Get all tickers from kucoin
CREATE TABLE all_tickers_table (
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

-- Websocket stream as a source
-- A source doesn't retain it's data once consumed by downstream
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

-- Websocket stream as a table
CREATE TABLE binance_mini_tickers_table (
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
