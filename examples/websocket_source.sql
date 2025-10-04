CREATE TABLE binance_all_symbols (
    symbol STRING,
    status STRING,
    baseAsset STRING,
    quoteAsset STRING
)
WITH (
    connector = 'http',
    url = 'https://api.binance.com/api/v3/exchangeInfo',
    method = 'GET',
    schedule = '*/1 * * * *',
    jq = '.symbols[] | {symbol, status, baseAsset, quoteAsset}',
    'headers.Content-Type' = 'application/json'
);

CREATE VIEW binance_all_symbols_most_btc AS
SELECT 
    *
FROM binance_all_symbols
WHERE symbol LIKE '%BTC'
ORDER BY quoteAsset DESC
LIMIT 1;

CREATE TABLE binance_mini_tickers_lookup (
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
    url = 'wss://fstream.binance.com/ws/$symbol!miniTicker',
    'jq' = '.[] | {
        event_type: .e,
        event_time: .E,
        symbol: .s,
        close: .c,
        open: .o,
        high: .h,
        low: .l,
        base_volume: .v,
        quote_volume: .q
    }',
    'on_start' = 'SELECT symbol FROM binance_all_symbols_most_btc'
);

-- CREATE VIEW binance_all_symbols_most_btc_view AS
-- SELECT 
--     *
-- FROM binance_all_symbols_most_btc AS basmb
-- LEFT JOIN binance_mini_tickers_lookup AS bmtl
--     ON basmb.symbol = bmtl.symbol;

