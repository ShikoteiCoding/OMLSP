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

CREATE VIEW binance_all_symbols_btc AS
SELECT 
    *
FROM binance_all_symbols
WHERE symbol LIKE '%BTC'
LIMIT 1;

CREATE TABLE binance_mini_tickers (
    event_type STRING,
    event_time BIGINT,
    $symbol STRING,
    close FLOAT,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    base_volume FLOAT,
    quote_volume FLOAT
)
WITH (
    connector = 'lookup-ws',
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
    }'
);

-- CREATE VIEW binance_all_symbols_btc AS
-- SELECT 
--     *
-- FROM binance_all_symbols AS bas
-- LEFT JOIN binance_mini_tickers AS bmt
--     ON bas.symbol = bmt.symbol
-- WHERE symbol LIKE '%BTC%';

