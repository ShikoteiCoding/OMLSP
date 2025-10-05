CREATE TABLE binance_all_spot_symbols (
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

-- CREATE TABLE binance_mini_tickers (
--     event_type STRING,
--     event_time BIGINT,
--     symbol STRING,
--     close FLOAT,
--     open FLOAT,
--     high FLOAT,
--     low FLOAT,
--     base_volume FLOAT,
--     quote_volume FLOAT
-- )
-- WITH (
--     connector = 'ws',
--     url = 'wss://fstream.binance.com/ws/!ticker@arr',
--     'jq' = '.[:2][] | {
--         event_type: .e,
--         event_time: .E,
--         symbol: .s,
--         close: .c,
--         open: .o,
--         high: .h,
--         low: .l,
--         base_volume: .v,
--         quote_volume: .q
--     }'
-- );


CREATE VIEW binance_most_spot_btc_volume AS
SELECT 
    *
FROM binance_all_spot_symbols
WHERE symbol LIKE '%BTC'
ORDER BY quoteAsset DESC
LIMIT 5;

CREATE TABLE binance_most_spot_btc_volume_mini_tickers (
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
    url = 'wss://stream.binance.com:443/ws/$symbol@miniTicker',
    'jq' = '{
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
    'on_start_query' = 'SELECT lower(symbol) AS symbol FROM binance_most_spot_btc_volume'
);

SELECT 
    symbol,
    COUNT(*) AS nb_records
FROM binance_most_spot_btc_volume_mini_tickers 
GROUP BY symbol;

-- CREATE VIEW binance_all_symbols_most_btc_view AS
-- SELECT 
--     *
-- FROM binance_all_symbols_most_btc AS basmb
-- LEFT JOIN binance_mini_tickers_lookup AS bmtl
--     ON basmb.symbol = bmtl.symbol;


-- CREATE TABLE binance_mini_tickers_test_symbol (
--     event_type STRING,
--     event_time BIGINT,
--     symbol STRING,
--     close FLOAT,
--     open FLOAT,
--     high FLOAT,
--     low FLOAT,
--     base_volume FLOAT,
--     quote_volume FLOAT
-- )
-- WITH (
--     connector = 'ws',
--     url = 'wss://stream.binance.com/ws/bnbbtc@miniTicker',
--     'jq' = '{
--         event_type: .e,
--         event_time: .E,
--         symbol: .s,
--         close: .c,
--         open: .o,
--         high: .h,
--         low: .l,
--         base_volume: .v,
--         quote_volume: .q
--     }',
-- );