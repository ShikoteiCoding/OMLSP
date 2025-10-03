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
LEFT JOIN ohlc AS oh
    ON ALT.symbol = oh.symbol;

SELECT ALT.symbol 
FROM all_tickers AS ALT 
LEFT JOIN all_tickers AS ALT2 
    on ALT.symbol = ALT2.symbol;

-- Test sink from table
-- CREATE SINK all_tickers_sink FROM all_tickers
-- WITH (
--     connector = 'kafka',
--     topic = 'tickers_topic',
--     server = 'localhost:9092',
-- );

-- Test view
CREATE MATERIALIZED VIEW my_first_view AS
SELECT
    symbol,
    buy
FROM all_tickers
WHERE buy IS NOT NULL AND buy < 20;

CREATE VIEW my_second_view FROM all_tickers;