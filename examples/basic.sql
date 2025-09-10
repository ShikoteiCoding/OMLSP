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
    'jq' = '.data.ticker[:10][] | {symbol, symbolName, buy, sell}',
    'headers.Content-Type' = 'application/json'
);

CREATE SINK all_tickers_sink FROM all_tickers
WITH (
    connector = 'kafka',
    topic = 'tickers_topic',
    server = 'localhost:9092',
    compression = 'snappy',
    acks = 'all'
);