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

CREATE SINK all_tickers_sink_bis FROM all_tickers
WITH (
    connector = 'kafka',
    topic = 'tickers_topic_2',
    server = 'localhost:9092',
    encode = 'json',
);

CREATE SINK all_tickers_sink_tris FROM all_tickers
WITH (
    connector = 'kafka',
    topic     = 'tickers_avro_topic',
    server    = 'localhost:9092',
    encode    = 'avro',
    registry  = 'http://localhost:8081',
    subject   = 'tickers_avro_topic-value',
    schema    = '{
        "type": "record",
        "name": "KucoinTickerSink",
        "fields": [
            {"name": "symbol", "type": "string"},
            {"name": "symbolName", "type": "string"},
            {"name": "buy", "type": "double"}, 
            {"name": "sell", "type": "double"}
        ]
    }'
);