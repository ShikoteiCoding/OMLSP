CREATE TABLE coinlore_market_tickers (
    symbol STRING,
    name STRING,
    price_usd FLOAT,
)
WITH (
    connector = 'http',
    url = 'https://api.coinlore.net/api/tickers/',
    method = 'GET',
    'pagination.type' = 'page-based',
    'pagination.page_param' = 'start',
    'param.limit' = '100',
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
    url = 'https://api.binance.com/api/v3/aggTrades', 
    method = 'GET',
    'pagination.type' = 'cursor-based',
    'pagination.cursor_id' = 'a',
    'pagination.cursor_param' = 'fromId',
    'param.limit' = '500',
    'param.symbol' = 'BTCUSDT',
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
    'pagination.type' = 'header-based',
    'pagination.cursor_param' = 'after',
    'pagination.next_header' = 'cb-after',
    'param.limit' = '40',
    schedule = '*/1 * * * *',
    jq = '.[] | {trade_id, side, size, price, time}',
    'headers.Accept' = 'application/json'
);