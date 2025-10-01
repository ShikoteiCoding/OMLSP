CREATE SECRET kucoin_api_key
WITH (
    backend = 'meta'
) AS 'fjnzekjfnzejfnzejfnzejfjzebf';

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
    'headers.Content-Type' = 'application/json',
    -- TODO: improve sqlglot parser to accept non quoted values
    'headers.X-API-Key' = 'SECRET kucoin_api_key' 
);