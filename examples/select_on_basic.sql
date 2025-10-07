-- Sample SELECT query for basic.sql

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