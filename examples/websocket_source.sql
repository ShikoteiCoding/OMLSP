CREATE TABLE binance_mini_tickers (
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