SELECT
    symbol,
    toDate(ts) as day,
    avg(close) as avg_price,
    avg(volume) as avg_volume
FROM market.market_ticks
GROUP BY symbol, day
