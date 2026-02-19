CREATE DATABASE IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.market_ticks
(
    symbol String,
    ts DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    return_log Float64,
    sma_5 Float64
)
ENGINE = MergeTree
ORDER BY (symbol, ts);
