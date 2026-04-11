-- assert_ohlcv_high_gte_low.sql
-- Fails if any candle has high_price < low_price (physically impossible).
-- A non-empty result set means the test has failed.
SELECT
    window_start,
    symbol,
    high_price,
    low_price
FROM {{ ref('gold_ohlcv_enriched') }}
WHERE high_price < low_price
