-- assert_ohlcv_close_within_hl.sql
-- Fails if close_price or open_price falls outside the [low_price, high_price] range.
-- A non-empty result set means the test has failed.
SELECT
    window_start,
    symbol,
    open_price,
    high_price,
    low_price,
    close_price
FROM {{ ref('gold_ohlcv_enriched') }}
WHERE close_price > high_price
   OR close_price < low_price
   OR open_price  > high_price
   OR open_price  < low_price
