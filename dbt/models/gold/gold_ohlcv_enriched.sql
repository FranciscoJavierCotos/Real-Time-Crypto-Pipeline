{{ config(
    materialized='incremental',
    unique_key=['window_start', 'symbol'],
    incremental_strategy='merge'
) }}

SELECT
    DATE_TRUNC('minute', open_time)                               AS window_start,
    DATE_TRUNC('minute', open_time) + INTERVAL 1 MINUTE           AS window_end,
    symbol,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    quote_volume,
    trade_count,
    taker_buy_volume,
    taker_buy_quote_volume,
    -- Direction of the candle
    CASE WHEN close_price >= open_price THEN 'bullish' ELSE 'bearish' END AS candle_direction,
    -- Intra-minute volatility: high-low range as % of open
    ROUND((high_price - low_price) / NULLIF(open_price, 0) * 100, 4)     AS price_range_pct,
    -- Taker buy ratio: fraction of volume that was aggressive buying
    -- > 0.5 means more buyers than sellers; < 0.5 means more sellers
    ROUND(taker_buy_volume / NULLIF(volume, 0), 4)                        AS taker_buy_ratio
FROM {{ source('btc', 'landing_btc_klines') }}
WHERE open_price > 0
  AND volume > 0
{% if is_incremental() %}
  AND open_time >= (
      SELECT COALESCE(MAX(window_start) - INTERVAL 5 MINUTES, TIMESTAMP('1970-01-01'))
      FROM {{ this }}
  )
{% endif %}
