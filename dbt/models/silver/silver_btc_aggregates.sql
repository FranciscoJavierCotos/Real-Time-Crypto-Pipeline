{{ config(
    materialized='incremental',
    unique_key=['window_start', 'symbol'],
    incremental_strategy='merge'
) }}

SELECT
    DATE_TRUNC('minute', event_time)                      AS window_start,
    DATE_TRUNC('minute', event_time) + INTERVAL 1 MINUTE  AS window_end,
    symbol,
    AVG(price)                                            AS avg_price,
    SUM(volume)                                           AS total_volume,
    COUNT(*)                                              AS trade_count
FROM {{ source('btc', 'landing_btc_trades') }}
WHERE price > 0
  AND volume > 0
  AND event_time IS NOT NULL
{% if is_incremental() %}
  AND event_time >= (
      SELECT COALESCE(MAX(window_start) - INTERVAL 5 MINUTES, TIMESTAMP('1970-01-01'))
      FROM {{ this }}
  )
{% endif %}
GROUP BY 1, 2, 3
