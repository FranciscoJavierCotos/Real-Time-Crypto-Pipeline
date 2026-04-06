{{ config(
    materialized='incremental',
    unique_key=['window_start', 'symbol'],
    incremental_strategy='merge'
) }}

-- Aggregate order-book snapshots into 1-minute liquidity windows.
-- Each snapshot is a sampled best-bid/ask from Binance bookTicker (~1 per 5 s),
-- so each window typically contains ~12 data points.
SELECT
    DATE_TRUNC('minute', event_time)                              AS window_start,
    DATE_TRUNC('minute', event_time) + INTERVAL 1 MINUTE          AS window_end,
    symbol,
    -- Spread metrics (USD)
    ROUND(AVG(spread), 4)                                         AS avg_spread,
    ROUND(MAX(spread), 4)                                         AS max_spread,
    ROUND(MIN(spread), 4)                                         AS min_spread,
    -- Spread as % of mid price
    ROUND(AVG(spread_pct), 6)                                     AS avg_spread_pct,
    -- Depth at top of book
    ROUND(AVG(best_bid_qty), 4)                                   AS avg_bid_qty,
    ROUND(AVG(best_ask_qty), 4)                                   AS avg_ask_qty,
    -- Bid-ask imbalance: +1 = pure buy pressure, -1 = pure sell pressure
    -- Formula: (bid_qty - ask_qty) / (bid_qty + ask_qty)
    ROUND(
        AVG(
            CASE WHEN (best_bid_qty + best_ask_qty) > 0
                 THEN (best_bid_qty - best_ask_qty) / (best_bid_qty + best_ask_qty)
                 ELSE 0.0
            END
        ), 4
    )                                                              AS bid_ask_imbalance,
    COUNT(*)                                                       AS snapshot_count
FROM {{ source('btc', 'landing_btc_orderbook') }}
WHERE spread >= 0
{% if is_incremental() %}
  AND event_time >= (
      SELECT COALESCE(MAX(window_start) - INTERVAL 5 MINUTES, TIMESTAMP('1970-01-01'))
      FROM {{ this }}
  )
{% endif %}
GROUP BY 1, 2, 3
