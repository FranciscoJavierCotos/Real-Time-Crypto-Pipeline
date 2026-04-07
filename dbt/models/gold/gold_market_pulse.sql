{{ config(
    materialized='incremental',
    unique_key=['window_start', 'symbol'],
    incremental_strategy='merge'
) }}

-- Master Gold table: joins all enrichment signals per 1-minute window.
--
-- Answers business questions such as:
--   "Is BTC trending bullish or bearish right now?"
--   "Does the spread widen during volatile minutes?"
--   "Is the current volume spike accompanied by directional buying?"
--   "What time of day sees the most aggressive buying activity?"

WITH ohlcv AS (
    SELECT *
    FROM {{ ref('gold_ohlcv_enriched') }}
    {% if is_incremental() %}
    WHERE window_start >= (
        SELECT COALESCE(MAX(window_start) - INTERVAL 5 MINUTES, TIMESTAMP('1970-01-01'))
        FROM {{ this }}
    )
    {% endif %}
),

liquidity AS (
    SELECT *
    FROM {{ ref('gold_liquidity_1min') }}
),

-- Pick the last ticker snapshot per 1-minute window (most up-to-date 24h stats)
ticker_latest AS (
    SELECT
        DATE_TRUNC('minute', event_time)  AS window_start,
        symbol,
        price_change_pct                  AS price_change_pct_24h,
        high_price                        AS high_24h,
        low_price                         AS low_24h,
        volume_24h,
        weighted_avg_price,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('minute', event_time), symbol
            ORDER BY event_time DESC
        )                                 AS rn
    FROM {{ source('btc', 'landing_btc_ticker') }}
),

ticker AS (
    SELECT * FROM ticker_latest WHERE rn = 1
),

-- Silver trade-derived aggregates (cross-validates kline volume)
silver AS (
    SELECT window_start, symbol, avg_price, total_volume
    FROM {{ ref('silver_btc_aggregates') }}
),

-- 30-candle rolling average volume for spike detection
volume_baseline AS (
    SELECT
        window_start,
        symbol,
        total_volume,
        AVG(total_volume) OVER (
            PARTITION BY symbol
            ORDER BY window_start
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS rolling_30min_avg_volume
    FROM silver
)

SELECT
    o.window_start,
    o.window_end,
    o.symbol,

    -- ── OHLC ────────────────────────────────────────────────────────────────
    o.open_price,
    o.high_price,
    o.low_price,
    o.close_price,
    o.volume                          AS kline_volume,
    o.trade_count,
    o.candle_direction,
    o.price_range_pct,
    o.taker_buy_ratio,

    -- ── Liquidity ───────────────────────────────────────────────────────────
    l.avg_spread,
    l.avg_spread_pct,
    l.max_spread,
    l.bid_ask_imbalance,
    l.snapshot_count                  AS orderbook_snapshots,

    -- ── 24-hour context ─────────────────────────────────────────────────────
    t.price_change_pct_24h,
    t.high_24h,
    t.low_24h,
    t.volume_24h,
    t.weighted_avg_price,

    -- ── Trade-stream cross-check ─────────────────────────────────────────────
    s.avg_price                       AS trade_avg_price,
    s.total_volume                    AS trade_volume,

    -- ── Volume spike detection ───────────────────────────────────────────────
    ROUND(v.rolling_30min_avg_volume, 6) AS rolling_30min_avg_volume,
    CASE
        WHEN v.rolling_30min_avg_volume > 0
         AND s.total_volume > 2 * v.rolling_30min_avg_volume
        THEN TRUE
        ELSE FALSE
    END                               AS volume_spike,

    -- ── Momentum signal ──────────────────────────────────────────────────────
    -- Combines candle direction with taker buy ratio to classify buying/selling pressure
    CASE
        WHEN o.taker_buy_ratio >= 0.65 AND o.candle_direction = 'bullish' THEN 'strong_buy'
        WHEN o.taker_buy_ratio >= 0.55 AND o.candle_direction = 'bullish' THEN 'moderate_buy'
        WHEN o.taker_buy_ratio <= 0.35 AND o.candle_direction = 'bearish' THEN 'strong_sell'
        WHEN o.taker_buy_ratio <= 0.45 AND o.candle_direction = 'bearish' THEN 'moderate_sell'
        ELSE 'neutral'
    END                               AS momentum_signal

FROM ohlcv o
LEFT JOIN liquidity      l ON o.window_start = l.window_start AND o.symbol = l.symbol
LEFT JOIN ticker         t ON o.window_start = t.window_start AND o.symbol = t.symbol
LEFT JOIN silver         s ON o.window_start = s.window_start AND o.symbol = s.symbol
LEFT JOIN volume_baseline v ON o.window_start = v.window_start AND o.symbol = v.symbol
