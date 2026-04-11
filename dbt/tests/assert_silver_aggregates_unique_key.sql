-- assert_silver_aggregates_unique_key.sql
-- Fails if any (window_start, symbol) combination appears more than once.
-- The incremental merge uses this as the unique_key, so duplicates indicate
-- a merge failure or a reprocessing bug.
SELECT
    window_start,
    symbol,
    COUNT(*) AS occurrences
FROM {{ ref('silver_btc_aggregates') }}
GROUP BY window_start, symbol
HAVING COUNT(*) > 1
