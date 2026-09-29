-- daily_spend and emi_daily are filled by materialized views on insert into
-- transactions. Reading them touches thousands of rows instead of a million.

-- Top spend categories over the last 30 days of data.
SELECT merchant_category, sum(txn_count) AS txns, sum(amount) AS spend
FROM bank.daily_spend
WHERE event_date >= (SELECT max(event_date) FROM bank.daily_spend) - 30
GROUP BY merchant_category
ORDER BY spend DESC;

-- The same answer from the raw table, for comparison. Check the read_rows
-- of both in queries/07-query-log.sql.
SELECT merchant_category, count() AS txns, sum(amount) AS spend
FROM bank.transactions
WHERE status = 'success' AND txn_type != 'emi_payment'
  AND event_date >= (SELECT max(event_date) FROM bank.transactions) - 30
GROUP BY merchant_category
ORDER BY spend DESC;

-- EMI failure rate by week. Aliases must not reuse the column names: an
-- alias called attempts would shadow the state column inside countMerge().
-- The aggregate states merge across shards and
-- across parts, so the unique customer count is exact per week, not a sum of
-- per-day uniques.
SELECT
    toMonday(event_date) AS week,
    countMerge(attempts) AS emi_attempts,
    round(100 * countIfMerge(failures) / emi_attempts, 1) AS failure_pct,
    uniqMerge(customers) AS paying_customers
FROM bank.emi_daily
GROUP BY week
ORDER BY week;
