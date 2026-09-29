-- transactions is ordered by (customer_id, event_time), which is right for
-- per-customer lookups and wrong for daily totals. The by_day_type projection
-- stores a pre-aggregated copy ordered for the second kind of query, and the
-- optimizer picks it automatically.

-- Look for "ReadFromMergeTree (by_day_type)" in the plan.
EXPLAIN projections = 1
SELECT event_date, txn_type, count(), sum(amount)
FROM shard_01.transactions
GROUP BY event_date, txn_type;

-- Same query with and without it. In queries/07-query-log.sql the first
-- reads a few thousand pre-aggregated rows, the second every transaction.
SELECT event_date, txn_type, count(), sum(amount)
FROM bank.transactions
GROUP BY event_date, txn_type
ORDER BY event_date DESC, txn_type
LIMIT 12;

SELECT event_date, txn_type, count(), sum(amount)
FROM bank.transactions
GROUP BY event_date, txn_type
ORDER BY event_date DESC, txn_type
LIMIT 12
SETTINGS optimize_use_projections = 0;
