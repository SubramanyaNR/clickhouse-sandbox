-- ReplacingMergeTree deduplicates when parts merge, not when rows arrive.
-- Until then both versions are visible.

-- A risk score update for one customer arrives as a new row.
-- Inserts into a Distributed table are queued and sent in the background by
-- default; foreground insert waits, so the next SELECT sees the row.
INSERT INTO bank.customers
SETTINGS distributed_foreground_insert = 1
SELECT customer_id, name, segment, city, kyc_status, 312 AS risk_score,
       onboarded_at, email, phone, now64(3) AS event_time
FROM bank.customers FINAL
WHERE customer_id = 'C-100042';

-- Both rows, old and new.
SELECT customer_id, risk_score, event_time
FROM bank.customers
WHERE customer_id = 'C-100042'
ORDER BY event_time;

-- FINAL collapses to the newest version at read time.
SELECT customer_id, risk_score, event_time
FROM bank.customers FINAL
WHERE customer_id = 'C-100042';

-- The same answer without FINAL, which is how it is often written for large
-- scans where FINAL is too expensive.
SELECT customer_id, argMax(risk_score, event_time) AS latest_risk_score, max(event_time) AS updated_at
FROM bank.customers
WHERE customer_id = 'C-100042'
GROUP BY customer_id;
