-- customer_dict holds name, segment, city and risk score in memory on every
-- server. dictGet replaces a join for enrichment and runs on each shard
-- without shipping the customers table around.

SELECT
    dictGet('bank.customer_dict', 'segment', customer_id) AS segment,
    count() AS txns,
    round(avg(amount), 2) AS avg_amount
FROM bank.transactions
WHERE status = 'failed'
GROUP BY segment
ORDER BY txns DESC;

-- High-risk customers with the most failed EMI payments.
SELECT
    customer_id,
    dictGet('bank.customer_dict', 'name', customer_id) AS name,
    dictGet('bank.customer_dict', 'risk_score', customer_id) AS risk,
    countIf(status != 'success') AS failed_emis
FROM bank.transactions
WHERE txn_type = 'emi_payment'
GROUP BY customer_id
HAVING risk < 500
ORDER BY failed_emis DESC
LIMIT 10;

SELECT name, status, element_count, formatReadableSize(bytes_allocated) AS memory, last_successful_update_time
FROM system.dictionaries;
