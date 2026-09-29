-- How the Distributed tables spread and route data.

-- Rows per shard. _shard_num is a virtual column on Distributed tables.
SELECT _shard_num AS shard, count() AS rows, uniq(customer_id) AS customers
FROM bank.transactions
GROUP BY shard
ORDER BY shard;

-- Sharding by cityHash64(customer_id) means one customer lives on one shard.
SELECT customer_id, groupUniqArray(_shard_num) AS shards
FROM bank.transactions
WHERE customer_id IN ('C-100001', 'C-100002', 'C-100003')
GROUP BY customer_id;

-- With optimize_skip_unused_shards the filter on the sharding key is used to
-- send the query to one shard instead of three. Compare the
-- parts_of_query column for these two in queries/07-query-log.sql.
SELECT count(), sum(amount) FROM bank.transactions WHERE customer_id = 'C-100001';

SELECT count(), sum(amount) FROM bank.transactions WHERE customer_id = 'C-100001'
SETTINGS optimize_skip_unused_shards = 1;
