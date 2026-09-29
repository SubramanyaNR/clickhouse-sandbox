-- Every query is logged on the server that ran it. Read the logs from all of
-- them to see what was expensive. system.query_log is flushed every few
-- seconds, so very recent queries may take a moment to show up.

SYSTEM FLUSH LOGS;

SELECT
    event_time,
    hostName() AS host,
    query_duration_ms AS ms,
    read_rows,
    formatReadableSize(memory_usage) AS memory,
    substring(replaceRegexpAll(query, '\\s+', ' '), 1, 90) AS query
FROM clusterAllReplicas(every_node, system.query_log)
WHERE type = 'QueryFinish'
  AND is_initial_query
  AND query_kind = 'Select'
  AND query NOT ILIKE '%system.%'
  AND event_time > now() - INTERVAL 1 HOUR
ORDER BY read_rows DESC
LIMIT 15;

-- A query against a Distributed table becomes one initial query plus one
-- child query per shard. initial_query_id ties them together. Shards with a
-- replica on the server that received the query are read locally
-- (prefer_localhost_replica), so the host list is often shorter than you
-- might expect.
SELECT
    initial_query_id,
    count() AS parts_of_query,
    groupArray(hostName()) AS hosts,
    max(query_duration_ms) AS slowest_ms
FROM clusterAllReplicas(every_node, system.query_log)
WHERE type = 'QueryFinish' AND event_time > now() - INTERVAL 1 HOUR AND query_kind = 'Select'
GROUP BY initial_query_id
HAVING parts_of_query > 1
ORDER BY slowest_ms DESC
LIMIT 5;
