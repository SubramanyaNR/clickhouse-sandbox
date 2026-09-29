-- What is loaded, how big it is on disk, and how well it compresses.
-- clusterAllReplicas reads system tables from every server, so replicas are
-- counted twice; the uncompressed/compressed ratio is unaffected by that.

SELECT
    table,
    sum(rows) / 2 AS rows,
    formatReadableSize(sum(data_compressed_bytes) / 2) AS on_disk,
    formatReadableSize(sum(data_uncompressed_bytes) / 2) AS raw,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) AS ratio
FROM clusterAllReplicas(every_node, system.parts)
WHERE active AND database LIKE 'shard_%'
GROUP BY table
ORDER BY sum(data_compressed_bytes) DESC;

-- Per column, for the biggest table. LowCardinality columns should be near
-- the bottom.
SELECT
    name,
    type,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) AS ratio
FROM clusterAllReplicas(every_node, system.columns)
WHERE database = 'shard_01' AND table = 'transactions'
GROUP BY name, type
ORDER BY sum(data_compressed_bytes) DESC;
