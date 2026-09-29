# clickhouse-sandbox: working context

Notes for picking this project up in a new session. User documentation is in
`README.md`.

## Status

Complete and verified on 2026-09-16. Not pushed anywhere, no `.git`.

## What was verified

- From nothing: `make clean && make up && make load`, then every file in
  `queries/` runs without error, then `make up-monitoring`.
- All three layouts. `circular`: 6 replica pairs with identical row counts per
  shard. `shards3`: 3,000 rows split ~1,000 per shard. `replicas3`: 3,000 rows
  on each of the three servers.
- Load: 3,471,365 rows in ~3.5 minutes (90 days). Materialized views
  populated; projection used by the planner (`ReadFromMergeTree (by_day_type)`);
  dictionary loaded 5,000 keys.
- The planted anomalies show up: payment funnel completion 23.1% on
  4.2.1/android vs 77-80% for every other version/platform; crash rate
  32-44% for two weeks vs ~3%; INCOME_VERIFICATION_FAIL share of rejections
  33% -> 71% (gold) and 26% -> 65% (personal) after the change date.
- `bin/break server 2`: full count served, insert ok.
- `bin/break keeper`: one keeper down, insert ok; two down, insert fails with
  "Table is in readonly mode", reads fine, all replicas `is_readonly=1`.
- `bin/break catchup`: 200,000 rows inserted with server2 down, server2's
  replication queue went 3 -> 0 within seconds, row counts equal afterwards.
- Monitoring: 7 targets up, 7 rules loaded and healthy, every dashboard query
  returns series, ClickHouse datasource answers SQL through Grafana's API.
- Kafka engine ingest from kafka-sandbox in linked mode: 6 partitions split
  across 3 servers, lag 20-90, 178,880 rows / 178,879 unique txn_ids.

## Design decisions and why

**Circular layout with one database per shard.** On three nodes, a node in a
3x2 ring hosts two different shards. Macros are per server, so a `{shard}`
macro cannot describe that. Each shard gets its own database, the Keeper path
uses the database name (`/clickhouse/tables/shard_01/transactions`), and the
cluster definition sets `default_database` per replica so one Distributed
table (`Distributed(bank, '', table)`) reaches the right database on each
host. The same scheme covers `shards3` and `replicas3`, so `bin/ch schema`
reads `system.clusters` and needs no per-layout code.

**`{any_db}` in distributed.sql.** `CREATE TABLE ... AS shard_01.x` fails on a
server that does not host shard 1 (shards3). The script substitutes the first
shard database present on that server.

**Two clusters defined**: `bank` (the data layout) and `every_node` (one shard
per server, no replicas) for `clusterAllReplicas`-style system table reads and
anything that must run once per host.

**One keeper.xml for all three.** `<server_id from_env="KEEPER_ID"/>`, and
the same trick for the server `replica` macro.

**Keeper healthcheck is HTTP `/ready` on 9182 at 127.0.0.1.** Two problems
solved on the way: `http_control` must be inside `<keeper_server>`, not at the
top level (it is silently ignored otherwise); and `localhost` resolves to
`::1` inside the container while the listener is `0.0.0.0`, so
`localhost` gets connection refused. The four-letter-word commands have the
same issue: `echo srvr | nc 127.0.0.1 9181` works, `localhost` does not.

**Sharding key `cityHash64(customer_id)` on every table**, so a customer's
transactions, sessions and tickets are co-located, which makes
`optimize_skip_unused_shards` and shard-local joins possible.

**Loader goes through a Python container, not clickhouse-client reading
bankgen's files.** bankgen writes `{"key":..., "value":{...}}` wrappers.
`tools/export.py` unwraps and streams to stdout, piped into
`clickhouse-client ... FORMAT JSONEachRow` with
`--insert_distributed_sync=1` so counts printed after each table are final.
Timestamps like `2026-09-05T10:38:37.880753+00:00` parse because the default
profile sets `date_time_input_format = best_effort`.

**Kafka engine table parses timestamps in the MV** with
`parseDateTime64BestEffort`, rather than relying on the profile setting
reaching background consumers.

**Memory caps**: `max_server_memory_usage_to_ram_ratio` 0.25 and 1 GB per
query. Needed because three servers share one machine and each would
otherwise believe it owns all of it.

**bankgen is vendored** in `tools/bankgen`, same copy as kafka-sandbox
(BOOTSTRAP_SERVERS from the environment). Generation is skipped when the
`bankgen-data` volume already has that many days (`/data/.days-N` marker).

**Makefile adds the linked overlay whenever `streaming-net` exists**, so no
target can recreate the servers off the shared network by accident.

**flink-pipeline creates a `flink` database on ch-server1** (non-replicated
Kafka engine tables for its outputs). `make clean` removes it with everything
else.

## Gotchas

- Inserts into Distributed tables are asynchronous by default. The first
  version of `02-replacing-merge-tree.sql` showed only the old row because
  the update had not been sent yet. Now uses
  `distributed_foreground_insert = 1`.
- An alias that reuses a column name shadows it inside aggregates:
  `countMerge(attempts) AS attempts` makes a later `countMerge(attempts)`
  refer to the alias (UInt64). Same for `max(event_time) AS event_time`.
- `merge()` with a regex needs `REGEXP('^shard_')`; the virtual column is
  `_database`, not `database`.
- **Host OOM.** With the full kafka-sandbox (~3.2 GB) plus this (~1.9 GB) and
  heavy queries, the kernel killed a ClickHouse server mid-scenario. Stop
  kafka-sandbox's ui/logging profiles when running both.
- `bin/break catchup` inserts 200,000 copied transactions each run, so counts
  drift upward. `make clean && make up && make load` resets.
- bankgen's data runs past its own anchor date (2026-09-08) to 2026-10-01 for
  a few hundred late-scheduled events. Queries that need the anomaly windows
  use fixed dates rather than `max(event_date)`.

## Known gaps / next steps

- No users, roles or quotas; `default` has no password. An RBAC example would
  fit the playground.
- No backup/restore scenario (`BACKUP TABLE ... TO Disk`, or S3 to MinIO).
- `chdig` (top for ClickHouse) was considered and not bundled.
- No alertmanager; alerts are visible in Prometheus only.
- The Kafka engine only covers `bank.dbo.transactions`. The flink-pipeline
  project writes enriched data here instead.
