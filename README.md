# clickhouse-sandbox

A replicated, sharded ClickHouse cluster on one machine: three Keeper nodes,
three servers, 3.5 million rows of banking data, a folder of worked queries,
and scripts that break the cluster so you can watch what still works.

## Start

```bash
make up          # keepers, servers, schema
make load        # generate 90 days of bankgen data and load it (~3.5 min)
make cluster     # topology and replica health
```

| What | Where |
|---|---|
| HTTP / Play UI | http://localhost:38123/play |
| Native protocol | `localhost:38900` |
| Other servers (HTTP) | `localhost:38124`, `localhost:38125` |
| Grafana (`make up-monitoring`) | http://localhost:38300 |
| Prometheus (`make up-monitoring`) | http://localhost:38090 |

`make shell` opens `clickhouse-client` on server 1 (`make shell N=3` for
another). Change `PORT_PREFIX` in `.env` if 38xxx is taken.

Memory: about 1.9 GB with data loaded and monitoring on (three servers at
~600 MB, three keepers at ~45 MB, Grafana and Prometheus ~150 MB). 

## Topology

Three servers, three shards, two copies of each shard, arranged in a ring:

```
             shard 1        shard 2        shard 3
ch-server1   replica 1                     replica 2
ch-server2   replica 2      replica 1
ch-server3                  replica 2      replica 1
```

Any single server can go down and every shard still has a live copy, which is
the point of doing it this way on three nodes rather than three shards with
no copies (fast, fragile) or one shard copied three times (safe, no
scale-out). Both of those are available too:

```bash
# in .env
LAYOUT=shards3      # 3 shards x 1 replica
LAYOUT=replicas3    # 1 shard  x 3 replicas
```

then `make clean && make up && make load`.

Because one server holds replicas of two different shards, each shard's
tables live in their own database (`shard_01`, `shard_02`, `shard_03`) and
the Keeper replication path is keyed by that database. The `bank` database on
every server holds `Distributed` tables over them, using the cluster's
per-shard `default_database`. Query `bank.*`; look at `shard_0N.*` when you
want to see what one replica holds.

Keeper runs as three separate nodes. One can fail without consequence; lose
two and replicated tables go read-only (see `make break-keeper`).

## Data and schema

`make load` runs [bankgen](tools/bankgen): 5,000 customers with accounts and
loans, and 90 days of transactions, clickstream, app logs, loan applications
and support tickets that reference them. Rows are sharded by
`cityHash64(customer_id)`, so one customer's rows sit on one shard.

| Table | Engine | Notes |
|---|---|---|
| `customers`, `accounts`, `loans` | ReplicatedReplacingMergeTree | versioned by `event_time`; use `FINAL` or `argMax` for current state |
| `transactions` | ReplicatedMergeTree | ordered by customer, with a `by_day_type` projection for daily rollups |
| `clickstream`, `application_logs` | ReplicatedMergeTree | ordered for version/device analysis; logs have a 1-year TTL |
| `loan_applications`, `call_center_events` | ReplicatedMergeTree | |
| `daily_spend` | ReplicatedSummingMergeTree | filled by a materialized view on `transactions` |
| `emi_daily` | ReplicatedAggregatingMergeTree | EMI attempts, failures, unique customers as aggregate states |
| `customer_dict` | dictionary | name, segment, city, risk score in memory for `dictGet` |

Schema is in `schema/local.sql` (per shard) and `schema/distributed.sql`.
`make schema` is idempotent.

## Queries

`queries/` is a playground. Each file runs as-is:

```bash
make query Q=queries/05-planted-anomalies.sql
```

| File | Shows |
|---|---|
| `01-first-look.sql` | table sizes, compression per table and per column |
| `02-replacing-merge-tree.sql` | an update creating a second row, `FINAL`, `argMax` |
| `03-materialized-views.sql` | rollup tables vs raw scans, `-State` / `-Merge` aggregates |
| `04-projections.sql` | `EXPLAIN projections = 1`, same query with the projection off |
| `05-planted-anomalies.sql` | crash spike by week, `windowFunnel` drop-off, a policy change |
| `06-dictionaries.sql` | `dictGet` enrichment instead of joins |
| `07-query-log.sql` | reading `system.query_log` from every server, fan-out per query |
| `08-distributed.sql` | rows per shard, shard pruning with `optimize_skip_unused_shards` |

The anomaly queries find what bankgen planted. The payment funnel, during the
two weeks of the crash spike:

```
   ┌─app_version─┬─device──┬─sessions─┬─completed_pct─┐
1. │ 4.2.1       │ android │     6055 │          23.1 │
2. │ 4.1.8       │ android │     1031 │          77.1 │
3. │ 4.2.1       │ ios     │     6904 │          77.9 │
...
```

## Breaking it

```bash
make break-server N=2    # stop a server: reads still cover all data, inserts still work
make break-keeper        # stop one keeper (fine), then a second (read-only)
make break-catchup       # write while a replica is down, watch it fetch the parts
make heal                # start everything
```

From `make break-keeper`:

```
stopping ch-keeper1: two of three keepers remain, quorum holds
insert: ok

stopping ch-keeper2 as well: quorum lost
insert: FAILED - Table is in readonly mode: replica_path=/clickhouse/tables/shard_01/call_center_events/replicas/ch-server1.

reads do not need Keeper
   ┌─count()─┐
1. │ 2701980 │
   └─────────┘
```

From `make break-catchup`:

```
starting ch-server2 and watching its replicas fetch the missing parts
ch-server2 replication queue / max delay (s): 3	15
ch-server2 replication queue / max delay (s): 0	0
```

## Monitoring

`make up-monitoring` adds Prometheus and Grafana. Servers and keepers expose
`/metrics` natively on 9363, so there is no exporter.

The **ClickHouse · Cluster** dashboard covers servers and keepers up, Keeper
leader count, read-only replicas, query and insert rates, failed queries,
replication queue and delay, parts per partition, merges, memory, queued
Distributed inserts, and Keeper latency. Grafana also has the ClickHouse
datasource plugin, pointed at `ch-server1`, for SQL panels.

Alert rules (`prometheus/rules/clickhouse.yml`): server down, read-only
replica, replication lag, too many parts, Distributed insert backlog, keeper
down, no Keeper leader.


## Stop

```bash
make down      # keep data
make clean     # remove volumes
```

## Layout

```
docker-compose.yml         keepers, servers, bankgen, monitoring
docker-compose.linked.yml  overlay that joins streaming-net
config/keeper/keeper.xml   one file for all keepers, id from the environment
config/server/             macros, Keeper hosts, Prometheus endpoint, users
config/layouts/            circular, shards3, replicas3 cluster definitions
schema/                    local (per shard) and distributed DDL
queries/                   worked examples
bin/ch                     schema, load, cluster, query, shell
bin/break                  failure scenarios
tools/                     bankgen and the JSON exporter for loading
prometheus/ grafana/       monitoring
integrations/kafka.sql     Kafka engine ingest from kafka-sandbox
```
