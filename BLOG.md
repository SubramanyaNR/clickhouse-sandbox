# Kill Two of Three Keepers and ClickHouse Doesn't Go Down. It Goes Read-Only.

[IMAGE: blog-diagram.png]

Most outages I've been paged for were not "the database is down." They were "the database is *up* and doing something I did not expect."

ClickHouse has a lovely example of this, and it's one of the first things I'd want a new team member to see with their own eyes rather than read in the docs. So I built a cluster on one machine specifically to break: [clickhouse-sandbox](https://github.com/nrsubramanya77/clickhouse-sandbox) — three Keeper nodes, three servers, 3.5 million rows of banking data, and scripts whose only job is to take pieces away.

## The Keeper scenario

```
$ make break-keeper

stopping ch-keeper1: two of three keepers remain, quorum holds
insert: ok

stopping ch-keeper2 as well: quorum lost
insert: FAILED - Table is in readonly mode:
        replica_path=/clickhouse/tables/shard_01/call_center_events/replicas/ch-server1

reads do not need Keeper
   ┌─count()─┐
1. │ 2701980 │
   └─────────┘
```

Read that output again, because the last two lines are the point.

With Keeper quorum lost, every replicated table went **read-only**. Writes fail immediately and loudly. But reads are completely unaffected — all 2.7 million rows still served — because reads don't need Keeper at all.

If your dashboards are built on `SELECT` health checks, this failure is invisible to them. Your monitoring stays green while every ingest pipeline you own is failing. That's a much more interesting on-call scenario than "the server is gone," and it's the one people are least prepared for.

The fix isn't clever — alert on `is_readonly` replicas, which the sandbox does — but you have to know the state exists before you think to watch for it.

## Three servers, three shards, two copies each

The default topology is a ring:

```
             shard 1        shard 2        shard 3
ch-server1   replica 1                     replica 2
ch-server2   replica 2      replica 1
ch-server3                  replica 2      replica 1
```

Any single server can die and every shard still has a live copy. Compare that to the two obvious alternatives on three nodes: three shards with no replicas (fast, fragile), or one shard copied three times (safe, no scale-out). Both are available in the repo by changing one variable, because seeing all three side by side is more useful than being told which is best.

```bash
# in .env
LAYOUT=shards3      # 3 shards x 1 replica
LAYOUT=replicas3    # 1 shard  x 3 replicas
```

The circular layout forces one genuinely awkward design problem, and it's worth knowing about before you meet it in production.

**A server in a ring hosts two different shards.** ClickHouse macros are per-server, so a single `{shard}` macro cannot describe that host — it *is* shard 1 and shard 3 simultaneously. My solution: each shard's tables live in their own database (`shard_01`, `shard_02`, `shard_03`), the Keeper replication path is keyed by the database name, and the cluster definition sets `default_database` per replica. One `Distributed` table then reaches the right database on each host automatically.

You query `bank.*` and never think about it. You look at `shard_0N.*` when you want to see what one physical replica actually holds.

## What losing one server actually looks like

```
$ make break-server N=2

stopping ch-server2

every shard still has a live replica, so reads cover all the data
   ┌─transactions─┬─customers─┐
1. │      3471365 │      5000 │
   └──────────────┴───────────┘

writes route to the surviving replica of each shard
insert: ok
```

`docker stop ch-server2` is a real stop, not a simulated one. Server 2 hosts two things in the ring: shard 1's second copy and shard 2's first copy. Killing it takes away one replica of each — but shard 1 still lives on server1, shard 2 still lives on server3, so no shard drops to zero copies.

The interesting part is that nothing routes around the failure explicitly. The query hits the `Distributed` table on a server that's still up, and ClickHouse's own load balancing quietly skips the dead replica for each shard fan-out. The insert does the same thing in reverse: `cityHash64(customer_id)` still says which shard a row belongs to, and the write lands on whichever replica of that shard is actually reachable.

That's the whole trick, and it's also why I ran this one before the Keeper scenario in my own head: this failure is nearly boring. A ClickHouse server dying is the case the architecture was built for. It's the Keeper quorum loss below that catches people off guard.

## What replica catch-up looks like

```
$ make break-catchup

starting ch-server2 and watching its replicas fetch the missing parts
ch-server2 replication queue / max delay (s): 3	15
ch-server2 replication queue / max delay (s): 0	0
```

200,000 rows written while a server was down. It comes back, the queue drains to zero within seconds, row counts match again.

This is the reassuring scenario, and it's worth running precisely because it's reassuring — knowing what a *healthy* recovery looks like on your own hardware is what lets you recognise an unhealthy one at 3am.

## The data is a real bank, with real problems planted in it

3.5 million rows over 90 days: 5,000 customers with accounts and loans, plus transactions, clickstream, app logs, loan applications and support tickets that all reference each other properly.

And three anomalies buried in it, which the queries in the repo go and find. This is the one I like:

```
   ┌─app_version─┬─device──┬─sessions─┬─completed_pct─┐
1. │ 4.2.1       │ android │     6055 │          23.1 │
2. │ 4.1.8       │ android │     1031 │          77.1 │
3. │ 4.2.1       │ ios     │     6904 │          77.9 │
```

A payment funnel where one specific app version on one specific platform completes 23% of the time while everything else sits near 78%. That's a `windowFunnel` query away, and it's exactly the shape of a real production incident — not broken, just quietly worse for a slice of users.

There's also a two-week window where crash rates go from ~3% to 32–44%, and a policy change date after which `INCOME_VERIFICATION_FAIL` jumps from 33% to 71% of gold-tier rejections. The `queries/` folder is eight files that walk through finding these with projections, materialized views, dictionaries and `argMax`.

## Get inside it yourself instead of reading my output

Everything above is copy-pasted from my terminal, and you should not trust it more than that. The whole point of building this as containers instead of a diagram is that you can go sit inside them:

```bash
docker exec -it ch-server1 clickhouse-client   # same as `make shell`
docker exec -it ch-server1 bash                # the filesystem, logs, config
docker exec -it ch-keeper1 bash
echo srvr | nc 127.0.0.1 9181                  # keeper's four-letter-word status
```

Once you're at a `clickhouse-client` prompt, `system.clusters` and `system.replicas` tell you more in five minutes of poking than any of my query files will. Run `SELECT count() FROM shard_01.transactions` on each server and watch the numbers only match on the two that actually hold a copy. Stop a server yourself, run the same query again before reading my explanation of why it still works — you'll believe it more.

## Three ClickHouse things that cost me an afternoon each

**Distributed inserts are asynchronous by default.** My first `ReplacingMergeTree` example showed only the old row after an update, and I spent a while convinced I'd misunderstood the engine. The insert simply hadn't been forwarded yet. `distributed_foreground_insert = 1` when you need to read your own write.

**An alias that reuses a column name shadows it inside aggregates.** Write `countMerge(attempts) AS attempts` and a later `countMerge(attempts)` in the same query now refers to your alias — a `UInt64` — not the original aggregate column. Same trap with `max(event_time) AS event_time`. The error message does not point at the alias.

**Keeper's healthcheck fights you twice.** `http_control` must be nested inside `<keeper_server>`; put it at the top level and it's silently ignored, with no warning. And `localhost` inside the container resolves to `::1` while the listener binds `0.0.0.0`, so you get connection refused from a health check that looks obviously correct. Use `127.0.0.1`. The four-letter-word commands have exactly the same problem.

## One thing I'd tell anyone running multiple ClickHouse servers on one host

Set `max_server_memory_usage_to_ram_ratio`.

By default each server believes it owns the whole machine. Three of them on one box, each confidently planning a query against all available RAM, and the kernel OOM killer makes the decision for you — which it did to me, mid-scenario, while I was busy blaming my query. The sandbox caps it at 0.25 with 1 GB per query.

## Gaps

No users, roles or quotas — `default` has no password, which is fine for a sandbox and nowhere near fine for anything else. An RBAC walkthrough would fit naturally and isn't there yet. No backup and restore scenario either, which would be a good pairing with an S3 target. Alerts exist in Prometheus but there's no Alertmanager routing.

## Takeaways

- **Read-only is a state, not a synonym for down.** Alert on `is_readonly`, or you will find out about Keeper quorum loss from a downstream team.
- **Reads survive Keeper loss. Writes do not.** Your `SELECT 1` health check cannot see this.
- **Circular replication buys real availability on three nodes**, and costs you one genuinely awkward schema decision. Know which you're trading.
- **Cap server memory when you co-locate.** The OOM killer does not negotiate.

Repo: [github.com/nrsubramanya77/clickhouse-sandbox](https://github.com/nrsubramanya77/clickhouse-sandbox)
