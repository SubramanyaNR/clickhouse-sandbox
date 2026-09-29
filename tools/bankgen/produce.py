"""Phase 2 driver: emit events to Kafka (or to files for a dry run).

Two modes, and the distinction matters:

  backfill  Generates BACKFILL_DAYS of history in event-time order as fast as
            the sink allows. Use this to populate Iceberg / history topics.
            NOTE: if you route backfill through windowed Flink jobs, every
            window will fire at once when the watermark races to now. Either
            land backfill directly, or accept the burst knowingly.

  live      Emits at EVENTS_PER_SEC with event_time == wall clock. This is what
            you demo against.

Usage:
    python produce.py --mode backfill --sink file --out ./data
    python produce.py --mode live --sink kafka
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import config as C
import events as E
import universe as U


# ---------------------------------------------------------------------- sinks
class FileSink:
    """Writes newline-delimited JSON per topic. For dry runs and inspection."""

    def __init__(self, outdir: str):
        os.makedirs(outdir, exist_ok=True)
        self.outdir = outdir
        self.handles = {}
        self.counts = defaultdict(int)

    def send(self, topic, key, value):
        if topic not in self.handles:
            fname = os.path.join(self.outdir, f"{topic}.jsonl")
            self.handles[topic] = open(fname, "w")
        self.handles[topic].write(json.dumps({"key": key, "value": value}) + "\n")
        self.counts[topic] += 1

    def close(self):
        for h in self.handles.values():
            h.close()


class KafkaSink:
    def __init__(self, bootstrap: str):
        try:
            from confluent_kafka import Producer
        except ImportError:
            sys.exit("confluent-kafka not installed:  pip install confluent-kafka")
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "linger.ms": 50,
            "compression.type": "lz4",
            "acks": "1",
        })
        self.counts = defaultdict(int)

    def send(self, topic, key, value):
        self.producer.produce(topic, key=str(key).encode(),
                              value=json.dumps(value).encode())
        self.counts[topic] += 1
        if sum(self.counts.values()) % 1000 == 0:
            self.producer.poll(0)

    def close(self):
        self.producer.flush(30)


# ------------------------------------------------------------------- helpers
def maybe_late(rng, ts):
    """Push a small fraction of events backwards in event time.

    Without this your watermark strategy and allowed-lateness config are
    completely untested, and you will find out in production instead.
    """
    if rng.random() < C.LATE_EVENT_RATE:
        return ts - timedelta(seconds=rng.randint(30, C.LATE_EVENT_MAX_LAG_SEC))
    return ts


def load_cohort_ids(path="cohort.json"):
    """Cohort customers are 10 out of 5,000. Picking uniformly in live mode
    means any given demo customer surfaces once every ~50s, so their data looks
    frozen while you're demoing against them. Bias the picker toward them."""
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [c["customer_id"] for c in json.load(f)]


def index_universe(raw: dict):
    by_cust_accounts = defaultdict(list)
    for a in raw["accounts"]:
        by_cust_accounts[a["customer_id"]].append(a)
    by_cust_loans = defaultdict(list)
    for l in raw["loans"]:
        by_cust_loans[l["customer_id"]].append(l)
    return by_cust_accounts, by_cust_loans


def emit_static(sink, raw):
    """Seed the compacted entity topics before any events reference them."""
    for c in raw["customers"]:
        sink.send(*E.customer_record(c))
    for a in raw["accounts"]:
        sink.send(*E.account_record(a))
    for l in raw["loans"]:
        sink.send(*E.loan_record(l))


def emit_day(rng, sink, raw, accounts_by_cust, loans_by_cust, day):
    """One simulated day of activity across all customers."""
    for cust in raw["customers"]:
        prof = C.PERSONA_PROFILE[cust["persona"]]
        accts = accounts_by_cust[cust["customer_id"]]
        loans = loans_by_cust.get(cust["customer_id"], [])
        if not accts:
            continue

        # --- transactions, clustered in waking hours
        n_txn = rng.randint(*prof["txn_per_day"])
        for _ in range(n_txn):
            ts = day + timedelta(hours=rng.triangular(6, 23, 19),
                                 minutes=rng.randint(0, 59))
            sink.send(*E.transaction(rng, cust, rng.choice(accts),
                                     maybe_late(rng, ts), loans))

        # --- EMI payments cluster on the loan's due day
        for loan in loans:
            if day.day == loan["emi_day"] and loan["status"] == "active":
                offset = -rng.randint(0, prof["days_early_max"]) \
                    if rng.random() < prof["emi_on_time_prob"] else rng.randint(1, 25)
                ts = day + timedelta(days=offset, hours=rng.randint(8, 21))
                sink.send(*E.transaction(rng, cust, accts[0], ts, [loan]))

        # --- app sessions
        if rng.random() < prof["sessions_per_week"][1] / 7.0:
            ts = day + timedelta(hours=rng.triangular(7, 23, 20))
            for ev in E.clickstream_session(rng, cust, ts):
                sink.send(*ev)

        # --- support tickets
        if rng.random() < prof["ticket_per_month_prob"] / 30.0:
            ts = day + timedelta(hours=rng.randint(9, 18))
            sink.send(*E.call_center(rng, cust, ts, loans))

        # --- loan applications (rare per customer per day)
        if rng.random() < 0.0012:
            for ev in E.loan_application(rng, cust, day + timedelta(hours=rng.randint(9, 20))):
                sink.send(*ev)

    # --- app logs are volume-driven, not per-customer.
    # Volume matters: TUMBLE(5 min) gives 288 windows/day, so anything under
    # ~5k/day leaves single-digit counts per window and your crash-rate
    # aggregation becomes statistical noise.
    for _ in range(rng.randint(8_000, 15_000)):
        ts = day + timedelta(hours=rng.triangular(0, 24, 20), minutes=rng.randint(0, 59))
        sink.send(*E.app_log(rng, maybe_late(rng, ts), rng.choice(raw["customers"])))


# ---------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["backfill", "live"], default="backfill")
    ap.add_argument("--sink", choices=["file", "kafka"], default="file")
    ap.add_argument("--out", default="./data")
    ap.add_argument("--days", type=int, default=C.BACKFILL_DAYS)
    ap.add_argument("--universe", default="universe.json")
    ap.add_argument("--skip-static", action="store_true")
    ap.add_argument("--cohort", default="cohort.json",
                    help="cohort manifest; live mode biases traffic toward these")
    ap.add_argument("--cohort-bias", type=float, default=0.30,
                    help="fraction of live events for cohort customers (0 to disable)")
    args = ap.parse_args()

    if not os.path.exists(args.universe):
        sys.exit(f"{args.universe} not found — run:  python universe.py")

    raw = U.load(args.universe)
    accounts_by_cust, loans_by_cust = index_universe(raw)
    rng = random.Random(C.SEED + 1)

    sink = FileSink(args.out) if args.sink == "file" else KafkaSink(C.BOOTSTRAP_SERVERS)

    try:
        if not args.skip_static:
            print("seeding entity topics...")
            emit_static(sink, raw)

        if args.mode == "backfill":
            start = C.NOW - timedelta(days=args.days)
            for d in range(args.days):
                day = (start + timedelta(days=d)).replace(hour=0, minute=0,
                                                          second=0, microsecond=0)
                emit_day(rng, sink, raw, accounts_by_cust, loans_by_cust, day)
                if (d + 1) % 15 == 0:
                    print(f"  day {d+1}/{args.days}  "
                          f"events={sum(sink.counts.values()):,}")
        else:
            by_id = {c["customer_id"]: c for c in raw["customers"]}
            cohort_ids = [i for i in load_cohort_ids(args.cohort) if i in by_id]
            if cohort_ids:
                print(f"cohort bias: {args.cohort_bias:.0%} of events to "
                      f"{len(cohort_ids)} demo customers")
            else:
                print("no cohort.json found — uniform customer selection")

            print(f"live mode at ~{C.EVENTS_PER_SEC} eps — ctrl-c to stop")
            while True:
                now = datetime.now(timezone.utc)
                batch_start = sum(sink.counts.values())

                if cohort_ids and rng.random() < args.cohort_bias:
                    cust = by_id[rng.choice(cohort_ids)]
                else:
                    cust = rng.choice(raw["customers"])

                accts = accounts_by_cust[cust["customer_id"]]
                loans = loans_by_cust.get(cust["customer_id"], [])
                if accts:
                    sink.send(*E.transaction(rng, cust, rng.choice(accts),
                                             maybe_late(rng, now), loans))
                if rng.random() < 0.25:
                    for ev in E.clickstream_session(rng, cust, now):
                        sink.send(*ev)
                if rng.random() < 0.40:
                    sink.send(*E.app_log(rng, maybe_late(rng, now), cust))
                if rng.random() < 0.02:
                    sink.send(*E.call_center(rng, cust, now, loans))
                if rng.random() < 0.01:
                    for ev in E.loan_application(rng, cust, now):
                        sink.send(*ev)

                emitted = sum(sink.counts.values()) - batch_start
                time.sleep(max(0.0, emitted / C.EVENTS_PER_SEC))
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        sink.close()
        print("\nevents per topic:")
        for t, n in sorted(sink.counts.items()):
            print(f"  {t:<32} {n:>10,}")
        print(f"  {'TOTAL':<32} {sum(sink.counts.values()):>10,}")


if __name__ == "__main__":
    main()
