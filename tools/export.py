"""Print one bankgen stream as JSON lines of bare records, for
clickhouse-client ... FORMAT JSONEachRow.

    export.py generate <days>    backfill into /data (skipped if already there)
    export.py <topic>            stream a generated topic to stdout
"""

import json
import os
import subprocess
import sys

DATA = "/data"


def generate(days):
    marker = os.path.join(DATA, f".days-{days}")
    if os.path.exists(marker):
        print(f"bankgen data for {days} days already generated", file=sys.stderr)
        return
    for name in os.listdir(DATA):
        os.remove(os.path.join(DATA, name))
    subprocess.run(
        ["python", "produce.py", "--mode", "backfill", "--sink", "file", "--out", DATA, "--days", str(days)],
        cwd="/app/bankgen", check=True, stdout=sys.stderr,
    )
    open(marker, "w").close()


def export(topic):
    with open(os.path.join(DATA, f"{topic}.jsonl")) as fh:
        for line in fh:
            sys.stdout.write(json.dumps(json.loads(line)["value"]) + "\n")


if __name__ == "__main__":
    if sys.argv[1] == "generate":
        generate(int(sys.argv[2]))
    else:
        export(sys.argv[1])
