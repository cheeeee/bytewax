"""Bounded native confirmation, including 10 through 1,000,000 events."""

import argparse
import hashlib
import json
import os
import resource
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from workloads import CASES, LARGE_CASES, build


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case")
    parser.add_argument("--events", type=int)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--budget", type=float, default=55)
    args = parser.parse_args()
    if args.case:
        with tempfile.TemporaryDirectory() as directory:
            workload = build(args.case, args.events, Path(directory) / "out.jsonl")
            start = time.perf_counter()
            result = workload.action()
            seconds = time.perf_counter() - start
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            checked = workload.verify(result)
            print(
                json.dumps(
                    {
                        "case": args.case,
                        "n": args.events,
                        "seconds": seconds,
                        "peak_rss_mib": rss
                        / (1024**2 if sys.platform == "darwin" else 1024),
                        "digest": hashlib.sha256(
                            json.dumps(checked).encode()
                        ).hexdigest(),
                    }
                )
            )
        return
    start = time.perf_counter()
    rows = []
    for case, n in CASES + LARGE_CASES:
        remaining = args.budget - (time.perf_counter() - start)
        if remaining <= 0:
            rows.append({"case": case, "n": n, "status": "budget_exceeded"})
            continue
        try:
            proc = subprocess.run(
                [sys.executable, __file__, "--case", case, "--events", str(n)],
                text=True,
                capture_output=True,
                timeout=remaining,
                check=True,
            )
            row = json.loads(proc.stdout.splitlines()[-1])
            row["status"] = "passed"
        except subprocess.TimeoutExpired:
            row = {"case": case, "n": n, "status": "budget_exceeded"}
        except subprocess.CalledProcessError as exc:
            row = {
                "case": case,
                "n": n,
                "status": "failed",
                "stderr": exc.stderr[-4000:],
            }
        rows.append(row)
        print(json.dumps(row), flush=True)
    result = {
        "commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "seconds": time.perf_counter() - start,
        "budget_seconds": args.budget,
        "python": sys.version,
        "hashseed": os.environ.get("PYTHONHASHSEED"),
        "rows": rows,
        "complete": all(row["status"] == "passed" for row in rows),
        "note": "One fresh process per case: screening, not a precise estimate of small gains. RSS includes prepared input; verification is untimed.",
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    if any(row["status"] == "failed" for row in rows):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
