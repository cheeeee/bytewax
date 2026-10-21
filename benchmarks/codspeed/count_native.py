"""Compare dense/sparse count dataflows in two checkouts within 55 seconds."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def main():
    """Alternate fresh processes, checking identical outputs for each case."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before", type=Path)
    parser.add_argument("after", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--budget", type=float, default=55)
    args = parser.parse_args()
    harness = Path(__file__).resolve().parent
    variants = {"before": args.before.resolve(), "after": args.after.resolve()}
    revisions = {
        name: subprocess.check_output(
            ["git", "-C", str(repo), "rev-parse", "HEAD"], text=True
        ).strip()
        for name, repo in variants.items()
    }
    binaries = {
        name: hashlib.sha256(
            next((repo / "pysrc/bytewax").glob("_bytewax*.so")).read_bytes()
        ).hexdigest()
        for name, repo in variants.items()
    }
    start = time.perf_counter()
    rows = []
    digests = {}
    for n in (10, 100, 10_000, 100_000, 1_000_000):
        for case in ("count_dense_file", "count_sparse_file"):
            # Keep the million-event confirmation within the shortlist budget.
            for repeat in range(1 if n == 1_000_000 else 3):
                order = list(variants.items())
                if repeat % 2:
                    order.reverse()
                for name, repo in order:
                    row = {"variant": name, "case": case, "n": n, "repeat": repeat}
                    remaining = args.budget - (time.perf_counter() - start)
                    if remaining <= 0:
                        row["status"] = "budget_exceeded"
                    else:
                        env = dict(os.environ, PYTHONHASHSEED="0")
                        env["PYTHONPATH"] = str(repo / "pysrc")
                        try:
                            proc = subprocess.run(
                                [
                                    sys.executable,
                                    str(harness / "native.py"),
                                    "--case",
                                    case,
                                    "--events",
                                    str(n),
                                ],
                                cwd=repo,
                                env=env,
                                text=True,
                                capture_output=True,
                                check=True,
                                timeout=remaining,
                            )
                            row.update(json.loads(proc.stdout.splitlines()[-1]))
                            digest = digests.setdefault((case, n), row["digest"])
                            assert row["digest"] == digest, (case, n, name)
                            row["status"] = "passed"
                        except subprocess.TimeoutExpired:
                            row["status"] = "budget_exceeded"
                    rows.append(row)
                    print(json.dumps(row), flush=True)
    result = {
        "commits": revisions,
        "native_sha256": binaries,
        "workload_sha256": hashlib.sha256(
            (harness / "workloads.py").read_bytes()
        ).hexdigest(),
        "seconds": time.perf_counter() - start,
        "budget_seconds": args.budget,
        "python": sys.version,
        "rows": rows,
        "complete": all(row["status"] == "passed" for row in rows),
        "note": "Action time includes FileSink flush/fsync; setup and verification "
        "are excluded. RSS includes input. Three samples per case except one at 1M.",
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
