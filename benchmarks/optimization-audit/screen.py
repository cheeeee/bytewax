"""Interleaved, process-isolated regression screen, capped at 55 seconds."""

import argparse
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--python", required=True)
    parser.add_argument("--build", action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--budget", type=float, default=55)
    parser.add_argument(
        "--group", choices=["hotpaths", "churn", "enabled"], default="hotpaths"
    )
    args = parser.parse_args()
    builds = [value.split("=", 1) for value in args.build]
    root = Path(__file__).resolve().parent
    cases = [("count_dense_file", n, 1) for n in (10, 100, 10_000, 100_000, 1_000_000)]
    cases += [
        (name, 100_000, 3)
        for name in (
            "stateful_1000keys",
            "stateful_100000keys",
            "filter_batch1",
            "queue_mixed",
        )
    ]
    cases += [("count_sparse_file", 10_000, 3)]
    if args.group == "churn":
        cases = [("churn", n, 1) for n in (10, 100, 10_000, 100_000, 1_000_000)]
        cases += [("churn_checkpoints", 100_000, 1)]
    elif args.group == "enabled":
        cases = [
            (name, 100_000, 3) for name in ("count_dense_file", "stateful_1000keys")
        ]
    metadata = {}
    for name, directory in builds:
        source = Path(directory).resolve()
        files = [
            source / "bytewax/operators/__init__.py",
            source / "bytewax/operators/windowing.py",
            *source.glob("bytewax/_bytewax*.so"),
        ]
        metadata[name] = {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files
        }
    start = time.monotonic()
    rows = []
    for case, n, repeat in cases:
        for trial in range(repeat):
            for name, directory in builds[:: 1 if trial % 2 == 0 else -1]:
                remaining = args.budget - (time.monotonic() - start)
                row = {"build": name, "case": case, "n": n, "trial": trial}
                if remaining <= 0:
                    row["status"] = "budget_exceeded"
                    rows.append(row)
                    continue
                if args.group == "enabled":
                    command = [args.python, str(root / "enabled.py"), case, str(n)]
                elif case.startswith("churn"):
                    command = [args.python, str(root / "churn.py"), str(n)]
                    if case.endswith("checkpoints"):
                        command.append("--checkpoints")
                else:
                    command = [
                        args.python,
                        str(root.parent / "codspeed/native.py"),
                        "--case",
                        case,
                        "--events",
                        str(n),
                    ]
                env = dict(
                    os.environ,
                    PYTHONPATH=str(Path(directory).resolve()),
                    PYTHONHASHSEED="0",
                )
                try:
                    result = subprocess.run(
                        command,
                        env=env,
                        capture_output=True,
                        text=True,
                        timeout=remaining,
                        check=True,
                    )
                    row.update(json.loads(result.stdout.splitlines()[-1]))
                    row["status"] = "passed"
                except subprocess.TimeoutExpired:
                    row["status"] = "budget_exceeded"
                except subprocess.CalledProcessError as error:
                    row.update(status="failed", error=error.stderr)
                rows.append(row)
                print(json.dumps(row), flush=True)
    result = {
        "budget_seconds": args.budget,
        "seconds": time.monotonic() - start,
        "build_files_sha256": metadata,
        "rows": rows,
        "note": "Screening only; three samples cannot establish small gains."
        " Timers include dataflow startup; RSS includes Python input storage.",
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    assert all(row["status"] == "passed" for row in rows)


if __name__ == "__main__":
    main()
