"""A real FileSink/dataflow run with Rust metrics and normal CLI logging on."""

import argparse
import hashlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path

from bytewax._bytewax import cli_main
from bytewax.tracing import setup_tracing

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "codspeed"))
import workloads  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("case")
    parser.add_argument("n", type=int)
    args = parser.parse_args()
    tracer = setup_tracing(log_level="ERROR")
    workloads.run_main = cli_main
    with tempfile.TemporaryDirectory() as directory:
        os.environ["BYTEWAX_DATAFLOW_API_ENABLED"] = "true"
        os.environ["BYTEWAX_DATAFLOW_API_PORT"] = "0"
        os.environ["BYTEWAX_DATAFLOW_API_CACHE_PATH"] = directory
        workload = workloads.build(args.case, args.n, Path(directory) / "out.jsonl")
        start = time.perf_counter()
        result = workload.action()
        seconds = time.perf_counter() - start
        checked = workload.verify(result)
        assert tracer is not None
        print(
            json.dumps(
                {
                    "case": args.case,
                    "n": args.n,
                    "seconds": seconds,
                    "digest": hashlib.sha256(json.dumps(checked).encode()).hexdigest(),
                    "metrics": "enabled",
                    "logging": "ERROR subscriber",
                }
            )
        )


if __name__ == "__main__":
    main()
