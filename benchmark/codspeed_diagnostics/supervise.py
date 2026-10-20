"""Bound each diagnostic and capture Valgrind guest stacks before timeout."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument("case", choices=["native", "none", "simulation", "fair", "no-perf"])
parser.add_argument("--output", required=True)
parser.add_argument("--timeout", type=int, default=75)
args = parser.parse_args()
directory = Path(args.output).resolve() / args.case
directory.mkdir(parents=True, exist_ok=True)
environment = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONMALLOC="malloc")
command = [sys.executable, str(Path(__file__).with_name("probe.py")), "plain"]
if args.case not in ("native", "none"):
    environment.update(CODSPEED_ENV="runner", CODSPEED_PROFILE_FOLDER=str(directory))
    command[-1] = "simulation"
    if args.case == "no-perf":
        environment["DIAG_NO_PERF"] = "1"
    command = [
        "valgrind", "-q", "--tool=callgrind", "--cache-sim=yes",
        "--I1=32768,8,64", "--D1=32768,8,64", "--LL=8388608,16,64",
        "--collect-systime=nsec", "--read-inline-info=yes", "--instr-atstart=no",
        "--separate-threads=no", "--cycle-estimation=yes", "--compress-strings=no",
        "--combine-dumps=yes", "--dump-line=no",
        f"--callgrind-out-file={directory}/callgrind.%p.out",
        f"--log-file={directory}/valgrind.%p.log",
        *(["--fair-sched=yes"] if args.case == "fair" else []), *command,
    ]
elif args.case == "none":
    command = ["valgrind", "-q", "--tool=none", *command]

start = time.monotonic()
snapshots = set()
with (directory / "process.log").open("w") as output:
    process = subprocess.Popen(
        command, env=environment, stdout=output, stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    while process.poll() is None and time.monotonic() - start < args.timeout:
        elapsed = time.monotonic() - start
        for after in (30, 55):
            if elapsed >= after and after not in snapshots:
                snapshots.add(after)
                with (directory / f"threads-{after}.log").open("w") as snapshot:
                    subprocess.run(
                        ["ps", "-L", "-p", str(process.pid), "-o",
                         "pid,tid,stat,pcpu,time,wchan:32,comm"],
                        stdout=snapshot, stderr=subprocess.STDOUT,
                    )
                if args.case != "native":
                    with (directory / f"stacks-{after}.log").open("w") as snapshot:
                        try:
                            subprocess.run(
                                ["gdb", "-q", "-batch", "-ex", "set pagination off",
                                 "-ex", "set debuginfod enabled off",
                                 "-ex", f"target remote | vgdb --pid={process.pid}",
                                 "-ex", "thread apply all bt 35", "-ex", "detach"],
                                stdout=snapshot, stderr=subprocess.STDOUT, timeout=12,
                            )
                        except subprocess.TimeoutExpired:
                            snapshot.write("GDB timeout\n")
        time.sleep(0.2)
    timed_out = process.poll() is None
    if timed_out:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
    process.wait()
result = dict(case=args.case, elapsed=time.monotonic() - start,
              timeout=timed_out, returncode=process.returncode, command=command)
(directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result), flush=True)
print((directory / "process.log").read_text()[-12000:], flush=True)
