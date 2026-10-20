"""Keep raw CodSpeed counter totals and run identity for reproducible A/B."""

import gzip
import hashlib
import json
import os
import platform
import shutil
from pathlib import Path

import bytewax._bytewax as native

dest = Path("ab-results")
dest.mkdir(exist_ok=True)
rows = []
for folder in sorted(Path("/tmp").glob("profile.*.out")):
    if not folder.is_dir():
        continue
    for profile in sorted(folder.glob("*.out")):
        current = {}
        with profile.open(errors="replace") as stream:
            for line in stream:
                if line.startswith("part:"):
                    current = {
                        "profile": profile.name,
                        "part": int(line.split(":", 1)[1]),
                    }
                elif line.startswith("desc: Trigger:"):
                    current["trigger"] = line.split("desc: Trigger:", 1)[1].strip()
                elif line.startswith("events:"):
                    current["events"] = line.split(":", 1)[1].split()
                elif line.startswith("totals:"):
                    values = list(map(int, line.split(":", 1)[1].split()))
                    # Callgrind omits zero-valued trailing counters.
                    values += [0] * (len(current.get("events", [])) - len(values))
                    current["totals"] = dict(zip(current.get("events", []), values))
                    rows.append(current.copy())
        with profile.open("rb") as source, gzip.open(
            dest / (folder.name + "-" + profile.name + ".gz"), "wb"
        ) as target:
            shutil.copyfileobj(source, target)

metadata = {
    "commit": os.environ.get("GITHUB_SHA"),
    "github_run_id": os.environ.get("GITHUB_RUN_ID"),
    "codspeed_run_id": os.environ.get("CODSPEED_RUN_ID"),
    "python": platform.python_version(),
    "machine": platform.machine(),
    "runner": "5.5.0",
    "fair_sched": True,
    "native_sha256": hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),
    "counters": rows,
}
(dest / "codspeed.json").write_text(json.dumps(metadata, indent=2) + "\n")
print(json.dumps({key: value for key, value in metadata.items() if key != "counters"}))
print("Recorded", len(rows), "raw profile parts")
