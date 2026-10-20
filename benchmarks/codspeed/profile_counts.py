"""Count source snapshot calls in saved CodSpeed callgraphs to audit epochs."""

import argparse
import gzip
import json
import re
from collections import defaultdict
from pathlib import Path


def source_snapshots(directory):
    """Count source snapshots per hot-path case across compressed callgraphs."""
    counts = defaultdict(int)
    for path in directory.rglob("*.gz"):
        names = {}
        benchmark = None
        callee = ""
        with gzip.open(path, "rt", errors="replace") as stream:
            for line in stream:
                if line.startswith("part:"):
                    benchmark = None
                    callee = ""
                elif line.startswith("desc: Trigger: Client Request:"):
                    if "::test_hotpath[" in line:
                        benchmark = line.strip().rsplit("::", 1)[1]
                        counts[benchmark] += 0
                elif line.startswith(("fn=", "cfn=")):
                    value = line.split("=", 1)[1].strip()
                    alias = re.fullmatch(r"\((\d+)\)(?: (.*))?", value)
                    if alias:
                        key, text = alias.groups()
                        if text is not None:
                            names[key] = text
                        value = names.get(key, "")
                    if line.startswith("cfn="):
                        callee = value
                elif benchmark and line.startswith("calls="):
                    if callee == "bytewax::inputs::StatefulPartition::snapshot":
                        counts[benchmark] += int(line[6:].split()[0])
    return dict(counts)


def main():
    """Print the snapshot audit for one captured artifact directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(source_snapshots(args.directory), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
