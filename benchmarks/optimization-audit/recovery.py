"""SQLite recovery across old/new builds, including a retained ordered queue."""

import argparse
import hashlib
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path


def child(directory, phase):
    import bytewax.operators as op
    import bytewax.operators.windowing as win
    from bytewax.connectors.files import FileSink
    from bytewax.dataflow import Dataflow
    from bytewax.recovery import RecoveryConfig, init_db_dir
    from bytewax.testing import TestingSource, run_main
    from bytewax.visualize import to_json

    origin = datetime(2024, 1, 1, tzinfo=timezone.utc)

    class ClockLogic(win.ClockLogic):
        def before_batch(self):
            pass

        def on_item(self, value):
            return value, origin - timedelta(days=1)

        def on_notify(self):
            return origin - timedelta(days=1)

        def on_eof(self):
            return win.UTC_MAX

        def to_system_utc(self, _):
            return None

        def snapshot(self):
            return None

    class Clock(win.Clock):
        def build(self, _):
            return ClockLogic()

    values = [origin + timedelta(milliseconds=(i * 7919) % 8192) for i in range(8192)]
    if phase != "reference":
        abort = TestingSource.ABORT()
        abort._triggered = phase == "resume"
        values.insert(4096, abort)
    flow = Dataflow("compatibility")
    up = op.input("input", flow, TestingSource(values, batch_size=256))
    keyed = op.key_on("key", up, lambda _: "one")
    state = op.stateful_map("sum", keyed, lambda s, _: ((s or 0) + 1, (s or 0) + 1))
    count = win.count_window(
        "count",
        up,
        Clock(),
        win.SlidingWindower(
            timedelta(seconds=10),
            timedelta(seconds=1),
            origin,
        ),
        lambda _: "one",
    )
    ordered = win.fold_window(
        "ordered",
        keyed,
        Clock(),
        win.TumblingWindower(
            timedelta(days=1),
            origin,
        ),
        lambda: 0,
        lambda s, _: s + 1,
        lambda a, b: a + b,
        ordered=True,
    )
    for name, stream in [
        ("state", state),
        ("count", count.down),
        ("ordered", ordered.down),
    ]:
        stream = op.map_value(f"encode_{name}", stream, json.dumps)
        op.output(f"out_{name}", stream, FileSink(directory / f"{name}.jsonl"))
    (directory / f"{phase}-graph.json").write_text(to_json(flow))
    database = directory / "db"
    if phase != "resume":
        database.mkdir()
        init_db_dir(database, 1)
    run_main(
        flow, epoch_interval=timedelta(0), recovery_config=RecoveryConfig(database)
    )
    if phase == "abort":
        ordered_path = directory / "ordered.jsonl"
        assert not ordered_path.exists() or not ordered_path.read_text()
        assert len((directory / "state.jsonl").read_text().splitlines()) == 4096


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--phase")
    parser.add_argument("--python")
    parser.add_argument("--before")
    parser.add_argument("--after")
    args = parser.parse_args()
    if args.phase:
        child(args.directory, args.phase)
        return
    results = []
    for name, versions in [
        ("reference", [(args.before, "reference")]),
        ("before-after", [(args.before, "abort"), (args.after, "resume")]),
        ("after-before", [(args.after, "abort"), (args.before, "resume")]),
    ]:
        directory = args.directory / name
        directory.mkdir(parents=True)
        for source, phase in versions:
            subprocess.run(
                [
                    args.python,
                    str(Path(__file__).resolve()),
                    "--directory",
                    str(directory.resolve()),
                    "--phase",
                    phase,
                ],
                env=dict(os.environ, PYTHONPATH=str(Path(source).resolve())),
                timeout=20,
                check=True,
            )
        outputs = {path.name: path.read_text() for path in directory.glob("*.jsonl")}
        graphs = [
            json.loads(path.read_text()) for path in directory.glob("*-graph.json")
        ]
        if name == "reference":
            reference, graph = outputs, graphs[0]
        else:
            assert outputs == reference
            assert all(g == graph for g in graphs)
        results.append(
            {
                "case": name,
                "status": "passed",
                "files": {
                    name: {
                        "records": len(text.splitlines()),
                        "sha256": hashlib.sha256(text.encode()).hexdigest(),
                    }
                    for name, text in outputs.items()
                },
            }
        )
    (args.directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
