"""Rebuild count-bound regression tables and a focused CPU comparison plot."""

import argparse
import csv
import json
import statistics
from pathlib import Path


def read(path):
    """Read a saved measurement or run receipt."""
    return json.loads(path.read_text())


def main():
    """Compare the fixed runtime with both the main baseline and variant E."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    root = args.directory
    manifest = read(root / "manifest.json")
    reference = root / manifest["reference_results"]
    entry = manifest["runs"][0]
    capture = read(root / "F/capture.json")
    github = read(root / "F/github.json")
    assert capture["commit"] == entry["sha"]
    assert capture["harness_commit"] == entry["workflow_sha"] == github["headSha"]
    assert github["conclusion"] == "success"
    assert capture["workload_sha256"] == manifest["workload_sha256"]
    previous_capture = read(reference / "E/capture.json")
    for field in ("runner", "python", "machine", "fair_sched"):
        assert capture[field] == previous_capture[field], field
    assert capture["fair_sched"] is True
    assert (
        capture["workload_sha256"]
        == read(reference / "manifest.json")["workload_sha256"]
    )
    counts = read(root / "F/source-snapshots.json")
    ci_native = read(root / "F/native-ci.json")
    local_native = read(root / "native-after.json")
    assert ci_native["complete"] and local_native["complete"]
    assert ci_native["commit"] == local_native["commit"] == entry["sha"]
    assert ci_native["native_sha256"] == capture["native_sha256"]
    assert ci_native["workload_sha256"] == local_native["workload_sha256"]
    ci_rows = {(r["case"], r["n"]): r for r in ci_native["rows"]}
    local_rows = {(r["case"], r["n"]): r for r in local_native["rows"]}
    assert ci_rows.keys() == local_rows.keys()
    for key, row in ci_rows.items():
        assert row["status"] == local_rows[key]["status"] == "passed"
        assert row["digest"] == local_rows[key]["digest"]
    values = {}
    for variant, path in (
        ("A", reference / "A/service.json"),
        ("E", reference / "E/service.json"),
        ("F", root / "F/service.json"),
    ):
        service = read(path)
        assert service["status"] == "COMPLETED"
        values[variant] = {
            row["benchmark"]["name"]: row["value"] for row in service["results"]
        }
    assert set(values["A"]) == set(values["E"]) == set(values["F"]) == set(counts)
    for name, count in counts.items():
        assert count == (0 if "[queue_" in name else 1)
    with (root / "cpu-comparison.csv").open("w") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(
            [
                "benchmark",
                "A_cpu_seconds",
                "E_cpu_seconds",
                "F_cpu_seconds",
                "F_speedup_vs_E",
                "F_speedup_vs_A",
                "F_CPU_change_percent_vs_E",
            ]
        )
        for name in values["F"]:
            a, e, f = (values[v][name] for v in ("A", "E", "F"))
            writer.writerow([name, a, e, f, e / f, a / f, (f / e - 1) * 100])

    native = read(root / "count-native.json")
    assert native["complete"]
    assert native["workload_sha256"] == manifest["workload_sha256"]
    assert native["commits"]["before"] == manifest["native_before_sha"]
    assert native["commits"]["after"] == entry["sha"]
    assert native["native_sha256"]["before"] == native["native_sha256"]["after"]
    with (root / "native-comparison.csv").open("w") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(
            [
                "case",
                "n",
                "samples_per_variant",
                "before_median_seconds",
                "after_median_seconds",
                "speedup",
                "before_peak_RSS_median_MiB",
                "after_peak_RSS_median_MiB",
            ]
        )
        for case in ("count_dense_file", "count_sparse_file"):
            for n in (10, 100, 10_000, 100_000, 1_000_000):
                rows = [r for r in native["rows"] if (r["case"], r["n"]) == (case, n)]
                assert all(r["status"] == "passed" for r in rows)
                assert len({r["digest"] for r in rows}) == 1
                medians = {
                    (variant, metric): statistics.median(
                        r[metric] for r in rows if r["variant"] == variant
                    )
                    for variant in ("before", "after")
                    for metric in ("seconds", "peak_rss_mib")
                }
                a, b = (medians[v, "seconds"] for v in ("before", "after"))
                writer.writerow(
                    [case, n, len(rows) // 2, a, b, a / b]
                    + [medians[v, "peak_rss_mib"] for v in ("before", "after")]
                )

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))
    for ax, case, title in zip(
        axes,
        ("count_sparse_file-10000", "count_dense_file-100000"),
        ("Sparse count: 10,000 events", "Dense count: 100,000 events"),
    ):
        numbers = [values[v][f"test_hotpath[{case}]"] for v in ("A", "E", "F")]
        bars = ax.bar(
            ["A: main base", "E: before fix", "F: reuse bounds"],
            numbers,
            color=["#829ab1", "#c38623", "#177e68"],
        )
        if case.startswith("count_dense"):
            ax.set_yscale("log")
            ax.set_ylabel("Simulated CPU seconds (log scale)")
        else:
            ax.set_ylabel("Simulated CPU seconds")
        ax.set_ylim(top=max(numbers) * 1.4)
        ax.bar_label(bars, fmt="%.3f", padding=4)
        ax.set_title(title)
        ax.grid(axis="y", alpha=0.15)
    fig.suptitle("Count + FileSink: fixed workloads, CodSpeed CPU simulation")
    fig.text(
        0.03,
        0.02,
        "Same workload hash, one source snapshot per run. CPU estimates exclude "
        "disk latency. Lower is better.",
        fontsize=9,
    )
    fig.tight_layout(rect=(0, 0.06, 1, 0.96))
    fig.savefig(root / "count-bounds.png", dpi=170)
    plt.close(fig)


if __name__ == "__main__":
    main()
