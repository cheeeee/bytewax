"""Rebuild tables and plots from saved, matching CodSpeed A/B results."""

import argparse
import csv
import json
import statistics
from pathlib import Path


def read(path):
    """Read one saved JSON measurement or receipt."""
    return json.loads(path.read_text())


def main():
    """Validate comparable measurements and generate the report's tables/plots."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    root = args.directory
    manifest = read(root / "manifest.json")
    variants = [entry["variant"] for entry in manifest["runs"]]
    values = {}
    native = {}
    for entry in manifest["runs"]:
        variant = entry["variant"]
        data = read(root / variant / "service.json")
        assert data["status"] == "COMPLETED"
        for part in entry.get("parts", [entry]):
            directory = root / variant
            if "part" in part:
                directory /= part["part"]
            github = read(directory / "github.json")
            assert github["headSha"] == part.get("workflow_sha", entry["sha"])
            assert github["conclusion"] == "success"
            capture = read(directory / "capture.json")
            assert capture["commit"] == entry["sha"]
            assert capture["harness_commit"] == manifest["harness"]
            assert capture["workflow_commit"] == github["headSha"]
            assert capture["workload_sha256"] == manifest["workload_sha256"]
            snapshots = read(directory / "source-snapshots.json")
            part_results = read(directory / "service.json")["results"]
            assert set(snapshots) == {row["benchmark"]["name"] for row in part_results}
            for name, count in snapshots.items():
                assert count == (0 if "[queue_" in name else 1), (variant, name, count)
        values[variant] = {}
        for row in data["results"]:
            assert row["benchmark"]["executor"] == "valgrind"
            name = row["benchmark"]["name"]
            assert name not in values[variant]
            values[variant][name] = row["value"]
        local = read(root / variant / "native-local.json")
        assert local["complete"]
        assert local["workload_sha256"] == manifest["workload_sha256"]
        assert local["epoch_interval_seconds"] == manifest["epoch_interval_seconds"]
        native[variant] = {(row["case"], row["n"]): row for row in local["rows"]}

    baseline = variants[0]
    names = list(values[baseline])
    for variant in variants:
        assert set(values[variant]) == set(names), variant
        assert set(native[variant]) == set(native[baseline]), variant
        for key, row in native[variant].items():
            assert row["status"] == "passed", (variant, key)
            assert row["digest"] == native[baseline][key]["digest"], (variant, key)

    # Match the deliberately chosen workload order in the native shortlist.
    order = {
        f"test_hotpath[{case}-{n}]": i for i, (case, n) in enumerate(native[baseline])
    }
    names.sort(key=lambda name: order[name])
    with (root / "cpu-comparison.csv").open("w") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(
            ["benchmark"]
            + [f"{v}_simulated_cpu_seconds" for v in variants]
            + [f"{v}_speedup_vs_{baseline}" for v in variants[1:]]
            + [
                f"{b}_incremental_speedup_vs_{a}"
                for a, b in zip(variants, variants[1:])
            ]
        )
        for name in names:
            writer.writerow(
                [name]
                + [values[v][name] for v in variants]
                + [values[baseline][name] / values[v][name] for v in variants[1:]]
                + [
                    values[a][name] / values[b][name]
                    for a, b in zip(variants, variants[1:])
                ]
            )

    with (root / "native-comparison.csv").open("w") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(
            ["case", "n"]
            + [
                f"{v}_{metric}"
                for v in variants
                for metric in ("seconds", "peak_rss_mib")
            ]
        )
        for key in native[baseline]:
            writer.writerow(
                list(key)
                + [
                    native[v][key][metric]
                    for v in variants
                    for metric in ("seconds", "peak_rss_mib")
                ]
            )

    # matplotlib is only needed to regenerate this report, not to run benchmarks.
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    fig, ax = plt.subplots(figsize=(12, 9))
    colors = ["#829ab1", "#2683c6", "#c38623", "#177e68"]
    for index, variant in enumerate(variants[1:]):
        ratios = [values[baseline][name] / values[variant][name] for name in names]
        y = [i + (index - 1.5) * 0.15 for i in range(len(names))]
        ax.scatter(ratios, y, s=34, label=variant, color=colors[index], zorder=3)
    ax.axvline(1, color="#a33", linewidth=1)
    final = variants[-1]
    for i, name in enumerate(names):
        ratio = values[baseline][name] / values[final][name]
        ax.annotate(
            f"{ratio:.2f}x",
            (ratio, i + 0.225),
            xytext=(7, 0),
            textcoords="offset points",
            fontsize=8,
            va="center",
        )
    ax.set_yticks(
        range(len(names)),
        [name.removeprefix("test_hotpath[").removesuffix("]") for name in names],
    )
    ax.invert_yaxis()
    ax.set_xscale("log")
    ax.set_xlim(
        0.55,
        max(values[baseline][n] / values[v][n] for n in names for v in variants) * 1.55,
    )
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:g}x"))
    ax.set_xlabel("Speedup vs A (log scale); left of 1x is slower")
    ax.set_title(
        "Bytewax: CodSpeed CPU simulation, identical workloads", loc="left", fontsize=14
    )
    ax.grid(axis="x", alpha=0.2, which="both")
    ax.legend(title="Cumulative variant", loc="lower right")
    fig.text(
        0.02,
        0.01,
        "A: merged main | B: queue/lookup | C: + count reuse | "
        "D: + stateful batching | E: + inactive instrumentation\n"
        "CPU estimates exclude disk latency. File cases use the public Python API "
        "and real FileSink. One measurement per variant/case.",
        fontsize=9,
    )
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    fig.savefig(root / "cpu-speedups.png", dpi=170)
    plt.close(fig)

    large = [key for key in native[baseline] if key[1] == 1_000_000]
    repeats = {
        (row["variant"], (row["case"], row["n"])): row
        for row in read(root / "million-confirmation.json")["rows"]
    }
    for (variant, key), row in repeats.items():
        assert row["digest"] == native[variant][key]["digest"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    for ax, metric, label in zip(
        axes,
        ("seconds", "peak_rss_mib"),
        ("Wall time (seconds, log scale)", "Peak process RSS (MiB)"),
    ):
        for index, variant in enumerate((baseline, final)):
            samples = [
                [native[variant][key][metric], repeats[variant, key][metric]]
                for key in large
            ]
            numbers = [statistics.median(sample) for sample in samples]
            bars = ax.bar(
                [i + index * 0.35 for i in range(len(large))],
                numbers,
                width=0.32,
                label=variant,
                color=("#829ab1", "#177e68")[index],
                yerr=[
                    [middle - min(sample) for middle, sample in zip(numbers, samples)],
                    [max(sample) - middle for middle, sample in zip(numbers, samples)],
                ],
                capsize=3,
            )
            ax.bar_label(bars, fmt="%.2f", padding=3, fontsize=9)
        ax.set_xticks(
            [i + 0.175 for i in range(len(large))],
            [
                "Retained queue\nstalled watermark",
                "Dense count + FileSink\n100 overlapping windows",
            ],
        )
        ax.set_ylabel(label)
        ax.legend()
        ax.grid(axis="y", alpha=0.2)
    axes[0].set_yscale("log")
    axes[0].set_ylim(top=max(native[baseline][k]["seconds"] for k in large) * 2)
    axes[1].set_ylim(
        top=max(native[v][k]["peak_rss_mib"] for k in large for v in (baseline, final))
        * 1.18
    )
    fig.suptitle("1,000,000 events: native screening on macOS ARM64", x=0.04, ha="left")
    fig.text(
        0.04,
        0.01,
        "Medians of two fresh processes; whiskers show min/max. "
        "RSS includes input. Action timer excludes setup/verification.",
        fontsize=9,
    )
    fig.tight_layout(rect=(0, 0.05, 1, 0.96))
    fig.savefig(root / "native-million.png", dpi=170)
    plt.close(fig)


if __name__ == "__main__":
    main()
