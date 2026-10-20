"""Download pinned fork A/B runs and their public CodSpeed CPU results."""

import argparse
import json
import subprocess
import urllib.request
from pathlib import Path

from profile_counts import source_snapshots

# Public read query used by CodSpeed runner v5.5.0, FetchLocalRun.gql.
# `value` is simulated CPU seconds, not GitHub job time or disk latency.
QUERY = """
query FetchLocalRun($owner: String!, $name: String!, $runId: String!) {
  repository(owner: $owner, name: $name) {
    run(id: $runId) {
      id status url
      results {
        benchmark { name executor }
        value
        issues { callgraphGenerationFailure }
        valgrind { timeDistribution { ir l1m llm sys } }
        walltime { iterations stdev totalTime }
        memory { peakMemory totalAllocated allocCalls }
      }
    }
  }
}
"""


def gh(*args):
    """Read GitHub metadata or download artifacts using the authenticated CLI."""
    return subprocess.check_output(["gh", *map(str, args)], text=True)


def fetch_run(repo, entry, runtime_sha, dest, label, workload_sha):
    """Collect one completed run and check its workload and runtime identity."""
    owner, name = repo.split("/")
    run_id = entry["github_run_id"]
    state = json.loads(
        gh(
            "run",
            "view",
            run_id,
            "--repo",
            repo,
            "--json",
            "headSha,status,conclusion,url",
        )
    )
    assert state["headSha"] == entry.get("workflow_sha", runtime_sha), (entry, state)
    print(label, state["status"], state["conclusion"], flush=True)
    if state["status"] != "completed":
        return None
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "github.json").write_text(json.dumps(state, indent=2) + "\n")
    captures = list(dest.rglob("codspeed.json"))
    if not captures:
        gh("run", "download", run_id, "--repo", repo, "--dir", dest)
        captures = list(dest.rglob("codspeed.json"))
    assert len(captures) == 1, captures
    capture = json.loads(captures[0].read_text())
    assert capture["commit"] == runtime_sha
    assert capture["harness_commit"] == state["headSha"]
    assert capture["workflow_commit"] == state["headSha"]
    assert capture["workload_sha256"] == workload_sha
    (dest / "capture.json").write_text(json.dumps(capture, indent=2) + "\n")
    counts = source_snapshots(dest)
    (dest / "source-snapshots.json").write_text(json.dumps(counts, indent=2) + "\n")
    native_files = list(dest.rglob("native.json"))
    if native_files:
        assert len(native_files) == 1
        (dest / "native-ci.json").write_bytes(native_files[0].read_bytes())
    codspeed_id = capture["codspeed_run_id"]
    if not codspeed_id or state["conclusion"] != "success":
        print("Incomplete measurement; inspect workflow failure", flush=True)
        return None
    for benchmark, count in counts.items():
        assert count == (0 if "[queue_" in benchmark else 1), (label, benchmark, count)
    if (dest / "service.json").exists():
        return json.loads((dest / "service.json").read_text())
    body = json.dumps(
        {"query": QUERY, "variables": dict(owner=owner, name=name, runId=codspeed_id)}
    ).encode()
    request = urllib.request.Request(
        "https://gql.codspeed.io/",
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "Bytewax-AB"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.load(response)
    if result.get("errors"):
        raise RuntimeError(result["errors"])
    run = result["data"]["repository"]["run"]
    if run["status"] != "COMPLETED":
        print("CodSpeed processing:", run["status"], flush=True)
        return None
    assert run["id"] == codspeed_id
    (dest / "service.json").write_text(json.dumps(run, indent=2) + "\n")
    print(len(run["results"]), "CPU results:", run["url"], flush=True)
    return run


def main():
    """Collect the manifest's variants, preserving receipts for split runs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--repo", default="cheeeee/bytewax")
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    for entry in manifest["runs"]:
        dest = args.destination / entry["variant"]
        parts = entry.get("parts")
        if parts is None:
            fetch_run(
                args.repo,
                entry,
                entry["sha"],
                dest,
                entry["variant"],
                manifest["workload_sha256"],
            )
            continue
        completed = []
        for part in parts:
            result = fetch_run(
                args.repo,
                part,
                entry["sha"],
                dest / part["part"],
                entry["variant"] + "/" + part["part"],
                manifest["workload_sha256"],
            )
            if result is not None:
                completed.append(result)
        if len(completed) == len(parts):
            # Explicit assembly of disjoint, unchanged test cases. Preserve each
            # original service response and GitHub receipt under its part name.
            rows = [row for run in completed for row in run["results"]]
            assert len({row["benchmark"]["name"] for row in rows}) == len(rows)
            assembled = {
                "status": "COMPLETED",
                "assembled_from": [
                    {"id": run["id"], "url": run["url"]} for run in completed
                ],
                "results": rows,
            }
            (dest / "service.json").write_text(json.dumps(assembled, indent=2) + "\n")


if __name__ == "__main__":
    main()
