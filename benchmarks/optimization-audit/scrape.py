"""Replay captured exposition into real Prometheus, including a drop workaround."""

import argparse
import json
import socket
import subprocess
import threading
import time
import urllib.request
from urllib.parse import quote
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prometheus", required=True)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    args.directory.mkdir(parents=True)
    samples = {"/before": args.before.read_bytes(), "/after": args.after.read_bytes()}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(samples[self.path])

        def log_message(self, *_):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever)
    thread.start()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = "global:\n  scrape_interval: 200ms\nscrape_configs:\n"
    for job, path, drop in [
        ("before", "/before", False),
        ("after", "/after", False),
        ("drop_conflicting_family", "/after", True),
    ]:
        config += f"""  - job_name: {job}
    metrics_path: {path}
    static_configs:
      - targets: ['127.0.0.1:{http.server_port}']
"""
        if drop:
            config += """    metric_relabel_configs:
      - source_labels: [__name__]
        regex: bytewax_partd_write_duration_seconds_(bucket|sum|count)
        action: drop
"""
    conf = args.directory / "prometheus.yml"
    conf.write_text(config)
    with (args.directory / "prometheus.log").open("w") as log:
        process = subprocess.Popen(
            [
                args.prometheus,
                f"--config.file={conf.resolve()}",
                f"--storage.tsdb.path={(args.directory / 'db').resolve()}",
                f"--web.listen-address=127.0.0.1:{port}",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{port}/api/v1/targets"
                    ) as resp:
                        targets = json.load(resp)["data"]["activeTargets"]
                    if len(targets) == 3 and all(
                        t["lastScrapeDuration"] > 0 for t in targets
                    ):
                        break
                except OSError:
                    pass
                time.sleep(0.1)
            else:
                raise AssertionError("Prometheus did not scrape")
            rows = [
                {
                    "job": t["labels"]["job"],
                    "health": t["health"],
                    "lastError": t["lastError"],
                }
                for t in targets
            ]
            queries = {}
            for query in ["up", "bytewax_partd_write_duration_seconds_count"]:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/api/v1/query?query={quote(query)}"
                ) as resp:
                    queries[query] = json.load(resp)
            result = {"targets": rows, "queries": queries}
            (args.directory / "result.json").write_text(
                json.dumps(result, indent=2) + "\n"
            )
            print(json.dumps(result, indent=2))
        finally:
            process.terminate()
            process.wait(timeout=10)
            http.shutdown()
            thread.join()
            http.server_close()


if __name__ == "__main__":
    main()
