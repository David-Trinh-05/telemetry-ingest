"""Exercise stdout backpressure and shutdown through a real HTTP socket on Linux."""

import http.client
import json
import select
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from time import monotonic, sleep


@contextmanager
def running_server(binary):
    with subprocess.Popen(
        [binary, "127.0.0.1:0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        pipesize=4096,
    ) as server:
        try:
            assert select.select([server.stderr], [], [], 5)[0], "server did not start"
            line = server.stderr.readline().decode().strip()
            assert line.startswith("listening on "), line
            port = int(line.rsplit(":", 1)[1])
            yield server, port
        finally:
            if server.poll() is None:
                server.kill()


def request(port, path, records=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        if records is None:
            connection.request("GET", path)
        else:
            connection.request(
                "POST", path, json.dumps({"records": records}),
                {"Content-Type": "application/json"},
            )
        response = connection.getresponse()
        body = response.read()
        return response.status, response.getheader("Retry-After"), json.loads(body) if body else None
    finally:
        connection.close()


def batch(sequence):
    return [
        {
            "device_id": "r" * 64,
            "timestamp_ms": sequence * 64 + index,
            "metric": "m" * 64,
            "value": 0.12,
        }
        for index in range(64)
    ]


def check_full_queue(binary):
    with running_server(binary) as (server, port):
        path = "/v1/telemetry"
        expected = batch(0)
        assert request(port, path, expected) == (202, None, {"accepted_records": 64})
        # The first batch exceeds the pipe buffer. Wait until the writer has
        # started writing before filling the queue, without reading the pipe.
        assert select.select([server.stdout], [], [], 5)[0], "writer did not start"
        _, _, metrics = request(port, "/metrics")
        capacity = metrics["queue_capacity_batches"]
        for sequence in range(1, capacity + 1):
            records = batch(sequence)
            assert request(port, path, records) == (202, None, {"accepted_records": 64})
            expected.extend(records)

        status, retry_after, error = request(port, path, batch(capacity + 1))
        assert (status, retry_after, error["code"]) == (503, "1", "queue_full")
        _, _, metrics = request(port, "/metrics")
        assert metrics["accepted_records"] == metrics["unconfirmed_records"] == len(expected)
        assert metrics["flushed_records"] == 0
        assert metrics["overload_requests"] == 1
        assert request(port, "/health")[0] == 204

        server.terminate()
        output, errors = server.communicate(timeout=10)
        assert server.returncode == 0, errors.decode()
        assert [json.loads(line) for line in output.splitlines()] == expected
        print(f"Queue full: HTTP 503. SIGTERM drained {len(expected)} records in order.")


def check_broken_pipe(binary):
    with running_server(binary) as (server, port):
        server.stdout.close()
        assert request(port, "/v1/telemetry", batch(0))[0] == 202
        deadline = monotonic() + 5
        while request(port, "/ready")[0] == 204 and monotonic() < deadline:
            sleep(0.01)

        assert request(port, "/ready")[0] == 503
        assert request(port, "/health")[0] == 204
        status, retry_after, error = request(port, "/v1/telemetry", batch(1))
        assert (status, retry_after, error["code"]) == (503, None, "sink_unavailable")
        _, _, metrics = request(port, "/metrics")
        assert metrics["sink_errors"] == 1
        assert metrics["flushed_records"] == 0
        assert metrics["unconfirmed_records"] == 64

        server.terminate()
        assert server.wait(timeout=10) == 1
        assert b"telemetry sink failed" in server.stderr.read()
        print("Broken stdout: /ready returned HTTP 503, with 64 unconfirmed records.")


if __name__ == "__main__":
    default_binary = Path(__file__).resolve().parents[1] / "target/debug/telemetry-ingest"
    target = sys.argv[1] if len(sys.argv) > 1 else default_binary
    check_full_queue(target)
    check_broken_pipe(target)
