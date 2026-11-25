# telemetry-ingest

Receive batches of sensor readings over HTTP and stream them as newline-delimited
JSON (NDJSON). The Rust service uses Axum and Tokio. Clients get an explicit
response when the server is overloaded, and an operator can see whether the
output is making progress.

## Demo

When the writer is blocked, the API returns HTTP `503` as soon as the queue
fills. On `SIGTERM`, the demo resumes reading stdout and verifies that all
**2,112 admitted records** are written in order before the server exits.
A second run checks readiness after the stdout pipe is closed.

```text
Queue full: HTTP 503. SIGTERM drained 2112 records in order.
Broken stdout: /ready returned HTTP 503, with 64 unconfirmed records.
```

Reproduce both results on Linux with Python 3.10 or later.

```sh
cargo build --locked
python3 scripts/check_pipeline.py
```

## Architecture

```mermaid
flowchart LR
    Device["Sensor or gateway"] -->|JSON batch| HTTP["Axum validation"]
    HTTP -->|Admit whole batch| Queue["Queue · 32 batches"]
    Queue --> Writer["One blocking writer"]
    Writer --> Stdout["NDJSON on stdout"]
```

## Try it

Start the service on loopback and redirect telemetry to a file. Diagnostics
stay on stderr.

```sh
cargo run --locked -- 127.0.0.1:3000 > /tmp/telemetry-ingest.ndjson
```

Send two readings from another terminal.

```sh
curl -i http://127.0.0.1:3000/v1/telemetry \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"device_id":"rover-01","timestamp_ms":1000,"metric":"imu.ax","value":0.12},{"device_id":"rover-01","timestamp_ms":1050,"metric":"imu.ax","value":0.14}]}'
curl -s http://127.0.0.1:3000/metrics
cat /tmp/telemetry-ingest.ndjson
```

The POST returns HTTP `202` with `{"accepted_records":2}`. After the writer
flushes, `flushed_records` is 2 and the output file contains one JSON object per
line. [http-pulse](https://github.com/tnphat8305/http-pulse) can probe both
`/health` and `/ready`.

## HTTP contract

Each request contains one object with a `records` array of 1–64 objects.
All four record fields are required. Unknown or duplicate fields are rejected.

| Field | Accepted values |
| --- | --- |
| `device_id`, `metric` | 1–64 ASCII characters. Start with a letter or digit. Subsequent characters may also include `_`, `-` and `.`. |
| `timestamp_ms` | Nonnegative signed 64-bit integer in Unix milliseconds. Historical samples and out-of-order timestamps are allowed. |
| `value` | Finite JSON number, represented as a Rust `f64`. |

The first invalid domain field includes a zero-based `record_index` and the
`field` name in the response. Rejection leaves the whole batch out of the
queue. Error bodies contain a stable `code` and a short `message`.

| Status | Code or outcome |
| --- | --- |
| `202` | The complete batch was admitted to the in-memory queue. |
| `400` | `invalid_body` for malformed, incomplete or unreadable JSON. |
| `408` | `body_timeout` after 2 seconds reading the request body. |
| `413` | `body_too_large` for a body over 32 KiB. |
| `415` | `unsupported_content_type` for a missing or non-JSON media type. |
| `422` | `invalid_schema`, `invalid_batch_size` or `invalid_record`. |
| `503` | `ingress_busy` or `queue_full`, with `Retry-After: 1`. |
| `503` | `sink_unavailable` after the writer closes. |

[HTTP semantics and Retry-After (RFC 9110)](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after)

## Backpressure and shutdown

- Up to 16 uploads can be read and validated at once. Each uses Axum's bounded
  JSON extractor. [Axum body limits](https://docs.rs/axum/0.8/axum/extract/struct.DefaultBodyLimit.html)
- The queue holds at most 32 batches, plus the batch currently being written.
  Admission uses `try_send` and returns immediately when full. The writer runs
  on a blocking thread and preserves queue order and order within each batch.
  [Tokio bounded channel](https://docs.rs/tokio/1/tokio/sync/mpsc/struct.Sender.html#method.try_send)
- `GET /health` returns `204` while the API is serving. `GET /ready` returns
  `204` while the writer's channel is open and `503` after writer failure.
  Temporary saturation surfaces on the POST and in `overload_requests`.
- `GET /metrics` reports cumulative admitted, flushed, rejected and overload
  counts. `unconfirmed_records` is the number admitted minus the number in
  successfully flushed batches. After a write failure, this includes both
  discarded queued batches and the failed batch, which may have been written
  partially. The process logs the error and returns a nonzero exit code on
  shutdown.
- Ctrl-C or SIGTERM on Unix stops new connections, lets active requests
  finish, and drains admitted batches before exit. Draining requires the
  output reader to make progress. A `202` acknowledges volatile admission.
  Flushing stdout hands data to the OS without a disk durability guarantee.
  Retries after an ambiguous client outcome can produce duplicates.

## Verify

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
```

The [demo](#demo) exercises the service over a real HTTP socket.
