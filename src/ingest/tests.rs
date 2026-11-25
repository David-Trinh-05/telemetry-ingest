use super::*;
use axum::body::{Body, Bytes, to_bytes};
use futures_util::stream;
use serde_json::{Value, json};
use tokio::{sync::oneshot, task::spawn_blocking};
use tower::ServiceExt;

fn record(timestamp: i64) -> Value {
    json!({"device_id": "rover-01", "timestamp_ms": timestamp, "metric": "imu.ax", "value": 0.12})
}

async fn send(app: &Router, records: Vec<Value>) -> Response {
    post_body(
        app,
        "application/json",
        Body::from(json!({"records": records}).to_string()),
    )
    .await
}

async fn post_body(app: &Router, media_type: &str, body: Body) -> Response {
    let request = Request::post("/v1/telemetry")
        .header(header::CONTENT_TYPE, media_type)
        .body(body)
        .unwrap();
    app.clone().oneshot(request).await.unwrap()
}

async fn get_path(app: &Router, path: &str) -> Response {
    app.clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn body_json(response: Response) -> Value {
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn assert_error(response: Response, status: StatusCode, code: &str) -> Value {
    assert_eq!(response.status(), status);
    let body = body_json(response).await;
    assert_eq!(body["code"], code);
    body
}

#[tokio::test]
async fn rejects_entire_batch_if_one_record_is_invalid() {
    let (app, worker) = pipeline(4);
    let mut bad = record(2);
    bad["device_id"] = json!("invalid identifier");
    let response = send(&app, vec![record(1), bad]).await;
    let detail = assert_error(response, StatusCode::UNPROCESSABLE_ENTITY, "invalid_record").await;
    assert_eq!(detail["record_index"], 1);
    assert_eq!(detail["field"], "device_id");

    let response = send(&app, vec![record(-1)]).await;
    let detail = assert_error(response, StatusCode::UNPROCESSABLE_ENTITY, "invalid_record").await;
    assert_eq!(detail["field"], "timestamp_ms");
    let counters = body_json(get_path(&app, "/metrics").await).await;
    assert_eq!(counters["accepted_records"], 0);
    assert_eq!(counters["rejected_requests"], 2);

    drop(app);
    let output = spawn_blocking(move || {
        let mut output = Vec::new();
        worker.run(&mut output).unwrap();
        output
    })
    .await
    .unwrap();
    assert!(output.is_empty());
}

#[tokio::test]
async fn applies_batch_and_body_limits_at_the_boundaries() {
    let (app, _worker) = pipeline(4);
    for records in [vec![], vec![record(1); 65]] {
        assert_error(
            send(&app, records).await,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_batch_size",
        )
        .await;
    }
    let body = json!({"records": vec![record(1); 64]}).to_string();
    let mut bytes = body.into_bytes();
    bytes.resize(BODY_LIMIT, b' ');
    let response = post_body(&app, "application/json", Body::from(bytes.clone())).await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(body_json(response).await["accepted_records"], 64);

    let body = Body::from_stream(stream::iter([
        Ok::<_, io::Error>(Bytes::from(bytes)),
        Ok(Bytes::from_static(b" ")),
    ]));
    assert_error(
        post_body(&app, "application/json", body).await,
        StatusCode::PAYLOAD_TOO_LARGE,
        "body_too_large",
    )
    .await;
}

#[tokio::test]
async fn distinguishes_media_syntax_schema_and_domain_errors() {
    let (app, _worker) = pipeline(4);
    for (media, body, status, code) in [
        (
            "text/plain",
            "{}",
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_content_type",
        ),
        (
            "application/json",
            r#"{"records":["#,
            StatusCode::BAD_REQUEST,
            "invalid_body",
        ),
        (
            "application/json",
            r#"{"records":[{}]}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_schema",
        ),
        (
            "application/json",
            r#"[[{"device_id":"d","timestamp_ms":1,"metric":"m","value":0}]]"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_schema",
        ),
        (
            "application/json",
            r#"{"records":[["d",1,"m",0]]}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_schema",
        ),
        (
            "application/json",
            r#"{"records":[],"records":[]}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_schema",
        ),
        (
            "application/json",
            r#"{"records":[],"unknown":true}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_schema",
        ),
        (
            "application/json",
            r#"{"records":[{"device_id":"d","timestamp_ms":1,"metric":"m","value":1e400}]}"#,
            StatusCode::BAD_REQUEST,
            "invalid_body",
        ),
    ] {
        assert_error(post_body(&app, media, Body::from(body)).await, status, code).await;
    }
}

#[tokio::test]
async fn rejects_overload_and_drains_admitted_batches_in_order() {
    let (app, worker) = pipeline(2);
    let expected = vec![record(1), record(2), record(3)];
    for records in [expected[..2].to_vec(), expected[2..].to_vec()] {
        assert_eq!(send(&app, records).await.status(), StatusCode::ACCEPTED);
    }
    let response = send(&app, vec![record(4)]).await;
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    assert_error(response, StatusCode::SERVICE_UNAVAILABLE, "queue_full").await;
    assert_eq!(
        get_path(&app, "/ready").await.status(),
        StatusCode::NO_CONTENT
    );
    let counters = body_json(get_path(&app, "/metrics").await).await;
    assert_eq!(counters["accepted_batches"], 2);
    assert_eq!(counters["accepted_records"], 3);
    assert_eq!(counters["unconfirmed_records"], 3);
    assert_eq!(counters["overload_requests"], 1);

    let counters = Arc::clone(&worker.counters);
    drop(app);
    let output = spawn_blocking(move || {
        let mut output = Vec::new();
        worker.run(&mut output).unwrap();
        output
    })
    .await
    .unwrap();
    let records: Vec<Value> = output
        .strip_suffix(b"\n")
        .unwrap()
        .split(|byte| *byte == b'\n')
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_eq!(records, expected);
    assert_eq!(counters.lock().unwrap().flushed_records, 3);
}

#[tokio::test(start_paused = true)]
async fn bounds_concurrent_body_reads_and_releases_timed_out_uploads() {
    let (app, _worker) = pipeline(2);
    let mut uploads = Vec::new();
    for _ in 0..INGRESS_LIMIT {
        let (started, wait) = oneshot::channel();
        let mut started = Some(started);
        let stalled = Body::from_stream(stream::poll_fn(move |_| {
            if let Some(started) = started.take() {
                started.send(()).unwrap();
            }
            std::task::Poll::Pending::<Option<Result<Bytes, io::Error>>>
        }));
        let app = app.clone();
        uploads.push(tokio::spawn(async move {
            post_body(&app, "application/json", stalled).await
        }));
        wait.await.unwrap();
    }

    let overloaded = send(&app, vec![record(1)]).await;
    assert_eq!(overloaded.headers()[header::RETRY_AFTER], "1");
    assert_error(overloaded, StatusCode::SERVICE_UNAVAILABLE, "ingress_busy").await;
    for upload in uploads {
        assert_error(
            upload.await.unwrap(),
            StatusCode::REQUEST_TIMEOUT,
            "body_timeout",
        )
        .await;
    }
    assert_eq!(
        send(&app, vec![record(1)]).await.status(),
        StatusCode::ACCEPTED
    );
}

#[tokio::test]
async fn reports_failed_flush_without_claiming_records_were_flushed() {
    struct FailingFlush;

    impl Write for FailingFlush {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }

    let (app, worker) = pipeline(2);
    assert_eq!(
        send(&app, vec![record(1)]).await.status(),
        StatusCode::ACCEPTED
    );
    let error = spawn_blocking(move || worker.run(FailingFlush))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);

    assert_eq!(
        get_path(&app, "/health").await.status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        get_path(&app, "/ready").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let response = send(&app, vec![record(2)]).await;
    assert!(!response.headers().contains_key(header::RETRY_AFTER));
    assert_error(
        response,
        StatusCode::SERVICE_UNAVAILABLE,
        "sink_unavailable",
    )
    .await;
    let counters = body_json(get_path(&app, "/metrics").await).await;
    assert_eq!(counters["sink_errors"], 1);
    assert_eq!(counters["flushed_records"], 0);
    assert_eq!(counters["unconfirmed_records"], 1);
    assert_eq!(counters["ready"], false);
}
