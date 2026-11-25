use std::{
    fmt,
    io::{self, Write},
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, FromRequest, Request, State, rejection::JsonRejection},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor, value::MapAccessDeserializer},
};
use tokio::sync::{Semaphore, mpsc};

const BODY_LIMIT: usize = 32 * 1024;
const BATCH_LIMIT: usize = 64;
const INGRESS_LIMIT: usize = 16;
const BODY_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Batch {
    records: Vec<Object<Reading>>,
}

// Serde's struct deserializer also accepts positional arrays. The wire
// contract uses named fields at both the batch and record levels.
struct Object<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Object<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectVisitor<T> {
            type Value = Object<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an object")
            }

            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<Self::Value, M::Error> {
                T::deserialize(MapAccessDeserializer::new(map)).map(Object)
            }
        }

        deserializer.deserialize_map(ObjectVisitor(PhantomData))
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Reading {
    device_id: String,
    timestamp_ms: i64,
    metric: String,
    value: f64,
}

impl Reading {
    fn invalid_field(&self) -> Option<&'static str> {
        if !valid_identifier(&self.device_id) {
            Some("device_id")
        } else if self.timestamp_ms < 0 {
            Some("timestamp_ms")
        } else if !valid_identifier(&self.metric) {
            Some("metric")
        } else if !self.value.is_finite() {
            Some("value")
        } else {
            None
        }
    }
}

fn valid_identifier(value: &str) -> bool {
    value.len() <= 64
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(&byte))
}

#[derive(Clone)]
struct Ingest {
    sender: mpsc::Sender<Batch>,
    ingress: Arc<Semaphore>,
    counters: Arc<Mutex<Counters>>,
}

#[derive(Clone, Copy, Default, Serialize)]
struct Counters {
    accepted_batches: u64,
    accepted_records: u64,
    flushed_batches: u64,
    flushed_records: u64,
    rejected_requests: u64,
    overload_requests: u64,
    sink_errors: u64,
}

#[derive(Serialize)]
struct Metrics {
    #[serde(flatten)]
    counters: Counters,
    queue_capacity_batches: usize,
    ready: bool,
    unconfirmed_records: u64,
}

pub(super) struct Worker {
    receiver: mpsc::Receiver<Batch>,
    counters: Arc<Mutex<Counters>>,
}

pub(super) fn pipeline(queue_capacity: usize) -> (Router, Worker) {
    let (sender, receiver) = mpsc::channel(queue_capacity);
    let counters = Arc::new(Mutex::new(Counters::default()));
    let app = Router::new()
        .route("/v1/telemetry", post(ingest))
        .route("/health", get(|| async { StatusCode::NO_CONTENT }))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .with_state(Ingest {
            sender,
            ingress: Arc::new(Semaphore::new(INGRESS_LIMIT)),
            counters: Arc::clone(&counters),
        });
    (app, Worker { receiver, counters })
}

impl Worker {
    pub(super) fn run(mut self, mut output: impl Write) -> io::Result<()> {
        let mut encoded = Vec::new();
        while let Some(batch) = self.receiver.blocking_recv() {
            if let Err(error) = write_batch(&mut output, &mut encoded, &batch) {
                let mut counters = self.counters.lock().unwrap();
                self.receiver.close();
                counters.sink_errors += 1;
                return Err(error);
            }
            let mut counters = self.counters.lock().unwrap();
            counters.flushed_batches += 1;
            counters.flushed_records += batch.records.len() as u64;
        }
        Ok(())
    }
}

fn write_batch(output: &mut impl Write, encoded: &mut Vec<u8>, batch: &Batch) -> io::Result<()> {
    encoded.clear();
    for Object(record) in &batch.records {
        serde_json::to_writer(&mut *encoded, record)?;
        encoded.push(b'\n');
    }
    output.write_all(encoded)?;
    output.flush()
}

async fn ready(State(state): State<Ingest>) -> StatusCode {
    if state.sender.is_closed() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::NO_CONTENT
    }
}

async fn metrics(State(state): State<Ingest>) -> Json<Metrics> {
    let counters = state.counters.lock().unwrap();
    Json(Metrics {
        counters: *counters,
        queue_capacity_batches: state.sender.max_capacity(),
        ready: !state.sender.is_closed(),
        unconfirmed_records: counters.accepted_records - counters.flushed_records,
    })
}

#[derive(Serialize)]
struct Accepted {
    accepted_records: usize,
}

async fn ingest(State(state): State<Ingest>, request: Request) -> Response {
    match accept(&state, request).await {
        Ok(accepted) => (StatusCode::ACCEPTED, Json(accepted)).into_response(),
        Err(error) => {
            let mut counters = state.counters.lock().unwrap();
            counters.rejected_requests += 1;
            counters.overload_requests += u64::from(error.retryable());
            error.into_response()
        }
    }
}

async fn accept(state: &Ingest, request: Request) -> Result<Accepted, ApiError> {
    if state.sender.is_closed() {
        return Err(ApiError::sink_unavailable());
    }
    let _permit = state.ingress.try_acquire().map_err(|_| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ingress_busy",
            "Too many active uploads",
        )
    })?;
    let Json(Object(batch)) = tokio::time::timeout(
        BODY_TIMEOUT,
        Json::<Object<Batch>>::from_request(request, &()),
    )
    .await
    .map_err(|_| {
        ApiError::new(
            StatusCode::REQUEST_TIMEOUT,
            "body_timeout",
            "Upload exceeded 2 seconds",
        )
    })??;

    let count = batch.records.len();
    if !(1..=BATCH_LIMIT).contains(&count) {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_batch_size",
            "Provide between 1 and 64 records",
        ));
    }
    for (index, Object(record)) in batch.records.iter().enumerate() {
        if let Some(field) = record.invalid_field() {
            let mut error = ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_record",
                "Record field is outside its allowed range or format",
            );
            error.detail.record_index = Some(index);
            error.detail.field = Some(field);
            return Err(error);
        }
    }

    // Share the lock with the writer so a snapshot always counts admission
    // before the writer can count a completed batch.
    let mut counters = state.counters.lock().unwrap();
    match state.sender.try_send(batch) {
        Ok(()) => {
            counters.accepted_batches += 1;
            counters.accepted_records += count as u64;
            Ok(Accepted {
                accepted_records: count,
            })
        }
        Err(mpsc::error::TrySendError::Full(_)) => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "queue_full",
            "The telemetry queue is full",
        )),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(ApiError::sink_unavailable()),
    }
}

#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    field: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    record_index: Option<usize>,
}

struct ApiError {
    status: StatusCode,
    detail: ErrorDetail,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            detail: ErrorDetail {
                code,
                message,
                field: None,
                record_index: None,
            },
        }
    }

    fn sink_unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "sink_unavailable",
            "The telemetry writer is unavailable",
        )
    }

    fn retryable(&self) -> bool {
        matches!(self.detail.code, "queue_full" | "ingress_busy")
    }
}

impl From<JsonRejection> for ApiError {
    fn from(error: JsonRejection) -> Self {
        let (status, code, message) = match error {
            JsonRejection::MissingJsonContentType(_) => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_content_type",
                "Send an application/json body",
            ),
            JsonRejection::JsonDataError(_) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_schema",
                "JSON does not match the telemetry schema",
            ),
            _ if error.status() == StatusCode::PAYLOAD_TOO_LARGE => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "body_too_large",
                "JSON body exceeds 32 KiB",
            ),
            _ => (
                StatusCode::BAD_REQUEST,
                "invalid_body",
                "Could not read a complete JSON body",
            ),
        };
        Self::new(status, code, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let retryable = self.retryable();
        let mut response = (self.status, Json(self.detail)).into_response();
        if retryable {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}

#[cfg(test)]
mod tests;
