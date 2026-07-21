//! The HTTP surface: a thin REST mapping onto the session grain, with SSE for the
//! live record stream.
//!
//! Every request authenticates by `Authorization: Bearer <token>` (the gateway
//! verifies it to a principal, then scopes the session key under it). Endpoints:
//! `prompt`/`records`/`cancel`/`sessions` are request-response; `stream` (and a
//! `prompt` with `Accept: text/event-stream`) ride a harness [`Follower`] as
//! Server-Sent Events. SSE is the agentic-runtime norm (Anthropic/OpenAI, MCP
//! streamable HTTP) and fits the harness's request-plus-server-stream shape.
//!
//! Unlike the old forwarding gateway, there is no control protocol underneath:
//! the handler holds a `GrainRef` and calls the grain directly
//! ([`SessionRef::prompt_within`], [`SessionRef::tail`], [`SessionRef::follow`]),
//! and records tenancy ownership through the client `Granary<Directory>`.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::Request;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header::ACCEPT;
use axum::http::header::AUTHORIZATION;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::response::Sse;
use axum::response::sse::Event;
use axum::response::sse::KeepAlive;
use axum::routing::get;
use axum::routing::post;
use serde::Deserialize;
use serde_json::json;
use tokio::task::JoinHandle;

use harness::Follower;
use harness::GrainError;
use harness::HarnessSystem;
use harness::Record;
use harness::RecordBody;
use harness::RunOutcome;
use harness::Seq;
use harness::Turn;
use harness::TurnId;

use crate::Gateway;
use crate::auth::PrincipalId;
use crate::auth::unscope_session;
use crate::error::GatewayError;

/// Build the router over the shared gateway state. `/healthz` and `/readyz` are
/// unauthenticated liveness/readiness probes outside the `/v1` tree. A
/// `DefaultBodyLimit` caps the request body and a request-log layer records one
/// concise line per non-probe request.
pub fn router<S: HarnessSystem>(gateway: Arc<Gateway<S>>) -> Router {
    let max_body = gateway.limits().max_body_bytes;
    Router::new()
        .route("/v1/sessions", get(list_sessions::<S>))
        .route("/v1/{kind}/{session}/prompt", post(prompt::<S>))
        .route("/v1/{kind}/{session}/records", get(records::<S>))
        .route("/v1/{kind}/{session}/stream", get(stream::<S>))
        .route("/v1/{kind}/{session}/cancel", post(cancel::<S>))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz::<S>))
        .layer(DefaultBodyLimit::max(max_body))
        .layer(axum::middleware::from_fn(log_request))
        .with_state(gateway)
}

/// Liveness: the process is up and serving. Unauthenticated, always `200`.
async fn healthz() -> Response {
    (StatusCode::OK, "ok").into_response()
}

/// Readiness: `200` while serving, `503` once a shutdown signal has flipped the
/// gateway to draining — so a load balancer stops routing new traffic before the
/// in-flight requests drain. Unauthenticated.
async fn readyz<S: HarnessSystem>(State(gw): State<Arc<Gateway<S>>>) -> Response {
    if gw.is_ready() {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response()
    }
}

/// One concise line per request (method, path, status) to stderr. Skips the
/// health probes so k8s liveness/readiness polling stays quiet. The path never
/// carries the bearer token (it rides the `Authorization` header), so this cannot
/// leak a secret. Request latency is deliberately omitted: timing must route
/// through the `Clock` seam (§18.1, never the wall clock), so per-request latency
/// belongs to a future metrics layer, not this edge log line.
async fn log_request(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let resp = next.run(req).await;
    if path != "/healthz" && path != "/readyz" {
        eprintln!("[gateway] {method} {path} -> {}", resp.status().as_u16());
    }
    resp
}

fn default_within() -> u64 {
    600
}
fn default_limit() -> u32 {
    500
}

#[derive(Deserialize)]
struct PromptBody {
    turn: String,
    content: String,
    #[serde(default = "default_within")]
    within_secs: u64,
}

#[derive(Deserialize)]
struct FromQuery {
    #[serde(default)]
    from: u64,
}

#[derive(Deserialize)]
struct StreamQuery {
    turn: String,
    #[serde(default)]
    from: u64,
}

#[derive(Deserialize)]
struct RecordsQuery {
    #[serde(default)]
    from: u64,
    #[serde(default = "default_limit")]
    limit: u32,
}

#[derive(Deserialize)]
struct CancelQuery {
    turn: String,
}

#[derive(Deserialize)]
struct ListQuery {
    kind: String,
}

/// Submit a turn. With `Accept: text/event-stream` the response is an SSE stream
/// of the run's records ending in an `outcome` event; otherwise it blocks and
/// returns the terminal outcome as JSON. `?from=` sets where a streamed watch
/// starts (the client's last-seen seq, or 0 for the whole run).
async fn prompt<S: HarnessSystem>(
    State(gw): State<Arc<Gateway<S>>>,
    Path((kind, session)): Path<(String, String)>,
    Query(q): Query<FromQuery>,
    headers: HeaderMap,
    body: Result<Json<PromptBody>, JsonRejection>,
) -> Result<Response, GatewayError> {
    let principal = principal(&gw, &headers)?;
    // A malformed body is the edge's own `400`, in the structured envelope (not
    // axum's default plain-text rejection).
    let Json(body) = body.map_err(|e| GatewayError::bad_request(e.body_text()))?;
    // Record ownership before the run (best-effort and idempotent — see
    // `Gateway::record_ownership`); the run proceeds regardless.
    gw.record_ownership(&principal, &kind, &session).await;
    let session_ref = gw.session(&principal, &kind, &session);
    if wants_sse(&headers) {
        // A reconnect resumes at `Last-Event-ID` (the last seq the client saw);
        // re-submitting the same turn id reattaches the run (idempotent, §7.4).
        return Ok(prompt_stream(
            session_ref,
            body,
            resume_from(&headers, q.from),
        ));
    }
    // Clamp the caller's timeout to the edge ceiling so a run cannot be parked
    // indefinitely; a run that *ran* and failed is a `200` outcome below, only a
    // transport failure becomes a non-2xx status.
    let within = body.within_secs.min(gw.limits().max_within_secs);
    let turn = Turn::new(TurnId::new(body.turn), body.content);
    let outcome = session_ref
        .prompt_within(turn, Duration::from_secs(within))
        .await?;
    Ok(Json(json!({ "outcome": outcome })).into_response())
}

/// Read a page of committed records.
async fn records<S: HarnessSystem>(
    State(gw): State<Arc<Gateway<S>>>,
    Path((kind, session)): Path<(String, String)>,
    Query(q): Query<RecordsQuery>,
    headers: HeaderMap,
) -> Result<Response, GatewayError> {
    let principal = principal(&gw, &headers)?;
    let session_ref = gw.session(&principal, &kind, &session);
    let records = session_ref.tail(Seq::new(q.from), q.limit).await?;
    Ok(Json(json!({ "records": records })).into_response())
}

/// Cancel a run (idempotent).
async fn cancel<S: HarnessSystem>(
    State(gw): State<Arc<Gateway<S>>>,
    Path((kind, session)): Path<(String, String)>,
    Query(q): Query<CancelQuery>,
    headers: HeaderMap,
) -> Result<Response, GatewayError> {
    let principal = principal(&gw, &headers)?;
    let session_ref = gw.session(&principal, &kind, &session);
    session_ref.cancel(&TurnId::new(q.turn)).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// List the tenant's sessions of a kind, from its directory. The entries' keys
/// are principal-scoped; strip the prefix back off so the client sees the session
/// ids it supplied (an entry that does not unscope is not this principal's and is
/// dropped).
async fn list_sessions<S: HarnessSystem>(
    State(gw): State<Arc<Gateway<S>>>,
    Query(q): Query<ListQuery>,
    headers: HeaderMap,
) -> Result<Response, GatewayError> {
    let principal = principal(&gw, &headers)?;
    let entries = gw
        .directory()
        .grain(principal.as_str())
        .ask(tenancy::ListByType { grain_type: q.kind })
        .await?;
    let sessions: Vec<_> = entries
        .into_iter()
        .filter_map(|entry| {
            unscope_session(&principal, entry.name.key())
                .map(|session| json!({ "session": session, "label": entry.meta.label }))
        })
        .collect();
    Ok(Json(json!({ "sessions": sessions })).into_response())
}

/// Stream a run's records live as SSE (no prompt; observe an in-flight or past
/// turn). Ends when the watched turn's `RunEnded` record arrives.
async fn stream<S: HarnessSystem>(
    State(gw): State<Arc<Gateway<S>>>,
    Path((kind, session)): Path<(String, String)>,
    Query(q): Query<StreamQuery>,
    headers: HeaderMap,
) -> Result<Response, GatewayError> {
    let principal = principal(&gw, &headers)?;
    let session_ref = gw.session(&principal, &kind, &session);
    let follower = session_ref.follow(Seq::new(resume_from(&headers, q.from)));
    let body = futures::stream::unfold(
        SessionStream::Streaming {
            follower,
            turn: q.turn,
            tail: Tail::End,
        },
        next_event,
    );
    Ok(Sse::new(body)
        .keep_alive(KeepAlive::default())
        .into_response())
}

/// The streaming `prompt`: submit the turn as a background task, stream the run's
/// records off a [`Follower`], and emit the terminal outcome once the watched
/// turn ends. Mirrors the old adapter's parked-prompt-plus-live-watch.
fn prompt_stream<S: HarnessSystem>(
    session_ref: harness::SessionRef<S>,
    body: PromptBody,
    from: u64,
) -> Response {
    let follower = session_ref.follow(Seq::new(from));
    let turn = body.turn.clone();
    let prompt_ref = session_ref.clone();
    let prompt: JoinHandle<Result<RunOutcome, GrainError>> = tokio::spawn(async move {
        let turn = Turn::new(TurnId::new(body.turn), body.content);
        prompt_ref
            .prompt_within(turn, Duration::from_secs(body.within_secs))
            .await
    });
    let body = futures::stream::unfold(
        SessionStream::Streaming {
            follower,
            turn,
            tail: Tail::Outcome(prompt),
        },
        next_event,
    );
    Sse::new(body)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// The terminal step after a [`SessionStream`]'s watched turn ends: the observe-only
/// `stream` emits a payload-less `end`; the streaming `prompt` awaits its background
/// task and emits the run's `outcome`. This tail is the *only* thing the two
/// endpoints differ in — the record-forwarding half is shared.
enum Tail {
    End,
    Outcome(JoinHandle<Result<RunOutcome, GrainError>>),
}

/// A server-sent-event stream over a session's records: forward record batches off a
/// [`Follower`] until the watched turn ends, then run the [`Tail`]. Both the observe
/// (`stream`) and streaming-prompt (`prompt_stream`) endpoints are this one machine.
enum SessionStream<S: HarnessSystem> {
    Streaming {
        follower: Follower<S>,
        turn: String,
        tail: Tail,
    },
    Terminal(Tail),
    Done,
}

async fn next_event<S: HarnessSystem>(
    state: SessionStream<S>,
) -> Option<(Result<Event, Infallible>, SessionStream<S>)> {
    match state {
        SessionStream::Streaming {
            mut follower,
            turn,
            tail,
        } => match follower.next().await {
            Ok(records) => {
                let event = records_event(&records);
                let next = if ends_turn(&records, &turn) {
                    SessionStream::Terminal(tail)
                } else {
                    SessionStream::Streaming {
                        follower,
                        turn,
                        tail,
                    }
                };
                Some((Ok(event), next))
            }
            Err(e) => Some((Ok(error_event(e)), SessionStream::Done)),
        },
        SessionStream::Terminal(Tail::End) => Some((
            Ok(Event::default().event("end").data("")),
            SessionStream::Done,
        )),
        SessionStream::Terminal(Tail::Outcome(prompt)) => {
            let event = match prompt.await {
                Ok(Ok(outcome)) => Event::default()
                    .event("outcome")
                    .data(serde_json::to_string(&outcome).unwrap_or_default()),
                // Transport failure reaching/committing the run — the structured
                // error envelope, same code taxonomy as the HTTP body.
                Ok(Err(e)) => Event::default()
                    .event("error")
                    .data(GatewayError::from(e).to_event_data()),
                Err(e) => Event::default().event("error").data(
                    GatewayError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        format!("prompt task failed: {e}"),
                    )
                    .to_event_data(),
                ),
            };
            Some((Ok(event), SessionStream::Done))
        }
        SessionStream::Done => None,
    }
}

/// Whether a record batch carries the `RunEnded` for `turn`.
fn ends_turn(records: &[(Seq, Record)], turn: &str) -> bool {
    records
        .iter()
        .any(|(_, r)| matches!(&r.body, RecordBody::RunEnded { turn: t, .. } if t.as_str() == turn))
}

/// A record batch as an SSE `records` event. The event `id:` is the last seq in
/// the batch, so a client that drops mid-stream reconnects with `Last-Event-ID`
/// (or `?from=`) and resumes exactly after it — the follower's cursor is
/// exclusive, so no record is delivered twice.
fn records_event(records: &[(Seq, Record)]) -> Event {
    let event = Event::default()
        .event("records")
        .data(serde_json::to_string(records).unwrap_or_default());
    match records.last() {
        Some((seq, _)) => event.id(seq.value().to_string()),
        None => event,
    }
}

/// A transport/durability error as an SSE `error` event, in the structured
/// `{ code, message }` envelope (the same taxonomy as the HTTP error body).
fn error_event(e: GrainError) -> Event {
    Event::default()
        .event("error")
        .data(GatewayError::from(e).to_event_data())
}

/// Verify the request's bearer token to the principal it names, or a `401`
/// [`GatewayError`] (carrying a `WWW-Authenticate: Bearer` challenge).
fn principal<S: HarnessSystem>(
    gw: &Gateway<S>,
    headers: &HeaderMap,
) -> Result<PrincipalId, GatewayError> {
    let token =
        bearer(headers).ok_or_else(|| GatewayError::unauthorized("missing bearer token"))?;
    gw.principal(token)
        .ok_or_else(|| GatewayError::unauthorized("invalid token"))
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

fn wants_sse(headers: &HeaderMap) -> bool {
    headers
        .get(ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("text/event-stream"))
        .unwrap_or(false)
}

/// The seq to resume an SSE stream from: the standard `Last-Event-ID` header (the
/// last event id the client received) if present and numeric, else the `?from=`
/// query value. Both are the follower's *exclusive* cursor, so resuming yields
/// only records strictly after it.
fn resume_from(headers: &HeaderMap, query_from: u64) -> u64 {
    headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(query_from)
}
