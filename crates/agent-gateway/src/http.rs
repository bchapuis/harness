//! The HTTP surface: a thin REST mapping onto the session entity, with SSE for the
//! live record stream.
//!
//! Every request authenticates by `Authorization: Bearer <token>` (the gateway
//! verifies it to a principal, then scopes the session key under it). Endpoints:
//! `prompt`/`records`/`cancel`/`sessions` are request-response; `stream` (and a
//! `prompt` with `Accept: text/event-stream`) ride a harness [`Follower`] as
//! Server-Sent Events (event types: `records`, `truncated`, `outcome`,
//! `error`, `end`). Both read forms surface snapshot compaction (§10.2):
//! `records` returns the journal's `base` beside the page, and a stream whose
//! cursor the compactor overtakes emits `truncated` before resuming.
//!
//! There is no control protocol underneath: the handler holds an `EntityRef` and
//! calls the entity directly ([`SessionRef::prompt_within`], [`SessionRef::tail`],
//! [`SessionRef::follow`]), and records tenancy ownership through the client
//! `Entities<Directory>`.

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
use entity::EntityError;
use futures::future::Either;
use serde::Deserialize;
use serde_json::json;
use tokio::task::JoinError;
use tokio::task::JoinHandle;

use agent::Followed;
use agent::Follower;
use agent::HarnessSystem;
use agent::PromptError;
use agent::Record;
use agent::RecordBody;
use agent::RunOutcome;
use agent::Seq;
use agent::SessionRef;
use agent::Turn;
use agent::TurnId;

use crate::Gateway;
use crate::auth::PrincipalId;
use crate::auth::is_valid_session;
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

/// The edge's tenant-isolation preamble, in its fixed order: verify the bearer
/// token, reject a malformed session id, resolve the principal-scoped ref (an
/// unknown kind is the client's `404`, a pure local check before any side
/// effect). Every session handler starts here, so a new endpoint cannot skip
/// a step and silently reopen the in-tenant collision hole `auth.rs` closes.
fn authed_session<S: HarnessSystem>(
    gw: &Gateway<S>,
    headers: &HeaderMap,
    kind: &str,
    session: &str,
) -> Result<(PrincipalId, SessionRef<S>), GatewayError> {
    let principal = principal(gw, headers)?;
    // A `400`, not a `404`: a session id the edge will not scope (see
    // [`is_valid_session`]) is malformed, and `404` would imply the name was
    // merely unused.
    if !is_valid_session(session) {
        return Err(GatewayError::bad_request(
            "session id must be non-empty and must not contain '/'",
        ));
    }
    let session_ref = gw.session(&principal, kind, session)?;
    Ok((principal, session_ref))
}

/// Liveness: the process is up and serving. Unauthenticated, always `200`.
async fn healthz() -> Response {
    (StatusCode::OK, "ok").into_response()
}

/// Readiness: `200` while serving, `503` once a shutdown signal has flipped the
/// gateway to draining, so a load balancer stops routing new traffic before the
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
/// leak a secret. Latency is omitted: timing must route through the `Clock` seam
/// (actor §11), never the wall clock.
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
    // The preamble resolves the kind before any side effect, so a typo or a
    // probing client never writes a phantom entry into the tenant's directory
    // index below.
    let (principal, session_ref) = authed_session(&gw, &headers, &kind, &session)?;
    // A malformed body is the edge's own `400`, in the structured envelope (not
    // axum's default plain-text rejection).
    let Json(mut body) = body.map_err(|e| GatewayError::bad_request(e.body_text()))?;
    // Clamp the caller's timeout to the edge ceiling once, before the
    // buffered/streaming branch, so a client cannot park a run (and the
    // resources behind it) indefinitely on either path.
    body.within_secs = body.within_secs.min(gw.limits().max_within_secs);
    // Record ownership before the run (best-effort and idempotent; see
    // `Gateway::record_ownership`); the run proceeds regardless.
    gw.record_ownership(&principal, &kind, &session).await;
    if wants_sse(&headers) {
        // A reconnect resumes at `Last-Event-ID` (the last seq the client saw);
        // re-submitting the same turn id reattaches the run (idempotent, §7.2).
        return Ok(prompt_stream(
            session_ref,
            body,
            resume_from(&headers, q.from),
        ));
    }
    // A run that *ran* and failed is a `200` outcome below; only a transport
    // failure or a rejected turn becomes a non-2xx status.
    let turn = Turn::new(TurnId::new(body.turn), body.content);
    let outcome = session_ref
        .prompt_within(turn, Duration::from_secs(body.within_secs))
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
    let (_, session_ref) = authed_session(&gw, &headers, &kind, &session)?;
    let history = session_ref.tail(Seq::new(q.from), q.limit).await?;
    // `base` is the compaction floor (§10.2): a `from` below it asked for
    // records the session's snapshot has subsumed, so the page's history
    // starts truncated; the client's one way to tell that from the journal
    // slots the projection legally skips.
    Ok(Json(json!({ "records": history.events, "base": history.base })).into_response())
}

/// Cancel a run (idempotent).
async fn cancel<S: HarnessSystem>(
    State(gw): State<Arc<Gateway<S>>>,
    Path((kind, session)): Path<(String, String)>,
    Query(q): Query<CancelQuery>,
    headers: HeaderMap,
) -> Result<Response, GatewayError> {
    let (_, session_ref) = authed_session(&gw, &headers, &kind, &session)?;
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
        .entity(principal.as_str())
        .ask(tenancy::ListByType { entity_type: q.kind })
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
/// turn). Ends when the watched turn's `RunEnded` record arrives, or at once
/// when the turn ended behind the cursor, where the follower never replays it.
async fn stream<S: HarnessSystem>(
    State(gw): State<Arc<Gateway<S>>>,
    Path((kind, session)): Path<(String, String)>,
    Query(q): Query<StreamQuery>,
    headers: HeaderMap,
) -> Result<Response, GatewayError> {
    let (_, session_ref) = authed_session(&gw, &headers, &kind, &session)?;
    let from = resume_from(&headers, q.from);
    // Asked once, before following: a turn that ends after this answer commits
    // its `RunEnded` past the cursor, where the follower delivers it. A failed
    // read streams as before, the follower then reporting the same failure.
    if let Ok(Some(_)) = session_ref.outcome(&TurnId::new(q.turn.clone())).await
        && !ends_ahead(&session_ref, Seq::new(from), &q.turn).await
    {
        let end = futures::stream::iter([Ok::<_, Infallible>(end_event())]);
        return Ok(Sse::new(end).into_response());
    }
    let follower = session_ref.follow(Seq::new(from));
    Ok(sse_response(session_ref, follower, from, q.turn, Tail::End))
}

/// The streaming `prompt`: submit the turn as a background task, stream the run's
/// records off a [`Follower`], and emit the terminal outcome once the watched
/// turn ends, or the task's error once its submit is refused or its wait ends.
fn prompt_stream<S: HarnessSystem>(
    session_ref: agent::SessionRef<S>,
    body: PromptBody,
    from: u64,
) -> Response {
    let follower = session_ref.follow(Seq::new(from));
    let turn = body.turn.clone();
    let prompt_ref = session_ref.clone();
    let prompt: JoinHandle<Result<RunOutcome, PromptError>> = tokio::spawn(async move {
        let turn = Turn::new(TurnId::new(body.turn), body.content);
        prompt_ref
            .prompt_within(turn, Duration::from_secs(body.within_secs))
            .await
    });
    sse_response(session_ref, follower, from, turn, Tail::Outcome(prompt))
}

/// The SSE response both streaming endpoints serve: forward record batches off
/// `follower` until `turn`'s run ends, then run the [`Tail`].
fn sse_response<S: HarnessSystem>(
    session: SessionRef<S>,
    follower: Follower<S>,
    from: u64,
    turn: String,
    tail: Tail,
) -> Response {
    let body = futures::stream::unfold(
        SessionStream::Streaming(Box::new(Live {
            session,
            follower,
            cursor: Seq::new(from),
            turn,
            tail,
        })),
        next_event,
    );
    Sse::new(body)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// The terminal step after a [`SessionStream`]'s watched turn ends: the observe-only
/// `stream` emits a payload-less `end`; the streaming `prompt` emits the run's
/// `outcome`, or the error that ended the caller's wait.
enum Tail {
    End,
    /// The prompt's background submit-and-await, still running.
    Outcome(JoinHandle<Result<RunOutcome, PromptError>>),
    /// The prompt's terminal event, settled while the turn's `RunEnded` was
    /// still ahead of the stream's cursor.
    Settled(Event),
}

/// The SSE stream's state: forwarding records, then the [`Tail`], then done.
enum SessionStream<S: HarnessSystem> {
    Streaming(Box<Live<S>>),
    Terminal(Tail),
    Done,
}

/// A stream still forwarding records for its watched turn.
struct Live<S: HarnessSystem> {
    session: SessionRef<S>,
    follower: Follower<S>,
    /// The last seq the stream delivered, or the cursor it started from.
    cursor: Seq,
    turn: String,
    tail: Tail,
}

/// What woke a streaming prompt: the follower, or its own background task.
enum Woke {
    Followed(Result<Followed, EntityError>),
    Prompt(Result<Result<RunOutcome, PromptError>, JoinError>),
}

async fn next_event<S: HarnessSystem>(
    state: SessionStream<S>,
) -> Option<(Result<Event, Infallible>, SessionStream<S>)> {
    match state {
        SessionStream::Streaming(mut live) => loop {
            // A streaming prompt races the follower against its own task: the
            // turn's records may never arrive (a rejected submit runs nothing;
            // a turn that ended before the cursor is not replayed), and the
            // caller's wait is bounded either way (edge §4.6). The follower is
            // polled first, so a batch that is ready wins a tie.
            let Live {
                session,
                follower,
                cursor,
                turn,
                tail,
            } = &mut *live;
            let woke = match tail {
                Tail::Outcome(prompt) => {
                    let followed = follower.next();
                    futures::pin_mut!(followed);
                    match futures::future::select(followed, prompt).await {
                        Either::Left((followed, _)) => Woke::Followed(followed),
                        Either::Right((joined, _)) => Woke::Prompt(joined),
                    }
                }
                Tail::End | Tail::Settled(_) => Woke::Followed(follower.next().await),
            };
            let event = match woke {
                Woke::Followed(Ok(Followed::Batch(records))) => {
                    if let Some((seq, _)) = records.last() {
                        *cursor = *seq;
                    }
                    let event = records_event(&records);
                    if ends_turn(&records, turn) {
                        return Some((Ok(event), SessionStream::Terminal(live.tail)));
                    }
                    event
                }
                // Compaction outran the stream's cursor (§10.2): the records up
                // to `base` are subsumed by the session's snapshot and cannot be
                // replayed. Tell the client rather than silently resuming past
                // the gap; the event id is the base, so a `Last-Event-ID`
                // reconnect also resumes after it.
                Woke::Followed(Ok(Followed::Truncated { base })) => {
                    *cursor = base;
                    Event::default()
                        .event("truncated")
                        .data(json!({ "base": base }).to_string())
                        .id(base.value().to_string())
                }
                Woke::Followed(Err(e)) => return Some((Ok(error_event(e)), SessionStream::Done)),
                // The run ended before the stream saw it end. If its `RunEnded`
                // is still ahead of the cursor, keep streaming up to it, so the
                // outcome follows the run's records; if it lies behind (a
                // re-prompt of a turn that ended before `from`), nothing more
                // of this run will arrive, so the outcome is the last word.
                Woke::Prompt(Ok(Ok(outcome))) => {
                    let event = outcome_event(&outcome);
                    if ends_ahead(session, *cursor, turn).await {
                        *tail = Tail::Settled(event);
                        continue;
                    }
                    return Some((Ok(event), SessionStream::Done));
                }
                // A rejected submit runs nothing, and an ended wait is the
                // caller's bound: either way the stream ends with the error.
                Woke::Prompt(joined) => {
                    return Some((Ok(prompt_event(joined)), SessionStream::Done));
                }
            };
            return Some((Ok(event), SessionStream::Streaming(live)));
        },
        SessionStream::Terminal(Tail::End) => Some((Ok(end_event()), SessionStream::Done)),
        SessionStream::Terminal(Tail::Settled(event)) => Some((Ok(event), SessionStream::Done)),
        SessionStream::Terminal(Tail::Outcome(prompt)) => {
            Some((Ok(prompt_event(prompt.await)), SessionStream::Done))
        }
        SessionStream::Done => None,
    }
}

/// The streaming prompt's terminal event, from its background task's result.
fn prompt_event(joined: Result<Result<RunOutcome, PromptError>, JoinError>) -> Event {
    match joined {
        Ok(Ok(outcome)) => outcome_event(&outcome),
        // A transport failure or a rejected turn: the structured error
        // envelope, same code taxonomy as the HTTP body.
        Ok(Err(e)) => error_event(e),
        Err(e) => error_event(GatewayError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            format!("prompt task failed: {e}"),
        )),
    }
}

/// The observe-only stream's terminal event: payload-less.
fn end_event() -> Event {
    Event::default().event("end").data("")
}

fn outcome_event(outcome: &RunOutcome) -> Event {
    Event::default()
        .event("outcome")
        .data(serde_json::to_string(outcome).unwrap_or_default())
}

/// Whether `turn`'s `RunEnded` is committed after `cursor`, read from the
/// journal page by page. A read that fails answers `false`: the outcome is
/// already in hand, and the client can page the records it missed.
async fn ends_ahead<S: HarnessSystem>(
    session: &SessionRef<S>,
    mut cursor: Seq,
    turn: &str,
) -> bool {
    loop {
        let Ok(page) = session.tail(cursor, agent::TAIL_PAGE).await else {
            return false;
        };
        let Some((last, _)) = page.events.last() else {
            return false;
        };
        if ends_turn(&page.events, turn) {
            return true;
        }
        cursor = *last;
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
/// (or `?from=`) and resumes exactly after it: the follower's cursor is
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

/// A failure as an SSE `error` event, in the structured `{ code, message }`
/// envelope (the same taxonomy as the HTTP error body).
fn error_event(e: impl Into<GatewayError>) -> Event {
    Event::default()
        .event("error")
        .data(e.into().to_event_data())
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
