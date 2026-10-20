//! Gateway integration: drive the axum router (via `oneshot`, no socket) against
//! an in-process `LocalSystem` that both *hosts* the Agent + Directory entities and
//! serves as the gateway's own cluster-client system. This is the single-node
//! analogue of the production split (nodes host; the gateway is a client over the
//! same transport): it proves the gateway terminates tenant auth, scopes each
//! session under its principal, drives a prompt to the entity **directly** (no
//! control protocol), and isolates tenants; alice never sees bob's sessions.

use std::sync::Arc;
use std::time::Duration;

use actor_core::BoxFuture;
use actor_core::LocalSystem;
use actor_runtime::OsEntropy;
use actor_runtime::TokioClock;
use actor_runtime::TokioSpawner;
use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use entity::Entities;
use entity::EntityConfig;
use entity::EntityExt;
use agent::Budget;
use agent::Harness;
use agent::Kind;
use agent::Kinds;
use agent::Model;
use agent::ModelError;
use agent::ModelParams;
use agent::ModelRequest;
use agent::ModelResponse;
use agent::Sandbox;
use agent::SandboxError;
use agent::SandboxProfile;
use agent::SandboxProvider;
use agent::SessionId;
use agent::Usage;
use agent_gateway::auth::InsecureTokens;
use serde_json::Value;
use tenancy::Directory;
use tower::ServiceExt;

type Sys = LocalSystem<TokioClock, OsEntropy, TokioSpawner>;

/// A model that immediately ends the run with a final message, no tool calls, so
/// the sandbox is never opened.
struct DoneModel;
impl Model for DoneModel {
    fn complete(
        &self,
        _req: ModelRequest,
    ) -> BoxFuture<'static, Result<ModelResponse, ModelError>> {
        Box::pin(async {
            Ok(ModelResponse {
                content: "done".to_string(),
                calls: Vec::new(),
                usage: Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                },
            })
        })
    }
}

/// A sandbox provider that is never reached (the model calls no tools).
struct NoSandbox;
impl SandboxProvider for NoSandbox {
    fn open(
        &self,
        _session: &SessionId,
        _profile: &SandboxProfile,
        _workspace: &std::path::Path,
    ) -> BoxFuture<'static, Result<Arc<dyn Sandbox>, SandboxError>> {
        Box::pin(async { unreachable!("no sandboxed tool is called in this test") })
    }
}

/// The hosting kinds: the `assistant`/`worker` names and default (4) shards the
/// client roster below addresses, each carrying model params so the agent loop
/// runs.
fn host_kinds() -> Kinds {
    let params = ModelParams {
        model: "test-model".to_string(),
        max_tokens: 64,
    };
    let build = |prompt: &str| {
        Kind::new(prompt)
            .model(params.clone())
            .entity(EntityConfig::default())
            .budget(Budget::new(1_000_000, 100))
    };
    Kinds::new()
        .register("assistant", build("assistant"))
        // `worker` snapshots aggressively, so the records test below crosses
        // the compaction trigger (storage §9) in a handful of turns. The
        // config is host-side policy: the client-side registry needs no match.
        .register(
            "worker",
            build("worker").entity(EntityConfig {
                snapshot_every: 6,
                ..EntityConfig::default()
            }),
        )
}

/// Stand up one LocalSystem hosting the entities, then build the gateway as a client
/// over the same system.
async fn gateway() -> Arc<agent_gateway::Gateway<Sys>> {
    let system = LocalSystem::new(TokioClock::new(), OsEntropy::new(), TokioSpawner::current());
    // Host the Agent kinds and the tenancy Directory on this system.
    let _hosted = Harness::cluster(
        system.clone(),
        &host_kinds(),
        Arc::new(DoneModel),
        Arc::new(NoSandbox),
    );
    let _directory: Entities<Directory<Sys>> = system.entities(EntityConfig::default());
    // The gateway joins as a client over the same system. On a single LocalSystem
    // the host gateways are in the receptionist immediately, so discovery returns
    // at once.
    agent_gateway::connect(
        system,
        &["assistant".to_string(), "worker".to_string()],
        Arc::new(InsecureTokens),
        agent_gateway::Limits::default(),
        Duration::from_secs(5),
    )
    .await
    .expect("the client discovers the host gateways on the same system")
}

fn prompt_req(token: Option<&str>, session: &str) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("/v1/assistant/{session}/prompt"))
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder
        .body(Body::from(
            r#"{"turn":"t-1","content":"hi","within_secs":30}"#,
        ))
        .unwrap()
}

/// A streaming prompt request (`Accept: text/event-stream`), which drives the SSE
/// path: record events plus a terminal `outcome`.
fn sse_prompt_req(token: &str, session: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/v1/assistant/{session}/prompt"))
        .header("content-type", "application/json")
        .header("accept", "text/event-stream")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(
            r#"{"turn":"t-1","content":"hi","within_secs":30}"#,
        ))
        .unwrap()
}

fn list_req(token: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri("/v1/sessions?kind=assistant")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

fn get_req(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

async fn body_text(resp: axum::response::Response) -> String {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn prompt_runs_the_entity_and_isolates_tenants() {
    let gw = gateway().await;

    // Alice prompts a session; the gateway scopes it, runs the entity to its
    // terminal outcome, and records ownership.
    let resp = agent_gateway::http::router(gw.clone())
        .oneshot(prompt_req(Some("alice"), "demo"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let outcome = body_json(resp).await;
    assert!(outcome.get("outcome").is_some(), "{outcome}");

    // Alice sees her session in the listing.
    let resp = agent_gateway::http::router(gw.clone())
        .oneshot(list_req("alice"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let listing = body_json(resp).await;
    let sessions = listing["sessions"].as_array().expect("sessions array");
    assert!(
        sessions.iter().any(|s| s["session"] == "demo"),
        "alice should see her own session: {listing}"
    );

    // Bob, a different tenant, sees none of alice's sessions: the principal scope
    // keeps her entities unreachable.
    let resp = agent_gateway::http::router(gw.clone())
        .oneshot(list_req("bob"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let listing = body_json(resp).await;
    assert_eq!(
        listing["sessions"]
            .as_array()
            .expect("sessions array")
            .len(),
        0,
        "bob must not see alice's sessions: {listing}"
    );
}

/// `GET /records` surfaces the journal's compaction base (§10.2): once a
/// session's snapshot has subsumed its oldest records, the page reports
/// `base > 0` beside the surviving records, so a client reading from 0 learns
/// its history starts truncated instead of mistaking the short page for the
/// whole transcript.
#[tokio::test]
async fn records_reports_the_compaction_base() {
    let gw = gateway().await;

    // Four turns against the compacting `worker` kind cross its snapshot
    // trigger (`snapshot_every: 6` in `host_kinds`) mid-history.
    for turn in ["t-1", "t-2", "t-3", "t-4"] {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/worker/demo/prompt")
            .header("content-type", "application/json")
            .header("authorization", "Bearer alice")
            .body(Body::from(format!(
                r#"{{"turn":"{turn}","content":"hi","within_secs":30}}"#
            )))
            .unwrap();
        let resp = agent_gateway::http::router(gw.clone())
            .oneshot(req)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let req = Request::builder()
        .method("GET")
        .uri("/v1/worker/demo/records?from=0")
        .header("authorization", "Bearer alice")
        .body(Body::empty())
        .unwrap();
    let resp = agent_gateway::http::router(gw.clone())
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let base = body["base"].as_u64().expect("the page carries the base");
    assert!(base > 0, "compaction ran and the page reports it: {body}");
    let records = body["records"].as_array().expect("records array");
    assert!(
        !records.is_empty(),
        "the post-snapshot suffix is still readable: {body}"
    );
    let first_seq = records[0][0].as_u64().expect("a [seq, record] tuple");
    assert!(
        first_seq > base,
        "every record served lies above the base: {body}"
    );
}

#[tokio::test]
async fn a_missing_token_is_unauthorized() {
    let gw = gateway().await;
    let resp = agent_gateway::http::router(gw)
        .oneshot(prompt_req(None, "demo"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    // A `401` carries the `WWW-Authenticate: Bearer` challenge and the structured
    // error envelope (a stable machine `code`, not a `Debug` string).
    assert_eq!(
        resp.headers()
            .get("www-authenticate")
            .and_then(|v| v.to_str().ok()),
        Some("Bearer")
    );
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "unauthorized", "{body}");
    assert!(body["error"]["message"].as_str().is_some(), "{body}");
}

#[tokio::test]
async fn a_malformed_body_is_a_structured_bad_request() {
    let gw = gateway().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/assistant/demo/prompt")
        .header("content-type", "application/json")
        .header("authorization", "Bearer alice")
        .body(Body::from(r#"{"turn":"t-1""#)) // truncated JSON
        .unwrap();
    let resp = agent_gateway::http::router(gw)
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "bad_request", "{body}");
}

/// A turn id reused with different content is the client's permanent mistake
/// (agent spec §7.2): a structured `409`, never a retryable or upstream status.
#[tokio::test]
async fn a_reused_turn_id_with_new_content_is_a_conflict() {
    let gw = gateway().await;
    let resp = agent_gateway::http::router(gw.clone())
        .oneshot(prompt_req(Some("alice"), "demo"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let req = Request::builder()
        .method("POST")
        .uri("/v1/assistant/demo/prompt")
        .header("content-type", "application/json")
        .header("authorization", "Bearer alice")
        .body(Body::from(
            r#"{"turn":"t-1","content":"something else","within_secs":30}"#,
        ))
        .unwrap();
    let resp = agent_gateway::http::router(gw).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "turn_conflict", "{body}");
}

#[tokio::test]
async fn health_is_live_and_readiness_reflects_draining() {
    let gw = gateway().await;

    let resp = agent_gateway::http::router(gw.clone())
        .oneshot(get_req("/healthz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = agent_gateway::http::router(gw.clone())
        .oneshot(get_req("/readyz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Once draining, readiness fails so a load balancer deregisters the pod.
    gw.set_ready(false);
    let resp = agent_gateway::http::router(gw)
        .oneshot(get_req("/readyz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn streaming_prompt_carries_seq_ids_and_a_terminal_outcome() {
    let gw = gateway().await;
    let resp = agent_gateway::http::router(gw)
        .oneshot(sse_prompt_req("alice", "streamed"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let stream = body_text(resp).await;
    // Record events carry an SSE `id:` (the seq) for `Last-Event-ID` resumption,
    // and the run ends with a terminal `outcome` event.
    assert!(stream.contains("event: records"), "{stream}");
    assert!(stream.contains("\nid: "), "{stream}");
    assert!(stream.contains("event: outcome"), "{stream}");
}

/// A streaming prompt for `t-1` with `content`, resuming at cursor `from`.
fn sse_prompt_from(session: &str, content: &str, from: u64) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/v1/assistant/{session}/prompt?from={from}"))
        .header("content-type", "application/json")
        .header("accept", "text/event-stream")
        .header("authorization", "Bearer alice")
        .body(Body::from(format!(
            r#"{{"turn":"t-1","content":"{content}","within_secs":30}}"#
        )))
        .unwrap()
}

/// Drain an SSE body, failing instead of hanging if the stream never ends.
async fn drained(resp: axum::response::Response) -> String {
    tokio::time::timeout(Duration::from_secs(10), body_text(resp))
        .await
        .expect("the stream ends")
}

/// A streamed prompt whose turn id was used for other content (a `409`) ends
/// with the conflict even when the original run's `RunEnded` lies behind the
/// stream's cursor, where the follower will never deliver it (edge §4.6).
#[tokio::test]
async fn a_streamed_conflict_past_the_old_run_ends_with_the_conflict() {
    let gw = gateway().await;
    let resp = agent_gateway::http::router(gw.clone())
        .oneshot(prompt_req(Some("alice"), "conflicted"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = agent_gateway::http::router(gw)
        .oneshot(sse_prompt_from("conflicted", "something else", 1_000))
        .await
        .unwrap();
    let stream = drained(resp).await;
    assert!(stream.contains("event: error"), "{stream}");
    assert!(stream.contains("turn_conflict"), "{stream}");
    assert!(!stream.contains("event: outcome"), "{stream}");
}

/// An idempotent re-prompt of a turn that already ended returns its recorded
/// outcome on the stream, even when its `RunEnded` lies behind the cursor.
#[tokio::test]
async fn a_streamed_reprompt_of_an_ended_turn_returns_its_outcome() {
    let gw = gateway().await;
    let resp = agent_gateway::http::router(gw.clone())
        .oneshot(prompt_req(Some("alice"), "replayed"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = agent_gateway::http::router(gw)
        .oneshot(sse_prompt_from("replayed", "hi", 1_000))
        .await
        .unwrap();
    let stream = drained(resp).await;
    assert!(stream.contains("event: outcome"), "{stream}");
    assert!(stream.contains("done"), "{stream}");
    assert!(
        !stream.contains("event: records"),
        "nothing after the cursor: {stream}"
    );
}

/// An observe-only stream of `t-1`, resuming at cursor `from`.
fn stream_req(session: &str, from: u64) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(format!("/v1/assistant/{session}/stream?turn=t-1&from={from}"))
        .header("accept", "text/event-stream")
        .header("authorization", "Bearer alice")
        .body(Body::empty())
        .unwrap()
}

/// Watching a turn that ended behind the cursor ends at once: the follower
/// never replays its `RunEnded`, so waiting for it would never end (edge §4.6).
#[tokio::test]
async fn a_stream_of_a_turn_that_ended_before_from_ends_at_once() {
    let gw = gateway().await;
    let resp = agent_gateway::http::router(gw.clone())
        .oneshot(prompt_req(Some("alice"), "watched"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = agent_gateway::http::router(gw)
        .oneshot(stream_req("watched", 1_000))
        .await
        .unwrap();
    let stream = drained(resp).await;
    assert!(stream.contains("event: end"), "{stream}");
    assert!(!stream.contains("event: records"), "{stream}");
}

/// Watching from before the run replays its records, then ends after the
/// `RunEnded` batch.
#[tokio::test]
async fn a_stream_from_before_the_run_replays_it_then_ends() {
    let gw = gateway().await;
    let resp = agent_gateway::http::router(gw.clone())
        .oneshot(prompt_req(Some("alice"), "replayed-stream"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = agent_gateway::http::router(gw)
        .oneshot(stream_req("replayed-stream", 0))
        .await
        .unwrap();
    let stream = drained(resp).await;
    let ended = stream.find("RunEnded").expect("the run's records replay");
    let end = stream.find("event: end").expect("the stream ends");
    assert!(ended < end, "`end` follows the `RunEnded` batch: {stream}");
}
