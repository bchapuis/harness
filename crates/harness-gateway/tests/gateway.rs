//! Gateway integration: drive the axum router (via `oneshot`, no socket) against
//! an in-process `LocalSystem` that both *hosts* the Agent + Directory grains and
//! serves as the gateway's own cluster-client system. This is the single-node
//! analogue of the production split (nodes host; the gateway is a client over the
//! same transport): it proves the gateway terminates tenant auth, scopes each
//! session under its principal, drives a prompt to the grain **directly** (no
//! control protocol), and isolates tenants — alice never sees bob's sessions.

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
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use harness::Budget;
use harness::Harness;
use harness::Kind;
use harness::Kinds;
use harness::Model;
use harness::ModelError;
use harness::ModelParams;
use harness::ModelRequest;
use harness::ModelResponse;
use harness::Sandbox;
use harness::SandboxError;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SessionId;
use harness::Usage;
use harness_gateway::auth::InsecureTokens;
use serde_json::Value;
use tenancy::Directory;
use tower::ServiceExt;

type Sys = LocalSystem<TokioClock, OsEntropy, TokioSpawner>;

/// A model that immediately ends the run with a final message — no tool calls, so
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
    ) -> BoxFuture<'static, Result<Arc<dyn Sandbox>, SandboxError>> {
        Box::pin(async { unreachable!("no sandboxed tool is called in this test") })
    }
}

/// The hosting kinds: the same `assistant`/`worker` names and default (4) shards
/// the gateway's `client_kinds` addresses, each carrying model params so the
/// agent loop runs.
fn host_kinds() -> Kinds {
    let params = ModelParams {
        model: "test-model".to_string(),
        max_tokens: 64,
    };
    let build = |prompt: &str| {
        Kind::new(prompt)
            .model(params.clone())
            .grain(GranaryConfig::default())
            .budget(Budget::new(1_000_000, 100))
    };
    Kinds::new()
        .register("assistant", build("assistant"))
        .register("worker", build("worker"))
}

/// Stand up one LocalSystem hosting the grains, then build the gateway as a client
/// over the same system.
async fn gateway() -> Arc<harness_gateway::Gateway<Sys>> {
    let system = LocalSystem::new(TokioClock::new(), OsEntropy::new(), TokioSpawner::current());
    // Host the Agent kinds and the tenancy Directory on this system.
    let _hosted = Harness::cluster(
        system.clone(),
        &host_kinds(),
        Arc::new(DoneModel),
        Arc::new(NoSandbox),
    );
    let _directory: Granary<Directory<Sys>> = system.granary(GranaryConfig::default());
    // The gateway joins as a client over the same system. On a single LocalSystem
    // the host gateways are in the receptionist immediately, so discovery returns
    // at once.
    harness_gateway::connect(
        system,
        harness_gateway::client_kinds(),
        GranaryConfig::default().shards,
        Box::new(InsecureTokens),
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

fn list_req(token: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri("/v1/sessions?kind=assistant")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn prompt_runs_the_grain_and_isolates_tenants() {
    let gw = gateway().await;

    // Alice prompts a session; the gateway scopes it, runs the grain to its
    // terminal outcome, and records ownership.
    let resp = harness_gateway::http::router(gw.clone())
        .oneshot(prompt_req(Some("alice"), "demo"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let outcome = body_json(resp).await;
    assert!(outcome.get("outcome").is_some(), "{outcome}");

    // Alice sees her session in the listing.
    let resp = harness_gateway::http::router(gw.clone())
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

    // Bob, a different tenant, sees none of alice's sessions — the principal scope
    // keeps her grains unreachable.
    let resp = harness_gateway::http::router(gw.clone())
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

#[tokio::test]
async fn a_missing_token_is_unauthorized() {
    let gw = gateway().await;
    let resp = harness_gateway::http::router(gw)
        .oneshot(prompt_req(None, "demo"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
