//! End-to-end test against the official `agent-client-protocol` Rust client.
//!
//! This is the strongest conformance check we have: the real SDK client spawns
//! the real `harness-standalone acp <addr>` binary as a subprocess (exactly as
//! an editor like Zed would), speaks ACP over its stdio, and deserializes every
//! reply through its strongly-typed `schema::v1` types. A shape the adapter gets
//! wrong fails to parse here — the wire is validated by construction.
//!
//! The agent connects to a [`FakeNode`] (the control-port double from
//! `common`), so the turn is deterministic with no real cluster, model, or
//! sandbox in the loop.

mod common;

use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;

use agent_client_protocol::AcpAgent;
use agent_client_protocol::Agent;
use agent_client_protocol::Client;
use agent_client_protocol::ConnectionTo;
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::ContentBlock;
use agent_client_protocol::schema::v1::InitializeRequest;
use agent_client_protocol::schema::v1::NewSessionRequest;
use agent_client_protocol::schema::v1::PromptRequest;
use agent_client_protocol::schema::v1::SessionNotification;
use agent_client_protocol::schema::v1::TextContent;
use common::*;
use serde_json::Value;
use serde_json::json;

#[tokio::test]
async fn the_official_client_drives_a_full_turn() {
    let scenario = Scenario::new().turn(
        vec![
            turn_submitted("t-1", "list the files"),
            model_text("t-1", "Listing now."),
            model_tool_call("t-1", "call-1", "shell", json!({ "command": "ls" })),
            tool_ok("t-1", "call-1", json!({ "exit_code": 0, "stdout": "a.txt\n" })),
            run_ended_ok("t-1", "Here are the files."),
        ],
        completion("Here are the files."),
    );
    let node = FakeNode::start(scenario).await;
    let addr = node.addr();

    // Spawn the real adapter binary, pointed at the fake node. The JSON `stdio`
    // form avoids shell-splitting a path that might contain spaces.
    let bin = env!("CARGO_BIN_EXE_harness-standalone");
    let spec = json!({
        "type": "stdio",
        "name": "harness-standalone-acp",
        "command": bin,
        "args": ["acp", addr],
        "env": [],
    })
    .to_string();
    let agent = AcpAgent::from_str(&spec).expect("valid agent spec");

    // Every `session/update` the agent streams, re-serialized to JSON. That the
    // SDK parsed each one into `SessionNotification` already proves wire
    // conformance; the JSON just lets us assert which kinds arrived.
    let updates: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = updates.clone();

    let (init, stop_reason): (Value, Value) = Client
        .builder()
        .on_receive_notification(
            move |notif: SessionNotification, _cx: ConnectionTo<Agent>| {
                let sink = sink.clone();
                async move {
                    sink.lock()
                        .unwrap()
                        .push(serde_json::to_value(&notif).expect("serialize notification"));
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .connect_with(agent, |conn: ConnectionTo<Agent>| async move {
            let init = conn
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let session = conn
                .send_request(NewSessionRequest::new(std::path::PathBuf::from("/tmp")))
                .block_task()
                .await?;
            let prompt = conn
                .send_request(PromptRequest::new(
                    session.session_id.clone(),
                    vec![ContentBlock::Text(TextContent::new(
                        "list the files".to_string(),
                    ))],
                ))
                .block_task()
                .await?;
            Ok((
                serde_json::to_value(&init).expect("serialize init"),
                serde_json::to_value(prompt.stop_reason).expect("serialize stop reason"),
            ))
        })
        .await
        .expect("client run");

    // The negotiation and capabilities the adapter advertised.
    assert_eq!(init["protocolVersion"], json!(1));
    assert_eq!(init["agentCapabilities"]["loadSession"], json!(true));
    assert_eq!(
        init["agentCapabilities"]["promptCapabilities"]["embeddedContext"],
        json!(true)
    );
    assert_eq!(init["agentInfo"]["name"], json!("harness-standalone"));

    // The terminal stop reason of the turn.
    assert_eq!(stop_reason, json!("end_turn"));

    // The streamed updates, by kind.
    let updates = updates.lock().unwrap();
    let kinds: Vec<String> = updates
        .iter()
        .filter_map(|u| u["update"]["sessionUpdate"].as_str().map(String::from))
        .collect();
    assert!(
        kinds.iter().any(|k| k == "agent_message_chunk"),
        "expected an agent message; got {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "tool_call"),
        "expected a tool call; got {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "tool_call_update"),
        "expected a tool call update; got {kinds:?}"
    );
}
