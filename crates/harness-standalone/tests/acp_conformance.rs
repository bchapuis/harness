//! Wire-level conformance tests for the ACP adapter.
//!
//! Each test drives `acp::serve` directly over an in-process duplex pipe — no
//! editor, no subprocess — against a [`FakeNode`] that scripts the control-port
//! replies. We assert on the exact JSON-RPC the adapter emits, which is the part
//! the strongly-typed SDK test (`acp_sdk.rs`) exercises end-to-end but cannot
//! pin field-by-field.

mod common;

use common::*;
use harness_standalone::acp;
use serde_json::Value;
use serde_json::json;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::DuplexStream;
use tokio::io::Lines;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;

/// A minimal JSON-RPC client over the adapter's pipe.
struct Client {
    writer: WriteHalf<DuplexStream>,
    lines: Lines<BufReader<ReadHalf<DuplexStream>>>,
}

impl Client {
    fn new(read: ReadHalf<DuplexStream>, write: WriteHalf<DuplexStream>) -> Client {
        Client {
            writer: write,
            lines: BufReader::new(read).lines(),
        }
    }

    async fn send(&mut self, frame: Value) {
        let mut line = serde_json::to_vec(&frame).expect("encode");
        line.push(b'\n');
        self.writer.write_all(&line).await.expect("write");
    }

    /// Send a raw line (for malformed-input tests).
    async fn send_raw(&mut self, line: &str) {
        self.writer
            .write_all(format!("{line}\n").as_bytes())
            .await
            .expect("write");
    }

    async fn recv(&mut self) -> Value {
        let line = self.lines.next_line().await.expect("read").expect("a frame");
        serde_json::from_str(&line).expect("json frame")
    }

    /// Issue a request, returning its response plus every notification that
    /// arrived before the response (e.g. the `session/update`s of a prompt).
    async fn request(&mut self, id: u64, method: &str, params: Value) -> (Value, Vec<Value>) {
        self.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await;
        let mut notifs = Vec::new();
        loop {
            let frame = self.recv().await;
            if frame.get("id") == Some(&json!(id)) {
                return (frame, notifs);
            }
            notifs.push(frame);
        }
    }

    async fn notify(&mut self, method: &str, params: Value) {
        self.send(json!({ "jsonrpc": "2.0", "method": method, "params": params }))
            .await;
    }
}

/// Stand up a fake node and an adapter wired to it over a pipe; return the
/// client and keep the node alive.
async fn connect(scenario: Scenario) -> (Client, FakeNode) {
    let node = FakeNode::start(scenario).await;
    let (adapter_end, client_end) = tokio::io::duplex(1 << 16);
    let (a_read, a_write) = tokio::io::split(adapter_end);
    let (c_read, c_write) = tokio::io::split(client_end);
    let addr = node.addr();
    tokio::spawn(async move {
        let _ = acp::serve(a_read, a_write, &addr).await;
    });
    (Client::new(c_read, c_write), node)
}

fn text_prompt(sid: &str, text: &str) -> Value {
    json!({ "sessionId": sid, "prompt": [ { "type": "text", "text": text } ] })
}

/// The `update` payloads of the `session/update` notifications for `sid`.
fn updates_for(notifs: &[Value], sid: &str) -> Vec<Value> {
    notifs
        .iter()
        .filter(|n| n.get("method") == Some(&json!("session/update")))
        .filter(|n| n["params"]["sessionId"] == json!(sid))
        .map(|n| n["params"]["update"].clone())
        .collect()
}

#[tokio::test]
async fn initialize_negotiates_version_and_advertises_capabilities() {
    let (mut client, _node) = connect(Scenario::new()).await;

    let (resp, _) = client
        .request(1, "initialize", json!({ "protocolVersion": 1 }))
        .await;
    let result = &resp["result"];
    assert_eq!(result["protocolVersion"], json!(1));
    assert_eq!(result["agentCapabilities"]["loadSession"], json!(true));
    assert_eq!(
        result["agentCapabilities"]["promptCapabilities"]["embeddedContext"],
        json!(true)
    );
    // We do not handle image/audio, and we must say so.
    assert_eq!(
        result["agentCapabilities"]["promptCapabilities"]["image"],
        json!(false)
    );
    assert_eq!(result["agentInfo"]["name"], json!("harness-standalone"));
    assert!(result["authMethods"].is_array());
}

#[tokio::test]
async fn initialize_clamps_a_newer_client_to_our_version() {
    let (mut client, _node) = connect(Scenario::new()).await;
    // A client advertising a future version is met at the highest we speak.
    let (resp, _) = client
        .request(1, "initialize", json!({ "protocolVersion": 99 }))
        .await;
    assert_eq!(resp["result"]["protocolVersion"], json!(1));
}

#[tokio::test]
async fn session_new_mints_an_opaque_id() {
    let (mut client, _node) = connect(Scenario::new()).await;
    let (resp, _) = client.request(1, "session/new", json!({ "cwd": "/tmp", "mcpServers": [] })).await;
    let sid = resp["result"]["sessionId"].as_str().expect("sessionId");
    assert!(sid.starts_with("assistant/acp-"), "got {sid}");
}

#[tokio::test]
async fn prompt_streams_updates_and_returns_end_turn() {
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
    let (mut client, _node) = connect(scenario).await;

    let sid = "assistant/demo";
    let (resp, notifs) = client
        .request(2, "session/prompt", text_prompt(sid, "list the files"))
        .await;
    assert_eq!(resp["result"]["stopReason"], json!("end_turn"));

    let updates = updates_for(&notifs, sid);
    let kinds: Vec<&str> = updates
        .iter()
        .filter_map(|u| u["sessionUpdate"].as_str())
        .collect();
    assert!(kinds.contains(&"user_message_chunk"), "kinds={kinds:?}");
    assert!(kinds.contains(&"agent_message_chunk"));
    assert!(kinds.contains(&"tool_call"));
    assert!(kinds.contains(&"tool_call_update"));

    // The agent message carries the model text as a text content block.
    let agent = updates
        .iter()
        .find(|u| u["sessionUpdate"] == json!("agent_message_chunk"))
        .unwrap();
    assert_eq!(agent["content"]["type"], json!("text"));
    assert_eq!(agent["content"]["text"], json!("Listing now."));

    // The tool call is an `execute` kind, opens pending, and carries its input.
    let call = updates
        .iter()
        .find(|u| u["sessionUpdate"] == json!("tool_call"))
        .unwrap();
    assert_eq!(call["toolCallId"], json!("call-1"));
    assert_eq!(call["kind"], json!("execute"));
    assert_eq!(call["status"], json!("pending"));
    assert_eq!(call["rawInput"]["command"], json!("ls"));

    // Its outcome completes the call and carries the raw output.
    let update = updates
        .iter()
        .find(|u| u["sessionUpdate"] == json!("tool_call_update"))
        .unwrap();
    assert_eq!(update["toolCallId"], json!("call-1"));
    assert_eq!(update["status"], json!("completed"));
    assert_eq!(update["rawOutput"]["exit_code"], json!(0));
    // Content is a ToolCallContent `content` block wrapping a text block.
    assert_eq!(update["content"][0]["type"], json!("content"));
    assert_eq!(update["content"][0]["content"]["type"], json!("text"));
}

#[tokio::test]
async fn a_failed_tool_call_reports_failed_status_without_raw_output() {
    let scenario = Scenario::new().turn(
        vec![
            turn_submitted("t-1", "break it"),
            model_tool_call("t-1", "call-1", "shell", json!({ "command": "nope" })),
            tool_err("t-1", "call-1", "command not found"),
            run_ended_ok("t-1", "It failed."),
        ],
        completion("It failed."),
    );
    let (mut client, _node) = connect(scenario).await;

    let sid = "assistant/demo";
    let (_resp, notifs) = client
        .request(2, "session/prompt", text_prompt(sid, "break it"))
        .await;
    let updates = updates_for(&notifs, sid);
    let update = updates
        .iter()
        .find(|u| u["sessionUpdate"] == json!("tool_call_update"))
        .unwrap();
    assert_eq!(update["status"], json!("failed"));
    assert!(update.get("rawOutput").is_none());
    // The failure text is the ToolError's Display rendering.
    let text = update["content"][0]["content"]["text"].as_str().unwrap();
    assert!(text.contains("command not found"), "got {text}");
}

#[tokio::test]
async fn load_replays_the_journal_as_updates() {
    let scenario = Scenario::new().history(vec![
        turn_submitted("t-1", "earlier question"),
        model_text("t-1", "earlier answer"),
        run_ended_ok("t-1", "earlier answer"),
    ]);
    let (mut client, _node) = connect(scenario).await;

    let sid = "assistant/demo";
    let (resp, notifs) = client
        .request(3, "session/load", json!({ "sessionId": sid, "cwd": "/tmp", "mcpServers": [] }))
        .await;
    assert!(resp["result"].is_object(), "load returns an empty result object");

    let updates = updates_for(&notifs, sid);
    // The user turn and the agent answer are replayed; RunEnded carries nothing.
    let user = updates
        .iter()
        .find(|u| u["sessionUpdate"] == json!("user_message_chunk"))
        .unwrap();
    assert_eq!(user["content"]["text"], json!("earlier question"));
    let agent = updates
        .iter()
        .find(|u| u["sessionUpdate"] == json!("agent_message_chunk"))
        .unwrap();
    assert_eq!(agent["content"]["text"], json!("earlier answer"));
}

#[tokio::test]
async fn cancel_resolves_the_turn_as_cancelled() {
    let scenario = Scenario::new().cancellable(vec![turn_submitted("t-1", "long task")]);
    let (mut client, _node) = connect(scenario).await;

    let sid = "assistant/demo";
    // Park the prompt: send the request, then read its first streamed update.
    client
        .send(json!({
            "jsonrpc": "2.0", "id": 4, "method": "session/prompt",
            "params": text_prompt(sid, "long task"),
        }))
        .await;
    let first = client.recv().await;
    assert_eq!(first["method"], json!("session/update"));
    assert_eq!(first["params"]["update"]["sessionUpdate"], json!("user_message_chunk"));

    // Cancel the session; the parked prompt must resolve as cancelled.
    client.notify("session/cancel", json!({ "sessionId": sid })).await;
    loop {
        let frame = client.recv().await;
        if frame.get("id") == Some(&json!(4)) {
            assert_eq!(frame["result"]["stopReason"], json!("cancelled"));
            break;
        }
    }
}

#[tokio::test]
async fn prompt_without_a_session_id_is_invalid_params() {
    let (mut client, _node) = connect(Scenario::new()).await;
    let (resp, _) = client
        .request(5, "session/prompt", json!({ "prompt": [] }))
        .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
}

#[tokio::test]
async fn an_unknown_method_is_method_not_found() {
    let (mut client, _node) = connect(Scenario::new()).await;
    let (resp, _) = client.request(6, "nonsense/method", json!({})).await;
    assert_eq!(resp["error"]["code"], json!(-32601));
}

#[tokio::test]
async fn malformed_input_is_a_parse_error_with_a_null_id() {
    let (mut client, _node) = connect(Scenario::new()).await;
    client.send_raw("{ this is not json").await;
    let frame = client.recv().await;
    assert_eq!(frame["error"]["code"], json!(-32700));
    assert_eq!(frame["id"], Value::Null);
}
