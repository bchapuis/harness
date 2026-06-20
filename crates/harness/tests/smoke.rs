//! A minimal end-to-end smoke test: a prompt drives one tool step and then a
//! final message, the loop running as a granary grain.

mod support;

use std::sync::Arc;

use actor_simulation::Simulation;
use harness::Kind;
use harness::Kinds;
use harness::SandboxProfile;
use harness::SessionId;
use harness::Tier;
use harness::Turn;
use harness::TurnId;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::final_message;
use support::harness_on;
use support::tool_call;

#[test]
fn prompt_completes_through_a_tool_step() {
    let sim = Simulation::new(1);
    let model = Arc::new(ScriptedModel::steps(vec![
        Ok(tool_call("c1", "shell", serde_json::json!({ "cmd": "ls" }))),
        Ok(final_message("done")),
    ]));
    let sandboxes = Arc::new(ScriptedSandboxes::echo());
    let schema = serde_json::json!({ "type": "object" });
    let kinds = Kinds::new().register(
        "researcher",
        Kind::new("You are a researcher.")
            .sandboxed("shell", "run a command", &schema, Tier::Native)
            .sandbox(SandboxProfile::image("base")),
    );
    let harness = harness_on(&sim, kinds, model, sandboxes.clone());
    let session = harness.session("researcher", SessionId::new("s1"));

    let out = sim.block_on(async move { session.prompt(Turn::new(TurnId::new("t1"), "go")).await });
    match out {
        Ok(Ok(completion)) => assert_eq!(completion.text(), "done"),
        other => panic!("expected a completion, got {other:?}"),
    }
    // The one sandboxed tool call ran in the session's sandbox.
    assert_eq!(sandboxes.stats.calls().len(), 1);
    assert_eq!(sandboxes.stats.opened(), 1);
}
