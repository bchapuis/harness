//! A minimal end-to-end smoke test: a prompt drives one tool step and then a
//! final message, the loop running as a granary grain.

mod support;

use std::sync::Arc;

use actor_core::LocalSystemBuilder;
use actor_simulation::Simulation;
use harness::Harness;
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
    let session = harness.session("researcher", SessionId::new("s1")).unwrap();

    let out = sim.block_on(async move { session.prompt(Turn::new(TurnId::new("t1"), "go")).await });
    match out {
        Ok(Ok(completion)) => assert_eq!(completion.text(), "done"),
        other => panic!("expected a completion, got {other:?}"),
    }
    // The one sandboxed tool call ran in the session's sandbox.
    assert_eq!(sandboxes.stats.calls().len(), 1);
    assert_eq!(sandboxes.stats.opened(), 1);
}

/// §7.4: a kind outside this node's directory is an error value — kind names
/// arrive from outside (a gateway's URL path), so the miss must not panic.
#[test]
fn addressing_an_unknown_kind_is_an_error_value() {
    let sim = Simulation::new(1);
    let kinds = Kinds::new().register("researcher", Kind::new("You are a researcher."));
    let harness = harness_on(
        &sim,
        kinds,
        Arc::new(ScriptedModel::steps(vec![])),
        Arc::new(ScriptedSandboxes::echo()),
    );
    let err = match harness.session("reseacher", SessionId::new("s1")) {
        Err(e) => e,
        Ok(_) => panic!("a kind outside the directory must not resolve"),
    };
    assert!(err.to_string().contains("reseacher"), "{err}");
}

/// §7.1: the directory must close over delegation — a hosted kind whose
/// allowlisted child this node neither hosts nor routes fails at build, not
/// per-call at launch (§8.1) or silently in cancel propagation (§9.2).
#[test]
#[should_panic(expected = "neither hosts nor routes")]
fn building_a_node_whose_directory_misses_a_delegate_panics() {
    let sim = Simulation::new(1);
    let kinds = Kinds::new()
        .register(
            "parent",
            Kind::new("You delegate.").delegates_to(&["worker"]),
        )
        .register("worker", Kind::new("You work."));
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let _ = Harness::builder(system, &kinds)
        .host(
            "parent",
            Arc::new(ScriptedModel::steps(vec![])),
            Arc::new(ScriptedSandboxes::echo()),
        )
        .build();
}
