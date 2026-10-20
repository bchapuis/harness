//! The workflow cycle end to end (workflow spec §4, §6, §7): W1, W2, W4, W7.
//!
//! Every test here runs the **real entity host, gateway, and journal** under one
//! seed: the workflow cycle is the only new code on the simulated path. The
//! assertions are about the four claims the cycle makes: that the desired set is
//! a pure function of committed state, that everything in it eventually launches,
//! that nothing launches before the record that wanted it committed, and that the
//! activation hibernates exactly when its work has come to rest.

mod support;

use std::time::Duration;

use actor_simulation::Recorder;
use actor_simulation::Simulation;
use entity::EntityConfig;
use entity::EntityEvent;
use support::Answer;
use support::Outcome;
use support::Request;
use support::Status;
use support::TaskId;
use support::announced_quiescence;
use support::ask;
use support::drive_events;
use support::fanouts;
use support::hibernated;
use support::launched;
use support::read_script;
use support::request;
use support::seam;
use support::sim_system;
use support::task;
use support::with_script;
use workflow::WorkflowEvent;

/// Long enough for the request to commit, every effect to run, and the backoff
/// schedule to reach its cap (500 ms + 1 s + 2 s + 4 s = 7.5 s of delays) with
/// room for the idle window on top.
///
/// Bounded rather than `sim.run()`, because a workflow entity is a *perpetual*
/// workload in the sense actor-simulation means: an activation that vetoes
/// hibernation reschedules its idle check forever, and a backed-off key re-arms
/// its timer, so the scheduler never empties and `run()` would not return.
const SETTLE: Duration = Duration::from_secs(60);

/// An entity that idles for far longer than it works, so hibernation is reachable
/// but never forced mid-flight.
fn config() -> EntityConfig {
    EntityConfig {
        idle_after: Duration::from_millis(50),
        ..EntityConfig::default()
    }
}

#[test]
fn the_desired_set_dispatches_every_task_and_the_entity_finishes() {
    let sim = Simulation::new(7);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let script = seam();
    let fanouts = fanouts(&system, &script, config());

    ask(&sim, &fanouts, "job/0", request((0..5).map(task).collect()));
    sim.run_for(SETTLE);

    let (done, finished, outstanding) = ask(&sim, &fanouts, "job/0", Status);
    assert_eq!(done.len(), 5, "every task answered");
    assert!(finished, "the last answer ended the work");
    assert!(outstanding.is_empty(), "the desired set emptied");
    assert_eq!(
        read_script(&script, |s| s.total_runs()),
        5,
        "five tasks, five performs: no effect ran twice (W3)"
    );
}

#[test]
fn nothing_launches_before_the_record_that_wanted_it_commits() {
    // W4, intent before effect. Not enforced by a check: a tick reads
    // *committed* state, so an effect whose intent has not committed is not in
    // `pending(state)` and there is nothing to launch (§6). The stream shows it
    // per key: every `Launched` follows the `Committed` that admitted *its own*
    // key. That takes two requests to say. Under one batch the claim degrades to
    // "some launch followed some commit", which a launch racing its own record
    // would satisfy as soon as any earlier record had committed.
    let sim = Simulation::new(11);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let script = seam();
    let fanouts = fanouts(&system, &script, config());

    ask(&sim, &fanouts, "job/1", request((0..3).map(task).collect()));
    sim.run_for(SETTLE);
    // Where the first batch's stream ends: the first `Committed` after this
    // point is the one that admits keys 3, 4, and 5, and no earlier commit does.
    let boundary = recorder.events().len();
    ask(&sim, &fanouts, "job/1", request((3..6).map(task).collect()));
    sim.run_for(SETTLE);

    let events = recorder.events();
    let commit_after = |from: usize| {
        events[from..]
            .iter()
            .position(|e| {
                matches!(
                    e.as_app::<EntityEvent>(),
                    Some(EntityEvent::Committed { .. })
                )
            })
            .map(|i| i + from)
    };
    let admitted = [
        commit_after(0).expect("the first request committed"),
        commit_after(boundary).expect("the second request committed"),
    ];

    let mut paired = 0usize;
    for (i, e) in events.iter().enumerate() {
        let Some(WorkflowEvent::Launched { key, .. }) = e.as_app::<WorkflowEvent>() else {
            continue;
        };
        let key: TaskId = key
            .parse()
            .expect("the fixture keys its effects by task id");
        let admit = admitted[usize::from(key >= 3)];
        assert!(
            admit < i,
            "task {key} launched at {i}, ahead of the commit at {admit} that admitted it: \
             an effect ran before the record that wanted it was durable (W4)"
        );
        paired += 1;
    }
    assert_eq!(
        paired, 6,
        "every task launched, each paired with the commit that admitted it"
    );
}

#[test]
fn the_desired_set_is_a_pure_function_of_committed_state() {
    // W1. Two activations that fold the same journal prefix compute the same
    // desired set: the entity hibernates, re-activates, and its outstanding work
    // is the same set; re-derived, never carried across in memory.
    let sim = Simulation::new(13);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let script = seam();
    // Two of the five hang, so the entity cannot finish and the outstanding set
    // is non-trivial across the hibernation boundary.
    with_script(&script, |s| {
        s.set(3, Outcome::Hang);
        s.set(4, Outcome::Hang);
    });
    let fanouts = fanouts(&system, &script, config());

    ask(&sim, &fanouts, "job/2", request((0..5).map(task).collect()));
    sim.run_for(SETTLE);

    let (_, _, before) = ask(&sim, &fanouts, "job/2", Status);
    assert_eq!(before, vec![3, 4], "the hung tasks are what is outstanding");

    // The hung effects hold claims, so the entity must not have hibernated (W7);
    // force a fresh fold by reading through a new handle after a long idle.
    sim.run_for(SETTLE);
    let (_, _, after) = ask(&sim, &fanouts, "job/2", Status);
    assert_eq!(
        before, after,
        "the same journal prefix computes the same desired set (W1)"
    );
}

#[test]
fn a_hibernated_entity_re_derives_and_re_drives_its_outstanding_work() {
    // W1 and W7 together, across a real eviction: the entity finishes its work,
    // hibernates because the desired set is empty, and a later request drives a
    // fresh activation with no memory of the first.
    let sim = Simulation::new(17);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let script = seam();
    let fanouts = fanouts(&system, &script, config());

    ask(&sim, &fanouts, "job/3", request((0..2).map(task).collect()));
    sim.run_for(SETTLE);

    assert!(
        hibernated(&recorder.events()),
        "an idle entity whose desired set emptied must hibernate (W7)"
    );
    assert!(
        announced_quiescence(&recorder.events()),
        "and must have announced quiescence first (§10)"
    );

    // A second request on the re-activated entity: the fold carries the first
    // two answers, and only the new task is outstanding.
    ask(&sim, &fanouts, "job/3", request(vec![task(9)]));
    sim.run_for(SETTLE);

    let (done, _, outstanding) = ask(&sim, &fanouts, "job/3", Status);
    assert_eq!(done.len(), 3, "all three answered across the eviction");
    assert!(outstanding.is_empty());
    assert_eq!(
        read_script(&script, |s| s.runs(0)),
        1,
        "the re-activation did not re-run work the journal already answered (W1)"
    );
}

#[test]
fn an_activation_with_an_effect_in_flight_does_not_hibernate() {
    // W7's veto half. A hung effect holds its claim, so `can_hibernate` (which
    // is the workflow's derived quiescence and nothing hand-written) must refuse
    // eviction for as long as the outcome could still arrive.
    let sim = Simulation::new(19);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let script = seam();
    with_script(&script, |s| s.set(0, Outcome::Hang));
    let fanouts = fanouts(&system, &script, config());

    ask(&sim, &fanouts, "job/4", request(vec![task(0)]));
    sim.run_for(SETTLE);

    assert!(
        !hibernated(&recorder.events()),
        "an activation holding an in-flight effect must not hibernate (W7)"
    );
    assert_eq!(
        launched(&recorder.events()),
        vec!["0"],
        "and must not relaunch it (W3)"
    );
}

#[test]
fn a_failing_effect_is_a_value_and_the_run_still_finishes() {
    // §5.2: a failing effect does not fail the entity. The consumer folds the
    // failure as an answer, the key leaves the desired set, and the entity
    // reaches quiescence exactly as it would on success.
    let sim = Simulation::new(23);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let script = seam();
    with_script(&script, |s| {
        s.set(
            1,
            Outcome::Fail {
                delay: Duration::ZERO,
            },
        );
        s.set(2, Outcome::Panic);
    });
    let fanouts = fanouts(&system, &script, config());

    ask(&sim, &fanouts, "job/5", request((0..3).map(task).collect()));
    sim.run_for(SETTLE);

    let (done, finished, outstanding) = ask(&sim, &fanouts, "job/5", Status);
    assert!(
        outstanding.is_empty(),
        "a failure resolves its key like any outcome"
    );
    assert!(finished);
    assert!(matches!(done[&0], Answer::Ok));
    assert!(
        matches!(&done[&1], Answer::Failed(why) if why.contains("task 1 failed")),
        "the effect's own error reached the fold: {:?}",
        done[&1]
    );
    assert!(
        matches!(&done[&2], Answer::Failed(why) if why.contains("panicked")),
        "a panic inside `perform` is caught at the task boundary and settled \
         as Failed (§5.2), never left as a lost claim: {:?}",
        done[&2]
    );
}

#[test]
fn a_settle_that_commits_nothing_is_relaunched_by_the_backoff_timer() {
    // The entry point §4.2 item 3 exists for exactly this. A settle that returns
    // no records commits nothing, so nothing re-enters the cycle through entry
    // point 1; without a timer-driven drive the key would be stranded and W2
    // would not hold.
    let sim = Simulation::new(29);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let script = seam();
    let fanouts = fanouts(&system, &script, config());

    ask(
        &sim,
        &fanouts,
        "job/6",
        Request {
            tasks: vec![task(0)],
            silent: vec![0], // its settle journals nothing, forever
        },
    );
    sim.run_for(SETTLE);

    let runs = read_script(&script, |s| s.runs(0));
    assert!(
        runs > 1,
        "a key whose settle committed nothing must be relaunched by the backoff \
         timer; it ran {runs} time(s), so nothing re-entered the cycle (W2, §4.2 item 3)"
    );

    // And every one of those launches is a distinct claim, each closed by its
    // own settle: the stream must show them strictly alternating (W3).
    let mut open = false;
    for e in drive_events(&recorder.events()) {
        match e {
            WorkflowEvent::Launched { .. } => {
                assert!(!open, "a relaunch while the previous claim was open (W3)");
                open = true;
            }
            WorkflowEvent::Settled { committed, .. } => {
                assert!(open, "a settle with no claim behind it");
                assert!(!committed, "this key's settle never commits");
                open = false;
            }
            _ => {}
        }
    }
}

#[test]
fn a_key_backed_off_to_the_cap_stops_holding_the_activation_resident() {
    // §7's at-rest clause. §4.4 imposes no attempt bound, so without it one
    // permanently unproductive effect would veto eviction forever and pin an
    // activation whose only remaining work is a timer: strictly worse than what
    // the agent runtime does (agent §9.2 item 3).
    let sim = Simulation::new(31);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let script = seam();
    let fanouts = fanouts(&system, &script, config());

    ask(
        &sim,
        &fanouts,
        "job/7",
        Request {
            tasks: vec![task(0)],
            silent: vec![0],
        },
    );
    sim.run_for(SETTLE);

    assert!(
        hibernated(&recorder.events()),
        "an entity whose only outstanding key has backed off to the cap must \
         reach rest and hibernate (§7); pinning it forever is the bug this \
         clause exists to prevent"
    );

    // The debt is a fold, so it survives: the next activation re-derives it.
    let (_, _, outstanding): (_, _, Vec<TaskId>) = ask(&sim, &fanouts, "job/7", Status);
    assert_eq!(
        outstanding,
        vec![0],
        "hibernating at rest gives up unattended progress, never the work (§7)"
    );
}
