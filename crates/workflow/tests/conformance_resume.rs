//! Differential resume (workflow spec §3.1, §4.3): W1 and W6, decided by comparing
//! independent computations of the same desired set.
//!
//! The workflow's central claim is that resume is not a special case: the desired
//! set an activation's first tick computes after a crash is the same function
//! its predecessor was computing on the live path, over the same committed
//! prefix. Three views of one entity are held equal here: the live activation
//! before the crash, the successor after it, and an **offline fold** of the
//! journal the test reads back and folds itself with the consumer's own `apply`
//! and `pending`. Any ephemeral state leaking into `pending` (a claim, a backoff
//! count, a binding) would show as a disagreement.

mod support;

use std::time::Duration;

use actor_simulation::Recorder;
use actor_simulation::SimNode;
use actor_simulation::Simulation;
use entity::Entities;
use entity::Entity;
use entity::Seq;
use support::Fanout;
use support::FanoutState;
use support::Outcome;
use support::Status;
use support::TaskId;
use support::cluster;
use support::launched_on;
use support::read_script;
use support::request;
use support::seam;
use support::task;
use support::with_script;
use workflow::Workflow;

const CALL: Duration = Duration::from_secs(8);
const SLOW: Duration = Duration::from_secs(30);

/// The desired set an offline fold of the entity's journal computes: `apply` over
/// every committed record, then the consumer's `pending`. Reads the journal
/// through the public read path (entity §6), so it sees exactly the committed
/// prefix and nothing an activation held in memory.
fn folded_outstanding(
    sim: &Simulation,
    entities: &Entities<Fanout<SimNode>>,
    key: &str,
) -> Vec<TaskId> {
    let entity = entities.entity(key);
    let history = cluster::drive(sim, CALL, async move {
        entity.events(Seq::ZERO, 1024).await.expect("journal read")
    });
    let mut state = FanoutState::default();
    for (_, record) in &history.events {
        Fanout::<SimNode>::apply(&mut state, record);
    }
    // `pending` is a method on the behavior, whose seam plays no part in it (§3.1).
    let behavior = Fanout::<SimNode>::new(seam());
    behavior
        .pending(&state)
        .into_iter()
        .map(|p| p.key)
        .collect()
}

#[test]
fn the_successors_first_drive_is_the_desired_set_its_predecessor_was_computing() {
    // W1 and W6 as one differential: live-before == offline fold == live-after ==
    // what the successor's first tick launched.
    let sim = Simulation::new(211);
    let recorder = Recorder::new();
    let script = seam();
    with_script(&script, |s| {
        // Three finish before the crash, three are in flight across it.
        for k in 0..3 {
            s.set(
                k,
                Outcome::Answer {
                    delay: Duration::from_millis(100),
                },
            );
        }
        for k in 3..6 {
            s.set(k, Outcome::Answer { delay: SLOW });
        }
    });
    let mut cluster = cluster::cluster(&sim, &recorder, &script);
    let key = cluster.key_led_elsewhere("job");
    let key = key.as_str();

    cluster.ask(&sim, key, request((0..6).map(task).collect()));
    sim.run_for(Duration::from_secs(2));

    let (_, _, live_before) = cluster.ask(&sim, key, Status);
    assert_eq!(live_before, vec![3, 4, 5], "the slow half is outstanding");
    let folded_before = folded_outstanding(&sim, cluster.anchor(), key);
    assert_eq!(
        folded_before, live_before,
        "the live activation's desired set is a function of the journal alone (W1)"
    );

    cluster.crash_leader_of(key, &script);
    sim.run_for(Duration::from_secs(3));

    let (done, _, live_after) = cluster.ask(&sim, key, Status);
    assert_eq!(
        live_after, live_before,
        "two activations folding the same prefix compute the same desired set (W1)"
    );
    assert_eq!(done.len(), 3, "and the same answers");
    let folded_after = folded_outstanding(&sim, cluster.anchor(), key);
    assert_eq!(folded_after, live_after);

    sim.run_for(Duration::from_millis(500));
    let successor = cluster.leader_of(key);
    let mut relaunched = launched_on(&recorder.events(), successor);
    relaunched.sort();
    assert_eq!(
        relaunched,
        vec!["3", "4", "5"],
        "the first tick's desired set is the orphan set, launched in full and \
         nothing else (W6)"
    );
    assert_eq!(
        read_script(&script, |s| (0..3).map(|k| s.runs(k)).collect::<Vec<_>>()),
        vec![1, 1, 1],
        "settled work is not re-derived as work (W1)"
    );
}

#[test]
fn a_second_crash_re_derives_what_the_first_resume_had_already_settled() {
    // Resume composes: the successor's own settles join the journal, and a
    // further crash re-derives from that longer prefix; no memory of either
    // activation carries across.
    let sim = Simulation::new(223);
    let recorder = Recorder::new();
    let script = seam();
    with_script(&script, |s| {
        s.set(
            0,
            Outcome::Answer {
                delay: Duration::from_secs(6),
            },
        );
        s.set(1, Outcome::Answer { delay: SLOW });
    });
    let mut cluster = cluster::cluster(&sim, &recorder, &script);
    let key = cluster.key_led_elsewhere("job");
    let key = key.as_str();

    cluster.ask(&sim, key, request(vec![task(0), task(1)]));
    sim.run_for(Duration::from_secs(1));

    // First crash: both in flight. The successor relaunches both; task 0 lands.
    cluster.crash_leader_of(key, &script);
    sim.run_for(Duration::from_secs(3));
    cluster.ask(&sim, key, Status);
    sim.run_for(Duration::from_secs(8));
    let (done, _, outstanding) = cluster.ask(&sim, key, Status);
    assert!(done.contains_key(&0));
    assert_eq!(outstanding, vec![1]);

    // Second crash: only task 1 is orphaned now, because task 0's record is in
    // the journal the third activation folds.
    cluster.crash_leader_of(key, &script);
    sim.run_for(Duration::from_secs(3));
    let (done, _, outstanding) = cluster.ask(&sim, key, Status);
    assert!(done.contains_key(&0));
    assert_eq!(outstanding, vec![1]);
    assert_eq!(folded_outstanding(&sim, cluster.anchor(), key), vec![1]);
    sim.run_for(Duration::from_millis(500));
    assert_eq!(
        read_script(&script, |s| (s.runs(0), s.runs(1))),
        (2, 3),
        "task 0 ran on the first two activations only; task 1 on all three (W1, W6)"
    );
}
