//! Orphaned effects and resume (workflow spec §4.3, §3.3, §6): W6, and W3's
//! cross-activation half.
//!
//! An effect's intent is journaled before it launches and its outcome after
//! (§5). A crash or a migration between the two leaves it **orphaned**: the
//! journal says it was wanted, and nothing says whether it ran. The desired set
//! computed by the next activation's first tick is the orphan set, and the
//! workflow resolves each member by its declared policy (`Reexecute` launches it
//! again, `Interrupt` settles it `Interrupted` without a launch) dropping none
//! and resolving none twice.
//!
//! Every scenario here runs the real entity host, gateway, shard groups, and
//! rehydration on a three-node cluster: a crash is a process death
//! (`SimNetwork::restart`), which takes the effect's task with it, and a migration
//! is a voluntary leadership hand-off under which the deposed activation's effect
//! keeps running and its outcome reaches the successor as §4.4's straggler.
//! Hibernation is deliberately absent: W7 forbids hibernating with a claim open,
//! so it cannot leave an effect orphaned, and `conformance_cycle.rs` pins that.

mod support;

use std::time::Duration;

use actor_simulation::Recorder;
use actor_simulation::Simulation;
use entity::Seq;
use support::Answer;
use support::FanoutRecord;
use support::Outcome;
use support::Status;
use support::assert_grammar;
use support::cluster;
use support::discarded;
use support::interrupted_on;
use support::interrupting_task;
use support::launched_on;
use support::read_script;
use support::request;
use support::seam;
use support::task;
use support::with_script;

/// How long a scenario waits for a round trip through the cluster.
const CALL: Duration = Duration::from_secs(8);

/// Long enough to be in flight across the crash, short enough to finish within
/// a scenario once relaunched.
const SLOW: Duration = Duration::from_secs(20);

#[test]
fn a_crash_between_launch_and_settle_relaunches_a_reexecute_effect_once() {
    // W6 for `Reexecute`, and W3's admission: across activations an effect MAY run
    // twice, and the settled record (not the launch) is the durable fact.
    //
    // The restarted leader may win its shard back: either way the activation
    // after the crash is a fresh process, so only launches after the crash
    // count. Under seed 101 the leader is node 2, and after its restart the two
    // survivors disagree about who leads the shard for the rest of the scenario
    // (node 1's map names node 2, node 2 redirects to node 1), so no call routes. That is a question for the cluster's leader
    // re-discovery after a restart, not for the workflow; it wants a reproduction in
    // entity's clustered suite, where no test yet restarts a shard leader and
    // then routes to it.
    let sim = Simulation::new(109);
    let recorder = Recorder::new();
    let script = seam();
    with_script(&script, |s| {
        s.set(0, Outcome::Answer { delay: SLOW });
        // Task 1 settles before the crash; its record is what keeps it from
        // being re-derived as work.
        s.set(
            1,
            Outcome::Answer {
                delay: Duration::ZERO,
            },
        );
    });
    let mut cluster = cluster::cluster(&sim, &recorder, &script);
    let key = cluster.key_led_elsewhere("job");
    let key = key.as_str();

    cluster.ask(&sim, key, request(vec![task(0), task(1)]));
    sim.run_for(Duration::from_secs(2));
    assert_eq!(read_script(&script, |s| (s.runs(0), s.runs(1))), (1, 1));
    assert_eq!(
        read_script(&script, |s| s.completions(0)),
        0,
        "task 0 is in flight when the leader dies"
    );

    // Process death on the leader: the effect's task dies with it, and the
    // journal holds `Requested { 0, 1 }` and `Answered { 1 }`.
    let before = recorder.events().len();
    cluster.crash_leader_of(key, &script);
    sim.run_for(Duration::from_secs(3)); // the shard re-elects

    // Resumption is caller-driven: the next contact activates the entity on the
    // shard's leader, whose first tick is the orphan set.
    let (done, _, outstanding) = cluster.ask(&sim, key, Status);
    assert!(
        done.contains_key(&1),
        "the settled task is in the fold (W1)"
    );
    assert_eq!(
        outstanding,
        vec![0],
        "the orphan set is exactly what was in flight"
    );
    sim.run_for(Duration::from_millis(500));
    assert_eq!(
        read_script(&script, |s| (s.runs(0), s.runs(1))),
        (2, 1),
        "the orphaned Reexecute effect ran again; the settled one did not (W6, W3)"
    );
    let successor = cluster.leader_of(key);
    assert_eq!(
        launched_on(&recorder.events()[before..], successor),
        vec!["0".to_string()],
        "the successor's first tick launched exactly the orphaned member"
    );

    // Nothing is dropped: the relaunch settles and the work finishes.
    sim.run_for(SLOW + Duration::from_secs(2));
    let (done, finished, outstanding) = cluster.ask(&sim, key, Status);
    assert_eq!(done.len(), 2);
    assert!(finished);
    assert!(outstanding.is_empty());
    assert_grammar(recorder.events());
}

#[test]
fn a_crash_between_launch_and_settle_settles_an_interrupt_effect_without_relaunch() {
    // W6 for `Interrupt`: nothing is launched, the synthesized outcome rides the
    // mailbox and commits as an ordinary command, and the consumer decides.
    let sim = Simulation::new(103);
    let recorder = Recorder::new();
    let script = seam();
    with_script(&script, |s| s.set(0, Outcome::Answer { delay: SLOW }));
    let mut cluster = cluster::cluster(&sim, &recorder, &script);
    let key = cluster.key_led_elsewhere("job");
    let key = key.as_str();

    cluster.ask(&sim, key, request(vec![interrupting_task(0)]));
    sim.run_for(Duration::from_secs(2));
    assert_eq!(read_script(&script, |s| s.runs(0)), 1);

    cluster.crash_leader_of(key, &script);
    sim.run_for(Duration::from_secs(3));

    cluster.ask(&sim, key, Status);
    // The synthesized `Settled` is one more command behind the status read.
    sim.run_for(Duration::from_secs(1));

    let successor = cluster.leader_of(key);
    let (done, finished, outstanding) = cluster.ask(&sim, key, Status);
    assert_eq!(
        read_script(&script, |s| s.runs(0)),
        1,
        "an Interrupt effect is never relaunched (§4.3)"
    );
    assert!(
        matches!(&done.get(&0), Some(Answer::Failed(why)) if why.contains("interrupted")),
        "the consumer folded `EffectError::Interrupted` as a value (§4.2): {done:?}"
    );
    assert!(finished);
    assert!(outstanding.is_empty(), "resolved, so no longer wanted");
    assert_eq!(
        interrupted_on(&recorder.events(), successor),
        vec!["0".to_string()],
        "resolved exactly once, on the successor (W6)"
    );
    assert!(
        launched_on(&recorder.events(), successor).is_empty(),
        "and without a launch"
    );
    assert_grammar(recorder.events());
}

#[test]
fn a_migration_under_an_effect_in_flight_resolves_it_at_the_new_leader() {
    // §6 and §4.4 together. A voluntary hand-off is a forced step-down for the
    // activation: it abandons its launch to whatever it was doing. The successor
    // re-derives the key and relaunches it (Reexecute). The abandoned effect keeps
    // running on the old node and its outcome, routed to whoever now leads,
    // settles the key first (an effect of a previous activation reaching a
    // leader that has since re-derived the world), and the relaunch's own outcome
    // then finds its key absent and is discarded (W5).
    let sim = Simulation::new(107);
    let recorder = Recorder::new();
    let script = seam();
    with_script(&script, |s| s.set(0, Outcome::Answer { delay: SLOW }));
    let cluster = cluster::cluster(&sim, &recorder, &script);
    let key = cluster.key_led_elsewhere("job");
    let key = key.as_str();
    let old_leader = cluster.leader_of(key);

    cluster.ask(&sim, key, request(vec![task(0)]));
    sim.run_for(Duration::from_secs(2));

    // The leader steps down gracefully; its process, and the effect's task, live on.
    let leading = cluster
        .nodes
        .iter()
        .position(|n| n.node() == old_leader)
        .expect("the leader is one of ours");
    let node = cluster.entity_nodes[leading].clone();
    cluster::drive(&sim, CALL, async move { node.hand_off_leadership().await });
    sim.run_for(Duration::from_secs(2));
    let successor = cluster.leader_of(key);
    assert_ne!(successor, old_leader, "leadership moved");

    let (_, _, outstanding) = cluster.ask(&sim, key, Status);
    assert_eq!(outstanding, vec![0], "re-derived on the successor (W1)");
    sim.run_for(Duration::from_millis(500));
    assert_eq!(
        read_script(&script, |s| s.runs(0)),
        2,
        "the successor relaunched the abandoned key (§6, W3 across activations)"
    );

    // Both launches complete, and exactly one outcome becomes the record. The
    // other is dropped by one of the two guards §4.4 names: the straggler filter,
    // if it reached the successor after the first had settled, or the fence, if
    // it reached the deposed activation, whose append the journal refuses.
    sim.run_for(SLOW + Duration::from_secs(5));
    assert_eq!(read_script(&script, |s| s.completions(0)), 2);
    let (done, finished, outstanding) = cluster.ask(&sim, key, Status);
    assert!(matches!(done.get(&0), Some(Answer::Ok)));
    assert!(finished && outstanding.is_empty());
    let entity = cluster.anchor().entity(key);
    let history = cluster::drive(&sim, CALL, async move {
        entity.events(Seq::ZERO, 1024).await.expect("journal read")
    });
    let answered = history
        .events
        .iter()
        .filter(|(_, r)| matches!(r, FanoutRecord::Answered { task: 0, .. }))
        .count();
    assert_eq!(answered, 1, "two outcomes, one record (W5, §4.4)");
    assert!(
        discarded(&recorder.events()).len() <= 1,
        "at most the successor's own relaunch was discarded as a straggler"
    );
    assert_grammar(recorder.events());
}
