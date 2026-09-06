//! Straggler rejection end to end (drive spec §4.5, §3.1): D5.
//!
//! An outcome MAY arrive after its key has left the desired set — a timeout
//! racing the real result, a retracted effect completing anyway, an effect of a
//! previous activation reaching a leader that has since re-derived the world.
//! The drive discards such a `Settled` without calling `settle`, and writes no
//! record for it.
//!
//! The suite has two halves, and the second is the more interesting. The first
//! shows the filter working: a key retracted mid-flight has its outcome dropped.
//! The second shows what the filter rests on. Its only test is **presence in the
//! set**, not identity, so §3.1's rule that a key is unique over the grain's
//! whole journal is not decoration: with a key reused across occurrences the
//! drive settles the *first* occurrence's outcome as the second's, and no
//! machinery here can tell. That test exists so the obligation is pinned by a
//! run rather than by a sentence.

mod support;

use std::time::Duration;

use actor_simulation::Recorder;
use actor_simulation::Simulation;
use granary::GranaryConfig;
use support::Outcome;
use support::Retract;
use support::Status;
use support::TaskId;
use support::announced_quiescence;
use support::ask;
use support::discarded;
use support::fanouts;
use support::read_script;
use support::request;
use support::seam;
use support::settled;
use support::sim_system;
use support::task;
use support::with_script;

/// Idle far enough out that hibernation never races the retract window, so a
/// discarded outcome is the *filter's* doing and not an activation boundary's.
fn config() -> GranaryConfig {
    GranaryConfig {
        idle_after: Duration::from_secs(30),
        ..GranaryConfig::default()
    }
}

#[test]
fn an_outcome_whose_key_left_the_desired_set_is_discarded_and_writes_nothing() {
    // D5. The effect is slow enough that the retract lands first: the key leaves
    // the desired set with its effect still running, and the outcome that
    // arrives afterwards has nothing to be settled against.
    let sim = Simulation::new(41);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let script = seam();
    with_script(&script, |s| {
        s.set(
            0,
            Outcome::Answer {
                delay: Duration::from_secs(2),
            },
        );
    });
    let fanouts = fanouts(&system, &script, config());

    ask(&sim, &fanouts, "job/0", request(vec![task(0), task(1)]));
    // Long enough for both to launch, short of the two-second answer.
    sim.run_for(Duration::from_millis(500));
    assert_eq!(
        read_script(&script, |s| s.completions(0)),
        0,
        "the retract must land while task 0's effect is still running"
    );

    ask(&sim, &fanouts, "job/0", Retract(0));
    sim.run_for(Duration::from_secs(10));

    assert_eq!(
        read_script(&script, |s| s.completions(0)),
        1,
        "the retracted effect ran to completion anyway — that is the race"
    );
    assert_eq!(
        discarded(&recorder.events()),
        vec!["0".to_string()],
        "the outcome of a key absent from the desired set is discarded (D5)"
    );
    assert!(
        !settled(&recorder.events())
            .iter()
            .any(|(key, _)| key == "0"),
        "a discarded outcome never reaches `settle` (D5)"
    );

    let (done, _, outstanding) = ask(&sim, &fanouts, "job/0", Status);
    assert!(
        !done.contains_key(&0),
        "no record is written for a discarded outcome (D5)"
    );
    assert!(
        done.contains_key(&1),
        "the untouched sibling settled normally"
    );
    assert!(
        outstanding.is_empty(),
        "the retracted key is not outstanding either"
    );
}

#[test]
fn a_retracted_key_that_never_returns_leaves_the_activation_free_to_rest() {
    // The other side of the same rule (§3.2 step 2): releasing the claim of a key
    // that has left the desired set is what bounds the claim table by the work
    // outstanding rather than by the work ever done. A hung effect whose key was
    // retracted must stop holding the activation resident, or a cancelled call
    // would pin a grain forever.
    let sim = Simulation::new(43);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let script = seam();
    with_script(&script, |s| s.set(0, Outcome::Hang));
    let fanouts = fanouts(&system, &script, config());

    ask(&sim, &fanouts, "job/1", request(vec![task(0)]));
    sim.run_for(Duration::from_millis(500));

    ask(&sim, &fanouts, "job/1", Retract(0));
    sim.run_for(Duration::from_secs(5));

    assert!(
        announced_quiescence(&recorder.events()),
        "with its only key retracted the activation has nothing in flight and \
         nothing at all in its desired set — it must reach quiescence (§6)"
    );
}

#[test]
fn a_reused_key_lets_the_first_occurrences_outcome_be_settled_as_the_seconds() {
    // §3.1 and §4.5 together, stated as the failure they prevent. The filter
    // decides *presence*, not identity. Reuse a key across occurrences and the
    // drive cannot tell them apart: the straggler of the first finds its key
    // present again and is settled as the second's outcome, while the second's
    // own result arrives to an absent key and is dropped.
    //
    // This is not a bug in the drive. It is why "a key MUST be unique over the
    // grain's whole journal" is the consumer's obligation and normative — the
    // harness's synthesized call ids and step keys repeat across runs, and have
    // to be qualified by the turn before they can sit here.
    let sim = Simulation::new(47);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let script = seam();
    with_script(&script, |s| {
        // One key, two occurrences, deliberately different durations: the first
        // occurrence's effect outlives its retraction and lands inside the
        // second's window.
        s.set(
            0,
            Outcome::Answer {
                delay: Duration::from_secs(1),
            },
        );
    });
    let fanouts = fanouts(&system, &script, config());

    ask(&sim, &fanouts, "job/2", request(vec![task(0)]));
    sim.run_for(Duration::from_millis(200));

    // Retract, let the drive release the claim, and re-request the *same* key: a
    // second visit to one call site, which §3.1 forbids and nothing in the drive
    // can detect.
    ask(&sim, &fanouts, "job/2", Retract(0));
    // The retract's drive must run before the re-request, or the second
    // occurrence inherits the first's still-open claim instead of launching.
    sim.run_for(Duration::from_millis(10));
    with_script(&script, |s| {
        s.set(
            0,
            Outcome::Answer {
                delay: Duration::from_secs(30),
            },
        );
    });
    ask(&sim, &fanouts, "job/2", request(vec![task(0)]));

    // Past the first occurrence's answer, far short of the second's.
    sim.run_for(Duration::from_secs(3));
    let (done, _, outstanding): (_, _, Vec<TaskId>) = ask(&sim, &fanouts, "job/2", Status);

    assert_eq!(
        read_script(&script, |s| (s.runs(0), s.completions(0))),
        (2, 1),
        "two launches of the one key, and only the first has returned"
    );
    assert!(
        done.contains_key(&0),
        "the first occurrence's outcome was settled as the second's, because \
         presence is the only test the filter has (§4.5, §3.1)"
    );
    assert!(
        outstanding.is_empty(),
        "and it closed the second occurrence, which had not run"
    );

    // The second occurrence's own outcome, when it finally lands, finds its key
    // absent and is discarded — the mirror image of the mistake above.
    sim.run_for(Duration::from_secs(60));
    assert_eq!(
        read_script(&script, |s| s.completions(0)),
        2,
        "the second effect ran to completion"
    );
    assert_eq!(
        discarded(&recorder.events()),
        vec!["0".to_string()],
        "exactly one outcome was dropped, and it was the wrong one"
    );
}
