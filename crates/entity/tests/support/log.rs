//! An appendable log entity and the reference subscription reconciler.
//!
//! Shared because two suites drive the same object and the same reconciliation
//! rule: `subscription_faults.rs` scripts individual §14 fault cases against it,
//! and `subscription_swarm.rs` sweeps it under the nemesis. An independently
//! maintained second copy of a reconciler is exactly the thing that drifts into
//! agreeing with whatever the code does (docs/simulation-testing.md).
//!
//! [`collect`] is the contract `agent::Follower` implements: subscribe,
//! backfill the gap by `load`, take live batches, and re-check the journal when
//! the stream goes quiet. Everything it does is forced by **E16** — push is a
//! latency optimization and correctness rests on the journal, so a sink that
//! trusted delivery would be asserting the wrong thing.

use actor_core::ActorSystem;
use std::time::Duration;

use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::SimNode;
use entity::Entity;
use entity::EntityCtx;
use entity::EntityHandler;
use entity::EntityRef;
use entity::EntitySystem;
use entity::Seq;
use entity::Subscription;
use serde::Deserialize;
use serde::Serialize;

/// How long a caught-up collector waits for a live record before re-checking the
/// journal — the liveness net that detects a silent leader move.
pub const RESYNC: Duration = Duration::from_millis(400);

// --- An entity whose records are an appendable log, readable by seq -------------

#[derive(Default)]
pub struct LogEntity;

#[derive(Default, Serialize, Deserialize)]
pub struct Log {
    pub events: Vec<i64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Val(pub i64);

impl Entity for LogEntity {
    type System = SimNode;
    type State = Log;
    type Event = Val;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "test.Log";

    fn apply(state: &mut Log, event: &Val) {
        state.events.push(event.0);
    }

    fn register(r: &mut entity::EntityRegistry<Self>) {
        r.accept::<Append>();
        r.accept::<ReadFrom>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Append(pub i64);
impl Message for Append {
    type Reply = u64; // the seq the appended record occupies
    const MANIFEST: Manifest = Manifest::new("test.Append");
}
impl EntityHandler<Append> for LogEntity {
    async fn handle(&self, state: &Log, msg: Append, _: &EntityCtx<Self>) -> (Vec<Val>, u64) {
        (vec![Val(msg.0)], state.events.len() as u64 + 1)
    }
}

/// The backfill read (the §7.3 `load` a subscriber reconciles against): records
/// after `from`, as `(seq, value)`.
#[derive(Clone, Serialize, Deserialize)]
pub struct ReadFrom {
    pub from: u64,
}
impl Message for ReadFrom {
    type Reply = Vec<(u64, i64)>;
    const MANIFEST: Manifest = Manifest::new("test.ReadFrom");
}
impl EntityHandler<ReadFrom> for LogEntity {
    async fn handle(
        &self,
        state: &Log,
        msg: ReadFrom,
        _: &EntityCtx<Self>,
    ) -> (Vec<Val>, Vec<(u64, i64)>) {
        let recs = (msg.from as usize..state.events.len())
            .map(|i| (i as u64 + 1, state.events[i]))
            .collect();
        (vec![], recs)
    }
}

// --- The reference reconciler (the contract `agent::Follower` implements) ----

/// Collect the entity's records up to `target` by reconciling a subscription with
/// journal backfill: subscribe, backfill the gap, take live batches, and on a
/// silent move re-check the journal after [`RESYNC`]. The returned values are the
/// reconstructed committed sequence.
pub async fn collect(system: SimNode, entity: EntityRef<LogEntity>, target: usize) -> Vec<i64> {
    collect_until(system, entity, |out| out.len() >= target).await
}

/// [`collect`], with the stopping condition given as a predicate over what has
/// been reconstructed so far.
///
/// A sweep cannot use a fixed `target` the way a scripted test can: under a
/// nemesis some appends never commit, so the number of records that will *ever*
/// exist is not known when the collector starts. It stops on a deadline instead
/// and is judged against the journal's actual head.
pub async fn collect_until(
    system: SimNode,
    entity: EntityRef<LogEntity>,
    done: impl Fn(&[i64]) -> bool,
) -> Vec<i64> {
    let mut last: u64 = 0;
    let mut out: Vec<i64> = Vec::new();
    let mut sub: Option<Subscription<LogEntity>> = None;
    while !done(&out) {
        if sub.is_none() {
            match entity.subscribe(Seq::new(last)).await {
                Ok(s) => sub = Some(s),
                Err(_) => {
                    system.sleep(RESYNC).await; // shard still electing; retry
                    continue;
                }
            }
        }
        // Backfill from the journal until caught up to the head.
        match entity.ask(ReadFrom { from: last }).await {
            Ok(recs) if !recs.is_empty() => {
                for (seq, v) in recs {
                    if seq > last {
                        last = seq;
                        out.push(v);
                    }
                }
                continue;
            }
            Ok(_) => {} // at the head
            Err(_) => {
                sub = None; // leader moved; re-subscribe + backfill
                system.sleep(RESYNC).await;
                continue;
            }
        }
        // Caught up: race a live batch against the re-sync timer.
        let rx = sub.as_ref().expect("subscribed").records.clone();
        let recv = rx.recv();
        let resync = system.sleep(RESYNC);
        futures::pin_mut!(recv);
        match futures::future::select(recv, resync).await {
            futures::future::Either::Left((Ok(stream), _)) => {
                if stream.from.value() <= last {
                    for (seq, v) in stream.records {
                        if seq.value() > last {
                            last = seq.value();
                            out.push(v.0);
                        }
                    }
                }
            }
            // Stream closed: re-subscribe.
            futures::future::Either::Left((Err(_), _)) => sub = None,
            // Timer won: the backfill at the loop top recovers any post-move
            // records the dead push path never delivered.
            futures::future::Either::Right(_) => {}
        }
    }
    out
}
