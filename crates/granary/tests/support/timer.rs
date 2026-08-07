//! The alarm-bearing `Timer` grain, shared by the alarm suites
//! (docs/simulation-testing.md).
//!
//! One durable deadline, one callerless `on_alarm` that journals a `Fired` event
//! and stops. Its whole job is to make a fire *countable*: `fired` is folded from
//! committed events, so a caller asking [`ReadFired`] learns how many times the
//! alarm's effect actually committed — not how many times the host invoked the
//! handler, which under faults is a different and much larger number (§7.16).
//!
//! Generic over the hosting system, like [`granary::AlarmIndex`] itself, so the
//! same grain runs on the `Local` tier in `alarm_index.rs` and on the clustered
//! `Quorum` tier in `alarm_cluster.rs` and `alarm_loss.rs`. Before it moved here
//! the first two carried byte-identical copies differing only in `type System`.

use std::marker::PhantomData;
use std::time::Duration;

use actor_core::Manifest;
use actor_core::Message;
use granary::Alarm;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GranarySystem;
use serde::Deserialize;
use serde::Serialize;

/// The grain type name the alarm index and the driver key on.
pub const TIMER_TYPE: &str = "test.Timer";

/// A grain whose only state is how many times its alarm has fired.
pub struct Timer<S>(PhantomData<fn() -> S>);

// Manual `Default` (not derived), for the reason `AlarmIndex`'s is manual: the
// grain holds only `PhantomData`, so it is `Default` for every `S`, whereas the
// derive would demand `S: Default`.
impl<S> Default for Timer<S> {
    fn default() -> Self {
        Timer(PhantomData)
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct TimerState {
    /// Committed fires. Incremented by folding [`TimerEvent::Fired`], so it counts
    /// what reached the journal through the output gate — an `on_alarm` whose
    /// commit failed leaves this untouched and the deadline still pending, which
    /// is at-most-once behaving correctly rather than a miss.
    pub fired: u64,
}

#[derive(Serialize, Deserialize)]
pub enum TimerEvent {
    Fired,
}

impl<S: GranarySystem> Grain for Timer<S> {
    type System = S;
    type State = TimerState;
    type Event = TimerEvent;
    type Facets = (Alarm,);
    const GRAIN_TYPE: &'static str = TIMER_TYPE;

    fn apply(state: &mut TimerState, event: &TimerEvent) {
        match event {
            TimerEvent::Fired => state.fired += 1,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Arm>();
        r.accept::<ReadFired>();
    }

    async fn on_alarm(&self, _s: &TimerState, _ctx: &GrainCtx<Self>) -> Vec<TimerEvent> {
        // Does not re-arm: the fired deadline's `Clear` is already staged, so this
        // grain's alarm consumes itself and the count can only rise once per arm.
        vec![TimerEvent::Fired]
    }
}

/// Arm the grain's alarm `after_ms` from the activation's logical clock.
#[derive(Clone, Serialize, Deserialize)]
pub struct Arm {
    pub after_ms: u64,
}

impl Message for Arm {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.timer.Arm");
}

impl<S: GranarySystem> GrainHandler<Arm> for Timer<S> {
    async fn handle(&self, _s: &TimerState, m: Arm, ctx: &GrainCtx<Self>) -> (Vec<TimerEvent>, ()) {
        ctx.alarm().set_after(Duration::from_millis(m.after_ms));
        (vec![], ())
    }
}

/// How many times this grain's alarm has committed a fire.
#[derive(Clone, Serialize, Deserialize)]
pub struct ReadFired;

impl Message for ReadFired {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("test.timer.ReadFired");
}

impl<S: GranarySystem> GrainHandler<ReadFired> for Timer<S> {
    async fn handle(
        &self,
        s: &TimerState,
        _m: ReadFired,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<TimerEvent>, u64) {
        (vec![], s.fired)
    }
}
