//! A recording [`EventSink`] for observing a run's event stream (spec §16).
//!
//! The simulator checks invariants and seed-reproducibility against this stream
//! (spec §18.1, §18.5). Cloning shares the same underlying log.

use std::sync::Arc;
use std::sync::Mutex;

use actor_core::Event;
use actor_core::EventSink;

/// Collects emitted [`Event`]s in order. Wrap in an `Arc` and hand to
/// [`LocalSystemBuilder::events`](actor_core::LocalSystemBuilder::events); keep
/// a clone to read the log back.
#[derive(Clone, Default)]
pub struct Recorder {
    events: Arc<Mutex<Vec<Event>>>,
}

impl Recorder {
    /// A fresh, empty recorder.
    pub fn new() -> Recorder {
        Recorder::default()
    }

    /// A snapshot of the events recorded so far, in emission order.
    pub fn events(&self) -> Vec<Event> {
        self.events.lock().expect("recorder mutex poisoned").clone()
    }
}

impl EventSink for Recorder {
    fn emit(&self, event: Event) {
        self.events
            .lock()
            .expect("recorder mutex poisoned")
            .push(event);
    }
}
