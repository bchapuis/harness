//! The virtual clock (spec §4.6, §18.2): logical time that advances only at
//! quiescence, driven by [`Simulation::run`](crate::Simulation::run).

use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use actor_core::Clock;
use actor_core::Instant;

use crate::executor::Shared;

/// A [`Clock`] backed by the simulation scheduler. Reads and advances the
/// shared virtual time; `sleep` registers a timer that the run loop fires.
#[derive(Clone)]
pub struct SimClock {
    shared: Shared,
}

impl SimClock {
    pub(crate) fn new(shared: Shared) -> SimClock {
        SimClock { shared }
    }
}

impl Clock for SimClock {
    fn now(&self) -> Instant {
        self.shared.lock().expect("scheduler mutex poisoned").now()
    }

    fn sleep(&self, dur: Duration) -> impl Future<Output = ()> + Send {
        let deadline = self.now() + dur;
        Sleep {
            shared: self.shared.clone(),
            deadline,
            registered: false,
        }
    }
}

/// Future returned by [`SimClock::sleep`]. Completes once virtual time reaches
/// `deadline`; until then it registers exactly one timer.
struct Sleep {
    shared: Shared,
    deadline: Instant,
    registered: bool,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let mut inner = this.shared.lock().expect("scheduler mutex poisoned");
        if inner.now() >= this.deadline {
            return Poll::Ready(());
        }
        // Register once. The scheduler's waker targets a task by id, so the
        // first-polled waker remains valid across re-polls (no need to refresh).
        if !this.registered {
            inner.register_timer(this.deadline, cx.waker().clone());
            this.registered = true;
        }
        Poll::Pending
    }
}
