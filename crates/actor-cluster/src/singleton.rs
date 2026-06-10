//! Cluster singleton (utilities spec §4).
//!
//! One logical instance of an actor cluster-wide. Every hosting node runs a
//! **manager** per singleton name: a tick loop that observes only the local
//! membership view and spawns the instance iff this node is the **anchor** —
//! the rendezvous owner of the singleton's name over the serving set
//! (utilities spec §2). The anchor is a pure function of the view, so converged
//! nodes agree on it in every control-plane mode; no coordination is added.
//!
//! The activated instance registers under the singleton's receptionist key, so
//! discovery, draining filters, and pruning-on-termination all ride the
//! existing receptionist machinery (core spec §13) — a [`SingletonProxy`] is
//! just a typed view over that listing. When the view stops naming this node
//! anchor, the manager hands off by delivering the user-supplied **stop
//! message** (there is deliberately no external kill: the actor winds down
//! through its own handler, which SHOULD call `ctx.stop()` promptly).
//!
//! **Guarantee honesty (utilities spec §4 item 3):** at most one activation per
//! *converged* view. While views diverge — a partition, detector lag — two
//! nodes MAY each believe they are anchor and both run an instance; once views
//! converge the surplus manager stops its instance. The singleton is *not* a
//! mutual-exclusion primitive; callers needing exclusivity must fence. Calls
//! through a proxy during a handoff gap fail fast (`DeadLetter`, or
//! `Unreachable`/`Timeout` on a stale entry) and are never buffered (core spec
//! §14.2).

use std::sync::Arc;
use std::sync::Mutex;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::Handler;
use actor_core::Key;
use actor_core::Message;
use actor_core::Spawner;

use crate::placement;
use crate::system::ClusterSystem;
use crate::transport::Transport;

impl<C, E, S, T> ClusterSystem<C, E, S, T>
where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    /// Host a cluster singleton on this node (utilities spec §4): run a manager
    /// for `name`, activating `factory`'s actor whenever this node's view names
    /// it anchor and handing off with `stop` when it no longer does. Every node
    /// that may host the singleton calls this with the same arguments; nodes
    /// that only call it use [`singleton_proxy`](Self::singleton_proxy).
    ///
    /// `stop` is the handoff message (`A: Handler<M>` proves the instance
    /// accepts it); its handler SHOULD `ctx.stop()` promptly — until the old
    /// instance terminates, its registration lingers and proxies may still
    /// reach it.
    pub fn singleton<A, M, F>(&self, name: &'static str, factory: F, stop: M) -> SingletonProxy<A>
    where
        A: Actor<System = Self> + Handler<M>,
        M: Message + Clone,
        F: FnMut() -> A + Send + 'static,
    {
        self.launch_task(manager(self.clone(), name, factory, stop));
        self.singleton_proxy(name)
    }

    /// A client-only handle to the singleton `name` (utilities spec §4): no
    /// manager runs, this node never hosts. The proxy re-resolves through the
    /// receptionist on every call, so it follows the instance across handoffs.
    pub fn singleton_proxy<A>(&self, name: &'static str) -> SingletonProxy<A>
    where
        A: Actor<System = Self>,
    {
        SingletonProxy {
            key: Key::new(name),
            system: self.clone(),
        }
    }
}

/// The manager's view of its local activation.
enum State<A: Actor> {
    /// Not anchor (or not yet activated): no local instance.
    Idle,
    /// This node activated the instance and still means to host it.
    Active(ActorRef<A>),
    /// The stop message was sent; awaiting the instance's termination. No new
    /// activation may start until it is gone (the per-node half of U2).
    Stopping(ActorRef<A>),
}

/// One singleton manager (utilities spec §4): a tick loop on the SWIM probe
/// cadence, evaluating the anchor against the local view only. Launched
/// per `singleton()` call, so systems that host no singleton run no loop
/// (core spec §18.1).
async fn manager<C, E, S, T, A, M, F>(
    system: ClusterSystem<C, E, S, T>,
    name: &'static str,
    factory: F,
    stop: M,
) where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
    A: Actor<System = ClusterSystem<C, E, S, T>> + Handler<M>,
    M: Message + Clone,
    F: FnMut() -> A + Send + 'static,
{
    let key: Key<A> = Key::new(name);
    let interval = system.probe_interval();
    // `spawn_with` consumes a factory per activation (it keeps it for
    // supervision restarts), and the manager re-activates after a stop — so the
    // one user factory is shared and each activation hands the host a fresh
    // delegating closure.
    let factory = Arc::new(Mutex::new(factory));
    let mut state: State<A> = State::Idle;
    loop {
        if system.is_shutting_down() {
            return;
        }
        let anchor = placement::owner(&system.membership().serving_members(), name.as_bytes());
        let is_anchor = anchor == Some(system.node());
        state = match state {
            State::Idle if is_anchor => {
                let shared = Arc::clone(&factory);
                let instance =
                    system.spawn_with(move || shared.lock().expect("factory mutex poisoned")());
                system.receptionist().register(key, &instance);
                system.emit(Event::SingletonStarted {
                    name,
                    actor: instance.id().clone(),
                });
                State::Active(instance)
            }
            // Observed terminated — by handoff, supervision Stop, or its own
            // doing. Back to Idle; the next tick re-activates iff still anchor
            // (the liveness half of U2).
            State::Active(instance) | State::Stopping(instance)
                if system.resolve_local::<A>(instance.id()).is_none() =>
            {
                system.emit(Event::SingletonStopped {
                    name,
                    actor: instance.id().clone(),
                });
                State::Idle
            }
            // The view moved the anchor elsewhere: hand off. The instance winds
            // down through its own handler; a local tell only fails when the
            // instance is already gone, which the arm above then observes.
            State::Active(instance) if !is_anchor => {
                let _ = instance.tell(stop.clone()).await;
                State::Stopping(instance)
            }
            other => other,
        };
        system.clock().sleep(interval).await;
    }
}

/// A typed handle to a cluster singleton (utilities spec §4): a view over the
/// singleton's receptionist listing that re-resolves on every call, following
/// the instance across handoffs. During a handoff gap — no live registration,
/// or a stale one — calls fail fast and are never buffered (core spec §14.2).
pub struct SingletonProxy<A: Actor> {
    key: Key<A>,
    system: A::System,
}

impl<A: Actor> Clone for SingletonProxy<A> {
    fn clone(&self) -> Self {
        SingletonProxy {
            key: self.key,
            system: self.system.clone(),
        }
    }
}

impl<A: Actor> SingletonProxy<A> {
    /// The current instance as this node sees it: the first entry of the
    /// serving listing (deterministic order, so converged nodes resolve the
    /// same instance even while a superseded registration lingers).
    pub fn resolve(&self) -> Option<ActorRef<A>> {
        self.system.receptionist().lookup(self.key).first().cloned()
    }

    /// Whether this node currently sees a live instance.
    pub fn is_active(&self) -> bool {
        self.resolve().is_some()
    }

    /// Request/response to the singleton. With no resolvable instance the call
    /// fails fast with [`CallError::DeadLetter`].
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        match self.resolve() {
            Some(instance) => instance.ask(msg).await,
            None => Err(CallError::DeadLetter),
        }
    }

    /// Fire-and-forget to the singleton. With no resolvable instance the call
    /// fails fast with [`CallError::DeadLetter`].
    pub async fn tell<M>(&self, msg: M) -> Result<(), CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        match self.resolve() {
            Some(instance) => instance.tell(msg).await,
            None => Err(CallError::DeadLetter),
        }
    }
}
