//! Group routers over receptionist listings (utilities spec §3).
//!
//! A [`Router`] is a node-local, typed view over a receptionist [`Key`]: every
//! routing decision draws from the **current serving listing** (core spec §13
//! req 4) and picks one routee by strategy. It replicates nothing, introduces
//! no frames, and adds no delivery guarantee beyond the underlying `ask`/`tell`
//! (core spec §7.2).
//!
//! Decisions are made against a fresh [`lookup`](actor_core::Receptionist::lookup)
//! snapshot rather than a subscription: the listing is replicated local state,
//! so the lookup is a synchronous read with nothing to await, and a snapshot
//! per decision needs no background pump. The listing's order is deterministic
//! (the registry is an ordered set), so round-robin is seed-reproducible, and
//! the random strategy draws from the system's seeded entropy (core spec
//! §18.1) — never a host RNG.
//!
//! Keyed (rendezvous-hashed) routing is the `*_by` family, available regardless
//! of the keyless [`RouteStrategy`]: the caller supplies the routing key as
//! bytes, and routees are ranked by their placement weight (utilities spec §2)
//! over the routee's actor tag. The key is an explicit per-call parameter — a
//! typed router serves many message types, so a stored extractor cannot be
//! expressed; message-trait sugar can layer on later without breaking this.
//!
//! An empty listing fails fast with [`CallError::DeadLetter`] — the router
//! never buffers, queues, or retries (core spec §1.2, §14.2). Callers that
//! want to pre-check use [`Router::is_empty`].

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Handler;
use actor_core::Key;
use actor_core::Listing;
use actor_core::Message;

use crate::placement;

/// The keyless selection strategies (utilities spec §3). Rendezvous-hashed
/// selection is the keyed [`route_by`](Router::route_by)/[`ask_by`](Router::ask_by)/
/// [`tell_by`](Router::tell_by) family, available on every router.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteStrategy {
    /// Cycle through the listing in its deterministic order.
    RoundRobin,
    /// Pick uniformly via the system's seeded entropy (core spec §18.1).
    Random,
}

/// A group router over a receptionist [`Key`] (utilities spec §3).
///
/// Cloning is cheap and clones share the round-robin cursor, so two handles to
/// one logical router keep cycling as one.
pub struct Router<A: Actor> {
    system: A::System,
    key: Key<A>,
    strategy: RouteStrategy,
    cursor: Arc<AtomicU64>,
}

impl<A: Actor> Clone for Router<A> {
    fn clone(&self) -> Self {
        Router {
            system: self.system.clone(),
            key: self.key,
            strategy: self.strategy,
            cursor: Arc::clone(&self.cursor),
        }
    }
}

impl<A: Actor> Router<A> {
    /// A router over `key` with the given keyless strategy.
    pub fn new(system: &A::System, key: Key<A>, strategy: RouteStrategy) -> Router<A> {
        Router {
            system: system.clone(),
            key,
            strategy,
            cursor: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The current serving listing (a snapshot; core spec §13 req 4 filter
    /// applies, so drained and down nodes' routees are omitted).
    pub fn routees(&self) -> Listing<A> {
        self.system.receptionist().lookup(self.key)
    }

    /// Whether the group currently has no serving routee. A route now would
    /// fail with [`CallError::DeadLetter`].
    pub fn is_empty(&self) -> bool {
        self.routees().is_empty()
    }

    /// Pick a routee by the keyless strategy; `None` when the listing is empty.
    pub fn route(&self) -> Option<ActorRef<A>> {
        let routees = self.routees().into_vec();
        if routees.is_empty() {
            return None;
        }
        let index = match self.strategy {
            RouteStrategy::RoundRobin => {
                self.cursor.fetch_add(1, Ordering::Relaxed) as usize % routees.len()
            }
            RouteStrategy::Random => self.system.next_random() as usize % routees.len(),
        };
        routees.into_iter().nth(index)
    }

    /// Pick a routee by rendezvous weight of `route_key` over the routees'
    /// actor tags (utilities spec §2, §3): the same key over the same listing
    /// selects the same routee on every node; ties resolve to the lower
    /// [`ActorId`](actor_core::ActorId). `None` when the listing is empty.
    pub fn route_by(&self, route_key: &[u8]) -> Option<ActorRef<A>> {
        self.routees().into_vec().into_iter().max_by_key(|routee| {
            (
                placement::weight(&placement::actor_tag(routee.id()), route_key),
                std::cmp::Reverse(routee.id().clone()),
            )
        })
    }

    /// Request/response to a routee picked by the keyless strategy. An empty
    /// group fails fast with [`CallError::DeadLetter`].
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        match self.route() {
            Some(routee) => routee.ask(msg).await,
            None => Err(CallError::DeadLetter),
        }
    }

    /// Request/response to the routee owning `route_key`. An empty group fails
    /// fast with [`CallError::DeadLetter`].
    pub async fn ask_by<M>(&self, route_key: &[u8], msg: M) -> Result<M::Reply, CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        match self.route_by(route_key) {
            Some(routee) => routee.ask(msg).await,
            None => Err(CallError::DeadLetter),
        }
    }

    /// Fire-and-forget to a routee picked by the keyless strategy. An empty
    /// group fails fast with [`CallError::DeadLetter`].
    pub async fn tell<M>(&self, msg: M) -> Result<(), CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        match self.route() {
            Some(routee) => routee.tell(msg).await,
            None => Err(CallError::DeadLetter),
        }
    }

    /// Fire-and-forget to the routee owning `route_key`. An empty group fails
    /// fast with [`CallError::DeadLetter`].
    pub async fn tell_by<M>(&self, route_key: &[u8], msg: M) -> Result<(), CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        match self.route_by(route_key) {
            Some(routee) => routee.tell(msg).await,
            None => Err(CallError::DeadLetter),
        }
    }
}
