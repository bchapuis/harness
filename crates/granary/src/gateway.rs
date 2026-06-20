//! The per-grain-type gateway: routing and the activation table (spec §5.3, §5.4).
//!
//! Each node runs one gateway actor per grain *type*, registered in the
//! receptionist (actor §13) under a single well-known key for that type — one
//! gateway entry per node ([`gateway_key`]). It owns the activation table mapping
//! `GrainName → Host` for shards this node leads, and getting-or-activating a name
//! is its one serial critical section: because the gateway is a serial actor, two
//! concurrent requests for a not-yet-active name are processed in order — the
//! first activates the host, the second finds it — so activation is
//! **exactly-once per node by construction**, with no lock (invariant **G6**).
//!
//! The gateway is also the **router** (§5.4), but **single-shot**: resolving a
//! name is two levels — name→shard by a stable hash ([`shard_for`]), shard→leader
//! from the system (`leads_shard`/`shard_leader`). When this node leads the shard
//! it activates the host locally and returns the handle; otherwise it returns
//! `NotLeader(hint)` *immediately* (ordinary Raft client redirection, §5.4 step
//! 4). The bounded redirect — following the hint, waiting out an election — is the
//! **caller's** job, driven by [`GrainRef`](crate::GrainRef) and bounded by the
//! caller's own deadline. Keeping the redirect off this serial handler is what
//! holds the gateway off the hot path even *during* a failover: a slow resolution
//! never blocks another grain's activation on this node (the prior in-handler loop
//! could pin the serial gateway for tens of seconds).

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Terminated;
use actor_core::receptionist::Key;
use serde::Deserialize;
use serde::Serialize;

use crate::config::GranaryConfig;
use crate::error::GrainError;
use crate::grain::Grain;
use crate::grain::GrainName;
use crate::host::Host;
use crate::shardmap::ShardMapSource;
use crate::system::GranarySystem;
use crate::system::ShardId;
use crate::system::shard_for;

/// The receptionist key the gateway for a grain type registers under (spec
/// §5.3): one well-known key per type, one entry per node. Routing looks the
/// leader node's gateway up here.
///
/// `grain_type` is the runtime type name (spec §5.1), `G::GRAIN_TYPE` by default
/// but a caller-supplied name when one Rust grain is hosted under several type
/// names ([`granary_named`](crate::GranaryExt::granary_named)). Two type names
/// thus register distinct keys even though both are `Key<Gateway<G>>`.
pub(crate) fn gateway_key<G: Grain>(grain_type: &'static str) -> Key<Gateway<G>> {
    Key::new(grain_type)
}

/// Get-or-activate the host for a name and return a handle to it (spec §5.4). The
/// reply is the live `Host` activation — on this node when it leads the name's
/// shard, otherwise the activation on the leader node (resolved by forwarding to
/// that node's gateway). The caller then sends the command straight to it, keeping
/// the serial gateway off the steady-state hot path.
///
/// Registered for network dispatch (see [`Gateway::register`]) so a caller on
/// another node can drive a remote activation; the returned `ActorRef<Host<G>>`
/// rebinds on the caller's node (the framework decodes replies under the local
/// system, actor §4.4).
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub(crate) struct Activate<G: Grain> {
    pub(crate) name: GrainName,
    #[serde(skip)]
    _marker: PhantomData<fn() -> G>,
}

impl<G: Grain> Activate<G> {
    /// An activation request. The receiving gateway answers **single-shot** (§5.4):
    /// it activates if this node leads the name's shard, else returns
    /// `NotLeader(hint)`. The caller follows the hint ([`GrainRef`](crate::GrainRef)).
    pub(crate) fn new(name: GrainName) -> Activate<G> {
        Activate {
            name,
            _marker: PhantomData,
        }
    }
}

impl<G: Grain> Message for Activate<G> {
    type Reply = Result<ActorRef<Host<G>>, GrainError>;
    const MANIFEST: Manifest = Manifest::new("granary.Activate");
}

/// The node-local gateway for grain type `G` (spec §5.3).
pub(crate) struct Gateway<G: Grain> {
    /// Names this node currently hosts. The serial actor is the only writer, so
    /// no lock guards it (**G6**).
    table: HashMap<GrainName, ActorRef<Host<G>>>,
    /// The runtime type name (spec §5.1), `G::GRAIN_TYPE` by default; a
    /// caller-supplied name when one Rust grain is hosted under several type
    /// names ([`granary_named`](crate::GranaryExt::granary_named)). Used to map a
    /// name to its shard.
    grain_type: &'static str,
    /// The shard map (§7.6): which nodes replicate each shard and this node's local
    /// store for the shards it replicates, read live as the consensus allocation
    /// commits.
    shard_map: Arc<dyn ShardMapSource>,
    /// The number of shards the type's namespace is partitioned into (§7.1).
    shards: usize,
    config: GranaryConfig,
    /// How a fresh activation's behavior value is built (the runtime instantiates
    /// the grain; the user supplies no value per `GrainName`).
    factory: Arc<dyn Fn() -> G + Send + Sync>,
}

impl<G: Grain> Gateway<G> {
    pub(crate) fn new(
        grain_type: &'static str,
        shard_map: Arc<dyn ShardMapSource>,
        shards: usize,
        config: GranaryConfig,
        factory: Arc<dyn Fn() -> G + Send + Sync>,
    ) -> Gateway<G> {
        Gateway {
            table: HashMap::new(),
            grain_type,
            shard_map,
            shards,
            config,
            factory,
        }
    }

    /// The number of shards this gateway routes over.
    fn shards(&self) -> usize {
        self.shards
    }

    /// Get-or-activate the host for `name` on **this** node (its shard's leader),
    /// returning a live handle. The serial actor makes this exactly-once per node
    /// (**G6**): a concurrent caller for the same name finds the host the first one
    /// activated. Called only when this node replicates and leads the shard, so the
    /// shard's `journal` is present.
    fn get_or_activate(&mut self, name: GrainName, shard: ShardId, ctx: &Ctx<Gateway<G>>) -> ActorRef<Host<G>> {
        // Get: return the live host if the cached entry is still alive. The
        // liveness check closes the eviction race (§10): a host that hibernated
        // but whose `Terminated` has not yet pruned the table is dropped here and
        // re-activated afresh, rather than handing back a dead reference.
        if let Some(host) = self.table.get(&name) {
            if ctx.system().resolve_local::<Host<G>>(host.id()).is_some() {
                return host.clone();
            }
            self.table.remove(&name);
        }

        // Activate: spawn a restartable host that rehydrates from its shard's
        // journal (§9). `spawn_with` keeps it restartable; the factory clones the
        // seam handles per (re)construction. A leader replicates the shard, so the
        // map has built its journal.
        let journal = self
            .shard_map
            .journal(shard.index)
            .expect("a leader replicates the shard, so its journal is present");
        let config = self.config.clone();
        let factory = Arc::clone(&self.factory);
        let gateway = ctx.this();
        let activated = name.clone();
        let grain_type = self.grain_type;
        let host = ctx.spawn_with(move || {
            Host::new(
                grain_type,
                (factory)(),
                activated.clone(),
                journal.clone(),
                config.clone(),
                gateway.clone(),
            )
        });
        // Prune the table when the host stops — idle hibernation or fault (§10).
        ctx.watch(&host);
        self.table.insert(name, host.clone());
        host
    }
}

impl<G: Grain> Actor for Gateway<G> {
    type System = G::System;

    /// Accept [`Activate`] over the network (spec §5.4): a caller on another node
    /// drives a remote activation by asking this node's gateway, and gets back the
    /// host handle. This is the gateway's whole network surface — the typed command
    /// then travels straight to the host (`RunTyped`, registered on [`Host`]).
    fn register(registry: &mut HandlerRegistry<Gateway<G>>) {
        registry.accept::<Activate<G>>();
    }
}

impl<G: Grain> Handler<Activate<G>> for Gateway<G> {
    /// Single-shot (§5.4): if this node replicates and leads the name's shard,
    /// get-or-activate the host and return it; otherwise return `NotLeader(hint)`
    /// at once. The caller follows the hint — the redirect loop lives in
    /// [`GrainRef`](crate::GrainRef), bounded by the caller's deadline — so this
    /// serial handler never blocks another grain's activation while a shard
    /// elects (the prior in-handler loop could pin the gateway for tens of
    /// seconds, hanging concurrent activations).
    async fn handle(
        &mut self,
        msg: Activate<G>,
        ctx: &Ctx<Gateway<G>>,
    ) -> Result<ActorRef<Host<G>>, GrainError> {
        let shard = shard_for(self.grain_type, msg.name.key(), self.shards());
        if self.shard_map.journal(shard.index).is_some() && ctx.system().leads_shard(shard) {
            Ok(self.get_or_activate(msg.name, shard, ctx))
        } else {
            Err(GrainError::NotLeader(self.redirect_hint(shard, ctx)))
        }
    }
}

impl<G: Grain> Gateway<G> {
    /// The node a non-leading gateway redirects the caller to (§5.4). The believed
    /// leader when known; otherwise, on a node that does **not** replicate the
    /// shard (so `shard_leader` cannot know the leader), a replica from the
    /// consensus-agreed shard map — so a non-replica caller reaches a replica that
    /// can serve or name the real leader. A replica mid-election (or a not-yet-
    /// committed map) hints itself, so the caller backs off and retries here until
    /// the shard settles.
    fn redirect_hint(&self, shard: ShardId, ctx: &Ctx<Gateway<G>>) -> NodeId {
        if let Some(leader) = ctx.system().shard_leader(shard) {
            return leader;
        }
        let replicas = self.shard_map.replicas(shard.index).unwrap_or_default();
        let me = ctx.system().node();
        match replicas.iter().find(|&&n| n != me) {
            // A non-replica: send the caller to a replica it can route through.
            Some(&replica) if !replicas.contains(&me) => replica,
            // A replica mid-election, or no map yet: retry us once the shard settles.
            _ => me,
        }
    }
}

impl<G: Grain> Handler<Terminated> for Gateway<G> {
    async fn handle(&mut self, signal: Terminated, _ctx: &Ctx<Gateway<G>>) {
        // Drop the stopped host from the activation table; the next message for
        // that name re-activates it (§10).
        self.table.retain(|_, host| *host.id() != signal.id);
    }
}
