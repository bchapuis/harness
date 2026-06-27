//! Durable recovery of the clustered `Quorum` journal under deterministic
//! simulation (granary §14), focused on the property the per-grain quorum substrate
//! makes non-trivial: **a full-cluster cold restart**.
//!
//! In the `Quorum` tier a grain's records live off the leader-election group's Raft
//! log, in each replica's [`GrainStore`](granary::GrainStore) (§7.2). So unlike a
//! shared-log design, surviving a simultaneous restart of every replica needs the
//! store itself to be durable — injected through [`GranaryConfig::grain_store`] and
//! preserved across the restart (the grain analogue of the Raft WAL, §7.4). These
//! tests inject a per-node store that outlives `net.restart`, commit, cold-restart
//! the whole cluster, and assert each grain recovers its committed state from a
//! quorum of the reloaded stores (**G14**). The healthy-path quorum behaviours
//! (append visible on every replica, follower fenced, quorum loss → `Unavailable`)
//! live in `tests/clustered_grains.rs` and `tests/partition_safety.rs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use granary::FileGrainStore;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GrainStore;
use granary::GrainStoreFactory;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::MemoryGrainStore;
use serde::Deserialize;
use serde::Serialize;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

// --- A minimal counter grain ---------------------------------------------------

#[derive(Default)]
struct Account;

#[derive(Default, Serialize, Deserialize)]
struct Balance {
    cents: i64,
}

#[derive(Serialize, Deserialize)]
enum Ledger {
    Deposited(u64),
}

impl Grain for Account {
    type System = SimCluster;
    type State = Balance;
    type Event = Ledger;
    const GRAIN_TYPE: &'static str = "bank.Account";

    fn apply(state: &mut Balance, event: &Ledger) {
        match event {
            Ledger::Deposited(n) => state.cents += *n as i64,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Deposit>();
        r.accept::<ReadBalance>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Deposit {
    cents: u64,
}
impl Message for Deposit {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.Deposit");
}
impl GrainHandler<Deposit> for Account {
    async fn handle(
        &self,
        state: &Balance,
        msg: Deposit,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<Ledger>, i64) {
        (
            vec![Ledger::Deposited(msg.cents)],
            state.cents + msg.cents as i64,
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadBalance;
impl Message for ReadBalance {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.ReadBalance");
}
impl GrainHandler<ReadBalance> for Account {
    async fn handle(
        &self,
        state: &Balance,
        _msg: ReadBalance,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<Ledger>, i64) {
        (vec![], state.cents)
    }
}

// --- Harness -------------------------------------------------------------------

fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

fn raft() -> RaftConfig {
    let mut config = RaftConfig::new(vec![A, B, C]);
    config.election_timeout = Duration::from_millis(500);
    config.heartbeat_interval = Duration::from_millis(100);
    config
}

fn leader_net(sim: &Simulation) -> SimNetwork {
    SimNetwork::new(sim).with_leader(swim(), raft(), DowningPolicy::Conservative)
}

/// A per-node grain store that **survives a restart**: the factory hands a restarted
/// node the same store it had before (the WAL-storage analogue, §7.4). Without this,
/// a full-cluster cold restart would lose every grain's records.
fn durable_stores() -> GrainStoreFactory {
    let stores: Arc<Mutex<HashMap<NodeId, Arc<MemoryGrainStore>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    Arc::new(move |node: NodeId| {
        let mut stores = stores.lock().expect("store map poisoned");
        let store = stores
            .entry(node)
            .or_insert_with(|| Arc::new(MemoryGrainStore::new()))
            .clone();
        store as Arc<dyn GrainStore>
    })
}

fn config(grain_store: GrainStoreFactory, snapshot_every: u64) -> GranaryConfig {
    GranaryConfig {
        shards: 2,
        replication_factor: 3,
        idle_after: Duration::from_secs(60),
        snapshot_every,
        grain_store: Some(grain_store),
        ..GranaryConfig::default()
    }
}

/// Drive an async call to completion under the perpetually-running cluster loops.
fn drive<T: Send + 'static>(
    sim: &Simulation,
    settle: Duration,
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    let cell: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let out = Arc::clone(&cell);
    sim.spawner().launch(Box::pin(async move {
        *out.lock().unwrap() = Some(future.await);
    }));
    sim.run_for(settle);
    cell.lock()
        .unwrap()
        .take()
        .expect("future did not complete")
}

/// Commit `deposits` to `key` through the cluster, returning the final balance.
fn deposit_each(sim: &Simulation, granary: &Granary<Account>, key: &'static str, deposits: &[u64]) {
    for &cents in deposits {
        let g = granary.clone();
        let outcome = drive(sim, Duration::from_secs(5), async move {
            g.grain(key).ask(Deposit { cents }).await
        });
        assert!(outcome.is_ok(), "deposit committed: {outcome:?}");
    }
}

#[test]
fn committed_writes_survive_a_full_cluster_cold_restart() {
    // The whole cluster goes down and comes back: no survivor keeps a leader or any
    // in-memory state, so every grain must recover its head from a quorum of the
    // replicas' *reloaded* stores (§8, G14). The injected store outlives the restart;
    // the records do not live in any Raft log.
    let sim = Simulation::new(1);
    let net = leader_net(&sim);
    let factory = durable_stores();
    let mut systems = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2));
    let granaries: Vec<Granary<Account>> = systems
        .iter()
        .map(|s| s.granary::<Account>(config(factory.clone(), 0)))
        .collect();
    sim.run_for(Duration::from_secs(3));

    let key = "account/1";
    deposit_each(&sim, &granaries[0], key, &[10, 20, 30, 40, 50]);

    // Cold-restart EVERY node, then re-host the grain type with the SAME store
    // factory, so each restarted node reattaches to its persisted store.
    for (idx, node) in [A, B, C].into_iter().enumerate() {
        systems[idx] = net.restart(node);
    }
    sim.run_for(Duration::from_secs(3));
    let granaries: Vec<Granary<Account>> = systems
        .iter()
        .map(|s| s.granary::<Account>(config(factory.clone(), 0)))
        .collect();
    sim.run_for(Duration::from_secs(3));

    // The grain recovers its committed balance from a quorum of reloaded stores.
    let balance = {
        let g = granaries[0].clone();
        drive(&sim, Duration::from_secs(8), async move {
            g.grain(key)
                .ask_timeout(ReadBalance, Duration::from_secs(7))
                .await
        })
    };
    assert_eq!(
        balance,
        Ok(150),
        "every committed deposit survives the cold restart"
    );

    // A fresh write lands contiguously on the recovered head — no gap, no loss.
    let after = {
        let g = granaries[0].clone();
        drive(&sim, Duration::from_secs(8), async move {
            g.grain(key)
                .ask_timeout(Deposit { cents: 5 }, Duration::from_secs(7))
                .await
        })
    };
    assert_eq!(
        after,
        Ok(155),
        "a re-elected leader commits the next event from the recovered head"
    );
}

#[test]
fn committed_writes_survive_a_full_cluster_cold_restart_with_a_file_store() {
    // The same total cold restart as above, but through the *production* durable
    // store: a file-backed [`FileGrainStore`] under a real directory (§7.4). To prove
    // recovery comes from disk and not a retained handle, the old factory (its
    // per-node cache and open append files) is dropped before the restart and a
    // brand-new factory is opened at the SAME path afterward — exactly what a process
    // restart does. Every committed deposit must come back from the reloaded files.
    let dir = tempfile::tempdir().expect("tempdir");
    let sim = Simulation::new(7);
    let net = leader_net(&sim);
    let factory = FileGrainStore::factory(dir.path());
    let mut systems = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2));
    let granaries: Vec<Granary<Account>> = systems
        .iter()
        .map(|s| s.granary::<Account>(config(factory.clone(), 0)))
        .collect();
    sim.run_for(Duration::from_secs(3));

    let key = "account/file";
    deposit_each(&sim, &granaries[0], key, &[10, 20, 30, 40, 50]);

    // Drop everything that holds the store open, then cold-restart every node and
    // re-host with a FRESH factory at the same path: nothing but the files survives.
    drop(granaries);
    drop(factory);
    for (idx, node) in [A, B, C].into_iter().enumerate() {
        systems[idx] = net.restart(node);
    }
    sim.run_for(Duration::from_secs(3));
    let factory = FileGrainStore::factory(dir.path());
    let granaries: Vec<Granary<Account>> = systems
        .iter()
        .map(|s| s.granary::<Account>(config(factory.clone(), 0)))
        .collect();
    sim.run_for(Duration::from_secs(3));

    let balance = {
        let g = granaries[0].clone();
        drive(&sim, Duration::from_secs(8), async move {
            g.grain(key)
                .ask_timeout(ReadBalance, Duration::from_secs(7))
                .await
        })
    };
    assert_eq!(
        balance,
        Ok(150),
        "every committed deposit survives the cold restart from disk"
    );

    // A fresh write lands contiguously on the head recovered from the reloaded files.
    let after = {
        let g = granaries[0].clone();
        drive(&sim, Duration::from_secs(8), async move {
            g.grain(key)
                .ask_timeout(Deposit { cents: 5 }, Duration::from_secs(7))
                .await
        })
    };
    assert_eq!(
        after,
        Ok(155),
        "a re-elected leader commits the next event from the recovered head"
    );
}

#[test]
fn a_compacted_grain_survives_a_full_cluster_cold_restart() {
    // Like the cold restart above, but snapshots are taken (`snapshot_every`), so a
    // recovering leader reloads each replica's snapshot plus the tail rather than the
    // whole history — the snapshot-recovery path of §9, exercised across a total
    // restart where there is no peer to install a snapshot from.
    let sim = Simulation::new(2);
    let net = leader_net(&sim);
    let factory = durable_stores();
    let mut systems = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2));
    let granaries: Vec<Granary<Account>> = systems
        .iter()
        .map(|s| s.granary::<Account>(config(factory.clone(), 8)))
        .collect();
    sim.run_for(Duration::from_secs(3));

    let key = "account/snap";
    let deposits: Vec<u64> = (1..=20).collect(); // crosses snapshot_every several times
    deposit_each(&sim, &granaries[0], key, &deposits);
    let expected: i64 = deposits.iter().map(|&c| c as i64).sum();

    for (idx, node) in [A, B, C].into_iter().enumerate() {
        systems[idx] = net.restart(node);
    }
    sim.run_for(Duration::from_secs(3));
    let granaries: Vec<Granary<Account>> = systems
        .iter()
        .map(|s| s.granary::<Account>(config(factory.clone(), 8)))
        .collect();
    sim.run_for(Duration::from_secs(3));

    let balance = {
        let g = granaries[0].clone();
        drive(&sim, Duration::from_secs(8), async move {
            g.grain(key)
                .ask_timeout(ReadBalance, Duration::from_secs(7))
                .await
        })
    };
    assert_eq!(
        balance,
        Ok(expected),
        "the snapshot + tail rebuild the full history across a compacted cold restart",
    );
}

#[test]
fn an_ephemeral_store_loses_state_on_a_full_cluster_cold_restart() {
    // The contrast that proves the seam matters: with the *default* (ephemeral)
    // store, a full-cluster cold restart cannot recover — the records were only in
    // memory. The grain comes back empty (or the call fails); it never returns the
    // pre-restart value. (Partial failover, where a quorum stays up, still survives —
    // that is `clustered_grains`.)
    let sim = Simulation::new(3);
    let net = leader_net(&sim);
    let factory = durable_stores();
    let mut systems = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2));
    // Note: a fresh ephemeral factory per `granary` call — nothing survives restart.
    let granaries: Vec<Granary<Account>> = systems
        .iter()
        .map(|s| s.granary::<Account>(config(factory.clone(), 0)))
        .collect();
    sim.run_for(Duration::from_secs(3));

    let key = "account/eph";
    deposit_each(&sim, &granaries[0], key, &[100]);

    // Cold-restart every node and re-host with a BRAND-NEW ephemeral factory, so no
    // restarted node reattaches to its old store.
    let ephemeral = durable_stores();
    for (idx, node) in [A, B, C].into_iter().enumerate() {
        systems[idx] = net.restart(node);
    }
    sim.run_for(Duration::from_secs(3));
    let granaries: Vec<Granary<Account>> = systems
        .iter()
        .map(|s| s.granary::<Account>(config(ephemeral.clone(), 0)))
        .collect();
    sim.run_for(Duration::from_secs(3));

    let balance = {
        let g = granaries[0].clone();
        drive(&sim, Duration::from_secs(8), async move {
            g.grain(key)
                .ask_timeout(ReadBalance, Duration::from_secs(7))
                .await
        })
    };
    assert_ne!(
        balance,
        Ok(100),
        "an ephemeral store cannot survive a total cold restart"
    );
}
