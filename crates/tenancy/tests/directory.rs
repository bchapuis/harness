//! The directory grain on the single-node `Local` tier (granary §7.4).
//!
//! Drives a principal's [`Directory`] through the public `GrainRef` API only:
//! record ownership, enumerate it, forget one, clear the rest. Asserts the
//! ownership-index contract — idempotent record/forget, stable enumeration, and
//! that the index survives hibernation because it is journaled, not in-memory.

use std::time::Duration;

use actor_core::LocalSystemBuilder;
use actor_simulation::Simulation;
use granary::GrainName;
use granary::GranaryConfig;
use granary::GranaryExt;
use tenancy::Clear;
use tenancy::Contains;
use tenancy::Count;
use tenancy::Directory;
use tenancy::Forget;
use tenancy::List;
use tenancy::Record;

fn owned(key: &str) -> GrainName {
    GrainName::new("app.Session", key)
}

#[test]
fn records_enumerates_and_forgets() {
    let sim = Simulation::new(0);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let directories = system.granary::<Directory<_>>(GranaryConfig::default());
    // One directory per principal: the principal id is the grain key.
    let dir = directories.grain("tenant/alice");

    sim.block_on(async move {
        // Record is idempotent: the first records, the second is a no-op read.
        assert!(dir.ask(Record { name: owned("s/1") }).await.unwrap(), "first record is new");
        assert!(!dir.ask(Record { name: owned("s/1") }).await.unwrap(), "re-record commits nothing");
        assert!(dir.ask(Record { name: owned("s/2") }).await.unwrap());
        assert!(dir.ask(Record { name: owned("s/3") }).await.unwrap());

        assert_eq!(dir.ask(Count).await.unwrap(), 3);
        assert!(dir.ask(Contains { name: owned("s/2") }).await.unwrap());
        assert!(!dir.ask(Contains { name: owned("s/9") }).await.unwrap());

        // Enumeration is stable (BTreeSet order), independent of insert order.
        let names = dir.ask(List).await.unwrap();
        assert_eq!(names, vec![owned("s/1"), owned("s/2"), owned("s/3")]);

        // Forget is idempotent too.
        assert!(dir.ask(Forget { name: owned("s/2") }).await.unwrap(), "present name removed");
        assert!(!dir.ask(Forget { name: owned("s/2") }).await.unwrap(), "absent name is a no-op");
        assert_eq!(dir.ask(Count).await.unwrap(), 2);

        // Clear drops the rest and reports how many it forgot.
        assert_eq!(dir.ask(Clear).await.unwrap(), 2, "clear forgets the remaining two");
        assert_eq!(dir.ask(Clear).await.unwrap(), 0, "clearing an empty index commits nothing");
        assert!(dir.ask(List).await.unwrap().is_empty());
    });
}

#[test]
fn index_survives_hibernation() {
    let sim = Simulation::new(7);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    // Aggressive idle window so the directory passivates between the two batches.
    let directories = system.granary::<Directory<_>>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every: 2,
        ..GranaryConfig::default()
    });
    let dir = directories.grain("tenant/bob");

    sim.block_on(async move {
        dir.ask(Record { name: owned("repo/x") }).await.unwrap();
        dir.ask(Record { name: owned("repo/y") }).await.unwrap();
    });

    // Drive past the idle window: the grain snapshots, passivates, and stops.
    sim.run();

    // A fresh ref re-activates the name; the journaled index rehydrates intact.
    let reread = directories.grain("tenant/bob");
    let names = sim.block_on(async move { reread.ask(List).await.unwrap() });
    assert_eq!(
        names,
        vec![owned("repo/x"), owned("repo/y")],
        "hibernation must not lose the ownership index",
    );
}
