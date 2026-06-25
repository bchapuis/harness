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
use tenancy::CountByType;
use tenancy::Directory;
use tenancy::Forget;
use tenancy::List;
use tenancy::ListByType;
use tenancy::Record;
use tenancy::Types;

fn owned(key: &str) -> GrainName {
    GrainName::new("app.Session", key)
}

fn named(grain_type: &str, key: &str) -> GrainName {
    GrainName::new(grain_type, key)
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
fn queries_grains_by_type() {
    let sim = Simulation::new(0);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let directories = system.granary::<Directory<_>>(GranaryConfig::default());
    let dir = directories.grain("tenant/carol");

    sim.block_on(async move {
        // A principal with several grains across several types.
        for name in [
            named("app.Session", "s/2"),
            named("app.Session", "s/1"),
            named("app.Repo", "r/1"),
            named("app.Sandbox", "box/1"),
            named("app.Repo", "r/2"),
        ] {
            dir.ask(Record { name }).await.unwrap();
        }

        // By-type enumeration returns only that type's names, in key order.
        assert_eq!(
            dir.ask(ListByType { grain_type: "app.Session".into() }).await.unwrap(),
            vec![named("app.Session", "s/1"), named("app.Session", "s/2")],
        );
        assert_eq!(
            dir.ask(ListByType { grain_type: "app.Repo".into() }).await.unwrap(),
            vec![named("app.Repo", "r/1"), named("app.Repo", "r/2")],
        );
        // A type the principal owns none of yields nothing.
        assert!(dir.ask(ListByType { grain_type: "app.Other".into() }).await.unwrap().is_empty());

        assert_eq!(dir.ask(CountByType { grain_type: "app.Repo".into() }).await.unwrap(), 2);
        assert_eq!(dir.ask(CountByType { grain_type: "app.Sandbox".into() }).await.unwrap(), 1);

        // The distinct types owned, in order, deduped.
        assert_eq!(
            dir.ask(Types).await.unwrap(),
            vec!["app.Repo".to_string(), "app.Sandbox".to_string(), "app.Session".to_string()],
        );
        assert_eq!(dir.ask(Count).await.unwrap(), 5, "by-type views do not change the total");
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
