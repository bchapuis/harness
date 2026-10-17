//! The directory entity on the single-node `Local` tier (entity §7.4).
//!
//! Drives a principal's [`Directory`] through the public `EntityRef` API only:
//! record ownership with listing metadata, enumerate it, query by type, forget
//! one, clear the rest. Asserts the ownership-index contract (idempotent
//! record/forget, metadata round-trips, stable enumeration) and that the
//! journaled index survives hibernation.

use std::collections::BTreeMap;
use std::time::Duration;

use actor_core::LocalSystemBuilder;
use actor_simulation::Simulation;
use entity::EntityConfig;
use entity::EntityExt;
use entity::EntityName;
use tenancy::Clear;
use tenancy::Directory;
use tenancy::Forget;
use tenancy::Get;
use tenancy::List;
use tenancy::ListByType;
use tenancy::Meta;
use tenancy::Record;
use tenancy::Recorded;

fn owned(key: &str) -> EntityName {
    EntityName::new("app.Session", key)
}

fn named(entity_type: &str, key: &str) -> EntityName {
    EntityName::new(entity_type, key)
}

fn meta(label: &str, created_at: u64) -> Meta {
    Meta {
        label: Some(label.to_string()),
        created_at: Some(created_at),
        attrs: BTreeMap::new(),
    }
}

#[test]
fn records_enumerates_and_forgets() {
    let sim = Simulation::new(0);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let directories = system.entities::<Directory<_>>(EntityConfig::default());
    // One directory per principal: the principal id is the entity key.
    let dir = directories.entity("tenant/alice");

    sim.block_on(async move {
        // Record is idempotent on identical metadata, and updates on changed.
        let r = dir
            .ask(Record {
                name: owned("s/1"),
                meta: meta("first", 100),
            })
            .await
            .unwrap();
        assert_eq!(r, Recorded::Created, "first record is new");
        let r = dir
            .ask(Record {
                name: owned("s/1"),
                meta: meta("first", 100),
            })
            .await
            .unwrap();
        assert_eq!(
            r,
            Recorded::Unchanged,
            "re-record with same meta commits nothing"
        );
        let r = dir
            .ask(Record {
                name: owned("s/1"),
                meta: meta("renamed", 100),
            })
            .await
            .unwrap();
        assert_eq!(r, Recorded::Updated, "changed meta updates in place");

        dir.ask(Record {
            name: owned("s/2"),
            meta: meta("second", 200),
        })
        .await
        .unwrap();
        dir.ask(Record {
            name: owned("s/3"),
            meta: meta("third", 300),
        })
        .await
        .unwrap();

        // Metadata round-trips: the listing shows the chosen label and time.
        let got = dir
            .ask(Get { name: owned("s/1") })
            .await
            .unwrap()
            .expect("owned");
        assert_eq!(got.label.as_deref(), Some("renamed"));
        assert_eq!(got.created_at, Some(100));
        assert!(dir.ask(Get { name: owned("s/9") }).await.unwrap().is_none());

        // Enumeration is stable (key order) and carries each entry's metadata.
        let listing = dir.ask(List).await.unwrap();
        let view: Vec<_> = listing
            .iter()
            .map(|e| (e.name.key(), e.meta.label.as_deref()))
            .collect();
        assert_eq!(
            view,
            vec![
                ("s/1", Some("renamed")),
                ("s/2", Some("second")),
                ("s/3", Some("third"))
            ],
        );

        // Forget is idempotent too.
        assert!(
            dir.ask(Forget { name: owned("s/2") }).await.unwrap(),
            "present name removed"
        );
        assert!(
            !dir.ask(Forget { name: owned("s/2") }).await.unwrap(),
            "absent name is a no-op"
        );
        assert_eq!(dir.ask(List).await.unwrap().len(), 2);

        // Clear drops the rest and reports how many it forgot.
        assert_eq!(
            dir.ask(Clear).await.unwrap(),
            2,
            "clear forgets the remaining two"
        );
        assert_eq!(
            dir.ask(Clear).await.unwrap(),
            0,
            "clearing an empty index commits nothing"
        );
        assert!(dir.ask(List).await.unwrap().is_empty());
    });
}

#[test]
fn queries_entities_by_type() {
    let sim = Simulation::new(0);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let directories = system.entities::<Directory<_>>(EntityConfig::default());
    let dir = directories.entity("tenant/carol");

    sim.block_on(async move {
        // A principal with several entities across several types.
        for (name, m) in [
            (named("app.Session", "s/2"), meta("second", 2)),
            (named("app.Session", "s/1"), meta("first", 1)),
            (named("app.Repo", "r/1"), meta("repo one", 3)),
            (named("app.Sandbox", "box/1"), meta("a box", 4)),
            (named("app.Repo", "r/2"), meta("repo two", 5)),
        ] {
            dir.ask(Record { name, meta: m }).await.unwrap();
        }

        // By-type enumeration returns only that type's entries, in key order,
        // with metadata attached.
        let sessions = dir
            .ask(ListByType {
                entity_type: "app.Session".into(),
            })
            .await
            .unwrap();
        let view: Vec<_> = sessions
            .iter()
            .map(|e| (e.name.key(), e.meta.label.as_deref()))
            .collect();
        assert_eq!(view, vec![("s/1", Some("first")), ("s/2", Some("second"))]);

        let repos = dir
            .ask(ListByType {
                entity_type: "app.Repo".into(),
            })
            .await
            .unwrap();
        assert_eq!(
            repos.iter().map(|e| e.name.key()).collect::<Vec<_>>(),
            vec!["r/1", "r/2"]
        );

        // A type the principal owns none of yields nothing.
        assert!(
            dir.ask(ListByType {
                entity_type: "app.Other".into()
            })
            .await
            .unwrap()
            .is_empty()
        );

        assert_eq!(
            dir.ask(ListByType {
                entity_type: "app.Sandbox".into()
            })
            .await
            .unwrap()
            .len(),
            1
        );
        assert_eq!(
            dir.ask(List).await.unwrap().len(),
            5,
            "by-type views do not change the total"
        );
    });
}

#[test]
fn index_survives_hibernation() {
    let sim = Simulation::new(7);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    // Aggressive idle window so the directory hibernates between the two batches.
    let directories = system.entities::<Directory<_>>(EntityConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every: 2,
        ..EntityConfig::default()
    });
    let dir = directories.entity("tenant/bob");

    sim.block_on(async move {
        dir.ask(Record {
            name: owned("repo/x"),
            meta: meta("x", 1),
        })
        .await
        .unwrap();
        dir.ask(Record {
            name: owned("repo/y"),
            meta: meta("y", 2),
        })
        .await
        .unwrap();
    });

    // Drive past the idle window: the entity snapshots, hibernates, and stops.
    sim.run();

    // A fresh ref re-activates the name; the journaled index and its metadata
    // rehydrate intact.
    let reread = directories.entity("tenant/bob");
    let listing = sim.block_on(async move { reread.ask(List).await.unwrap() });
    let view: Vec<_> = listing
        .iter()
        .map(|e| (e.name.key(), e.meta.label.as_deref()))
        .collect();
    assert_eq!(
        view,
        vec![("repo/x", Some("x")), ("repo/y", Some("y"))],
        "hibernation must not lose the ownership index or its metadata",
    );
}
