//! The durable workspace filesystem grain on the single-node `Local` tier
//! (durable-workspace design; granary §7.10, §14).
//!
//! Drives [`Fs`] through its public command API: write and read files (whole and
//! ranged, including multi-block content), `mkdir -p` on nested writes, list and stat,
//! overwrite, truncate (shrink and zero-fill grow), remove, rename, destroy — and
//! that the workspace survives hibernation because the metadata is journaled and the
//! blocks live in the grain's colocated blob area (§7.10).

use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::GrainEvent;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::fs::BLOCK_BYTES;
use granary::fs::Destroy;
use granary::fs::DirEntry;
use granary::fs::Fs;
use granary::fs::FsError;
use granary::fs::ListDir;
use granary::fs::Metadata;
use granary::fs::ReadFile;
use granary::fs::Remove;
use granary::fs::Rename;
use granary::fs::Stat;
use granary::fs::Truncate;
use granary::fs::WriteFile;

type Ws = Fs<SimSystem>;

fn local() -> (Simulation, SimSystem) {
    let sim = Simulation::new(7);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    (sim, system)
}

fn write(path: &str, content: &[u8]) -> WriteFile {
    WriteFile {
        path: path.into(),
        content: content.to_vec(),
    }
}

fn read(path: &str) -> ReadFile {
    ReadFile {
        path: path.into(),
        range: None,
    }
}

#[test]
fn write_then_read_round_trips_whole_and_ranged() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        let content = b"a workspace file, written through the grain".to_vec();
        assert_eq!(
            g.ask(write("notes.txt", &content)).await.unwrap(),
            Ok(content.len() as u64)
        );
        assert_eq!(g.ask(read("notes.txt")).await.unwrap(), Ok(content.clone()));
        // A ranged read slices the file.
        assert_eq!(
            g.ask(ReadFile {
                path: "notes.txt".into(),
                range: Some((2, 11))
            })
            .await
            .unwrap(),
            Ok(content[2..11].to_vec())
        );
        // A missing file is a typed application error, not wrong bytes.
        assert_eq!(
            g.ask(read("missing")).await.unwrap(),
            Err(FsError::NotFound)
        );
    });
}

#[test]
fn nested_write_creates_parent_dirs_and_lists() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        g.ask(write("src/app/main.rs", b"fn main() {}"))
            .await
            .unwrap()
            .unwrap();
        g.ask(write("src/app/lib.rs", b"// lib"))
            .await
            .unwrap()
            .unwrap();
        // mkdir -p created src and src/app; both are directories.
        assert_eq!(
            g.ask(Stat { path: "src".into() }).await.unwrap(),
            Ok(Metadata { dir: true, size: 0 })
        );
        // The directory lists its two files, name-sorted, with sizes.
        let listing = g
            .ask(ListDir {
                path: "src/app".into(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            listing,
            vec![
                DirEntry {
                    name: "lib.rs".into(),
                    dir: false,
                    size: 6
                },
                DirEntry {
                    name: "main.rs".into(),
                    dir: false,
                    size: 12
                },
            ]
        );
    });
}

#[test]
fn overwrite_replaces_the_whole_file() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        g.ask(write("f", b"the original longer contents"))
            .await
            .unwrap()
            .unwrap();
        g.ask(write("f", b"short")).await.unwrap().unwrap();
        // The shorter overwrite fully replaces — no stale tail from the first write.
        assert_eq!(g.ask(read("f")).await.unwrap(), Ok(b"short".to_vec()));
        assert_eq!(
            g.ask(Stat { path: "f".into() }).await.unwrap(),
            Ok(Metadata {
                dir: false,
                size: 5
            })
        );
    });
}

#[test]
fn multi_block_content_round_trips_across_the_block_boundary() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        // Just over one block, so the write chunks into two blobs.
        let mut content = vec![0u8; BLOCK_BYTES + 1000];
        for (i, b) in content.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        g.ask(write("big.bin", &content)).await.unwrap().unwrap();
        assert_eq!(g.ask(read("big.bin")).await.unwrap(), Ok(content.clone()));
        // A range straddling the block boundary reassembles from both blocks.
        let (lo, hi) = (BLOCK_BYTES as u64 - 10, BLOCK_BYTES as u64 + 10);
        assert_eq!(
            g.ask(ReadFile {
                path: "big.bin".into(),
                range: Some((lo, hi))
            })
            .await
            .unwrap(),
            Ok(content[lo as usize..hi as usize].to_vec())
        );
    });
}

#[test]
fn truncate_shrinks_and_grows_with_zero_fill() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        g.ask(write("f", b"0123456789")).await.unwrap().unwrap();
        // Shrink: the read clamps to the new size.
        g.ask(Truncate {
            path: "f".into(),
            size: 4,
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(g.ask(read("f")).await.unwrap(), Ok(b"0123".to_vec()));
        // Grow: the gap reads back as zeros.
        g.ask(Truncate {
            path: "f".into(),
            size: 7,
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(g.ask(read("f")).await.unwrap(), Ok(b"0123\0\0\0".to_vec()));
    });
}

#[test]
fn remove_and_rename() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        g.ask(write("dir/a", b"a")).await.unwrap().unwrap();
        g.ask(write("dir/b", b"b")).await.unwrap().unwrap();
        // A non-recursive remove of a non-empty directory is refused.
        assert_eq!(
            g.ask(Remove {
                path: "dir".into(),
                recursive: false
            })
            .await
            .unwrap(),
            Err(FsError::NotEmpty)
        );
        // Remove one file; rename the other within the directory.
        g.ask(Remove {
            path: "dir/a".into(),
            recursive: false,
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(g.ask(read("dir/a")).await.unwrap(), Err(FsError::NotFound));
        g.ask(Rename {
            from: "dir/b".into(),
            to: "dir/c".into(),
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(g.ask(read("dir/c")).await.unwrap(), Ok(b"b".to_vec()));
        // Recursive remove clears the directory subtree.
        g.ask(Remove {
            path: "dir".into(),
            recursive: true,
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            g.ask(Stat { path: "dir".into() }).await.unwrap(),
            Err(FsError::NotFound)
        );
    });
}

#[test]
fn destroy_resets_the_workspace() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        g.ask(write("a", b"a")).await.unwrap().unwrap();
        g.ask(write("b/c", b"c")).await.unwrap().unwrap();
        g.ask(Destroy).await.unwrap().unwrap();
        assert_eq!(g.ask(read("a")).await.unwrap(), Err(FsError::NotFound));
        assert_eq!(
            g.ask(Stat { path: "b".into() }).await.unwrap(),
            Err(FsError::NotFound)
        );
        // The workspace is usable again after the reset.
        g.ask(write("fresh", b"new")).await.unwrap().unwrap();
        assert_eq!(g.ask(read("fresh")).await.unwrap(), Ok(b"new".to_vec()));
    });
}

#[test]
fn workspace_survives_hibernation() {
    let sim = Simulation::new(7);
    let recorder = Recorder::new();
    let sink: std::sync::Arc<dyn EventSink> = std::sync::Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let ws = system.granary::<Ws>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every: 1,
        ..GranaryConfig::default()
    });

    sim.block_on({
        let g = ws.grain("ws/0");
        async move {
            g.ask(write("keep/notes.txt", b"durable working state"))
                .await
                .unwrap()
                .unwrap();
        }
    });

    // Drive past the idle window: the grain snapshots its metadata and hibernates.
    sim.run();
    assert!(
        recorder.events().iter().any(|e| matches!(
            e.as_app::<GrainEvent>(),
            Some(GrainEvent::Passivated { .. })
        )),
        "the idle workspace must hibernate",
    );

    // A fresh activation rehydrates the metadata from the snapshot and the file blocks
    // are still in the colocated blob area — the workspace survives the eviction.
    let g = ws.grain("ws/0");
    let got = sim.block_on(async move { g.ask(read("keep/notes.txt")).await.unwrap() });
    assert_eq!(got, Ok(b"durable working state".to_vec()));
}
