//! The production node's stop, the counterpart of [`join`](crate::join()): the
//! signal that starts it and the departure that ends it. Held here once so the
//! order `leave`/`shutdown` document, and the operator lines the systemd units
//! are read against (`deploy/systemd`), are not restated per binary.

use std::future::Future;
use std::time::Duration;

use crate::TcpCluster;

/// The most a [`depart`] waits on the shard handoff before leaving anyway. Each
/// handoff is bounded on its own, so this is the backstop against a transport that
/// stops answering, sized well inside the 120 s stop budget the units allow
/// (`deploy/systemd`).
const HANDOFF_TIMEOUT: Duration = Duration::from_secs(30);

/// Resolve on SIGTERM or SIGINT: a unit's stop signal, and ctrl-c at a
/// terminal. Without a SIGTERM handler the default disposition kills the
/// process outright, and no graceful stop runs. Off unix only ctrl-c is wired.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::SignalKind;
        use tokio::signal::unix::signal;
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                term.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

/// Leave the cluster gracefully: run `handoff` (the node's shard handoffs,
/// resolving to how many led shards kept their leader) for at most a fixed
/// backstop sized inside the units' stop budget, announce `Leaving`, wait two SWIM
/// rounds at this node's probe interval for the gossip to carry the departure and
/// the control leader to commit it, then release the transport.
///
/// The handoff runs while this node is still a full member, so the transfers are
/// accepted; skipping it is safe but turns a rolling restart into one failover
/// per led shard. Each handoff is bounded on its own (entity's
/// `hand_off_leadership`: a fixed number of rounds, one transfer round trip per
/// led shard), so the timeout is only the backstop against a transport that stops
/// answering.
pub async fn depart(system: &TcpCluster, handoff: impl Future<Output = usize>) {
    let node = system.node();
    eprintln!("[{node}] stop signal received; handing off shard leadership");
    match tokio::time::timeout(HANDOFF_TIMEOUT, handoff).await {
        Ok(0) => eprintln!("[{node}] handed off every led shard; leaving the cluster"),
        Ok(n) => eprintln!(
            "[{node}] {n} shard(s) kept their leader (no caught-up replica); leaving the cluster"
        ),
        Err(_) => eprintln!(
            "[{node}] handoff still running after {HANDOFF_TIMEOUT:?}; leaving the cluster"
        ),
    }
    system.leave();
    tokio::time::sleep(2 * system.probe_interval()).await;
    system.shutdown();
}
