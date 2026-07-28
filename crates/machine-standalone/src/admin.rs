//! The node's admin socket: how `create`, `status`, and `key` reach the
//! cluster.
//!
//! **Why the CLI is not a cluster client.** The harness gateway joins the
//! transport as a non-voting member and addresses grains directly, and that is
//! the right shape for it: it is *long-lived*. A CLI process lives for one
//! command. A member that short never survives long enough for the failure
//! detector to probe it alive, so after the first invocation its id sits
//! unreachable, gossip stops flowing to it, and the next process to claim the
//! id discovers no hosts at all. Nothing there is a bug to fix — a detector
//! that believes an absent member is absent is correct. The mismatch is the
//! shape, so the CLI stops being a member.
//!
//! Instead each node offers a loopback admin socket, and the node — already a
//! member, already routing — issues the grain command. One JSON request per
//! line, one JSON reply. Every operation is an ordinary journaled grain
//! command (machine §3); this socket adds no state, no authority, and no
//! durability of its own, so it stays a transport detail rather than a second
//! control plane. It binds loopback because it carries no authentication: the
//! machine's *own* boundary is the front door and its journaled key set (M4).

use std::time::Duration;

use granary::Granary;
use machine::AddKey;
use machine::MachineError;
use machine::Provision;
use machine::RevokeKey;
use machine::Status;
use machine::StatusReply;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;

use crate::authority::NodeMachine;

/// How long an admin-issued grain command waits. Provisioning imports the base
/// image whole (grain §7.15), so it gets the long end.
const ADMIN_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Clone, Serialize, Deserialize)]
pub enum AdminRequest {
    Provision {
        machine: String,
        owner: String,
        base_image: String,
        vcpus: u8,
        mem_mib: u32,
        checkpoint_secs: u64,
        lease_secs: u64,
        authorized_keys: std::collections::BTreeMap<String, String>,
    },
    Status {
        machine: String,
    },
    AddKey {
        machine: String,
        fingerprint: String,
        key: String,
    },
    RevokeKey {
        machine: String,
        fingerprint: String,
    },
}

#[derive(Serialize, Deserialize)]
pub enum AdminReply {
    Done,
    /// The machine was already provisioned — reported rather than failed, so a
    /// re-run of a demo, or a retried command whose first landing was lost, is
    /// a no-op instead of an error.
    AlreadyProvisioned,
    Status(Box<StatusReply>),
    Error(String),
}

/// Serve admin requests until the process ends. One connection, one request.
pub async fn serve(listener: tokio::net::TcpListener, machines: Granary<NodeMachine>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let machines = machines.clone();
        tokio::spawn(async move {
            let _ = handle(stream, machines).await;
        });
    }
}

async fn handle(
    stream: tokio::net::TcpStream,
    machines: Granary<NodeMachine>,
) -> std::io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut line = String::new();
    BufReader::new(read).read_line(&mut line).await?;
    let reply = match serde_json::from_str::<AdminRequest>(&line) {
        Ok(request) => dispatch(request, &machines).await,
        Err(e) => AdminReply::Error(format!("malformed admin request: {e}")),
    };
    let mut encoded = serde_json::to_string(&reply)
        .unwrap_or_else(|e| format!("{{\"Error\":\"reply could not be encoded: {e}\"}}"));
    encoded.push('\n');
    write.write_all(encoded.as_bytes()).await?;
    write.flush().await
}

async fn dispatch(request: AdminRequest, machines: &Granary<NodeMachine>) -> AdminReply {
    match request {
        AdminRequest::Provision {
            machine,
            owner,
            base_image,
            vcpus,
            mem_mib,
            checkpoint_secs,
            lease_secs,
            authorized_keys,
        } => {
            let outcome = machines
                .grain(machine)
                .ask_timeout(
                    Provision {
                        owner,
                        base_image,
                        vcpus,
                        mem_mib,
                        checkpoint: Duration::from_secs(checkpoint_secs),
                        lease: Duration::from_secs(lease_secs),
                        authorized_keys,
                    },
                    ADMIN_TIMEOUT,
                )
                .await;
            match outcome {
                Ok(Ok(())) => AdminReply::Done,
                Ok(Err(MachineError::AlreadyProvisioned)) => AdminReply::AlreadyProvisioned,
                Ok(Err(e)) => AdminReply::Error(e.to_string()),
                Err(e) => AdminReply::Error(e.to_string()),
            }
        }
        AdminRequest::Status { machine } => {
            match machines
                .grain(machine)
                .ask_timeout(Status, ADMIN_TIMEOUT)
                .await
            {
                Ok(status) => AdminReply::Status(Box::new(status)),
                Err(e) => AdminReply::Error(e.to_string()),
            }
        }
        AdminRequest::AddKey {
            machine,
            fingerprint,
            key,
        } => reply_of(
            machines
                .grain(machine)
                .ask_timeout(AddKey { fingerprint, key }, ADMIN_TIMEOUT)
                .await,
        ),
        AdminRequest::RevokeKey {
            machine,
            fingerprint,
        } => reply_of(
            machines
                .grain(machine)
                .ask_timeout(RevokeKey { fingerprint }, ADMIN_TIMEOUT)
                .await,
        ),
    }
}

fn reply_of<E: std::fmt::Display>(outcome: Result<(), E>) -> AdminReply {
    match outcome {
        Ok(()) => AdminReply::Done,
        Err(e) => AdminReply::Error(e.to_string()),
    }
}

/// Issue one request, trying each node's admin socket in turn.
///
/// Trying several is the point: a machine is reachable through *any* node, so
/// the CLI keeps working after the node it usually talks to is killed — which
/// is exactly what the failure drill does.
pub async fn call(addrs: &[String], request: AdminRequest) -> Result<AdminReply, String> {
    let mut last = "no --admin address given".to_string();
    // A node can answer `Error` while the cluster is still assembling (no
    // shard leader yet); retry across all of them before giving up.
    for round in 0..30 {
        for addr in addrs {
            match try_call(addr, &request).await {
                Ok(AdminReply::Error(e)) => last = e,
                Ok(reply) => return Ok(reply),
                Err(e) => last = format!("{addr}: {e}"),
            }
        }
        if round == 0 {
            eprintln!("cluster still assembling ({last}); retrying");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(last)
}

async fn try_call(addr: &str, request: &AdminRequest) -> Result<AdminReply, String> {
    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| e.to_string())?;
    let (read, mut write) = stream.into_split();
    let mut encoded = serde_json::to_string(request).map_err(|e| e.to_string())?;
    encoded.push('\n');
    write
        .write_all(encoded.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    write.flush().await.map_err(|e| e.to_string())?;
    let mut line = String::new();
    BufReader::new(read)
        .read_line(&mut line)
        .await
        .map_err(|e| e.to_string())?;
    serde_json::from_str(&line).map_err(|e| format!("malformed admin reply: {e}"))
}
