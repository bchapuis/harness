//! The node's admin socket: how `create`, `status`, `key`, and `egress` reach
//! the cluster.
//!
//! **Why the CLI is not a cluster client.** The agent runtime gateway joins the
//! transport as a non-voting member and addresses entities directly, and that is
//! the right shape for it: it is *long-lived*. A CLI process lives for one
//! command. A member that short never survives long enough for the failure
//! detector to probe it alive, so after the first invocation its id sits
//! unreachable, gossip stops flowing to it, and the next process to claim the
//! id discovers no hosts at all. A detector that believes an absent member is
//! absent is correct: the mismatch is the shape.
//!
//! Instead each node offers a loopback admin socket, and the node (already a
//! member, already routing) issues the entity command. One JSON request per
//! line, one JSON reply. Every operation is an ordinary journaled entity
//! command (machine §3); this socket adds no state, no authority, and no
//! durability of its own, so it stays a transport detail rather than a second
//! control plane. It binds loopback because it carries no authentication: the
//! machine's *own* boundary is the front door and its journaled key set (M4).

use std::ops::ControlFlow;
use std::time::Duration;

use entity::Entities;
use machine_entity::AddKey;
use machine_entity::EgressPolicy;
use machine_entity::MachineError;
use machine_entity::Provision;
use machine_entity::RevokeKey;
use machine_entity::SetEgress;
use machine_entity::Status;
use machine_entity::StatusReply;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;

use crate::authority::NodeMachine;

/// How long an admin-issued entity command waits. Provisioning imports the base
/// image whole (entity §7.15), so it gets the long end.
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
    SetEgress {
        machine: String,
        policy: EgressPolicy,
    },
}

#[derive(Serialize, Deserialize)]
pub enum AdminReply {
    Done,
    Status(Box<StatusReply>),
    /// The machine answered and said no (machine §3): not provisioned, already
    /// provisioned, a base image that would not import. Final: every node
    /// reaches the same journal, so asking again gets the same answer.
    Refused(MachineError),
    /// The command did not land (no shard leader yet, a timeout, a request
    /// this node could not read), so nothing was decided and another attempt,
    /// here or on another node, may succeed.
    Unavailable(String),
}

/// Serve admin requests until the process ends. One connection, one request.
pub async fn serve(listener: tokio::net::TcpListener, machines: Entities<NodeMachine>) {
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
    machines: Entities<NodeMachine>,
) -> std::io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut line = String::new();
    BufReader::new(read).read_line(&mut line).await?;
    let reply = match serde_json::from_str::<AdminRequest>(&line) {
        Ok(request) => dispatch(request, &machines).await,
        Err(e) => AdminReply::Unavailable(format!("malformed admin request: {e}")),
    };
    let mut encoded = serde_json::to_string(&reply)
        .unwrap_or_else(|e| format!("{{\"Unavailable\":\"reply could not be encoded: {e}\"}}"));
    encoded.push('\n');
    write.write_all(encoded.as_bytes()).await?;
    write.flush().await
}

async fn dispatch(request: AdminRequest, machines: &Entities<NodeMachine>) -> AdminReply {
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
        } => reply_of(
            machines
                .entity(machine)
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
                .await,
        ),
        AdminRequest::Status { machine } => {
            match machines
                .entity(machine)
                .ask_timeout(Status, ADMIN_TIMEOUT)
                .await
            {
                Ok(status) => AdminReply::Status(Box::new(status)),
                Err(e) => AdminReply::Unavailable(e.to_string()),
            }
        }
        AdminRequest::AddKey {
            machine,
            fingerprint,
            key,
        } => reply_of(
            machines
                .entity(machine)
                .ask_timeout(AddKey { fingerprint, key }, ADMIN_TIMEOUT)
                .await,
        ),
        AdminRequest::RevokeKey {
            machine,
            fingerprint,
        } => reply_of(
            machines
                .entity(machine)
                .ask_timeout(RevokeKey { fingerprint }, ADMIN_TIMEOUT)
                .await,
        ),
        AdminRequest::SetEgress { machine, policy } => reply_of(
            machines
                .entity(machine)
                .ask_timeout(SetEgress { policy }, ADMIN_TIMEOUT)
                .await,
        ),
    }
}

/// A mutating command's reply, keeping apart the two ways it fails: the
/// machine refused it, or the call did not land.
fn reply_of<E: std::fmt::Display>(outcome: Result<Result<(), MachineError>, E>) -> AdminReply {
    match outcome {
        Ok(Ok(())) => AdminReply::Done,
        Ok(Err(refusal)) => AdminReply::Refused(refusal),
        Err(e) => AdminReply::Unavailable(e.to_string()),
    }
}

/// Issue one request, trying each node's admin socket in turn.
///
/// Trying several is the point: a machine is reachable through *any* node, so
/// the CLI keeps working after the node it usually talks to is killed, which
/// is exactly what the failure drill does.
pub async fn call(addrs: &[String], request: AdminRequest) -> Result<AdminReply, String> {
    let mut last = "no --admin address given".to_string();
    // A node answers `Unavailable` while the cluster is still assembling (no
    // shard leader yet); retry across all of them before giving up.
    for round in 0..30 {
        for addr in addrs {
            match verdict(addr, try_call(addr, &request).await) {
                ControlFlow::Break(reply) => return Ok(reply),
                ControlFlow::Continue(why) => last = why,
            }
        }
        if round == 0 {
            eprintln!("cluster still assembling ({last}); retrying");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(last)
}

/// Whether one attempt settles the command. Any answer from the machine does,
/// a refusal included: re-sending it would only repeat the refusal on every
/// node. A call that did not land leaves the reason to retry with.
fn verdict(addr: &str, attempt: Result<AdminReply, String>) -> ControlFlow<AdminReply, String> {
    match attempt {
        Ok(AdminReply::Unavailable(why)) | Err(why) => {
            ControlFlow::Continue(format!("{addr}: {why}"))
        }
        Ok(reply) => ControlFlow::Break(reply),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_call_that_did_not_land_is_retried() {
        let unavailable = AdminReply::Unavailable("no shard leader".into());
        assert!(verdict("a", Ok(unavailable)).is_continue());
        assert!(verdict("a", Err("connection refused".into())).is_continue());

        for refusal in [
            MachineError::NotProvisioned,
            MachineError::AlreadyProvisioned,
            MachineError::Disk("base image unreadable".into()),
        ] {
            assert!(
                verdict("a", Ok(AdminReply::Refused(refusal))).is_break(),
                "a refusal is the machine's answer, not a reason to ask again",
            );
        }
        assert!(verdict("a", Ok(AdminReply::Done)).is_break());
    }
}
