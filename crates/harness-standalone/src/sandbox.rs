//! The local [`SandboxProvider`]: one workspace directory per session.
//!
//! The spec mandates *that* tool effects run behind the sandbox seam, not
//! *how* (harness spec §5.1, §5.3); this deployment's "how" is the simplest
//! useful one — a working directory and `/bin/sh`. It scopes each session's
//! files apart and gives the model a real shell, but it is NOT an isolation
//! boundary: a command can read and write anything the node process can.
//! Run it with inputs you trust.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use actor_core::BoxFuture;
use harness::Sandbox;
use harness::SandboxError;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SessionId;
use harness::ToolError;
use serde_json::Value;
use serde_json::json;

use crate::ids::sanitize;

/// Cap on each captured stream, so one chatty command cannot blow up the
/// journal and the model context it feeds.
const OUTPUT_CAP: usize = 16 * 1024;

/// Provisions one workspace directory per session under a root.
pub struct LocalSandboxes {
    root: PathBuf,
}

impl LocalSandboxes {
    pub fn new(root: impl Into<PathBuf>) -> LocalSandboxes {
        LocalSandboxes { root: root.into() }
    }
}

impl SandboxProvider for LocalSandboxes {
    fn open(
        &self,
        session: &SessionId,
        _profile: &SandboxProfile,
    ) -> BoxFuture<'static, Result<Arc<dyn Sandbox>, SandboxError>> {
        let dir = self.root.join(sanitize(session.as_str()));
        Box::pin(async move {
            tokio::fs::create_dir_all(&dir)
                .await
                .map_err(|e| SandboxError(format!("{}: {e}", dir.display())))?;
            Ok(Arc::new(LocalSandbox { dir }) as Arc<dyn Sandbox>)
        })
    }
}

/// One session's workspace.
struct LocalSandbox {
    dir: PathBuf,
}

impl Sandbox for LocalSandbox {
    fn call(&self, name: &str, input: Value) -> BoxFuture<'static, Result<Value, ToolError>> {
        let dir = self.dir.clone();
        let name = name.to_string();
        Box::pin(async move {
            // The registry allowlist already guards dispatch (§5.2); this is
            // the provider's own check that it implements what was named.
            if name != "shell" {
                return Err(ToolError::Sandbox(format!(
                    "tool not provided by this sandbox: {name}"
                )));
            }
            let command = input
                .get("command")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ToolError::InvalidArguments("`command` must be a string".to_string())
                })?;
            // kill_on_drop: the harness enforces the tool timeout by dropping
            // this future (§5.3 item 3) — the child must die with it.
            let output = tokio::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(command)
                .current_dir(&dir)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .output()
                .await
                .map_err(|e| ToolError::Sandbox(format!("spawn: {e}")))?;
            Ok(json!({
                "exit_code": output.status.code(),
                "stdout": capped(&output.stdout),
                "stderr": capped(&output.stderr),
            }))
        })
    }

    fn release(&self) -> BoxFuture<'static, ()> {
        // Tear down working state (the trait's contract): the next activation
        // opens a fresh directory, matching the WorkspaceReset the harness
        // journals for it (§5.5). Idempotent — NotFound is success.
        let dir = self.dir.clone();
        Box::pin(async move {
            let _ = tokio::fs::remove_dir_all(&dir).await;
        })
    }
}

fn capped(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.len() <= OUTPUT_CAP {
        return text.into_owned();
    }
    let mut end = OUTPUT_CAP;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… [truncated {} bytes]", &text[..end], text.len() - end)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn open(root: &std::path::Path, session: &str) -> Arc<dyn Sandbox> {
        LocalSandboxes::new(root)
            .open(&SessionId::new(session), &SandboxProfile::default())
            .await
            .expect("open")
    }

    #[tokio::test]
    async fn shell_runs_in_the_session_workspace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sandbox = open(dir.path(), "s").await;
        let out = sandbox
            .call("shell", json!({"command": "echo hi > f && cat f"}))
            .await
            .expect("call");
        assert_eq!(out["exit_code"], 0);
        assert_eq!(out["stdout"], "hi\n");
        // The file landed inside this session's workspace, nowhere else.
        assert!(dir.path().join(sanitize("s")).join("f").exists());
    }

    #[tokio::test]
    async fn failures_are_outcomes_not_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sandbox = open(dir.path(), "s").await;
        let out = sandbox
            .call("shell", json!({"command": "exit 3"}))
            .await
            .expect("a failing command is still an outcome");
        assert_eq!(out["exit_code"], 3);
        let bad = sandbox.call("shell", json!({"command": 42})).await;
        assert!(matches!(bad, Err(ToolError::InvalidArguments(_))));
    }

    #[tokio::test]
    async fn output_is_capped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sandbox = open(dir.path(), "s").await;
        let out = sandbox
            .call(
                "shell",
                json!({"command": "head -c 100000 /dev/zero | tr '\\0' 'x'"}),
            )
            .await
            .expect("call");
        let stdout = out["stdout"].as_str().expect("stdout");
        assert!(stdout.len() < 2 * OUTPUT_CAP);
        assert!(stdout.contains("[truncated"));
    }

    #[tokio::test]
    async fn release_removes_the_workspace_and_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sandbox = open(dir.path(), "s").await;
        sandbox
            .call("shell", json!({"command": "touch f"}))
            .await
            .expect("call");
        sandbox.release().await;
        assert!(!dir.path().join(sanitize("s")).exists());
        sandbox.release().await;
    }
}
