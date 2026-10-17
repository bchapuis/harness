//! The agent runtime's tuning (agent spec §5.2, §7.1): identical on every node,
//! read by the client view and by the pure desired set alike.

use std::time::Duration;

/// Harness tuning (agent spec §5.2, §7.1): the few knobs the spec calls
/// configurable. Idle/snapshot/shard policy is per-kind `EntityConfig` (§5.1),
/// not here; durability retries are the entity's.
#[derive(Clone, Debug)]
pub struct HarnessConfig {
    /// Default deadline bounding a caller's wait on `prompt` (§2.6), never the
    /// run, which continues unaffected when the caller times out.
    pub submit_deadline: Duration,
    /// Default per-tool execution bound (§4.3), overridable per
    /// declaration. SHOULD default to about 5 minutes. Timed against the virtual
    /// `Clock`, so the bound is deterministic under simulation.
    pub tool_timeout: Duration,
    /// The token floor below which no model call is issued (§7.1 item 2): a
    /// near-zero `max_tokens` call still pays its full input. Defaults to the
    /// default per-call `max_tokens` ([`ModelParams`](crate::ModelParams)), so a
    /// run stops rather than issue a final call that cannot fit a full-size
    /// response; set to 0 to disable the floor.
    pub budget_floor: u64,
    /// Floor of a delegating parent's total wait on a child run (§6.1): the
    /// wait is this floor plus [`child_wait_per_step`](Self::child_wait_per_step)
    /// × the child's carved `steps`, so even a zero-step carve gets a grace
    /// window for queueing and transport.
    pub child_wait_floor: Duration,
    /// Per-step allowance in the child wait (§6.1). The carve's `steps` bound
    /// the whole subtree's model calls (§7.1 item 3), so steps are the right
    /// scale for how long a child may legitimately take; the allowance covers
    /// one model call plus its tool round, so it SHOULD be at least the tool
    /// timeout.
    pub child_wait_per_step: Duration,
}

impl HarnessConfig {
    /// A delegating parent's total wait on a child run (§6.1), scaled to the
    /// child's carved budget: `child_wait_floor + child_wait_per_step × steps`.
    /// Past it the delegation resolves as a `ToolError` for the parent's model;
    /// the child run continues, its budget the backstop (§7.2 item 3).
    pub fn child_wait(&self, budget: &crate::Budget) -> Duration {
        self.child_wait_floor
            .saturating_add(self.child_wait_per_step.saturating_mul(budget.steps))
    }
}

impl Default for HarnessConfig {
    fn default() -> Self {
        HarnessConfig {
            submit_deadline: Duration::from_secs(30),
            tool_timeout: Duration::from_secs(300),
            budget_floor: crate::ModelParams::default().max_tokens,
            child_wait_floor: Duration::from_secs(600),
            child_wait_per_step: Duration::from_secs(300),
        }
    }
}
