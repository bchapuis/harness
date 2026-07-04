//! Budgets and spend accounting (harness spec §9.1).
//!
//! Every run has a [`Budget`]; spend is the model-**reported** usage summed
//! over the run's journaled calls, plus the carve-outs of its delegations.
//! Enforcement is pre-call: a model call is issued only while spend is below
//! the budget, with `max_tokens` clamped to the remainder — so output
//! overshoot is zero and total overshoot is bounded by one call's input
//! (invariant H4). The bound is compositional with **no cross-node accounting
//! protocol**: a delegation reserves an explicit slice of the parent's
//! remainder and the child enforces its slice locally, so
//! `own spend + Σ carve-outs ≤ budget` holds at every node of the tree.

use serde::Deserialize;
use serde::Serialize;

/// A run's spending limit (harness spec §9.1): model tokens (input plus
/// output, as the model reports them) and loop steps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budget {
    pub tokens: u64,
    pub steps: u32,
}

impl Budget {
    pub const fn new(tokens: u64, steps: u32) -> Budget {
        Budget { tokens, steps }
    }
}

/// Model-reported token usage for one call (harness spec §4.1). The harness
/// never counts tokens itself: spend is whatever the model reports, journaled
/// with the response it belongs to (§9.1.4).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

impl Usage {
    /// Total tokens this call cost, the unit budgets are stated in.
    pub fn total(&self) -> u64 {
        self.input_tokens + self.output_tokens
    }
}

/// A run's journaled spend so far (harness spec §9.1): own model usage plus
/// the slices carved out for children. Folded from the journal — the fold is
/// the accountant, so a resumed run resumes its accounting too (H1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spend {
    /// Tokens of this run's own journaled model calls.
    pub own_tokens: u64,
    /// Tokens reserved for children (`ChildRun` carve-outs, §9.1 item 3).
    pub carved_tokens: u64,
    /// Steps consumed: this run's journaled model calls.
    pub own_steps: u32,
    /// Steps reserved for children.
    pub carved_steps: u32,
}

impl Spend {
    /// Total tokens charged against the budget.
    pub fn tokens(&self) -> u64 {
        self.own_tokens + self.carved_tokens
    }

    /// Total steps charged against the budget.
    pub fn steps(&self) -> u32 {
        self.own_steps + self.carved_steps
    }

    /// Tokens still spendable under `budget`, saturating at zero.
    pub fn remaining_tokens(&self, budget: &Budget) -> u64 {
        budget.tokens.saturating_sub(self.tokens())
    }

    /// Steps still spendable under `budget`, saturating at zero.
    pub fn remaining_steps(&self, budget: &Budget) -> u32 {
        budget.steps.saturating_sub(self.steps())
    }

    /// Whether another model call may be issued (harness spec §9.1 item 2):
    /// tokens remain above the configured `floor` and a step remains. The
    /// floor keeps the loop from paying a full input for a near-zero
    /// `max_tokens` call.
    pub fn allows_call(&self, budget: &Budget, floor: u64) -> bool {
        self.remaining_tokens(budget) > floor && self.remaining_steps(budget) > 0
    }

    /// Clamp a child's requested carve-out to this run's remainder (harness
    /// spec §9.1 item 3): the slice can never exceed what the parent has left,
    /// which is what makes the tree bound compositional (H4).
    pub fn carve(&self, budget: &Budget, requested: Budget) -> Budget {
        Budget {
            tokens: requested.tokens.min(self.remaining_tokens(budget)),
            steps: requested.steps.min(self.remaining_steps(budget)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_call_enforcement_floors_and_steps() {
        let budget = Budget::new(1_000, 2);
        let mut spend = Spend::default();
        assert!(spend.allows_call(&budget, 0));
        spend.own_tokens = 999;
        assert!(spend.allows_call(&budget, 0));
        assert!(!spend.allows_call(&budget, 1)); // remainder 1 is not above floor 1
        spend.own_steps = 2;
        assert!(!spend.allows_call(&budget, 0)); // steps exhausted
    }

    #[test]
    fn carve_clamps_to_remainder() {
        let budget = Budget::new(1_000, 10);
        let spend = Spend {
            own_tokens: 600,
            carved_tokens: 300,
            own_steps: 4,
            carved_steps: 4,
        };
        let carved = spend.carve(&budget, Budget::new(500, 5));
        assert_eq!(carved, Budget::new(100, 2));
    }
}
