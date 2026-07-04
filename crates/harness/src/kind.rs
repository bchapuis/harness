//! Kinds: the agent-type definitions (harness spec §7.1).
//!
//! A [`Kind`] is a named agent definition — system prompt, toolset, sandbox
//! profile, model parameters, default budget, delegation allowlist — plus the
//! [`GranaryConfig`] for the grain type it hosts. Code-and-config, agreed
//! cluster-wide like the codec: every node MUST register every kind (§7.1).
//!
//! The inheritance made literal (§2.2): a `KindId` **is** a grain type. The
//! harness hosts one `Agent` grain ([`crate::agent`]) under each kind's name via
//! `granary_named` ([`crate::client::Harness::builder`]), so each kind is its own
//! grain type — its own gateway, shard map, namespace, and `GranaryConfig` —
//! while sharing one run loop. A session of a kind is one grain of that type.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use granary::GranaryConfig;
use serde_json::Value;

use crate::budget::Budget;
use crate::model::ModelParams;
use crate::sandbox::SandboxProfile;
use crate::sandbox::Tier;
use crate::session::KindId;
use crate::session::content_digest;
use crate::tool::OnDangling;
use crate::tool::ToolDecl;
use crate::tool::ToolRegistry;

/// A named agent definition (harness spec §2.2, §7.1): system prompt, toolset,
/// sandbox profile, model parameters, default budget, delegation allowlist, and
/// the grain type's [`GranaryConfig`]. Every node MUST register every kind.
#[derive(Clone, Debug)]
pub struct Kind {
    pub system_prompt: String,
    pub params: ModelParams,
    pub tools: ToolRegistry,
    pub profile: SandboxProfile,
    pub default_budget: Budget,
    /// Child kinds this kind may delegate to (§8.1). Non-empty ⇒ the built-in
    /// `delegate` tool is in the model's toolset.
    pub delegates: Vec<KindId>,
    /// The grain type's configuration (§7.1, granary Appendix A): shard count,
    /// replication factor, idle window, snapshot policy. The harness passes it to
    /// `granary_named` when it hosts this kind's grain type.
    pub config: GranaryConfig,
}

impl Kind {
    /// Start a kind from its system prompt, with conservative defaults. The
    /// grain config defaults to granary's (`idle_after` ≈ 10s, §7.2).
    pub fn new(system_prompt: impl Into<String>) -> Kind {
        Kind {
            system_prompt: system_prompt.into(),
            params: ModelParams::default(),
            tools: ToolRegistry::new(),
            profile: SandboxProfile::default(),
            default_budget: Budget::new(100_000, 25),
            delegates: Vec::new(),
            config: GranaryConfig::default(),
        }
    }

    /// Set the model parameters.
    pub fn model(mut self, params: ModelParams) -> Kind {
        self.params = params;
        self
    }

    /// Declare a sandboxed tool (§5.2) at its required tier (§5.6), with the
    /// safe dangling policy (`Interrupt`, §5.5): on a crash-resume boundary the
    /// model, not the harness, decides whether to retry the side effect. An
    /// idempotent tool opts into blind re-execution via [`Kind::tool`]. The tier
    /// is explicit because it is digest-covered deployment configuration (§7.1):
    /// visible at the declaration site, never defaulted.
    pub fn sandboxed(
        mut self,
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: &Value,
        tier: Tier,
    ) -> Kind {
        self.tools.declare(ToolDecl {
            name: name.into(),
            description: description.into(),
            input_schema: input_schema.clone(),
            tier,
            on_dangling: OnDangling::Interrupt,
            timeout: None,
        });
        self
    }

    /// Declare a sandboxed tool with full control over its declaration.
    pub fn tool(mut self, decl: ToolDecl) -> Kind {
        self.tools.declare(decl);
        self
    }

    /// Permit delegation to the named kinds (§8.1): the allowlist a locked down
    /// kind cannot escalate past (§5.2 — naming any other kind is a synthesized
    /// `ToolError`).
    pub fn delegates_to(mut self, kinds: &[&str]) -> Kind {
        self.delegates = kinds.iter().map(|k| KindId::new(*k)).collect();
        self
    }

    /// Set the sandbox profile (§5.3 item 4).
    pub fn sandbox(mut self, profile: SandboxProfile) -> Kind {
        self.profile = profile;
        self
    }

    /// Set the default run budget (§9.1).
    pub fn budget(mut self, budget: Budget) -> Kind {
        self.default_budget = budget;
        self
    }

    /// Set the grain type's configuration (§7.1, granary Appendix A): the kind
    /// IS a grain type, so this is its shard/replication/idle/snapshot policy.
    pub fn grain(mut self, config: GranaryConfig) -> Kind {
        self.config = config;
        self
    }

    /// The effective tier cap (§5.3 item 4): the profile's explicit set, or the
    /// spec default — exactly the tiers the declared tools require.
    pub fn tier_cap(&self) -> BTreeSet<Tier> {
        self.profile
            .tier_cap
            .clone()
            .unwrap_or_else(|| self.tools.iter().map(|d| d.tier).collect())
    }

    /// A digest of the definition, pinned by `SessionCreated` (§7.1, §10.5) so a
    /// reader can tell whether a journal reconstruction is exact or merely
    /// indicative because a deployment changed the kind mid-session. Covers each
    /// tool's declared tier, the profile's effective cap (§5.6), and the grain
    /// config: what a session may acquire — and how its type is sharded — is
    /// cluster-wide agreement.
    ///
    /// The canonical form is length-prefixed (netstring-style) with a count
    /// before each variable-length list: no concatenation of two distinct
    /// definitions can collide, which bare juxtaposition cannot promise
    /// ("fo"+"obar" reads as "foo"+"bar").
    pub fn digest(&self) -> u64 {
        let mut canon = String::new();
        let mut frame = |field: &str| {
            canon.push_str(&field.len().to_string());
            canon.push(':');
            canon.push_str(field);
        };
        frame(&self.system_prompt);
        frame(&self.params.model);
        frame(&self.params.max_tokens.to_string());
        frame(&format!("tools={}", self.tools.iter().count()));
        for decl in self.tools.iter() {
            frame(&decl.name);
            frame(&decl.description);
            frame(&decl.input_schema.to_string());
            frame(&format!("{:?}", decl.tier));
            frame(&format!("{:?}", decl.on_dangling));
            // Frame durations by their nanosecond count, not `Debug`: `Duration`'s
            // Debug format is a std detail, but this digest is journaled and compared
            // cluster-wide (§7.1), so it must not shift under a std upgrade.
            frame(&decl.timeout.map_or_else(
                || "none".to_string(),
                |d| d.as_nanos().to_string(),
            ));
        }
        frame(&self.profile.image);
        let cap = self.tier_cap();
        frame(&format!("cap={}", cap.len()));
        for tier in cap {
            frame(&format!("{tier:?}"));
        }
        frame(&format!("egress={}", self.profile.egress.len()));
        for host in &self.profile.egress {
            frame(host);
        }
        frame(&self.profile.compute.memory_bytes.to_string());
        frame(&self.profile.compute.fuel.to_string());
        frame(&self.default_budget.tokens.to_string());
        frame(&self.default_budget.steps.to_string());
        frame(&self.config.shards.to_string());
        frame(&self.config.replication_factor.to_string());
        frame(&self.config.snapshot_every.to_string());
        frame(&self.config.idle_after.as_nanos().to_string());
        frame(&format!("delegates={}", self.delegates.len()));
        for kind in &self.delegates {
            frame(kind.as_str());
        }
        content_digest(&canon)
    }
}

/// The cluster-wide `KindId → Kind` map (harness spec §7.1), identical on every
/// node. The harness hosts one grain type per registered kind (§2.2).
#[derive(Clone, Debug, Default)]
pub struct Kinds {
    map: BTreeMap<KindId, Arc<Kind>>,
}

impl Kinds {
    pub fn new() -> Kinds {
        Kinds::default()
    }

    /// Register a kind under its name. Builder-style, used at deployment
    /// configuration time.
    ///
    /// Panics when a declared tool's tier falls outside the kind's tier cap
    /// (§5.3 item 4): a deployment configuration error, surfaced here as loudly
    /// as a duplicate tool name — never discovered at dispatch. The loop performs
    /// no runtime cap check: the cap is unreachable by construction (§5.6,
    /// sandbox spec S4).
    pub fn register(mut self, name: &str, kind: Kind) -> Kinds {
        let cap = kind.tier_cap();
        for decl in kind.tools.iter() {
            assert!(
                cap.contains(&decl.tier),
                "kind '{name}': tool '{}' declares tier {:?} outside the tier cap {:?}",
                decl.name,
                decl.tier,
                cap
            );
        }
        self.map.insert(KindId::new(name), Arc::new(kind));
        self
    }

    /// The definition for `kind`, if this deployment registers it (§7.1).
    pub fn get(&self, kind: &KindId) -> Option<Arc<Kind>> {
        self.map.get(kind).cloned()
    }

    /// Every registered `(KindId, Kind)`, for hosting one grain type per kind.
    pub fn iter(&self) -> impl Iterator<Item = (&KindId, &Arc<Kind>)> {
        self.map.iter()
    }
}
