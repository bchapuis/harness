//! Kinds: the agent-type definitions (agent spec §5.1).
//!
//! A [`Kind`] is a named agent definition (system prompt, toolset, sandbox
//! profile, model parameters, default budget, delegation allowlist) plus the
//! [`EntityConfig`] for the entity type it hosts. Every node MUST register every
//! kind (§5.1).
//!
//! A `KindId` **is** an entity type (§3.1). The agent runtime hosts one `Agent` entity
//! ([`crate::agent`]) under each kind's name via `entities_named`
//! ([`crate::client::Harness::builder`]), so each kind is its own entity type
//! (its own gateway, shard map, namespace, and `EntityConfig`) while sharing
//! one run loop. A session of a kind is one entity of that type.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use entity::EntityConfig;
use serde_json::Value;

use crate::budget::Budget;
use crate::model::ModelParams;
use crate::sandbox::SandboxProfile;
use crate::sandbox::Tier;
use crate::session::KindId;
use crate::session::content_digest;
use crate::tool::OnOrphan;
use crate::tool::ToolDecl;
use crate::tool::ToolRegistry;

/// A named agent definition (agent spec §3.1, §5.1): system prompt, toolset,
/// sandbox profile, model parameters, default budget, delegation allowlist, and
/// the entity type's [`EntityConfig`]. Every node MUST register every kind.
#[derive(Clone, Debug)]
pub struct Kind {
    pub system_prompt: String,
    pub model: ModelParams,
    pub tools: ToolRegistry,
    pub sandbox: SandboxProfile,
    /// The budget of a turn that states none (§7.1).
    pub budget: Budget,
    /// Child kinds this kind may delegate to (§6.1). Non-empty ⇒ the built-in
    /// `delegate` tool is in the model's toolset.
    pub delegates: Vec<KindId>,
    /// The entity type's configuration (§5.1): shard count,
    /// replication factor, idle window, snapshot policy. The agent runtime passes it to
    /// `entities_named` when it hosts this kind's entity type.
    pub entity: EntityConfig,
}

impl Kind {
    /// Start a kind from its system prompt, with conservative defaults. The entity
    /// config defaults to entity's, whose idle window is minutes rather than seconds
    /// (§5.2), which suits a session, since the gap between turns is the user's
    /// reading time and a model call is itself seconds long.
    pub fn new(system_prompt: impl Into<String>) -> Kind {
        Kind {
            system_prompt: system_prompt.into(),
            model: ModelParams::default(),
            tools: ToolRegistry::new(),
            sandbox: SandboxProfile::default(),
            budget: Budget::new(100_000, 25),
            delegates: Vec::new(),
            entity: EntityConfig::default(),
        }
    }

    /// Set the model parameters.
    pub fn model(mut self, params: ModelParams) -> Kind {
        self.model = params;
        self
    }

    /// Declare a sandboxed tool (§2.4) at its required tier (§4.5), with the
    /// safe orphaned policy (`Interrupt`, §4.4): on a crash-resume boundary the
    /// model, not the agent runtime, decides whether to retry the side effect. An
    /// idempotent tool opts into blind re-execution via [`Kind::declare`]. The
    /// tier is explicit, never defaulted: it is digest-covered deployment
    /// configuration (§5.1).
    pub fn tool(
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
            on_orphan: OnOrphan::Interrupt,
            timeout: None,
        });
        self
    }

    /// Declare a sandboxed tool with full control over its declaration.
    pub fn declare(mut self, decl: ToolDecl) -> Kind {
        self.tools.declare(decl);
        self
    }

    /// Permit delegation to the named kinds (§6.1): naming any other kind is a
    /// synthesized `ToolError` (§2.4).
    pub fn delegates_to(mut self, kinds: &[&str]) -> Kind {
        self.delegates = kinds.iter().map(|k| KindId::new(*k)).collect();
        self
    }

    /// Set the sandbox profile (§2.4).
    pub fn sandbox(mut self, profile: SandboxProfile) -> Kind {
        self.sandbox = profile;
        self
    }

    /// Set the default run budget (§7.1).
    pub fn budget(mut self, budget: Budget) -> Kind {
        self.budget = budget;
        self
    }

    /// Set the entity type's shard/replication/idle/snapshot policy (§5.1).
    pub fn entity(mut self, config: EntityConfig) -> Kind {
        self.entity = config;
        self
    }

    /// The effective tier cap (§2.4): the profile's explicit set, or the
    /// spec default; exactly the tiers the declared tools require.
    pub fn tier_cap(&self) -> BTreeSet<Tier> {
        self.sandbox
            .tier_cap
            .clone()
            .unwrap_or_else(|| self.tools.iter().map(|d| d.tier).collect())
    }

    /// A digest of the definition, pinned by `SessionCreated` (§5.1, §8.4) so a
    /// reader can tell whether a journal reconstruction is exact or merely
    /// indicative because a deployment changed the kind mid-session. Covers each
    /// tool's declared tier, the profile's effective cap (§4.5), and the entity
    /// config.
    ///
    /// The canonical form is length-prefixed (netstring-style) with a count
    /// before each variable-length list, so no two distinct definitions collide
    /// the way bare juxtaposition allows ("fo"+"obar" reads as "foo"+"bar").
    pub fn digest(&self) -> u64 {
        let mut canon = String::new();
        let mut frame = |field: &str| crate::session::frame_field(&mut canon, field);
        frame(&self.system_prompt);
        frame(&self.model.model);
        frame(&self.model.max_tokens.to_string());
        frame(&format!("tools={}", self.tools.iter().count()));
        for decl in self.tools.iter() {
            frame(&decl.name);
            frame(&decl.description);
            frame(&decl.input_schema.to_string());
            frame(&format!("{:?}", decl.tier));
            frame(&format!("{:?}", decl.on_orphan));
            // Frame durations by their nanosecond count, not `Debug`: `Duration`'s
            // Debug format is a std detail, but this digest is journaled and compared
            // cluster-wide (§5.1), so it must not shift under a std upgrade.
            frame(
                &decl
                    .timeout
                    .map_or_else(|| "none".to_string(), |d| d.as_nanos().to_string()),
            );
        }
        frame(&self.sandbox.image);
        let cap = self.tier_cap();
        frame(&format!("cap={}", cap.len()));
        for tier in cap {
            frame(&format!("{tier:?}"));
        }
        frame(&format!("egress={}", self.sandbox.egress.len()));
        for host in &self.sandbox.egress {
            frame(host);
        }
        frame(&self.sandbox.compute.memory_bytes.to_string());
        frame(&self.sandbox.compute.fuel.to_string());
        frame(&self.budget.tokens.to_string());
        frame(&self.budget.steps.to_string());
        frame(&self.entity.shards.to_string());
        frame(&self.entity.replication_factor.to_string());
        frame(&self.entity.snapshot_every.to_string());
        frame(&self.entity.idle_after.as_nanos().to_string());
        frame(&format!("delegates={}", self.delegates.len()));
        for kind in &self.delegates {
            frame(kind.as_str());
        }
        content_digest(&canon)
    }
}

/// The cluster-wide `KindId → Kind` map (agent spec §5.1), identical on every
/// node. The agent runtime hosts one entity type per registered kind (§3.1).
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
    /// (§2.4): a deployment configuration error, never discovered at
    /// dispatch. The loop performs no runtime cap check: the cap is unreachable
    /// by construction (§4.5, sandbox spec S4).
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

    /// The definition for `kind`, if this deployment registers it (§5.1).
    pub fn get(&self, kind: &KindId) -> Option<Arc<Kind>> {
        self.map.get(kind).cloned()
    }

    /// Every registered `(KindId, Kind)`, for hosting one entity type per kind.
    pub fn iter(&self) -> impl Iterator<Item = (&KindId, &Arc<Kind>)> {
        self.map.iter()
    }
}
