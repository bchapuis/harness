//! Tenant authentication at the gateway edge: a bearer token names the principal
//! a request acts as (design: `docs/multi-tenant-edge.md`).
//!
//! The trust boundary is the gateway. A request presents a token in its
//! `Authorization: Bearer` header; the gateway verifies it here, at the edge,
//! never in an entity, whose fold must stay pure (entity §4.1), to a
//! [`PrincipalId`]. Every session that request then addresses is scoped under
//! that principal by [`scoped_session`], so one tenant can never name another's
//! entities.
//!
//! Two verifiers ship behind [`TokenVerifier`]: [`StaticTokens`] (the secure
//! mode) and [`InsecureTokens`] (loopback-only dev). Not implemented: a stateless
//! signed-token verifier (HMAC, JWT) behind the same seam.

use std::collections::HashMap;
use std::net::IpAddr;
use std::net::SocketAddr;

/// The tenant identity a request acts as.
///
/// The charset is deliberately narrow: ASCII alphanumerics plus `_`, `-`, `.`,
/// and excludes `/`, so a principal can prefix a session key as
/// `"{principal}/{session}"` ([`scoped_session`]) and the prefix splits back
/// unambiguously. That namespacing is what isolates one tenant's entities.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PrincipalId(String);

impl PrincipalId {
    /// Parse a principal id, accepting only `[A-Za-z0-9_.-]` and rejecting the
    /// empty string. `None` for anything else: notably a `/`, which would break
    /// the session-key prefixing the isolation relies on.
    pub fn parse(s: &str) -> Option<PrincipalId> {
        let ok = !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
        ok.then(|| PrincipalId(s.to_string()))
    }

    /// The principal id as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PrincipalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Whether a **client-supplied** session id is well formed: non-empty and free of
/// `/`.
///
/// Cross-tenant isolation does not depend on this: the principal prefix already
/// confines a client to its own namespace, whatever it sends ([`scoped_session`]).
/// The rule protects the namespace *inside* a tenant, by keeping a client's ids
/// one flat segment: a key a client supplies then splits back at the first `/`
/// exactly as [`unscope_session`] expects, and cannot be shaped to look like a
/// nested key of its own.
///
/// Only the client-supplied segment is checked; the internally-minted child ids that
/// legitimately contain `/` never pass through here.
pub fn is_valid_session(session: &str) -> bool {
    !session.is_empty() && !session.contains('/')
}

/// The entity session key for `session` under `principal`: the principal-prefixed
/// namespace that isolates tenants. The gateway always supplies the prefix (a
/// client only ever names the unprefixed `session`), so a client cannot address
/// an entity outside its own principal.
///
/// A delegated child's id is not a key in this shape at all: the agent derives it
/// by netstring-framing each component (`10:alice/demo/3:t-1/5:tu_42` under the
/// parent key `alice/demo`), which begins with digits and a `:`, characters a
/// [`PrincipalId`] excludes. So [`unscope_session`] answers `None` for it under
/// every principal, and no client can name one: children are reached only from
/// inside, by the agent that minted them.
pub fn scoped_session(principal: &PrincipalId, session: &str) -> String {
    format!("{}/{}", principal.as_str(), session)
}

/// The unprefixed session for a principal-scoped entity key, or `None` if the key
/// is not under this principal. The inverse of [`scoped_session`]: it strips the
/// `"{principal}/"` prefix the gateway added, so a listing hands a client back
/// the session ids it actually supplied.
pub fn unscope_session<'a>(principal: &PrincipalId, key: &'a str) -> Option<&'a str> {
    key.strip_prefix(principal.as_str())
        .and_then(|rest| rest.strip_prefix('/'))
}

/// Whether `host` is a loopback interface: the bind the insecure dev mode is
/// confined to. Accepts the common literals and any address that parses as a
/// loopback IP.
fn is_loopback(host: &str) -> bool {
    host == "localhost"
        || host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

/// Whether a `<host>:<port>` listen address binds a loopback interface: the
/// bind the insecure dev mode is confined to. The address is parsed as a
/// socket address first, so an IPv6 literal keeps its brackets together with
/// its port (`[::1]:8080`); a host that is not an IP literal (`localhost:8080`)
/// falls back to splitting the port off the end and testing the host.
pub fn bind_is_loopback(bind: &str) -> bool {
    match bind.parse::<SocketAddr>() {
        Ok(addr) => addr.ip().is_loopback(),
        Err(_) => is_loopback(bind.rsplit_once(':').map(|(host, _)| host).unwrap_or(bind)),
    }
}

/// Verify a bearer token, returning the principal it authenticates, or `None`
/// if the token is unknown or malformed. Runs at the gateway edge, so it may be
/// stateful and impure (unlike an entity fold).
pub trait TokenVerifier: Send + Sync {
    fn verify(&self, token: &str) -> Option<PrincipalId>;
}

/// The secure mode: a static map of opaque secret token → principal, supplied by
/// the operator (`--auth-tokens <file>`). A token is an API key (secret,
/// high-entropy, possession-grants-the-principal), so verification is a lookup.
#[derive(Clone, Debug, Default)]
pub struct StaticTokens {
    by_token: HashMap<String, PrincipalId>,
}

impl StaticTokens {
    /// Bind `token` to `principal`. A later bind of the same token wins.
    pub fn insert(&mut self, principal: PrincipalId, token: impl Into<String>) {
        self.by_token.insert(token.into(), principal);
    }

    /// Parse a tokens file: one entry per line as `<principal> <token>`
    /// (whitespace-separated), blank lines and `#` comments ignored. The
    /// principal must be a valid [`PrincipalId`]; the token is any non-empty,
    /// whitespace-free secret. A token reused across two principals is an error
    /// (it would make the principal ambiguous).
    pub fn parse(text: &str) -> Result<StaticTokens, String> {
        let mut tokens = StaticTokens::default();
        for (n, raw) in text.lines().enumerate() {
            let line = raw.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let mut parts = line.split_whitespace();
            let principal = parts.next().expect("non-empty line has a first field");
            let token = parts
                .next()
                .ok_or_else(|| format!("line {}: expected `<principal> <token>`", n + 1))?;
            if parts.next().is_some() {
                return Err(format!("line {}: trailing text after the token", n + 1));
            }
            let principal = PrincipalId::parse(principal)
                .ok_or_else(|| format!("line {}: invalid principal `{principal}`", n + 1))?;
            if tokens.by_token.contains_key(token) {
                return Err(format!("line {}: token reused across principals", n + 1));
            }
            tokens.insert(principal, token);
        }
        Ok(tokens)
    }
}

impl TokenVerifier for StaticTokens {
    fn verify(&self, token: &str) -> Option<PrincipalId> {
        self.by_token.get(token).cloned()
    }
}

/// The loopback-only dev mode: the token *is* the principal, with no secret at
/// all. Lets the local demo and tests pick a tenant by naming it.
#[derive(Clone, Copy, Debug, Default)]
pub struct InsecureTokens;

impl TokenVerifier for InsecureTokens {
    fn verify(&self, token: &str) -> Option<PrincipalId> {
        PrincipalId::parse(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_charset_excludes_the_key_separator() {
        assert!(PrincipalId::parse("alice").is_some());
        assert!(PrincipalId::parse("acme.corp_1-2").is_some());
        assert!(PrincipalId::parse("").is_none());
        // A `/` would let a client forge another tenant's prefix.
        assert!(PrincipalId::parse("a/b").is_none());
        assert!(PrincipalId::parse("a b").is_none());
    }

    #[test]
    fn a_client_session_id_may_not_contain_a_path_separator() {
        assert!(is_valid_session("demo"));
        assert!(is_valid_session("demo-1.2_3"));
        // A client's id stays one flat segment, so a key splits back where
        // `unscope_session` expects.
        assert!(!is_valid_session("demo/t-1/c-2"));
        assert!(!is_valid_session(""));
    }

    #[test]
    fn a_delegated_child_key_is_addressable_by_no_principal() {
        // The shape `derive_child` mints under the parent key `alice/demo`:
        // each component netstring-framed (agent spec §8.1).
        let child = "10:alice/demo/3:t-1/5:tu_42";
        let alice = PrincipalId::parse("alice").unwrap();
        // Not under alice, though her name appears inside it.
        assert_eq!(unscope_session(&alice, child), None);
        // And no principal can be parsed from its leading segment, because the
        // netstring length prefix carries a `:`.
        assert!(PrincipalId::parse("10:alice").is_none());
        // A client cannot send it either: it carries `/`.
        assert!(!is_valid_session(child));
    }

    #[test]
    fn scoped_session_prefixes_with_the_principal() {
        let alice = PrincipalId::parse("alice").unwrap();
        assert_eq!(scoped_session(&alice, "demo"), "alice/demo");
        // The prefix is textual: whatever the session segment holds rides under it.
        assert_eq!(scoped_session(&alice, "demo/t-1/c-2"), "alice/demo/t-1/c-2");
    }

    #[test]
    fn unscope_inverts_scope_and_rejects_other_principals() {
        let alice = PrincipalId::parse("alice").unwrap();
        assert_eq!(unscope_session(&alice, "alice/demo"), Some("demo"));
        assert_eq!(unscope_session(&alice, "alice/demo/t-1"), Some("demo/t-1"));
        // Another principal's key, or a near-miss prefix, is not ours.
        assert_eq!(unscope_session(&alice, "bob/demo"), None);
        assert_eq!(unscope_session(&alice, "alicia/demo"), None);
        assert_eq!(unscope_session(&alice, "alice"), None);
    }

    #[test]
    fn static_tokens_authenticate_only_known_secrets() {
        let tokens =
            StaticTokens::parse("# tenants\nalice  s3cret-alice\nbob\ts3cret-bob  # bob's key\n")
                .unwrap();
        assert_eq!(
            tokens.verify("s3cret-alice"),
            Some(PrincipalId::parse("alice").unwrap())
        );
        assert_eq!(
            tokens.verify("s3cret-bob"),
            Some(PrincipalId::parse("bob").unwrap())
        );
        assert_eq!(tokens.verify("guess"), None);
    }

    #[test]
    fn static_tokens_reject_a_reused_token() {
        let err = StaticTokens::parse("alice shared\nbob shared\n").unwrap_err();
        assert!(err.contains("reused"), "{err}");
    }

    #[test]
    fn static_tokens_reject_a_bad_principal() {
        assert!(StaticTokens::parse("a/b token").is_err());
    }

    #[test]
    fn insecure_tokens_take_the_token_as_the_principal() {
        assert_eq!(
            InsecureTokens.verify("alice"),
            Some(PrincipalId::parse("alice").unwrap())
        );
        // Still charset-validated, so it can prefix a key.
        assert_eq!(InsecureTokens.verify("a/b"), None);
    }

    #[test]
    fn loopback_detection() {
        assert!(is_loopback("127.0.0.1"));
        assert!(is_loopback("::1"));
        assert!(is_loopback("localhost"));
        assert!(!is_loopback("0.0.0.0"));
        assert!(!is_loopback("10.0.0.5"));
    }

    #[test]
    fn a_bind_keeps_an_ipv6_literal_with_its_port() {
        assert!(bind_is_loopback("127.0.0.1:8080"));
        assert!(bind_is_loopback("localhost:8080"));
        // The brackets belong to the address, not to a host/port split.
        assert!(bind_is_loopback("[::1]:8080"));
        assert!(!bind_is_loopback("[::]:8080"));
        assert!(!bind_is_loopback("0.0.0.0:8080"));
        assert!(!bind_is_loopback("10.0.0.5:8080"));
    }
}
