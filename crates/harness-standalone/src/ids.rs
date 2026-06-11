//! Filesystem-safe names for session ids.
//!
//! Session ids are arbitrary strings — delegation derives child ids like
//! `parent/turn/call` (`harness::session::derive_child`), so they contain
//! `/` and anything else a caller picks. Both the file journal and the
//! workspace sandbox key directories by session, and two distinct ids must
//! never map to one directory: merged journals would fence against each
//! other, and merged workspaces would leak state across sessions. The digest
//! suffix makes the mapping injective; the sanitized prefix keeps the
//! directory listing readable.

use harness::session::content_digest;

/// How much of the raw id is kept as the readable prefix.
const PREFIX: usize = 80;

/// A filesystem-safe directory name for `id`: a sanitized, truncated prefix
/// plus a digest of the full raw id.
pub fn sanitize(id: &str) -> String {
    let prefix: String = id
        .chars()
        .take(PREFIX)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{prefix}-{:016x}", content_digest(id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_ids_never_share_a_directory() {
        // The sanitized prefixes collide; the digest suffix must not.
        assert_ne!(sanitize("a/b"), sanitize("a_b"));
        assert_ne!(sanitize("a:b"), sanitize("a/b"));
        // Long ids differing only past the prefix still map apart.
        let long_a = format!("{}x", "s".repeat(120));
        let long_b = format!("{}y", "s".repeat(120));
        assert_ne!(sanitize(&long_a), sanitize(&long_b));
    }

    #[test]
    fn names_are_filesystem_safe() {
        let name = sanitize("research/t-1/tu_42");
        assert!(
            name.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        );
    }
}
