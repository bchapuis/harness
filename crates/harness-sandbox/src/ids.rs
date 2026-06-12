//! Filesystem-safe names for session ids (cf. `harness-standalone`'s
//! identical helper).
//!
//! Session ids are arbitrary strings — delegation derives child ids like
//! `parent/turn/call` — and two distinct ids must never map to one
//! directory: merged workspaces would leak state across sessions, the very
//! thing the seam exists to prevent (harness spec H8). The digest suffix
//! makes the mapping injective; the sanitized prefix keeps the directory
//! listing readable.

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
