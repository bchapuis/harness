//! The identifiers the specifications name, checked against the tree.
//!
//! The specs are written in terms of the code: `EntityHandler`, `ReadReply::head`,
//! `load_snapshot()`. Three hundred-odd such names across the set, and a rename
//! moves the code while the prose keeps the old one: the same failure the
//! citation gate catches for sections, one level down.
//!
//! This module indexes every item name the tree declares and reports the names a
//! spec uses that the index does not hold.
//!
//! # Why the index is a scan and not rustdoc
//!
//! `cargo rustdoc --output-format json` would resolve names properly (with
//! types, paths, and visibility), but it needs nightly, and `rust-toolchain.toml`
//! pins a stable release on purpose (the trybuild snapshots are byte-compared
//! against one rustc). So the index is a scan, in the manner the wal catalogue's
//! `every_named_test_exists` already uses: it knows a name exists somewhere, not
//! where it lives or what it is. That is enough for the failure being guarded
//! (a name that no longer exists at all), and no more.
//!
//! # What is checked
//!
//! Only backticked tokens of a shape the tree would own:
//!
//! - a type-like `Foo`, at least two characters, so a generic `T` is not a claim;
//! - a generic `Foo<T>`, checked on its head;
//! - `Foo::bar`, checked on the member;
//! - `some_crate::Foo`, checked on the type;
//! - `some_crate::item` or `some_crate::item()`, checked on the item;
//! - `foo_bar()`, checked on the name.
//!
//! Everything else in backticks (flags, paths, shell, JSON, prose in code voice)
//! is left alone.
//!
//! A fenced ```` ```rust ```` block is read as code rather than prose. Its
//! own declarations are in scope (an example's `struct Account` is not a claim
//! about the tree), and every other type-like name, `Foo::bar` path, and
//! `foo(...)` call in it is held to the index the same way a backticked token
//! is. Comments, string literals, attributes, macro names, and method calls
//! after a `.` are skipped: the first three are prose, and the last two are
//! mostly `std`'s. Other fence languages are diagrams and shell, and stay
//! prose.
//!
//! # Why there is an exemption list, and why it cannot rot
//!
//! Some names a spec uses are correct and will never be in the index. They come in
//! three kinds, and [`EXEMPT`] records each with its reason:
//!
//! - **Not this tree's**: `Option`, `HashMap`, `Serialize`, Akka's `ClusterClient`.
//! - **Not an identifier**: the `TODO` the docs promise you will not find, SQL's
//!   `TRUNCATE`.
//! - **Named to say it does not exist**: the agent runtime "mints no `RunResumed` event
//!   of its own". This kind is the reason a plain "every name must resolve" rule
//!   would be wrong: a specification says what a system does *not* have, in the
//!   same voice it says what it does.
//!
//! An exemption is an assertion that a name is absent, so it is checked in that
//! direction too: [`stale`] reports any exemption the tree has since started to
//! define. The list cannot quietly outlive its reasons, which is what separates it
//! from a suppression file.

use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::path::Path;

use crate::DOCUMENTS;
use crate::markdown;
use crate::markdown::Fence;

/// Why a name a spec uses is not in the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Why {
    /// Owned by `std`, a dependency, or another system entirely.
    Elsewhere,
    /// Not a Rust identifier: a table column, a SQL keyword, an env var.
    NotAnIdentifier,
    /// The specification names it to say the tree does not have it.
    DeclaredAbsent,
}

/// A name a specification uses that the tree does not define, and why that is
/// right.
#[derive(Debug, Clone, Copy)]
pub struct Exempt {
    /// The token as the specs write it.
    pub name: &'static str,
    /// Which kind of absence this is.
    pub why: Why,
    /// The reason, in enough words to judge whether it still holds.
    pub reason: &'static str,
}

/// Every name the specs use that the tree does not define.
///
/// Checked in both directions: a name here that the tree *starts* defining fails
/// [`stale`], so the list has to be revisited rather than accumulate.
pub const EXEMPT: &[Exempt] = &[
    // Not this tree's. Only the ones no crate imports need listing: the index
    // reads `use` trees, so it already holds `String` and `Dir`.
    e("Option", Why::Elsewhere, "std"),
    e("Result", Why::Elsewhere, "std"),
    e("HashMap", Why::Elsewhere, "std"),
    e("Copy", Why::Elsewhere, "std, a derive"),
    e("Default", Why::Elsewhere, "std, a derive"),
    e("Debug", Why::Elsewhere, "std, a derive"),
    e("Eq", Why::Elsewhere, "std, a derive"),
    e("Hash", Why::Elsewhere, "std, a derive"),
    e("Send", Why::Elsewhere, "std, an auto trait"),
    // The rest of `std` the rust fences spell out: bounds, closures, futures,
    // and `Duration`'s constructors in the config examples.
    e("Sized", Why::Elsewhere, "std, a bound in the trait fences"),
    e("Ord", Why::Elsewhere, "std, a bound on a workflow key"),
    e("Fn", Why::Elsewhere, "std, a closure bound"),
    e("FnMut", Why::Elsewhere, "std, a closure bound"),
    e("FnOnce", Why::Elsewhere, "std, a closure bound"),
    e("Future", Why::Elsewhere, "std, the seams' return type"),
    e("Item", Why::Elsewhere, "std, `Stream`'s associated type"),
    e("Range", Why::Elsewhere, "std, a blob `get`'s byte range"),
    e(
        "PhantomData",
        Why::Elsewhere,
        "std, in the receptionist `Key`",
    ),
    e(
        "Duration::from_secs",
        Why::Elsewhere,
        "std, in the config examples",
    ),
    e(
        "Duration::from_millis",
        Why::Elsewhere,
        "std, in the config examples",
    ),
    e(
        "DeserializeOwned",
        Why::Elsewhere,
        "serde, the serialization requirement's bound",
    ),
    e(
        "InvalidData",
        Why::Elsewhere,
        "std, the `io::ErrorKind` a `wal` open refusal rides",
    ),
    e("Serialize", Why::Elsewhere, "serde, a derive"),
    e("Deserialize", Why::Elsewhere, "serde, a derive"),
    e(
        "ChaCha8",
        Why::Elsewhere,
        "rand_chacha: the simulation's seeded generator",
    ),
    e(
        "ClusterClient",
        Why::Elsewhere,
        "Akka's, cited by multi-tenant-edge for the tradeoff the gateway takes",
    ),
    e(
        "unwrap()",
        Why::Elsewhere,
        "std, cited as a thing not to write",
    ),
    // Not identifiers.
    e(
        "TRUNCATE",
        Why::NotAnIdentifier,
        "SQL, in the SQL facet's discussion",
    ),
    e(
        "TODO",
        Why::NotAnIdentifier,
        "docs/README promises a sweep finds none; the word is the subject",
    ),
    e("FIXME", Why::NotAnIdentifier, "as TODO"),
    e("HACK", Why::NotAnIdentifier, "as TODO"),
    // Named to say the tree does not have it.
    e(
        "RunResumed",
        Why::DeclaredAbsent,
        "agent §10: a resume is recognized from Activated plus ModelCompleted, so \
         \"the agent runtime mints no RunResumed event of its own\"",
    ),
    e(
        "Quorum",
        Why::DeclaredAbsent,
        "entity §7.4 names the two durability tiers Local and Quorum; the code \
         distinguishes them by which Replicator is installed, not by a named variant",
    ),
    e(
        "MachineId",
        Why::DeclaredAbsent,
        "machine §1: a machine's identity is an EntityName, and MachineId is the spec's \
         word for it rather than a type",
    ),
];

const fn e(name: &'static str, why: Why, reason: &'static str) -> Exempt {
    Exempt { name, why, reason }
}

/// A name a specification uses that neither the tree nor [`EXEMPT`] accounts for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unknown {
    /// The [`crate::DocSpec::id`] of the citing document.
    pub from: &'static str,
    /// One-based line number.
    pub line: usize,
    /// The token as written, without its backticks.
    pub token: String,
    /// The part that was looked up.
    pub looked_up: String,
    /// Enough surrounding text to judge it.
    pub quote: String,
}

impl fmt::Display for Unknown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}: `{}`, nothing named `{}` is declared in crates/\n    {}",
            self.from, self.line, self.token, self.looked_up, self.quote,
        )
    }
}

/// Every item name declared anywhere under `crates/`.
///
/// Deliberately flat and deliberately generous: declarations, enum variants,
/// struct fields, method names, macro-body bare identifiers, and the all-caps
/// contents of string literals (a format magic like `GRSNAP` is a name a spec
/// cites and the code writes as bytes). Over-inclusion costs a missed rename;
/// under-inclusion costs a false alarm on every build, which is worse.
#[derive(Debug, Default)]
pub struct Index {
    names: BTreeSet<String>,
}

impl Index {
    /// Scan `crates/` under `root`, both `src/` and `tests/`.
    ///
    /// This crate is skipped. It is the tooling that reads the specifications
    /// rather than anything they describe, and its fixtures name things precisely
    /// because the tree does *not* have them, so indexing itself would answer its
    /// own questions.
    pub fn build(root: &Path) -> io::Result<Self> {
        let mut names = BTreeSet::new();
        for entry in std::fs::read_dir(root.join("crates"))? {
            let dir = entry?.path();
            if dir.file_name().is_some_and(|n| n == "spec") {
                continue;
            }
            for sub in ["src", "tests"] {
                collect(&dir.join(sub), &mut names)?;
            }
        }
        Ok(Self { names })
    }

    /// Whether anything under `crates/` declares this name.
    pub fn contains(&self, name: &str) -> bool {
        self.names.contains(name)
    }

    /// How many names were indexed.
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Whether the index is empty, which for a real tree means the scan failed.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

fn collect(dir: &Path, names: &mut BTreeSet<String>) -> io::Result<()> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(()); // a crate need not have tests/
    };
    for entry in entries {
        let path = entry?.path();
        if path.is_dir() {
            collect(&path, names)?;
        } else if path.extension().is_some_and(|x| x == "rs") {
            harvest(&std::fs::read_to_string(&path)?, names);
        }
    }
    Ok(())
}

/// Every name one source file declares, by the loosest reading that stays a name.
fn harvest(text: &str, names: &mut BTreeSet<String>) {
    for line in text.lines() {
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();

        // `pub struct Foo`, `fn bar`, `type Seq`, `const MAX`, wherever they sit.
        let mut words = trimmed.split_whitespace().peekable();
        while let Some(word) = words.next() {
            if matches!(
                word,
                "fn" | "struct" | "enum" | "trait" | "type" | "const" | "static" | "union" | "mod"
            ) && let Some(next) = words.peek()
            {
                // The name is what the word *starts* with: `EntityHandler<M>:` and
                // `shutdown(&self)` both carry their name in front of the
                // punctuation, not behind it.
                if let Some(name) = leading_ident(next) {
                    names.insert(name.to_string());
                }
            }
        }

        // An enum variant, or a name declared inside a macro body: indented,
        // capitalised, and either alone on its line or followed by a delimiter.
        if indent >= 2
            && let Some(name) = leading_ident(trimmed)
            && name.starts_with(|c: char| c.is_ascii_uppercase())
        {
            let tail = trimmed[name.len()..].trim();
            if tail.is_empty() || tail.starts_with(['{', '(', ',', '=', ':']) {
                names.insert(name.to_string());
            }
        }

        // A struct field: `pub head: Seq`, `name: String`. A lone `:` opens the
        // type; a `::` opens a path, which is a use rather than a declaration.
        let field = trimmed.strip_prefix("pub ").unwrap_or(trimmed);
        if let Some(name) = leading_ident(field) {
            let tail = &field[name.len()..];
            if tail.starts_with(':') && !tail.starts_with("::") {
                names.insert(name.to_string());
            }
        }
    }

    // A format magic lives in the bytes, not in a declaration: `b"GRSNAP"`. Only
    // byte strings count: an ordinary `"TRUNCATE"` is SQL a statement happens to
    // contain, and indexing it would answer for a name the tree does not define.
    let mut rest = text;
    while let Some(open) = rest.find("b\"") {
        let after = &rest[open + 2..];
        let Some(close) = after.find('"') else { break };
        let literal = &after[..close];
        if literal.len() >= 4
            && literal
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        {
            names.insert(literal.to_string());
        }
        rest = &after[close + 1..];
    }
}

fn leading_ident(text: &str) -> Option<&str> {
    let end = text
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(text.len());
    (end > 0 && !text.starts_with(|c: char| c.is_ascii_digit())).then(|| &text[..end])
}

fn is_ident(text: &str) -> bool {
    !text.is_empty()
        && !text.starts_with(|c: char| c.is_ascii_digit())
        && text.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The name a backticked token claims, if it claims one.
///
/// Returns the token's own text and the part to look up, which differ for a
/// qualified path.
pub fn claimed(token: &str) -> Option<(&str, &str)> {
    if let Some(name) = token.strip_suffix("()")
        && is_ident(name)
        && name.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
    {
        return Some((token, name));
    }
    // A generic, `Wal<T>` or `ClusterSystem<C, E, S, T>`: the head is the claim,
    // and the parameters are the citing spec's own letters.
    if let Some((head, _)) = token.split_once('<') {
        return type_like(head).then_some((token, head));
    }
    if let Some((left, right)) = token.split_once("::") {
        let right = right.trim_end_matches("()");
        if !is_ident(left) || !is_ident(right) {
            return None;
        }
        // `G::default` is a generic parameter's associated item, not a claim about
        // any type this tree declares.
        if left.len() < 2 {
            return None;
        }
        return match (
            left.starts_with(|c: char| c.is_ascii_uppercase()),
            right.starts_with(|c: char| c.is_ascii_uppercase()),
        ) {
            // `ReadReply::head`: the member is the part that moves.
            (true, false) => Some((token, right)),
            // `compat::Stamp`: a crate path; the type is the part that moves.
            (false, true) => Some((token, right)),
            // `entity::store`, `wal::checksum()`: a crate's module or function;
            // the item is the part that moves.
            (false, false) => Some((token, right)),
            (true, true) => None,
        };
    }
    type_like(token).then_some((token, token))
}

/// A type-like name: capitalised and at least two characters, so a generic
/// parameter `T` is not a claim.
fn type_like(text: &str) -> bool {
    text.len() >= 2
        && text.starts_with(|c: char| c.is_ascii_uppercase())
        && text.chars().all(|c| c.is_ascii_alphanumeric())
}

/// Every identifier the [`scanned`](crate::DocSpec::scanned) documents name that
/// nothing declares.
pub fn unknown(root: &Path, index: &Index) -> io::Result<Vec<Unknown>> {
    let exempt: BTreeSet<&str> = EXEMPT.iter().map(|x| x.name).collect();
    let mut out = Vec::new();

    for doc in DOCUMENTS.iter().filter(|d| d.scanned) {
        let path = root.join(doc.path);
        let text = std::fs::read_to_string(&path)
            .map_err(|e| io::Error::new(e.kind(), format!("reading {}: {e}", path.display())))?;

        // A rust fence is collected whole before it is checked, because an
        // example may use a name above the line that declares it. Other fences
        // are diagrams and shell, read as prose.
        let mut rust_block: Option<Vec<(usize, &str)>> = None;
        for line in markdown::lines(&text) {
            match line.fence {
                Fence::Opens(info) => {
                    if info == "rust" {
                        rust_block = Some(Vec::new());
                    }
                    continue;
                }
                Fence::Closes => {
                    if let Some(block) = rust_block.take() {
                        code_block(doc.id, &block, &exempt, index, &mut out);
                    }
                    continue;
                }
                Fence::Inside | Fence::Outside => {}
            }
            if let Some(block) = rust_block.as_mut() {
                block.push((line.index + 1, line.text));
                continue;
            }
            for token in markdown::backticked(line.text) {
                let Some((token, name)) = claimed(token) else {
                    continue;
                };
                if exempt.contains(token) || exempt.contains(name) || index.contains(name) {
                    continue;
                }
                out.push(Unknown {
                    from: doc.id,
                    line: line.index + 1,
                    token: token.to_string(),
                    looked_up: name.to_string(),
                    quote: quote_near(line.text, token),
                });
            }
        }
    }
    Ok(out)
}

/// Check one ```` ```rust ```` block: every name it uses is one it declares
/// itself, an exemption, or a name the tree declares.
fn code_block(
    from: &'static str,
    lines: &[(usize, &str)],
    exempt: &BTreeSet<&str>,
    index: &Index,
    out: &mut Vec<Unknown>,
) {
    let text = lines.iter().map(|(_, l)| *l).collect::<Vec<_>>().join("\n");
    let mut local = BTreeSet::new();
    harvest(&text, &mut local);
    let mut in_comment = false;
    for (n, line) in lines {
        let code = code_only(line, &mut in_comment);
        for (token, name) in code_claims(&code) {
            if local.contains(name)
                || exempt.contains(token)
                || exempt.contains(name)
                || index.contains(name)
            {
                continue;
            }
            out.push(Unknown {
                from,
                line: *n,
                token: token.to_string(),
                looked_up: name.to_string(),
                quote: quote_near(line, token),
            });
        }
    }
}

/// `line` with its string literals, its comments, and any attribute removed:
/// the parts of a code line that are prose, not names. `in_comment` carries a
/// `/* … */` comment across lines.
fn code_only(line: &str, in_comment: &mut bool) -> String {
    if line.trim_start().starts_with("#[") {
        return String::new();
    }
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if *in_comment {
            if c == '*' && chars.peek() == Some(&'/') {
                chars.next();
                *in_comment = false;
                out.push(' ');
            }
            continue;
        }
        match c {
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                *in_comment = true;
            }
            '"' => {
                // Skip to the closing quote, honouring a backslash escape.
                while let Some(d) = chars.next() {
                    if d == '\\' {
                        chars.next();
                    } else if d == '"' {
                        break;
                    }
                }
                out.push(' ');
            }
            '/' if chars.peek() == Some(&'/') => break,
            _ => out.push(c),
        }
    }
    out
}

/// The names one line of code claims: each `(token, looked up)` pair, in the
/// vocabulary of [`claimed`].
///
/// A token is a maximal run of identifier characters and `::`. A method call
/// (`x.foo(`) and a macro (`foo!`) are skipped; so is a lifetime. A path is
/// checked segment by segment: each type-like segment on its own, and a
/// lowercase segment that follows a type (`ReadReply::head`) or ends a call
/// (`wal::checksum(`) as a member or a function.
fn code_claims(code: &str) -> Vec<(&str, &str)> {
    let mut claims = Vec::new();
    let bytes = code.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if !(c.is_ascii_alphabetic() || c == '_') {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() {
            let c = bytes[i] as char;
            if c.is_ascii_alphanumeric() || c == '_' {
                i += 1;
            } else if code[i..].starts_with("::") {
                i += 2;
            } else {
                break;
            }
        }
        let token = code[start..i].trim_end_matches("::");
        let before = code[..start].chars().next_back();
        let after = code[i..].chars().next();
        if matches!(before, Some('.' | '\'')) || after == Some('!') {
            continue;
        }
        let called = after == Some('(');
        let segments: Vec<&str> = token.split("::").filter(|s| !s.is_empty()).collect();
        for (k, segment) in segments.iter().enumerate() {
            let type_like = segment.len() >= 2
                && segment.starts_with(|c: char| c.is_ascii_uppercase())
                && segment.chars().all(|c| c.is_ascii_alphanumeric());
            let after_type = k > 0
                && segments[k - 1].starts_with(|c: char| c.is_ascii_uppercase())
                && segments[k - 1].len() >= 2;
            let last = k + 1 == segments.len();
            let lowercase = segment.starts_with(|c: char| c.is_ascii_lowercase() || c == '_');
            if type_like || (lowercase && (after_type || (last && called))) {
                claims.push((token, *segment));
            }
        }
    }
    claims
}

/// Exemptions the tree has started to define, so their reason no longer holds.
pub fn stale(index: &Index) -> Vec<&'static Exempt> {
    EXEMPT
        .iter()
        .filter(|x| {
            let name = claimed(x.name).map_or(x.name, |(_, n)| n);
            index.contains(name)
        })
        .collect()
}

/// Enough of `line` around `token` to judge the citation by eye.
fn quote_near(line: &str, token: &str) -> String {
    let at = line.find(token).unwrap_or(0);
    markdown::excerpt(line, at, at + token.len(), 44)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn indexed(source: &str) -> Index {
        let mut names = BTreeSet::new();
        harvest(source, &mut names);
        Index { names }
    }

    #[test]
    fn a_declaration_is_indexed_whatever_its_visibility() {
        let index =
            indexed("pub struct EntityRef;\nfn fold(x: u8) {}\npub(crate) type Seq = u64;\n");
        assert!(index.contains("EntityRef"));
        assert!(index.contains("fold"));
        assert!(index.contains("Seq"));
    }

    #[test]
    fn an_enum_variant_is_indexed() {
        let index = indexed("pub enum Outcome {\n    Committed { seq: u64 },\n    NotLeader,\n}\n");
        assert!(index.contains("Committed"));
        assert!(index.contains("NotLeader"));
    }

    #[test]
    fn a_macro_declared_type_is_indexed() {
        // `TurnId` is declared inside a macro body, alone on its line.
        let index = indexed("id_type! {\n    /// A turn.\n    TurnId\n}\n");
        assert!(index.contains("TurnId"), "macro bodies declare names too");
    }

    #[test]
    fn a_struct_field_is_indexed() {
        let index = indexed("pub struct ReadReply {\n    pub head: Seq,\n}\n");
        assert!(index.contains("head"));
    }

    #[test]
    fn a_format_magic_is_indexed_from_its_bytes() {
        let index = indexed(r#"const MAGIC: &[u8] = b"GRSNAP";"#);
        assert!(index.contains("GRSNAP"), "a magic is a name the specs cite");
    }

    #[test]
    fn a_type_name_is_a_claim() {
        assert_eq!(
            claimed("EntityHandler"),
            Some(("EntityHandler", "EntityHandler"))
        );
    }

    #[test]
    fn a_single_letter_is_a_generic_parameter_not_a_claim() {
        assert_eq!(claimed("T"), None);
        assert_eq!(claimed("G::default"), None);
    }

    #[test]
    fn a_member_path_is_checked_on_its_member() {
        assert_eq!(
            claimed("ReadReply::head"),
            Some(("ReadReply::head", "head"))
        );
    }

    #[test]
    fn a_crate_path_is_checked_on_its_type() {
        assert_eq!(claimed("compat::Stamp"), Some(("compat::Stamp", "Stamp")));
    }

    #[test]
    fn a_crate_path_to_a_module_or_function_is_checked_on_the_item() {
        assert_eq!(claimed("entity::store"), Some(("entity::store", "store")));
        assert_eq!(
            claimed("wal::checksum()"),
            Some(("wal::checksum()", "checksum"))
        );
    }

    #[test]
    fn a_generic_is_checked_on_its_head() {
        assert_eq!(claimed("Wal<T>"), Some(("Wal<T>", "Wal")));
        assert_eq!(
            claimed("ClusterSystem<C, E, S, T>"),
            Some(("ClusterSystem<C, E, S, T>", "ClusterSystem"))
        );
        assert_eq!(claimed("T<U>"), None, "a one-letter head is a parameter");
    }

    #[test]
    fn a_call_is_checked_on_its_name() {
        assert_eq!(
            claimed("load_snapshot()"),
            Some(("load_snapshot()", "load_snapshot"))
        );
    }

    #[test]
    fn prose_in_code_voice_is_not_a_claim() {
        for token in [
            "--sandbox docker",
            "alpine:3.20",
            "127.0.0.1:8080",
            "cargo build",
            "text/event-stream",
            "storage.setAlarm()",
        ] {
            assert_eq!(claimed(token), None, "{token:?} is not an identifier claim");
        }
    }

    #[test]
    fn a_code_line_keeps_its_names_and_drops_its_prose() {
        let mut in_comment = false;
        let code = code_only(
            r#"let id = BlobId::of(b"GRSNAP"); // a comment naming `Missing`"#,
            &mut in_comment,
        );
        assert_eq!(code, "let id = BlobId::of(b ); ");
        assert!(code_only("#[derive(Debug, Clone)]", &mut in_comment).is_empty());

        let code = code_only("x /* the SAME TurnId", &mut in_comment);
        assert_eq!(code, "x ");
        assert!(in_comment);
        let code = code_only("across lines */ Seq::ZERO", &mut in_comment);
        assert_eq!(code, "  Seq::ZERO");
        assert!(!in_comment);
    }

    #[test]
    fn a_code_line_claims_types_paths_and_calls() {
        let claims = code_claims("fn fold(state: &mut Balance, e: &Ledger) -> Seq");
        assert_eq!(
            claims,
            vec![
                ("fold", "fold"),
                ("Balance", "Balance"),
                ("Ledger", "Ledger"),
                ("Seq", "Seq")
            ],
        );
        assert_eq!(
            code_claims("ReadReply::head(Duration::from_secs(2))"),
            vec![
                ("ReadReply::head", "ReadReply"),
                ("ReadReply::head", "head"),
                ("Duration::from_secs", "Duration"),
                ("Duration::from_secs", "from_secs"),
            ],
        );
        assert_eq!(
            code_claims("wal::checksum(&bytes)"),
            vec![("wal::checksum", "checksum")]
        );
        assert_eq!(
            code_claims("actor_core::Event::Undelivered"),
            vec![
                ("actor_core::Event::Undelivered", "Event"),
                ("actor_core::Event::Undelivered", "Undelivered"),
            ],
        );
    }

    #[test]
    fn a_code_line_skips_methods_macros_generics_and_lifetimes() {
        assert_eq!(
            code_claims("assert_eq!(x.clone().len(), 'a T)"),
            Vec::<(&str, &str)>::new()
        );
        assert_eq!(code_claims("let bytes = ctx.blobs().get(&id)"), vec![]);
        assert_eq!(
            code_claims("impl<S: EntitySystem> Foo<S>"),
            vec![("EntitySystem", "EntitySystem"), ("Foo", "Foo")]
        );
    }

    #[test]
    fn a_rust_fence_declares_its_own_names_and_claims_the_rest() {
        let index = indexed("pub struct ClusterSystem;\n");
        let exempt: BTreeSet<&str> = ["Option"].into_iter().collect();
        let block = [
            (1, "struct Account { cents: i64 }"),
            (2, "impl Entity for Account {"),
            (3, "    type System = ClusterSystem;"),
            (
                4,
                "    type State = Option<Balance>; // Balance is declared nowhere",
            ),
            (5, "}"),
        ];
        let mut out = Vec::new();
        code_block("test", &block, &exempt, &index, &mut out);
        let unknown: Vec<(usize, &str)> = out.iter().map(|u| (u.line, u.token.as_str())).collect();
        assert_eq!(unknown, vec![(2, "Entity"), (4, "Balance")]);
    }

    #[test]
    fn every_exemption_states_a_reason() {
        for x in EXEMPT {
            assert!(!x.reason.is_empty(), "{} has no reason", x.name);
        }
    }

    #[test]
    fn no_exemption_is_listed_twice() {
        let mut names: Vec<&str> = EXEMPT.iter().map(|x| x.name).collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "an exemption is listed twice");
    }

    #[test]
    fn an_exemption_the_tree_defines_is_stale() {
        let index = indexed("pub struct RunResumed;\n");
        let found = stale(&index);
        assert!(
            found.iter().any(|x| x.name == "RunResumed"),
            "an exemption must fail once its reason stops holding",
        );
    }
}
