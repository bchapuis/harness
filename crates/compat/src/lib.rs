//! The format-stamp vocabulary (compatibility spec §3).
//!
//! Every durable byte sequence in the tree, and every association, is produced by
//! one build and consumed by another. At each such place one question decides
//! safety: can the reader tell what it is looking at, and does it refuse when it
//! cannot? A **misparse** turns a version skew, which an operator diagnoses in a
//! minute, into silent corruption, which they cannot. So a format carries a
//! **stamp**, the reading build holds one **version** per format, and the two are
//! compared before any decoder runs. Equal, the body is decoded; anything else is
//! refused by name. There is no range, no negotiation, and no second decoder: a
//! build reads exactly the version it writes.
//!
//! # Seams
//!
//! This crate owns no format, reads no file, and depends on nothing in the tree, so
//! every format shares one definition of "readable" and one wording for the
//! refusal. It is a convention rather than a plug point: each format's owner (the
//! wire protocol, the Raft files, the entity store and snapshot, the `wal` header)
//! uses these types on both its write and its read path.
//!
//! - [`Format`] is the **policy**: the one version this build reads and writes at a
//!   named format, and [`Format::admit`], the verdict. Used alone where the version
//!   travels some other way, such as the association handshake or a log header.
//! - [`Stamp`] is the **byte layout**: a magic and a version written ahead of a
//!   body, for a format that puts bytes at rest.
//! - [`Incompatible`] is the refusal, and [`Version`] the number it compares.
//!
//! A decoder MUST admit the stamp before it decodes the body, as the first thing
//! it does with the bytes. Nothing in a body's encoding enforces this, so a reader
//! that decodes first compiles and misparses.
//!
//! # Mechanism
//!
//! A stamp is the magic, then the version as a little-endian `u16`, then the body.
//! [`Stamp::unstamp`] checks the length and the magic, then hands the version to
//! [`Format::admit`], and only then returns the body, so no caller can reach the
//! bytes behind a stamp it has not admitted. Every [`Incompatible`] carries the
//! format's name and both sides, and its `Display` is the one wording every
//! refusal in the tree reports.
//!
//! Specified by `docs/compatibility-spec.md`, which also holds the registry of
//! every format in the tree; `tests/registry.rs` holds that registry equal to
//! the formats the code declares.

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

/// A format's version.
///
/// Formats are versioned independently, so a bare `Version` says nothing: a wire
/// version 3 and a snapshot version 3 are unrelated. It is meaningful only
/// alongside the [`Format`] that names it, which is why every refusal in this
/// crate carries that name. It is compared for equality only: `admit` accepts one
/// version and refuses every other, so no ordering between versions is defined.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Version(pub u16);

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// One build's policy at one format: its stable dotted name and the single version
/// this build reads and writes there (compatibility spec §3).
///
/// The name travels in the same value as the version, so no refusal can be raised
/// without it.
///
/// Construct it `const`, one per format:
///
/// ```
/// use compat::Format;
/// const SNAPSHOT: Format = Format::new("entity.snapshot", 1);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Format {
    name: &'static str,
    version: Version,
}

impl Format {
    /// The format `name` at `version`.
    ///
    /// `name` is a stable, dotted name (`"actor.wire"`, `"entity.snapshot"`), the
    /// one the compatibility spec's registry (§4) lists.
    pub const fn new(name: &'static str, version: u16) -> Format {
        Format {
            name,
            version: Version(version),
        }
    }

    /// The format's name.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// The version this build reads and writes.
    pub const fn version(&self) -> Version {
        self.version
    }

    /// The verdict: accept `found` if it is this build's version, or refuse it by
    /// name (**V2**).
    ///
    /// # Errors
    ///
    /// [`Incompatible::Version`] when `found` is any other version.
    pub fn admit(&self, found: Version) -> Result<(), Incompatible> {
        if found == self.version {
            Ok(())
        } else {
            Err(Incompatible::Version {
                format: self.name,
                found,
                expected: self.version,
            })
        }
    }
}

/// A magic-prefixed version stamp for bytes at rest, and the format that judges it
/// (compatibility spec §3).
///
/// The stamp sits **outside** the body's own encoding, because a positional format
/// such as postcard cannot version itself: it has no field name to make optional
/// and no discriminant to grow. So any change to such a body, an enum variant
/// appended at the end included, is a new version (compatibility spec §3.3):
/// without one, an older build reads the new body as damage, which a log's
/// torn-tail rule drops silently, instead of refusing it by name. Reading the
/// version before any decoder runs is what lets a wrong-format input be refused
/// instead of misparsed (**V2**). The magic travels with the format in one value,
/// so a writer and a reader cannot disagree on it.
#[derive(Clone, Copy, Debug)]
pub struct Stamp {
    magic: &'static [u8],
    format: Format,
}

impl Stamp {
    /// The stamp writing `magic` ahead of `format`'s version.
    ///
    /// `magic` must not be a prefix of any other stamp's magic, and no unstamped
    /// bytes a reader can meet in this format's place (a headerless predecessor, a
    /// bare body of the same role) may begin with it. See `wal`'s header for the
    /// arithmetic that keeps a headerless log from ever reading as a stamped one.
    pub const fn new(magic: &'static [u8], format: Format) -> Stamp {
        assert!(!magic.is_empty(), "a stamp needs a magic to recognize");
        Stamp { magic, format }
    }

    /// The format this stamp judges against.
    pub const fn format(&self) -> &Format {
        &self.format
    }

    /// The magic this stamp writes and recognizes.
    pub const fn magic(&self) -> &'static [u8] {
        self.magic
    }

    /// Prefix `body` with the magic and this build's version.
    pub fn stamp(&self, body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.magic.len() + size_of::<u16>() + body.len());
        bytes.extend_from_slice(self.magic);
        bytes.extend_from_slice(&self.format.version.0.to_le_bytes());
        bytes.extend_from_slice(body);
        bytes
    }

    /// The body behind a stamped input, admitting the version first. Nothing
    /// downstream sees the body until the version is accepted.
    ///
    /// # Errors
    ///
    /// [`Incompatible::Unstamped`] when the magic does not match or the input is
    /// too short to carry a stamp; [`Incompatible::Version`] when the version is
    /// not this build's.
    pub fn unstamp<'a>(&self, bytes: &'a [u8]) -> Result<&'a [u8], Incompatible> {
        let head = self.magic.len() + size_of::<u16>();
        let unstamped = || Incompatible::Unstamped {
            format: self.format.name,
            expected: self.format.version,
        };
        if bytes.len() < head || !bytes.starts_with(self.magic) {
            return Err(unstamped());
        }
        let raw = bytes[self.magic.len()..head]
            .try_into()
            .map_err(|_| unstamped())?;
        self.format.admit(Version(u16::from_le_bytes(raw)))?;
        Ok(&bytes[head..])
    }
}

/// A format's refusal (**V2**): the bytes or the peer are not something this
/// build can read.
///
/// Every variant carries both sides, so the message tells an operator which end to
/// move rather than only that something is wrong. Callers propagate it, and may add
/// a path or a remedy, without branching on the variant or rewording it, so the
/// wording lives in one place. It never reads as damage: a skew's remedy (run the
/// matching build, keep the state) is the opposite of corruption's.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Incompatible {
    /// Not this format at all: the magic did not match, or the input was too short
    /// to carry a stamp.
    Unstamped {
        format: &'static str,
        expected: Version,
    },
    /// This format, at a version other than this build's.
    Version {
        format: &'static str,
        found: Version,
        expected: Version,
    },
}

impl fmt::Display for Incompatible {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Incompatible::Unstamped { format, expected } => write!(
                f,
                "{format}: not a stamped {format} record (this build reads {expected})"
            ),
            Incompatible::Version {
                format,
                found,
                expected,
            } => write!(f, "{format}: found {found}, this build reads {expected}"),
        }
    }
}

impl std::error::Error for Incompatible {}

#[cfg(test)]
mod tests {
    use super::*;

    const FORMAT: Format = Format::new("test.format", 3);
    const STAMP: Stamp = Stamp::new(b"TSTAMP", FORMAT);

    #[test]
    fn a_format_admits_its_own_version_and_refuses_every_other() {
        assert_eq!(FORMAT.admit(Version(3)), Ok(()));
        for other in [0, 2, 4, u16::MAX] {
            assert_eq!(
                FORMAT.admit(Version(other)),
                Err(Incompatible::Version {
                    format: "test.format",
                    found: Version(other),
                    expected: Version(3),
                }),
            );
        }
    }

    #[test]
    fn a_stamp_round_trips_its_body() {
        let bytes = STAMP.stamp(b"body");
        assert_eq!(&bytes[..6], b"TSTAMP");
        assert_eq!(bytes[6..8], 3u16.to_le_bytes());
        assert_eq!(STAMP.unstamp(&bytes), Ok(&b"body"[..]));
    }

    #[test]
    fn another_version_is_refused_before_the_body_is_seen() {
        let mut bytes = STAMP.stamp(b"body");
        bytes[6..8].copy_from_slice(&4u16.to_le_bytes());
        assert_eq!(
            STAMP.unstamp(&bytes),
            Err(Incompatible::Version {
                format: "test.format",
                found: Version(4),
                expected: Version(3),
            }),
        );
    }

    #[test]
    fn a_missing_or_foreign_magic_is_unstamped() {
        let unstamped = Err(Incompatible::Unstamped {
            format: "test.format",
            expected: Version(3),
        });
        assert_eq!(STAMP.unstamp(b""), unstamped);
        assert_eq!(STAMP.unstamp(b"TSTAM"), unstamped);
        assert_eq!(STAMP.unstamp(b"OTHER\x03\x00body"), unstamped);
    }

    #[test]
    fn a_refusal_names_the_format_and_both_versions() {
        let skew = Incompatible::Version {
            format: "test.format",
            found: Version(9),
            expected: Version(3),
        }
        .to_string();
        assert!(skew.contains("test.format") && skew.contains("v9") && skew.contains("v3"));

        let unstamped = Incompatible::Unstamped {
            format: "test.format",
            expected: Version(3),
        }
        .to_string();
        assert!(unstamped.contains("test.format") && unstamped.contains("not a stamped"));
    }
}
