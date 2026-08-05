//! The compatibility-boundary vocabulary (compatibility spec §2).
//!
//! A **compatibility boundary** is any place bytes produced by one build are
//! consumed by another. Every boundary needs the same three things and no more —
//! a **stamp** (the bytes, or the peer, say which revision they are), a **window**
//! (this build declares which revisions it accepts and which it writes), and a
//! **verdict** (outside the window it refuses, by name, without parsing).
//!
//! The verdict carries the safety: a misparse turns a version skew, which an
//! operator diagnoses in a minute, into silent corruption, which they cannot.
//!
//! This crate owns no format, reads no file, and depends on nothing in the tree, so
//! every boundary in the workspace shares one definition of "compatible" and one
//! wording for the refusal.
//!
//! - [`Window`] is the **policy**: which revisions this build accepts, which one it
//!   writes. Used alone by a boundary with no bytes at rest, such as the
//!   association handshake.
//! - [`Stamp`] is the **byte layout**: a magic and a revision written ahead of a
//!   body, for a boundary that puts bytes on disk.
//! - [`Extensions`] is **headroom**: a tagged area a durable envelope carries so it
//!   can grow without a revision bump at all.
//!
//! A durable boundary that predates its own stamp needs one move more:
//! **adoption**. Bytes already on disk carry no magic, so a reader accepts both
//! forms — [`Stamp::is_stamped`] asks which one it is holding — reads a legacy file
//! with the legacy decoder, and lets the next ordinary write rewrite it stamped.
//! What adoption must not do is treat "no magic" and "a revision I refuse" alike:
//! the first is a predecessor and the second is a skew, and answering the second
//! with the old decoder is exactly the misparse the stamp was added to stop.

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

/// A format's revision at one compatibility boundary.
///
/// Boundaries are numbered independently (**V6**), so a bare `Version` says
/// nothing: a wire revision 3 and a snapshot revision 3 are unrelated. It is
/// meaningful only alongside the [`Window`] that names its boundary, which is why
/// every refusal in this crate carries that name.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct Version(pub u16);

impl From<u16> for Version {
    fn from(n: u16) -> Version {
        Version(n)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// An inclusive range of accepted revisions — one side of a compatibility check.
///
/// Kept apart from [`Window`], which also carries the revision its build *writes*:
/// a peer announces what it can read, never what it will write.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Accepted {
    pub lo: Version,
    pub hi: Version,
}

impl Accepted {
    /// The range accepting `lo..=hi`.
    pub const fn new(lo: u16, hi: u16) -> Accepted {
        Accepted {
            lo: Version(lo),
            hi: Version(hi),
        }
    }

    /// The range accepting exactly one revision — what a peer that announces a
    /// single version amounts to.
    pub const fn only(v: u16) -> Accepted {
        Accepted::new(v, v)
    }

    /// Whether `found` falls inside the range.
    pub const fn holds(&self, found: Version) -> bool {
        self.lo.0 <= found.0 && found.0 <= self.hi.0
    }
}

impl fmt::Display for Accepted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}..=v{}", self.lo.0, self.hi.0)
    }
}

/// The revisions one build accepts at one boundary, and the single revision it
/// writes there (compatibility spec §2).
///
/// Construct it `const`, one per boundary, so the invariant that a build reads
/// what it wrote (**V3**) is a build failure rather than a rollback that discovers
/// its own data is unreadable:
///
/// ```
/// use compat::Window;
/// const SNAPSHOT: Window = Window::new("granary.snapshot", 1, 1, 1);
/// ```
///
/// `writes` is a single revision, not a range, because a build that could write
/// two revisions would have to decide between them at every write — a decision
/// belonging to the release, not the call site.
#[derive(Clone, Copy, Debug)]
pub struct Window {
    boundary: &'static str,
    accepted: Accepted,
    writes: Version,
}

impl Window {
    /// The window accepting `lo..=hi` and writing `writes` at `boundary`.
    ///
    /// Panics unless `lo <= writes <= hi` (**V3**). In the `const` initializer this
    /// type is built for, that panic is a compile error: a build that cannot read
    /// what it writes must not link.
    ///
    /// `boundary` is a stable, dotted name for the format (`"actor.wire"`,
    /// `"granary.record"`).
    pub const fn new(boundary: &'static str, lo: u16, hi: u16, writes: u16) -> Window {
        assert!(lo <= writes, "a window must accept the revision it writes");
        assert!(writes <= hi, "a window must accept the revision it writes");
        Window {
            boundary,
            accepted: Accepted::new(lo, hi),
            writes: Version(writes),
        }
    }

    /// The window at a single revision: accepts only `revision`, writes it.
    ///
    /// A bump replaces this with `new`: widen to accept both revisions for a
    /// release, move `writes` in a later one (**V4**), and return here once the old
    /// revision leaves the support window.
    pub const fn at(boundary: &'static str, revision: u16) -> Window {
        Window::new(boundary, revision, revision, revision)
    }

    /// The boundary this window governs.
    pub const fn boundary(&self) -> &'static str {
        self.boundary
    }

    /// The revision this build stamps onto anything it writes.
    pub const fn writes(&self) -> Version {
        self.writes
    }

    /// The range to announce across the boundary — what this build can read.
    pub const fn accepted(&self) -> Accepted {
        self.accepted
    }

    /// The one-sided verdict, for bytes at rest: admit `found`, or refuse it by
    /// name (**V2**).
    ///
    /// # Errors
    ///
    /// [`Incompatible::Version`] when `found` lies outside the window.
    pub fn admit(&self, found: Version) -> Result<Version, Incompatible> {
        if self.accepted.holds(found) {
            Ok(found)
        } else {
            Err(Incompatible::Version {
                boundary: self.boundary,
                found,
                accepted: self.accepted,
            })
        }
    }

    /// The two-sided verdict, for a live peer: the highest revision both ends
    /// accept, or a refusal when the two ranges do not overlap.
    ///
    /// Both ends compute it from the same two ranges and so reach the same answer,
    /// which is why the handshake needs no confirmation round.
    ///
    /// # Errors
    ///
    /// [`Incompatible::Disjoint`] when no revision is acceptable to both ends.
    pub fn negotiate(&self, peer: Accepted) -> Result<Version, Incompatible> {
        let lo = self.accepted.lo.max(peer.lo);
        let hi = self.accepted.hi.min(peer.hi);
        if lo <= hi {
            Ok(hi)
        } else {
            Err(Incompatible::Disjoint {
                boundary: self.boundary,
                ours: self.accepted,
                theirs: peer,
            })
        }
    }
}

/// A magic-prefixed revision stamp for bytes at rest, and the window that judges
/// it (compatibility spec §2).
///
/// The stamp sits **outside** the body's own encoding, because a positional format
/// such as postcard cannot version itself. Reading the revision before any decoder
/// runs is what lets a wrong-format input be refused instead of misparsed (**V2**).
#[derive(Clone, Copy, Debug)]
pub struct Stamp {
    magic: &'static [u8],
    window: Window,
}

impl Stamp {
    /// The stamp writing `magic` ahead of `window`'s revision.
    ///
    /// `magic` should be chosen so the formats it might be confused with cannot
    /// begin with it — see `wal`'s header for the arithmetic that keeps a
    /// headerless log from ever reading as a stamped one.
    pub const fn new(magic: &'static [u8], window: Window) -> Stamp {
        assert!(!magic.is_empty(), "a stamp needs a magic to recognize");
        Stamp { magic, window }
    }

    /// The window this stamp judges against.
    pub const fn window(&self) -> &Window {
        &self.window
    }

    /// The magic this stamp writes and recognizes.
    pub const fn magic(&self) -> &'static [u8] {
        self.magic
    }

    /// Whether `bytes` carry this stamp's magic — the question an **adopting**
    /// reader asks before [`unstamp`](Stamp::unstamp), so an unstamped predecessor
    /// can be read by its own decoder while a stamped input at an unreadable
    /// revision is still refused (**V2**).
    ///
    /// Without it the two are one [`Incompatible::Unstamped`], and a reader that
    /// took that to mean "try the old decoder" would silently downgrade a refusal
    /// into a misparse — the failure the stamp exists to prevent.
    ///
    /// This is a question about the *format*, not about the shape of a verdict: a
    /// caller still propagates [`Incompatible`] unexamined.
    pub fn is_stamped(&self, bytes: &[u8]) -> bool {
        bytes.starts_with(self.magic)
    }

    /// Prefix `body` with the magic and the revision this build writes.
    pub fn stamp(&self, body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.magic.len() + size_of::<u16>() + body.len());
        bytes.extend_from_slice(self.magic);
        bytes.extend_from_slice(&self.window.writes.0.to_le_bytes());
        bytes.extend_from_slice(body);
        bytes
    }

    /// Split a stamped input into its revision and body, admitting the revision
    /// first. Nothing downstream sees the body until the revision is accepted.
    ///
    /// # Errors
    ///
    /// [`Incompatible::Unstamped`] when the magic does not match or the input is
    /// too short to carry a stamp; [`Incompatible::Version`] when the revision
    /// lies outside the window.
    pub fn unstamp<'a>(&self, bytes: &'a [u8]) -> Result<(Version, &'a [u8]), Incompatible> {
        let head = self.magic.len() + size_of::<u16>();
        let unstamped = || Incompatible::Unstamped {
            boundary: self.window.boundary,
            accepted: self.window.accepted,
        };
        if bytes.len() < head || !bytes.starts_with(self.magic) {
            return Err(unstamped());
        }
        let raw = bytes[self.magic.len()..head]
            .try_into()
            .map_err(|_| unstamped())?;
        let found = self.window.admit(Version(u16::from_le_bytes(raw)))?;
        Ok((found, &bytes[head..]))
    }
}

/// A tagged extension area, for a durable envelope that must be able to grow
/// without a revision bump (compatibility spec §2.1).
///
/// An extension area is only sound if unknown entries can be *safely* ignored, and
/// that is not true of every change. So criticality is carried in the key:
///
/// - a key with [`CRITICAL`](Extensions::CRITICAL) set MUST be understood — a reader
///   that does not know it refuses the whole input;
/// - any other key MAY be ignored, so a reader that predates it behaves exactly as
///   it did before.
///
/// This is PNG's ancillary/critical chunk rule. A change that alters how existing
/// bytes are interpreted is not an extension at all: bump the revision.
///
/// An empty area costs one byte.
#[derive(Clone, Default, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Extensions(Vec<(u16, Vec<u8>)>);

impl Extensions {
    /// Set in a key whose meaning a reader MUST understand to read the input at all.
    pub const CRITICAL: u16 = 0x8000;

    /// An empty area.
    pub fn new() -> Extensions {
        Extensions(Vec::new())
    }

    /// Whether the area carries nothing.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The value stored under `key`, if any.
    pub fn get(&self, key: u16) -> Option<&[u8]> {
        self.0
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.as_slice())
    }

    /// Store `value` under `key`, replacing any previous entry. Entries are kept in
    /// key order so an envelope's bytes depend on its content and not on the order
    /// its fields happened to be written.
    pub fn insert(&mut self, key: u16, value: Vec<u8>) {
        match self.0.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(at) => self.0[at].1 = value,
            Err(at) => self.0.insert(at, (key, value)),
        }
    }

    /// Refuse the input if it carries a **critical** key not in `known` (**V2**).
    ///
    /// Ancillary keys are not checked: not knowing one is the case this design
    /// exists to make harmless.
    ///
    /// # Errors
    ///
    /// [`Incompatible::UnknownCritical`] naming the first such key.
    pub fn admit(&self, boundary: &'static str, known: &[u16]) -> Result<(), Incompatible> {
        for (key, _) in &self.0 {
            if *key & Extensions::CRITICAL != 0 && !known.contains(key) {
                return Err(Incompatible::UnknownCritical {
                    boundary,
                    key: *key,
                });
            }
        }
        Ok(())
    }
}

/// A boundary's refusal (**V2**): the bytes or the peer are not something this
/// build can read.
///
/// Every variant carries both sides, so the message tells an operator which end to
/// move rather than only that something is wrong.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Incompatible {
    /// Not this format at all: the magic did not match, or the input was too short
    /// to carry a stamp.
    Unstamped {
        boundary: &'static str,
        accepted: Accepted,
    },
    /// Stamped, and outside this build's window.
    Version {
        boundary: &'static str,
        found: Version,
        accepted: Accepted,
    },
    /// Two ranges with no revision in common — the handshake case, where neither
    /// end is wrong and one of them has to move.
    Disjoint {
        boundary: &'static str,
        ours: Accepted,
        theirs: Accepted,
    },
    /// The revision is one this build reads, but the input carries an extension its
    /// writer marked as one a reader must understand ([`Extensions::CRITICAL`]).
    UnknownCritical { boundary: &'static str, key: u16 },
}

impl Incompatible {
    /// The boundary that refused, for a caller labelling its own error.
    pub fn boundary(&self) -> &'static str {
        match self {
            Incompatible::Unstamped { boundary, .. }
            | Incompatible::Version { boundary, .. }
            | Incompatible::Disjoint { boundary, .. }
            | Incompatible::UnknownCritical { boundary, .. } => boundary,
        }
    }
}

impl fmt::Display for Incompatible {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Incompatible::Unstamped { boundary, accepted } => write!(
                f,
                "{boundary}: unstamped input (no {boundary} magic); this build reads {accepted}"
            ),
            Incompatible::Version {
                boundary,
                found,
                accepted,
            } => write!(
                f,
                "{boundary}: found {found}, but this build reads {accepted}"
            ),
            Incompatible::Disjoint {
                boundary,
                ours,
                theirs,
            } => write!(
                f,
                "{boundary}: no shared revision — we read {ours}, the peer reads {theirs}"
            ),
            Incompatible::UnknownCritical { boundary, key } => write!(
                f,
                "{boundary}: carries required extension {key:#06x}, which this build \
                 does not implement"
            ),
        }
    }
}

impl std::error::Error for Incompatible {}

#[cfg(test)]
mod tests {
    use super::*;

    const WIRE: Window = Window::new("test.wire", 1, 3, 2);

    #[test]
    fn a_window_admits_its_range_and_refuses_outside_it() {
        assert_eq!(WIRE.admit(Version(1)), Ok(Version(1)));
        assert_eq!(WIRE.admit(Version(3)), Ok(Version(3)));
        let err = WIRE.admit(Version(4)).unwrap_err();
        assert!(matches!(err, Incompatible::Version { found, .. } if found == Version(4)));
        assert_eq!(
            err.to_string(),
            "test.wire: found v4, but this build reads v1..=v3"
        );
    }

    #[test]
    fn a_window_admits_what_it_writes() {
        // V3, the property `Window::new` asserts. Restated as a test because the
        // const assert only fires for the windows a build actually declares.
        assert_eq!(WIRE.admit(WIRE.writes()), Ok(WIRE.writes()));
    }

    #[test]
    fn negotiation_picks_the_highest_shared_revision() {
        assert_eq!(WIRE.negotiate(Accepted::new(2, 5)), Ok(Version(3)));
        assert_eq!(WIRE.negotiate(Accepted::new(1, 1)), Ok(Version(1)));
        assert_eq!(WIRE.negotiate(Accepted::only(2)), Ok(Version(2)));
    }

    #[test]
    fn negotiation_is_symmetric() {
        // Both ends compute the same answer from the same two ranges, so the
        // handshake needs no confirmation round.
        let peer = Window::new("test.wire", 2, 5, 4);
        assert_eq!(
            WIRE.negotiate(peer.accepted()),
            peer.negotiate(WIRE.accepted())
        );
    }

    #[test]
    fn disjoint_ranges_refuse_and_name_both_sides() {
        let err = WIRE.negotiate(Accepted::new(7, 9)).unwrap_err();
        assert_eq!(
            err.to_string(),
            "test.wire: no shared revision — we read v1..=v3, the peer reads v7..=v9"
        );
    }

    #[test]
    fn a_stamp_round_trips_its_body() {
        const STAMP: Stamp = Stamp::new(b"TESTMG", WIRE);
        let bytes = STAMP.stamp(b"payload");
        assert!(bytes.starts_with(b"TESTMG"), "the magic leads");
        let (found, body) = STAMP.unstamp(&bytes).unwrap();
        assert_eq!(found, WIRE.writes());
        assert_eq!(body, b"payload");
    }

    #[test]
    fn a_stamp_refuses_a_foreign_or_truncated_input() {
        const STAMP: Stamp = Stamp::new(b"TESTMG", WIRE);
        for input in [&b"OTHERMG\x01\x00body"[..], &b"TES"[..], &b""[..]] {
            assert!(matches!(
                STAMP.unstamp(input),
                Err(Incompatible::Unstamped { .. })
            ));
        }
    }

    #[test]
    fn a_stamp_refuses_an_unreadable_revision_without_exposing_the_body() {
        const STAMP: Stamp = Stamp::new(b"TESTMG", WIRE);
        let mut bytes = STAMP.stamp(b"payload");
        bytes[6..8].copy_from_slice(&9u16.to_le_bytes());
        let err = STAMP.unstamp(&bytes).unwrap_err();
        assert!(matches!(err, Incompatible::Version { found, .. } if found == Version(9)));
    }

    #[test]
    fn is_stamped_separates_a_foreign_input_from_an_unreadable_revision() {
        const STAMP: Stamp = Stamp::new(b"TESTMG", WIRE);
        assert!(
            !STAMP.is_stamped(b"{\"legacy\":true}"),
            "an unstamped predecessor is not this format, and adoption may read it"
        );
        let mut bytes = STAMP.stamp(b"payload");
        bytes[6..8].copy_from_slice(&9u16.to_le_bytes());
        assert!(
            STAMP.is_stamped(&bytes),
            "a revision this build refuses is still this format, and must not be adopted"
        );
        assert!(matches!(
            STAMP.unstamp(&bytes),
            Err(Incompatible::Version { .. })
        ));
    }

    #[test]
    fn at_is_the_single_revision_window() {
        const A: Window = Window::at("test.at", 3);
        assert_eq!(A.writes(), Version(3));
        assert_eq!(A.accepted(), Accepted::only(3));
    }

    #[test]
    fn an_empty_extension_area_costs_almost_nothing_and_admits() {
        let ext = Extensions::new();
        assert!(ext.is_empty());
        assert_eq!(ext.admit("test.ext", &[]), Ok(()));
    }

    #[test]
    fn an_unknown_ancillary_extension_is_ignored() {
        // The case the design exists for: a build predating the entry reads the
        // input exactly as it did before.
        let mut ext = Extensions::new();
        ext.insert(0x0001, vec![9]);
        assert_eq!(ext.admit("test.ext", &[]), Ok(()));
        assert_eq!(ext.get(0x0001), Some(&[9][..]));
        assert_eq!(ext.get(0x0002), None);
    }

    #[test]
    fn an_unknown_critical_extension_is_refused_by_name() {
        // The other half: an entry whose writer marked it must-understand cannot be
        // skipped, so it cannot smuggle meaning past an old reader.
        let mut ext = Extensions::new();
        ext.insert(Extensions::CRITICAL | 0x12, vec![]);
        let err = ext.admit("test.ext", &[]).unwrap_err();
        assert!(
            matches!(err, Incompatible::UnknownCritical { key, .. } if key == 0x8012),
            "expected an UnknownCritical refusal, got {err}"
        );
        assert_eq!(
            err.to_string(),
            "test.ext: carries required extension 0x8012, which this build does not implement"
        );
        assert_eq!(ext.admit("test.ext", &[0x8012]), Ok(()));
    }

    #[test]
    fn extensions_are_ordered_by_key_whatever_the_insertion_order() {
        // Durable bytes must depend on content, not on the order fields happened to
        // be written, or the same envelope hashes two ways.
        let mut a = Extensions::new();
        a.insert(0x0020, vec![2]);
        a.insert(0x0010, vec![1]);
        let mut b = Extensions::new();
        b.insert(0x0010, vec![1]);
        b.insert(0x0020, vec![2]);
        assert_eq!(a, b);
        a.insert(0x0010, vec![7]);
        assert_eq!(a.get(0x0010), Some(&[7][..]));
        assert_eq!(a.0.len(), 2);
    }
}
