//! The KV facet (spec §7.13): an ordered map of small values on the grain.
//!
//! The smallest **logical facet** (§7.12): its records are `Put`/`Delete`
//! operations under the kv tag, folded into an ordered map; its
//! composite-snapshot contribution is the encoded map; its blob roots are the
//! ids of **spilled** values. A grain declares it with `type Facets = (Kv, …)`
//! and reaches it through [`GrainCtx::kv`](crate::GrainCtx::kv) — configuration,
//! refs, cursors, indices, without designing an event vocabulary for them.
//!
//! **Staging (§7.12).** Writes stage in a per-command overlay: reads through the
//! handle see committed-plus-staged (read-your-staged-writes), and the staged
//! operations become the command's tagged records — committed atomically with
//! the grain's events and every other facet's records (G19), or dropped for
//! free.
//!
//! **Transparent spill.** A value above [`INLINE_MAX`] is stored in the grain's
//! blob area (durable on a write quorum *before* the map references it, the
//! §7.10 discipline) and the map keeps only its [`BlobId`]; `get` fetches it
//! back, verified by content (G17). Spilled ids are the facet's root set (F3),
//! so a deleted or overwritten key's spilled bytes become orphans reclaimed by
//! the next [`GrainBlobs::gc`](crate::GrainBlobs::gc) sweep — which unions the
//! facet roots automatically, so a sweep can never drop a *live* spilled value.
//!
//! **Why native, not sugar over SQL (§7.13).** Durable Objects backs its KV API
//! with the object's SQLite database; the facet model permits that convergence
//! later without API change. A native logical facet adds no C dependency, holds
//! its form in memory, and runs unchanged under deterministic simulation (§14).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::blobs::BlobId;
use crate::blobs::GrainBlobs;
use crate::error::GrainError;
use crate::facet::Facet;
use crate::facet::FacetCell;
use crate::facet::FacetEnv;
use crate::facet::FacetError;
use crate::facet::HasFacet;
use crate::facet::decode_payload;
use crate::facet::encode_payload;
use crate::facet::sealed::Sealed;
use crate::grain::Grain;
use crate::grain::GrainCtx;

/// Values at or below this many bytes are stored inline in the map; larger
/// values spill to the grain's blob area (spec §7.13). The bound keeps the
/// composite snapshot small (the map is re-encoded whole at every snapshot)
/// while sparing small values a blob round-trip.
pub const INLINE_MAX: usize = 64 << 10; // 64 KiB

/// The KV facet marker (spec §7.13): declare `type Facets = (Kv, …)` and reach
/// the map through [`GrainCtx::kv`](crate::GrainCtx::kv).
pub struct Kv;

impl Sealed for Kv {}

/// One stored value: inline bytes, or a content id of spilled bytes plus their
/// length (so `len` needs no blob fetch).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum KvValue {
    Inline(Vec<u8>),
    Spilled { id: BlobId, len: u64 },
}

impl KvValue {
    fn len(&self) -> u64 {
        match self {
            KvValue::Inline(bytes) => bytes.len() as u64,
            KvValue::Spilled { len, .. } => *len,
        }
    }
}

/// One kv record (spec §7.13): the unit of durable change under the kv tag.
/// Encoded with `postcard` — facet payloads are runtime-internal (§7.12).
#[derive(Serialize, Deserialize)]
enum KvOp {
    Put { key: String, value: KvValue },
    Delete { key: String },
}

/// The committed form: the folded ordered map.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct KvForm {
    map: BTreeMap<String, KvValue>,
}

/// The per-command stage (spec §7.12): the staged writes as an overlay —
/// `Some` = staged put, `None` = staged delete — giving the handler
/// read-your-staged-writes. It drains into one record per touched key (last
/// write to a key wins, exactly as the fold would resolve the sequence).
#[derive(Default)]
pub struct KvStage {
    overlay: BTreeMap<String, Option<KvValue>>,
}

impl Facet for Kv {
    const TAG: u8 = 1;

    type Form = KvForm;
    type Stage = KvStage;

    fn drain(stage: KvStage) -> Vec<Vec<u8>> {
        stage
            .overlay
            .into_iter()
            .map(|(key, staged)| {
                let op = match staged {
                    Some(value) => KvOp::Put { key, value },
                    None => KvOp::Delete { key },
                };
                encode_payload(&op)
            })
            .collect()
    }

    fn fold(form: &mut KvForm, payload: &[u8]) -> Result<(), FacetError> {
        match decode_payload("kv record", payload)? {
            KvOp::Put { key, value } => {
                form.map.insert(key, value);
            }
            KvOp::Delete { key } => {
                form.map.remove(&key);
            }
        }
        Ok(())
    }

    async fn snapshot(form: &KvForm, _env: &FacetEnv) -> Result<Vec<u8>, FacetError> {
        Ok(encode_payload(form))
    }

    async fn restore(part: Option<&[u8]>, _env: &FacetEnv) -> Result<KvForm, FacetError> {
        match part {
            Some(bytes) => decode_payload("kv restore", bytes),
            None => Ok(KvForm::default()),
        }
    }

    fn roots(form: &KvForm) -> BTreeSet<BlobId> {
        form.map
            .values()
            .filter_map(|value| match value {
                KvValue::Spilled { id, .. } => Some(*id),
                KvValue::Inline(_) => None,
            })
            .collect()
    }
}

/// The handler-facing kv accessor (spec §7.13), obtained from
/// [`GrainCtx::kv`](crate::GrainCtx::kv). Reads see committed-plus-staged;
/// writes stage into the current command's atomic batch (§7.12).
pub struct KvHandle<'a, G: Grain, I>
where
    G::Facets: HasFacet<Kv, I>,
{
    cell: &'a Arc<FacetCell<G::Facets>>,
    blobs: GrainBlobs,
    _index: std::marker::PhantomData<I>,
}

impl<G: Grain> GrainCtx<G> {
    /// The grain's KV map (spec §7.13). Compiles exactly when the grain declares
    /// the [`Kv`] facet (`type Facets = (Kv, …)`) — the G10 discipline applied
    /// to storage. Writes are valid only inside a command handler (§7.12).
    pub fn kv<I>(&self) -> KvHandle<'_, G, I>
    where
        G::Facets: HasFacet<Kv, I>,
    {
        KvHandle {
            cell: self.facet_cell(),
            blobs: self.blobs(),
            _index: std::marker::PhantomData,
        }
    }
}

impl<G: Grain, I> KvHandle<'_, G, I>
where
    G::Facets: HasFacet<Kv, I>,
{
    /// Resolve `key`'s current value through the overlay — a staged put or
    /// delete wins over the committed map (read-your-staged-writes, §7.12) —
    /// and reduce it with `read`, without cloning the stored bytes.
    fn resolve<R>(&self, key: &str, read: impl FnOnce(Option<&KvValue>) -> R) -> R {
        self.cell.with_overlay::<Kv, I, _>(|form, stage| {
            match stage.and_then(|s| s.overlay.get(key)) {
                Some(staged) => read(staged.as_ref()), // staged put (Some) or delete (None)
                None => read(form.map.get(key)),       // fall through to committed
            }
        })
    }

    /// The value at `key`, or `None`. A spilled value is fetched from the blob
    /// area and verified by content (G17).
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, GrainError> {
        match self.resolve(key, |value| value.cloned()) {
            None => Ok(None),
            Some(KvValue::Inline(bytes)) => Ok(Some(bytes)),
            Some(KvValue::Spilled { id, .. }) => Ok(Some(self.blobs.get(id, None).await?)),
        }
    }

    /// Whether `key` is present.
    pub fn contains(&self, key: &str) -> bool {
        self.resolve(key, |value| value.is_some())
    }

    /// The stored length of `key`'s value, without fetching a spilled one.
    pub fn len_of(&self, key: &str) -> Option<u64> {
        self.resolve(key, |value| value.map(KvValue::len))
    }

    /// Stage `key = value` into the current command's batch (§7.12). A value
    /// above [`INLINE_MAX`] spills to the grain's blob area first — durable on a
    /// write quorum before the map references it (§7.10); if the command later
    /// fails, the spilled bytes are an orphan the next sweep reclaims.
    pub async fn put(&self, key: impl Into<String>, value: Vec<u8>) -> Result<(), GrainError> {
        let stored = if value.len() > INLINE_MAX {
            let len = value.len() as u64;
            let id = self.blobs.put(value).await?;
            KvValue::Spilled { id, len }
        } else {
            KvValue::Inline(value)
        };
        let key = key.into();
        self.cell.with_stage::<Kv, I, _>(|stage| {
            stage.overlay.insert(key, Some(stored));
        });
        Ok(())
    }

    /// Stage the removal of `key`. A spilled value's bytes become an orphan the
    /// next sweep reclaims (its id leaves the facet roots at commit).
    pub fn delete(&self, key: impl Into<String>) {
        let key = key.into();
        self.cell.with_stage::<Kv, I, _>(|stage| {
            stage.overlay.insert(key, None);
        });
    }

    /// The committed entries with `prefix` overlaid with this command's staged
    /// puts and deletes, in key order — the prefix walk behind
    /// [`list`](Self::list) (read-your-staged-writes, §7.12).
    fn merged_range(&self, prefix: &str) -> BTreeMap<String, KvValue> {
        self.cell.with_overlay::<Kv, I, _>(|form, stage| {
            let mut entries: BTreeMap<String, KvValue> = form
                .map
                .range(prefix.to_string()..)
                .take_while(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if let Some(stage) = stage {
                for (key, staged) in &stage.overlay {
                    if !key.starts_with(prefix) {
                        continue;
                    }
                    match staged {
                        Some(value) => {
                            entries.insert(key.clone(), value.clone());
                        }
                        None => {
                            entries.remove(key);
                        }
                    }
                }
            }
            entries
        })
    }

    /// The keys with `prefix`, in order, through the overlay (§7.13). Walks keys
    /// only — it never touches the values, so a large inline payload under the
    /// prefix costs nothing here (unlike [`list`](Self::list)).
    pub fn keys(&self, prefix: &str) -> Vec<String> {
        self.cell.with_overlay::<Kv, I, _>(|form, stage| {
            let mut keys: BTreeSet<String> = form
                .map
                .range(prefix.to_string()..)
                .take_while(|(k, _)| k.starts_with(prefix))
                .map(|(k, _)| k.clone())
                .collect();
            if let Some(stage) = stage {
                for (key, staged) in &stage.overlay {
                    if !key.starts_with(prefix) {
                        continue;
                    }
                    match staged {
                        Some(_) => {
                            keys.insert(key.clone());
                        }
                        None => {
                            keys.remove(key);
                        }
                    }
                }
            }
            keys.into_iter().collect()
        })
    }

    /// The `(key, value)` pairs with `prefix`, in key order, through the overlay.
    /// Spilled values are fetched concurrently and verified (G17).
    pub async fn list(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, GrainError> {
        let entries = self.merged_range(prefix);
        futures::future::try_join_all(entries.into_iter().map(|(key, value)| async move {
            Ok(match value {
                KvValue::Inline(bytes) => (key, bytes),
                KvValue::Spilled { id, .. } => (key, self.blobs.get(id, None).await?),
            })
        }))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ops_fold_into_the_map_and_round_trip_the_snapshot() {
        let mut form = KvForm::default();
        let put = postcard::to_allocvec(&KvOp::Put {
            key: "a".into(),
            value: KvValue::Inline(vec![1]),
        })
        .unwrap();
        Kv::fold(&mut form, &put).unwrap();
        assert_eq!(form.map.get("a"), Some(&KvValue::Inline(vec![1])));

        let snapshot = postcard::to_allocvec(&form).unwrap();
        let restored: KvForm = postcard::from_bytes(&snapshot).unwrap();
        assert_eq!(restored.map, form.map);

        let delete = postcard::to_allocvec(&KvOp::Delete { key: "a".into() }).unwrap();
        Kv::fold(&mut form, &delete).unwrap();
        assert!(form.map.is_empty());
    }

    #[test]
    fn roots_are_exactly_the_spilled_ids() {
        let mut form = KvForm::default();
        let id = BlobId::of(b"big");
        form.map
            .insert("big".into(), KvValue::Spilled { id, len: 3 });
        form.map.insert("small".into(), KvValue::Inline(vec![0]));
        assert_eq!(Kv::roots(&form), BTreeSet::from([id]));
    }

    #[test]
    fn corrupt_record_is_a_facet_error() {
        let mut form = KvForm::default();
        assert!(Kv::fold(&mut form, &[0xFF, 0xFF, 0xFF]).is_err());
    }
}
