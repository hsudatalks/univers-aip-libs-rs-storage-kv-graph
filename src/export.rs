//! Complete, read-only export of this repository's stored graph indexes.
use std::collections::{BTreeMap, BTreeSet};

use super::{
    GraphRepositoryError, KvGraphRepository, FORWARD, LEGACY_EDGE_STATE, PROJECTION_FENCE_KEY,
    PROJECTION_TOMBSTONE, REVERSE, SEP,
};

/// An actual edge endpoint, not an entity record or a synthesized node body.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct GraphEndpoint {
    pub table: String,
    pub id: String,
}

/// A live edge, identified by its exact stored forward key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphSnapshotEdge {
    pub physical_key: String,
    pub source: GraphEndpoint,
    pub target: GraphEndpoint,
    pub kind: String,
    /// Empty historical values mean no metadata; JSON null remains Some(Null).
    pub metadata: Option<serde_json::Value>,
    /// Original metadata bytes, including their spelling and any stored fence.
    pub raw_metadata: Vec<u8>,
}

/// One consistent repository read. This repository stores no entity bodies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphSnapshot {
    /// Ordered by the original physical forward key.
    pub edges: Vec<GraphSnapshotEdge>,
    /// Unique live endpoints ordered by (table, id); these are not node properties.
    pub endpoints: Vec<GraphEndpoint>,
}

fn invalid(message: &str) -> GraphRepositoryError {
    GraphRepositoryError::ConstraintViolation(message.into())
}

fn key_parts<'a>(key: &'a str, prefix: &str) -> Result<[&'a str; 5], GraphRepositoryError> {
    let mut parts = key
        .strip_prefix(prefix)
        .ok_or_else(|| invalid("graph snapshot contains an unexpected key prefix"))?
        .split(SEP);
    let fields = [
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ];
    match fields {
        [Some(a), Some(b), Some(c), Some(d), Some(e)] if parts.next().is_none() => {
            Ok([a, b, c, d, e])
        }
        _ => Err(invalid("graph snapshot contains a malformed graph key")),
    }
}

fn index_rows(
    rows: Vec<(String, Vec<u8>)>,
    prefix: &str,
) -> Result<BTreeMap<String, Vec<u8>>, GraphRepositoryError> {
    let mut indexed = BTreeMap::new();
    for (key, value) in rows {
        key_parts(&key, prefix)?;
        if indexed.insert(key, value).is_some() {
            return Err(invalid("graph snapshot contains duplicate physical keys"));
        }
    }
    Ok(indexed)
}

impl KvGraphRepository {
    /// Export all live edges from one backend-owned logical read snapshot.
    ///
    /// Requires `KvStore::scan_prefixes`; unsupported backends remain errors.
    /// It never falls back to independent reads, repairs indexes or writes keys.
    /// Missing/mismatched indexes, corrupt metadata or incomplete projection
    /// repairs fail the whole export. Valid tombstones are checked but omitted.
    pub async fn snapshot_export(&self) -> Result<GraphSnapshot, GraphRepositoryError> {
        let prefixes = vec![FORWARD.into(), REVERSE.into(), PROJECTION_TOMBSTONE.into()];
        let batches = self
            .kv
            .scan_prefixes(&prefixes)
            .await
            .map_err(Self::map_kv_err)?;
        let [forward, reverse, states]: [Vec<(String, Vec<u8>)>; 3] = batches
            .try_into()
            .map_err(|_| invalid("graph snapshot scan returned the wrong number of prefixes"))?;
        let forward = index_rows(forward, FORWARD)?;
        let mut reverse = index_rows(reverse, REVERSE)?;
        let mut states = index_rows(states, PROJECTION_TOMBSTONE)?;
        let mut edges = Vec::with_capacity(forward.len());
        let mut endpoints = BTreeSet::new();
        for (physical_key, raw_metadata) in forward {
            let [source_table, source_id, kind, target_table, target_id] =
                key_parts(&physical_key, FORWARD)?;
            let reverse_key = Self::rev_key(target_table, target_id, kind, source_table, source_id);
            if reverse.remove(&reverse_key).as_ref() != Some(&raw_metadata) {
                return Err(invalid("graph snapshot forward/reverse indexes disagree"));
            }
            let metadata: Option<serde_json::Value> = if raw_metadata.is_empty() {
                None
            } else {
                Some(serde_json::from_slice(&raw_metadata).map_err(Self::map_kv_err)?)
            };
            let state_key = Self::projection_tombstone_key(
                source_table,
                source_id,
                kind,
                target_table,
                target_id,
            );
            match states.remove(&state_key) {
                Some(state) if state.as_slice() != LEGACY_EDGE_STATE => {
                    let (_, _, deleted) = Self::decode_projection(&state)?;
                    if deleted || state != raw_metadata {
                        return Err(invalid(
                            "graph snapshot live edge and projection state disagree",
                        ));
                    }
                }
                _ => {
                    if metadata
                        .as_ref()
                        .is_some_and(|value| value.get(PROJECTION_FENCE_KEY).is_some())
                    {
                        return Err(invalid(
                            "graph snapshot projection edge has no matching state",
                        ));
                    }
                }
            }
            let source = GraphEndpoint {
                table: source_table.into(),
                id: source_id.into(),
            };
            let target = GraphEndpoint {
                table: target_table.into(),
                id: target_id.into(),
            };
            endpoints.insert(source.clone());
            endpoints.insert(target.clone());
            edges.push(GraphSnapshotEdge {
                kind: kind.into(),
                physical_key,
                source,
                target,
                metadata,
                raw_metadata,
            });
        }
        if !reverse.is_empty() {
            return Err(invalid(
                "graph snapshot has reverse indexes without forward edges",
            ));
        }
        for state in states.values() {
            if state.as_slice() == LEGACY_EDGE_STATE || !Self::decode_projection(state)?.2 {
                return Err(invalid(
                    "graph snapshot contains an incomplete live-edge repair",
                ));
            }
        }
        Ok(GraphSnapshot {
            edges,
            endpoints: endpoints.into_iter().collect(),
        })
    }
}
