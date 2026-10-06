//! KvGraphRepository — GraphRepository on any KvStore.
//!
//! Implements `GraphRepository` over the public `KvStore` Port, including its
//! batch compare-and-swap capability for coordinated projection updates. The
//! caller supplies the backend; unsupported capabilities remain errors. There
//! is no concrete storage backend or owner runtime dependency.
//!
//! ## Key encoding
//!
//! ```text
//! Forward:  "e\x1f{src_table}\x1f{src_id}\x1f{rel}\x1f{tgt_table}\x1f{tgt_id}"
//! Reverse:  "r\x1f{tgt_table}\x1f{tgt_id}\x1f{rel}\x1f{src_table}\x1f{src_id}"
//! Value:    metadata JSON bytes (b"" when no metadata)
//! ```
//!
//! `\x1f` (Unit Separator) delimits fields — safe because table/id names are
//! alphanumeric + underscores/dashes, never `\x1f`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;

use univers_aip_contracts_data::storage::graph_repository::{
    GraphRelationStats, GraphRepository, GraphRepositoryError, RelationMetadata,
    RelationProjectionFence, RelationProjectionMutation,
};
use univers_aip_contracts_data::storage::kv_store::KvStore;

mod export;
pub use export::{GraphEndpoint, GraphSnapshot, GraphSnapshotEdge};

const SEP: char = '\u{001f}';
const FORWARD: &str = "e\u{001f}";
const REVERSE: &str = "r\u{001f}";
const PROJECTION_TOMBSTONE: &str = "p\u{001f}";
const PROJECTION_FENCE_KEY: &str = "_univers_projection_fence";
const LEGACY_EDGE_STATE: &[u8] = b"legacy";

/// A `GraphRepository` backed by any `KvStore`.
///
/// Forward index (`e\x1f…`) answers `find_targets`; reverse index (`r\x1f…`)
/// answers `find_sources`. Both indices are written on every `relate` so
/// bidirectional queries are single-scan.
pub struct KvGraphRepository {
    kv: Arc<dyn KvStore>,
}

impl KvGraphRepository {
    pub fn new(kv: Arc<dyn KvStore>) -> Self {
        Self { kv }
    }

    // ── key helpers ──────────────────────────────────────────

    fn fwd_key(src_t: &str, src_id: &str, rel: &str, tgt_t: &str, tgt_id: &str) -> String {
        format!("{FORWARD}{src_t}{SEP}{src_id}{SEP}{rel}{SEP}{tgt_t}{SEP}{tgt_id}")
    }

    fn rev_key(tgt_t: &str, tgt_id: &str, rel: &str, src_t: &str, src_id: &str) -> String {
        format!("{REVERSE}{tgt_t}{SEP}{tgt_id}{SEP}{rel}{SEP}{src_t}{SEP}{src_id}")
    }

    fn projection_tombstone_key(
        src_t: &str,
        src_id: &str,
        rel: &str,
        tgt_t: &str,
        tgt_id: &str,
    ) -> String {
        format!("{PROJECTION_TOMBSTONE}{src_t}{SEP}{src_id}{SEP}{rel}{SEP}{tgt_t}{SEP}{tgt_id}")
    }

    fn parse_projection_key(key: &str) -> Option<(&str, &str, &str, &str, &str)> {
        let rest = key.strip_prefix(PROJECTION_TOMBSTONE)?;
        let mut parts = rest.split(SEP);
        Some((
            parts.next()?,
            parts.next()?,
            parts.next()?,
            parts.next()?,
            parts.next()?,
        ))
    }

    /// Forward prefix for `find_targets(src_t, src_id, rel)`.
    fn fwd_prefix(src_t: &str, src_id: &str, rel: &str) -> String {
        format!("{FORWARD}{src_t}{SEP}{src_id}{SEP}{rel}{SEP}")
    }

    /// Reverse prefix for `find_sources(tgt_t, tgt_id, rel)`.
    fn rev_prefix(tgt_t: &str, tgt_id: &str, rel: &str) -> String {
        format!("{REVERSE}{tgt_t}{SEP}{tgt_id}{SEP}{rel}{SEP}")
    }

    /// Forward prefix scoped to a source entity (for `unrelate_all`).
    fn fwd_entity_prefix(src_t: &str, src_id: &str) -> String {
        format!("{FORWARD}{src_t}{SEP}{src_id}{SEP}")
    }

    /// Reverse prefix scoped to a target entity (for `unrelate_all`).
    fn rev_entity_prefix(tgt_t: &str, tgt_id: &str) -> String {
        format!("{REVERSE}{tgt_t}{SEP}{tgt_id}{SEP}")
    }

    /// Parse `tgt_table, tgt_id` from the tail of a forward key.
    /// Full key: `e\x1f{src_t}\x1f{src_id}\x1f{rel}\x1f{tgt_t}\x1f{tgt_id}`
    /// Returns (tgt_table, tgt_id, relation_type).
    fn parse_fwd(key: &str) -> Option<(&str, &str, &str)> {
        let rest = key.strip_prefix(FORWARD)?;
        let mut parts = rest.split(SEP);
        // skip src_t, src_id
        parts.next()?;
        parts.next()?;
        let rel = parts.next()?;
        let tgt_t = parts.next()?;
        let tgt_id = parts.next()?;
        Some((tgt_t, tgt_id, rel))
    }

    /// Parse `src_table, src_id, relation_type` from a reverse key.
    /// Full key: `r\x1f{tgt_t}\x1f{tgt_id}\x1f{rel}\x1f{src_t}\x1f{src_id}`
    fn parse_rev(key: &str) -> Option<(&str, &str, &str)> {
        let rest = key.strip_prefix(REVERSE)?;
        let mut parts = rest.split(SEP);
        parts.next()?; // tgt_t
        parts.next()?; // tgt_id
        let rel = parts.next()?;
        let src_t = parts.next()?;
        let src_id = parts.next()?;
        Some((src_t, src_id, rel))
    }

    fn map_kv_err(e: impl std::fmt::Display) -> GraphRepositoryError {
        GraphRepositoryError::Storage(e.to_string())
    }

    fn legacy_projection_mutation_error() -> GraphRepositoryError {
        GraphRepositoryError::ConstraintViolation(
            "legacy relation mutation cannot modify a fenced projection".to_string(),
        )
    }

    fn encode_projection(
        mut metadata: RelationMetadata,
        fence: &RelationProjectionFence,
        deleted: bool,
    ) -> Result<Vec<u8>, GraphRepositoryError> {
        if metadata.contains_key(PROJECTION_FENCE_KEY) {
            return Err(GraphRepositoryError::ConstraintViolation(format!(
                "relation metadata field '{PROJECTION_FENCE_KEY}' is reserved"
            )));
        }
        metadata.insert(
            PROJECTION_FENCE_KEY.to_string(),
            serde_json::json!({
                "owner": fence.owner,
                "revision": fence.revision,
                "deleted": deleted,
            }),
        );
        serde_json::to_vec(&metadata).map_err(Self::map_kv_err)
    }

    fn decode_projection(
        value: &[u8],
    ) -> Result<(RelationProjectionFence, RelationMetadata, bool), GraphRepositoryError> {
        let mut metadata: RelationMetadata = serde_json::from_slice(value).map_err(|error| {
            GraphRepositoryError::ConstraintViolation(format!(
                "relation projection has invalid metadata: {error}"
            ))
        })?;
        let raw_fence = metadata.remove(PROJECTION_FENCE_KEY).ok_or_else(|| {
            GraphRepositoryError::ConstraintViolation(
                "cannot conditionally mutate an unfenced relation edge".to_string(),
            )
        })?;
        let deleted = raw_fence
            .get("deleted")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| {
                GraphRepositoryError::ConstraintViolation(
                    "relation projection fence is missing its deletion state".to_string(),
                )
            })?;
        let fence = serde_json::from_value(raw_fence).map_err(|error| {
            GraphRepositoryError::ConstraintViolation(format!(
                "relation projection has an invalid fence: {error}"
            ))
        })?;
        Ok((fence, metadata, deleted))
    }

    fn compare_upsert_projection(
        current: &RelationProjectionFence,
        requested: &RelationProjectionFence,
    ) -> Option<RelationProjectionMutation> {
        if current.owner != requested.owner && current.revision >= requested.revision {
            Some(RelationProjectionMutation::OwnershipMismatch {
                current: current.clone(),
            })
        } else if current.revision > requested.revision {
            Some(RelationProjectionMutation::Superseded {
                current: current.clone(),
            })
        } else {
            None
        }
    }

    fn compare_delete_projection(
        current: &RelationProjectionFence,
        requested: &RelationProjectionFence,
    ) -> Option<RelationProjectionMutation> {
        if current.owner != requested.owner {
            Some(RelationProjectionMutation::OwnershipMismatch {
                current: current.clone(),
            })
        } else if current.revision > requested.revision {
            Some(RelationProjectionMutation::Superseded {
                current: current.clone(),
            })
        } else {
            None
        }
    }

    async fn legacy_put_index_pair(
        &self,
        projection_key: &str,
        forward_key: &str,
        reverse_key: &str,
        value: Vec<u8>,
        require_existing: bool,
    ) -> Result<Option<bool>, GraphRepositoryError> {
        loop {
            let state = self
                .kv
                .get(projection_key)
                .await
                .map_err(Self::map_kv_err)?;
            if state.as_deref().is_some_and(|raw| raw != LEGACY_EDGE_STATE) {
                return Err(Self::legacy_projection_mutation_error());
            }
            let current = self.kv.get(forward_key).await.map_err(Self::map_kv_err)?;
            if require_existing && current.is_none() {
                return Ok(None);
            }
            let created = current.is_none();
            let result = self
                .kv
                .compare_and_swap_batch(
                    projection_key,
                    state.as_deref(),
                    Some(LEGACY_EDGE_STATE),
                    vec![
                        (forward_key.to_string(), value.clone()),
                        (reverse_key.to_string(), value.clone()),
                    ],
                    Vec::new(),
                )
                .await
                .map_err(Self::map_kv_err)?;
            if result.is_some() {
                return Ok(Some(created));
            }
        }
    }

    async fn legacy_delete_index_pair(
        &self,
        projection_key: &str,
        forward_key: &str,
        reverse_key: &str,
    ) -> Result<bool, GraphRepositoryError> {
        loop {
            let state = self
                .kv
                .get(projection_key)
                .await
                .map_err(Self::map_kv_err)?;
            if state.as_deref().is_some_and(|raw| raw != LEGACY_EDGE_STATE) {
                return Err(Self::legacy_projection_mutation_error());
            }
            let result = self
                .kv
                .compare_and_swap_batch(
                    projection_key,
                    state.as_deref(),
                    None,
                    Vec::new(),
                    vec![forward_key.to_string(), reverse_key.to_string()],
                )
                .await
                .map_err(Self::map_kv_err)?;
            if let Some(deleted) = result {
                return Ok(deleted.first().is_some_and(Option::is_some));
            }
        }
    }

    async fn ensure_legacy_edge_unfenced(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<(), GraphRepositoryError> {
        let projection_key = Self::projection_tombstone_key(
            source_table,
            source_id,
            relation,
            target_table,
            target_id,
        );
        let state = self
            .kv
            .get(&projection_key)
            .await
            .map_err(Self::map_kv_err)?;
        if state.as_deref().is_some_and(|raw| raw != LEGACY_EDGE_STATE) {
            return Err(Self::legacy_projection_mutation_error());
        }
        Ok(())
    }

    async fn ensure_legacy_entity_unfenced(
        &self,
        table: &str,
        id: &str,
        relation_names: &[&str],
    ) -> Result<(), GraphRepositoryError> {
        let states = self
            .kv
            .scan_prefix(PROJECTION_TOMBSTONE)
            .await
            .map_err(Self::map_kv_err)?;
        let has_fenced_projection = states.iter().any(|(key, value)| {
            let Some((source_table, source_id, relation, target_table, target_id)) =
                Self::parse_projection_key(key)
            else {
                return false;
            };
            let touches_entity = (source_table == table && source_id == id)
                || (target_table == table && target_id == id);
            let selected = relation_names.is_empty() || relation_names.contains(&relation);
            touches_entity && selected && value != LEGACY_EDGE_STATE
        });
        if has_fenced_projection {
            return Err(Self::legacy_projection_mutation_error());
        }
        Ok(())
    }
}

#[async_trait]
impl GraphRepository for KvGraphRepository {
    // ── Core CRUD ───────────────────────────────────────────

    async fn relate(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<bool, GraphRepositoryError> {
        let fk = Self::fwd_key(source_table, source_id, relation, target_table, target_id);
        let rk = Self::rev_key(target_table, target_id, relation, source_table, source_id);
        let pk = Self::projection_tombstone_key(
            source_table,
            source_id,
            relation,
            target_table,
            target_id,
        );
        Ok(self
            .legacy_put_index_pair(&pk, &fk, &rk, Vec::new(), false)
            .await?
            .expect("legacy relate does not require an existing edge"))
    }

    async fn relate_with_metadata(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
        metadata: serde_json::Value,
    ) -> Result<(), GraphRepositoryError> {
        let fk = Self::fwd_key(source_table, source_id, relation, target_table, target_id);
        let rk = Self::rev_key(target_table, target_id, relation, source_table, source_id);
        let pk = Self::projection_tombstone_key(
            source_table,
            source_id,
            relation,
            target_table,
            target_id,
        );
        let val = serde_json::to_vec(&metadata)
            .map_err(|error| GraphRepositoryError::Storage(error.to_string()))?;
        self.legacy_put_index_pair(&pk, &fk, &rk, val, false)
            .await?;
        Ok(())
    }

    async fn upsert_relation_projection(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
        metadata: RelationMetadata,
        fence: RelationProjectionFence,
    ) -> Result<RelationProjectionMutation, GraphRepositoryError> {
        let fk = Self::fwd_key(source_table, source_id, relation, target_table, target_id);
        let rk = Self::rev_key(target_table, target_id, relation, source_table, source_id);
        let pk = Self::projection_tombstone_key(
            source_table,
            source_id,
            relation,
            target_table,
            target_id,
        );
        let live_value = Self::encode_projection(metadata.clone(), &fence, false)?;

        loop {
            let current_state = self.kv.get(&pk).await.map_err(Self::map_kv_err)?;
            let current_edge = self.kv.get(&fk).await.map_err(Self::map_kv_err)?;
            let current_reverse = self.kv.get(&rk).await.map_err(Self::map_kv_err)?;
            if self.kv.get(&pk).await.map_err(Self::map_kv_err)? != current_state {
                continue;
            }
            match current_state.as_deref() {
                None if current_edge.is_some() || current_reverse.is_some() => {
                    return Err(GraphRepositoryError::ConstraintViolation(
                        "cannot conditionally mutate an unfenced relation edge".to_string(),
                    ));
                }
                Some(raw) => {
                    let (current, current_metadata, deleted) = Self::decode_projection(raw)?;
                    if deleted && (current_edge.is_some() || current_reverse.is_some()) {
                        return Err(GraphRepositoryError::ConstraintViolation(
                            "relation projection edge and durable fence are inconsistent"
                                .to_string(),
                        ));
                    }
                    if let Some(outcome) = Self::compare_upsert_projection(&current, &fence) {
                        return Ok(outcome);
                    }
                    if current.revision == fence.revision {
                        if !deleted && current_metadata == metadata {
                            let forward_matches = current_edge.as_deref() == Some(raw);
                            let reverse_matches = current_reverse.as_deref() == Some(raw);
                            if forward_matches && reverse_matches {
                                return Ok(RelationProjectionMutation::AlreadyApplied);
                            }
                            if current_edge.is_some() && !forward_matches
                                || current_reverse.is_some() && !reverse_matches
                            {
                                return Err(GraphRepositoryError::ConstraintViolation(
                                    "relation projection index payload does not match its durable fence"
                                        .to_string(),
                                ));
                            }
                        } else {
                            return Err(GraphRepositoryError::ConstraintViolation(
                                "equal relation projection fence is bound to a different mutation"
                                    .to_string(),
                            ));
                        }
                    }
                }
                None => {}
            }

            let result = self
                .kv
                .compare_and_swap_batch(
                    &pk,
                    current_state.as_deref(),
                    Some(&live_value),
                    vec![
                        (fk.clone(), live_value.clone()),
                        (rk.clone(), live_value.clone()),
                    ],
                    Vec::new(),
                )
                .await
                .map_err(Self::map_kv_err)?;
            if result.is_some() {
                return Ok(RelationProjectionMutation::Applied);
            }
        }
    }

    async fn update_relation_metadata(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
        metadata: serde_json::Value,
    ) -> Result<(), GraphRepositoryError> {
        let fk = Self::fwd_key(source_table, source_id, relation, target_table, target_id);
        let rk = Self::rev_key(target_table, target_id, relation, source_table, source_id);
        let pk = Self::projection_tombstone_key(
            source_table,
            source_id,
            relation,
            target_table,
            target_id,
        );
        let val = serde_json::to_vec(&metadata)
            .map_err(|error| GraphRepositoryError::Storage(error.to_string()))?;
        if self
            .legacy_put_index_pair(&pk, &fk, &rk, val, true)
            .await?
            .is_none()
        {
            return Err(GraphRepositoryError::NotFound(format!(
                "relation {source_table}:{source_id} -[{relation}]-> {target_table}:{target_id}"
            )));
        }
        Ok(())
    }

    async fn unrelate(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<(), GraphRepositoryError> {
        let fk = Self::fwd_key(source_table, source_id, relation, target_table, target_id);
        let rk = Self::rev_key(target_table, target_id, relation, source_table, source_id);
        let pk = Self::projection_tombstone_key(
            source_table,
            source_id,
            relation,
            target_table,
            target_id,
        );
        self.legacy_delete_index_pair(&pk, &fk, &rk).await?;
        Ok(())
    }

    async fn delete_relation_projection(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
        fence: RelationProjectionFence,
    ) -> Result<RelationProjectionMutation, GraphRepositoryError> {
        let fk = Self::fwd_key(source_table, source_id, relation, target_table, target_id);
        let rk = Self::rev_key(target_table, target_id, relation, source_table, source_id);
        let pk = Self::projection_tombstone_key(
            source_table,
            source_id,
            relation,
            target_table,
            target_id,
        );
        let tombstone = Self::encode_projection(RelationMetadata::new(), &fence, true)?;

        loop {
            let current_state = self.kv.get(&pk).await.map_err(Self::map_kv_err)?;
            let current_edge = self.kv.get(&fk).await.map_err(Self::map_kv_err)?;
            let current_reverse = self.kv.get(&rk).await.map_err(Self::map_kv_err)?;
            if self.kv.get(&pk).await.map_err(Self::map_kv_err)? != current_state {
                continue;
            }
            match current_state.as_deref() {
                None if current_edge.is_some() || current_reverse.is_some() => {
                    return Err(GraphRepositoryError::ConstraintViolation(
                        "cannot conditionally mutate an unfenced relation edge".to_string(),
                    ));
                }
                Some(raw) => {
                    let (current, _, deleted) = Self::decode_projection(raw)?;
                    if deleted && (current_edge.is_some() || current_reverse.is_some()) {
                        return Err(GraphRepositoryError::ConstraintViolation(
                            "relation projection edge and durable fence are inconsistent"
                                .to_string(),
                        ));
                    }
                    if let Some(outcome) = Self::compare_delete_projection(&current, &fence) {
                        return Ok(outcome);
                    }
                    if current.revision == fence.revision {
                        if deleted {
                            return Ok(RelationProjectionMutation::AlreadyApplied);
                        }
                        return Err(GraphRepositoryError::ConstraintViolation(
                            "equal relation projection fence is bound to a different mutation"
                                .to_string(),
                        ));
                    }
                }
                None => {}
            }

            let result = self
                .kv
                .compare_and_swap_batch(
                    &pk,
                    current_state.as_deref(),
                    Some(&tombstone),
                    Vec::new(),
                    vec![fk.clone(), rk.clone()],
                )
                .await
                .map_err(Self::map_kv_err)?;
            if result.is_some() {
                return Ok(RelationProjectionMutation::Applied);
            }
        }
    }

    async fn unrelate_all(
        &self,
        table: &str,
        id: &str,
        relation_names: &[&str],
    ) -> Result<(), GraphRepositoryError> {
        self.ensure_legacy_entity_unfenced(table, id, relation_names)
            .await?;
        let prefixes = vec![
            Self::fwd_entity_prefix(table, id),
            Self::rev_entity_prefix(table, id),
        ];
        let mut batches = self
            .kv
            .scan_prefixes(&prefixes)
            .await
            .map_err(Self::map_kv_err)?
            .into_iter();

        let mut doomed = HashSet::new();
        for (key, _) in batches.next().unwrap_or_default() {
            if let Some((target_table, target_id, relation)) = Self::parse_fwd(&key) {
                if relation_names.is_empty() || relation_names.contains(&relation) {
                    doomed.insert((
                        table.to_string(),
                        id.to_string(),
                        target_table.to_string(),
                        target_id.to_string(),
                        relation.to_string(),
                    ));
                }
            }
        }
        for (key, _) in batches.next().unwrap_or_default() {
            if let Some((source_table, source_id, relation)) = Self::parse_rev(&key) {
                if relation_names.is_empty() || relation_names.contains(&relation) {
                    doomed.insert((
                        source_table.to_string(),
                        source_id.to_string(),
                        table.to_string(),
                        id.to_string(),
                        relation.to_string(),
                    ));
                }
            }
        }
        for (st, si, tt, ti, rel) in &doomed {
            self.ensure_legacy_edge_unfenced(st, si, tt, ti, rel)
                .await?;
        }
        for (st, si, tt, ti, rel) in doomed {
            let fk = Self::fwd_key(&st, &si, &rel, &tt, &ti);
            let rk = Self::rev_key(&tt, &ti, &rel, &st, &si);
            let pk = Self::projection_tombstone_key(&st, &si, &rel, &tt, &ti);
            self.legacy_delete_index_pair(&pk, &fk, &rk).await?;
        }
        Ok(())
    }

    /// Full bidirectional ghost-edge sweep — O(degree), not O(graph): both
    /// index directions are prefix scans keyed by this entity, and each hit
    /// derives its twin key on the other endpoint. Deletes run in chunked
    /// batches (512 keys) to keep individual write transactions short.
    async fn sweep_entity(&self, table: &str, id: &str) -> Result<u64, GraphRepositoryError> {
        self.ensure_legacy_entity_unfenced(table, id, &[]).await?;
        let mut doomed = HashSet::new();
        // Outgoing edges: fwd keys under this entity + their reverse twins.
        for (key, _) in self
            .kv
            .scan_prefix(&Self::fwd_entity_prefix(table, id))
            .await
            .map_err(Self::map_kv_err)?
        {
            if let Some((tgt_t, tgt_id, rel)) = Self::parse_fwd(&key) {
                doomed.insert((
                    table.to_string(),
                    id.to_string(),
                    tgt_t.to_string(),
                    tgt_id.to_string(),
                    rel.to_string(),
                ));
            }
        }
        // Incoming edges: rev keys under this entity + their forward twins.
        for (key, _) in self
            .kv
            .scan_prefix(&Self::rev_entity_prefix(table, id))
            .await
            .map_err(Self::map_kv_err)?
        {
            if let Some((src_t, src_id, rel)) = Self::parse_rev(&key) {
                doomed.insert((
                    src_t.to_string(),
                    src_id.to_string(),
                    table.to_string(),
                    id.to_string(),
                    rel.to_string(),
                ));
            }
        }
        for (st, si, tt, ti, rel) in &doomed {
            self.ensure_legacy_edge_unfenced(st, si, tt, ti, rel)
                .await?;
        }
        let edges = doomed.len() as u64;
        for (st, si, tt, ti, rel) in doomed {
            let fk = Self::fwd_key(&st, &si, &rel, &tt, &ti);
            let rk = Self::rev_key(&tt, &ti, &rel, &st, &si);
            let pk = Self::projection_tombstone_key(&st, &si, &rel, &tt, &ti);
            self.legacy_delete_index_pair(&pk, &fk, &rk).await?;
        }
        Ok(edges)
    }

    async fn exists(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<bool, GraphRepositoryError> {
        let fk = Self::fwd_key(source_table, source_id, relation, target_table, target_id);
        Ok(self.kv.get(&fk).await.map_err(Self::map_kv_err)?.is_some())
    }

    // ── Query Operations ─────────────────────────────────────

    async fn find_targets(
        &self,
        source_table: &str,
        source_id: &str,
        relation: &str,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        let prefix = Self::fwd_prefix(source_table, source_id, relation);
        let rows = self
            .kv
            .scan_prefix(&prefix)
            .await
            .map_err(Self::map_kv_err)?;
        Ok(rows
            .iter()
            .filter_map(|(k, _)| Self::parse_fwd(k))
            .map(|(tt, ti, _)| (tt.to_string(), ti.to_string()))
            .collect())
    }

    async fn find_sources(
        &self,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        let prefix = Self::rev_prefix(target_table, target_id, relation);
        let rows = self
            .kv
            .scan_prefix(&prefix)
            .await
            .map_err(Self::map_kv_err)?;
        Ok(rows
            .iter()
            .filter_map(|(k, _)| Self::parse_rev(k))
            .map(|(st, si, _)| (st.to_string(), si.to_string()))
            .collect())
    }

    /// Server-side paginated targets: a bounded range scan (limit+offset rows)
    /// instead of scanning the full fan-out. Overrides the default
    /// scan-all-then-slice so the KV read cost is bounded by the page window.
    async fn find_targets_paginated(
        &self,
        source_table: &str,
        source_id: &str,
        relation: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        let prefix = Self::fwd_prefix(source_table, source_id, relation);
        let need = offset.saturating_add(limit);
        let rows = self
            .kv
            .scan_prefix_paginated(&prefix, need, None)
            .await
            .map_err(Self::map_kv_err)?;
        Ok(rows
            .iter()
            .skip(offset)
            .filter_map(|(k, _)| Self::parse_fwd(k))
            .map(|(tt, ti, _)| (tt.to_string(), ti.to_string()))
            .take(limit)
            .collect())
    }

    /// Server-side paginated sources (bounded range scan).
    async fn find_sources_paginated(
        &self,
        target_table: &str,
        target_id: &str,
        relation: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        let prefix = Self::rev_prefix(target_table, target_id, relation);
        let need = offset.saturating_add(limit);
        let rows = self
            .kv
            .scan_prefix_paginated(&prefix, need, None)
            .await
            .map_err(Self::map_kv_err)?;
        Ok(rows
            .iter()
            .skip(offset)
            .filter_map(|(k, _)| Self::parse_rev(k))
            .map(|(st, si, _)| (st.to_string(), si.to_string()))
            .take(limit)
            .collect())
    }

    /// Count outgoing edges via a key-only range traversal (no value materialization).
    async fn count_targets(
        &self,
        source_table: &str,
        source_id: &str,
        relation: &str,
    ) -> Result<u64, GraphRepositoryError> {
        let prefix = Self::fwd_prefix(source_table, source_id, relation);
        self.kv
            .count_prefix(&prefix)
            .await
            .map_err(Self::map_kv_err)
    }

    /// Count incoming edges via a key-only range traversal.
    async fn count_sources(
        &self,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<u64, GraphRepositoryError> {
        let prefix = Self::rev_prefix(target_table, target_id, relation);
        self.kv
            .count_prefix(&prefix)
            .await
            .map_err(Self::map_kv_err)
    }

    async fn find_targets_by_table(
        &self,
        source_table: &str,
        source_id: &str,
        relation: &str,
        target_table_filter: &str,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        let all = self.find_targets(source_table, source_id, relation).await?;
        if target_table_filter.is_empty() {
            Ok(all)
        } else {
            Ok(all
                .into_iter()
                .filter(|(t, _)| t == target_table_filter)
                .collect())
        }
    }

    async fn find_sources_by_table(
        &self,
        target_table: &str,
        target_id: &str,
        relation: &str,
        source_table_filter: &str,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        let all = self.find_sources(target_table, target_id, relation).await?;
        if source_table_filter.is_empty() {
            Ok(all)
        } else {
            Ok(all
                .into_iter()
                .filter(|(t, _)| t == source_table_filter)
                .collect())
        }
    }

    async fn find_related(
        &self,
        table: &str,
        id: &str,
        relation_names: &[&str],
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();

        for &rel in relation_names {
            for (t, i) in self.find_targets(table, id, rel).await? {
                let key = (t.clone(), i.clone());
                if seen.insert(key) {
                    result.push((t, i));
                }
            }
            for (t, i) in self.find_sources(table, id, rel).await? {
                let key = (t.clone(), i.clone());
                if seen.insert(key) {
                    result.push((t, i));
                }
            }
        }
        Ok(result)
    }

    async fn find_all_relations(
        &self,
        table: &str,
        id: &str,
        relation_names: &[&str],
    ) -> Result<HashMap<String, Vec<(String, String)>>, GraphRepositoryError> {
        let mut map: HashMap<String, Vec<(String, String)>> = HashMap::new();
        for &rel in relation_names {
            let targets = self.find_targets(table, id, rel).await?;
            map.entry(rel.to_string()).or_default().extend(targets);
        }
        Ok(map)
    }

    async fn find_by_type(
        &self,
        relation: &str,
    ) -> Result<Vec<((String, String), (String, String))>, GraphRepositoryError> {
        let rows = self
            .kv
            .scan_prefix(FORWARD)
            .await
            .map_err(Self::map_kv_err)?;
        Ok(rows
            .iter()
            .filter_map(|(k, _)| {
                let (tgt_t, tgt_id, rel) = Self::parse_fwd(k)?;
                (rel == relation).then(|| {
                    // parse source from key: e\x1f{src_t}\x1f{src_id}\x1f…
                    let rest = k.strip_prefix(FORWARD)?;
                    let mut parts = rest.split(SEP);
                    let st = parts.next()?;
                    let si = parts.next()?;
                    Some((
                        (st.to_string(), si.to_string()),
                        (tgt_t.to_string(), tgt_id.to_string()),
                    ))
                })
            })
            .flatten()
            .collect())
    }

    async fn find_targets_with_metadata(
        &self,
        source_table: &str,
        source_id: &str,
        relation: &str,
    ) -> Result<Vec<((String, String), Option<RelationMetadata>)>, GraphRepositoryError> {
        let prefix = Self::fwd_prefix(source_table, source_id, relation);
        let rows = self
            .kv
            .scan_prefix(&prefix)
            .await
            .map_err(Self::map_kv_err)?;
        let mut results = Vec::new();
        for (k, v) in &rows {
            if let Some((tt, ti, _)) = Self::parse_fwd(k) {
                let metadata: Option<RelationMetadata> = if v.is_empty() {
                    None
                } else {
                    serde_json::from_slice(v).ok()
                };
                results.push(((tt.to_string(), ti.to_string()), metadata));
            }
        }
        Ok(results)
    }

    async fn find_sources_with_metadata(
        &self,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<Vec<((String, String), Option<RelationMetadata>)>, GraphRepositoryError> {
        let prefix = Self::rev_prefix(target_table, target_id, relation);
        let rows = self
            .kv
            .scan_prefix(&prefix)
            .await
            .map_err(Self::map_kv_err)?;
        let mut results = Vec::new();
        for (k, v) in &rows {
            if let Some((st, si, _)) = Self::parse_rev(k) {
                let metadata: Option<RelationMetadata> = if v.is_empty() {
                    None
                } else {
                    serde_json::from_slice(v).ok()
                };
                results.push(((st.to_string(), si.to_string()), metadata));
            }
        }
        Ok(results)
    }

    async fn get_stats(
        &self,
        relation_names: &[&str],
    ) -> Result<GraphRelationStats, GraphRepositoryError> {
        let rows = self
            .kv
            .scan_prefix(FORWARD)
            .await
            .map_err(Self::map_kv_err)?;
        let filter = relation_names.is_empty();

        let mut stats = GraphRelationStats::default();
        for (k, _) in &rows {
            let Some(rest) = k.strip_prefix(FORWARD) else {
                continue;
            };
            let mut parts = rest.split(SEP);
            let (st, _si, rel, tt, _ti) = (
                parts.next(),
                parts.next(),
                parts.next(),
                parts.next(),
                parts.next(),
            );
            let (Some(st), Some(rel), Some(tt)) = (st, rel, tt) else {
                continue;
            };
            if !filter && !relation_names.contains(&rel) {
                continue;
            }
            *stats.by_type.entry(rel.to_string()).or_default() += 1;
            *stats.by_source_table.entry(st.to_string()).or_default() += 1;
            *stats.by_target_table.entry(tt.to_string()).or_default() += 1;
            stats.total += 1;
        }
        Ok(stats)
    }

    // ── Batch Operations ─────────────────────────────────────

    async fn relate_batch(
        &self,
        relations: Vec<(String, String, String, String, String)>,
    ) -> Result<usize, GraphRepositoryError> {
        for (st, si, tt, ti, rel) in &relations {
            self.ensure_legacy_edge_unfenced(st, si, tt, ti, rel)
                .await?;
        }
        let mut count = 0;
        for (st, si, tt, ti, rel) in &relations {
            if self.relate(st, si, tt, ti, rel).await? {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn unrelate_batch(
        &self,
        relations: Vec<(String, String, String, String, String)>,
    ) -> Result<usize, GraphRepositoryError> {
        for (st, si, tt, ti, rel) in &relations {
            self.ensure_legacy_edge_unfenced(st, si, tt, ti, rel)
                .await?;
        }
        let mut count = 0;
        for (st, si, tt, ti, rel) in &relations {
            let fk = Self::fwd_key(st, si, rel, tt, ti);
            let rk = Self::rev_key(tt, ti, rel, st, si);
            let pk = Self::projection_tombstone_key(st, si, rel, tt, ti);
            if self.legacy_delete_index_pair(&pk, &fk, &rk).await? {
                count += 1;
            }
        }
        Ok(count)
    }

    // ── Transactions (no-op — KV stores are per-op atomic) ──

    async fn begin_transaction(&self) -> Result<(), GraphRepositoryError> {
        Ok(())
    }
    async fn commit_transaction(&self) -> Result<(), GraphRepositoryError> {
        Ok(())
    }
    async fn rollback_transaction(&self) -> Result<(), GraphRepositoryError> {
        Ok(())
    }

    // ── Entity Operations (stubs — KV store doesn't hold entities) ──

    async fn get_record_json(
        &self,
        _table: &str,
        _id: &str,
    ) -> Result<Option<serde_json::Value>, GraphRepositoryError> {
        Ok(None)
    }

    async fn set_namespace(
        &self,
        _namespace: &str,
        _database: &str,
    ) -> Result<(), GraphRepositoryError> {
        Ok(())
    }

    async fn list_record_ids(
        &self,
        _table: &str,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        Ok(Vec::new())
    }

    async fn query_records_by_field(
        &self,
        _table: &str,
        _field_path: &str,
        _value: &serde_json::Value,
    ) -> Result<Vec<serde_json::Value>, GraphRepositoryError> {
        Ok(Vec::new())
    }

    async fn query_records_by_conditions(
        &self,
        _table: &str,
        _conditions: &[(&str, serde_json::Value)],
    ) -> Result<Vec<serde_json::Value>, GraphRepositoryError> {
        Ok(Vec::new())
    }
}
