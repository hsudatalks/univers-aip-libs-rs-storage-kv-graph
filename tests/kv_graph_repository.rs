//! Tests for KvGraphRepository — uses an in-memory KvStore mock.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use univers_aip_contracts_data::storage::{
    graph_repository::{GraphRepository, RelationProjectionFence, RelationProjectionMutation},
    kv_store::KvStore,
    RepositoryResult,
};
use univers_aip_lib_storage_kv_graph::KvGraphRepository;

// ── minimal in-memory KvStore for testing ───────────────────

struct MemKv(Mutex<BTreeMap<String, Vec<u8>>>, AtomicUsize, bool);

impl MemKv {
    fn new() -> Self {
        Self(Mutex::new(BTreeMap::new()), AtomicUsize::new(0), true)
    }
}

#[async_trait]
impl KvStore for MemKv {
    async fn scan_prefixes(
        &self,
        prefixes: &[String],
    ) -> univers_aip_contracts_data::storage::RepositoryResult<Vec<Vec<(String, Vec<u8>)>>> {
        self.1.fetch_add(1, Ordering::SeqCst);
        if !self.2 {
            return Err(
                univers_aip_contracts_data::storage::RepositoryError::validation(
                    "snapshot prefix scans are unavailable",
                ),
            );
        }
        let values = self.0.lock().unwrap();
        Ok(prefixes
            .iter()
            .map(|prefix| {
                values
                    .range(prefix.clone()..)
                    .take_while(|(key, _)| key.starts_with(prefix))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .collect())
    }

    async fn scan_prefix_paginated(
        &self,
        prefix: &str,
        limit: usize,
        after_key: Option<&str>,
    ) -> univers_aip_contracts_data::storage::RepositoryResult<Vec<(String, Vec<u8>)>> {
        let values = self.0.lock().unwrap();
        let start = after_key.filter(|after| *after >= prefix).map_or_else(
            || std::ops::Bound::Included(prefix.to_string()),
            |after| std::ops::Bound::Excluded(after.to_string()),
        );
        Ok(values
            .range((start, std::ops::Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    async fn count_prefix(
        &self,
        prefix: &str,
    ) -> univers_aip_contracts_data::storage::RepositoryResult<u64> {
        let values = self.0.lock().unwrap();
        Ok(values
            .range(prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .count() as u64)
    }

    async fn write_batch(
        &self,
        puts: Vec<(String, Vec<u8>)>,
        deletes: Vec<String>,
    ) -> univers_aip_contracts_data::storage::RepositoryResult<Vec<Option<Vec<u8>>>> {
        let mut values = self.0.lock().unwrap();
        let deleted = deletes.into_iter().map(|key| values.remove(&key)).collect();
        values.extend(puts);
        Ok(deleted)
    }

    async fn get(&self, key: &str) -> RepositoryResult<Option<Vec<u8>>> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }

    async fn put(&self, key: &str, value: &[u8]) -> RepositoryResult<()> {
        self.0
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &str) -> RepositoryResult<Option<Vec<u8>>> {
        Ok(self.0.lock().unwrap().remove(key))
    }

    async fn scan_prefix(&self, prefix: &str) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        let map = self.0.lock().unwrap();
        Ok(map
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        replacement: Option<&[u8]>,
    ) -> RepositoryResult<bool> {
        let mut values = self.0.lock().unwrap();
        if values.get(key).map(Vec::as_slice) != expected {
            return Ok(false);
        }
        match replacement {
            Some(value) => {
                values.insert(key.to_string(), value.to_vec());
            }
            None => {
                values.remove(key);
            }
        }
        Ok(true)
    }

    async fn compare_and_swap_batch(
        &self,
        guard_key: &str,
        expected: Option<&[u8]>,
        replacement: Option<&[u8]>,
        puts: Vec<(String, Vec<u8>)>,
        deletes: Vec<String>,
    ) -> RepositoryResult<Option<Vec<Option<Vec<u8>>>>> {
        let mut values = self.0.lock().unwrap();
        if values.get(guard_key).map(Vec::as_slice) != expected {
            return Ok(None);
        }
        match replacement {
            Some(value) => {
                values.insert(guard_key.to_string(), value.to_vec());
            }
            None => {
                values.remove(guard_key);
            }
        }
        let deleted = deletes.into_iter().map(|key| values.remove(&key)).collect();
        values.extend(puts);
        Ok(Some(deleted))
    }
}

fn repo() -> KvGraphRepository {
    KvGraphRepository::new(std::sync::Arc::new(MemKv::new()))
}

fn repo_with_store() -> (KvGraphRepository, std::sync::Arc<MemKv>) {
    let store = std::sync::Arc::new(MemKv::new());
    (KvGraphRepository::new(store.clone()), store)
}

// ── tests ───────────────────────────────────────────────────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn fenced_projection_rejects_stale_or_foreign_mutations_and_retains_delete_fence() {
    let (r, store) = repo_with_store();
    let metadata =
        std::collections::HashMap::from([("payload".to_string(), serde_json::json!("v1"))]);
    let owner = |revision| RelationProjectionFence {
        owner: "relation/ref-1".to_string(),
        revision,
    };

    assert_eq!(
        r.upsert_relation_projection("a", "1", "b", "2", "contains", metadata.clone(), owner(1),)
            .await
            .unwrap(),
        RelationProjectionMutation::Applied
    );
    assert_eq!(
        r.upsert_relation_projection("a", "1", "b", "2", "contains", metadata.clone(), owner(1),)
            .await
            .unwrap(),
        RelationProjectionMutation::AlreadyApplied
    );
    assert_eq!(
        r.upsert_relation_projection(
            "a",
            "1",
            "b",
            "2",
            "contains",
            metadata.clone(),
            RelationProjectionFence {
                owner: "relation/ref-2".to_string(),
                revision: 1,
            },
        )
        .await
        .unwrap(),
        RelationProjectionMutation::OwnershipMismatch { current: owner(1) }
    );
    store
        .delete("r\u{1f}b\u{1f}2\u{1f}contains\u{1f}a\u{1f}1")
        .await
        .unwrap();
    assert_eq!(
        r.upsert_relation_projection("a", "1", "b", "2", "contains", metadata.clone(), owner(1),)
            .await
            .unwrap(),
        RelationProjectionMutation::Applied
    );
    assert_eq!(
        r.find_sources("b", "2", "contains").await.unwrap(),
        vec![("a".to_string(), "1".to_string())]
    );
    assert!(r.unrelate("a", "1", "b", "2", "contains").await.is_err());
    store
        .put(
            "e\u{1f}a\u{1f}1\u{1f}contains\u{1f}b\u{1f}2",
            br#"{"payload":"legacy-mutated"}"#,
        )
        .await
        .unwrap();
    assert!(r
        .upsert_relation_projection("a", "1", "b", "2", "contains", metadata.clone(), owner(1),)
        .await
        .is_err());
    let durable = store
        .get("p\u{1f}a\u{1f}1\u{1f}contains\u{1f}b\u{1f}2")
        .await
        .unwrap()
        .unwrap();
    store
        .put("e\u{1f}a\u{1f}1\u{1f}contains\u{1f}b\u{1f}2", &durable)
        .await
        .unwrap();
    assert!(r
        .upsert_relation_projection(
            "a",
            "1",
            "b",
            "2",
            "contains",
            std::collections::HashMap::from([(
                "payload".to_string(),
                serde_json::json!("changed"),
            )]),
            owner(1),
        )
        .await
        .is_err());
    assert_eq!(
        r.delete_relation_projection("a", "1", "b", "2", "contains", owner(2))
            .await
            .unwrap(),
        RelationProjectionMutation::Applied
    );
    assert!(!r.exists("a", "1", "b", "2", "contains").await.unwrap());
    assert_eq!(
        r.upsert_relation_projection("a", "1", "b", "2", "contains", metadata.clone(), owner(1),)
            .await
            .unwrap(),
        RelationProjectionMutation::Superseded { current: owner(2) }
    );
    assert_eq!(
        r.upsert_relation_projection(
            "a",
            "1",
            "b",
            "2",
            "contains",
            metadata,
            RelationProjectionFence {
                owner: "relation/ref-2".to_string(),
                revision: 3,
            },
        )
        .await
        .unwrap(),
        RelationProjectionMutation::Applied
    );
    assert!(r.exists("a", "1", "b", "2", "contains").await.unwrap());
    assert_eq!(
        r.delete_relation_projection("a", "1", "b", "2", "contains", owner(4))
            .await
            .unwrap(),
        RelationProjectionMutation::OwnershipMismatch {
            current: RelationProjectionFence {
                owner: "relation/ref-2".to_string(),
                revision: 3,
            }
        }
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn legacy_mutators_fail_closed_for_projection_live_rows_and_tombstones() {
    let r = repo();
    let fence = RelationProjectionFence {
        owner: "relation/ref-1".to_string(),
        revision: 1,
    };
    let edge = (
        "tasks".to_string(),
        "hub".to_string(),
        "agents".to_string(),
        "a1".to_string(),
        "assigned_to".to_string(),
    );
    r.upsert_relation_projection(
        &edge.0,
        &edge.1,
        &edge.2,
        &edge.3,
        &edge.4,
        std::collections::HashMap::new(),
        fence.clone(),
    )
    .await
    .unwrap();
    r.relate("tasks", "hub", "agents", "legacy", "observes")
        .await
        .unwrap();

    assert!(r
        .relate_with_metadata(
            &edge.0,
            &edge.1,
            &edge.2,
            &edge.3,
            &edge.4,
            serde_json::json!({})
        )
        .await
        .is_err());
    assert!(r
        .update_relation_metadata(
            &edge.0,
            &edge.1,
            &edge.2,
            &edge.3,
            &edge.4,
            serde_json::json!({})
        )
        .await
        .is_err());
    assert!(r
        .unrelate(&edge.0, &edge.1, &edge.2, &edge.3, &edge.4)
        .await
        .is_err());
    assert!(r.unrelate_all("tasks", "hub", &[]).await.is_err());
    assert!(r.sweep_entity("tasks", "hub").await.is_err());
    assert!(r
        .relate_batch(vec![
            (
                "tasks".into(),
                "new".into(),
                "agents".into(),
                "a2".into(),
                "observes".into(),
            ),
            edge.clone(),
        ])
        .await
        .is_err());
    assert!(!r
        .exists("tasks", "new", "agents", "a2", "observes")
        .await
        .unwrap());
    assert!(r
        .unrelate_batch(vec![
            (
                "tasks".into(),
                "hub".into(),
                "agents".into(),
                "legacy".into(),
                "observes".into(),
            ),
            edge.clone(),
        ])
        .await
        .is_err());
    assert!(r
        .exists("tasks", "hub", "agents", "legacy", "observes")
        .await
        .unwrap());

    r.delete_relation_projection(
        &edge.0,
        &edge.1,
        &edge.2,
        &edge.3,
        &edge.4,
        RelationProjectionFence {
            owner: fence.owner,
            revision: 2,
        },
    )
    .await
    .unwrap();
    assert!(r
        .relate_with_metadata(
            &edge.0,
            &edge.1,
            &edge.2,
            &edge.3,
            &edge.4,
            serde_json::json!({})
        )
        .await
        .is_err());
    assert!(r
        .unrelate(&edge.0, &edge.1, &edge.2, &edge.3, &edge.4)
        .await
        .is_err());
    assert!(r.unrelate_all("tasks", "hub", &[]).await.is_err());
    assert!(r.sweep_entity("tasks", "hub").await.is_err());
}

#[tokio::test]
async fn relate_exists_unrelate_roundtrip() {
    let r = repo();
    // create
    let created = r.relate("a", "1", "b", "2", "contains").await.unwrap();
    assert!(created, "first relate should return true");
    // idempotent
    let again = r.relate("a", "1", "b", "2", "contains").await.unwrap();
    assert!(!again, "second relate should return false");
    // exists
    assert!(r.exists("a", "1", "b", "2", "contains").await.unwrap());
    assert!(!r.exists("a", "1", "b", "9", "contains").await.unwrap());
    // delete
    r.unrelate("a", "1", "b", "2", "contains").await.unwrap();
    assert!(!r.exists("a", "1", "b", "2", "contains").await.unwrap());
}

#[tokio::test]
async fn concurrent_relate_reports_exactly_one_create_and_keeps_both_indexes() {
    let repository = std::sync::Arc::new(repo());
    let writes = (0..32).map(|_| {
        let repository = repository.clone();
        tokio::spawn(async move {
            repository
                .relate("subject", "building", "subject", "room", "contains")
                .await
                .unwrap()
        })
    });
    let results = futures::future::join_all(writes).await;
    assert_eq!(
        results
            .into_iter()
            .filter(|result| *result.as_ref().unwrap())
            .count(),
        1
    );
    assert_eq!(
        repository
            .find_targets("subject", "building", "contains")
            .await
            .unwrap(),
        vec![("subject".to_string(), "room".to_string())]
    );
    assert_eq!(
        repository
            .find_sources("subject", "room", "contains")
            .await
            .unwrap(),
        vec![("subject".to_string(), "building".to_string())]
    );
}

#[tokio::test]
async fn retry_repairs_a_legacy_forward_only_edge() {
    let (repository, store) = repo_with_store();
    store
        .put(
            "e\u{001f}subject\u{001f}building\u{001f}contains\u{001f}subject\u{001f}room",
            b"",
        )
        .await
        .unwrap();

    let created = repository
        .relate("subject", "building", "subject", "room", "contains")
        .await
        .unwrap();
    assert!(!created);
    assert_eq!(
        repository
            .find_sources("subject", "room", "contains")
            .await
            .unwrap(),
        vec![("subject".to_string(), "building".to_string())]
    );
}

#[tokio::test]
async fn find_targets_and_sources() {
    let r = repo();
    r.relate("a", "1", "b", "2", "monitors").await.unwrap();
    r.relate("a", "1", "b", "3", "monitors").await.unwrap();
    r.relate("a", "1", "c", "x", "contains").await.unwrap();

    let targets = r.find_targets("a", "1", "monitors").await.unwrap();
    assert_eq!(targets.len(), 2);
    assert!(targets.contains(&("b".into(), "2".into())));
    assert!(targets.contains(&("b".into(), "3".into())));

    let sources = r.find_sources("b", "2", "monitors").await.unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0], ("a".into(), "1".into()));
}

#[tokio::test]
async fn find_targets_by_table_filters() {
    let r = repo();
    r.relate("a", "1", "b", "2", "monitors").await.unwrap();
    r.relate("a", "1", "c", "x", "monitors").await.unwrap();

    let filtered = r
        .find_targets_by_table("a", "1", "monitors", "c")
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0], ("c".into(), "x".into()));
}

#[tokio::test]
async fn metadata_roundtrip() {
    let r = repo();
    let meta = serde_json::json!({"role": "admin", "weight": 5});
    r.relate_with_metadata("a", "1", "b", "2", "owns", meta.clone())
        .await
        .unwrap();

    let results = r
        .find_targets_with_metadata("a", "1", "owns")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    let metadata = results[0].1.as_ref().expect("metadata should exist");
    assert_eq!(metadata.get("role").unwrap(), &serde_json::json!("admin"));
    assert_eq!(metadata.get("weight").unwrap(), &serde_json::json!(5));

    // update
    let new_meta = serde_json::json!({"role": "user"});
    r.update_relation_metadata("a", "1", "b", "2", "owns", new_meta.clone())
        .await
        .unwrap();
    let results = r
        .find_targets_with_metadata("a", "1", "owns")
        .await
        .unwrap();
    assert_eq!(
        results[0].1.as_ref().unwrap().get("role").unwrap(),
        &serde_json::json!("user")
    );
    // old key gone
    assert!(!results[0].1.as_ref().unwrap().contains_key("weight"));
}

#[tokio::test]
async fn find_related_bidirectional() {
    let r = repo();
    r.relate("a", "1", "b", "2", "connects").await.unwrap();
    r.relate("c", "x", "a", "1", "monitors").await.unwrap();

    let related = r
        .find_related("a", "1", &["connects", "monitors"])
        .await
        .unwrap();
    assert_eq!(related.len(), 2);
    assert!(related.contains(&("b".into(), "2".into())));
    assert!(related.contains(&("c".into(), "x".into())));
}

#[tokio::test]
async fn find_all_relations_grouped() {
    let r = repo();
    r.relate("a", "1", "b", "2", "monitors").await.unwrap();
    r.relate("a", "1", "c", "x", "contains").await.unwrap();

    let map = r
        .find_all_relations("a", "1", &["monitors", "contains"])
        .await
        .unwrap();
    assert_eq!(map.get("monitors").unwrap().len(), 1);
    assert_eq!(map.get("contains").unwrap().len(), 1);
}

#[tokio::test]
async fn find_by_type_scans_all() {
    let r = repo();
    r.relate("a", "1", "b", "2", "contains").await.unwrap();
    r.relate("a", "3", "c", "x", "contains").await.unwrap();
    r.relate("a", "1", "b", "9", "monitors").await.unwrap();

    let pairs = r.find_by_type("contains").await.unwrap();
    assert_eq!(pairs.len(), 2);
}

#[tokio::test]
async fn batch_create_idempotent() {
    let r = repo();
    let batch = vec![
        (
            "a".into(),
            "1".into(),
            "b".into(),
            "2".into(),
            "owns".into(),
        ),
        (
            "a".into(),
            "3".into(),
            "c".into(),
            "x".into(),
            "owns".into(),
        ),
        (
            "a".into(),
            "1".into(),
            "b".into(),
            "2".into(),
            "owns".into(),
        ), // dup
    ];
    let count = r.relate_batch(batch).await.unwrap();
    assert_eq!(count, 2, "duplicates should not be counted");
    assert!(r.exists("a", "1", "b", "2", "owns").await.unwrap());
    assert!(r.exists("a", "3", "c", "x", "owns").await.unwrap());
}

#[tokio::test]
async fn unrelate_all_removes_both_directions() {
    let r = repo();
    r.relate("a", "1", "b", "2", "contains").await.unwrap();
    r.relate("a", "1", "b", "3", "contains").await.unwrap();
    r.relate("a", "1", "c", "x", "monitors").await.unwrap();

    // delete only "contains" for a:1
    r.unrelate_all("a", "1", &["contains"]).await.unwrap();
    assert!(!r.exists("a", "1", "b", "2", "contains").await.unwrap());
    assert!(!r.exists("a", "1", "b", "3", "contains").await.unwrap());
    // monitors still there
    assert!(r.exists("a", "1", "c", "x", "monitors").await.unwrap());
    // reverse index cleaned too
    let sources = r.find_sources("b", "2", "contains").await.unwrap();
    assert!(sources.is_empty());
}

#[tokio::test]
async fn unrelate_all_removes_incoming_relations() {
    let r = repo();
    r.relate("building", "1", "chiller", "2", "contains")
        .await
        .unwrap();
    r.relate("building", "1", "chiller", "2", "monitors")
        .await
        .unwrap();

    r.unrelate_all("chiller", "2", &["contains"]).await.unwrap();

    assert!(!r
        .exists("building", "1", "chiller", "2", "contains")
        .await
        .unwrap());
    assert!(r
        .exists("building", "1", "chiller", "2", "monitors")
        .await
        .unwrap());
    assert!(r
        .find_sources("chiller", "2", "contains")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn get_stats_counts_by_type() {
    let r = repo();
    r.relate("a", "1", "b", "2", "contains").await.unwrap();
    r.relate("a", "1", "b", "3", "contains").await.unwrap();
    r.relate("a", "1", "c", "x", "monitors").await.unwrap();

    let stats = r.get_stats(&["contains"]).await.unwrap();
    assert_eq!(stats.total, 2);
    assert_eq!(*stats.by_type.get("contains").unwrap(), 2);
    assert!(!stats.by_type.contains_key("monitors"));
}

#[tokio::test]
async fn pagination_skip_take() {
    let r = repo();
    for i in 0..10 {
        r.relate("a", "1", "b", &i.to_string(), "contains")
            .await
            .unwrap();
    }

    let page1 = r
        .find_targets_paginated("a", "1", "contains", 3, 0)
        .await
        .unwrap();
    let page2 = r
        .find_targets_paginated("a", "1", "contains", 3, 3)
        .await
        .unwrap();
    let page4 = r
        .find_targets_paginated("a", "1", "contains", 3, 9)
        .await
        .unwrap();

    assert_eq!(page1.len(), 3);
    assert_eq!(page2.len(), 3);
    assert_eq!(page4.len(), 1);
    // no overlap
    let p1: std::collections::HashSet<_> = page1.into_iter().collect();
    let p2: std::collections::HashSet<_> = page2.into_iter().collect();
    assert!(p1.is_disjoint(&p2));
}

#[tokio::test]
async fn sweep_entity_removes_both_directions_and_twins() {
    let repo = repo();
    // A -> B, C -> A: A has one outgoing and one incoming edge.
    repo.relate("t", "a", "t", "b", "feeds").await.unwrap();
    repo.relate("t", "c", "t", "a", "feeds").await.unwrap();

    let swept = repo.sweep_entity("t", "a").await.unwrap();
    assert_eq!(swept, 2, "one outgoing + one incoming edge");

    // Point lookups miss in both directions.
    assert!(!repo.exists("t", "a", "t", "b", "feeds").await.unwrap());
    assert!(!repo.exists("t", "c", "t", "a", "feeds").await.unwrap());
    // The OTHER endpoints' indexes are clean too (no ghost twins):
    // C's forward index no longer lists A...
    assert!(repo
        .find_targets("t", "c", "feeds")
        .await
        .unwrap()
        .is_empty());
    // ...and B's reverse index no longer lists A.
    assert!(repo
        .find_sources("t", "b", "feeds")
        .await
        .unwrap()
        .is_empty());

    // Idempotent: re-sweeping a clean entity is a no-op.
    assert_eq!(repo.sweep_entity("t", "a").await.unwrap(), 0);
}

#[tokio::test]
async fn snapshot_exports_the_entire_graph_with_original_bytes_and_actual_endpoints() {
    let (repository, store) = repo_with_store();
    for id in 0..1001 {
        repository
            .relate("source", "root", "target", &id.to_string(), "k")
            .await
            .unwrap();
    }
    // Historical raw metadata preserves JSON spelling, wide integers and null.
    let raw = b"{ \"wide\":18446744073709551615, \"nullable\":null }".to_vec();
    {
        let mut values = store.0.lock().unwrap();
        values.insert(
            "e\x1fsource\x1froot\x1fk\x1ftarget\x1f0".into(),
            raw.clone(),
        );
        values.insert(
            "r\x1ftarget\x1f0\x1fk\x1fsource\x1froot".into(),
            raw.clone(),
        );
    }
    repository
        .relate_with_metadata("s", "null", "t", "null", "k", serde_json::Value::Null)
        .await
        .unwrap();
    let fence = |revision| RelationProjectionFence {
        owner: "stored-row".into(),
        revision,
    };
    repository
        .upsert_relation_projection("s", "live", "t", "live", "k", Default::default(), fence(1))
        .await
        .unwrap();
    repository
        .delete_relation_projection("s", "gone", "t", "gone", "k", fence(2))
        .await
        .unwrap();
    let before = store.0.lock().unwrap().clone();
    let snapshot = repository.snapshot_export().await.unwrap();
    assert_eq!(store.1.load(Ordering::SeqCst), 1);
    assert_eq!(*store.0.lock().unwrap(), before);
    assert_eq!(snapshot.edges.len(), 1003);
    let edge = snapshot
        .edges
        .iter()
        .find(|edge| edge.source.table == "source" && edge.target.id == "0")
        .unwrap();
    assert_eq!(edge.physical_key, "e\x1fsource\x1froot\x1fk\x1ftarget\x1f0");
    assert_eq!(edge.source.id, "root");
    assert_eq!(edge.target.table, "target");
    assert_eq!(edge.kind, "k");
    assert_eq!(edge.raw_metadata, raw);
    assert_eq!(
        edge.metadata.as_ref().unwrap()["wide"].as_u64(),
        Some(u64::MAX)
    );
    assert!(edge.metadata.as_ref().unwrap()["nullable"].is_null());
    assert!(snapshot.edges.iter().any(|edge| edge.metadata.is_none()));
    assert!(snapshot
        .edges
        .iter()
        .any(|edge| edge.metadata == Some(serde_json::Value::Null)));
    assert!(snapshot.edges.iter().any(|edge| edge.source.id == "live"));
    assert!(snapshot
        .endpoints
        .iter()
        .all(|endpoint| endpoint.id != "gone"));
    // Only endpoint pairs are available, not synthesized entity properties.
    assert!(repository
        .get_record_json("source", "root")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn snapshot_rejects_bad_indexes_and_metadata_without_changing_any_key() {
    let (repository, store) = repo_with_store();
    repository.relate("s", "1", "t", "2", "k").await.unwrap();
    let original = store.0.lock().unwrap().clone();
    let forward = "e\x1fs\x1f1\x1fk\x1ft\x1f2";
    let reverse = "r\x1ft\x1f2\x1fk\x1fs\x1f1";
    let state = "p\x1fs\x1f1\x1fk\x1ft\x1f2";
    let mut corruptions = Vec::new();
    let mut values = original.clone();
    values.remove(forward);
    corruptions.push(values);
    let mut values = original.clone();
    values.remove(reverse);
    corruptions.push(values);
    let mut values = original.clone();
    values.insert(reverse.into(), b"null".to_vec());
    corruptions.push(values);
    let mut values = original.clone();
    let raw = values.remove(forward).unwrap();
    values.insert(format!("{forward}\x1fextra"), raw);
    corruptions.push(values);
    let mut values = original.clone();
    values.insert(forward.into(), b"{invalid".to_vec());
    values.insert(reverse.into(), b"{invalid".to_vec());
    corruptions.push(values);
    let mut values = original.clone();
    values.insert(state.into(), b"unknown-state".to_vec());
    corruptions.push(values);
    for values in corruptions {
        *store.0.lock().unwrap() = values.clone();
        assert!(repository.snapshot_export().await.is_err());
        assert_eq!(*store.0.lock().unwrap(), values);
    }
}

#[tokio::test]
async fn snapshot_does_not_export_incomplete_projection_repairs_or_tombstone_edges() {
    let (repository, store) = repo_with_store();
    let fence = |revision| RelationProjectionFence {
        owner: "stored-row".into(),
        revision,
    };
    repository
        .upsert_relation_projection("s", "1", "t", "2", "k", Default::default(), fence(1))
        .await
        .unwrap();
    let original = store.0.lock().unwrap().clone();
    let forward = "e\x1fs\x1f1\x1fk\x1ft\x1f2";
    let reverse = "r\x1ft\x1f2\x1fk\x1fs\x1f1";
    let state = "p\x1fs\x1f1\x1fk\x1ft\x1f2";
    for keys in [vec![forward], vec![forward, reverse], vec![state]] {
        let mut values = original.clone();
        for key in keys {
            values.remove(key);
        }
        *store.0.lock().unwrap() = values.clone();
        assert!(repository.snapshot_export().await.is_err());
        assert_eq!(*store.0.lock().unwrap(), values);
    }
    *store.0.lock().unwrap() = original.clone();
    repository
        .delete_relation_projection("s", "1", "t", "2", "k", fence(2))
        .await
        .unwrap();
    assert!(repository.snapshot_export().await.unwrap().edges.is_empty());
    let mut values = store.0.lock().unwrap().clone();
    values.insert(forward.into(), original[forward].clone());
    values.insert(reverse.into(), original[reverse].clone());
    *store.0.lock().unwrap() = values.clone();
    assert!(repository.snapshot_export().await.is_err());
    assert_eq!(*store.0.lock().unwrap(), values);
}

#[tokio::test]
async fn snapshot_capability_is_required_without_falling_back_to_separate_scans() {
    let store = Arc::new(MemKv(
        Mutex::new(BTreeMap::new()),
        AtomicUsize::new(0),
        false,
    ));
    let repository = KvGraphRepository::new(store.clone());
    repository.relate("s", "1", "t", "2", "k").await.unwrap();
    let before = store.0.lock().unwrap().clone();
    assert!(repository.snapshot_export().await.is_err());
    assert_eq!(store.1.load(Ordering::SeqCst), 1);
    assert_eq!(*store.0.lock().unwrap(), before);
}
