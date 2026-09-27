//! Retry-safe application helper for relation-record-owned World relations.

use std::sync::Arc;

use univers_aip_contracts_data::core::{
    DataKind, DataPointError, DataPointResult, DataProvenance, DataRef, DataTemporalMetadata,
    DataWriteReceipt,
};
use univers_aip_contracts_world::relation::{
    relation_recorded_at, DataRelation, DataRelationPort, DataRelationReplaceRequest,
    DataRelationReplacementDirection, DataRelationType,
};

/// Lightweight coordinator over the canonical typed Relation Port.
///
/// Source-object-owned relations are intentionally rejected by the World Kernel
/// and must be projected by the store that owns the source object.
#[derive(Clone)]
pub struct WorldRelationClient {
    relations: Arc<dyn DataRelationPort>,
    owner: String,
}

impl WorldRelationClient {
    pub fn new(
        relations: Arc<dyn DataRelationPort>,
        owner: impl Into<String>,
    ) -> DataPointResult<Self> {
        let owner = owner.into();
        validate_owner(&owner)?;
        Ok(Self { relations, owner })
    }

    /// Build the stable identity shared by retries of one canonical edge.
    pub fn reference(
        &self,
        source: &DataRef,
        target: &DataRef,
        relation_type: DataRelationType,
    ) -> DataPointResult<DataRef> {
        let (source, target) = canonical_endpoints(source, target, relation_type)?;
        let natural_key = serde_json::to_vec(&(&source, &target, relation_type))
            .map_err(|error| DataPointError::Serialization(error.to_string()))?;
        DataRef::new(
            source.organization_id(),
            DataKind::Relation,
            uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, &natural_key).to_string(),
        )
    }

    /// Converge one relation-record-owned edge. Repeating the same logical
    /// payload preserves its original recording time and replays safely.
    pub async fn ensure(
        &self,
        source: DataRef,
        target: DataRef,
        relation_type: DataRelationType,
        provenance: DataProvenance,
        temporal: DataTemporalMetadata,
        metadata: serde_json::Value,
    ) -> DataPointResult<DataWriteReceipt> {
        self.ensure_inner(
            source,
            target,
            relation_type,
            provenance,
            temporal,
            metadata,
            None,
        )
        .await
    }

    /// Converge one relation using a caller-owned durable idempotency key.
    ///
    /// The key is persisted by the canonical Relation Port, so a retry after
    /// this process restarts can still return a replayed durable receipt.
    #[allow(clippy::too_many_arguments)]
    pub async fn ensure_with_idempotency_key(
        &self,
        source: DataRef,
        target: DataRef,
        relation_type: DataRelationType,
        provenance: DataProvenance,
        temporal: DataTemporalMetadata,
        metadata: serde_json::Value,
        idempotency_key: &str,
    ) -> DataPointResult<DataWriteReceipt> {
        self.ensure_inner(
            source,
            target,
            relation_type,
            provenance,
            temporal,
            metadata,
            Some(idempotency_key),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn ensure_inner(
        &self,
        source: DataRef,
        target: DataRef,
        relation_type: DataRelationType,
        provenance: DataProvenance,
        temporal: DataTemporalMetadata,
        metadata: serde_json::Value,
        idempotency_key: Option<&str>,
    ) -> DataPointResult<DataWriteReceipt> {
        let (source, target) = canonical_endpoints(&source, &target, relation_type)?;
        let existing = self.find_existing(&source, &target, relation_type).await?;
        let reference = existing
            .as_ref()
            .map(|relation| relation.reference.clone())
            .unwrap_or(self.reference(&source, &target, relation_type)?);
        let metadata = owned_metadata(metadata, &self.owner)?;
        let mut relation = DataRelation {
            reference,
            source,
            target,
            relation_type,
            provenance,
            temporal,
            metadata,
        };
        relation.validate()?;
        if let Some(existing) = existing {
            ensure_same_owner(&existing, &self.owner)?;
            if idempotency_key.is_some() {
                relation.temporal = existing.temporal.clone();
            } else {
                relation.preserve_recorded_at_for_replay(&existing);
            }
        }
        let key = match idempotency_key {
            Some(key) => key.to_string(),
            None => mutation_key(&self.owner, "upsert", &relation)?,
        };
        self.relations.upsert(relation, &key).await
    }

    /// Convenience for a currently reported relation without a validity window.
    pub async fn ensure_reported(
        &self,
        source: DataRef,
        target: DataRef,
        relation_type: DataRelationType,
        metadata: serde_json::Value,
    ) -> DataPointResult<DataWriteReceipt> {
        self.ensure(
            source,
            target,
            relation_type,
            DataProvenance::Reported,
            relation_recorded_at(chrono::Utc::now()),
            metadata,
        )
        .await
    }

    /// Convenience for a reported relation with a caller-owned durable key.
    pub async fn ensure_reported_with_idempotency_key(
        &self,
        source: DataRef,
        target: DataRef,
        relation_type: DataRelationType,
        metadata: serde_json::Value,
        idempotency_key: &str,
    ) -> DataPointResult<DataWriteReceipt> {
        self.ensure_with_idempotency_key(
            source,
            target,
            relation_type,
            DataProvenance::Reported,
            relation_recorded_at(chrono::Utc::now()),
            metadata,
            idempotency_key,
        )
        .await
    }

    /// Atomically replace all outgoing edges of one relation type for a source.
    pub async fn replace_outgoing(
        &self,
        source: DataRef,
        target: DataRef,
        relation_type: DataRelationType,
        replaced: Vec<DataRef>,
        metadata: serde_json::Value,
    ) -> DataPointResult<DataWriteReceipt> {
        self.replace_outgoing_inner(source, target, relation_type, replaced, metadata, None)
            .await
    }

    /// Atomically replace outgoing edges using a caller-owned durable key.
    pub async fn replace_outgoing_with_idempotency_key(
        &self,
        source: DataRef,
        target: DataRef,
        relation_type: DataRelationType,
        replaced: Vec<DataRef>,
        metadata: serde_json::Value,
        idempotency_key: &str,
    ) -> DataPointResult<DataWriteReceipt> {
        self.replace_outgoing_inner(
            source,
            target,
            relation_type,
            replaced,
            metadata,
            Some(idempotency_key),
        )
        .await
    }

    /// Atomically replace incoming edges using a caller-owned durable key.
    pub async fn replace_incoming_with_idempotency_key(
        &self,
        source: DataRef,
        target: DataRef,
        relation_type: DataRelationType,
        replaced: Vec<DataRef>,
        metadata: serde_json::Value,
        idempotency_key: &str,
    ) -> DataPointResult<DataWriteReceipt> {
        let (source, target) = canonical_endpoints(&source, &target, relation_type)?;
        let relation = DataRelation {
            reference: self.reference(&source, &target, relation_type)?,
            source,
            target,
            relation_type,
            provenance: DataProvenance::Reported,
            temporal: relation_recorded_at(chrono::Utc::now()),
            metadata: owned_metadata(metadata, &self.owner)?,
        };
        self.relations
            .replace_incoming(DataRelationReplaceRequest {
                replacement: relation,
                replaced,
                idempotency_key: idempotency_key.to_string(),
                direction: DataRelationReplacementDirection::Incoming,
            })
            .await
    }

    async fn replace_outgoing_inner(
        &self,
        source: DataRef,
        target: DataRef,
        relation_type: DataRelationType,
        replaced: Vec<DataRef>,
        metadata: serde_json::Value,
        idempotency_key: Option<&str>,
    ) -> DataPointResult<DataWriteReceipt> {
        let (source, target) = canonical_endpoints(&source, &target, relation_type)?;
        let relation = DataRelation {
            reference: self.reference(&source, &target, relation_type)?,
            source,
            target,
            relation_type,
            provenance: DataProvenance::Reported,
            temporal: relation_recorded_at(chrono::Utc::now()),
            metadata: owned_metadata(metadata, &self.owner)?,
        };
        let key = match idempotency_key {
            Some(key) => key.to_string(),
            None => mutation_key(&self.owner, "replace", &relation)?,
        };
        self.relations
            .replace_outgoing(DataRelationReplaceRequest {
                replacement: relation,
                replaced,
                idempotency_key: key,
                direction: DataRelationReplacementDirection::Outgoing,
            })
            .await
    }

    /// Idempotently remove the stable edge identified by its endpoints and type.
    pub async fn remove(
        &self,
        source: &DataRef,
        target: &DataRef,
        relation_type: DataRelationType,
    ) -> DataPointResult<DataWriteReceipt> {
        self.remove_inner(source, target, relation_type, None).await
    }

    /// Remove one edge using a caller-owned durable idempotency key.
    pub async fn remove_with_idempotency_key(
        &self,
        source: &DataRef,
        target: &DataRef,
        relation_type: DataRelationType,
        idempotency_key: &str,
    ) -> DataPointResult<DataWriteReceipt> {
        self.remove_inner(source, target, relation_type, Some(idempotency_key))
            .await
    }

    /// Recover a keyed deletion receipt when the active edge is already gone.
    pub async fn replay_delete_by_idempotency_key(
        &self,
        source: &DataRef,
        relation_type: DataRelationType,
        idempotency_key: &str,
    ) -> DataPointResult<Option<DataWriteReceipt>> {
        self.relations
            .replay_delete_by_idempotency_key(source, relation_type, idempotency_key)
            .await
    }

    async fn remove_inner(
        &self,
        source: &DataRef,
        target: &DataRef,
        relation_type: DataRelationType,
        idempotency_key: Option<&str>,
    ) -> DataPointResult<DataWriteReceipt> {
        let (source, target) = canonical_endpoints(source, target, relation_type)?;
        let existing = self.find_existing(&source, &target, relation_type).await?;
        if existing.is_none() {
            if let Some(idempotency_key) = idempotency_key {
                if let Some(receipt) = self
                    .relations
                    .replay_delete_by_idempotency_key(&source, relation_type, idempotency_key)
                    .await?
                {
                    return Ok(receipt);
                }
            }
        }
        let reference = existing
            .as_ref()
            .map(|relation| relation.reference.clone())
            .unwrap_or(self.reference(&source, &target, relation_type)?);
        if let Some(existing) = existing {
            ensure_same_owner(&existing, &self.owner)?;
        }
        let key = match idempotency_key {
            Some(key) => key.to_string(),
            None => format!("{}:relation:delete:{}", self.owner, reference.id()),
        };
        self.relations.delete(&reference, &key).await
    }

    async fn find_existing(
        &self,
        source: &DataRef,
        target: &DataRef,
        relation_type: DataRelationType,
    ) -> DataPointResult<Option<DataRelation>> {
        let relations = self.relations.find_targets(source, relation_type).await?;
        validate_query_result(&relations, source, relation_type, true)?;
        let mut matches = relations
            .into_iter()
            .filter(|relation| relation.target == *target);
        let first = matches.next();
        if matches.next().is_some() {
            return Err(DataPointError::Storage(format!(
                "multiple relation records claim the same natural edge {:?}:{} -> {:?}:{} ({relation_type})",
                source.kind(),
                source.id(),
                target.kind(),
                target.id()
            )));
        }
        Ok(first)
    }

    pub async fn targets(
        &self,
        source: &DataRef,
        relation_type: DataRelationType,
    ) -> DataPointResult<Vec<DataRef>> {
        Ok(self
            .target_relations(source, relation_type)
            .await?
            .into_iter()
            .map(|relation| relation.target)
            .collect())
    }

    pub async fn sources(
        &self,
        target: &DataRef,
        relation_type: DataRelationType,
    ) -> DataPointResult<Vec<DataRef>> {
        Ok(self
            .source_relations(target, relation_type)
            .await?
            .into_iter()
            .map(|relation| relation.source)
            .collect())
    }

    pub async fn target_relations(
        &self,
        source: &DataRef,
        relation_type: DataRelationType,
    ) -> DataPointResult<Vec<DataRelation>> {
        let relations = self.relations.find_targets(source, relation_type).await?;
        validate_query_result(&relations, source, relation_type, true)?;
        Ok(relations)
    }

    pub async fn source_relations(
        &self,
        target: &DataRef,
        relation_type: DataRelationType,
    ) -> DataPointResult<Vec<DataRelation>> {
        let relations = self.relations.find_sources(target, relation_type).await?;
        validate_query_result(&relations, target, relation_type, false)?;
        Ok(relations)
    }
}

fn canonical_endpoints(
    source: &DataRef,
    target: &DataRef,
    relation_type: DataRelationType,
) -> DataPointResult<(DataRef, DataRef)> {
    if source.organization_id() != target.organization_id() {
        return Err(DataPointError::Validation(
            "relation source and target must belong to the same organization".to_string(),
        ));
    }
    let mut source = source.clone();
    let mut target = target.clone();
    if relation_type.as_standard().is_symmetric()
        && reference_token(&target)? < reference_token(&source)?
    {
        std::mem::swap(&mut source, &mut target);
    }
    Ok((source, target))
}

fn reference_token(reference: &DataRef) -> DataPointResult<Vec<u8>> {
    serde_json::to_vec(reference).map_err(|error| DataPointError::Serialization(error.to_string()))
}

fn owned_metadata(metadata: serde_json::Value, owner: &str) -> DataPointResult<serde_json::Value> {
    let mut metadata = metadata.as_object().cloned().ok_or_else(|| {
        DataPointError::Validation("relation metadata must be a JSON object".to_string())
    })?;
    match metadata.get("owner").and_then(serde_json::Value::as_str) {
        Some(existing) if existing != owner => {
            return Err(DataPointError::InvalidOperation(format!(
                "relation metadata owner '{existing}' does not match client owner '{owner}'"
            )));
        }
        Some(_) => {}
        None => {
            metadata.insert(
                "owner".to_string(),
                serde_json::Value::String(owner.to_string()),
            );
        }
    }
    Ok(serde_json::Value::Object(metadata))
}

fn ensure_same_owner(relation: &DataRelation, owner: &str) -> DataPointResult<()> {
    match relation
        .metadata
        .get("owner")
        .and_then(serde_json::Value::as_str)
    {
        Some(existing) if existing == owner => Ok(()),
        Some(existing) => Err(DataPointError::InvalidOperation(format!(
            "relation '{}' is owned by '{existing}', not '{owner}'",
            relation.reference.id()
        ))),
        None => Err(DataPointError::InvalidOperation(format!(
            "relation '{}' has no owner metadata and cannot be mutated by '{owner}'",
            relation.reference.id()
        ))),
    }
}

fn mutation_key(owner: &str, operation: &str, relation: &DataRelation) -> DataPointResult<String> {
    let payload = serde_json::to_vec(relation)
        .map_err(|error| DataPointError::Serialization(error.to_string()))?;
    let digest = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, &payload);
    Ok(format!(
        "{owner}:relation:{operation}:{}:{digest}",
        relation.reference.id()
    ))
}

fn validate_owner(owner: &str) -> DataPointResult<()> {
    if owner.is_empty() || owner.trim() != owner || owner.contains(char::is_whitespace) {
        return Err(DataPointError::Validation(
            "relation client owner must be a non-empty identity without whitespace".to_string(),
        ));
    }
    Ok(())
}

fn validate_query_result(
    relations: &[DataRelation],
    endpoint: &DataRef,
    relation_type: DataRelationType,
    outgoing: bool,
) -> DataPointResult<()> {
    for relation in relations {
        relation.validate()?;
        let actual = if outgoing {
            &relation.source
        } else {
            &relation.target
        };
        if actual != endpoint || relation.relation_type != relation_type {
            return Err(DataPointError::Storage(format!(
                "Relation Port returned unrelated relation '{}' for {:?} '{}'",
                relation.reference.id(),
                endpoint.kind(),
                endpoint.id()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use async_trait::async_trait;
    use univers_aip_contracts_data::core::DataWriteOutcome;
    use univers_aip_contracts_world::relation::{DataRelationPage, DataRelationQuery};

    use super::*;

    #[derive(Default)]
    struct MemoryRelations {
        records: Mutex<HashMap<DataRef, DataRelation>>,
        idempotency_keys: Mutex<HashMap<DataRef, String>>,
    }

    #[async_trait]
    impl DataRelationPort for MemoryRelations {
        async fn upsert(
            &self,
            relation: DataRelation,
            idempotency_key: &str,
        ) -> DataPointResult<DataWriteReceipt> {
            relation.validate()?;
            let reference = relation.reference.clone();
            let mut records = self.records.lock().unwrap();
            let mut keys = self.idempotency_keys.lock().unwrap();
            if records.get(&reference) == Some(&relation)
                && keys
                    .get(&reference)
                    .is_some_and(|key| key == idempotency_key)
            {
                let mut receipt = DataWriteReceipt::new(
                    DataWriteOutcome::Skipped,
                    reference,
                    relation_recorded_at(chrono::Utc::now()).recorded_at,
                )
                .with_idempotency_key(idempotency_key)?;
                receipt.mark_replayed()?;
                return Ok(receipt);
            }
            let outcome = if records.insert(reference.clone(), relation).is_some() {
                DataWriteOutcome::Skipped
            } else {
                DataWriteOutcome::Created
            };
            keys.insert(reference.clone(), idempotency_key.to_string());
            DataWriteReceipt::new(
                outcome,
                reference,
                relation_recorded_at(chrono::Utc::now()).recorded_at,
            )
            .with_idempotency_key(idempotency_key)
        }

        async fn delete(
            &self,
            reference: &DataRef,
            idempotency_key: &str,
        ) -> DataPointResult<DataWriteReceipt> {
            self.records.lock().unwrap().remove(reference);
            DataWriteReceipt::new(
                DataWriteOutcome::Deleted,
                reference.clone(),
                relation_recorded_at(chrono::Utc::now()).recorded_at,
            )
            .with_idempotency_key(idempotency_key)
        }

        async fn get(&self, reference: &DataRef) -> DataPointResult<Option<DataRelation>> {
            Ok(self.records.lock().unwrap().get(reference).cloned())
        }

        async fn list(&self, query: DataRelationQuery) -> DataPointResult<DataRelationPage> {
            let items = self
                .records
                .lock()
                .unwrap()
                .values()
                .filter(|relation| query.matches(relation))
                .cloned()
                .collect();
            Ok(DataRelationPage {
                items,
                next_cursor: None,
            })
        }

        async fn find_targets(
            &self,
            source: &DataRef,
            relation_type: DataRelationType,
        ) -> DataPointResult<Vec<DataRelation>> {
            Ok(self
                .records
                .lock()
                .unwrap()
                .values()
                .filter(|relation| {
                    relation.source == *source && relation.relation_type == relation_type
                })
                .cloned()
                .collect())
        }

        async fn find_sources(
            &self,
            target: &DataRef,
            relation_type: DataRelationType,
        ) -> DataPointResult<Vec<DataRelation>> {
            Ok(self
                .records
                .lock()
                .unwrap()
                .values()
                .filter(|relation| {
                    relation.target == *target && relation.relation_type == relation_type
                })
                .cloned()
                .collect())
        }
    }

    fn subject(id: &str) -> DataRef {
        DataRef::new("org-a", DataKind::Subject, id).unwrap()
    }

    #[tokio::test]
    async fn replay_preserves_identity_and_original_recording_time() {
        let store = Arc::new(MemoryRelations::default());
        let client =
            WorldRelationClient::new(store.clone(), "cognitive").expect("valid relation client");
        let source = subject("conversation-1");
        let target = subject("message-1");

        let first = client
            .ensure_reported(
                source.clone(),
                target.clone(),
                DataRelationType::contains(),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        let first_relation = store.get(&first.reference).await.unwrap().unwrap();
        let replay = client
            .ensure_reported(
                source,
                target,
                DataRelationType::contains(),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        let replayed_relation = store.get(&replay.reference).await.unwrap().unwrap();

        assert_eq!(first.reference, replay.reference);
        assert_eq!(
            first_relation.temporal.recorded_at,
            replayed_relation.temporal.recorded_at
        );
    }

    #[tokio::test]
    async fn caller_owned_key_replays_after_client_restart() {
        let store = Arc::new(MemoryRelations::default());
        let source = subject("member-1");
        let target = subject("position-1");
        let first = WorldRelationClient::new(store.clone(), "org-members").unwrap();
        let first_receipt = first
            .ensure_reported_with_idempotency_key(
                source.clone(),
                target.clone(),
                DataRelationType::new("appointed_to").unwrap(),
                serde_json::json!({}),
                "org-assignment:restart-1",
            )
            .await
            .unwrap();
        assert!(!first_receipt.replayed);

        // A fresh client models a restarted HTTP/service process while the
        // canonical relation authority remains durable and shared.
        let restarted = WorldRelationClient::new(store, "org-members").unwrap();
        let replay = restarted
            .ensure_reported_with_idempotency_key(
                source,
                target,
                DataRelationType::new("appointed_to").unwrap(),
                serde_json::json!({}),
                "org-assignment:restart-1",
            )
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(
            replay.idempotency_key.as_deref(),
            Some("org-assignment:restart-1")
        );
    }

    #[tokio::test]
    async fn symmetric_identity_is_independent_of_endpoint_order() {
        let client =
            WorldRelationClient::new(Arc::new(MemoryRelations::default()), "cognitive").unwrap();
        let left = subject("conversation-1");
        let right = subject("conversation-2");
        let relation_type = DataRelationType::new("associated_with").unwrap();

        assert_eq!(
            client.reference(&left, &right, relation_type).unwrap(),
            client.reference(&right, &left, relation_type).unwrap()
        );
    }

    #[tokio::test]
    async fn one_owner_cannot_overwrite_another_owners_edge() {
        let store = Arc::new(MemoryRelations::default());
        let first = WorldRelationClient::new(store.clone(), "owner-a").unwrap();
        let second = WorldRelationClient::new(store, "owner-b").unwrap();
        let source = subject("source");
        let target = subject("target");

        first
            .ensure_reported(
                source.clone(),
                target.clone(),
                DataRelationType::contains(),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        let error = second
            .ensure_reported(
                source,
                target,
                DataRelationType::contains(),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, DataPointError::InvalidOperation(_)));
    }

    #[tokio::test]
    async fn existing_natural_edge_keeps_its_stable_reference() {
        let store = Arc::new(MemoryRelations::default());
        let source = subject("source");
        let target = subject("target");
        let legacy_reference =
            DataRef::new("org-a", DataKind::Relation, "existing-stable-id").unwrap();
        store
            .upsert(
                DataRelation {
                    reference: legacy_reference.clone(),
                    source: source.clone(),
                    target: target.clone(),
                    relation_type: DataRelationType::contains(),
                    provenance: DataProvenance::Reported,
                    temporal: relation_recorded_at(chrono::Utc::now()),
                    metadata: serde_json::json!({"owner": "cognitive"}),
                },
                "fixture-existing-edge",
            )
            .await
            .unwrap();
        let client = WorldRelationClient::new(store, "cognitive").unwrap();

        let receipt = client
            .ensure_reported(
                source,
                target,
                DataRelationType::contains(),
                serde_json::json!({}),
            )
            .await
            .unwrap();

        assert_eq!(receipt.reference, legacy_reference);
    }
}
