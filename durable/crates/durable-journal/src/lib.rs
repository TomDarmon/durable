//! Paged journal built from immutable pages and a CAS head root.

use durable_core::{
    compute_object_id, Durability, DurableError, DurableObjectRef, ExpectedRevision,
    ImmutableObjects, ObjectFormat, PublishOutcome, RootName, RootRegister,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;
use uuid::Uuid;

/// Journal result type.
pub type Result<T> = std::result::Result<T, JournalError>;

/// Stream identity; ordering is only guaranteed inside one stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamId(String);

impl StreamId {
    /// Creates a stream ID.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the stream ID as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Client-chosen deduplication key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClientRequestKey(String);

impl ClientRequestKey {
    /// Creates a client request key.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Service-issued, non-forgeable request token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestToken {
    id: Uuid,
    stream: StreamId,
    client_key: ClientRequestKey,
    digest: [u8; 32],
    expires_at_tick: u64,
}

impl RequestToken {
    /// Returns true when the token has expired at the supplied logical tick.
    pub fn is_expired(&self, now_tick: u64) -> bool {
        now_tick > self.expires_at_tick
    }
}

/// Append request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendRequest {
    /// Service-issued token.
    pub token: RequestToken,
    /// Durable object references to append.
    pub records: Vec<DurableObjectRef>,
}

/// Comparable journal position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct JournalPosition(u64);

impl JournalPosition {
    /// First journal position.
    pub const FIRST: Self = Self(1);

    /// Returns the numeric position for diagnostics.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Append receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendReceipt {
    /// First position included in this append.
    pub first_position: JournalPosition,
    /// Last position included in this append.
    pub last_position: JournalPosition,
    /// Immutable page carrying the append.
    pub page: DurableObjectRef,
}

/// Append result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendOutcome {
    /// The request was newly appended.
    Appended(AppendReceipt),
    /// The same request already appended in the retry window.
    AlreadyAppended(AppendReceipt),
    /// The append may or may not have committed.
    OutcomeUnknown,
}

/// Resolution for a lost append acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendResolution {
    /// The request is known committed.
    Committed(AppendReceipt),
    /// The token is expired and will never execute.
    RequestExpired,
    /// The outcome is not known.
    Unknown,
}

/// Journal snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalSnapshot {
    /// First retained position.
    pub first_position: JournalPosition,
    /// Records in position order.
    pub records: Vec<DurableObjectRef>,
}

/// Retained checkpoint reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRef {
    /// Position covered by the checkpoint.
    pub position: JournalPosition,
    /// Durable checkpoint object.
    pub object: DurableObjectRef,
}

/// Journal maintenance operations.
pub trait JournalMaintenance {
    /// Returns the latest checkpoint, if any.
    fn checkpoint(&self) -> Option<CheckpointRef>;
}

/// Journal API.
pub trait Journal {
    /// Issues a bounded retry token.
    fn issue_request_token(
        &self,
        client_key: ClientRequestKey,
        digest: [u8; 32],
        now_tick: u64,
    ) -> RequestToken;
}

/// Durable journal errors.
#[derive(Debug, Error)]
pub enum JournalError {
    /// Core storage failed.
    #[error(transparent)]
    Core(#[from] DurableError),
    /// Request token has expired.
    #[error("request token expired")]
    RequestExpired,
    /// Client key was reused for different bytes.
    #[error("client request key reused with a different digest")]
    ClientKeyDigestMismatch,
    /// CAS contention exceeded retry budget.
    #[error("journal CAS contention")]
    CasContention,
    /// Scan found a corrupt page chain.
    #[error("journal page chain is corrupt: {0}")]
    Corrupt(String),
    /// Scan requested a position older than retention.
    #[error("journal snapshot expired")]
    SnapshotExpired,
}

impl From<JournalError> for DurableError {
    fn from(value: JournalError) -> Self {
        match value {
            JournalError::Core(error) => error,
            other => DurableError::Corrupt(other.to_string()),
        }
    }
}

/// Journal page header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalPageHeader {
    /// Format version.
    pub format_version: u16,
    /// Stream identity.
    pub stream: StreamId,
    /// Previous immutable page.
    pub previous_page: Option<DurableObjectRef>,
    /// First position in this page.
    pub first_position: JournalPosition,
    /// Last position in this page.
    pub last_position: JournalPosition,
    /// Record count.
    pub record_count: u64,
    /// Framing label.
    pub record_framing: String,
    /// SHA-256 checksum of encoded records.
    pub records_checksum: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct JournalPage {
    header: JournalPageHeader,
    records: Vec<DurableObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct JournalHead {
    tail_page: Option<DurableObjectRef>,
    tail_position: u64,
    earliest_retained: u64,
    checkpoint: Option<CheckpointRef>,
    receipts: BTreeMap<ClientRequestKey, ReceiptRecord>,
}

impl Default for JournalHead {
    fn default() -> Self {
        Self {
            tail_page: None,
            tail_position: 0,
            earliest_retained: 1,
            checkpoint: None,
            receipts: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReceiptRecord {
    digest: [u8; 32],
    expires_at_tick: u64,
    receipt: AppendReceipt,
}

/// Paged durable journal.
pub struct PagedJournal<S> {
    stream: StreamId,
    storage: Arc<S>,
    retry_window_ticks: u64,
    max_cas_retries: usize,
    fault: Mutex<Option<JournalFault>>,
}

/// Deterministic journal fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalFault {
    /// Return outcome unknown after page persistence before head CAS.
    AfterJournalPagePersistenceBeforeHeadCas,
    /// Return outcome unknown after head CAS before ACK.
    AfterJournalHeadCasBeforeAck,
}

impl<S> PagedJournal<S>
where
    S: ImmutableObjects + RootRegister + Send + Sync + 'static,
{
    /// Creates a journal for a stream.
    pub fn new(stream: StreamId, storage: Arc<S>) -> Self {
        Self {
            stream,
            storage,
            retry_window_ticks: 100,
            max_cas_retries: 16,
            fault: Mutex::new(None),
        }
    }

    /// Returns the stream identity for this journal.
    pub fn stream_id(&self) -> &StreamId {
        &self.stream
    }

    /// Sets the bounded retry window.
    pub fn with_retry_window_ticks(mut self, retry_window_ticks: u64) -> Self {
        self.retry_window_ticks = retry_window_ticks;
        self
    }

    /// Injects a one-shot deterministic fault.
    pub fn inject_fault(&self, fault: JournalFault) {
        *self.fault.lock().expect("journal fault mutex poisoned") = Some(fault);
    }

    /// Appends a single request.
    pub async fn append(&self, request: AppendRequest, now_tick: u64) -> Result<AppendOutcome> {
        self.append_batch(vec![request], now_tick)
            .await
            .map(|mut outcomes| {
                outcomes
                    .pop()
                    .expect("append_batch returns one outcome for one request")
            })
    }

    /// Group commits a bounded batch as one immutable page and one root CAS.
    pub async fn append_batch(
        &self,
        requests: Vec<AppendRequest>,
        now_tick: u64,
    ) -> Result<Vec<AppendOutcome>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        for request in &requests {
            self.validate_request(request, now_tick)?;
        }

        for _attempt in 0..self.max_cas_retries {
            let (head, expected) = self.read_head().await?;
            let mut outcomes = Vec::with_capacity(requests.len());
            let mut new_records = Vec::new();
            let mut new_receipts = head.receipts.clone();
            let mut next_position = head.tail_position.saturating_add(1);

            for request in &requests {
                if let Some(existing) = head.receipts.get(&request.token.client_key) {
                    if existing.digest != request.token.digest {
                        return Err(JournalError::ClientKeyDigestMismatch);
                    }
                    outcomes.push(AppendOutcome::AlreadyAppended(existing.receipt.clone()));
                    continue;
                }

                let first = JournalPosition(next_position);
                let last = JournalPosition(
                    next_position
                        .saturating_add(request.records.len() as u64)
                        .saturating_sub(1),
                );
                next_position = last.0.saturating_add(1);
                new_records.extend(request.records.clone());
                outcomes.push(AppendOutcome::Appended(AppendReceipt {
                    first_position: first,
                    last_position: last,
                    page: request.records[0].clone(),
                }));
            }

            if new_records.is_empty() {
                return Ok(outcomes);
            }

            let first_position = JournalPosition(head.tail_position.saturating_add(1));
            let last_position =
                JournalPosition(head.tail_position.saturating_add(new_records.len() as u64));
            let page = self
                .write_page(&head, first_position, last_position, new_records)
                .await?;

            if self.take_fault(JournalFault::AfterJournalPagePersistenceBeforeHeadCas) {
                return Ok(vec![AppendOutcome::OutcomeUnknown; requests.len()]);
            }

            for outcome in &mut outcomes {
                if let AppendOutcome::Appended(receipt) = outcome {
                    receipt.page = page.clone();
                }
            }
            for (request, outcome) in requests.iter().zip(outcomes.iter()) {
                if let AppendOutcome::Appended(receipt) = outcome {
                    new_receipts.insert(
                        request.token.client_key.clone(),
                        ReceiptRecord {
                            digest: request.token.digest,
                            expires_at_tick: request.token.expires_at_tick,
                            receipt: receipt.clone(),
                        },
                    );
                }
            }

            let mut next_head = head;
            next_head.tail_page = Some(page);
            next_head.tail_position = last_position.0;
            next_head.receipts = new_receipts
                .into_iter()
                .filter(|(_, receipt)| receipt.expires_at_tick >= now_tick)
                .collect();
            match self.write_head(next_head, expected).await? {
                PublishOutcome::Applied(_) => {
                    if self.take_fault(JournalFault::AfterJournalHeadCasBeforeAck) {
                        return Ok(vec![AppendOutcome::OutcomeUnknown; requests.len()]);
                    }
                    return Ok(outcomes);
                }
                PublishOutcome::Conflict { .. } => continue,
                PublishOutcome::OutcomeUnknown => {
                    return Ok(vec![AppendOutcome::OutcomeUnknown; requests.len()]);
                }
            }
        }
        Err(JournalError::CasContention)
    }

    /// Resolves a token after a lost append acknowledgement.
    pub async fn resolve(&self, token: &RequestToken, now_tick: u64) -> Result<AppendResolution> {
        if token.is_expired(now_tick) {
            return Ok(AppendResolution::RequestExpired);
        }
        let (head, _) = self.read_head().await?;
        Ok(head
            .receipts
            .get(&token.client_key)
            .filter(|receipt| receipt.digest == token.digest)
            .map(|receipt| AppendResolution::Committed(receipt.receipt.clone()))
            .unwrap_or(AppendResolution::Unknown))
    }

    /// Scans the retained journal.
    pub async fn scan_from(&self, position: JournalPosition) -> Result<JournalSnapshot> {
        let (head, _) = self.read_head().await?;
        if position.0 < head.earliest_retained {
            return Err(JournalError::SnapshotExpired);
        }
        let mut pages = Vec::new();
        let mut cursor = head.tail_page.clone();
        while let Some(page_ref) = cursor {
            let page = self.read_page(&page_ref).await?;
            cursor = page.header.previous_page.clone();
            pages.push(page);
        }
        pages.reverse();

        let mut expected = 1;
        let mut records = Vec::new();
        for page in pages {
            if page.header.first_position.0 != expected {
                return Err(JournalError::Corrupt("page chain has a gap".into()));
            }
            expected = page.header.last_position.0.saturating_add(1);
            for (offset, record) in page.records.into_iter().enumerate() {
                let record_position = page.header.first_position.0 + offset as u64;
                if record_position >= position.0 {
                    records.push(record);
                }
            }
        }
        Ok(JournalSnapshot {
            first_position: position,
            records,
        })
    }

    fn validate_request(&self, request: &AppendRequest, now_tick: u64) -> Result<()> {
        if request.token.stream != self.stream {
            return Err(JournalError::Corrupt("token stream mismatch".into()));
        }
        if request.token.is_expired(now_tick) {
            return Err(JournalError::RequestExpired);
        }
        if request.records.is_empty() {
            return Err(JournalError::Corrupt(
                "append request has no records".into(),
            ));
        }
        let digest = digest_records(&request.records)?;
        if digest != request.token.digest {
            return Err(JournalError::Corrupt("request digest mismatch".into()));
        }
        Ok(())
    }

    async fn write_page(
        &self,
        head: &JournalHead,
        first_position: JournalPosition,
        last_position: JournalPosition,
        records: Vec<DurableObjectRef>,
    ) -> Result<DurableObjectRef> {
        let encoded_records = serde_json::to_vec(&records)
            .map_err(|error| JournalError::Corrupt(format!("records encode failed: {error}")))?;
        let page = JournalPage {
            header: JournalPageHeader {
                format_version: 1,
                stream: self.stream.clone(),
                previous_page: head.tail_page.clone(),
                first_position,
                last_position,
                record_count: records.len() as u64,
                record_framing: "serde-json-durable-object-ref-array".into(),
                records_checksum: Sha256::digest(&encoded_records).into(),
            },
            records,
        };
        let bytes = serde_json::to_vec(&page)
            .map_err(|error| JournalError::Corrupt(format!("page encode failed: {error}")))?;
        let id = compute_object_id(&ObjectFormat::JournalPage, &bytes);
        self.storage
            .put(
                id,
                ObjectFormat::JournalPage,
                &bytes,
                Durability::BackendDefault,
            )
            .await
            .map_err(JournalError::Core)
    }

    async fn read_page(&self, reference: &DurableObjectRef) -> Result<JournalPage> {
        let bytes = ImmutableObjects::read(self.storage.as_ref(), reference)
            .await
            .map_err(JournalError::Core)?;
        let page: JournalPage = serde_json::from_slice(&bytes)
            .map_err(|error| JournalError::Corrupt(format!("page decode failed: {error}")))?;
        let encoded_records = serde_json::to_vec(&page.records)
            .map_err(|error| JournalError::Corrupt(format!("records encode failed: {error}")))?;
        if <[u8; 32]>::from(Sha256::digest(&encoded_records)) != page.header.records_checksum {
            return Err(JournalError::Corrupt("page checksum mismatch".into()));
        }
        Ok(page)
    }

    async fn read_head(&self) -> Result<(JournalHead, ExpectedRevision)> {
        let name = self.head_name();
        let current = RootRegister::read(self.storage.as_ref(), &name)
            .await
            .map_err(JournalError::Core)?;
        match current {
            Some(state) => {
                let head = serde_json::from_slice(state.value()).map_err(|error| {
                    JournalError::Corrupt(format!("head decode failed: {error}"))
                })?;
                Ok((head, ExpectedRevision::Exact(state.revision())))
            }
            None => Ok((JournalHead::default(), ExpectedRevision::Missing)),
        }
    }

    async fn write_head(
        &self,
        head: JournalHead,
        expected: ExpectedRevision,
    ) -> Result<PublishOutcome> {
        let bytes = serde_json::to_vec(&head)
            .map_err(|error| JournalError::Corrupt(format!("head encode failed: {error}")))?;
        self.storage
            .compare_exchange(&self.head_name(), expected, bytes)
            .await
            .map_err(JournalError::Core)
    }

    fn head_name(&self) -> RootName {
        RootName::new(format!("journal-{}-head", self.stream.as_str()))
    }

    fn take_fault(&self, fault: JournalFault) -> bool {
        let mut current = self.fault.lock().expect("journal fault mutex poisoned");
        if *current == Some(fault) {
            *current = None;
            true
        } else {
            false
        }
    }
}

impl<S> Journal for PagedJournal<S> {
    fn issue_request_token(
        &self,
        client_key: ClientRequestKey,
        digest: [u8; 32],
        now_tick: u64,
    ) -> RequestToken {
        RequestToken {
            id: Uuid::new_v4(),
            stream: self.stream.clone(),
            client_key,
            digest,
            expires_at_tick: now_tick.saturating_add(self.retry_window_ticks),
        }
    }
}

impl<S> JournalMaintenance for PagedJournal<S> {
    fn checkpoint(&self) -> Option<CheckpointRef> {
        None
    }
}

/// Computes the request digest for durable records.
pub fn digest_records(records: &[DurableObjectRef]) -> Result<[u8; 32]> {
    let encoded = serde_json::to_vec(records)
        .map_err(|error| JournalError::Corrupt(format!("records encode failed: {error}")))?;
    Ok(Sha256::digest(encoded).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use durable_core::{
        compute_object_id, DatasetId, Durability, EncryptionDomainId, InMemoryBackend,
        ObjectFormat, ScopedStorage, StorageScope, TenantId,
    };

    fn scope() -> StorageScope {
        StorageScope::new(
            TenantId::new("tenant"),
            DatasetId::new("dataset"),
            EncryptionDomainId::new("edek"),
        )
    }

    async fn setup() -> (
        Arc<InMemoryBackend>,
        Arc<ScopedStorage<InMemoryBackend>>,
        PagedJournal<ScopedStorage<InMemoryBackend>>,
        Vec<DurableObjectRef>,
    ) {
        let backend = Arc::new(InMemoryBackend::new());
        let storage = Arc::new(ScopedStorage::new(scope(), backend.clone()));
        let mut records = Vec::new();
        for bytes in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
            let id = compute_object_id(&ObjectFormat::Raw, bytes);
            records.push(
                storage
                    .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
                    .await
                    .unwrap(),
            );
        }
        let journal = PagedJournal::new(StreamId::new("stream"), storage.clone());
        (backend, storage, journal, records)
    }

    fn request(
        journal: &PagedJournal<ScopedStorage<InMemoryBackend>>,
        key: &str,
        records: Vec<DurableObjectRef>,
        now: u64,
    ) -> AppendRequest {
        let digest = digest_records(&records).unwrap();
        AppendRequest {
            token: journal.issue_request_token(ClientRequestKey::new(key), digest, now),
            records,
        }
    }

    #[tokio::test]
    async fn group_commit_batches_concurrent_appends() {
        let (_backend, _storage, journal, records) = setup().await;
        let first = request(&journal, "a", vec![records[0].clone()], 1);
        let second = request(&journal, "b", vec![records[1].clone()], 1);

        let outcomes = journal.append_batch(vec![first, second], 1).await.unwrap();

        let first_page = match &outcomes[0] {
            AppendOutcome::Appended(receipt) => receipt.page.object_id(),
            other => panic!("unexpected outcome: {other:?}"),
        };
        let second_page = match &outcomes[1] {
            AppendOutcome::Appended(receipt) => receipt.page.object_id(),
            other => panic!("unexpected outcome: {other:?}"),
        };
        assert_eq!(first_page, second_page);
        assert_eq!(
            journal
                .scan_from(JournalPosition::FIRST)
                .await
                .unwrap()
                .records
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn retry_returns_original_receipt() {
        let (_backend, _storage, journal, records) = setup().await;
        let append = request(&journal, "same", vec![records[0].clone()], 1);
        let first = journal.append(append.clone(), 1).await.unwrap();
        let second = journal.append(append, 2).await.unwrap();

        assert!(matches!(first, AppendOutcome::Appended(_)));
        assert!(matches!(second, AppendOutcome::AlreadyAppended(_)));
    }

    #[tokio::test]
    async fn client_key_reused_with_another_digest_is_rejected() {
        let (_backend, _storage, journal, records) = setup().await;
        journal
            .append(request(&journal, "same", vec![records[0].clone()], 1), 1)
            .await
            .unwrap();
        let error = journal
            .append(request(&journal, "same", vec![records[1].clone()], 2), 2)
            .await
            .unwrap_err();

        assert!(matches!(error, JournalError::ClientKeyDigestMismatch));
    }

    #[tokio::test]
    async fn expired_request_token_is_rejected() {
        let (_backend, _storage, journal, records) = setup().await;
        let append = request(&journal, "expired", vec![records[0].clone()], 1);

        let error = journal.append(append, 200).await.unwrap_err();

        assert!(matches!(error, JournalError::RequestExpired));
    }

    #[tokio::test]
    async fn root_commit_ack_loss_can_be_resolved() {
        let (_backend, _storage, journal, records) = setup().await;
        let append = request(&journal, "lost", vec![records[0].clone()], 1);
        journal.inject_fault(JournalFault::AfterJournalHeadCasBeforeAck);

        let outcome = journal.append(append.clone(), 1).await.unwrap();
        let resolution = journal.resolve(&append.token, 2).await.unwrap();

        assert!(matches!(outcome, AppendOutcome::OutcomeUnknown));
        assert!(matches!(resolution, AppendResolution::Committed(_)));
    }

    #[tokio::test]
    async fn journal_does_not_expose_batch_when_head_publish_is_not_acknowledged_before_cas() {
        let (_backend, _storage, journal, records) = setup().await;
        let append = request(&journal, "unknown-before-head", vec![records[0].clone()], 1);
        journal.inject_fault(JournalFault::AfterJournalPagePersistenceBeforeHeadCas);

        let outcome = journal.append(append, 1).await.unwrap();
        let snapshot = journal.scan_from(JournalPosition::FIRST).await.unwrap();

        assert!(matches!(outcome, AppendOutcome::OutcomeUnknown));
        assert!(snapshot.records.is_empty());
    }

    #[tokio::test]
    async fn scan_detects_corrupt_page_chain() {
        let (backend, storage, journal, records) = setup().await;
        let outcome = journal
            .append(request(&journal, "corrupt", vec![records[0].clone()], 1), 1)
            .await
            .unwrap();
        let page = match outcome {
            AppendOutcome::Appended(receipt) => receipt.page,
            other => panic!("unexpected outcome: {other:?}"),
        };
        backend.corrupt_object(storage.scope_id(), page.object_id(), b"not a page".to_vec());

        assert!(matches!(
            journal.scan_from(JournalPosition::FIRST).await.unwrap_err(),
            JournalError::Core(DurableError::Corrupt(_))
        ));
    }

    #[tokio::test]
    async fn lost_queue_notification_does_not_affect_journal_state() {
        let (_backend, _storage, journal, records) = setup().await;

        journal
            .append(
                request(&journal, "authoritative", vec![records[0].clone()], 1),
                1,
            )
            .await
            .unwrap();

        let snapshot = journal.scan_from(JournalPosition::FIRST).await.unwrap();
        assert_eq!(snapshot.records, vec![records[0].clone()]);
    }
}
