//! S3-compatible backend implementation, intended for local RustFS validation.

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{error::ProvideErrorMetadata, primitives::ByteStream, Client};
use aws_types::region::Region;
use durable_core::{
    BackendVersion, DurableError, ObjectId, PutObjectResult, RawBackend, Result, RootName,
    RootState, ScopeId, VersionedRoot,
};
use std::{fmt::Debug, ops::Range};

/// Configuration for an S3-compatible durable backend.
#[derive(Debug, Clone)]
pub struct S3BackendConfig {
    /// Endpoint URL, for example `http://127.0.0.1:9000`.
    pub endpoint: String,
    /// Region name.
    pub region: String,
    /// Bucket name.
    pub bucket: String,
    /// Access key.
    pub access_key: String,
    /// Secret key.
    pub secret_key: String,
    /// Whether to create the bucket if missing.
    pub create_bucket_if_missing: bool,
}

impl S3BackendConfig {
    /// Returns the default local RustFS configuration.
    pub fn local_rustfs() -> Self {
        Self {
            endpoint: "http://127.0.0.1:9000".into(),
            region: "us-east-1".into(),
            bucket: "durable-dev".into(),
            access_key: "durable".into(),
            secret_key: "durable-secret".into(),
            create_bucket_if_missing: true,
        }
    }
}

/// S3-compatible raw backend.
#[derive(Clone)]
pub struct S3Backend {
    config: S3BackendConfig,
    client: Client,
}

impl S3Backend {
    /// Creates a backend and verifies the target bucket.
    pub async fn new(config: S3BackendConfig) -> Result<Self> {
        let creds = Credentials::new(
            config.access_key.clone(),
            config.secret_key.clone(),
            None,
            None,
            "durable-static",
        );
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .credentials_provider(creds)
            .endpoint_url(config.endpoint.clone())
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .endpoint_url(config.endpoint.clone())
            .force_path_style(true)
            .build();
        let backend = Self {
            config,
            client: Client::from_conf(s3_config),
        };
        backend.ensure_bucket().await?;
        Ok(backend)
    }

    /// Returns the backend configuration.
    pub fn config(&self) -> &S3BackendConfig {
        &self.config
    }

    async fn ensure_bucket(&self) -> Result<()> {
        match self
            .client
            .head_bucket()
            .bucket(&self.config.bucket)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_missing(&error) && self.config.create_bucket_if_missing => {
                self.client
                    .create_bucket()
                    .bucket(&self.config.bucket)
                    .send()
                    .await
                    .map_err(classify_s3_error)?;
                Ok(())
            }
            Err(error) if is_missing(&error) => Err(DurableError::Missing(format!(
                "bucket {}",
                self.config.bucket
            ))),
            Err(error) => Err(classify_s3_error(error)),
        }
    }

    fn object_key(scope_id: ScopeId, object_id: ObjectId) -> String {
        format!(
            "scopes/{}/objects/{}",
            scope_id.to_hex(),
            object_id.to_hex()
        )
    }

    fn root_key(scope_id: ScopeId, name: &RootName) -> String {
        format!(
            "scopes/{}/roots/{}.json",
            scope_id.to_hex(),
            escape(name.as_str())
        )
    }
}

#[async_trait]
impl RawBackend for S3Backend {
    async fn put_object_if_absent(
        &self,
        scope_id: ScopeId,
        object_id: ObjectId,
        bytes: &[u8],
    ) -> Result<PutObjectResult> {
        let key = Self::object_key(scope_id, object_id);
        match self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from(bytes.to_vec()))
            .send()
            .await
        {
            Ok(_) => Ok(PutObjectResult::Written),
            Err(error) if is_precondition(&error) => Ok(PutObjectResult::AlreadyExists),
            Err(error) => Err(classify_s3_error(error)),
        }
    }

    async fn read_object(&self, scope_id: ScopeId, object_id: ObjectId) -> Result<Vec<u8>> {
        let key = Self::object_key(scope_id, object_id);
        let output = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .map_err(classify_s3_error)?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|error| DurableError::Unavailable(format!("failed to read S3 body: {error}")))?
            .into_bytes();
        Ok(bytes.to_vec())
    }

    async fn read_object_range(
        &self,
        scope_id: ScopeId,
        object_id: ObjectId,
        range: Range<u64>,
    ) -> Result<Vec<u8>> {
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let key = Self::object_key(scope_id, object_id);
        let range_header = format!("bytes={}-{}", range.start, range.end - 1);
        let output = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .range(range_header)
            .send()
            .await
            .map_err(classify_s3_error)?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|error| DurableError::Unavailable(format!("failed to read S3 body: {error}")))?
            .into_bytes();
        Ok(bytes.to_vec())
    }

    async fn read_root(&self, scope_id: ScopeId, name: &RootName) -> Result<Option<VersionedRoot>> {
        let key = Self::root_key(scope_id, name);
        match self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => {
                let version = output.e_tag().map(ToOwned::to_owned).ok_or_else(|| {
                    DurableError::Corrupt("root object is missing an S3 ETag".into())
                })?;
                let bytes = output
                    .body
                    .collect()
                    .await
                    .map_err(|error| {
                        DurableError::Unavailable(format!("failed to read root body: {error}"))
                    })?
                    .into_bytes();
                let state: RootState = serde_json::from_slice(&bytes).map_err(|error| {
                    DurableError::Corrupt(format!("invalid root JSON: {error}"))
                })?;
                Ok(Some(VersionedRoot {
                    state,
                    version: BackendVersion(version),
                }))
            }
            Err(error) if is_missing(&error) => Ok(None),
            Err(error) => Err(classify_s3_error(error)),
        }
    }

    async fn compare_exchange_root(
        &self,
        scope_id: ScopeId,
        name: &RootName,
        expected: Option<&BackendVersion>,
        state: &RootState,
    ) -> Result<bool> {
        let key = Self::root_key(scope_id, name);
        let bytes = serde_json::to_vec(state)
            .map_err(|error| DurableError::Corrupt(format!("failed to encode root: {error}")))?;
        let request = self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(ByteStream::from(bytes));
        let request = match expected {
            Some(version) => request.if_match(version.0.clone()),
            None => request.if_none_match("*"),
        };

        match request.send().await {
            Ok(_) => Ok(true),
            Err(error) if is_precondition(&error) || is_missing(&error) => Ok(false),
            Err(error) => Err(classify_s3_error(error)),
        }
    }
}

fn escape(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                vec![byte as char]
            }
            other => format!("%{other:02x}").chars().collect(),
        })
        .collect()
}

fn is_missing<E>(error: &aws_sdk_s3::error::SdkError<E>) -> bool
where
    E: ProvideErrorMetadata + Debug,
{
    let code = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code);
    matches!(
        code,
        Some("NoSuchKey" | "NotFound" | "NoSuchBucket" | "404")
    ) || format!("{error:?}").contains("NotFound")
}

fn is_precondition<E>(error: &aws_sdk_s3::error::SdkError<E>) -> bool
where
    E: ProvideErrorMetadata + Debug,
{
    let code = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code);
    matches!(
        code,
        Some("PreconditionFailed" | "ConditionalRequestConflict" | "412" | "409")
    ) || format!("{error:?}").contains("Precondition")
}

fn classify_s3_error<E>(error: aws_sdk_s3::error::SdkError<E>) -> DurableError
where
    E: ProvideErrorMetadata + Debug,
{
    if is_missing(&error) {
        DurableError::Missing(format!("{error:?}"))
    } else if is_precondition(&error) {
        DurableError::PreconditionFailed(format!("{error:?}"))
    } else {
        DurableError::Unavailable(format!("{error:?}"))
    }
}
