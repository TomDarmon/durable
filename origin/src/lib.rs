//! Origin-like Git storage built on durable primitives.
//!
//! The durable library stays Git-agnostic. This crate owns Git repository
//! identity, bare-repository materialization, ref publication, and recovery.

use axum::Json;
use axum::{middleware::Next, response::IntoResponse};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
    time::Instant,
};
use substrate::{
    compute_object_id, DatasetId, Durability, DurableError, DurableObjectRef, EncryptionDomainId,
    ExpectedRevision, ImmutableObjects, ObjectFormat, PublishOutcome, RawBackend, RootName,
    RootRegister, RootState, ScopedStorage, StorageScope, TenantId,
};
use thiserror::Error;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};
use walkdir::WalkDir;

/// Origin result type.
pub type Result<T> = std::result::Result<T, OriginError>;

const CACHE_MARKER_FILE: &str = ".origin-cache-publication";
const CATALOG_TENANT: &str = "__origin_system";
const CATALOG_DATASET: &str = "repository-catalog";
const CATALOG_ENCRYPTION_DOMAIN: &str = "edek";

/// Origin error taxonomy.
#[derive(Debug, Error)]
pub enum OriginError {
    /// Durable storage failed.
    #[error(transparent)]
    Durable(#[from] DurableError),
    /// Filesystem operation failed.
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON encoding failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// Git command failed.
    #[error("git command failed: {program} {args:?}: {stderr}")]
    Git {
        /// Program name.
        program: String,
        /// Arguments.
        args: Vec<String>,
        /// Standard error.
        stderr: String,
    },
    /// A path inside a bare repository was not safe to persist.
    #[error("unsafe git repository path: {0}")]
    UnsafePath(String),
    /// The repository root changed concurrently.
    #[error("repository root changed concurrently")]
    Conflict,
    /// A root publication returned an ambiguous outcome.
    #[error("repository root publication outcome is unknown")]
    OutcomeUnknown,
    /// HTTP request could not be served.
    #[error("http error: {0}")]
    Http(String),
}

/// Repository identity mapped to a durable storage scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryScope {
    tenant: String,
    repository: String,
    encryption_domain: String,
}

impl RepositoryScope {
    /// Creates a repository scope.
    pub fn new(
        tenant: impl Into<String>,
        repository: impl Into<String>,
        encryption_domain: impl Into<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            repository: repository.into(),
            encryption_domain: encryption_domain.into(),
        }
    }

    /// Converts the Origin repository identity to a durable scope.
    pub fn storage_scope(&self) -> StorageScope {
        StorageScope::new(
            TenantId::new(&self.tenant),
            DatasetId::new(&self.repository),
            EncryptionDomainId::new(&self.encryption_domain),
        )
    }
}

/// Durable-backed repository handle.
#[derive(Clone)]
pub struct OriginRepository<B> {
    storage: Arc<ScopedStorage<B>>,
    root_name: RootName,
}

/// Local bare repository materialized from one exact durable root revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializedRepository {
    expected: ExpectedRevision,
}

/// Published state for one Git repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitPublication {
    /// Stable format version.
    pub version: u32,
    /// Deterministic digest of captured file paths and durable object IDs.
    pub digest: String,
    /// Published refs captured from the bare repository.
    #[serde(default)]
    pub refs: Vec<GitRefEntry>,
    /// Git object catalog captured from the bare repository object database.
    #[serde(default)]
    pub objects: Vec<GitObjectEntry>,
    /// Files required to reconstruct the bare serving repository.
    pub files: Vec<GitFileEntry>,
}

/// One Git ref published in the repository manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRefEntry {
    /// Full ref name, for example `refs/heads/main`.
    pub name: String,
    /// Git object ID targeted by the ref.
    pub target: String,
}

/// One Git object published in the repository manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitObjectEntry {
    /// Git object ID.
    pub oid: String,
    /// Git object type, for example `commit`, `tree`, `blob`, or `tag`.
    pub kind: String,
    /// Uncompressed object size reported by Git.
    pub size: u64,
}

/// One durable file inside the materialized bare Git repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitFileEntry {
    /// Relative path inside the bare repository.
    pub path: String,
    /// Durable immutable bytes for that path.
    pub object: DurableObjectRef,
}

/// Durable catalog of repositories known to this Origin deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryCatalog {
    /// Stable format version.
    pub version: u32,
    /// Known repositories.
    pub repositories: Vec<RepositoryCatalogEntry>,
}

impl Default for RepositoryCatalog {
    fn default() -> Self {
        Self {
            version: 1,
            repositories: Vec::new(),
        }
    }
}

/// One repository visible through the Origin browser API.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RepositoryCatalogEntry {
    /// Tenant or organization name.
    pub tenant: String,
    /// Repository name without the `.git` suffix.
    pub name: String,
}

/// Durable-backed repository catalog.
#[derive(Clone)]
pub struct OriginCatalog<B> {
    storage: Arc<ScopedStorage<B>>,
    root_name: RootName,
}

impl<B> OriginCatalog<B>
where
    B: RawBackend + 'static,
{
    /// Creates a catalog handle over a scope-bound durable store.
    pub fn new(storage: Arc<ScopedStorage<B>>) -> Self {
        Self {
            storage,
            root_name: RootName::new("origin.repository.catalog.v1"),
        }
    }

    /// Reads the current catalog.
    pub async fn read(&self) -> Result<RepositoryCatalog> {
        let Some(root) = RootRegister::read(self.storage.as_ref(), &self.root_name).await? else {
            debug!("repository catalog is empty");
            return Ok(RepositoryCatalog::default());
        };
        let catalog: RepositoryCatalog = serde_json::from_slice(root.value())?;
        debug!(
            repositories = catalog.repositories.len(),
            "read repository catalog"
        );
        Ok(catalog)
    }

    /// Registers a repository if it is not already present.
    pub async fn register(&self, tenant: &str, repo: &str) -> Result<()> {
        let entry = RepositoryCatalogEntry {
            tenant: tenant.to_string(),
            name: repo.to_string(),
        };
        for _ in 0..8 {
            let current_root = RootRegister::read(self.storage.as_ref(), &self.root_name).await?;
            let expected = current_root
                .as_ref()
                .map_or(ExpectedRevision::Missing, |root| {
                    ExpectedRevision::Exact(root.revision())
                });
            let mut catalog = current_root
                .as_ref()
                .map(|root| serde_json::from_slice::<RepositoryCatalog>(root.value()))
                .transpose()?
                .unwrap_or_default();
            if catalog.repositories.contains(&entry) {
                debug!(tenant, repo, "repository already present in catalog");
                return Ok(());
            }
            catalog.repositories.push(entry.clone());
            catalog.repositories.sort();
            let value = serde_json::to_vec(&catalog)?;
            match self
                .storage
                .compare_exchange(&self.root_name, expected, value)
                .await?
            {
                PublishOutcome::Applied(_) => {
                    info!(
                        tenant,
                        repo,
                        repositories = catalog.repositories.len(),
                        "registered repository"
                    );
                    return Ok(());
                }
                PublishOutcome::Conflict { .. } => {
                    debug!(tenant, repo, "repository catalog CAS conflict; retrying");
                    continue;
                }
                PublishOutcome::OutcomeUnknown => {
                    error!(tenant, repo, "repository catalog publish outcome unknown");
                    return Err(OriginError::OutcomeUnknown);
                }
            }
        }
        warn!(
            tenant,
            repo, "repository catalog registration retries exhausted"
        );
        Err(OriginError::Conflict)
    }
}

impl<B> OriginRepository<B>
where
    B: RawBackend + 'static,
{
    /// Creates a repository handle over a scope-bound durable store.
    pub fn new(storage: Arc<ScopedStorage<B>>) -> Self {
        Self {
            storage,
            root_name: RootName::new("origin.git.repository.v1"),
        }
    }

    /// Returns the underlying durable storage handle.
    pub fn storage(&self) -> &Arc<ScopedStorage<B>> {
        &self.storage
    }

    /// Captures a bare Git repository and publishes it against the latest root.
    ///
    /// Prefer `publish_materialized_bare_repository` when publishing a repo
    /// cache produced by `materialize_bare_repository`; it preserves the base
    /// revision and rejects stale writers.
    pub async fn publish_bare_repository(&self, bare_repo: impl AsRef<Path>) -> Result<()> {
        let expected = RootRegister::read(self.storage.as_ref(), &self.root_name)
            .await?
            .map_or(ExpectedRevision::Missing, |root| {
                ExpectedRevision::Exact(root.revision())
            });
        self.publish_bare_repository_with_expected(bare_repo.as_ref(), expected)
            .await
            .map(|_| ())
    }

    /// Publishes a materialized bare repository against its captured base.
    pub async fn publish_materialized_bare_repository(
        &self,
        bare_repo: impl AsRef<Path>,
        materialized: MaterializedRepository,
    ) -> Result<MaterializedRepository> {
        self.publish_bare_repository_with_expected(bare_repo.as_ref(), materialized.expected)
            .await
    }

    async fn publish_bare_repository_with_expected(
        &self,
        bare_repo: &Path,
        expected: ExpectedRevision,
    ) -> Result<MaterializedRepository> {
        info!(bare_repo = %bare_repo.display(), "publishing repository");
        git(bare_repo, ["fsck", "--no-dangling"])?;

        let publication = self.capture_bare_repository(bare_repo).await?;
        let publication_ref = self.put_publication(&publication).await?;
        let root_value = serde_json::to_vec(&publication_ref)?;

        match self
            .storage
            .compare_exchange(&self.root_name, expected, root_value)
            .await?
        {
            PublishOutcome::Applied(root) => {
                write_cache_marker(bare_repo, &publication.digest)?;
                info!(
                    bare_repo = %bare_repo.display(),
                    digest = %publication.digest,
                    refs = publication.refs.len(),
                    objects = publication.objects.len(),
                    files = publication.files.len(),
                    revision = ?root.revision(),
                    "published repository"
                );
                Ok(MaterializedRepository {
                    expected: ExpectedRevision::Exact(root.revision()),
                })
            }
            PublishOutcome::Conflict { .. } => {
                warn!(bare_repo = %bare_repo.display(), "repository publish conflict");
                Err(OriginError::Conflict)
            }
            PublishOutcome::OutcomeUnknown => {
                error!(bare_repo = %bare_repo.display(), "repository publish outcome unknown");
                Err(OriginError::OutcomeUnknown)
            }
        }
    }

    /// Materializes the current durable repository state to a local bare repo.
    pub async fn materialize_bare_repository(
        &self,
        bare_repo: impl AsRef<Path>,
    ) -> Result<MaterializedRepository> {
        let bare_repo = bare_repo.as_ref();
        if let Some(root) = RootRegister::read(self.storage.as_ref(), &self.root_name).await? {
            info!(
                bare_repo = %bare_repo.display(),
                revision = ?root.revision(),
                "materializing repository"
            );
            let publication = self.load_publication(&root).await?;
            self.write_publication_to_bare_repository(bare_repo, &publication)
                .await?;
            Ok(MaterializedRepository {
                expected: ExpectedRevision::Exact(root.revision()),
            })
        } else {
            info!(
                bare_repo = %bare_repo.display(),
                "initializing empty repository"
            );
            materialize_empty_bare_repo(bare_repo)?;
            self.publish_bare_repository_with_expected(bare_repo, ExpectedRevision::Missing)
                .await
        }
    }

    /// Materializes the current durable state, reusing a verified local cache when it matches.
    pub async fn materialize_bare_repository_cached(
        &self,
        bare_repo: impl AsRef<Path>,
    ) -> Result<MaterializedRepository> {
        let bare_repo = bare_repo.as_ref();
        if let Some(root) = RootRegister::read(self.storage.as_ref(), &self.root_name).await? {
            let publication = self.load_publication(&root).await?;
            if cache_marker_matches(bare_repo, &publication.digest)
                && git(bare_repo, ["fsck", "--no-dangling"]).is_ok()
            {
                debug!(
                    bare_repo = %bare_repo.display(),
                    digest = %publication.digest,
                    "using repository cache"
                );
                return Ok(MaterializedRepository {
                    expected: ExpectedRevision::Exact(root.revision()),
                });
            }
            info!(
                bare_repo = %bare_repo.display(),
                digest = %publication.digest,
                "refreshing repository cache"
            );
            self.write_publication_to_bare_repository(bare_repo, &publication)
                .await?;
            Ok(MaterializedRepository {
                expected: ExpectedRevision::Exact(root.revision()),
            })
        } else {
            info!(
                bare_repo = %bare_repo.display(),
                "initializing empty repository cache"
            );
            materialize_empty_bare_repo(bare_repo)?;
            self.publish_bare_repository_with_expected(bare_repo, ExpectedRevision::Missing)
                .await
        }
    }

    /// Reads the currently published repository state.
    pub async fn current_publication(&self) -> Result<Option<GitPublication>> {
        let Some(root) = RootRegister::read(self.storage.as_ref(), &self.root_name).await? else {
            return Ok(None);
        };
        Ok(Some(self.load_publication(&root).await?))
    }

    async fn capture_bare_repository(&self, bare_repo: &Path) -> Result<GitPublication> {
        let refs = capture_git_refs(bare_repo)?;
        let objects = capture_git_objects(bare_repo)?;
        let mut files = Vec::new();
        for entry in WalkDir::new(bare_repo).follow_links(false) {
            let entry = entry.map_err(|error| OriginError::Io(error.into()))?;
            if !entry.file_type().is_file() {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(bare_repo)
                .map_err(|_| OriginError::UnsafePath(entry.path().display().to_string()))?;
            let path = normalize_relative_path(relative)?;
            if !should_persist_git_file(&path) {
                continue;
            }
            let bytes = fs::read(entry.path())?;
            let object = self
                .put_custom_object(ObjectFormat::Custom("origin.git.file.v1".into()), &bytes)
                .await?;
            files.push(GitFileEntry { path, object });
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let digest = publication_digest(&refs, &objects, &files);
        debug!(
            bare_repo = %bare_repo.display(),
            digest = %digest,
            refs = refs.len(),
            objects = objects.len(),
            files = files.len(),
            "captured repository publication"
        );
        Ok(GitPublication {
            version: 1,
            digest,
            refs,
            objects,
            files,
        })
    }

    async fn load_publication(&self, root: &RootState) -> Result<GitPublication> {
        let publication_ref: DurableObjectRef = serde_json::from_slice(root.value())?;
        let bytes = ImmutableObjects::read(self.storage.as_ref(), &publication_ref).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn write_publication_to_bare_repository(
        &self,
        bare_repo: &Path,
        publication: &GitPublication,
    ) -> Result<()> {
        materialize_empty_bare_repo(bare_repo)?;
        for file in &publication.files {
            let path = safe_join(bare_repo, &file.path)?;
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let bytes = ImmutableObjects::read(self.storage.as_ref(), &file.object).await?;
            fs::write(path, bytes)?;
        }
        git(bare_repo, ["fsck", "--no-dangling"])?;
        write_cache_marker(bare_repo, &publication.digest)?;
        Ok(())
    }

    async fn put_publication(&self, publication: &GitPublication) -> Result<DurableObjectRef> {
        let bytes = serde_json::to_vec(publication)?;
        self.put_custom_object(
            ObjectFormat::Custom("origin.git.publication.v1".into()),
            &bytes,
        )
        .await
    }

    async fn put_custom_object(
        &self,
        format: ObjectFormat,
        bytes: &[u8],
    ) -> Result<DurableObjectRef> {
        let id = compute_object_id(&format, bytes);
        Ok(self
            .storage
            .put(id, format, bytes, Durability::BackendDefault)
            .await?)
    }
}

/// Builds a local RustFS-backed Origin repository.
pub async fn local_rustfs_repository(
    scope: RepositoryScope,
) -> Result<OriginRepository<s3::S3Backend>> {
    rustfs_repository(scope, local_rustfs_config()).await
}

/// Builds a local RustFS-backed Origin repository catalog.
pub async fn local_rustfs_catalog() -> Result<OriginCatalog<s3::S3Backend>> {
    rustfs_catalog(local_rustfs_config()).await
}

/// Builds a RustFS/S3-backed Origin repository from an explicit config.
pub async fn rustfs_repository(
    scope: RepositoryScope,
    config: s3::S3BackendConfig,
) -> Result<OriginRepository<s3::S3Backend>> {
    let backend = Arc::new(s3::S3Backend::new(config).await?);
    let storage = Arc::new(ScopedStorage::new(scope.storage_scope(), backend));
    Ok(OriginRepository::new(storage))
}

/// Builds a RustFS/S3-backed Origin repository catalog from an explicit config.
pub async fn rustfs_catalog(config: s3::S3BackendConfig) -> Result<OriginCatalog<s3::S3Backend>> {
    let backend = Arc::new(s3::S3Backend::new(config).await?);
    let scope = StorageScope::new(
        TenantId::new(CATALOG_TENANT),
        DatasetId::new(CATALOG_DATASET),
        EncryptionDomainId::new(CATALOG_ENCRYPTION_DOMAIN),
    );
    let storage = Arc::new(ScopedStorage::new(scope, backend));
    Ok(OriginCatalog::new(storage))
}

/// Returns local RustFS config, compatible with durable's integration env vars.
pub fn local_rustfs_config() -> s3::S3BackendConfig {
    s3::S3BackendConfig {
        endpoint: std::env::var("DURABLE_RUSTFS_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9100".into()),
        region: "us-east-1".into(),
        bucket: std::env::var("DURABLE_RUSTFS_BUCKET").unwrap_or_else(|_| "origin-dev".into()),
        access_key: std::env::var("DURABLE_RUSTFS_ACCESS_KEY").unwrap_or_else(|_| "origin".into()),
        secret_key: std::env::var("DURABLE_RUSTFS_SECRET_KEY")
            .unwrap_or_else(|_| "origin-secret".into()),
        create_bucket_if_missing: true,
    }
}

/// Runs a minimal smart-HTTP Git server on the supplied listener.
pub async fn serve_http(listener: TcpListener, config: s3::S3BackendConfig) -> Result<()> {
    let cache_temp = tempfile::tempdir()?;
    serve_http_with_cache_dir_and_guard(
        listener,
        config,
        cache_temp.path().to_path_buf(),
        Some(cache_temp),
    )
    .await
}

/// Runs a minimal smart-HTTP Git server using a persistent local repository cache.
pub async fn serve_http_with_cache_dir(
    listener: TcpListener,
    config: s3::S3BackendConfig,
    cache_root: impl Into<PathBuf>,
) -> Result<()> {
    serve_http_with_cache_dir_and_guard(listener, config, cache_root.into(), None).await
}

async fn serve_http_with_cache_dir_and_guard(
    listener: TcpListener,
    config: s3::S3BackendConfig,
    cache_root: PathBuf,
    cache_temp: Option<tempfile::TempDir>,
) -> Result<()> {
    fs::create_dir_all(&cache_root)?;
    info!(
        address = %listener.local_addr().map_err(|error| OriginError::Http(error.to_string()))?,
        cache_root = %cache_root.display(),
        "serving origin http"
    );
    let state = Arc::new(HttpState {
        cache_root,
        _cache_temp: cache_temp,
        config,
        repositories: Mutex::new(HashMap::new()),
    });
    let app = axum::Router::new()
        .route("/healthz", axum::routing::get(healthz))
        .route("/api/repos", axum::routing::get(api_repositories))
        .route(
            "/api/repos/{tenant}/{repo}/refs",
            axum::routing::get(api_refs),
        )
        .route(
            "/api/repos/{tenant}/{repo}/tree",
            axum::routing::get(api_tree),
        )
        .route(
            "/api/repos/{tenant}/{repo}/blob",
            axum::routing::get(api_blob),
        )
        .route(
            "/{tenant}/{repo}/{*git_path}",
            axum::routing::get(git_http).post(git_http),
        )
        .layer(axum::middleware::from_fn(log_request))
        .with_state(state);
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(|error| OriginError::Http(error.to_string()))
}

async fn healthz() -> axum::http::StatusCode {
    axum::http::StatusCode::NO_CONTENT
}

async fn log_request(request: axum::extract::Request, next: Next) -> axum::response::Response {
    let method = request.method().clone();
    let path = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let started = Instant::now();
    let response = next.run(request).await;
    let status = response.status();
    let elapsed_ms = started.elapsed().as_millis();
    if status.is_server_error() {
        error!(%method, %path, %status, elapsed_ms, "http request failed");
    } else if status.is_client_error() {
        warn!(%method, %path, %status, elapsed_ms, "http request rejected");
    } else {
        info!(%method, %path, %status, elapsed_ms, "http request");
    }
    response
}

#[derive(Debug, Serialize)]
struct ApiRepositories {
    repositories: Vec<RepositoryCatalogEntry>,
}

#[derive(Debug, Serialize)]
struct ApiRefs {
    refs: Vec<GitRefEntry>,
}

#[derive(Debug, Deserialize)]
struct TreeQuery {
    #[serde(rename = "ref")]
    reference: Option<String>,
    path: Option<String>,
}

#[derive(Debug, Serialize)]
struct ApiTree {
    reference: String,
    path: String,
    entries: Vec<ApiTreeEntry>,
}

#[derive(Debug, Serialize)]
struct ApiTreeEntry {
    name: String,
    path: String,
    kind: String,
    oid: String,
    size: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct BlobQuery {
    #[serde(rename = "ref")]
    reference: Option<String>,
    path: String,
}

#[derive(Debug, Serialize)]
struct ApiBlob {
    reference: String,
    path: String,
    content: String,
}

async fn api_repositories(
    axum::extract::State(state): axum::extract::State<Arc<HttpState>>,
) -> axum::response::Response {
    api_result(async move {
        let catalog = rustfs_catalog(state.config.clone()).await?;
        let catalog = catalog.read().await?;
        info!(
            repositories = catalog.repositories.len(),
            "api listed repositories"
        );
        Ok(Json(ApiRepositories {
            repositories: catalog.repositories,
        }))
    })
    .await
}

async fn api_refs(
    axum::extract::State(state): axum::extract::State<Arc<HttpState>>,
    axum::extract::Path((tenant, repo)): axum::extract::Path<(String, String)>,
) -> axum::response::Response {
    api_result(async move {
        validate_repository_name(&repo)?;
        let repository = rustfs_repository(
            RepositoryScope::new(tenant.clone(), repo.clone(), "edek"),
            state.config.clone(),
        )
        .await?;
        let publication = repository
            .current_publication()
            .await?
            .ok_or_else(|| OriginError::Http("repository not found".into()))?;
        info!(
            tenant = %tenant,
            repo = %repo,
            refs = publication.refs.len(),
            "api listed refs"
        );
        Ok(Json(ApiRefs {
            refs: publication.refs,
        }))
    })
    .await
}

async fn api_tree(
    axum::extract::State(state): axum::extract::State<Arc<HttpState>>,
    axum::extract::Path((tenant, repo)): axum::extract::Path<(String, String)>,
    axum::extract::Query(query): axum::extract::Query<TreeQuery>,
) -> axum::response::Response {
    api_result(async move {
        let reference = normalize_browser_ref(query.reference.as_deref());
        let path = normalize_browser_path(query.path.as_deref())?;
        let bare_repo = materialize_browser_repository(&state, &tenant, &repo).await?;
        let entries = list_git_tree(&bare_repo, &reference, &path)?;
        info!(
            tenant = %tenant,
            repo = %repo,
            reference = %reference,
            path = %path,
            entries = entries.len(),
            "api listed tree"
        );
        Ok(Json(ApiTree {
            reference,
            path,
            entries,
        }))
    })
    .await
}

async fn api_blob(
    axum::extract::State(state): axum::extract::State<Arc<HttpState>>,
    axum::extract::Path((tenant, repo)): axum::extract::Path<(String, String)>,
    axum::extract::Query(query): axum::extract::Query<BlobQuery>,
) -> axum::response::Response {
    api_result(async move {
        let reference = normalize_browser_ref(query.reference.as_deref());
        let path = normalize_browser_path(Some(&query.path))?;
        let bare_repo = materialize_browser_repository(&state, &tenant, &repo).await?;
        let content = read_git_blob(&bare_repo, &reference, &path)?;
        info!(
            tenant = %tenant,
            repo = %repo,
            reference = %reference,
            path = %path,
            bytes = content.len(),
            "api read blob"
        );
        Ok(Json(ApiBlob {
            reference,
            path,
            content,
        }))
    })
    .await
}

async fn api_result<F, T>(future: F) -> axum::response::Response
where
    F: std::future::Future<Output = Result<Json<T>>>,
    T: Serialize,
{
    match future.await {
        Ok(response) => response.into_response(),
        Err(error) => {
            let status = match error {
                OriginError::UnsafePath(_) => axum::http::StatusCode::NOT_FOUND,
                OriginError::Conflict => axum::http::StatusCode::CONFLICT,
                OriginError::Http(_) => axum::http::StatusCode::BAD_REQUEST,
                _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, error.to_string()).into_response()
        }
    }
}

struct HttpState {
    cache_root: PathBuf,
    _cache_temp: Option<tempfile::TempDir>,
    config: s3::S3BackendConfig,
    repositories: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl HttpState {
    fn repository_lock(&self, tenant: &str, repo: &str) -> Arc<tokio::sync::Mutex<()>> {
        let key = format!("{tenant}/{repo}");
        let mut repositories = self
            .repositories
            .lock()
            .expect("repository lock map mutex poisoned");
        repositories
            .entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

async fn git_http(
    axum::extract::State(state): axum::extract::State<Arc<HttpState>>,
    axum::extract::Path((tenant, repo_segment, git_path)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    method: axum::http::Method,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    match git_http_inner(GitHttpRequest {
        state,
        tenant,
        repo_segment,
        git_path,
        query: raw_query.unwrap_or_default(),
        method,
        headers,
        body,
    })
    .await
    {
        Ok(response) => response,
        Err(error) => {
            let status = match error {
                OriginError::UnsafePath(_) => axum::http::StatusCode::NOT_FOUND,
                OriginError::Conflict => axum::http::StatusCode::CONFLICT,
                _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, error.to_string()).into_response()
        }
    }
}

struct GitHttpRequest {
    state: Arc<HttpState>,
    tenant: String,
    repo_segment: String,
    git_path: String,
    query: String,
    method: axum::http::Method,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
}

async fn git_http_inner(request: GitHttpRequest) -> Result<axum::response::Response> {
    let repo_name = request
        .repo_segment
        .strip_suffix(".git")
        .ok_or_else(|| OriginError::UnsafePath(request.repo_segment.clone()))?;
    validate_path_segment(&request.tenant)?;
    validate_repository_name(repo_name)?;
    let repo_lock = request.state.repository_lock(&request.tenant, repo_name);
    let _guard = repo_lock.lock().await;
    let scope = RepositoryScope::new(request.tenant.clone(), repo_name, "edek");
    let repository = rustfs_repository(scope, request.state.config.clone()).await?;
    rustfs_catalog(request.state.config.clone())
        .await?
        .register(&request.tenant, repo_name)
        .await?;
    let tenant_root = safe_join(&request.state.cache_root, &request.tenant)?;
    fs::create_dir_all(&tenant_root)?;
    let bare_repo = safe_join(&tenant_root, &request.repo_segment)?;
    let materialized = repository
        .materialize_bare_repository_cached(&bare_repo)
        .await?;

    let path_info = format!("/{}/{}", request.repo_segment, request.git_path);
    let operation = git_operation(&path_info, &request.query);
    info!(
        tenant = %request.tenant,
        repo = %repo_name,
        operation,
        path_info = %path_info,
        method = %request.method,
        bytes = request.body.len(),
        "git http operation started"
    );
    let output = run_git_http_backend(
        &tenant_root,
        &path_info,
        &request.query,
        request.method,
        request.headers,
        request.body,
    )?;
    if path_info.ends_with("/git-receive-pack") {
        repository
            .publish_materialized_bare_repository(&bare_repo, materialized)
            .await?;
        info!(
            tenant = %request.tenant,
            repo = %repo_name,
            "git receive-pack published"
        );
    }
    let response = cgi_to_response(&output)?;
    info!(
        tenant = %request.tenant,
        repo = %repo_name,
        operation,
        status = %response.status(),
        "git http operation completed"
    );
    Ok(response)
}

fn run_git_http_backend(
    project_root: &Path,
    path_info: &str,
    query: &str,
    method: axum::http::Method,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Vec<u8>> {
    let mut command = Command::new("git");
    command
        .arg("http-backend")
        .env("GIT_PROJECT_ROOT", path_str(project_root)?)
        .env("GIT_HTTP_EXPORT_ALL", "")
        .env("REQUEST_METHOD", method.as_str())
        .env("PATH_INFO", path_info)
        .env("QUERY_STRING", query)
        .env("CONTENT_LENGTH", body.len().to_string());
    if let Some(content_type) = headers.get(axum::http::header::CONTENT_TYPE) {
        command.env(
            "CONTENT_TYPE",
            content_type
                .to_str()
                .map_err(|error| OriginError::Http(error.to_string()))?,
        );
    }
    let output = command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(mut stdin) = child.stdin.take() {
                stdin.write_all(&body)?;
            }
            child.wait_with_output()
        })?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(OriginError::Git {
            program: "git".into(),
            args: vec!["http-backend".into()],
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

fn cgi_to_response(bytes: &[u8]) -> Result<axum::response::Response> {
    let (header_bytes, body) = split_cgi_response(bytes)
        .ok_or_else(|| OriginError::Http("git http-backend response had no headers".into()))?;
    let headers = String::from_utf8_lossy(header_bytes);
    let mut status = axum::http::StatusCode::OK;
    let mut response_headers = BTreeMap::new();
    for line in headers.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("Status") {
            let code = value
                .trim()
                .split(' ')
                .next()
                .and_then(|code| code.parse::<u16>().ok())
                .ok_or_else(|| OriginError::Http(format!("invalid CGI status: {value}")))?;
            status = axum::http::StatusCode::from_u16(code)
                .map_err(|error| OriginError::Http(error.to_string()))?;
        } else {
            response_headers.insert(name.trim().to_string(), value.trim().to_string());
        }
    }
    let mut builder = axum::http::Response::builder().status(status);
    for (name, value) in response_headers {
        builder = builder.header(name, value);
    }
    builder
        .body(axum::body::Body::from(body.to_vec()))
        .map_err(|error| OriginError::Http(error.to_string()))
}

fn split_cgi_response(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| (&bytes[..index], &bytes[index + 4..]))
        .or_else(|| {
            bytes
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|index| (&bytes[..index], &bytes[index + 2..]))
        })
}

fn git_operation(path_info: &str, query: &str) -> &'static str {
    if path_info.ends_with("/git-receive-pack") || query.contains("service=git-receive-pack") {
        "receive-pack"
    } else if path_info.ends_with("/git-upload-pack") || query.contains("service=git-upload-pack") {
        "upload-pack"
    } else {
        "metadata"
    }
}

fn materialize_empty_bare_repo(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    command("git", ["init", "--bare", path_str(path)?])?;
    git(path, ["config", "http.receivepack", "true"])?;
    Ok(())
}

fn cache_marker_matches(bare_repo: &Path, digest: &str) -> bool {
    fs::read_to_string(cache_marker_path(bare_repo))
        .map(|cached| cached.trim() == digest)
        .unwrap_or(false)
}

fn write_cache_marker(bare_repo: &Path, digest: &str) -> Result<()> {
    fs::write(cache_marker_path(bare_repo), format!("{digest}\n"))?;
    Ok(())
}

fn cache_marker_path(bare_repo: &Path) -> PathBuf {
    bare_repo.join(CACHE_MARKER_FILE)
}

fn should_persist_git_file(path: &str) -> bool {
    path == "HEAD"
        || path == "config"
        || path == "packed-refs"
        || path.starts_with("objects/")
        || path.starts_with("refs/")
}

fn normalize_relative_path(path: &Path) -> Result<String> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => parts.push(
                value
                    .to_str()
                    .ok_or_else(|| OriginError::UnsafePath(path.display().to_string()))?,
            ),
            _ => return Err(OriginError::UnsafePath(path.display().to_string())),
        }
    }
    if parts.is_empty() {
        return Err(OriginError::UnsafePath(path.display().to_string()));
    }
    Ok(parts.join("/"))
}

fn safe_join(base: &Path, relative: &str) -> Result<PathBuf> {
    let path = Path::new(relative);
    if path.is_absolute() {
        return Err(OriginError::UnsafePath(relative.into()));
    }
    let normalized = normalize_relative_path(path)?;
    Ok(base.join(normalized))
}

fn validate_path_segment(value: &str) -> Result<()> {
    if value.is_empty()
        || value.starts_with('-')
        || value.contains('/')
        || value.contains('\\')
        || value.contains("..")
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(OriginError::UnsafePath(value.into()));
    }
    Ok(())
}

fn validate_repository_name(value: &str) -> Result<()> {
    validate_path_segment(value)?;
    if value.ends_with(".git") {
        return Err(OriginError::UnsafePath(value.into()));
    }
    Ok(())
}

fn normalize_browser_ref(value: Option<&str>) -> String {
    value
        .filter(|reference| !reference.trim().is_empty())
        .unwrap_or("refs/heads/main")
        .to_string()
}

fn validate_browser_ref(value: &str) -> Result<()> {
    if value.is_empty()
        || value.starts_with('-')
        || value.contains("..")
        || value.contains(' ')
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(OriginError::UnsafePath(value.into()));
    }
    Ok(())
}

fn normalize_browser_path(value: Option<&str>) -> Result<String> {
    let Some(value) = value.filter(|path| !path.is_empty()) else {
        return Ok(String::new());
    };
    let path = Path::new(value);
    if path.is_absolute() {
        return Err(OriginError::UnsafePath(value.into()));
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(
                part.to_str()
                    .ok_or_else(|| OriginError::UnsafePath(value.into()))?,
            ),
            _ => return Err(OriginError::UnsafePath(value.into())),
        }
    }
    Ok(parts.join("/"))
}

async fn materialize_browser_repository(
    state: &HttpState,
    tenant: &str,
    repo: &str,
) -> Result<PathBuf> {
    validate_path_segment(tenant)?;
    validate_repository_name(repo)?;
    let repo_lock = state.repository_lock(tenant, repo);
    let _guard = repo_lock.lock().await;
    let repository = rustfs_repository(
        RepositoryScope::new(tenant.to_string(), repo.to_string(), "edek"),
        state.config.clone(),
    )
    .await?;
    if repository.current_publication().await?.is_none() {
        return Err(OriginError::Http("repository not found".into()));
    }
    let tenant_root = safe_join(&state.cache_root, tenant)?;
    fs::create_dir_all(&tenant_root)?;
    let bare_repo = safe_join(&tenant_root, &format!("{repo}.git"))?;
    repository
        .materialize_bare_repository_cached(&bare_repo)
        .await?;
    Ok(bare_repo)
}

fn list_git_tree(repo: &Path, reference: &str, path: &str) -> Result<Vec<ApiTreeEntry>> {
    validate_browser_ref(reference)?;
    let treeish = git_treeish(reference, path);
    let output = git_bytes_output(repo, ["ls-tree", "-z", "-l", &treeish])?;
    let mut entries = Vec::new();
    for raw in output.split(|byte| *byte == 0) {
        if raw.is_empty() {
            continue;
        }
        let line = String::from_utf8_lossy(raw);
        let (metadata, name) = line
            .split_once('\t')
            .ok_or_else(|| OriginError::Http(format!("invalid git tree line: {line}")))?;
        let mut fields = metadata.split_whitespace();
        let _mode = fields
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid git tree line: {line}")))?;
        let kind = fields
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid git tree line: {line}")))?;
        let oid = fields
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid git tree line: {line}")))?;
        let size = fields
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid git tree line: {line}")))?;
        if fields.next().is_some() || !is_git_oid(oid) || !is_git_tree_kind(kind) {
            return Err(OriginError::Http(format!("invalid git tree line: {line}")));
        }
        let entry_path = if path.is_empty() {
            name.to_string()
        } else {
            format!("{path}/{name}")
        };
        entries.push(ApiTreeEntry {
            name: name.to_string(),
            path: entry_path,
            kind: kind.to_string(),
            oid: oid.to_string(),
            size: if size == "-" {
                None
            } else {
                Some(size.parse().map_err(|error| {
                    OriginError::Http(format!("invalid git tree size: {error}"))
                })?)
            },
        });
    }
    entries.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .reverse()
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(entries)
}

fn read_git_blob(repo: &Path, reference: &str, path: &str) -> Result<String> {
    validate_browser_ref(reference)?;
    if path.is_empty() {
        return Err(OriginError::UnsafePath(path.into()));
    }
    let output = git_bytes_output(repo, ["show", &git_treeish(reference, path)])?;
    Ok(String::from_utf8_lossy(&output).into_owned())
}

fn git_treeish(reference: &str, path: &str) -> String {
    if path.is_empty() {
        reference.to_string()
    } else {
        format!("{reference}:{path}")
    }
}

fn is_git_tree_kind(value: &str) -> bool {
    matches!(value, "blob" | "commit" | "tag" | "tree")
}

fn capture_git_refs(repo: &Path) -> Result<Vec<GitRefEntry>> {
    let output = git_output(
        repo,
        ["for-each-ref", "--format=%(refname)%00%(objectname)"],
    )?;
    let mut refs = Vec::new();
    for line in output.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let (name, target) = line
            .split_once('\0')
            .ok_or_else(|| OriginError::Http(format!("invalid git ref line: {line}")))?;
        if !name.starts_with("refs/") || !is_git_oid(target) {
            return Err(OriginError::Http(format!("invalid git ref line: {line}")));
        }
        refs.push(GitRefEntry {
            name: name.to_string(),
            target: target.to_string(),
        });
    }
    refs.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(refs)
}

fn capture_git_objects(repo: &Path) -> Result<Vec<GitObjectEntry>> {
    let output = git_output(
        repo,
        [
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname) %(objecttype) %(objectsize)",
        ],
    )?;
    let mut objects = BTreeMap::new();
    for line in output.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let mut parts = line.split(' ');
        let oid = parts
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid git object line: {line}")))?;
        let kind = parts
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid git object line: {line}")))?;
        let size = parts
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid git object line: {line}")))?
            .parse()
            .map_err(|error| OriginError::Http(format!("invalid git object size: {error}")))?;
        if parts.next().is_some() || !is_git_oid(oid) || !is_git_object_kind(kind) {
            return Err(OriginError::Http(format!(
                "invalid git object line: {line}"
            )));
        }
        let entry = GitObjectEntry {
            oid: oid.to_string(),
            kind: kind.to_string(),
            size,
        };
        if let Some(previous) = objects.insert(entry.oid.clone(), entry.clone()) {
            if previous != entry {
                return Err(OriginError::Http(format!(
                    "conflicting metadata for git object {oid}"
                )));
            }
        }
    }
    Ok(objects.into_values().collect())
}

fn is_git_oid(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_git_object_kind(value: &str) -> bool {
    matches!(value, "blob" | "commit" | "tag" | "tree")
}

fn publication_digest(
    refs: &[GitRefEntry],
    objects: &[GitObjectEntry],
    files: &[GitFileEntry],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"origin.git.publication.digest.v1\0");
    for git_ref in refs {
        hasher.update(b"ref\0");
        hasher.update(git_ref.name.as_bytes());
        hasher.update([0]);
        hasher.update(git_ref.target.as_bytes());
        hasher.update([0]);
    }
    for object in objects {
        hasher.update(b"object\0");
        hasher.update(object.oid.as_bytes());
        hasher.update([0]);
        hasher.update(object.kind.as_bytes());
        hasher.update([0]);
        hasher.update(object.size.to_string().as_bytes());
        hasher.update([0]);
    }
    for file in files {
        hasher.update(b"file\0");
        hasher.update(file.path.as_bytes());
        hasher.update([0]);
        hasher.update(file.object.object_id().as_bytes());
        hasher.update([0]);
    }
    hex(&hasher.finalize())
}

fn git<I, S>(repo: &Path, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut all_args = vec!["-C".to_string(), path_str(repo)?.to_string()];
    all_args.extend(args.into_iter().map(|arg| arg.as_ref().to_string()));
    command("git", all_args)
}

fn git_output<I, S>(repo: &Path, args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let output = git_bytes_output(repo, args)?;
    Ok(String::from_utf8_lossy(&output).into_owned())
}

fn git_bytes_output<I, S>(repo: &Path, args: I) -> Result<Vec<u8>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut all_args = vec!["-C".to_string(), path_str(repo)?.to_string()];
    all_args.extend(args.into_iter().map(|arg| arg.as_ref().to_string()));
    command_output("git", all_args)
}

fn command<I, S>(program: &str, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    command_output(program, args).map(|_| ())
}

fn command_output<I, S>(program: &str, args: I) -> Result<Vec<u8>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect::<Vec<_>>();
    let output = Command::new(program).args(&args).output()?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(OriginError::Git {
            program: program.into(),
            args,
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

fn path_str(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| OriginError::UnsafePath(path.display().to_string()))
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use substrate::InMemoryBackend;

    #[tokio::test]
    async fn empty_repository_round_trips_through_durable() {
        let backend = Arc::new(InMemoryBackend::new());
        let storage = Arc::new(ScopedStorage::new(test_scope("repo"), backend));
        let repo = OriginRepository::new(storage);
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first.git");
        let second = temp.path().join("second.git");

        repo.materialize_bare_repository(&first).await.unwrap();
        repo.materialize_bare_repository(&second).await.unwrap();

        assert!(second.join("HEAD").exists());
        assert_eq!(
            repo.current_publication().await.unwrap().unwrap().version,
            1
        );
    }

    #[tokio::test]
    async fn publication_manifest_records_refs_and_objects() {
        let backend = Arc::new(InMemoryBackend::new());
        let repo = OriginRepository::new(Arc::new(ScopedStorage::new(test_scope("repo"), backend)));
        let temp = tempfile::tempdir().unwrap();
        let bare = temp.path().join("origin.git");
        let client = temp.path().join("client");

        let materialized = repo.materialize_bare_repository(&bare).await.unwrap();
        command("git", ["init", path_str(&client).unwrap()]).unwrap();
        git(&client, ["config", "user.email", "agent@example.com"]).unwrap();
        git(&client, ["config", "user.name", "Agent"]).unwrap();
        fs::write(client.join("README.md"), "hello from manifest\n").unwrap();
        git(&client, ["add", "README.md"]).unwrap();
        git(&client, ["commit", "-m", "initial"]).unwrap();
        git(&client, ["branch", "-M", "main"]).unwrap();
        git(
            &client,
            ["remote", "add", "origin", path_str(&bare).unwrap()],
        )
        .unwrap();
        git(&client, ["push", "-u", "origin", "main"]).unwrap();

        repo.publish_materialized_bare_repository(&bare, materialized)
            .await
            .unwrap();
        let publication = repo.current_publication().await.unwrap().unwrap();

        let main = publication
            .refs
            .iter()
            .find(|git_ref| git_ref.name == "refs/heads/main")
            .expect("main ref should be recorded");
        assert!(is_git_oid(&main.target));
        assert!(publication
            .objects
            .iter()
            .any(|object| object.kind == "commit"));
        assert!(publication
            .objects
            .iter()
            .any(|object| object.kind == "tree"));
        assert!(publication
            .objects
            .iter()
            .any(|object| object.kind == "blob"));
        assert!(publication
            .files
            .iter()
            .any(|file| file.path == "refs/heads/main"));
    }

    #[tokio::test]
    async fn repository_scopes_are_isolated() {
        let backend = Arc::new(InMemoryBackend::new());
        let first = OriginRepository::new(Arc::new(ScopedStorage::new(
            test_scope("first"),
            backend.clone(),
        )));
        let second =
            OriginRepository::new(Arc::new(ScopedStorage::new(test_scope("second"), backend)));
        let temp = tempfile::tempdir().unwrap();

        first
            .materialize_bare_repository(temp.path().join("first.git"))
            .await
            .unwrap();

        assert!(first.current_publication().await.unwrap().is_some());
        assert!(second.current_publication().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn stale_materialized_repository_publish_is_rejected() {
        let backend = Arc::new(InMemoryBackend::new());
        let repo = OriginRepository::new(Arc::new(ScopedStorage::new(test_scope("repo"), backend)));
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first.git");
        let second = temp.path().join("second.git");

        let first_materialized = repo.materialize_bare_repository(&first).await.unwrap();
        let second_materialized = repo.materialize_bare_repository(&second).await.unwrap();

        let first_result = repo
            .publish_materialized_bare_repository(&first, first_materialized)
            .await;
        assert!(first_result.is_ok());

        let second_result = repo
            .publish_materialized_bare_repository(&second, second_materialized)
            .await;
        assert!(matches!(second_result, Err(OriginError::Conflict)));
    }

    fn test_scope(name: &str) -> StorageScope {
        RepositoryScope::new("tenant", name, "edek").storage_scope()
    }
}
