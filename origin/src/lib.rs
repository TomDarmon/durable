//! Origin-like Git storage built on durable primitives.
//!
//! The durable library stays Git-agnostic. This crate owns Git repository
//! identity, bare-repository materialization, ref publication, and recovery.

use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
};
use substrate::{
    compute_object_id, DatasetId, Durability, DurableError, DurableObjectRef, EncryptionDomainId,
    ExpectedRevision, ImmutableObjects, ObjectFormat, PublishOutcome, RawBackend, RootName,
    RootRegister, RootState, ScopedStorage, StorageScope, TenantId,
};
use thiserror::Error;
use tokio::net::TcpListener;
use walkdir::WalkDir;

/// Origin result type.
pub type Result<T> = std::result::Result<T, OriginError>;

const CACHE_MARKER_FILE: &str = ".origin-cache-publication";

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
                Ok(MaterializedRepository {
                    expected: ExpectedRevision::Exact(root.revision()),
                })
            }
            PublishOutcome::Conflict { .. } => Err(OriginError::Conflict),
            PublishOutcome::OutcomeUnknown => Err(OriginError::OutcomeUnknown),
        }
    }

    /// Materializes the current durable repository state to a local bare repo.
    pub async fn materialize_bare_repository(
        &self,
        bare_repo: impl AsRef<Path>,
    ) -> Result<MaterializedRepository> {
        let bare_repo = bare_repo.as_ref();
        if let Some(root) = RootRegister::read(self.storage.as_ref(), &self.root_name).await? {
            let publication = self.load_publication(&root).await?;
            self.write_publication_to_bare_repository(bare_repo, &publication)
                .await?;
            Ok(MaterializedRepository {
                expected: ExpectedRevision::Exact(root.revision()),
            })
        } else {
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
                return Ok(MaterializedRepository {
                    expected: ExpectedRevision::Exact(root.revision()),
                });
            }
            self.write_publication_to_bare_repository(bare_repo, &publication)
                .await?;
            Ok(MaterializedRepository {
                expected: ExpectedRevision::Exact(root.revision()),
            })
        } else {
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

/// Builds a RustFS/S3-backed Origin repository from an explicit config.
pub async fn rustfs_repository(
    scope: RepositoryScope,
    config: s3::S3BackendConfig,
) -> Result<OriginRepository<s3::S3Backend>> {
    let backend = Arc::new(s3::S3Backend::new(config).await?);
    let storage = Arc::new(ScopedStorage::new(scope.storage_scope(), backend));
    Ok(OriginRepository::new(storage))
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
    let state = Arc::new(HttpState {
        cache_root,
        _cache_temp: cache_temp,
        config,
        repositories: Mutex::new(HashMap::new()),
    });
    let app = axum::Router::new()
        .route("/healthz", axum::routing::get(healthz))
        .route(
            "/{tenant}/{repo}/{*git_path}",
            axum::routing::get(git_http).post(git_http),
        )
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
    let repo_lock = request.state.repository_lock(&request.tenant, repo_name);
    let _guard = repo_lock.lock().await;
    let scope = RepositoryScope::new(request.tenant.clone(), repo_name, "edek");
    let repository = rustfs_repository(scope, request.state.config.clone()).await?;
    let tenant_root = safe_join(&request.state.cache_root, &request.tenant)?;
    fs::create_dir_all(&tenant_root)?;
    let bare_repo = safe_join(&tenant_root, &request.repo_segment)?;
    let materialized = repository
        .materialize_bare_repository_cached(&bare_repo)
        .await?;

    let path_info = format!("/{}/{}", request.repo_segment, request.git_path);
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
    }
    cgi_to_response(&output)
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
    let mut all_args = vec!["-C".to_string(), path_str(repo)?.to_string()];
    all_args.extend(args.into_iter().map(|arg| arg.as_ref().to_string()));
    let output = command_output("git", all_args)?;
    Ok(String::from_utf8_lossy(&output).into_owned())
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
