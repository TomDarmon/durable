//! Origin-like Git storage built on durable primitives.
//!
//! The durable library stays Git-agnostic. This crate owns Git repository
//! identity, bare-repository materialization, ref publication, and recovery.

use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    process::Command,
    sync::Arc,
};
use substrate::{
    compute_object_id, DatasetId, Durability, DurableError, DurableObjectRef, EncryptionDomainId,
    ExpectedRevision, ImmutableObjects, ObjectFormat, PublishOutcome, RawBackend, RootName,
    RootRegister, ScopedStorage, StorageScope, TenantId,
};
use thiserror::Error;
use tokio::net::TcpListener;
use walkdir::WalkDir;

/// Origin result type.
pub type Result<T> = std::result::Result<T, OriginError>;

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

/// Published state for one Git repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitPublication {
    /// Stable format version.
    pub version: u32,
    /// Deterministic digest of captured file paths and durable object IDs.
    pub digest: String,
    /// Files required to reconstruct the bare serving repository.
    pub files: Vec<GitFileEntry>,
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

    /// Captures a bare Git repository and publishes it as the new root state.
    pub async fn publish_bare_repository(&self, bare_repo: impl AsRef<Path>) -> Result<()> {
        let bare_repo = bare_repo.as_ref();
        git(bare_repo, ["fsck", "--no-dangling"])?;

        let publication = self.capture_bare_repository(bare_repo).await?;
        let publication_ref = self.put_publication(&publication).await?;
        let root_value = serde_json::to_vec(&publication_ref)?;
        let expected = RootRegister::read(self.storage.as_ref(), &self.root_name)
            .await?
            .map_or(ExpectedRevision::Missing, |root| {
                ExpectedRevision::Exact(root.revision())
            });

        match self
            .storage
            .compare_exchange(&self.root_name, expected, root_value)
            .await?
        {
            PublishOutcome::Applied(_) => Ok(()),
            PublishOutcome::Conflict { .. } => Err(OriginError::Conflict),
            PublishOutcome::OutcomeUnknown => Err(OriginError::OutcomeUnknown),
        }
    }

    /// Materializes the current durable repository state to a local bare repo.
    pub async fn materialize_bare_repository(&self, bare_repo: impl AsRef<Path>) -> Result<()> {
        let bare_repo = bare_repo.as_ref();
        if let Some(publication) = self.current_publication().await? {
            materialize_empty_bare_repo(bare_repo)?;
            for file in publication.files {
                let path = safe_join(bare_repo, &file.path)?;
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let bytes = ImmutableObjects::read(self.storage.as_ref(), &file.object).await?;
                fs::write(path, bytes)?;
            }
            git(bare_repo, ["fsck", "--no-dangling"])?;
        } else {
            materialize_empty_bare_repo(bare_repo)?;
            self.publish_bare_repository(bare_repo).await?;
        }
        Ok(())
    }

    /// Reads the currently published repository state.
    pub async fn current_publication(&self) -> Result<Option<GitPublication>> {
        let Some(root) = RootRegister::read(self.storage.as_ref(), &self.root_name).await? else {
            return Ok(None);
        };
        let publication_ref: DurableObjectRef = serde_json::from_slice(root.value())?;
        let bytes = ImmutableObjects::read(self.storage.as_ref(), &publication_ref).await?;
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    async fn capture_bare_repository(&self, bare_repo: &Path) -> Result<GitPublication> {
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
        Ok(GitPublication {
            version: 1,
            digest: publication_digest(&files),
            files,
        })
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
    let state = Arc::new(HttpState { config });
    let app = axum::Router::new()
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

#[derive(Clone)]
struct HttpState {
    config: s3::S3BackendConfig,
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
    let scope = RepositoryScope::new(request.tenant, repo_name, "edek");
    let repository = rustfs_repository(scope, request.state.config.clone()).await?;
    let temp = tempfile::tempdir()?;
    let bare_repo = temp.path().join(&request.repo_segment);
    repository.materialize_bare_repository(&bare_repo).await?;

    let path_info = format!("/{}/{}", request.repo_segment, request.git_path);
    let output = run_git_http_backend(
        temp.path(),
        &path_info,
        &request.query,
        request.method,
        request.headers,
        request.body,
    )?;
    if path_info.ends_with("/git-receive-pack") {
        repository.publish_bare_repository(&bare_repo).await?;
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

fn publication_digest(files: &[GitFileEntry]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"origin.git.publication.digest.v1\0");
    for file in files {
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

fn command<I, S>(program: &str, args: I) -> Result<()>
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
        Ok(())
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

    fn test_scope(name: &str) -> StorageScope {
        RepositoryScope::new("tenant", name, "edek").storage_scope()
    }
}
