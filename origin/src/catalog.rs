use crate::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use substrate::{
    compute_object_id, Durability, DurableObjectRef, ExpectedRevision, ImmutableObjects,
    ObjectFormat, PublishOutcome, RawBackend, RootName, RootRegister, RootState, ScopedStorage,
};
use tracing::{debug, error, info, warn};

pub(crate) const CATALOG_TENANT: &str = "__origin_system";
pub(crate) const CATALOG_DATASET: &str = "repository-catalog";
pub(crate) const CATALOG_ENCRYPTION_DOMAIN: &str = "edek";

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
        let catalog = self.load_catalog(&root).await?;
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
            let mut catalog = if let Some(root) = current_root.as_ref() {
                self.load_catalog(root).await?
            } else {
                RepositoryCatalog::default()
            };
            if catalog.repositories.contains(&entry) {
                debug!(tenant, repo, "repository already present in catalog");
                return Ok(());
            }
            catalog.repositories.push(entry.clone());
            catalog.repositories.sort();
            let catalog_ref = self.put_catalog(&catalog).await?;
            let value = serde_json::to_vec(&catalog_ref)?;
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
                    return Err(crate::OriginError::OutcomeUnknown);
                }
            }
        }
        warn!(
            tenant,
            repo, "repository catalog registration retries exhausted"
        );
        Err(crate::OriginError::Conflict)
    }

    async fn load_catalog(&self, root: &RootState) -> Result<RepositoryCatalog> {
        match serde_json::from_slice::<DurableObjectRef>(root.value()) {
            Ok(catalog_ref) => {
                let bytes = ImmutableObjects::read(self.storage.as_ref(), &catalog_ref).await?;
                Ok(serde_json::from_slice(&bytes)?)
            }
            Err(_) => Ok(serde_json::from_slice(root.value())?),
        }
    }

    async fn put_catalog(&self, catalog: &RepositoryCatalog) -> Result<DurableObjectRef> {
        let bytes = serde_json::to_vec(catalog)?;
        let format = ObjectFormat::Custom("origin.repository.catalog.v1".into());
        let id = compute_object_id(&format, &bytes);
        Ok(self
            .storage
            .put(id, format, &bytes, Durability::BackendDefault)
            .await?)
    }
}
