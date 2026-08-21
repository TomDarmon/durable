mod support;

use origin::{local_rustfs_repository, RepositoryScope};
use std::error::Error;
use support::{create_client_with_initial_commit, git, git_stdout, path_str};

#[tokio::test]
#[ignore = "requires `make e2e-up`"]
async fn git_push_survives_server_cache_loss_and_can_be_cloned() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let remote_cache = temp.path().join("remote-cache.git");
    let restored_cache = temp.path().join("restored-cache.git");
    let client = temp.path().join("client");
    let clone = temp.path().join("clone");
    let scope = RepositoryScope::new("tenant", format!("repo-{}", uuid::Uuid::new_v4()), "edek");

    let origin = local_rustfs_repository(scope.clone()).await?;
    let materialized = origin.materialize_bare_repository(&remote_cache).await?;

    git(temp.path(), ["init", path_str(&client)?])?;
    git(&client, ["config", "user.email", "agent@example.com"])?;
    git(&client, ["config", "user.name", "Agent"])?;
    std::fs::write(client.join("README.md"), "hello from origin\n")?;
    git(&client, ["add", "README.md"])?;
    git(&client, ["commit", "-m", "initial"])?;
    git(&client, ["branch", "-M", "main"])?;
    git(
        &client,
        ["remote", "add", "origin", path_str(&remote_cache)?],
    )?;
    git(&client, ["push", "-u", "origin", "main"])?;

    origin
        .publish_materialized_bare_repository(&remote_cache, materialized)
        .await?;
    std::fs::remove_dir_all(&remote_cache)?;

    let reopened = local_rustfs_repository(scope).await?;
    reopened
        .materialize_bare_repository(&restored_cache)
        .await?;
    git(
        temp.path(),
        ["clone", path_str(&restored_cache)?, path_str(&clone)?],
    )?;

    assert_eq!(
        std::fs::read_to_string(clone.join("README.md"))?,
        "hello from origin\n"
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires `make e2e-up`"]
async fn second_push_updates_the_durable_repository_root() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let remote_cache = temp.path().join("remote-cache.git");
    let restored_cache = temp.path().join("restored-cache.git");
    let client = temp.path().join("client");
    let clone = temp.path().join("clone");
    let scope = RepositoryScope::new("tenant", format!("repo-{}", uuid::Uuid::new_v4()), "edek");

    let origin = local_rustfs_repository(scope.clone()).await?;
    let materialized = origin.materialize_bare_repository(&remote_cache).await?;
    create_client_with_initial_commit(temp.path(), &client, path_str(&remote_cache)?)?;
    git(&client, ["push", "-u", "origin", "main"])?;
    let materialized = origin
        .publish_materialized_bare_repository(&remote_cache, materialized)
        .await?;

    std::fs::write(client.join("README.md"), "hello twice\n")?;
    git(&client, ["add", "README.md"])?;
    git(&client, ["commit", "-m", "second"])?;
    git(&client, ["push", "origin", "main"])?;
    origin
        .publish_materialized_bare_repository(&remote_cache, materialized)
        .await?;
    std::fs::remove_dir_all(&remote_cache)?;

    let reopened = local_rustfs_repository(scope).await?;
    reopened
        .materialize_bare_repository(&restored_cache)
        .await?;
    git(
        temp.path(),
        ["clone", path_str(&restored_cache)?, path_str(&clone)?],
    )?;

    assert_eq!(
        std::fs::read_to_string(clone.join("README.md"))?,
        "hello twice\n"
    );
    assert_eq!(
        git_stdout(&clone, ["rev-list", "--count", "HEAD"])?.trim(),
        "2"
    );

    Ok(())
}
