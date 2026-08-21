use origin::{local_rustfs_config, local_rustfs_repository, serve_http, RepositoryScope};
use std::{
    error::Error,
    path::Path,
    process::Command,
    thread,
    time::{Duration, Instant},
};
use tokio::net::TcpListener;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires `make e2e-up`"]
async fn smart_http_server_accepts_push_and_serves_clone() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let client = temp.path().join("client");
    let clone = temp.path().join("clone");
    let repo_name = format!("repo-{}", uuid::Uuid::new_v4());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        serve_http(listener, local_rustfs_config())
            .await
            .expect("origin http server failed");
    });
    let remote_url = format!("http://{address}/tenant/{repo_name}.git");

    create_client_with_initial_commit(temp.path(), &client, &remote_url)?;
    git(&client, ["push", "-u", "origin", "main"])?;
    git(temp.path(), ["clone", &remote_url, path_str(&clone)?])?;

    assert_eq!(
        std::fs::read_to_string(clone.join("README.md"))?,
        "hello from origin\n"
    );

    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires `make e2e-up`"]
async fn docker_compose_origin_service_accepts_push_and_serves_clone() -> Result<(), Box<dyn Error>>
{
    let temp = tempfile::tempdir()?;
    let client = temp.path().join("client");
    let clone = temp.path().join("clone");
    let repo_name = format!("repo-{}", uuid::Uuid::new_v4());
    let remote_url = format!("http://127.0.0.1:9200/tenant/{repo_name}.git");

    create_client_with_initial_commit(temp.path(), &client, &remote_url)?;
    git(&client, ["push", "-u", "origin", "main"])?;
    git(temp.path(), ["clone", &remote_url, path_str(&clone)?])?;

    assert_eq!(
        std::fs::read_to_string(clone.join("README.md"))?,
        "hello from origin\n"
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires `make e2e-up`"]
async fn docker_compose_origin_service_rejects_stale_push() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    let final_clone = temp.path().join("final-clone");
    let repo_name = format!("repo-{}", uuid::Uuid::new_v4());
    let remote_url = format!("http://127.0.0.1:9200/tenant/{repo_name}.git");

    create_client_with_initial_commit(temp.path(), &first, &remote_url)?;
    git(&first, ["push", "-u", "origin", "main"])?;
    git(temp.path(), ["clone", &remote_url, path_str(&second)?])?;
    git(&second, ["config", "user.email", "agent@example.com"])?;
    git(&second, ["config", "user.name", "Agent"])?;

    write_commit_push(&first, "README.md", "winner\n", "winning update")?;
    write_commit(&second, "README.md", "stale loser\n", "stale update")?;
    let stale = git_result(&second, ["push", "origin", "main"])?;
    assert!(
        !stale.status.success(),
        "stale push unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stale.stdout),
        String::from_utf8_lossy(&stale.stderr)
    );

    git(temp.path(), ["clone", &remote_url, path_str(&final_clone)?])?;
    assert_eq!(
        std::fs::read_to_string(final_clone.join("README.md"))?,
        "winner\n"
    );

    Ok(())
}

fn git<I, S>(cwd: &Path, args: I) -> Result<(), Box<dyn Error>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect::<Vec<_>>();
    let output = git_output(cwd, &args)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "git {args:?} failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
    }
}

fn git_result<I, S>(cwd: &Path, args: I) -> Result<std::process::Output, Box<dyn Error>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect::<Vec<_>>();
    git_output(cwd, &args)
}

fn git_output(cwd: &Path, args: &[String]) -> Result<std::process::Output, Box<dyn Error>> {
    let mut child = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if child.try_wait()?.is_some() {
            return Ok(child.wait_with_output()?);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child.wait_with_output()?;
            return Err(format!(
                "git {args:?} timed out\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn git_stdout<I, S>(cwd: &Path, args: I) -> Result<String, Box<dyn Error>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect::<Vec<_>>();
    let output = git_output(cwd, &args)?;
    if output.status.success() {
        Ok(String::from_utf8(output.stdout)?)
    } else {
        Err(format!(
            "git {args:?} failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
    }
}

fn create_client_with_initial_commit(
    cwd: &Path,
    client: &Path,
    remote: impl AsRef<str>,
) -> Result<(), Box<dyn Error>> {
    git(cwd, ["init", path_str(client)?])?;
    git(client, ["config", "user.email", "agent@example.com"])?;
    git(client, ["config", "user.name", "Agent"])?;
    std::fs::write(client.join("README.md"), "hello from origin\n")?;
    git(client, ["add", "README.md"])?;
    git(client, ["commit", "-m", "initial"])?;
    git(client, ["branch", "-M", "main"])?;
    git(client, ["remote", "add", "origin", remote.as_ref()])?;
    Ok(())
}

fn write_commit_push(
    repo: &Path,
    path: &str,
    contents: &str,
    message: &str,
) -> Result<(), Box<dyn Error>> {
    write_commit(repo, path, contents, message)?;
    git(repo, ["push", "origin", "main"])?;
    Ok(())
}

fn write_commit(
    repo: &Path,
    path: &str,
    contents: &str,
    message: &str,
) -> Result<(), Box<dyn Error>> {
    std::fs::write(repo.join(path), contents)?;
    git(repo, ["add", path])?;
    git(repo, ["commit", "-m", message])?;
    Ok(())
}

fn path_str(path: &Path) -> Result<&str, Box<dyn Error>> {
    path.to_str()
        .ok_or_else(|| format!("path is not UTF-8: {}", path.display()).into())
}
