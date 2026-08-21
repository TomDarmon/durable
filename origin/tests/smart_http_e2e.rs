mod support;

use origin::{local_rustfs_config, local_rustfs_repository, serve_http, RepositoryScope};
use serde_json::Value;
use std::error::Error;
use support::{
    create_client_with_initial_commit, git, git_stdout, http_get, http_status, path_str,
    write_commit_push,
};
use tokio::net::TcpListener;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires `make e2e-up`"]
async fn smart_http_health_endpoint_reports_ready() -> Result<(), Box<dyn Error>> {
    let repo_name = format!("repo-{}", uuid::Uuid::new_v4());
    let (server, remote_url) = spawn_http_origin(&repo_name).await?;
    let address = remote_url
        .strip_prefix("http://")
        .and_then(|url| url.split('/').next())
        .ok_or("remote URL did not include an address")?;

    assert_eq!(http_status(address, "/healthz")?, 204);

    server.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires `make e2e-up`"]
async fn smart_http_server_accepts_push_and_serves_clone() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let client = temp.path().join("client");
    let clone = temp.path().join("clone");
    let repo_name = format!("repo-{}", uuid::Uuid::new_v4());
    let (server, remote_url) = spawn_http_origin(&repo_name).await?;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires `make e2e-up`"]
async fn smart_http_api_browses_published_repository() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let client = temp.path().join("client");
    let repo_name = format!("repo-{}", uuid::Uuid::new_v4());
    let (server, remote_url) = spawn_http_origin(&repo_name).await?;
    let address = remote_url
        .strip_prefix("http://")
        .and_then(|url| url.split('/').next())
        .ok_or("remote URL did not include an address")?;

    create_client_with_initial_commit(temp.path(), &client, &remote_url)?;
    git(&client, ["push", "-u", "origin", "main"])?;

    let repos = get_json(address, "/api/repos")?;
    assert!(repos["repositories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|repo| { repo["tenant"] == "tenant" && repo["name"] == repo_name.as_str() }));

    let refs = get_json(address, &format!("/api/repos/tenant/{repo_name}/refs"))?;
    assert!(refs["refs"]
        .as_array()
        .unwrap()
        .iter()
        .any(|git_ref| git_ref["name"] == "refs/heads/main"));

    let tree = get_json(
        address,
        &format!("/api/repos/tenant/{repo_name}/tree?ref=refs/heads/main"),
    )?;
    assert!(tree["entries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| { entry["name"] == "README.md" && entry["kind"] == "blob" }));

    let blob = get_json(
        address,
        &format!("/api/repos/tenant/{repo_name}/blob?ref=refs/heads/main&path=README.md"),
    )?;
    assert_eq!(blob["content"], "hello from origin\n");

    server.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires `make e2e-up`"]
async fn smart_http_fetch_sees_later_push() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let writer = temp.path().join("writer");
    let reader = temp.path().join("reader");
    let repo_name = format!("repo-{}", uuid::Uuid::new_v4());
    let (server, remote_url) = spawn_http_origin(&repo_name).await?;

    create_client_with_initial_commit(temp.path(), &writer, &remote_url)?;
    git(&writer, ["push", "-u", "origin", "main"])?;
    git(temp.path(), ["clone", &remote_url, path_str(&reader)?])?;

    write_commit_push(&writer, "README.md", "fetched update\n", "update")?;
    git(&reader, ["fetch", "origin", "main"])?;
    git(&reader, ["merge", "--ff-only", "origin/main"])?;

    assert_eq!(
        std::fs::read_to_string(reader.join("README.md"))?,
        "fetched update\n"
    );
    assert_eq!(
        git_stdout(&reader, ["rev-list", "--count", "HEAD"])?.trim(),
        "2"
    );

    server.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires `make e2e-up`"]
async fn smart_http_clone_preserves_branches_and_tags() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let client = temp.path().join("client");
    let clone = temp.path().join("clone");
    let repo_name = format!("repo-{}", uuid::Uuid::new_v4());
    let (server, remote_url) = spawn_http_origin(&repo_name).await?;

    create_client_with_initial_commit(temp.path(), &client, &remote_url)?;
    git(&client, ["checkout", "-b", "feature"])?;
    std::fs::write(client.join("feature.txt"), "feature branch\n")?;
    git(&client, ["add", "feature.txt"])?;
    git(&client, ["commit", "-m", "feature"])?;
    git(
        &client,
        ["-c", "tag.gpgSign=false", "tag", "-a", "v1", "-m", "v1"],
    )?;
    git(&client, ["push", "origin", "main", "feature", "v1"])?;

    git(temp.path(), ["clone", &remote_url, path_str(&clone)?])?;

    assert_eq!(
        git_stdout(&clone, ["rev-parse", "--verify", "origin/feature"])?
            .trim()
            .len(),
        40
    );
    assert_eq!(git_stdout(&clone, ["tag", "-l", "v1"])?.trim(), "v1");
    assert_eq!(
        git_stdout(&clone, ["show", "origin/feature:feature.txt"])?,
        "feature branch\n"
    );
    let publication = local_rustfs_repository(RepositoryScope::new("tenant", repo_name, "edek"))
        .await?
        .current_publication()
        .await?
        .ok_or("repository was not published")?;
    assert!(publication
        .refs
        .iter()
        .any(|git_ref| git_ref.name == "refs/heads/main"));
    assert!(publication
        .refs
        .iter()
        .any(|git_ref| git_ref.name == "refs/heads/feature"));
    assert!(publication
        .refs
        .iter()
        .any(|git_ref| git_ref.name == "refs/tags/v1"));
    assert!(publication
        .objects
        .iter()
        .any(|object| object.kind == "tag"));

    server.abort();
    Ok(())
}

async fn spawn_http_origin(
    repo_name: &str,
) -> Result<(tokio::task::JoinHandle<()>, String), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        serve_http(listener, local_rustfs_config())
            .await
            .expect("origin http server failed");
    });
    Ok((server, format!("http://{address}/tenant/{repo_name}.git")))
}

fn get_json(address: &str, path: &str) -> Result<Value, Box<dyn Error>> {
    let response = http_get(address, path)?;
    assert_eq!(response.status, 200, "unexpected body: {}", response.body);
    Ok(serde_json::from_str(&response.body)?)
}
