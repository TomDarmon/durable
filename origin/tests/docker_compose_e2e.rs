mod support;

use std::{error::Error, thread};
use support::{
    assert_container_cache_marker, create_client_with_initial_commit, docker_compose, git,
    git_output, git_result, git_stdout, http_status, path_str, wait_for_remote, write_commit,
    write_commit_push, DOCKER_COMPOSE_ORIGIN_LOCK,
};

#[tokio::test]
#[ignore = "requires `make e2e-up`"]
async fn docker_compose_origin_services_report_health() -> Result<(), Box<dyn Error>> {
    let _docker_origin = DOCKER_COMPOSE_ORIGIN_LOCK.lock().await;

    assert_eq!(http_status("127.0.0.1:9200", "/healthz")?, 204);
    assert_eq!(http_status("127.0.0.1:9202", "/healthz")?, 204);

    Ok(())
}

#[tokio::test]
#[ignore = "requires `make e2e-up`"]
async fn docker_compose_origin_browser_serves_react_app() -> Result<(), Box<dyn Error>> {
    let _docker_origin = DOCKER_COMPOSE_ORIGIN_LOCK.lock().await;

    assert_eq!(http_status("127.0.0.1:9300", "/")?, 200);

    Ok(())
}

#[tokio::test]
#[ignore = "requires `make e2e-up`"]
async fn docker_compose_origin_service_accepts_push_and_serves_clone() -> Result<(), Box<dyn Error>>
{
    let _docker_origin = DOCKER_COMPOSE_ORIGIN_LOCK.lock().await;
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
    assert_container_cache_marker("origin", &repo_name)?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires `make e2e-up`"]
async fn docker_compose_origin_service_recovers_after_restart() -> Result<(), Box<dyn Error>> {
    let _docker_origin = DOCKER_COMPOSE_ORIGIN_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let client = temp.path().join("client");
    let clone = temp.path().join("clone");
    let repo_name = format!("repo-{}", uuid::Uuid::new_v4());
    let remote_url = format!("http://127.0.0.1:9200/tenant/{repo_name}.git");

    create_client_with_initial_commit(temp.path(), &client, &remote_url)?;
    git(&client, ["push", "-u", "origin", "main"])?;

    docker_compose(["restart", "origin"])?;
    wait_for_remote(&remote_url)?;
    git(temp.path(), ["clone", &remote_url, path_str(&clone)?])?;

    assert_eq!(
        std::fs::read_to_string(clone.join("README.md"))?,
        "hello from origin\n"
    );
    assert_container_cache_marker("origin", &repo_name)?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires `make e2e-up`"]
async fn docker_compose_origin_service_rejects_stale_push() -> Result<(), Box<dyn Error>> {
    let _docker_origin = DOCKER_COMPOSE_ORIGIN_LOCK.lock().await;
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

#[tokio::test]
#[ignore = "requires `make e2e-up`"]
async fn docker_compose_origin_services_linearize_conflicting_pushes() -> Result<(), Box<dyn Error>>
{
    let _docker_origin = DOCKER_COMPOSE_ORIGIN_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let primary = temp.path().join("primary");
    let alternate = temp.path().join("alternate");
    let primary_clone = temp.path().join("primary-clone");
    let alternate_clone = temp.path().join("alternate-clone");
    let repo_name = format!("repo-{}", uuid::Uuid::new_v4());
    let primary_url = format!("http://127.0.0.1:9200/tenant/{repo_name}.git");
    let alternate_url = format!("http://127.0.0.1:9202/tenant/{repo_name}.git");

    wait_for_remote(&primary_url)?;
    wait_for_remote(&alternate_url)?;

    create_client_with_initial_commit(temp.path(), &primary, &primary_url)?;
    git(&primary, ["push", "-u", "origin", "main"])?;
    git(temp.path(), ["clone", &primary_url, path_str(&alternate)?])?;
    git(&alternate, ["config", "user.email", "agent@example.com"])?;
    git(&alternate, ["config", "user.name", "Agent"])?;
    git(&alternate, ["remote", "set-url", "origin", &alternate_url])?;

    write_commit(&primary, "README.md", "primary winner\n", "primary update")?;
    write_commit(
        &alternate,
        "README.md",
        "alternate winner\n",
        "alternate update",
    )?;

    let primary_push = {
        let repo = primary.clone();
        thread::spawn(move || {
            git_output(&repo, &["push".into(), "origin".into(), "main".into()])
                .map_err(|error| error.to_string())
        })
    };
    let alternate_push = {
        let repo = alternate.clone();
        thread::spawn(move || {
            git_output(&repo, &["push".into(), "origin".into(), "main".into()])
                .map_err(|error| error.to_string())
        })
    };
    let primary_output = primary_push
        .join()
        .map_err(|_| "primary push thread panicked")?
        .map_err(|error| format!("primary push failed to run: {error}"))?;
    let alternate_output = alternate_push
        .join()
        .map_err(|_| "alternate push thread panicked")?
        .map_err(|error| format!("alternate push failed to run: {error}"))?;
    let primary_succeeded = primary_output.status.success();
    let alternate_succeeded = alternate_output.status.success();

    assert_ne!(
        primary_succeeded, alternate_succeeded,
        "exactly one conflicting push should succeed\nprimary stdout:\n{}\nprimary stderr:\n{}\nalternate stdout:\n{}\nalternate stderr:\n{}",
        String::from_utf8_lossy(&primary_output.stdout),
        String::from_utf8_lossy(&primary_output.stderr),
        String::from_utf8_lossy(&alternate_output.stdout),
        String::from_utf8_lossy(&alternate_output.stderr)
    );
    let losing_output = if primary_succeeded {
        &alternate_output
    } else {
        &primary_output
    };
    let losing_stderr = String::from_utf8_lossy(&losing_output.stderr);
    assert!(
        losing_stderr.contains("409") || losing_stderr.contains("Conflict"),
        "losing push should surface an HTTP conflict\nstderr:\n{losing_stderr}"
    );

    let expected_contents = if primary_succeeded {
        "primary winner\n"
    } else {
        "alternate winner\n"
    };

    git(
        temp.path(),
        ["clone", &primary_url, path_str(&primary_clone)?],
    )?;
    git(
        temp.path(),
        ["clone", &alternate_url, path_str(&alternate_clone)?],
    )?;
    assert_eq!(
        std::fs::read_to_string(primary_clone.join("README.md"))?,
        expected_contents
    );
    assert_eq!(
        std::fs::read_to_string(alternate_clone.join("README.md"))?,
        expected_contents
    );
    assert_eq!(
        git_stdout(&primary_clone, ["rev-list", "--count", "HEAD"])?.trim(),
        "2"
    );
    assert_eq!(
        git_stdout(&alternate_clone, ["rev-list", "--count", "HEAD"])?.trim(),
        "2"
    );
    assert_container_cache_marker("origin", &repo_name)?;
    assert_container_cache_marker("origin-alt", &repo_name)?;

    Ok(())
}
