mod support;

use origin::{OriginRepository, RepositoryScope};
use std::{error::Error, sync::Arc, time::Instant};
use substrate::{InMemoryBackend, ScopedStorage};
use support::{create_client_with_initial_commit, git, path_str, write_commit};

#[tokio::test]
#[ignore = "benchmark profile; run with --ignored --nocapture"]
async fn incremental_publication_scaling_profile() -> Result<(), Box<dyn Error>> {
    let base_commits = env_usize("ORIGIN_BENCH_BASE_COMMITS", 50);
    let incremental_commits = env_usize("ORIGIN_BENCH_INCREMENTAL_COMMITS", 10);
    let temp = tempfile::tempdir()?;
    let bare = temp.path().join("origin.git");
    let restored = temp.path().join("restored.git");
    let clone = temp.path().join("clone");
    let client = temp.path().join("client");
    let backend = Arc::new(InMemoryBackend::new());
    let repo = OriginRepository::new(Arc::new(ScopedStorage::new(
        RepositoryScope::new("tenant", "bench", "edek").storage_scope(),
        backend,
    )));

    let materialized = repo.materialize_bare_repository(&bare).await?;
    create_client_with_initial_commit(temp.path(), &client, path_str(&bare)?)?;
    for index in 0..base_commits {
        write_commit(
            &client,
            &format!("base-{index}.txt"),
            &format!("base {index}\n"),
            &format!("base {index}"),
        )?;
    }
    git(&client, ["push", "-u", "origin", "main"])?;

    let initial_started = Instant::now();
    let materialized = repo
        .publish_materialized_bare_repository(&bare, materialized)
        .await?;
    let initial_elapsed = initial_started.elapsed();
    let initial = repo.current_publication().await?.unwrap();

    for index in 0..incremental_commits {
        write_commit(
            &client,
            &format!("incremental-{index}.txt"),
            &format!("incremental {index}\n"),
            &format!("incremental {index}"),
        )?;
        git(&client, ["push", "origin", "main"])?;
    }

    let incremental_started = Instant::now();
    repo.publish_materialized_bare_repository(&bare, materialized)
        .await?;
    let incremental_elapsed = incremental_started.elapsed();
    let incremental = repo.current_publication().await?.unwrap();

    let range_read_started = Instant::now();
    let mut range_reads = 0usize;
    for object in incremental.objects.iter().take(25) {
        let bytes = repo.read_git_object_pack_range(&object.oid).await?;
        assert_eq!(bytes.len(), object.location.packed_size as usize);
        range_reads += 1;
    }
    let range_read_elapsed = range_read_started.elapsed();

    let materialize_started = Instant::now();
    repo.materialize_bare_repository(&restored).await?;
    let materialize_elapsed = materialize_started.elapsed();
    git(
        temp.path(),
        ["clone", path_str(&restored)?, path_str(&clone)?],
    )?;

    println!(
        "origin_incremental_publication_benchmark \
         base_commits={base_commits} incremental_commits={incremental_commits} \
         initial_objects={} incremental_objects={} new_objects={} \
         initial_packs={} incremental_packs={} new_packs={} \
         initial_publish_ms={} incremental_publish_ms={} \
         range_reads={} range_read_ms={} compatibility_materialize_ms={}",
        initial.objects.len(),
        incremental.objects.len(),
        incremental
            .objects
            .len()
            .saturating_sub(initial.objects.len()),
        initial.packs.len(),
        incremental.packs.len(),
        incremental.packs.len().saturating_sub(initial.packs.len()),
        initial_elapsed.as_millis(),
        incremental_elapsed.as_millis(),
        range_reads,
        range_read_elapsed.as_millis(),
        materialize_elapsed.as_millis(),
    );

    assert_eq!(initial.packs.len(), 1);
    assert_eq!(incremental.packs.len(), 2);
    assert!(incremental.objects.len() > initial.objects.len());
    Ok(())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
