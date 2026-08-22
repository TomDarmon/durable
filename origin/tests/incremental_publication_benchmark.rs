mod support;

use origin::{local_rustfs_repository, OriginRepositoryMetricsSnapshot, RepositoryScope};
use std::{error::Error, path::Path, time::Duration, time::Instant};
use support::{create_client_with_initial_commit, git, git_stdout, path_str, write_commit};

#[tokio::test]
#[ignore = "benchmark profile; run with --ignored --nocapture"]
async fn wal_engine_scaling_profile() -> Result<(), Box<dyn Error>> {
    let base_commits = env_usize("ORIGIN_BENCH_BASE_COMMITS", 50);
    let small_pushes = env_usize("ORIGIN_BENCH_INCREMENTAL_COMMITS", 10);
    let clone_fetches = env_usize("ORIGIN_BENCH_CLONE_FETCHES", 6);
    let temp = tempfile::tempdir()?;
    let primary_bare = temp.path().join("primary.git");
    let secondary_bare = temp.path().join("secondary.git");
    let restored_bare = temp.path().join("restored.git");
    let writer = temp.path().join("writer");
    let scope = RepositoryScope::new("tenant", format!("bench-{}", uuid::Uuid::new_v4()), "edek");
    let primary = local_rustfs_repository(scope.clone()).await?;
    let secondary = local_rustfs_repository(scope).await?;
    let mut push_latencies = Vec::new();
    let mut fetch_clone_latencies = Vec::new();
    let mut git_pushes = 0usize;

    let mut primary_materialized = primary.materialize_bare_repository(&primary_bare).await?;
    create_client_with_initial_commit(temp.path(), &writer, path_str(&primary_bare)?)?;
    for index in 0..base_commits {
        write_commit(
            &writer,
            &format!("base-{index}.txt"),
            &format!("base {index}\n"),
            &format!("base {index}"),
        )?;
    }
    git(&writer, ["push", "-u", "origin", "main"])?;
    git_pushes += 1;
    primary_materialized = timed(&mut push_latencies, || async {
        primary
            .publish_materialized_bare_repository(&primary_bare, primary_materialized)
            .await
    })
    .await?;

    for index in 0..small_pushes {
        write_commit(
            &writer,
            &format!("incremental-{index}.txt"),
            &format!("incremental {index}\n"),
            &format!("incremental {index}"),
        )?;
        git(&writer, ["push", "origin", "main"])?;
        git_pushes += 1;
        primary_materialized = timed(&mut push_latencies, || async {
            primary
                .publish_materialized_bare_repository(&primary_bare, primary_materialized.clone())
                .await
        })
        .await?;
    }

    let secondary_materialized = secondary
        .materialize_bare_repository(&secondary_bare)
        .await?;
    let first_conflict = temp.path().join("first-conflict");
    let second_conflict = temp.path().join("second-conflict");
    clone_local(&primary_bare, &first_conflict)?;
    clone_local(&secondary_bare, &second_conflict)?;
    git(&first_conflict, ["config", "commit.gpgSign", "false"])?;
    git(&second_conflict, ["config", "commit.gpgSign", "false"])?;
    git(
        &first_conflict,
        ["remote", "set-url", "origin", path_str(&primary_bare)?],
    )?;
    git(
        &second_conflict,
        ["remote", "set-url", "origin", path_str(&secondary_bare)?],
    )?;
    write_commit(
        &first_conflict,
        "conflict.txt",
        "primary\n",
        "primary conflict",
    )?;
    write_commit(
        &second_conflict,
        "conflict.txt",
        "secondary\n",
        "secondary conflict",
    )?;
    git(&first_conflict, ["push", "origin", "main"])?;
    git(&second_conflict, ["push", "origin", "main"])?;
    git_pushes += 2;

    let first_started = Instant::now();
    let second_started = Instant::now();
    let (first_publish, second_publish) = tokio::join!(
        primary.publish_materialized_bare_repository(&primary_bare, primary_materialized),
        secondary.publish_materialized_bare_repository(&secondary_bare, secondary_materialized)
    );
    push_latencies.push(first_started.elapsed());
    push_latencies.push(second_started.elapsed());
    let concurrent_conflicts = [first_publish.is_err(), second_publish.is_err()]
        .into_iter()
        .filter(|failed| *failed)
        .count();

    for index in 0..clone_fetches {
        let clone = temp.path().join(format!("clone-{index}"));
        let started = Instant::now();
        secondary
            .materialize_bare_repository_cached(&secondary_bare)
            .await?;
        clone_local(&secondary_bare, &clone)?;
        git(&clone, ["fetch", "origin", "main"])?;
        fetch_clone_latencies.push(started.elapsed());
    }

    let restore_started = Instant::now();
    if restored_bare.exists() {
        std::fs::remove_dir_all(&restored_bare)?;
    }
    primary.materialize_bare_repository(&restored_bare).await?;
    let restore_elapsed = restore_started.elapsed();

    let compaction_started = Instant::now();
    primary.compact_wal().await?;
    let compaction_elapsed = compaction_started.elapsed();

    let publication = primary.current_publication().await?.unwrap();
    let wal_index = primary.current_wal_index().await?.unwrap();
    let wal_index_size = serde_json::to_vec(&wal_index)?.len();
    let metrics = sum_metrics(primary.metrics(), secondary.metrics());

    println!(
        "origin_wal_benchmark \
         commits={} pushes={} wal_entries={} wal_index_size_bytes={} \
         push_latency_p50_ms={} push_latency_p95_ms={} push_latency_p99_ms={} \
         fetch_clone_latency_p50_ms={} fetch_clone_latency_p95_ms={} fetch_clone_latency_p99_ms={} \
         restore_from_wal_ms={} local_materializations={} \
         bytes_written_to_durable_storage={} bytes_read_from_durable_storage={} \
         cas_retries={} cas_conflicts={} compaction_time_ms={} \
         cache_hits={} cache_misses={}",
        base_commits + small_pushes + 3,
        git_pushes,
        wal_index.entries.len(),
        wal_index_size,
        percentile_ms(&push_latencies, 50),
        percentile_ms(&push_latencies, 95),
        percentile_ms(&push_latencies, 99),
        percentile_ms(&fetch_clone_latencies, 50),
        percentile_ms(&fetch_clone_latencies, 95),
        percentile_ms(&fetch_clone_latencies, 99),
        restore_elapsed.as_millis(),
        metrics.local_materializations,
        metrics.durable_bytes_written,
        metrics.durable_bytes_read,
        metrics.cas_retries,
        metrics.cas_conflicts,
        compaction_elapsed.as_millis(),
        metrics.cache_hits,
        metrics.cache_misses,
    );

    assert!(publication.objects.len() > base_commits);
    assert!(wal_index.entries.len() >= small_pushes + 3);
    assert_eq!(concurrent_conflicts, 1);
    assert!(
        git_stdout(&restored_bare, ["rev-list", "--count", "HEAD"])?
            .trim()
            .parse::<usize>()?
            >= base_commits
    );
    Ok(())
}

async fn timed<T, Fut>(
    timings: &mut Vec<Duration>,
    operation: impl FnOnce() -> Fut,
) -> Result<T, Box<dyn Error>>
where
    Fut: std::future::Future<Output = origin::Result<T>>,
{
    let started = Instant::now();
    let result = operation().await?;
    timings.push(started.elapsed());
    Ok(result)
}

fn clone_local(remote: &Path, target: &Path) -> Result<(), Box<dyn Error>> {
    git(
        target.parent().ok_or("clone target has no parent")?,
        ["clone", path_str(remote)?, path_str(target)?],
    )
}

fn percentile_ms(values: &[Duration], percentile: usize) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let mut millis = values.iter().map(Duration::as_millis).collect::<Vec<_>>();
    millis.sort_unstable();
    let rank = ((millis.len() - 1) * percentile).div_ceil(100);
    millis[rank]
}

fn sum_metrics(
    left: OriginRepositoryMetricsSnapshot,
    right: OriginRepositoryMetricsSnapshot,
) -> OriginRepositoryMetricsSnapshot {
    OriginRepositoryMetricsSnapshot {
        durable_bytes_written: left.durable_bytes_written + right.durable_bytes_written,
        durable_bytes_read: left.durable_bytes_read + right.durable_bytes_read,
        local_materializations: left.local_materializations + right.local_materializations,
        cache_hits: left.cache_hits + right.cache_hits,
        cache_misses: left.cache_misses + right.cache_misses,
        cas_conflicts: left.cas_conflicts + right.cas_conflicts,
        cas_retries: left.cas_retries + right.cas_retries,
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
