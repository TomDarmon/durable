#![allow(dead_code)]

use std::{
    error::Error,
    io::{Read, Write},
    net::TcpStream,
    path::Path,
    process::Command,
    thread,
    time::{Duration, Instant},
};

pub static DOCKER_COMPOSE_ORIGIN_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub fn wait_for_remote(remote_url: &str) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match git_result(Path::new("."), ["ls-remote", remote_url]) {
            Ok(output) if output.status.success() => return Ok(()),
            Ok(output) if Instant::now() >= deadline => {
                return Err(format!(
                    "origin did not become ready\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                )
                .into());
            }
            Err(error) if Instant::now() >= deadline => {
                return Err(format!("origin did not become ready: {error}").into());
            }
            _ => thread::sleep(Duration::from_millis(250)),
        }
    }
}

pub fn docker_compose<I, S>(args: I) -> Result<(), Box<dyn Error>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect::<Vec<_>>();
    let output = Command::new("docker")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .arg("compose")
        .args(&args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()?;

    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "docker compose {args:?} failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
    }
}

pub fn assert_container_cache_marker(service: &str, repo_name: &str) -> Result<(), Box<dyn Error>> {
    let marker = format!("/var/lib/origin/cache/tenant/{repo_name}.git/.origin-cache-publication");
    docker_compose(vec![
        "exec".to_string(),
        "-T".to_string(),
        service.to_string(),
        "test".to_string(),
        "-s".to_string(),
        marker,
    ])
}

pub fn http_status(address: &str, path: &str) -> Result<u16, Box<dyn Error>> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    parse_http_status(&response)
}

fn parse_http_status(response: &str) -> Result<u16, Box<dyn Error>> {
    let status_line = response
        .lines()
        .next()
        .ok_or("HTTP response did not include a status line")?;
    let code = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| format!("HTTP status line had no code: {status_line}"))?
        .parse()?;
    Ok(code)
}

pub fn git<I, S>(cwd: &Path, args: I) -> Result<(), Box<dyn Error>>
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

pub fn git_result<I, S>(cwd: &Path, args: I) -> Result<std::process::Output, Box<dyn Error>>
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

pub fn git_output(cwd: &Path, args: &[String]) -> Result<std::process::Output, Box<dyn Error>> {
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

pub fn git_stdout<I, S>(cwd: &Path, args: I) -> Result<String, Box<dyn Error>>
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

pub fn create_client_with_initial_commit(
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

pub fn write_commit_push(
    repo: &Path,
    path: &str,
    contents: &str,
    message: &str,
) -> Result<(), Box<dyn Error>> {
    write_commit(repo, path, contents, message)?;
    git(repo, ["push", "origin", "main"])?;
    Ok(())
}

pub fn write_commit(
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

pub fn path_str(path: &Path) -> Result<&str, Box<dyn Error>> {
    path.to_str()
        .ok_or_else(|| format!("path is not UTF-8: {}", path.display()).into())
}
