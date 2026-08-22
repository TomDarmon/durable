use crate::{OriginError, Result};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Component, Path, PathBuf},
    process::Command,
};

use crate::wal::{GitObjectEntry, GitObjectLocation, GitRefEntry};

pub(crate) const CACHE_MARKER_FILE: &str = ".origin-cache-wal";

pub(crate) fn materialize_empty_bare_repo(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    command("git", ["init", "--bare", path_str(path)?])?;
    git(path, ["config", "http.receivepack", "true"])?;
    Ok(())
}

pub(crate) fn cache_marker_matches(bare_repo: &Path, digest: &str) -> bool {
    fs::read_to_string(cache_marker_path(bare_repo))
        .map(|cached| cached.trim() == digest)
        .unwrap_or(false)
}

pub(crate) fn write_cache_marker(bare_repo: &Path, digest: &str) -> Result<()> {
    fs::write(cache_marker_path(bare_repo), format!("{digest}\n"))?;
    Ok(())
}

fn cache_marker_path(bare_repo: &Path) -> PathBuf {
    bare_repo.join(CACHE_MARKER_FILE)
}

pub(crate) fn normalize_relative_path(path: &Path) -> Result<String> {
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

pub(crate) fn safe_join(base: &Path, relative: &str) -> Result<PathBuf> {
    let path = Path::new(relative);
    if path.is_absolute() {
        return Err(OriginError::UnsafePath(relative.into()));
    }
    let normalized = normalize_relative_path(path)?;
    Ok(base.join(normalized))
}

pub(crate) fn validate_path_segment(value: &str) -> Result<()> {
    if value.is_empty()
        || value.starts_with('-')
        || value.contains('/')
        || value.contains('\\')
        || value.contains("..")
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(OriginError::UnsafePath(value.into()));
    }
    Ok(())
}

pub(crate) fn validate_repository_name(value: &str) -> Result<()> {
    validate_path_segment(value)?;
    if value.ends_with(".git") {
        return Err(OriginError::UnsafePath(value.into()));
    }
    Ok(())
}

pub(crate) fn capture_git_refs(repo: &Path) -> Result<Vec<GitRefEntry>> {
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

pub(crate) fn capture_git_objects(repo: &Path) -> Result<Vec<GitObjectEntry>> {
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
            location: GitObjectLocation::default(),
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

pub(crate) fn pack_index_locations(idx_path: &Path) -> Result<BTreeMap<String, (u64, u64)>> {
    let output = String::from_utf8_lossy(&command_output(
        "git",
        ["verify-pack", "-v", path_str(idx_path)?],
    )?)
    .into_owned();
    let mut locations = BTreeMap::new();
    for line in output.lines() {
        let mut fields = line.split_whitespace();
        let Some(oid) = fields.next() else {
            continue;
        };
        if !is_git_oid(oid) {
            continue;
        }
        let Some(kind) = fields.next() else {
            continue;
        };
        if !is_git_object_kind(kind) {
            continue;
        }
        let _uncompressed_size = fields
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid verify-pack line: {line}")))?;
        let packed_size = fields
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid verify-pack line: {line}")))?
            .parse()
            .map_err(|error| OriginError::Http(format!("invalid packed object size: {error}")))?;
        let pack_offset = fields
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid verify-pack line: {line}")))?
            .parse()
            .map_err(|error| OriginError::Http(format!("invalid packed object offset: {error}")))?;
        locations.insert(oid.to_string(), (pack_offset, packed_size));
    }
    Ok(locations)
}

pub(crate) fn is_git_oid(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(crate) fn is_git_object_kind(value: &str) -> bool {
    matches!(value, "blob" | "commit" | "tag" | "tree")
}

pub(crate) fn write_loose_refs(repo: &Path, refs: &[GitRefEntry]) -> Result<()> {
    let mut head_target = refs
        .iter()
        .find(|git_ref| git_ref.name == "refs/heads/main")
        .or_else(|| {
            refs.iter()
                .find(|git_ref| git_ref.name.starts_with("refs/heads/"))
        })
        .map(|git_ref| git_ref.name.clone());
    for git_ref in refs {
        let path = safe_join(repo, &git_ref.name)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, format!("{}\n", git_ref.target))?;
        if head_target.is_none() && git_ref.name.starts_with("refs/") {
            head_target = Some(git_ref.name.clone());
        }
    }
    if let Some(target) = head_target {
        git(repo, ["symbolic-ref", "HEAD", &target])?;
    }
    Ok(())
}

pub(crate) fn git<I, S>(repo: &Path, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut all_args = vec!["-C".to_string(), path_str(repo)?.to_string()];
    all_args.extend(args.into_iter().map(|arg| arg.as_ref().to_string()));
    command("git", all_args)
}

pub(crate) fn git_output<I, S>(repo: &Path, args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let output = git_bytes_output(repo, args)?;
    Ok(String::from_utf8_lossy(&output).into_owned())
}

pub(crate) fn git_bytes_output<I, S>(repo: &Path, args: I) -> Result<Vec<u8>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut all_args = vec!["-C".to_string(), path_str(repo)?.to_string()];
    all_args.extend(args.into_iter().map(|arg| arg.as_ref().to_string()));
    command_output("git", all_args)
}

pub(crate) fn command<I, S>(program: &str, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    command_output(program, args).map(|_| ())
}

pub(crate) fn command_output<I, S>(program: &str, args: I) -> Result<Vec<u8>>
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

pub(crate) fn command_output_with_input<I, S>(
    program: &str,
    args: I,
    input: &[u8],
) -> Result<Vec<u8>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect::<Vec<_>>();
    let output = Command::new(program)
        .args(&args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            if let Some(mut stdin) = child.stdin.take() {
                stdin.write_all(input)?;
            }
            child.wait_with_output()
        })?;
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

pub(crate) fn path_str(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| OriginError::UnsafePath(path.display().to_string()))
}
