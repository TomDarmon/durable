from __future__ import annotations

from .errors import GatewayError, GitClientError

_FORBIDDEN = {"", ".", ".."}


def validate_tenant(value: str) -> str:
    if not _is_safe_path_segment(value, max_length=128):
        raise GatewayError("invalid_tenant", "tenant is malformed", 404)
    return value


def validate_repo(value: str) -> str:
    if not _is_safe_path_segment(value, max_length=192) or value.endswith(".git"):
        raise GatewayError("invalid_repository", "repository is malformed", 404)
    return value


def validate_git_subpath(value: str) -> str:
    if value.startswith("/") or "\\" in value:
        raise GitClientError("invalid_git_path", "git path is malformed", 404)
    if any(part in _FORBIDDEN for part in value.split("/")):
        raise GitClientError("invalid_git_path", "git path is malformed", 404)
    return value


def parse_git_route(tenant: str, repo_and_path: str) -> tuple[str, str, str]:
    tenant = validate_tenant(tenant)
    marker = ".git/"
    if marker not in repo_and_path:
        if repo_and_path.endswith(".git"):
            raise GitClientError("invalid_git_path", "git smart HTTP path is required", 404)
        raise GitClientError("invalid_repository", "repository path must end in .git", 404)
    repo, remainder = repo_and_path.rsplit(marker, maxsplit=1)
    if not repo or not remainder:
        raise GitClientError("invalid_git_path", "git smart HTTP path is required", 404)
    validate_repo(repo)
    validate_git_subpath(remainder)
    return tenant, repo, remainder


def _is_safe_path_segment(value: str, *, max_length: int) -> bool:
    return (
        0 < len(value) <= max_length
        and value not in _FORBIDDEN
        and not value.startswith("-")
        and "/" not in value
        and "\\" not in value
        and ".." not in value
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
    )
