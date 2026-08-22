from __future__ import annotations

import pytest

from origin_gateway.errors import GatewayError
from origin_gateway.validation import parse_git_route, validate_repo, validate_tenant


def test_parse_git_route_extracts_repo_and_path() -> None:
    assert parse_git_route("tenant", "repo.git/info/refs") == (
        "tenant",
        "repo",
        "info/refs",
    )


def test_parse_git_route_keeps_dot_git_inside_repo_name() -> None:
    assert parse_git_route("tenant", "repo.git.backup.git/info/refs") == (
        "tenant",
        "repo.git.backup",
        "info/refs",
    )


@pytest.mark.parametrize("value", ["../tenant", "tenant/name", "", ".", "..", "-tenant"])
def test_validate_tenant_rejects_malformed_segments(value: str) -> None:
    with pytest.raises(GatewayError):
        validate_tenant(value)


@pytest.mark.parametrize("value", ["../repo", "repo/name", "", ".", "..", "repo/", "-repo"])
def test_validate_repo_rejects_traversal_and_nested_paths(value: str) -> None:
    with pytest.raises(GatewayError):
        validate_repo(value)


def test_validate_repo_accepts_origin_safe_names_with_spaces() -> None:
    assert validate_repo("repo name") == "repo name"


def test_parse_git_route_requires_git_suffix() -> None:
    with pytest.raises(GatewayError):
        parse_git_route("tenant", "repo/info/refs")


@pytest.mark.parametrize("value", ["repo.git", "repo.gitfoo/info/refs", ".git/info/refs"])
def test_parse_git_route_requires_git_suffix_boundary_and_subpath(value: str) -> None:
    with pytest.raises(GatewayError):
        parse_git_route("tenant", value)
