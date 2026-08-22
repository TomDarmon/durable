from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest

TOKEN = "local-dev-token-123456"

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="session", autouse=True)
def require_e2e() -> None:
    if os.getenv("GATEWAY_E2E") != "1":
        pytest.skip("set GATEWAY_E2E=1 to run gateway e2e tests")


@pytest.fixture
def gateway_url() -> Iterator[str]:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
        "ORIGIN_GATEWAY_ENVIRONMENT": "test",
        "ORIGIN_GATEWAY_BIND_HOST": "127.0.0.1",
        "ORIGIN_GATEWAY_BIND_PORT": str(port),
        "ORIGIN_GATEWAY_ORIGIN_BACKENDS": ('["http://127.0.0.1:9200","http://127.0.0.1:9202"]'),
        "ORIGIN_GATEWAY_CONTROL_BACKEND_URL": "http://127.0.0.1:9210",
        "ORIGIN_GATEWAY_DEV_AUTH_TOKEN": TOKEN,
        "ORIGIN_GATEWAY_DEV_AUTH_TENANTS": '["tenant"]',
        "ORIGIN_GATEWAY_REQUEST_BODY_LIMIT_BYTES": str(64 * 1024 * 1024),
        "ORIGIN_GATEWAY_RATE_LIMITS": (
            '{"git_receive_pack":{"capacity":20,"refill_per_second":1},'
            '"git_upload_pack":{"capacity":100,"refill_per_second":10},'
            '"git_metadata":{"capacity":100,"refill_per_second":10},'
            '"admin":{"capacity":100,"refill_per_second":10}}'
        ),
    }
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "origin_gateway.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base_url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                alive = httpx.get(f"{base_url}/healthz", timeout=1.0).status_code == 204
                ready = httpx.get(f"{base_url}/readyz", timeout=1.0).status_code == 200
                if alive and ready:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
        else:
            output = process.stdout.read() if process.stdout else ""
            raise RuntimeError(f"gateway did not start:\n{output}")
        yield base_url
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def test_authenticated_push_clone_fetch_and_metadata(gateway_url: str, tmp_path: Path) -> None:
    repo_name = f"repo-{uuid.uuid4()}"
    remote = authed_remote(gateway_url, "tenant", repo_name)
    source = tmp_path / "source"
    clone = tmp_path / "clone"

    git(tmp_path, "init", "-b", "main", str(source))
    configure_git_identity(source)
    (source / "README.md").write_text("hello through gateway\n")
    git(source, "add", "README.md")
    git(source, "commit", "-m", "initial")
    git(source, "remote", "add", "origin", remote)
    git(source, "push", "origin", "main")

    git(tmp_path, "clone", remote, str(clone))
    assert (clone / "README.md").read_text() == "hello through gateway\n"

    (source / "README.md").write_text("second version\n")
    git(source, "commit", "-am", "second")
    git(source, "push", "origin", "main")
    git(clone, "fetch", "origin")
    assert (
        git_output(clone, "rev-parse", "origin/main").strip()
        == git_output(source, "rev-parse", "main").strip()
    )

    response = httpx.get(
        f"{gateway_url}/admin/repos",
        headers={"Authorization": f"Bearer {TOKEN}"},
        timeout=10,
    )
    response.raise_for_status()
    assert any(
        item["tenant"] == "tenant" and item["name"] == repo_name
        for item in response.json()["repositories"]
    )


def test_unauthorized_and_wrong_tenant_requests_are_rejected(
    gateway_url: str, tmp_path: Path
) -> None:
    repo_name = f"repo-{uuid.uuid4()}"

    unauthorized = run_git(
        tmp_path,
        "ls-remote",
        f"{gateway_url}/tenant/{repo_name}.git",
        check=False,
    )
    assert unauthorized.returncode != 0

    wrong_tenant = run_git(
        tmp_path,
        "ls-remote",
        authed_remote(gateway_url, "other-tenant", repo_name),
        check=False,
    )
    assert wrong_tenant.returncode != 0


def test_large_push_passes_within_configured_limit(gateway_url: str, tmp_path: Path) -> None:
    repo_name = f"repo-{uuid.uuid4()}"
    source = tmp_path / "large-source"

    git(tmp_path, "init", "-b", "main", str(source))
    configure_git_identity(source)
    (source / "blob.bin").write_bytes(b"x" * (2 * 1024 * 1024))
    git(source, "add", "blob.bin")
    git(source, "commit", "-m", "large")
    git(source, "remote", "add", "origin", authed_remote(gateway_url, "tenant", repo_name))
    git(source, "push", "origin", "main")


def test_two_origin_backends_are_ready_behind_gateway(gateway_url: str) -> None:
    response = httpx.get(f"{gateway_url}/readyz", timeout=10)
    response.raise_for_status()
    body = response.json()
    assert body["ready"] is True
    assert {item["url"] for item in body["backends"]} == {
        "http://127.0.0.1:9200",
        "http://127.0.0.1:9202",
    }


def test_rate_limited_push_is_rejected_compatibly(tmp_path: Path) -> None:
    with limited_gateway_url() as base_url:
        repo_name = f"repo-{uuid.uuid4()}"
        source = tmp_path / "limited-source"
        git(tmp_path, "init", "-b", "main", str(source))
        configure_git_identity(source)
        (source / "README.md").write_text("one\n")
        git(source, "add", "README.md")
        git(source, "commit", "-m", "one")
        git(source, "remote", "add", "origin", authed_remote(base_url, "tenant", repo_name))
        git(source, "push", "origin", "main")

        (source / "README.md").write_text("two\n")
        git(source, "commit", "-am", "two")
        rejected = run_git(source, "push", "origin", "main", check=False)

    assert rejected.returncode != 0
    assert "rate limit exceeded" in rejected.stderr.lower()


@contextmanager
def limited_gateway_url() -> Iterator[str]:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
        "ORIGIN_GATEWAY_ORIGIN_BACKENDS": ('["http://127.0.0.1:9200","http://127.0.0.1:9202"]'),
        "ORIGIN_GATEWAY_CONTROL_BACKEND_URL": "http://127.0.0.1:9210",
        "ORIGIN_GATEWAY_DEV_AUTH_TOKEN": TOKEN,
        "ORIGIN_GATEWAY_DEV_AUTH_TENANTS": '["tenant"]',
        "ORIGIN_GATEWAY_RATE_LIMITS": (
            '{"git_receive_pack":{"capacity":2,"refill_per_second":0.001},'
            '"git_upload_pack":{"capacity":100,"refill_per_second":10},'
            '"git_metadata":{"capacity":100,"refill_per_second":10},'
            '"admin":{"capacity":100,"refill_per_second":10}}'
        ),
    }
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "origin_gateway.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base_url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                alive = httpx.get(f"{base_url}/healthz", timeout=1.0).status_code == 204
                ready = httpx.get(f"{base_url}/readyz", timeout=1.0).status_code == 200
                if alive and ready:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
        else:
            output = process.stdout.read() if process.stdout else ""
            raise RuntimeError(f"gateway did not start:\n{output}")
        yield base_url
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def authed_remote(base_url: str, tenant: str, repo_name: str) -> str:
    stripped = base_url.removeprefix("http://")
    return f"http://local-dev:{TOKEN}@{stripped}/{tenant}/{repo_name}.git"


def configure_git_identity(repo: Path) -> None:
    git(repo, "config", "user.email", "gateway@example.com")
    git(repo, "config", "user.name", "Gateway E2E")
    git(repo, "config", "commit.gpgSign", "false")


def git(cwd: Path, *args: str) -> None:
    run_git(cwd, *args)


def git_output(cwd: Path, *args: str) -> str:
    return run_git(cwd, *args).stdout


def run_git(cwd: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"git {' '.join(args)} failed with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result
