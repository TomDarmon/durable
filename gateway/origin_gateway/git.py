from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qs

from .config import OperationClass
from .errors import GitClientError


@dataclass(frozen=True)
class GitOperation:
    operation_class: OperationClass
    is_mutating: bool


def classify_git_operation(method: str, git_path: str, query_string: str) -> GitOperation:
    query = parse_qs(query_string, keep_blank_values=True)
    service = query.get("service", [""])[0]
    if git_path.endswith("git-receive-pack") or service == "git-receive-pack":
        return GitOperation(OperationClass.GIT_RECEIVE_PACK, True)
    if git_path.endswith("git-upload-pack") or service == "git-upload-pack":
        return GitOperation(OperationClass.GIT_UPLOAD_PACK, False)
    if method.upper() == "GET":
        return GitOperation(OperationClass.GIT_METADATA, False)
    raise GitClientError(
        "unsupported_git_operation",
        "unsupported git smart HTTP operation",
        400,
    )
