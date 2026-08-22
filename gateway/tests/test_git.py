from __future__ import annotations

import pytest

from origin_gateway.config import OperationClass
from origin_gateway.errors import GitClientError
from origin_gateway.git import classify_git_operation


def test_classifies_receive_pack_from_rpc_path() -> None:
    operation = classify_git_operation("POST", "git-receive-pack", "")

    assert operation.operation_class == OperationClass.GIT_RECEIVE_PACK
    assert operation.is_mutating


def test_classifies_upload_pack_from_info_refs_query() -> None:
    operation = classify_git_operation("GET", "info/refs", "service=git-upload-pack")

    assert operation.operation_class == OperationClass.GIT_UPLOAD_PACK
    assert not operation.is_mutating


def test_classifies_plain_info_refs_as_metadata() -> None:
    operation = classify_git_operation("GET", "info/refs", "")

    assert operation.operation_class == OperationClass.GIT_METADATA


def test_rejects_unknown_post_operation() -> None:
    with pytest.raises(GitClientError):
        classify_git_operation("POST", "not-a-git-rpc", "")
