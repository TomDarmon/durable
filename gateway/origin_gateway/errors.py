from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GatewayError(Exception):
    code: str
    message: str
    status_code: int = 400
    headers: dict[str, str] | None = None

    def json_body(self, request_id: str | None = None) -> dict[str, Any]:
        error: dict[str, Any] = {"code": self.code, "message": self.message}
        if request_id is not None:
            error["request_id"] = request_id
        return {"error": error}


class GitClientError(GatewayError):
    """Error shape for Git smart HTTP clients.

    Git clients do not expect the gateway's JSON API error envelope. These
    errors are rendered as terse text/plain responses and keep auth challenges
    compatible with command-line Git.
    """
