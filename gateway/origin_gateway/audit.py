from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("origin_gateway.audit")

SECRET_FIELDS = {"authorization", "x-origin-gateway-token", "cookie", "set-cookie"}


def audit(event: str, **fields: Any) -> None:
    safe_fields = {
        key: ("[redacted]" if key.lower() in SECRET_FIELDS else value)
        for key, value in fields.items()
    }
    logger.info(event, extra={"event": event, **safe_fields})
