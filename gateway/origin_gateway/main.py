from __future__ import annotations

import uvicorn

from .app import create_app
from .config import GatewaySettings
from .logging import configure_logging

settings = GatewaySettings()
configure_logging(settings.log_level)
app = create_app(settings)


def main() -> None:
    uvicorn.run(
        "origin_gateway.main:app",
        host=settings.bind_host,
        port=settings.bind_port,
        log_config=None,
    )


if __name__ == "__main__":
    main()
