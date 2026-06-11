"""Cognis Trade — Binance Spot **Testnet** URL override (no upstream edits).

Hummingbot's upstream binance connector only knows two domains — ``com`` and
``us`` — both reached via the template ``https://api.binance.{domain}/api/``.
Binance's Spot **Testnet** lives at a different host entirely
(``https://testnet.binance.vision``), which the ``{domain}`` placeholder can't
express. Rather than edit the upstream connector (forbidden — Cython/Python
rebase risk, see CLAUDE.md), we patch the connector's module-level URL
*constants* at runtime to the testnet base. Every URL the connector builds —
REST (``REST_URL.format(domain)``), the order-book WS (``WSS_URL``), and the
user-stream WS-API (``WSS_API_URL``) — is derived from these constants, so a
single override redirects the whole connector to testnet.

The override is reversible (``apply_binance_testnet`` returns a restore
callable) and idempotent. It is applied ONLY on the live path when the venue
is binance + testnet is requested; the paper path never imports this module.

Testnet endpoints (per binance.vision / Binance Spot Testnet docs):
  * REST     https://testnet.binance.vision/api/v3/...
  * WS feed  wss://stream.testnet.binance.vision/ws        (market-data streams)
  * WS API   wss://ws-api.testnet.binance.vision/ws-api/v3 (user/account WS-API)

Note the websocket hosts differ from the REST host — testnet mirrors production's
``stream.binance.com`` / ``ws-api.binance.com`` split with ``*.testnet.binance.vision``
subdomains. Pointing the WS at the bare REST host (``testnet.binance.vision/ws``)
404s, so we set the proper stream hosts here.

This is genuinely the testnet — orders placed against it are real connector
orders against Binance's matching engine, with no real money.
"""

from __future__ import annotations

import logging
from typing import Callable

logger = logging.getLogger(__name__)

TESTNET_REST_URL = "https://testnet.binance.vision/api/"
TESTNET_WSS_URL = "wss://stream.testnet.binance.vision/ws"
TESTNET_WSS_API_URL = "wss://ws-api.testnet.binance.vision/ws-api/v3"

# Marker so we don't double-apply / so callers can introspect.
_APPLIED_ATTR = "_cognis_testnet_applied"


def apply_binance_testnet() -> Callable[[], None]:
    """Point the upstream binance connector at Spot Testnet.

    Overrides the module-level URL constants (used by REST web_utils and both
    websocket data sources) so the connector talks to testnet.binance.vision.
    Returns a callable that restores the original production URLs.
    """
    from hummingbot.connector.exchange.binance import binance_constants as C

    if getattr(C, _APPLIED_ATTR, False):
        logger.info("[cognis.live] binance testnet override already applied")
        return lambda: None

    original = {
        "REST_URL": C.REST_URL,
        "WSS_URL": C.WSS_URL,
        "WSS_API_URL": C.WSS_API_URL,
    }

    C.REST_URL = TESTNET_REST_URL
    C.WSS_URL = TESTNET_WSS_URL
    C.WSS_API_URL = TESTNET_WSS_API_URL
    setattr(C, _APPLIED_ATTR, True)

    logger.info(
        "[cognis.live] binance connector redirected to SPOT TESTNET: rest=%s ws=%s",
        TESTNET_REST_URL,
        TESTNET_WSS_URL,
    )

    def _restore() -> None:
        C.REST_URL = original["REST_URL"]
        C.WSS_URL = original["WSS_URL"]
        C.WSS_API_URL = original["WSS_API_URL"]
        setattr(C, _APPLIED_ATTR, False)
        logger.info("[cognis.live] binance connector restored to production URLs")

    return _restore
