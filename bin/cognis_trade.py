#!/usr/bin/env python
"""Cognis Trade — CLI entrypoint.

This is the wrapper customers run. It does three things:

1. Prints the Cognis brand banner (no-op when COGNIS_BRANDING is unset).
2. If COGNIS_BRIDGE_URL + COGNIS_TRADE_API_TOKEN are set, boots the
   ``CognisSession`` which polls Cognis Bridge for tenant config and
   reconciles strategies. This is the production path.
3. Otherwise, defers to upstream's bin/hummingbot.py for local-only /
   unmanaged usage (parity with stock Hummingbot — useful for off-network
   smoke tests and for power users running outside the platform).

Zero upstream edits required to install this — it's a new file under bin/.
"""

from __future__ import annotations

import asyncio
import os
import runpy
import signal
import sys
from pathlib import Path

# Ensure the bin/ directory is on sys.path so `import path_util` works the
# same way upstream's bin/hummingbot.py expects it. Mirrors upstream init.
_BIN_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_BIN_DIR))
sys.path.insert(0, str(_BIN_DIR.parent))  # repo root for `hummingbot.*`

import path_util  # noqa: F401, E402 — must come after the sys.path tweak above

from hummingbot.cognis.branding.banner import (  # noqa: E402
    cognis_log_prefix,
    print_cognis_banner,
)
from hummingbot.cognis.branding.cognis_brand import (  # noqa: E402
    branding_enabled,
    load_brand,
)


def _bridge_mode_configured() -> bool:
    """True iff the customer has supplied Bridge credentials in env."""
    return bool(os.environ.get("COGNIS_BRIDGE_URL")) and bool(
        os.environ.get("COGNIS_TRADE_API_TOKEN")
    )


async def _run_bridge_session() -> None:
    """The production path — Bridge-managed bot lifecycle."""
    # Imported lazily so the upstream parity path doesn't pay for httpx
    # / risk-guard imports unless the customer is on the Cognis platform.
    from hummingbot.cognis.session import CognisSession

    session = await CognisSession.bootstrap()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(session.shutdown()))
        except NotImplementedError:
            # Windows: signal handlers via loop.add_signal_handler aren't
            # supported; fall back to default Ctrl+C → KeyboardInterrupt.
            pass

    await session.run_forever()


def _run_upstream_passthrough() -> None:
    """No-Bridge parity path — defer to upstream's bin/hummingbot.py.

    Uses runpy so the upstream entrypoint runs with the same sys.argv +
    sys.path the customer expects. Equivalent to `python bin/hummingbot.py`.
    """
    upstream = _BIN_DIR / "hummingbot.py"
    if not upstream.exists():
        sys.stderr.write(
            f"{cognis_log_prefix()}: bin/hummingbot.py not found at {upstream}; "
            f"cannot defer to upstream CLI.\n"
        )
        raise SystemExit(2)
    runpy.run_path(str(upstream), run_name="__main__")


def main() -> None:
    print_cognis_banner()

    if _bridge_mode_configured():
        brand = load_brand()
        if branding_enabled():
            sys.stdout.write(
                f"\n{cognis_log_prefix()}: connecting to Bridge — "
                f"strategies and risk caps are managed at {brand.portal_url}.\n\n"
            )
        try:
            asyncio.run(_run_bridge_session())
        except KeyboardInterrupt:
            sys.stdout.write(f"\n{cognis_log_prefix()}: shutdown requested.\n")
        return

    if branding_enabled():
        sys.stdout.write(
            f"\n{cognis_log_prefix()}: no Bridge credentials set "
            f"(COGNIS_BRIDGE_URL + COGNIS_TRADE_API_TOKEN). Falling back to "
            f"upstream Hummingbot CLI. Connect via the Cognis portal to manage "
            f"strategies + risk caps centrally.\n\n"
        )
    _run_upstream_passthrough()


if __name__ == "__main__":
    main()
