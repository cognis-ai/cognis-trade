"""Cognis Trade — CLI banner.

Replaces Hummingbot's stock banner with Cognis Trade copy on startup.
Plainspoken per the Cognis brand voice (periods, not exclamation marks; no
banned marketing words; product framing as "a worker that does the job",
not "a tool you use").

Invoked from ``bin/cognis_trade.py`` BEFORE upstream's CLI initializes, so
the very first thing the customer sees is Cognis branding. If
``COGNIS_BRANDING`` env is unset, this is a no-op (upstream's banner shows
through).
"""

from __future__ import annotations

import sys
import textwrap
from typing import TextIO

from hummingbot.cognis.branding.cognis_brand import branding_enabled, load_brand


_BANNER_WIDTH = 78


def print_cognis_banner(stream: TextIO = sys.stdout) -> None:
    """Print the Cognis Trade banner to ``stream`` (default: stdout).

    No-op if the Cognis overlay is not enabled — keeps upstream parity for
    operators running the binary off-Cognis (e.g. local-only dev).
    """
    if not branding_enabled():
        return

    brand = load_brand()
    bar = "=" * _BANNER_WIDTH
    title = brand.product_name.center(_BANNER_WIDTH)
    tagline = brand.product_tagline.center(_BANNER_WIDTH)

    paragraphs = [
        bar,
        "",
        title,
        tagline,
        "",
        bar,
        "",
        "What this is",
        "-" * len("What this is"),
        textwrap.fill(brand.product_job, width=_BANNER_WIDTH),
        "",
        "Default mode",
        "-" * len("Default mode"),
        textwrap.fill(
            "Strategies start in paper mode. Going live is a separate step you "
            "take in the portal — it requires a completed paper run and an "
            "explicit confirmation that's recorded for audit.",
            width=_BANNER_WIDTH,
        ),
        "",
        "Where to go for help",
        "-" * len("Where to go for help"),
        f"  Portal:  {brand.portal_url}",
        f"  Docs:    {brand.docs_url}",
        f"  Support: {brand.support_email}",
        "",
        bar,
        "",
    ]

    stream.write("\n".join(paragraphs))
    stream.flush()


def cognis_log_prefix() -> str:
    """Short brand prefix for log lines. Always returns "Cognis Trade" so
    log output is grep-friendly even when branding is off."""
    return "Cognis Trade"
