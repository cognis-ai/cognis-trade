"""Cognis Trade — brand identity constants.

Env-driven so a single binary can ship per Cognis sub-brand without recompile.
Defaults match the Cognis Trade product identity (the "AI worker that runs
trading strategies, on schedule" framing — see
cognis-platform/apps/portal/lib/products.ts).

Every customer-facing string the bot prints — CLI banner, log prefixes,
status messages — pulls from this module rather than hard-coding Hummingbot
copy. Per feedback_full_cognis_branding memory: every visible surface must
say Cognis Trade end-to-end, never the upstream name.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class CognisBrand:
    """Branded strings rendered into customer-facing surfaces."""

    product_name: str = "Cognis Trade"
    product_tagline: str = "Runs trading strategies, on schedule."
    product_job: str = "Executes your strategy 24/7 on the exchanges you connect."
    support_email: str = "support@cognisai.com"
    docs_url: str = "https://cognisai.com/docs/trade"
    portal_url: str = "https://app.cognisai.com/dashboard/trade"
    # CLI banner colours — Prompt Toolkit style strings.
    banner_color_primary: str = "ansiblue"
    banner_color_accent: str = "ansigreen"


def load_brand() -> CognisBrand:
    """Load the Cognis brand from env. Falls back to product defaults."""
    return CognisBrand(
        product_name=os.environ.get("COGNIS_PRODUCT_NAME", CognisBrand.product_name),
        product_tagline=os.environ.get(
            "COGNIS_PRODUCT_TAGLINE", CognisBrand.product_tagline
        ),
        product_job=os.environ.get("COGNIS_PRODUCT_JOB", CognisBrand.product_job),
        support_email=os.environ.get("COGNIS_SUPPORT_EMAIL", CognisBrand.support_email),
        docs_url=os.environ.get("COGNIS_DOCS_URL", CognisBrand.docs_url),
        portal_url=os.environ.get("COGNIS_PORTAL_URL", CognisBrand.portal_url),
        banner_color_primary=os.environ.get(
            "COGNIS_BANNER_COLOR_PRIMARY", CognisBrand.banner_color_primary
        ),
        banner_color_accent=os.environ.get(
            "COGNIS_BANNER_COLOR_ACCENT", CognisBrand.banner_color_accent
        ),
    )


def branding_enabled() -> bool:
    """True iff the operator opted into the Cognis overlay.

    The default is OFF so a fork checkout still passes upstream parity tests
    that compare exact stdout. Production deploys set ``COGNIS_BRANDING=on``
    in the container env.
    """
    raw = os.environ.get("COGNIS_BRANDING", "").strip().lower()
    return raw in ("on", "true", "1", "yes")
