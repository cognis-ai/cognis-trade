"""Cognis Trade — branding overlays.

Two surfaces:

- ``cognis_brand`` — env-driven product brand strings (name, support email,
  documentation URL, banner colours). Importable as
  ``from hummingbot.cognis.branding import cognis_brand``.
- ``banner`` — Cognis CLI banner printer. Called by ``bin/cognis_trade.py``
  before deferring to upstream's CLI startup.

Both modules are safe to import without any Cognis env vars set (they fall
back to sensible defaults so the upstream `bin/hummingbot.py` entrypoint
still works for parity-testing). Production deploys MUST set
``COGNIS_BRANDING=on`` so the banner replaces upstream's banner; otherwise
the customer sees Hummingbot copy and the brand promise breaks. See
feedback_full_cognis_branding memory + CLAUDE.md hard rule on customer-
visible surfaces.
"""

__all__ = ["cognis_brand", "banner"]
