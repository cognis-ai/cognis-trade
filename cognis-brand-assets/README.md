# Cognis Trade brand assets

The Cognis Trade product is CLI-only — there is no upstream web UI to brand, so this directory is reserved for any future visual assets we ship (favicon for a future dashboard page in the portal, social-card SVG for marketing, splash image baked into a v1.5 GUI mode).

Today the brand surfaces this fork ships are:

- `bin/cognis_trade.py` — Cognis CLI banner (text only, in `hummingbot/cognis/branding/banner.py`)
- `Dockerfile.cognis` — sets `COGNIS_BRANDING=on` + the brand env defaults
- `COGNIS-README.md` — customer-facing README overlay
- Bot log prefix — every line from the Cognis layer is prefixed `Cognis Trade:` (see `cognis_log_prefix()` in `branding/banner.py`)

The upstream `README.md` is left untouched to keep rebase-diff minimal. The portal's "Connect your bot" wizard links to `COGNIS-README.md`, not `README.md`.
