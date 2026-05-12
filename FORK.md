# Fork of hummingbot/hummingbot

This repo is a **soft fork** of [`hummingbot/hummingbot`](https://github.com/hummingbot/hummingbot), maintained as `cognis-trade` under the Cognis AI platform. License posture: Apache-2.0 throughout — upstream is single-tier Apache-2.0, no enterprise/proprietary directory to strip at fork time.

`cognis-trade` is the algorithmic-trading product (Phase 5) of the Cognis platform-of-products. Hummingbot is the **most technical** of the five Cognis product forks (Python, exchange-API connectors, strategy templates, market-making cores) — the productization layer in Bridge must hide that complexity behind a backtest-first, paper-trading-default UX. See [`cognis-platform`](https://github.com/cognis-ai/cognis-platform) for the shared brain (Bridge, LiteLLM proxy, Clerk auth, billing).

## Branches

| Branch | Purpose |
|---|---|
| `vendor/upstream` | Mirror of `hummingbot/hummingbot:master`. NEVER edit. Rebased by the nightly bot. Upstream's default branch is `master`, not `main`. |
| `cognis/main` | Cognis work. Rebased monthly onto `vendor/upstream`. Default branch. |

## Commit prefixes (grep-friendly across rebases)

- `fork:` — surgical edits to upstream files (last resort; prefer Bridge integration)
- `brand:` — branding (logos, default strategy names, copy)
- `wire:` — Cognis integration plumbing (Clerk-via-Bridge auth, `@cognis/llm-client` for strategy suggestions, Bridge API calls for tenant key resolution)
- `ci:` — GitHub Actions, license gate, rebase bot
- `docs:` — FORK.md, CLAUDE.md, CODEOWNERS, READMEs

## License-trap status

Verified 2026-05-12 at upstream SHA `91ff6bfa3c4b0c97f0d4f34eb635ef6f5b772db0`:

- LICENSE is plain Apache-2.0 (Copyright 2023 Hummingbot Foundation). No Commons Clause rider, no addendum, no separate COPYING / NOTICE files.
- No `enterprise/`, `ee/`, `cloud/`, `pro/`, `premium/`, `saas/`, `platform/`, `connectors_proprietary/` directories at any depth.
- README confirms: "The Hummingbot codebase is free and publicly available under the Apache 2.0 open-source license."
- `pyproject.toml` declares only build-system + tool config (black/isort/pytest) — no premium extras, no commercial-only optional-deps groups.
- Surveyed the broader `hummingbot` GitHub org for premium-connector or commercial-license sibling repos (the historical "Hummingbot Foundation" concern). All public org repos are Apache-2.0 or MIT (`gateway`, `dashboard`, `quants-lab`, `deploy`, `mcp`, `hummingbot-api`, `condor`, etc.). No commercial connector repo is published under a non-permissive license that we'd risk shipping inadvertently.

If upstream introduces a `pro/`, `premium/`, `ee/`, `connectors_proprietary/`, or equivalent directory in a future rebase, the strip happens in a **separate commit** before merging — same doctrine as `cognis-support`.

## Fork-diff target

≤3% of upstream LOC (default per fork-ops.md). Tracked on every PR via `git diff vendor/upstream...cognis/main --stat`. Hard cap 5% — build fails above that.

Hummingbot's surface (strategies + exchange connectors) is the *productizable* surface, but Cognis-specific logic stays out of the fork:

- **Strategy parameters / risk caps per plan** → Bridge (`trade_accounts` table + admin API).
- **Per-tenant exchange API key encryption + storage** → Bridge (envelope-encrypted at rest in Postgres).
- **Backtest-first UX, paper-trading default** → portal + Bridge orchestration; the fork only exposes Hummingbot's existing paper-trading config.
- **LLM-generated strategy suggestions** → Bridge calls `@cognis/llm-client`, hands a config blob to Hummingbot. The bot itself never calls an LLM provider.

## Rebase cadence

- Nightly bot: `.github/workflows/upstream-rebase.yml` (workflow lives on `cognis/main`; cron is **commented out** at bootstrap — flip it on AFTER the first manual `workflow_dispatch` run confirms a clean rebase against Hummingbot's pace).
- Auto-merge clean rebases via Mergify (configured at platform level once first rebase lands)
- Conflicts → bot opens issue labeled `rebase-conflict`; human review
- Shared `rerere-cache` committed to `cognis-platform/infra/rerere-cache/cognis-trade/` (Cython `.pyx`/`.pxd` files are the highest-conflict surface — rerere will earn its keep here fast)

## Cognis-side surface

What lives on `cognis/main` (and ONLY here, today, post-bootstrap):

- `FORK.md`, `CLAUDE.md`, `CODEOWNERS` — fork meta
- `.github/workflows/license-gate.yml` — ScanCode allowlist enforcement
- `.github/workflows/upstream-rebase.yml` — nightly rebase bot (cron disabled at bootstrap)
- `tools/check_no_proprietary.py` — license allowlist enforcement script

Planned (Phase 5):

- `hummingbot/cognis/auth.py` — Clerk JWT validator hook (only needed if direct Hummingbot UI access is offered; default = Pattern A through Bridge)
- `hummingbot/cognis/bridge_client.py` — exchange-API-key fetch from Bridge (envelope-encrypted at rest, decrypted in-memory per session)
- `hummingbot/cognis/risk_guards.py` — per-tenant guardrails read from Bridge (max position size, daily loss cap, allowed-pair allowlist by plan)
- `hummingbot/cognis/branding/` — Cognis CLI banner, default strategy template names, prompts

All product-level multi-tenant + billing + LLM-suggestion logic lives in `cognis-platform/apps/bridge`, NOT here.

## Upstream-PR policy

Contribute back to `hummingbot/hummingbot` *before* merging to `cognis/main`:

- Bug fixes in connectors, perf patches, test improvements, type fixes, refactors that shrink fork diff
- Strategy correctness fixes — these are mandatory upstream first; we don't want to ship a Cognis-only correctness patch that the broader community can't audit

Keep in fork (do NOT upstream):

- Clerk / Stripe / LiteLLM-gateway / Cognis-branded code
- Per-tenant exchange-API-key wiring through Bridge
- Risk-guardrail enforcement keyed on Cognis billing plan
- LLM-strategy-suggestion plumbing (the Cognis "I want a market-maker for SOL/USDC" → JSON config pipeline)

## References

- Fork-ops doctrine: `cognis-platform/docs/specs/fork-ops.md`
- Phase 5 productization rules: `cognis-platform/docs/plans/phase-1-support.md` (Phase 5 Trade stub)
- Bridge integration spec: `cognis-platform/docs/specs/bridge-service.md`
- LLM gateway: `cognis-platform/docs/specs/ai-gateway.md`
