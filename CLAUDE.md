# Cognis Trade — repo context for Claude

This is a soft fork of `hummingbot/hummingbot` (algorithmic crypto trading bots, market-making, exchange API connectors). **The fork is not the product — `cognis-platform/apps/bridge` is.** Every hour spent editing Hummingbot's Python strategies or Cython connector cores here costs 3-5× at next rebase (Cython `.pyx` / `.pxd` rebases are the worst — the conflict surface is high and the build is slow).

Hummingbot is the **most technical fork in the Cognis platform**. The productization tension: Cognis sells "no engineers needed," but Hummingbot is genuinely an engineering tool. **Bridge is where we hide that complexity. The fork stays close to upstream.**

## Branches

- `cognis/main` — default. Cognis work.
- `vendor/upstream` — mirror of hummingbot/hummingbot:**master** (NOT `main` — Hummingbot's default branch is `master`). NEVER edit.

## Hard rules

1. **Never edit upstream Python or Cython files casually.** Prefer wrapping (subclasses, plugin registration, environment-variable swaps, Bridge-side config injection) over surgical edits. If you must touch an upstream file, the PR upstream is **mandatory** before merging to `cognis/main`. Cython `.pyx` / `.pxd` edits are particularly painful at rebase — avoid them entirely if possible.
2. **Never `import openai` / `import anthropic` / `import litellm` directly.** Hummingbot ships no LLM calls upstream today. If we add any (e.g. strategy-suggestion bridge), it goes through `@cognis/llm-client` → `llm.cognisai.com` proxy. The bot itself should not be aware that LLMs exist — Bridge generates a strategy config and hands it to Hummingbot.
3. **Backtest-first, paper-trading-default UX is non-negotiable.** No customer reaches live trading without (a) a paper-trading run from a portal-rendered button, AND (b) an explicit "go live" confirmation that records audit (`audit_log.action=trade_go_live`). The fork ships Hummingbot's existing paper-trading config knob; Bridge enforces the gating.
4. **Per-tenant exchange API keys are envelope-encrypted at rest in Bridge.** They MUST NOT live in this repo, NOT in `conf/`, NOT in `.env`, NOT in upstream's `gateway` config files. Bridge fetches the wrapped key, unwraps in-memory for the running bot session, and rotates on schedule. Any PR that adds plaintext API-key handling to this fork fails review.
5. **Risk guardrails per plan are enforced by Bridge, not the fork.** Free / Starter / Pro / Enterprise plans each have caps: max position size (notional USD), daily loss cap (% of capital), allowed-pair allowlist, max concurrent strategies. Bridge writes these into the per-tenant strategy config before launching a bot. The fork is welcome to *re-validate* the caps client-side as defense-in-depth, but Bridge is the source of truth.
6. **Fork-diff cap: 5% of upstream LOC.** Tracked per-PR. Target ≤3%.
7. **Commit prefixes only:** `fork:` / `brand:` / `wire:` / `ci:` / `docs:`.
8. **All Cognis-specific multi-tenant + billing + LLM logic goes in Bridge.** This repo holds: Cognis-branded prompts / banners, optional Clerk auth hook, optional Bridge bridge-client, FORK.md/CLAUDE.md/CODEOWNERS, CI workflows.
9. **License posture: Apache-2.0 only.** Upstream is single-tier Apache-2.0; do not introduce dependencies under AGPL, SSPL, BUSL, FSL, PolyForm, Commons Clause, or Elastic License. License-gate CI will catch this on PR.

## Stack (upstream)

- Python 3.11 + Cython (compiled connector cores in `hummingbot/connector/*/*.pyx`)
- Conda environment via `setup/environment.yml` + `setup.py`
- `setuptools` + `numpy ≥ 2.2.6` + `cython ≥ 3.0.12` build (`pyproject.toml`)
- Strategy templates in `hummingbot/strategy/` (legacy v1) and `hummingbot/strategy_v2/` (current)
- Exchange connectors in `hummingbot/connector/{exchange,derivative,gateway}/` — one subpackage per venue (binance, kucoin, coinbase, hyperliquid, backpack, etc.)
- `controllers/` — strategy-controller framework for `strategy_v2`
- `scripts/` — example bot scripts ("hello world" market-makers, arbitrage scripts)
- `conf/` — runtime config (per-bot YAML). Generated, not committed.
- `pytest` for tests; `black` + `isort` for formatting

## Cognis-specific surface (to be built in Phase 5)

- `hummingbot/cognis/bridge_client.py` — Bridge API client (resolve `org_id` from Clerk JWT, fetch wrapped exchange keys, fetch risk-guardrail config for the tenant's plan)
- `hummingbot/cognis/risk_guards.py` — defense-in-depth caps (re-validate notional, daily PnL, pair allowlist). Pulls config from `bridge_client`, never from local YAML.
- `hummingbot/cognis/branding/` — Cognis CLI banner, default strategy display names
- `.github/workflows/license-gate.yml` — ScanCode CI gate (blocks proprietary licenses sneaking in via Python deps — pip-installable packages occasionally relicense)
- `.github/workflows/upstream-rebase.yml` — nightly rebase bot (cron disabled at bootstrap; flip ON only after first manual run)
- `tools/check_no_proprietary.py` — license allowlist enforcement

## Build & test

This is upstream Hummingbot tooling: conda env, `./compile`, `pytest`. **Do NOT run pip/uv/poetry installs as part of fork-bootstrap or routine Claude work** — Hummingbot's build is heavy (Cython compile, full conda env) and the platform owner's instruction is explicit: no pip installs from Claude. See upstream README + CONTRIBUTING.md for human-driven build steps.

## What NOT to do

- Don't `import openai` / `import anthropic` / `import litellm` here — strategy intelligence comes from Bridge
- Don't add NestJS / Bridge / billing logic here — that's in `cognis-platform/apps/bridge`
- Don't touch `vendor/upstream` directly — it's a mirror branch
- Don't commit credentials. Exchange API keys NEVER live in this repo. `conf/` is generated per-session by Bridge and gitignored upstream.
- Don't switch off `black` / `isort` / `pytest` — keep upstream's tooling to minimize rebase friction
- Don't edit Cython `.pyx` / `.pxd` files (`connector_base.pyx`, `exchange_base.pyx`, etc.) unless you're upstream-PR-ing the same change first. Cython rebases are the worst-case scenario here.
- Don't enable the nightly rebase cron until the first manual `workflow_dispatch` run has been verified clean — Hummingbot's release pace can produce noisy rebases, and we want one clean baseline before going autonomous.
- Don't run pip/uv/poetry installs from Claude (platform owner's explicit instruction)
