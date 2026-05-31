# Cognis Trade

Runs trading strategies, on schedule.

Cognis Trade is the algorithmic-trading worker of the [Cognis AI](https://cognisai.com) platform. You hire it like an employee, hand it a strategy, and it runs the trades 24/7 on the exchanges you connect.

This repo is the **runtime binary** customers run on their own infrastructure. The strategies, risk caps, exchange keys, and live-trading approvals live in the [Cognis portal](https://app.cognisai.com/dashboard/trade) — the bot pulls them from Cognis Bridge over HTTPS on startup.

---

## What's in this repo

This is a soft fork of [`hummingbot/hummingbot`](https://github.com/hummingbot/hummingbot). The upstream binary is excellent; the Cognis layer adds:

- A **central control plane**: strategies, risk caps, and exchange keys are managed in one portal across every bot you run.
- **Backtest-first, paper-trading-default**: every strategy runs in paper mode first. Going live is a separate, audited confirmation in the portal.
- **Per-tenant exchange API keys, envelope-encrypted at rest** in Cognis Bridge. The bot decrypts them in-memory only.
- **Plan-tier risk guardrails** enforced both server-side (Bridge) and bot-side (`hummingbot/cognis/risk_guards.py`). A bug in one can't bypass the other.

The fork stays close to upstream. Everything Cognis-specific lives under `hummingbot/cognis/`, `bin/cognis_trade.py`, and `Dockerfile.cognis`. The rest is upstream Hummingbot, rebased monthly.

---

## Quick start

### Docker (recommended)

```bash
docker run --rm \
  -e COGNIS_BRIDGE_URL=https://bridge.cognisai.com \
  -e COGNIS_TRADE_API_TOKEN=<your bot token> \
  -v $(pwd)/cognis-trade-data:/home/hummingbot/data \
  -v $(pwd)/cognis-trade-logs:/home/hummingbot/logs \
  cognis/trade:latest
```

The bot will:

1. Connect to Cognis Bridge with your token.
2. Fetch your tenant config (strategies, risk caps, decrypted exchange keys).
3. Start every strategy whose status is `paper_running` or `live_running` in the portal.
4. Report events back so the portal shows live status.

### From source

Requires Python 3.11 and conda. See upstream [`README.md`](./README.md) for the conda setup, then:

```bash
# Bridge-managed mode (the production path)
export COGNIS_BRANDING=on
export COGNIS_BRIDGE_URL=https://bridge.cognisai.com
export COGNIS_TRADE_API_TOKEN=<your bot token from the portal>
./bin/cognis_trade.py

# Stock-Hummingbot parity mode (no Cognis env set)
./bin/cognis_trade.py    # falls through to upstream's bin/hummingbot.py
```

### Getting a bot token

In the [Cognis portal](https://app.cognisai.com/dashboard/trade):

1. Open Settings → "Connect your bot".
2. Click "Generate bot token". The token shows once — copy it immediately.
3. Paste it into your bot's `COGNIS_TRADE_API_TOKEN` env.

Rotating the token immediately invalidates the previous one. If a bot session disconnects, the customer rotates rather than re-keying.

---

## How strategies work

Strategies are defined in the portal, not in YAML on the bot host. The bot's job is to execute what the portal says is running.

- **Draft** — strategy exists, not started.
- **Paper running** — strategy is executing against Hummingbot's `*_paper_trade` connectors. No live orders.
- **Paper completed** — paper run finished; results are uploaded to Bridge.
- **Live running** — strategy is executing against the real venue. Requires:
  1. A completed paper run (paper_results_json populated).
  2. A configured exchange key for the venue.
  3. The customer's plan allows live trading (Starter and up).
  4. An explicit `liveConfirmation: true` in the start request — recorded as an `AuditLog action=trade_go_live` row.

Stopping is one-step. Live → Stopped requires no confirmation; safety wins over friction.

---

## Risk guardrails

| Plan       | Max notional (USD) | Daily loss cap | Concurrent strategies | Live trading |
| ---------- | ------------------ | -------------- | --------------------- | ------------ |
| Free       | 0 (paper only)     | —              | 1                     | No           |
| Starter    | $500               | 2%             | 2                     | Yes          |
| Pro        | $10,000            | 5%             | 5                     | Yes          |
| Enterprise | $250,000           | 10%            | 25                    | Yes          |

These are defaults. Operators can override via the portal admin API (caps are stored per-tenant in `trade_accounts.risk_caps_json`). The bot re-validates every order against these caps locally — a Bridge bug can't accidentally let a strategy exceed them.

---

## Exchanges

Every connector upstream Hummingbot supports is available — spot (Binance, Coinbase Advanced Trade, KuCoin, OKX, etc.) and perpetuals (Binance Perpetual, Bybit Perpetual, Hyperliquid, etc.). Add an exchange via the portal:

1. Trade → Settings → "Add exchange key".
2. Pick the venue, paste the API key + secret (+ passphrase if the venue uses one).
3. The key is envelope-encrypted with AES-256-GCM before it hits the Cognis database. The bot decrypts in-memory for the session.

To remove a key: same screen, "Remove". Active strategies on that venue stop on the next config poll (default 15s).

---

## Logs and observability

- Bot logs go to `logs/cognis-trade.log` (or stdout in Docker).
- Strategy events (started, stopped, order placed, order filled, error) are streamed to Cognis Bridge and visible in the portal's Trade → Activity tab.
- Filled orders are metered as billing events (`metric=fills`) and roll up into your usage view.

---

## License

Apache-2.0 throughout. Upstream's LICENSE applies to the Hummingbot codebase; the Cognis layer (`hummingbot/cognis/`, `bin/cognis_trade.py`, `Dockerfile.cognis`, `COGNIS-README.md`) is Apache-2.0 as well, copyright Cognis AI.

---

## Support

- **Docs**: <https://cognisai.com/docs/trade>
- **Portal**: <https://app.cognisai.com/dashboard/trade>
- **Support**: <support@cognisai.com>
- **Upstream community** (for Hummingbot-specific questions): <https://discord.gg/hummingbot>
