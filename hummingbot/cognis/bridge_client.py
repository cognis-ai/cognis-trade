"""Cognis Bridge HTTP client (async, aiohttp-based).

The cognis-trade fork uses this to talk to Cognis Bridge:

- ``GET /trade-bot/config`` — fetch tenant config (decrypted exchange keys,
  risk caps, strategy list) on bot startup and on every poll interval.
- ``POST /trade-bot/events`` — report lifecycle events back to Bridge so
  the portal can show live status.
- ``POST /trade-bot/paper-results`` — submit paper-trading results so the
  strategy can be promoted to live (gated in the portal by an explicit
  customer confirmation).

Auth: ``X-Cognis-Bot-Token: <token>`` header. The token is provisioned by
Bridge at tenant-create time and surfaced ONCE to the customer; they paste
it into the bot env as ``COGNIS_TRADE_API_TOKEN``. Rotation is via the
portal's admin rotate-bot-token endpoint.

The bot must NEVER persist decrypted exchange keys to disk — they live in
the BotConfig dataclass returned by ``fetch_config()``, scoped to the
bot's lifetime. Hummingbot's own connector init takes ``api_key`` /
``api_secret`` directly, so we just pass through.

Uses aiohttp (an upstream Hummingbot dependency already in setup/environment.yml
— no new pip install required, no rebase risk from a new dep).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

import aiohttp

logger = logging.getLogger(__name__)


class CognisBridgeError(RuntimeError):
    """Any failure talking to Cognis Bridge."""

    def __init__(self, message: str, *, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class ExchangeKey:
    exchange: str
    label: str
    api_key: str
    api_secret: str
    passphrase: Optional[str]


@dataclass(frozen=True)
class StrategyDef:
    id: str
    name: str
    kind: str
    exchange: str
    trading_pair: str
    status: str  # draft | paper_running | paper_completed | live_running | stopped | failed
    config: Mapping[str, Any]

    def is_running(self) -> bool:
        return self.status in ("paper_running", "live_running")

    def is_live(self) -> bool:
        return self.status == "live_running"


@dataclass(frozen=True)
class RiskCaps:
    max_notional_usd: float
    daily_loss_cap_pct: float
    allowed_pairs: Optional[List[str]]
    max_concurrent_strategies: int
    paper_trading_default: bool
    allow_live_trading: bool


@dataclass(frozen=True)
class BotConfig:
    cognis_org_id: str
    plan: str
    risk_caps: RiskCaps
    exchange_keys: List[ExchangeKey]
    strategies: List[StrategyDef]
    server_time: str
    poll_interval_seconds: int = 15

    def key_for(self, exchange: str, label: Optional[str] = None) -> Optional[ExchangeKey]:
        """Find an exchange key by venue + optional label."""
        for k in self.exchange_keys:
            if k.exchange == exchange and (label is None or k.label == label):
                return k
        return None

    def running_strategies(self) -> List[StrategyDef]:
        return [s for s in self.strategies if s.is_running()]

    def live_strategies(self) -> List[StrategyDef]:
        return [s for s in self.strategies if s.is_live()]


class BridgeClient:
    """Async client wrapping the Cognis Bridge /trade-bot/* surface.

    Construction args fall back to env if omitted, so the standard
    ``BridgeClient.from_env()`` factory is the production path.
    """

    def __init__(
        self,
        bridge_url: str,
        bot_token: str,
        *,
        timeout_s: float = 10.0,
    ) -> None:
        if not bridge_url:
            raise CognisBridgeError("bridge_url is empty")
        if not bot_token or len(bot_token) < 16:
            raise CognisBridgeError("bot_token is empty or too short")
        self._bridge_url = bridge_url.rstrip("/")
        self._bot_token = bot_token
        self._timeout = aiohttp.ClientTimeout(total=timeout_s, connect=3.0)
        # The session is lazily created on first call so __init__ stays sync
        # — easier to construct from non-async test code without warnings.
        self._session: Optional[aiohttp.ClientSession] = None

    @classmethod
    def from_env(cls) -> "BridgeClient":
        bridge_url = os.environ.get("COGNIS_BRIDGE_URL", "").strip()
        bot_token = os.environ.get("COGNIS_TRADE_API_TOKEN", "").strip()
        if not bridge_url:
            raise CognisBridgeError(
                "COGNIS_BRIDGE_URL is unset. Set it to your Cognis Bridge "
                "URL (e.g. https://bridge.cognisai.com) before launching the bot."
            )
        if not bot_token:
            raise CognisBridgeError(
                "COGNIS_TRADE_API_TOKEN is unset. Get a fresh token from the "
                "Cognis portal (Trade → Settings → Rotate bot token) and paste "
                "it into your bot env."
            )
        return cls(bridge_url, bot_token)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers={
                    "X-Cognis-Bot-Token": self._bot_token,
                    "Accept": "application/json",
                },
            )
        return self._session

    async def fetch_config(self) -> BotConfig:
        session = await self._ensure_session()
        try:
            async with session.get(f"{self._bridge_url}/trade-bot/config") as resp:
                body_text = await resp.text()
                if resp.status >= 400:
                    raise CognisBridgeError(
                        f"fetch_config -> {resp.status}: {body_text[:300]}",
                        status=resp.status,
                    )
                return _parse_bot_config(_safe_json(body_text))
        except aiohttp.ClientError as err:
            raise CognisBridgeError(f"network: {err}") from err

    async def post_events(self, events: List[Dict[str, Any]]) -> int:
        """Returns the count of events Bridge accepted. ``events`` shape:

            [{"type": "order_filled", "strategyId": "...", "payload": {...}, "ts": "ISO"}]

        ``ts`` is optional; Bridge falls back to its receive time.
        """
        if not events:
            return 0
        session = await self._ensure_session()
        try:
            async with session.post(
                f"{self._bridge_url}/trade-bot/events",
                json={"events": events},
            ) as resp:
                body_text = await resp.text()
                if resp.status >= 400:
                    raise CognisBridgeError(
                        f"post_events -> {resp.status}: {body_text[:300]}",
                        status=resp.status,
                    )
                body = _safe_json(body_text)
                return int(body.get("accepted", 0))
        except aiohttp.ClientError as err:
            raise CognisBridgeError(f"network: {err}") from err

    async def post_paper_results(
        self, strategy_id: str, results: Dict[str, Any]
    ) -> str:
        """Submit paper-trading results. Returns the strategy id Bridge confirms."""
        if not strategy_id:
            raise CognisBridgeError("strategy_id is empty")
        session = await self._ensure_session()
        try:
            async with session.post(
                f"{self._bridge_url}/trade-bot/paper-results",
                json={"strategyId": strategy_id, "results": results},
            ) as resp:
                body_text = await resp.text()
                if resp.status >= 400:
                    raise CognisBridgeError(
                        f"post_paper_results -> {resp.status}: {body_text[:300]}",
                        status=resp.status,
                    )
                body = _safe_json(body_text)
                sid = body.get("strategyId")
                if not isinstance(sid, str):
                    raise CognisBridgeError("post_paper_results: malformed response")
                return sid
        except aiohttp.ClientError as err:
            raise CognisBridgeError(f"network: {err}") from err

    async def aclose(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()


def _safe_json(text: str) -> Dict[str, Any]:
    """Decode JSON, raising CognisBridgeError on malformed payloads."""
    import json

    try:
        body = json.loads(text)
    except json.JSONDecodeError as err:
        raise CognisBridgeError(f"Bridge returned non-JSON body: {text[:200]}") from err
    if not isinstance(body, dict):
        raise CognisBridgeError("Bridge returned a non-object JSON body")
    return body


def _parse_bot_config(raw: Mapping[str, Any]) -> BotConfig:
    """Parse the wire JSON Bridge returns from ``/trade-bot/config``.

    Defensive over the boundary — every field is explicitly extracted with
    the expected type. Loose typing is a security risk for risk caps in
    particular (a stray ``str`` instead of ``float`` for ``max_notional_usd``
    would silently disable the guard).
    """
    caps_raw = raw.get("riskCaps") or {}
    allowed_pairs_raw = caps_raw.get("allowedPairs")
    risk_caps = RiskCaps(
        max_notional_usd=float(caps_raw.get("maxNotionalUsd") or 0),
        daily_loss_cap_pct=float(caps_raw.get("dailyLossCapPct") or 0),
        allowed_pairs=(
            [str(p) for p in allowed_pairs_raw] if isinstance(allowed_pairs_raw, list) else None
        ),
        max_concurrent_strategies=int(caps_raw.get("maxConcurrentStrategies") or 1),
        paper_trading_default=bool(caps_raw.get("paperTradingDefault", True)),
        allow_live_trading=bool(caps_raw.get("allowLiveTrading", False)),
    )

    keys_raw = raw.get("exchangeKeys") or []
    keys = [
        ExchangeKey(
            exchange=str(k.get("exchange")),
            label=str(k.get("label", "")),
            api_key=str(k.get("apiKey")),
            api_secret=str(k.get("apiSecret")),
            passphrase=str(k["passphrase"]) if k.get("passphrase") else None,
        )
        for k in keys_raw
        if isinstance(k, dict) and k.get("exchange") and k.get("apiKey")
    ]

    strategies_raw = raw.get("strategies") or []
    strategies = [
        StrategyDef(
            id=str(s.get("id")),
            name=str(s.get("name")),
            kind=str(s.get("kind")),
            exchange=str(s.get("exchange")),
            trading_pair=str(s.get("tradingPair")),
            status=str(s.get("status", "draft")),
            config=s.get("config") or {},
        )
        for s in strategies_raw
        if isinstance(s, dict) and s.get("id")
    ]

    return BotConfig(
        cognis_org_id=str(raw.get("cognisOrgId", "")),
        plan=str(raw.get("plan", "free")),
        risk_caps=risk_caps,
        exchange_keys=keys,
        strategies=strategies,
        server_time=str(raw.get("serverTime", "")),
        poll_interval_seconds=int(raw.get("pollIntervalSeconds") or 15),
    )
