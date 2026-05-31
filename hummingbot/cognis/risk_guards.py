"""Cognis Trade — bot-side risk guardrails.

**Bridge is the source of truth** for plan-tier caps (max notional, daily
loss cap %, allowed-pair allowlist, concurrent-strategy cap). This module
implements the same caps client-side as defense in depth: if Bridge has a
bug that lets a config through that exceeds caps, the bot still refuses.

Wired into the bot lifecycle in three places:

1. ``RiskGuards.validate_config()`` — called when the bot reads a strategy
   config from Bridge's ``/trade-bot/config`` response. Rejects configs
   exceeding plan caps before any order is placed.
2. ``RiskGuards.intercept_order()`` — called per outbound order. Notional
   check; if exceeded, the order is dropped and an ``error`` event is sent
   to Bridge.
3. ``RiskGuards.track_pnl()`` — running PnL accumulator. If the day's loss
   exceeds the cap %, the bot stops all live strategies for the day.

This module does NOT call Hummingbot core directly — the bot's main loop
calls ``RiskGuards`` and decides what to do with the verdict. Keeps the
guard testable in isolation and avoids any Cython interop.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable, List, Mapping, Optional

from hummingbot.cognis.bridge_client import RiskCaps, StrategyDef

logger = logging.getLogger(__name__)


@dataclass
class RiskViolation:
    """Returned by guard methods when a check fails. Caller decides what to
    do (skip the order, stop the strategy, post an event to Bridge)."""

    code: str  # short id: "pair_not_allowed" | "notional_exceeded" | "concurrent_cap" | "daily_loss_cap" | "live_blocked"
    message: str
    cap: float
    observed: float


@dataclass
class RiskGuards:
    caps: RiskCaps
    # Per-day cumulative PnL in USD. Reset by ``track_pnl`` when the UTC
    # date changes. Treated as signed (negative = loss).
    _running_pnl_usd: Decimal = field(default_factory=lambda: Decimal("0"))
    _pnl_date_utc: Optional[dt.date] = None
    # Estimated tenant equity in USD used as the denominator for the
    # daily-loss-cap percentage. Set from /trade-bot/config in production;
    # falls back to max_notional_usd in dev.
    _account_equity_usd: Optional[Decimal] = None

    # ---------------------------------------------------------------------
    # Strategy config validation
    # ---------------------------------------------------------------------

    def validate_strategy(self, strat: StrategyDef) -> Optional[RiskViolation]:
        """Validate a strategy definition against the loaded caps.

        Called after the bot reads /trade-bot/config and before it starts
        the strategy's runtime.
        """
        if strat.is_live() and not self.caps.allow_live_trading:
            return RiskViolation(
                code="live_blocked",
                message=(
                    f"Strategy {strat.name!r} is marked live but the current "
                    f"plan does not allow live trading. Upgrade your plan in "
                    f"the Cognis portal or run in paper mode."
                ),
                cap=0,
                observed=1,
            )

        if self.caps.allowed_pairs is not None and strat.trading_pair not in self.caps.allowed_pairs:
            return RiskViolation(
                code="pair_not_allowed",
                message=(
                    f"Trading pair {strat.trading_pair} not in plan allowlist "
                    f"({', '.join(self.caps.allowed_pairs) or 'none'})."
                ),
                cap=0,
                observed=1,
            )
        return None

    def validate_concurrent_running(
        self, running_strategies: Iterable[StrategyDef]
    ) -> Optional[RiskViolation]:
        running = sum(1 for s in running_strategies if s.is_running())
        if running > self.caps.max_concurrent_strategies:
            return RiskViolation(
                code="concurrent_cap",
                message=(
                    f"{running} running strategies exceeds plan cap of "
                    f"{self.caps.max_concurrent_strategies}."
                ),
                cap=self.caps.max_concurrent_strategies,
                observed=running,
            )
        return None

    # ---------------------------------------------------------------------
    # Per-order notional check
    # ---------------------------------------------------------------------

    def intercept_order(
        self,
        *,
        strategy: StrategyDef,
        notional_usd: Decimal,
    ) -> Optional[RiskViolation]:
        """Returns a violation if the order's notional exceeds the cap.

        ``notional_usd`` is the order amount times the price quoted in USD.
        Caller is responsible for computing it (mid-price * amount works
        for most strategies; for limit orders use the limit price).
        """
        if strategy.is_live() and notional_usd > Decimal(self.caps.max_notional_usd):
            return RiskViolation(
                code="notional_exceeded",
                message=(
                    f"Order notional ${notional_usd:.2f} exceeds plan cap "
                    f"${self.caps.max_notional_usd:.2f} on strategy {strategy.name!r}."
                ),
                cap=float(self.caps.max_notional_usd),
                observed=float(notional_usd),
            )
        return None

    # ---------------------------------------------------------------------
    # Daily PnL cap
    # ---------------------------------------------------------------------

    def set_account_equity(self, equity_usd: Decimal) -> None:
        """Set the tenant's account equity used as the daily-loss-cap denom.

        Pulled periodically from the connector's account balance during
        normal bot operation. If never set, the cap falls back to using
        ``max_notional_usd`` as a conservative denominator.
        """
        self._account_equity_usd = equity_usd

    def track_pnl(self, delta_usd: Decimal) -> Optional[RiskViolation]:
        """Accumulate per-order PnL and check the daily-loss cap.

        Returns a violation when the running negative PnL exceeds the
        configured percent of equity. Caller is responsible for stopping
        live strategies in response.
        """
        today = dt.datetime.utcnow().date()
        if self._pnl_date_utc != today:
            self._pnl_date_utc = today
            self._running_pnl_usd = Decimal("0")
        self._running_pnl_usd += delta_usd

        if self.caps.daily_loss_cap_pct <= 0:
            return None  # cap disabled (free plan)

        denom = self._account_equity_usd or Decimal(self.caps.max_notional_usd)
        if denom <= 0:
            return None
        loss_pct = (-self._running_pnl_usd / denom) * Decimal("100")
        if loss_pct > Decimal(self.caps.daily_loss_cap_pct):
            return RiskViolation(
                code="daily_loss_cap",
                message=(
                    f"Daily PnL {self._running_pnl_usd:.2f} USD = {loss_pct:.2f}% "
                    f"of equity ({denom:.2f}). Plan cap is "
                    f"{self.caps.daily_loss_cap_pct:.2f}%. Stopping live strategies."
                ),
                cap=float(self.caps.daily_loss_cap_pct),
                observed=float(loss_pct),
            )
        return None

    # ---------------------------------------------------------------------
    # Snapshot for logging / events
    # ---------------------------------------------------------------------

    def snapshot(self) -> Mapping[str, float | int | bool | List[str] | None]:
        return {
            "max_notional_usd": self.caps.max_notional_usd,
            "daily_loss_cap_pct": self.caps.daily_loss_cap_pct,
            "max_concurrent_strategies": self.caps.max_concurrent_strategies,
            "paper_trading_default": self.caps.paper_trading_default,
            "allow_live_trading": self.caps.allow_live_trading,
            "allowed_pairs": self.caps.allowed_pairs,
            "running_pnl_usd": float(self._running_pnl_usd),
            "account_equity_usd": (
                float(self._account_equity_usd) if self._account_equity_usd is not None else None
            ),
        }
