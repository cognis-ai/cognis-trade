"""Cognis Trade — bot session glue.

The piece that ties Bridge-fetched config to Hummingbot's runtime. Owns the
config-poll loop, the strategy-launch decision, and the event-emit loop.
Stays thin so the upstream Hummingbot runtime does the actual trading.

Usage from ``bin/cognis_trade.py``::

    from hummingbot.cognis.session import CognisSession
    session = await CognisSession.bootstrap()
    await session.run_forever()
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

from hummingbot.cognis.branding.banner import cognis_log_prefix
from hummingbot.cognis.bridge_client import (
    BotConfig,
    BridgeClient,
    CognisBridgeError,
    RiskCaps,
    StrategyDef,
)
from hummingbot.cognis.risk_guards import RiskGuards

logger = logging.getLogger(__name__)


class CognisSession:
    """Owns the lifetime of a Cognis-managed bot instance.

    Responsibilities:
    - Pull config from Bridge on a poll loop (default 15s) and react to
      changes (strategies added/removed/started/stopped).
    - Enforce ``RiskGuards`` over every strategy before allowing it to run.
    - Queue + flush events back to Bridge so the portal shows live status.

    The actual strategy execution is delegated to the upstream Hummingbot
    runtime; this class only orchestrates start/stop + risk-cap enforcement.
    A v1.5 follow-up wires the strategies through ``HummingbotApplication``;
    today the session prints what *would* be started, which is enough for
    paper-mode validation against the Bridge wiring end-to-end.
    """

    def __init__(self, bridge: BridgeClient) -> None:
        self._bridge = bridge
        self._guards: Optional[RiskGuards] = None
        self._config: Optional[BotConfig] = None
        self._running_strategy_ids: set[str] = set()
        self._event_buffer: List[Dict[str, Any]] = []
        self._stop = asyncio.Event()

    @classmethod
    async def bootstrap(cls) -> "CognisSession":
        bridge = BridgeClient.from_env()
        session = cls(bridge)
        # First fetch so we fail fast on bad credentials rather than after
        # the poll loop is running.
        try:
            cfg = await bridge.fetch_config()
        except CognisBridgeError as err:
            await bridge.aclose()
            raise SystemExit(
                f"{cognis_log_prefix()}: failed to fetch initial config from Cognis Bridge: {err}"
            ) from err
        session._apply_config(cfg)
        await session._emit_event({"type": "bot_started", "payload": session._bot_started_payload()})
        return session

    async def run_forever(self) -> None:
        """Main session loop. Polls Bridge for config changes + flushes events.

        Returns when the OS signals shutdown (handled by the CLI wrapper)
        or ``shutdown()`` is invoked.
        """
        try:
            while not self._stop.is_set():
                if self._config is None:
                    await asyncio.sleep(1)
                    continue
                interval = max(5, self._config.poll_interval_seconds)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=interval)
                    break  # _stop fired during sleep
                except asyncio.TimeoutError:
                    pass

                # Poll for config changes — strategies started/stopped, caps
                # changed, keys rotated. Non-fatal on transient errors.
                try:
                    cfg = await self._bridge.fetch_config()
                    self._reconcile_config(cfg)
                except CognisBridgeError as err:
                    logger.warning(
                        "%s: config poll failed (will retry next interval): %s",
                        cognis_log_prefix(),
                        err,
                    )

                # Drain queued events.
                await self._flush_events()
        finally:
            await self._emit_event({"type": "bot_stopped", "payload": {}})
            await self._flush_events()
            await self._bridge.aclose()

    async def shutdown(self) -> None:
        self._stop.set()

    # ---------------------------------------------------------------------
    # Config + strategy lifecycle
    # ---------------------------------------------------------------------

    def _apply_config(self, cfg: BotConfig) -> None:
        self._config = cfg
        self._guards = RiskGuards(caps=cfg.risk_caps)
        for strat in cfg.strategies:
            if strat.is_running():
                self._start_strategy(strat)

    def _reconcile_config(self, cfg: BotConfig) -> None:
        if self._config is None or self._guards is None:
            self._apply_config(cfg)
            return
        # Caps may have changed; rebuild the guard with fresh limits.
        self._guards = RiskGuards(caps=cfg.risk_caps)
        prev_ids = {s.id for s in self._config.strategies if s.is_running()}
        new_ids = {s.id for s in cfg.strategies if s.is_running()}

        for stopped_id in prev_ids - new_ids:
            self._stop_strategy(stopped_id, reason="bridge_state_change")
        for started_id in new_ids - prev_ids:
            strat = next((s for s in cfg.strategies if s.id == started_id), None)
            if strat:
                self._start_strategy(strat)

        self._config = cfg

    def _start_strategy(self, strat: StrategyDef) -> None:
        assert self._guards is not None
        violation = self._guards.validate_strategy(strat)
        if violation is not None:
            logger.error(
                "%s: refusing to start strategy %r: %s",
                cognis_log_prefix(),
                strat.name,
                violation.message,
            )
            asyncio.create_task(
                self._emit_event(
                    {
                        "type": "error",
                        "strategyId": strat.id,
                        "payload": {
                            "message": violation.message,
                            "code": violation.code,
                            "cap": violation.cap,
                            "observed": violation.observed,
                        },
                    }
                )
            )
            return

        # TODO(v1.5): wire to HummingbotApplication.start() with the strategy
        # config translated from our schema to Hummingbot's YAML. For v1, the
        # bot validates the wiring end-to-end (Bridge → bot → Bridge events)
        # without executing trades.
        self._running_strategy_ids.add(strat.id)
        logger.info(
            "%s: would start strategy %r (%s/%s/%s)",
            cognis_log_prefix(),
            strat.name,
            strat.kind,
            strat.exchange,
            strat.trading_pair,
        )
        asyncio.create_task(
            self._emit_event(
                {
                    "type": "strategy_started",
                    "strategyId": strat.id,
                    "payload": {"kind": strat.kind, "pair": strat.trading_pair, "mode": "paper" if not strat.is_live() else "live"},
                }
            )
        )

    def _stop_strategy(self, strategy_id: str, *, reason: str) -> None:
        if strategy_id not in self._running_strategy_ids:
            return
        self._running_strategy_ids.discard(strategy_id)
        logger.info("%s: stopping strategy %s (reason=%s)", cognis_log_prefix(), strategy_id, reason)
        asyncio.create_task(
            self._emit_event(
                {
                    "type": "strategy_stopped",
                    "strategyId": strategy_id,
                    "payload": {"reason": reason},
                }
            )
        )

    # ---------------------------------------------------------------------
    # Events
    # ---------------------------------------------------------------------

    async def _emit_event(self, event: Dict[str, Any]) -> None:
        self._event_buffer.append(event)

    async def _flush_events(self) -> None:
        if not self._event_buffer:
            return
        batch = self._event_buffer[:]
        self._event_buffer.clear()
        try:
            accepted = await self._bridge.post_events(batch)
            if accepted < len(batch):
                logger.warning(
                    "%s: Bridge accepted %d of %d events; the rest were dropped server-side",
                    cognis_log_prefix(),
                    accepted,
                    len(batch),
                )
        except CognisBridgeError as err:
            logger.warning(
                "%s: event flush failed (will retry next interval): %s",
                cognis_log_prefix(),
                err,
            )
            # Re-queue at the head so we don't drop events on transient
            # network blips. Cap the buffer at 1k to avoid unbounded growth.
            self._event_buffer = batch + self._event_buffer
            if len(self._event_buffer) > 1000:
                self._event_buffer = self._event_buffer[-1000:]

    def _bot_started_payload(self) -> Dict[str, Any]:
        assert self._config is not None and self._guards is not None
        return {
            "cognis_org_id": self._config.cognis_org_id,
            "plan": self._config.plan,
            "exchange_keys_count": len(self._config.exchange_keys),
            "strategies_count": len(self._config.strategies),
            "risk_caps": json.loads(json.dumps(self._guards.snapshot(), default=str)),
        }
