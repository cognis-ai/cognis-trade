"""Cognis Trade — shared connector event-forwarding base.

Both the paper runner (``paper_runner.PaperStrategyRunner``) and the live
runner (``live_runner.LiveStrategyRunner``) drive an upstream Hummingbot
connector + ``Clock`` and forward the connector's REAL ``BuyOrderCreated`` /
``SellOrderCreated`` / ``OrderFilled`` / ``OrderCancelled`` events back to
Bridge as Cognis events. That wiring is identical regardless of whether the
connector is a ``PaperTradeExchange`` (paper) or a real ``BinanceExchange``
pointed at testnet (live), so it lives here once.

Subclasses own connector construction and the strategy/order loop; this base
owns:
  * the EventForwarder listener bindings (connector events -> Cognis events),
  * the order_placed / order_filled / order_canceled event shaping,
  * the running bookkeeping (orders_placed / fills / realized quote),
  * the ``_schedule_emit`` loop-bounce helper.

No upstream files are touched — this is Cognis-layer glue only.
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, List, Optional

from hummingbot.cognis.bridge_client import StrategyDef
from hummingbot.cognis.risk_guards import RiskGuards

logger = logging.getLogger(__name__)

EmitCallback = Callable[[Dict[str, Any]], Awaitable[None]]


def dec_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    try:
        return str(Decimal(str(val)))
    except Exception:  # noqa: BLE001
        return str(val)


class ConnectorEventRunner:
    """Base for runners that forward upstream connector events to Bridge.

    ``mode`` is "paper" or "live" — surfaced in the source field of emitted
    events so the portal/event log can distinguish paper fills from real ones.
    """

    mode: str = "paper"

    def __init__(
        self,
        strat: StrategyDef,
        *,
        emit: EmitCallback,
        guards: Optional[RiskGuards] = None,
    ) -> None:
        self._strat = strat
        self._emit = emit
        self._guards = guards

        self._trading_pair = strat.trading_pair
        self._connector: Any = None
        self._clock: Any = None

        # Bookkeeping for the results summary.
        self._orders_placed = 0
        self._fills = 0
        self._realized_quote = Decimal("0")  # signed quote delta from fills
        self._forwarders: List[Any] = []
        self._listening = False

    # ------------------------------------------------------------------
    # Event wiring (REAL connector events -> Cognis events)
    # ------------------------------------------------------------------

    def _wire_event_listeners(self) -> None:
        from hummingbot.core.event.event_forwarder import EventForwarder
        from hummingbot.core.event.events import MarketEvent

        def _on_buy_created(evt: Any) -> None:
            self._orders_placed += 1
            self._schedule_emit(self._order_created_event(evt, "buy"))

        def _on_sell_created(evt: Any) -> None:
            self._orders_placed += 1
            self._schedule_emit(self._order_created_event(evt, "sell"))

        def _on_filled(evt: Any) -> None:
            self._record_fill(evt)
            self._schedule_emit(self._order_filled_event(evt))

        def _on_cancelled(evt: Any) -> None:
            self._schedule_emit(
                {
                    "type": "order_canceled",
                    "strategyId": self._strat.id,
                    "payload": {
                        "order_id": getattr(evt, "order_id", None),
                        "pair": self._trading_pair,
                        "mode": self.mode,
                    },
                }
            )

        bindings = [
            (MarketEvent.BuyOrderCreated, _on_buy_created),
            (MarketEvent.SellOrderCreated, _on_sell_created),
            (MarketEvent.OrderFilled, _on_filled),
            (MarketEvent.OrderCancelled, _on_cancelled),
        ]
        for tag, fn in bindings:
            fwd = EventForwarder(fn)
            self._connector.add_listener(tag, fwd)
            self._forwarders.append(fwd)
        self._listening = True

    def _order_created_event(self, evt: Any, side: str) -> Dict[str, Any]:
        return {
            "type": "order_placed",
            "strategyId": self._strat.id,
            "payload": {
                "order_id": getattr(evt, "order_id", None),
                "pair": getattr(evt, "trading_pair", self._trading_pair),
                "side": side,
                "price": dec_str(getattr(evt, "price", None)),
                "amount": dec_str(getattr(evt, "amount", None)),
                "source": "hummingbot_connector",
                "mode": self.mode,
            },
        }

    def _order_filled_event(self, evt: Any) -> Dict[str, Any]:
        trade_type = getattr(evt, "trade_type", None)
        side = getattr(trade_type, "name", str(trade_type)).lower() if trade_type else None
        return {
            "type": "order_filled",
            "strategyId": self._strat.id,
            "payload": {
                "order_id": getattr(evt, "order_id", None),
                "pair": getattr(evt, "trading_pair", self._trading_pair),
                "side": side,
                "price": dec_str(getattr(evt, "price", None)),
                "amount": dec_str(getattr(evt, "amount", None)),
                "exchange_trade_id": getattr(evt, "exchange_trade_id", None),
                "source": "hummingbot_connector",
                "mode": self.mode,
            },
        }

    def _record_fill(self, evt: Any) -> None:
        self._fills += 1
        try:
            price = Decimal(str(getattr(evt, "price")))
            amount = Decimal(str(getattr(evt, "amount")))
            trade_type = getattr(evt, "trade_type", None)
            side = getattr(trade_type, "name", "").lower()
            quote = price * amount
            self._realized_quote += quote if side == "sell" else -quote
        except Exception:  # noqa: BLE001
            pass

    def _schedule_emit(self, event: Dict[str, Any]) -> None:
        try:
            asyncio.ensure_future(self._emit(event))
        except RuntimeError:
            logger.debug(
                "[cognis.%s] could not schedule emit for %s",
                self.mode,
                event.get("type"),
            )
