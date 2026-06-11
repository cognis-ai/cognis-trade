"""Cognis Trade — genuine paper-trade engine runner.

This is the piece that makes a Cognis strategy *actually execute* through the
upstream Hummingbot connector + clock stack, with NO fabricated events.

Hummingbot ships a ``PaperTradeExchange`` (``hummingbot.connector.exchange.
paper_trade``) that mirrors a REAL venue's public order book and fills your
orders against that live book locally — no API keys, no real money, but a
genuine connector order/fill lifecycle. That is exactly the "real paper" we
want: the order-created and order-filled events this runner forwards to Bridge
originate from Hummingbot's own connector events, not from us.

Design (kept deliberately minimal + robust for headless operation):

1. Build a paper connector for the strategy's pair via the upstream helper
   ``create_paper_trade_market(exchange, [pair])``. It wires an
   ``OrderBookTracker`` against the venue's real public market-data source.
2. Seed paper balances so the connector can place orders.
3. Add the connector to a REALTIME ``Clock`` and tick it inside the asyncio
   loop. The connector's network/order-book tracker spins up on first start.
4. Subscribe ``EventForwarder`` listeners for ``BuyOrderCreated`` /
   ``SellOrderCreated`` / ``OrderFilled`` (and cancel/complete). When the
   connector emits one, we map it to a Cognis event and hand it to the
   session's emit callback (``order_placed`` / ``order_filled`` / ...).
5. Once the connector is ``ready`` (real book loaded), periodically place a
   small LIMIT order priced to cross the current book so it fills against
   the live order book — producing a genuine ``OrderFilledEvent``.
6. On stop: cancel open orders, stop the clock + connector network, and
   return a small results summary the session posts via ``post_paper_results``
   so the strategy transitions to ``paper_completed`` from REAL activity.

Two execution paths (chosen at runtime by ``strat.kind``):

* ``pure_market_making`` → instantiate the upstream
  ``PureMarketMakingStrategy`` directly (the way Hummingbot's own unit tests
  do — ``init_params`` + ``clock.add_iterator``, NO ``HummingbotApplication``)
  and add it to the same realtime clock as the connector. It posts two-sided
  resting maker quotes (bid + ask) near the touch; the ``PaperTradeExchange``
  fills them as the live book trades through them — genuine maker fills, not
  crossing. This is the realistic strategy path.
* anything else (or if PMM init throws) → fall back to the legacy
  ``_place_crossing_order`` loop below, which lifts the ask each refresh so the
  paper connector matches it immediately. Deterministic, but always crosses
  (negative P&L) — a demo, not a strategy.

Either way the EventForwarder listeners are identical: strategy-placed orders
fire the SAME connector ``BuyOrderCreated`` / ``SellOrderCreated`` /
``OrderFilled`` events, so order_placed now shows BOTH sides under the PMM path.

RiskGuards: the crossing fallback validates per-order notional before placing.
Under the PMM path the strategy places orders directly via the connector, so
per-order interception isn't reachable without subclassing upstream (avoided);
we keep the cap snapshot/validate at strategy-start (in the session) and a
notional cap check at quote-config time — documented below.
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, Dict, Optional

from hummingbot.cognis.bridge_client import StrategyDef
from hummingbot.cognis.risk_guards import RiskGuards
from hummingbot.cognis.runner_base import ConnectorEventRunner, EmitCallback, dec_str

logger = logging.getLogger(__name__)


def _cfg_decimal(config: Any, key: str, default: str) -> Decimal:
    try:
        val = config.get(key) if hasattr(config, "get") else None
        if val is None:
            return Decimal(default)
        return Decimal(str(val))
    except Exception:  # noqa: BLE001 — config comes off the wire; be forgiving
        return Decimal(default)


class PaperStrategyRunner(ConnectorEventRunner):
    """Runs one Cognis strategy as a genuine paper-trade loop.

    One instance per running strategy. ``start()`` spins up the connector +
    clock and returns once the background task is launched; ``stop()`` tears
    it down and returns a results summary. Event forwarding + bookkeeping are
    inherited from ``ConnectorEventRunner``.
    """

    mode = "paper"

    def __init__(
        self,
        strat: StrategyDef,
        *,
        emit: EmitCallback,
        guards: Optional[RiskGuards] = None,
    ) -> None:
        super().__init__(strat, emit=emit, guards=guards)

        # Strategy params (sane defaults mirror the Bridge seed config).
        cfg = strat.config or {}
        self._bid_spread = _cfg_decimal(cfg, "bid_spread", "0.5") / Decimal("100")
        self._ask_spread = _cfg_decimal(cfg, "ask_spread", "0.5") / Decimal("100")
        self._order_amount = _cfg_decimal(cfg, "order_amount", "0.001")
        try:
            self._order_refresh_time = float(cfg.get("order_refresh_time") or 10)
        except Exception:  # noqa: BLE001
            self._order_refresh_time = 10.0

        self._trading_pair = strat.trading_pair
        self._exchange = strat.exchange  # e.g. binance_paper_trade
        # The upstream ``create_paper_trade_market`` helper wants the BASE venue
        # name (e.g. ``binance``) — it imports that connector's module and wraps
        # its real public-data OrderBookTracker in a PaperTradeExchange. Passing
        # ``binance_paper_trade`` makes it try to import a module of that name,
        # which does not exist (ModuleNotFoundError). Mirror upstream
        # ``connector_manager`` which strips the ``_paper_trade`` suffix.
        self._base_venue = (
            self._exchange[: -len("_paper_trade")]
            if self._exchange.endswith("_paper_trade")
            else self._exchange
        )

        self._kind = (strat.kind or "").strip().lower()

        # Connector/clock + bookkeeping (_connector, _clock, _orders_placed,
        # _fills, _realized_quote, _forwarders, _listening) come from the base.
        self._strategy: Any = None  # upstream PureMarketMakingStrategy when PMM path runs
        self._ran_path: str = "crossing_loop_fallback"  # updated to pmm when PMM init succeeds
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._loop = asyncio.get_event_loop()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Build the connector + clock and launch the background tick loop."""
        # Imports are lazy so the upstream-passthrough path (no Bridge) never
        # pays for the heavy connector import graph.
        from hummingbot.client.settings import AllConnectorSettings
        from hummingbot.connector.exchange.paper_trade import create_paper_trade_market
        from hummingbot.core.clock import Clock, ClockMode

        # Register the `*_paper_trade` connector settings. Upstream does this in
        # bin/hummingbot.py at app startup; the Cognis Bridge path bypasses that
        # entry point, so we must initialize it ourselves before building the
        # paper market — otherwise AllConnectorSettings has no entry for
        # ``<venue>_paper_trade`` and create_paper_trade_market raises KeyError.
        self._init_paper_trade_settings(AllConnectorSettings)

        logger.info(
            "[cognis.paper] building paper connector exchange=%s (base=%s) pair=%s",
            self._exchange,
            self._base_venue,
            self._trading_pair,
        )
        self._connector = create_paper_trade_market(self._base_venue, [self._trading_pair])

        base, quote = self._trading_pair.split("-")
        # Seed generous paper balances so order placement is never balance-bound.
        # These are simulated funds — no real money is involved.
        self._connector.set_balance(quote, Decimal("100000"))
        self._connector.set_balance(base, Decimal("1000"))

        self._wire_event_listeners()

        self._clock = Clock(ClockMode.REALTIME)
        self._clock.add_iterator(self._connector)

        self._task = asyncio.ensure_future(self._run())
        logger.info("[cognis.paper] runner task launched for strategy %s", self._strat.id)

    def _init_paper_trade_settings(self, all_connector_settings: Any) -> None:
        """Register the ``*_paper_trade`` connector variants.

        Idempotent: skips work if the strategy's paper connector is already
        registered. Uses the upstream default paper-trade venue list plus the
        venue implied by this strategy (so a non-default venue still works).
        """
        try:
            existing = all_connector_settings.get_connector_settings()
            if self._exchange in existing:
                return
        except Exception:  # noqa: BLE001
            existing = {}

        # Derive the base venue name (binance_paper_trade -> binance) and merge
        # with the upstream defaults so common pairs resolve out of the box.
        base_venue = self._exchange[: -len("_paper_trade")] if self._exchange.endswith(
            "_paper_trade"
        ) else self._exchange
        venues = ["binance", "kucoin", "kraken", "gate_io"]
        if base_venue and base_venue not in venues:
            venues.append(base_venue)
        all_connector_settings.initialize_paper_trade_settings(venues)

    async def stop(self) -> Dict[str, Any]:
        """Cancel orders, tear down the clock, return a results summary."""
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=15)
            except asyncio.TimeoutError:
                self._task.cancel()
            except Exception as err:  # noqa: BLE001
                logger.warning("[cognis.paper] task join error: %s", err)

        # Stop the PMM strategy iterator (if it ran) and cancel its resting
        # maker quotes. The clock __exit__ stops iterators too, but do it
        # explicitly so resting bid/ask orders are cancelled deterministically.
        try:
            if self._strategy is not None:
                # PMM has no bulk-cancel helper; cancel each active maker order
                # via the strategy, then rely on the connector cancel loop below
                # as a backstop. Stop the iterator so it doesn't re-quote.
                try:
                    market_info = getattr(self._strategy, "market_info", None)
                    for o in list(getattr(self._strategy, "active_orders", []) or []):
                        try:
                            self._strategy.cancel_order(market_info, o.client_order_id)
                        except Exception:  # noqa: BLE001
                            pass
                except Exception:  # noqa: BLE001
                    pass
                try:
                    self._strategy.stop(self._clock)
                except Exception:  # noqa: BLE001
                    pass
                try:
                    if self._clock is not None:
                        self._clock.remove_iterator(self._strategy)
                except Exception:  # noqa: BLE001
                    pass
        except Exception as err:  # noqa: BLE001
            logger.warning("[cognis.paper] strategy stop error: %s", err)

        # Best-effort cancel of any resting orders still on the connector.
        try:
            if self._connector is not None:
                for lo in list(getattr(self._connector, "limit_orders", []) or []):
                    try:
                        self._connector.cancel(self._trading_pair, lo.client_order_id)
                    except Exception:  # noqa: BLE001
                        pass
        except Exception as err:  # noqa: BLE001
            logger.warning("[cognis.paper] cancel-all error: %s", err)

        await self._teardown_clock()

        results = {
            "orders_placed": self._orders_placed,
            "fills": self._fills,
            "pnl_usd": float(self._realized_quote),
            "exchange": self._exchange,
            "pair": self._trading_pair,
            "engine": "hummingbot-paper-trade",
            "ran_path": self._ran_path,
        }
        logger.info("[cognis.paper] strategy %s results: %s", self._strat.id, results)
        return results

    async def _teardown_clock(self) -> None:
        try:
            if self._connector is not None:
                await self._connector.stop_network()
        except Exception as err:  # noqa: BLE001
            logger.warning("[cognis.paper] stop_network error: %s", err)

    # Event wiring (_wire_event_listeners, _order_created_event,
    # _order_filled_event, _record_fill, _schedule_emit) is inherited from
    # ConnectorEventRunner — identical for paper + live connectors.

    # ------------------------------------------------------------------
    # The tick + order loop
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        # The upstream Clock uses a *sync* context manager (`with`, not `async
        # with`). `clock.run()` ticks every child iterator (the connector)
        # forever in REALTIME; we run it as a background task and supervise
        # readiness + order placement alongside it. We must keep the `with`
        # block open for the whole lifetime — `run_til`/`run` require an active
        # context, and __exit__ stops the iterators.
        clock_task: Optional[asyncio.Task] = None
        with self._clock:
            # Kick the connector's network (order-book tracker) up.
            try:
                await self._connector.start_network()
            except Exception as err:  # noqa: BLE001
                logger.warning("[cognis.paper] start_network error: %s", err)

            # Long-lived realtime tick loop for the connector.
            clock_task = asyncio.ensure_future(self._clock.run())

            # 1) Wait until the connector reports ready (real book loaded).
            ready = await self._wait_until_ready()
            if not ready:
                logger.error(
                    "[cognis.paper] connector never became ready for %s/%s — "
                    "venue public market data unreachable?",
                    self._exchange,
                    self._trading_pair,
                )
                await self._emit(
                    {
                        "type": "warning",
                        "strategyId": self._strat.id,
                        "payload": {
                            "message": (
                                f"paper connector {self._exchange} did not load "
                                f"order book for {self._trading_pair} (market data "
                                f"unreachable)"
                            )
                        },
                    }
                )
                if clock_task is not None:
                    clock_task.cancel()
                return

            logger.info(
                "[cognis.paper] connector ready; mid=%s — selecting execution path",
                self._safe_mid(),
            )

            # 2) Choose the execution path. For pure_market_making, drive the
            #    REAL upstream strategy (two-sided maker quoting). For anything
            #    else — or if PMM init throws — fall back to the crossing loop.
            pmm_started = False
            if self._kind == "pure_market_making":
                pmm_started = self._try_start_pmm_strategy()

            try:
                if pmm_started:
                    self._ran_path = "pure_market_making_strategy"
                    logger.info(
                        "[cognis.paper] running PureMarketMakingStrategy "
                        "(two-sided maker quoting) bid_spread=%s ask_spread=%s "
                        "order_amount=%s refresh=%ss",
                        self._bid_spread,
                        self._ask_spread,
                        self._order_amount,
                        self._order_refresh_time,
                    )
                    # The strategy is a clock iterator now; the background
                    # clock_task ticks it, which places + refreshes its own
                    # bid/ask maker orders. We just supervise until stop.
                    while not self._stop.is_set():
                        await asyncio.sleep(1.0)
                else:
                    self._ran_path = "crossing_loop_fallback"
                    logger.info(
                        "[cognis.paper] running crossing_loop_fallback "
                        "(kind=%r) — deterministic crossing LIMIT orders",
                        self._kind,
                    )
                    last_order_ts = 0.0
                    while not self._stop.is_set():
                        now = time.time()
                        if now - last_order_ts >= self._order_refresh_time:
                            self._place_crossing_order()
                            last_order_ts = now
                        await asyncio.sleep(1.0)
            finally:
                if clock_task is not None:
                    clock_task.cancel()
                    try:
                        await clock_task
                    except (asyncio.CancelledError, Exception):  # noqa: BLE001
                        pass

    def _try_start_pmm_strategy(self) -> bool:
        """Instantiate the upstream PureMarketMakingStrategy and add it to the
        clock — the headless path Hummingbot's own unit tests use (no
        HummingbotApplication). Returns True if the strategy was wired in.

        On ANY failure we log it and return False so the caller falls back to
        the crossing loop — an honest, working path beats a broken PMM.
        """
        try:
            from hummingbot.strategy.market_trading_pair_tuple import (
                MarketTradingPairTuple,
            )
            from hummingbot.strategy.pure_market_making.pure_market_making import (
                PureMarketMakingStrategy,
            )

            base, quote = self._trading_pair.split("-")

            # Optional defense-in-depth: snapshot the per-order notional cap so
            # we don't quote sizes that would breach the plan. The strategy
            # places via the connector directly, so we can't intercept each
            # order through the strategy path without subclassing upstream
            # (avoided per fork rules). We at least validate the quoted notional
            # at config time against the guard's order cap.
            if self._guards is not None:
                try:
                    mid = self._safe_mid() or Decimal("0")
                    notional = mid * self._order_amount
                    violation = self._guards.intercept_order(
                        strategy=self._strat, notional_usd=notional
                    )
                    if violation is not None:
                        logger.warning(
                            "[cognis.paper] PMM quote notional blocked by guard: %s",
                            violation.message,
                        )
                        self._schedule_emit(
                            {
                                "type": "error",
                                "strategyId": self._strat.id,
                                "payload": {
                                    "message": violation.message,
                                    "code": violation.code,
                                },
                            }
                        )
                        return False
                except Exception as gerr:  # noqa: BLE001 — guard advisory only
                    logger.debug("[cognis.paper] PMM guard check skipped: %s", gerr)

            market_info = MarketTradingPairTuple(
                self._connector, self._trading_pair, base, quote
            )
            strategy = PureMarketMakingStrategy()
            # Map Cognis config → upstream params. Spreads are fractions
            # (already divided by 100 in __init__). filled_order_delay is set
            # short so the strategy re-quotes promptly after a maker fill rather
            # than idling for the upstream 60s default — keeps the demo lively.
            strategy.init_params(
                market_info=market_info,
                bid_spread=self._bid_spread,
                ask_spread=self._ask_spread,
                order_amount=self._order_amount,
                order_refresh_time=float(self._order_refresh_time),
                filled_order_delay=min(float(self._order_refresh_time), 5.0),
                order_refresh_tolerance_pct=Decimal("0"),
            )
            self._strategy = strategy
            self._clock.add_iterator(strategy)
            logger.info(
                "[cognis.paper] PureMarketMakingStrategy added to clock for %s",
                self._trading_pair,
            )
            return True
        except Exception as err:  # noqa: BLE001 — fall back to crossing loop
            logger.warning(
                "[cognis.paper] PMM init failed (%s) — falling back to crossing loop",
                err,
                exc_info=True,
            )
            self._strategy = None
            return False

    async def _wait_until_ready(self, timeout_s: float = 120.0) -> bool:
        """Poll the connector's readiness while the background clock ticks."""
        deadline = time.time() + timeout_s
        while time.time() < deadline and not self._stop.is_set():
            try:
                if self._connector.ready:
                    return True
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(1.0)
        return bool(getattr(self._connector, "ready", False))

    def _safe_mid(self) -> Optional[Decimal]:
        try:
            buy = self._connector.get_price(self._trading_pair, True)
            sell = self._connector.get_price(self._trading_pair, False)
            if buy and sell:
                return (Decimal(str(buy)) + Decimal(str(sell))) / Decimal("2")
        except Exception:  # noqa: BLE001
            pass
        return None

    def _place_crossing_order(self) -> None:
        """Place a small LIMIT BUY priced to cross the ask so it fills now.

        Using a crossing limit (price = best ask * (1 + ask_spread)) guarantees
        the paper connector matches it against the live book → a genuine fill,
        rather than resting forever like a normal maker order would in a quiet
        backtest window. This keeps the demo deterministic while remaining a
        REAL connector fill against REAL market data.
        """
        from hummingbot.core.event.events import OrderType

        try:
            best_ask = self._connector.get_price(self._trading_pair, True)
        except Exception as err:  # noqa: BLE001
            logger.debug("[cognis.paper] get_price failed: %s", err)
            return
        if not best_ask or Decimal(str(best_ask)).is_nan():
            return

        ask = Decimal(str(best_ask))
        # Price slightly through the ask to ensure a cross/fill.
        price = (ask * (Decimal("1") + self._ask_spread)).quantize(Decimal("0.01"))
        amount = self._order_amount

        # RiskGuards defense-in-depth on notional (paper is not live, so this is
        # advisory here, but we wire it through as designed).
        if self._guards is not None:
            notional = price * amount
            violation = self._guards.intercept_order(
                strategy=self._strat, notional_usd=notional
            )
            if violation is not None:
                logger.warning("[cognis.paper] order blocked by guard: %s", violation.message)
                self._schedule_emit(
                    {
                        "type": "error",
                        "strategyId": self._strat.id,
                        "payload": {"message": violation.message, "code": violation.code},
                    }
                )
                return

        try:
            order_id = self._connector.buy(
                self._trading_pair, amount, OrderType.LIMIT, price
            )
            # NOTE: orders_placed is incremented in the BuyOrderCreated listener
            # (fires for this order too), so we do NOT bump it here to avoid
            # double-counting.
            logger.info(
                "[cognis.paper] placed LIMIT BUY %s %s @ %s (order_id=%s)",
                amount,
                self._trading_pair,
                price,
                order_id,
            )
        except Exception as err:  # noqa: BLE001
            logger.warning("[cognis.paper] buy() failed: %s", err)
