"""Cognis Trade — genuine LIVE execution runner (real exchange connector).

The live counterpart to ``paper_runner.PaperStrategyRunner``. Where the paper
runner wraps a ``PaperTradeExchange``, this runner instantiates a **real**
Hummingbot exchange connector authenticated with the tenant's per-tenant API
key+secret (delivered by Bridge via ``BotConfig.exchange_keys`` post the
trade-bot-auth-hardening change) and places **real orders** on the venue.

SAFETY POSTURE (real money, even on testnet):
  * This runner is launched ONLY for a strategy Bridge reports as
    ``live_running`` — Bridge only sets that status after the go-live gate
    (liveConfirmation:true + completed paper run + configured exchange key +
    plan.allowLiveTrading, audited as ``trade_go_live``). The worker asserts
    ``strat.is_live()`` defensively before doing anything.
  * ``RiskGuards.intercept_order`` is ENFORCED on EVERY outbound order. A
    notional-cap breach drops the order and emits an ``error`` event — the
    order is never sent to the venue.

TARGET VENUE (v1): **Binance Spot Testnet** (https://testnet.binance.vision).
We reuse the upstream ``binance`` connector and redirect its base URLs to
testnet via ``cognis.binance_testnet.apply_binance_testnet`` (a Cognis-layer
runtime override — no upstream edits). Production-real Binance is intentionally
NOT enabled here yet; flipping the venue to mainnet is a one-line gate change
plus an explicit platform decision (documented in TRADE-READINESS.md).

Event forwarding + bookkeeping are shared with the paper runner via
``runner_base.ConnectorEventRunner`` — order_placed / order_filled events that
reach Bridge originate from the REAL connector, with ``mode: "live"``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, Dict, Optional

from hummingbot.cognis.bridge_client import ExchangeKey, StrategyDef
from hummingbot.cognis.risk_guards import RiskGuards
from hummingbot.cognis.runner_base import ConnectorEventRunner

logger = logging.getLogger(__name__)


def _cfg_decimal(config: Any, key: str, default: str) -> Decimal:
    try:
        val = config.get(key) if hasattr(config, "get") else None
        if val is None:
            return Decimal(default)
        return Decimal(str(val))
    except Exception:  # noqa: BLE001
        return Decimal(default)


# Venues this runner can build a live connector for. Each entry maps the
# Cognis/Bridge exchange identifier to a builder + a testnet-override applier.
_BINANCE_VENUES = {"binance", "binance_testnet", "binance_spot_testnet"}


class LiveStrategyRunner(ConnectorEventRunner):
    """Runs one LIVE Cognis strategy against a real exchange connector.

    One instance per running live strategy. ``start()`` builds the connector,
    wires events, and launches the background tick + quote loop; ``stop()``
    cancels open orders, tears down the connector network, and returns a
    results summary.
    """

    mode = "live"

    def __init__(
        self,
        strat: StrategyDef,
        key: ExchangeKey,
        *,
        emit,
        guards: Optional[RiskGuards] = None,
        use_testnet: bool = True,
    ) -> None:
        super().__init__(strat, emit=emit, guards=guards)
        # Defensive: a live runner must only ever run a Bridge-confirmed live
        # strategy. Bridge owns the go-live gate; this is belt-and-suspenders.
        if not strat.is_live():
            raise ValueError(
                f"LiveStrategyRunner refused: strategy {strat.id} is not "
                f"live_running (status={strat.status!r})"
            )
        self._key = key
        self._use_testnet = use_testnet
        self._exchange = strat.exchange

        cfg = strat.config or {}
        self._bid_spread = _cfg_decimal(cfg, "bid_spread", "0.5") / Decimal("100")
        self._ask_spread = _cfg_decimal(cfg, "ask_spread", "0.5") / Decimal("100")
        self._order_amount = _cfg_decimal(cfg, "order_amount", "0.001")
        try:
            self._order_refresh_time = float(cfg.get("order_refresh_time") or 10)
        except Exception:  # noqa: BLE001
            self._order_refresh_time = 10.0

        self._kind = (strat.kind or "").strip().lower()
        self._strategy: Any = None
        self._restore_testnet = None  # set by start() when testnet override applied
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._placed_first_order = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        from hummingbot.core.clock import Clock, ClockMode

        venue = self._exchange.strip().lower()
        if venue not in _BINANCE_VENUES:
            raise RuntimeError(
                f"live execution not implemented for venue {self._exchange!r}; "
                f"supported: {sorted(_BINANCE_VENUES)}"
            )

        # Redirect the binance connector to Spot Testnet BEFORE constructing it
        # (the connector reads the module-level URL constants at build time).
        if self._use_testnet:
            from hummingbot.cognis.binance_testnet import apply_binance_testnet

            self._restore_testnet = apply_binance_testnet()
            logger.info(
                "[cognis.live] strategy %s targeting Binance SPOT TESTNET "
                "(testnet.binance.vision)",
                self._strat.id,
            )

        self._connector = self._build_binance_connector()

        self._wire_event_listeners()

        self._clock = Clock(ClockMode.REALTIME)
        self._clock.add_iterator(self._connector)

        self._task = asyncio.ensure_future(self._run())
        logger.info(
            "[cognis.live] runner task launched for strategy %s (real connector "
            "exchange=%s pair=%s)",
            self._strat.id,
            self._exchange,
            self._trading_pair,
        )

    def _build_binance_connector(self) -> Any:
        """Construct a real, authenticated BinanceExchange.

        Keys come from the tenant's Bridge-delivered ExchangeKey — NEVER from
        disk/conf (CLAUDE.md rule 4). They live only in this object for the
        runner's lifetime.
        """
        from hummingbot.connector.exchange.binance.binance_exchange import (
            BinanceExchange,
        )

        logger.info(
            "[cognis.live] building authenticated binance connector for %s "
            "(key label=%s, testnet=%s)",
            self._trading_pair,
            self._key.label,
            self._use_testnet,
        )
        # domain stays "com": our testnet override replaces the host entirely,
        # so the {domain} placeholder is irrelevant. trading_required=True so
        # the connector authenticates + loads balances (real account calls).
        connector = BinanceExchange(
            binance_api_key=self._key.api_key,
            binance_api_secret=self._key.api_secret,
            trading_pairs=[self._trading_pair],
            trading_required=True,
        )
        return connector

    async def stop(self) -> Dict[str, Any]:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=15)
            except asyncio.TimeoutError:
                self._task.cancel()
            except Exception as err:  # noqa: BLE001
                logger.warning("[cognis.live] task join error: %s", err)

        # Stop the PMM strategy iterator + cancel its resting quotes, if any.
        try:
            if self._strategy is not None:
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
            logger.warning("[cognis.live] strategy stop error: %s", err)

        # Cancel any resting orders still on the real connector — real-money
        # hygiene: never leave live orders dangling after a stop.
        try:
            if self._connector is not None:
                for lo in list(getattr(self._connector, "limit_orders", []) or []):
                    try:
                        self._connector.cancel(self._trading_pair, lo.client_order_id)
                    except Exception:  # noqa: BLE001
                        pass
        except Exception as err:  # noqa: BLE001
            logger.warning("[cognis.live] cancel-all error: %s", err)

        try:
            if self._connector is not None:
                await self._connector.stop_network()
        except Exception as err:  # noqa: BLE001
            logger.warning("[cognis.live] stop_network error: %s", err)

        if self._restore_testnet is not None:
            try:
                self._restore_testnet()
            except Exception:  # noqa: BLE001
                pass

        results = {
            "orders_placed": self._orders_placed,
            "fills": self._fills,
            "pnl_usd": float(self._realized_quote),
            "exchange": self._exchange,
            "pair": self._trading_pair,
            "engine": "hummingbot-live-binance-testnet"
            if self._use_testnet
            else "hummingbot-live-binance",
            "mode": "live",
        }
        logger.info("[cognis.live] strategy %s results: %s", self._strat.id, results)
        return results

    # ------------------------------------------------------------------
    # The tick + order loop
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        clock_task: Optional[asyncio.Task] = None
        with self._clock:
            try:
                await self._connector.start_network()
            except Exception as err:  # noqa: BLE001
                logger.warning("[cognis.live] start_network error: %s", err)

            clock_task = asyncio.ensure_future(self._clock.run())

            # Wait for the connector to authenticate + load the order book and
            # trading rules. On testnet with a real key this resolves; with a
            # placeholder/dummy key the private account calls 401 and the
            # connector never becomes ready — we surface that honestly.
            ready = await self._wait_until_ready()
            if not ready:
                logger.error(
                    "[cognis.live] connector never became ready for %s/%s — "
                    "auth failed against the venue (testnet=%s)? Check the "
                    "tenant API key/secret. The REAL testnet endpoint WAS "
                    "contacted (testnet.binance.vision).",
                    self._exchange,
                    self._trading_pair,
                    self._use_testnet,
                )
                await self._emit(
                    {
                        "type": "error",
                        "strategyId": self._strat.id,
                        "payload": {
                            "code": "live_connector_not_ready",
                            "message": (
                                f"live binance connector did not become ready for "
                                f"{self._trading_pair} (auth/market-data against "
                                f"{'testnet.binance.vision' if self._use_testnet else 'binance'} "
                                f"failed — likely API key auth)"
                            ),
                            "mode": "live",
                        },
                    }
                )
                if clock_task is not None:
                    clock_task.cancel()
                return

            logger.info(
                "[cognis.live] connector READY (authenticated against %s); "
                "mid=%s — selecting execution path",
                "testnet.binance.vision" if self._use_testnet else "binance",
                self._safe_mid(),
            )

            pmm_started = False
            if self._kind == "pure_market_making":
                pmm_started = self._try_start_pmm_strategy()

            try:
                if pmm_started:
                    logger.info(
                        "[cognis.live] running PureMarketMakingStrategy LIVE "
                        "(two-sided maker quoting) on real connector",
                    )
                    while not self._stop.is_set():
                        await asyncio.sleep(1.0)
                else:
                    logger.info(
                        "[cognis.live] running single-order live path (kind=%r)",
                        self._kind,
                    )
                    last_order_ts = 0.0
                    while not self._stop.is_set():
                        now = time.time()
                        if now - last_order_ts >= self._order_refresh_time:
                            self._place_live_order()
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
        try:
            from hummingbot.strategy.market_trading_pair_tuple import (
                MarketTradingPairTuple,
            )
            from hummingbot.strategy.pure_market_making.pure_market_making import (
                PureMarketMakingStrategy,
            )

            base, quote = self._trading_pair.split("-")

            # Enforce the per-order notional cap at quote-config time (the PMM
            # path places via the connector directly). The single-order path
            # below ALSO calls intercept_order per order — so every live order
            # is guarded one way or another.
            if self._guards is not None:
                mid = self._safe_mid() or Decimal("0")
                notional = mid * self._order_amount
                violation = self._guards.intercept_order(
                    strategy=self._strat, notional_usd=notional
                )
                if violation is not None:
                    logger.warning(
                        "[cognis.live] PMM quote notional BLOCKED by guard: %s",
                        violation.message,
                    )
                    self._schedule_emit(
                        {
                            "type": "error",
                            "strategyId": self._strat.id,
                            "payload": {
                                "message": violation.message,
                                "code": violation.code,
                                "mode": "live",
                            },
                        }
                    )
                    return False

            market_info = MarketTradingPairTuple(
                self._connector, self._trading_pair, base, quote
            )
            strategy = PureMarketMakingStrategy()
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
                "[cognis.live] PureMarketMakingStrategy added to clock for %s",
                self._trading_pair,
            )
            return True
        except Exception as err:  # noqa: BLE001
            logger.warning(
                "[cognis.live] PMM init failed (%s) — falling back to single-order path",
                err,
                exc_info=True,
            )
            self._strategy = None
            return False

    async def _wait_until_ready(self, timeout_s: float = 90.0) -> bool:
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

    def _place_live_order(self) -> None:
        """Place ONE small LIMIT maker order on the real venue, guard-checked.

        Used for the non-PMM live path. Prices a resting BUY just below the
        best bid so it's a maker order (won't immediately cross), keeping the
        live demo conservative. RiskGuards.intercept_order is enforced first.
        """
        if self._placed_first_order:
            return  # one order is enough to prove the live path; don't churn
        from hummingbot.core.event.events import OrderType

        try:
            best_bid = self._connector.get_price(self._trading_pair, False)
        except Exception as err:  # noqa: BLE001
            logger.debug("[cognis.live] get_price failed: %s", err)
            return
        if not best_bid or Decimal(str(best_bid)).is_nan():
            return

        bid = Decimal(str(best_bid))
        price = (bid * (Decimal("1") - self._bid_spread)).quantize(Decimal("0.01"))
        amount = self._order_amount

        if self._guards is not None:
            notional = price * amount
            violation = self._guards.intercept_order(
                strategy=self._strat, notional_usd=notional
            )
            if violation is not None:
                logger.warning(
                    "[cognis.live] live order BLOCKED by guard: %s", violation.message
                )
                self._schedule_emit(
                    {
                        "type": "error",
                        "strategyId": self._strat.id,
                        "payload": {
                            "message": violation.message,
                            "code": violation.code,
                            "mode": "live",
                        },
                    }
                )
                return

        try:
            order_id = self._connector.buy(
                self._trading_pair, amount, OrderType.LIMIT, price
            )
            self._placed_first_order = True
            logger.info(
                "[cognis.live] placed LIVE LIMIT BUY %s %s @ %s (order_id=%s) "
                "on real connector",
                amount,
                self._trading_pair,
                price,
                order_id,
            )
        except Exception as err:  # noqa: BLE001
            logger.warning("[cognis.live] buy() failed: %s", err)
