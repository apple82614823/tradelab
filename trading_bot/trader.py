from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable
import uuid

from .binance_client import BinanceAPIError, BinanceFuturesClient
from .config import Config
from .precision import ceil_to_step, decimal_to_api, floor_to_step
from .state import (
    DryRunAccountStore,
    PositionState,
    StateStore,
    _strict_state_value_equal,
)


_EXPECTED_LOCAL_STATE_UNSET = object()
_N16_STOP_MODE = "trend_support_continuation_margin_capped"
_N17_STOP_MODE = "range_support_absorption_margin_capped"
_N18_STOP_MODE = "ascending_triangle_breakout_margin_capped"
_N19_STOP_MODE = "staircase_exhaustion_margin_capped"
_N20_STOP_MODE = "relative_strength_recovery_margin_capped"
_MICRO_STOP_MODE = "micro_observation_margin_capped"
_MICRO_STRATEGY_IDS = frozenset({"N21", "N22", "N23", "N24", "N25"})


@dataclass(frozen=True)
class SymbolRules:
    tick_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    step_size: Decimal
    min_notional: Decimal


@dataclass(frozen=True)
class TradePlan:
    symbol: str
    leverage: int
    quantity: Decimal
    entry_price: Decimal
    stop_loss_price: Decimal
    take_profit_price: Decimal
    stop_loss_pct: Decimal
    take_profit_pct: Decimal
    amplitude_24h_pct: Decimal
    high_24h_price: Decimal
    low_24h_price: Decimal
    risk_amount: Decimal
    notional_value: Decimal
    required_margin: Decimal
    balance: Decimal
    stop_mode: str = "amplitude"
    risk_reward_ratio: Decimal = Decimal("5")
    structure_id: str | None = None
    structure_stop_price: Decimal | None = None
    structure_target_price: Decimal | None = None
    target_risk_amount: Decimal | None = None
    actual_risk_amount: Decimal | None = None
    risk_capped_by_margin: bool = False
    pretrade_quantity: Decimal | None = None
    executed_quantity: Decimal | None = None
    final_protected_quantity: Decimal | None = None
    post_fill_actual_risk_amount: Decimal | None = None
    post_fill_required_margin: Decimal | None = None
    reduced_after_fill: bool = False
    entry_candle_open_time_ms: int | None = None
    entry_deadline_ms: int | None = None
    entry_min_price: Decimal | None = None
    entry_max_price: Decimal | None = None
    actual_entry_price: Decimal | None = None
    structure_context: dict[str, Any] | None = None


class EntryWindowExpiredError(BinanceAPIError):
    def __init__(self, plan: TradePlan, now_ms: int):
        self.now_ms = now_ms
        self.entry_deadline_ms = plan.entry_deadline_ms
        super().__init__(
            "ENTRY_WINDOW_EXPIRED_BEFORE_ORDER:"
            f"symbol={plan.symbol} now_ms={now_ms} deadline_ms={plan.entry_deadline_ms}"
        )


class MarketOrderExecutionPendingError(BinanceAPIError):
    """The BUY request may have executed; only reconciliation may resolve it."""


def assert_entry_window_open(plan: TradePlan, now_ms: int) -> None:
    if plan.entry_deadline_ms is not None and now_ms >= plan.entry_deadline_ms:
        raise EntryWindowExpiredError(plan, now_ms)


@dataclass(frozen=True)
class DryRunCloseResult:
    state: PositionState
    exit_reason: str
    exit_price: Decimal
    mark_price: Decimal
    pnl_amount: Decimal
    pnl_pct: Decimal
    balance_before: Decimal
    balance_after: Decimal


@dataclass(frozen=True)
class SyncResult:
    has_position: bool
    closed_state: PositionState | None = None
    execution_pending: bool = False
    pending_resolved: bool = False
    pending_detail: dict[str, Any] | None = None


@dataclass(frozen=True)
class LiveCloseResolution:
    result: str
    exit_reason: str
    exit_price: Decimal | None
    detail: dict[str, Any]


class Trader:
    def __init__(
        self,
        client: BinanceFuturesClient,
        config: Config,
        state: StateStore,
        logger,
        clock_ms: Callable[[], int] | None = None,
    ):
        self.client = client
        self.config = config
        self.state = state
        self.logger = logger
        self.clock_ms = clock_ms or (
            lambda: int(datetime.now(timezone.utc).timestamp() * 1000)
        )
        self.dry_run_account = DryRunAccountStore(config.dry_run_account_file, config.dry_run_balance_usdt)

    def get_open_positions(self) -> list[dict[str, Any]]:
        return self.client.get_open_positions()

    @staticmethod
    def _strict_local_state_equal(left: Any, right: Any) -> bool:
        """Compare persisted execution evidence without Python numeric coercion."""
        return _strict_state_value_equal(left, right)

    def _load_expected_local_state(
        self,
        expected_local_state: PositionState | None | object,
    ) -> PositionState | None:
        local_state = self.state.load()
        if (
            expected_local_state is not _EXPECTED_LOCAL_STATE_UNSET
            and not self._strict_local_state_equal(
                local_state,
                expected_local_state,
            )
        ):
            raise BinanceAPIError("LOCAL_STATE_CHANGED_BEFORE_RECONCILIATION")
        return local_state

    def close_dry_run_position_if_triggered(
        self,
        *,
        expected_local_state: PositionState | None | object = (
            _EXPECTED_LOCAL_STATE_UNSET
        ),
        defer_settlement: bool = False,
    ) -> DryRunCloseResult | None:
        local_state = self._load_expected_local_state(expected_local_state)
        if local_state is None or not local_state.dry_run:
            return None

        mark_price = self.client.get_mark_price(local_state.symbol)
        entry_price = Decimal(local_state.entry_price)
        quantity = Decimal(local_state.quantity)
        stop_loss_price = Decimal(local_state.stop_loss_price)
        take_profit_price = Decimal(local_state.take_profit_price)

        exit_reason = None
        exit_price = None
        if mark_price <= stop_loss_price:
            exit_reason = "STOP_LOSS"
            exit_price = stop_loss_price
        elif mark_price >= take_profit_price:
            exit_reason = "TAKE_PROFIT"
            exit_price = take_profit_price

        if exit_reason is None or exit_price is None:
            return None

        pnl_amount = (exit_price - entry_price) * quantity
        pnl_pct = (exit_price - entry_price) / entry_price
        if defer_settlement:
            balance_before = self.dry_run_account.preview_balance()
            balance_after = balance_before + pnl_amount
        else:
            balance_after = self.dry_run_account.apply_pnl(pnl_amount)
            balance_before = balance_after - pnl_amount
            self.state.clear()
        return DryRunCloseResult(
            state=local_state,
            exit_reason=exit_reason,
            exit_price=exit_price,
            mark_price=mark_price,
            pnl_amount=pnl_amount,
            pnl_pct=pnl_pct,
            balance_before=balance_before,
            balance_after=balance_after,
        )

    def settle_dry_run_balance_once(
        self,
        trade_review_id: int,
        expected_balance_before: Decimal,
        balance_after: Decimal,
    ) -> Decimal:
        return self.dry_run_account.settle_balance_once(
            trade_review_id,
            expected_balance_before,
            balance_after,
        )

    def sync_state_with_exchange(
        self,
        *,
        expected_local_state: PositionState | None | object = (
            _EXPECTED_LOCAL_STATE_UNSET
        ),
    ) -> SyncResult:
        local_state = self._load_expected_local_state(expected_local_state)
        if local_state and local_state.dry_run:
            self.logger.info("Dry-run position state exists, skipping scan: %s", local_state.symbol)
            return SyncResult(has_position=True)

        if (
            local_state is not None
            and isinstance(local_state.orders, dict)
            and isinstance(local_state.orders.get("execution_pending"), dict)
        ):
            return self._sync_market_order_execution_pending(local_state)
        if (
            local_state is not None
            and isinstance(local_state.orders, dict)
            and isinstance(
                local_state.orders.get("execution_cleanup_resolved"), dict
            )
        ):
            return SyncResult(
                has_position=False,
                execution_pending=False,
                pending_resolved=True,
                pending_detail=local_state.orders[
                    "execution_cleanup_resolved"
                ],
            )
        if (
            local_state is not None
            and isinstance(local_state.orders, dict)
            and isinstance(
                local_state.orders.get("emergency_cleanup_pending"), dict
            )
        ):
            return self._sync_emergency_cleanup_pending(local_state)

        open_positions = self.get_open_positions()
        if open_positions:
            if local_state is None:
                position = open_positions[0]
                self.state.save(
                    PositionState(
                        symbol=position.get("symbol", ""),
                        quantity=str(position.get("positionAmt", "0")),
                        entry_price=str(position.get("entryPrice", "0")),
                        stop_loss_price="",
                        take_profit_price="",
                        leverage=int(Decimal(str(position.get("leverage", "0")))),
                        opened_at=datetime.now(timezone.utc).isoformat(),
                        dry_run=False,
                        orders={"source": "detected_existing_exchange_position"},
                    )
                )
            return SyncResult(has_position=True)

        if local_state and not local_state.dry_run:
            self.logger.info("Exchange position is empty, close handling required for %s", local_state.symbol)
            return SyncResult(has_position=False, closed_state=local_state)
        return SyncResult(has_position=False)

    def _persist_pending_protection_progress(
        self,
        state: PositionState,
        pending_key: str,
        order_field: str,
        cleanup_audit: dict[str, Any],
    ) -> None:
        updated_orders = cleanup_audit.get("updated_orders")
        if not isinstance(updated_orders, list) or any(
            not isinstance(order, dict) for order in updated_orders
        ):
            return
        orders = dict(state.orders)
        pending = orders.get(pending_key)
        if not isinstance(pending, dict):
            return
        pending = dict(pending)
        pending[order_field] = updated_orders
        orders[pending_key] = pending
        self.state.save(replace(state, orders=orders))

    def _sync_market_order_execution_pending(
        self,
        state: PositionState,
    ) -> SyncResult:
        pending = state.orders["execution_pending"]
        client_order_id = pending.get("client_order_id")
        if not isinstance(client_order_id, str) or not client_order_id.strip():
            return SyncResult(
                has_position=True,
                execution_pending=True,
                pending_detail={"reason": "PENDING_CLIENT_ORDER_ID_INVALID"},
            )
        possible_protection_orders = pending.get(
            "possible_protection_orders", []
        )
        if not isinstance(possible_protection_orders, list) or any(
            not isinstance(order, dict) for order in possible_protection_orders
        ):
            return SyncResult(
                has_position=True,
                execution_pending=True,
                pending_detail={
                    "reason": "PENDING_PROTECTION_AUDIT_INVALID",
                    "client_order_id": client_order_id,
                },
            )
        if pending.get("phase") == "PRE_SUBMIT_RESERVED":
            strategy = state.orders.get("strategy")
            plan_payload = state.orders.get("plan")
            reserved_strategy_id = (
                strategy.get("strategy_id") if type(strategy) is dict else None
            )
            reserved_stop_mode = {
                "N16": _N16_STOP_MODE,
                "N17": _N17_STOP_MODE,
                "N18": _N18_STOP_MODE,
                "N19": _N19_STOP_MODE,
                "N20": _N20_STOP_MODE,
                **{strategy_id: _MICRO_STOP_MODE for strategy_id in _MICRO_STRATEGY_IDS},
            }.get(reserved_strategy_id)
            reservation_valid = (
                type(strategy) is dict
                and reserved_strategy_id in {
                    "N16", "N17", "N18", "N19", "N20", *_MICRO_STRATEGY_IDS
                }
                and type(strategy.get("signal_id")) is int
                and strategy["signal_id"] > 0
                and type(strategy.get("structure_id")) is str
                and len(strategy["structure_id"]) == 24
                and type(plan_payload) is dict
                and plan_payload.get("stop_mode") == reserved_stop_mode
                and plan_payload.get("structure_id")
                == strategy["structure_id"]
                and pending.get("strategy_id") == reserved_strategy_id
                and type(pending.get("signal_id")) is int
                and pending["signal_id"] == strategy["signal_id"]
                and pending.get("structure_id")
                == strategy["structure_id"]
                and pending.get("stop_mode") == reserved_stop_mode
                and len(possible_protection_orders) == 2
                and tuple(
                    order.get("role")
                    for order in possible_protection_orders
                ) == ("STOP", "TAKE_PROFIT")
                and all(
                    type(order) is dict
                    and order.get("submission_attempted") is False
                    and set(order) == {
                        "role",
                        "clientAlgoId",
                        "submission_attempted",
                        "absence_confirmations",
                    }
                    for order in possible_protection_orders
                )
            )
            if not reservation_valid:
                return SyncResult(
                    has_position=True,
                    execution_pending=True,
                    pending_detail={
                        "reason": "PRE_SUBMIT_RESERVATION_INVALID",
                        "client_order_id": client_order_id,
                    },
                )
            return SyncResult(
                has_position=False,
                execution_pending=False,
                pending_resolved=True,
                pending_detail={
                    "reason": "MARKET_ORDER_CONFIRMED_NOT_EXECUTED",
                    "resolution": "PRE_SUBMIT_RESERVATION_RELEASED",
                    "phase": "PRE_SUBMIT_RESERVED",
                    "strategy_id": reserved_strategy_id,
                    "signal_id": strategy["signal_id"],
                    "structure_id": strategy["structure_id"],
                    "stop_mode": reserved_stop_mode,
                    "client_order_id": client_order_id,
                },
            )
        query_confirms_absence = False
        try:
            order = self.client.get_order(
                state.symbol,
                orig_client_order_id=client_order_id,
            )
            executed_quantity, status = self._validate_market_order_response(
                state.symbol,
                order,
                client_order_id,
            )
            query_error = None
        except Exception as exc:
            order = None
            executed_quantity = Decimal("0")
            status = ""
            query_error = str(exc)
            query_confirms_absence = self._order_query_confirms_absence(exc)
        try:
            quantity = self.client.get_long_position_quantity_or_zero(
                state.symbol
            )
        except Exception as exc:
            return SyncResult(
                has_position=True,
                execution_pending=True,
                pending_detail={
                    "reason": "PENDING_POSITION_QUERY_FAILED",
                    "order_query_error": query_error,
                    "position_query_error": str(exc),
                    "client_order_id": client_order_id,
                },
            )

        if (
            pending.get("phase") == "MARKET_ORDER_SUBMITTING"
            and pending.get("strategy_id") in {
                "N16", "N17", "N18", "N19", "N20", *_MICRO_STRATEGY_IDS
            }
            and order is None
            and query_confirms_absence
            and quantity <= 0
        ):
            previous_confirmations = pending.get(
                "market_order_absence_confirmations", 0
            )
            if (
                type(previous_confirmations) is not int
                or previous_confirmations < 0
                or previous_confirmations > 1
            ):
                return SyncResult(
                    has_position=True,
                    execution_pending=True,
                    pending_detail={
                        "reason": "MARKET_ORDER_ABSENCE_AUDIT_INVALID",
                        "client_order_id": client_order_id,
                    },
                )
            confirmations = previous_confirmations + 1
            if confirmations < 2:
                orders = dict(state.orders)
                updated_pending = dict(pending)
                updated_pending[
                    "market_order_absence_confirmations"
                ] = confirmations
                orders["execution_pending"] = updated_pending
                replacement = replace(state, orders=orders)
                compare_and_save = getattr(self.state, "compare_and_save", None)
                try:
                    persisted = bool(
                        compare_and_save is not None
                        and compare_and_save(state, replacement)
                    )
                except Exception:
                    persisted = False
                return SyncResult(
                    has_position=True,
                    execution_pending=True,
                    pending_detail={
                        "reason": (
                            "MARKET_ORDER_ABSENCE_CONFIRMATION_PENDING"
                            if persisted
                            else "MARKET_ORDER_ABSENCE_CONFIRMATION_WRITE_FAILED"
                        ),
                        "client_order_id": client_order_id,
                        "absence_confirmations": (
                            confirmations if persisted else previous_confirmations
                        ),
                    },
                )
            return SyncResult(
                has_position=False,
                execution_pending=False,
                pending_resolved=True,
                pending_detail={
                    "reason": "MARKET_ORDER_CONFIRMED_NOT_EXECUTED",
                    "resolution": "MARKET_ORDER_ABSENCE_CONFIRMED_TWICE",
                    "strategy_id": pending.get("strategy_id"),
                    "signal_id": pending.get("signal_id"),
                    "structure_id": pending.get("structure_id"),
                    "stop_mode": pending.get("stop_mode"),
                    "client_order_id": client_order_id,
                    "absence_confirmations": confirmations,
                },
            )

        terminal_unexecuted = {"CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}
        if (
            order is not None
            and status in terminal_unexecuted
            and executed_quantity <= 0
            and quantity <= 0
        ):
            protection_cleanup = self._cancel_placed_protection_orders(
                state.symbol,
                possible_protection_orders,
            )
            if not protection_cleanup.get("confirmed_all_canceled"):
                self._persist_pending_protection_progress(
                    state,
                    "execution_pending",
                    "possible_protection_orders",
                    protection_cleanup,
                )
                return SyncResult(
                    has_position=True,
                    execution_pending=True,
                    pending_detail={
                        "reason": "UNEXECUTED_MARKET_PROTECTION_CLEANUP_PENDING",
                        "status": status,
                        "client_order_id": client_order_id,
                        "protection_cleanup": protection_cleanup,
                    },
                )
            return SyncResult(
                has_position=False,
                execution_pending=False,
                pending_resolved=True,
                pending_detail={
                    "reason": "MARKET_ORDER_CONFIRMED_NOT_EXECUTED",
                    "status": status,
                    "client_order_id": client_order_id,
                    "protection_cleanup": protection_cleanup,
                },
            )

        if quantity > 0 or executed_quantity > 0 or status == "FILLED":
            cleanup_audit: dict[str, Any] = {
                "reason": "LATE_MARKET_ORDER_EXECUTION_CLEANUP",
                "client_order_id": client_order_id,
                "observed_quantity": decimal_to_api(quantity),
                "order_status": status,
                "order_executed_quantity": decimal_to_api(executed_quantity),
            }
            cleanup_audit["protection_cleanup_before_close"] = (
                self._cancel_placed_protection_orders(
                    state.symbol,
                    possible_protection_orders,
                )
            )
            remaining = quantity
            cleanup_audit["close_attempts"] = []
            for attempt_number in range(1, 3):
                attempt: dict[str, Any] = {
                    "attempt": attempt_number,
                    "quantity": decimal_to_api(remaining),
                }
                if remaining > 0:
                    try:
                        attempt["close_order"] = self.client.close_position_market(
                            state.symbol,
                            remaining,
                        )
                    except Exception as exc:
                        attempt["close_error"] = str(exc)
                try:
                    remaining = self.client.get_long_position_quantity_or_zero(
                        state.symbol
                    )
                    attempt["remaining_quantity"] = decimal_to_api(remaining)
                except Exception as exc:
                    attempt["position_query_error"] = str(exc)
                    cleanup_audit["close_attempts"].append(attempt)
                    return SyncResult(
                        has_position=True,
                        execution_pending=True,
                        pending_detail=cleanup_audit,
                    )
                cleanup_audit["close_attempts"].append(attempt)
                if remaining <= 0:
                    break
            cleanup_audit["protection_cleanup_after_close"] = (
                self._cancel_placed_protection_orders(
                    state.symbol,
                    possible_protection_orders,
                )
            )
            cleanup_audit["remaining_quantity"] = decimal_to_api(remaining)
            if (
                remaining > 0
                or not cleanup_audit["protection_cleanup_after_close"].get(
                    "confirmed_all_canceled"
                )
            ):
                self._persist_pending_protection_progress(
                    state,
                    "execution_pending",
                    "possible_protection_orders",
                    cleanup_audit["protection_cleanup_after_close"],
                )
                return SyncResult(
                    has_position=True,
                    execution_pending=True,
                    pending_detail=cleanup_audit,
                )
            return SyncResult(
                has_position=False,
                execution_pending=False,
                pending_resolved=True,
                pending_detail={
                    **cleanup_audit,
                    "reason": "LATE_MARKET_ORDER_EXECUTION_CLEANED",
                },
            )

        return SyncResult(
            has_position=True,
            execution_pending=True,
            pending_detail={
                "reason": "MARKET_ORDER_EXECUTION_STILL_UNKNOWN",
                "client_order_id": client_order_id,
                "order_status": status,
                "order_query_error": query_error,
                "executed_quantity": decimal_to_api(executed_quantity),
                "position_quantity": decimal_to_api(quantity),
            },
        )

    @staticmethod
    def _order_query_confirms_absence(error: Exception) -> bool:
        """Recognize only Binance's explicit order-not-found response."""

        detail = str(error).lower()
        return (
            "-2013" in detail
            or "order does not exist" in detail
            or "unknown order sent" in detail
        )

    def _sync_emergency_cleanup_pending(
        self,
        state: PositionState,
        *,
        phase_key: str = "emergency_cleanup_pending",
    ) -> SyncResult:
        pending = state.orders[phase_key]
        audit: dict[str, Any] = {
            "reason": "EMERGENCY_CLEANUP_RETRY",
            "previous": pending,
            "attempts": [],
        }
        try:
            ordinary_open_orders = self.client.get_open_orders(state.symbol)
        except Exception as exc:
            audit["ordinary_open_orders_query_error"] = str(exc)
            return SyncResult(
                has_position=True,
                execution_pending=True,
                pending_detail=audit,
            )
        audit["ordinary_open_orders"] = ordinary_open_orders
        if ordinary_open_orders:
            audit["reason"] = "EMERGENCY_ORDINARY_OPEN_ORDERS_REMAIN"
            return SyncResult(
                has_position=True,
                execution_pending=True,
                pending_detail=audit,
            )
        try:
            algo_open_orders = self.client.get_open_algo_orders(state.symbol)
        except Exception as exc:
            audit["algo_open_orders_query_error"] = str(exc)
            return SyncResult(
                has_position=True,
                execution_pending=True,
                pending_detail=audit,
            )
        audit["algo_open_orders"] = algo_open_orders
        if algo_open_orders:
            audit["reason"] = "EMERGENCY_ALGO_OPEN_ORDERS_REMAIN"
            return SyncResult(
                has_position=True,
                execution_pending=True,
                pending_detail=audit,
            )
        pending_orders = pending.get("placed_protection_orders", [])
        if not isinstance(pending_orders, list) or any(
            not isinstance(order, dict) for order in pending_orders
        ):
            audit["protection_cleanup_error"] = "INVALID_PENDING_ORDER_AUDIT"
            return SyncResult(
                has_position=True,
                execution_pending=True,
                pending_detail=audit,
            )
        try:
            quantity = self.client.get_long_position_quantity_or_zero(
                state.symbol
            )
        except Exception as exc:
            audit["query_error"] = str(exc)
            return SyncResult(
                has_position=True,
                execution_pending=True,
                pending_detail=audit,
            )
        if quantity > 0:
            audit["reason"] = "EMERGENCY_POSITION_REMAINS"
            audit["remaining_quantity"] = decimal_to_api(quantity)
            return SyncResult(
                has_position=True,
                execution_pending=True,
                pending_detail=audit,
            )
        audit["confirmed_closed"] = True
        updated_orders: list[dict[str, Any]] = []
        absence_pending = False
        for order in pending_orders:
            updated = dict(order)
            algo_id, client_algo_id = self._algo_order_identity(updated)
            submitted = updated.get("submission_attempted")
            if submitted is not False and algo_id is None and not client_algo_id:
                audit["reason"] = "EMERGENCY_PROTECTION_IDENTITY_INCOMPLETE"
                return SyncResult(
                    has_position=True,
                    execution_pending=True,
                    pending_detail=audit,
                )
            previous_absence = updated.get("absence_confirmations", 0)
            if type(previous_absence) is not int or previous_absence < 0:
                audit["reason"] = "EMERGENCY_PROTECTION_AUDIT_INVALID"
                return SyncResult(
                    has_position=True,
                    execution_pending=True,
                    pending_detail=audit,
                )
            if submitted is False:
                updated["absence_confirmations"] = 2
            elif previous_absence < 1:
                updated["absence_confirmations"] = 1
                absence_pending = True
            else:
                updated["absence_confirmations"] = previous_absence + 1
            updated_orders.append(updated)
        final_protection_cleanup = {
            "attempts": [],
            "confirmed_all_canceled": not absence_pending,
            "symbol": state.symbol,
            "open_orders_before_fallback": [],
            "updated_orders": updated_orders,
        }
        audit["protection_cleanup"] = final_protection_cleanup
        audit["final_protection_cleanup"] = final_protection_cleanup
        if not final_protection_cleanup.get("confirmed_all_canceled"):
            self._persist_pending_protection_progress(
                state,
                phase_key,
                "placed_protection_orders",
                final_protection_cleanup,
            )
            return SyncResult(
                has_position=True,
                execution_pending=True,
                pending_detail=audit,
            )
        audit["reason"] = (
            "EMERGENCY_CLEANUP_CONFIRMED_FLAT_WITHOUT_LIVE_RESULT"
        )
        return SyncResult(
            has_position=False,
            execution_pending=False,
            pending_resolved=True,
            pending_detail=audit,
        )

    def _query_protection_execution(
        self,
        symbol: str,
        role: str,
        saved_order: Any,
    ) -> dict[str, Any]:
        if not isinstance(saved_order, dict):
            raise BinanceAPIError(f"Missing saved {role} algo order for {symbol}.")
        algo_id = saved_order.get("algoId")
        client_algo_id = saved_order.get("clientAlgoId")
        if algo_id in {None, ""} and not client_algo_id:
            raise BinanceAPIError(f"Missing saved {role} algo identifier for {symbol}.")

        algo_order = self.client.get_algo_order(
            algo_id=algo_id if algo_id not in {None, ""} else None,
            client_algo_id=str(client_algo_id) if client_algo_id else None,
        )
        if not isinstance(algo_order, dict) or not algo_order:
            raise BinanceAPIError(
                f"Invalid queried {role} algo order for {symbol}: {algo_order}"
            )
        saved_algo_id, saved_client_algo_id = self._algo_order_identity(
            saved_order
        )
        queried_algo_id, queried_client_algo_id = self._algo_order_identity(
            algo_order
        )
        expected_order_type = (
            "STOP_MARKET" if role == "STOP_LOSS" else "TAKE_PROFIT_MARKET"
        )
        if (
            (saved_algo_id is not None and queried_algo_id != saved_algo_id)
            or (
                saved_client_algo_id is not None
                and queried_client_algo_id != saved_client_algo_id
            )
            or algo_order.get("symbol") != symbol
            or algo_order.get("algoType") != "CONDITIONAL"
            or algo_order.get("orderType") != expected_order_type
            or algo_order.get("side") != "SELL"
            or algo_order.get("closePosition") is not True
        ):
            raise BinanceAPIError(
                f"Queried {role} algo identity mismatch for {symbol}: "
                f"saved={saved_order} queried={algo_order}"
            )
        algo_status = str(algo_order.get("algoStatus", "")).upper()
        raw_actual_order_id = algo_order.get("actualOrderId")
        actual_order_id = self._normalized_exchange_order_id(raw_actual_order_id)
        actual_order: dict[str, Any] | None = None
        filled = False
        exit_price: Decimal | None = None
        actual_status = ""

        if not self._is_missing_actual_order_id(raw_actual_order_id):
            if actual_order_id is None:
                raise BinanceAPIError(
                    f"Invalid actual {role} order id for {symbol}: "
                    f"{raw_actual_order_id!r}"
                )
            actual_order = self.client.get_order(
                symbol,
                order_id=actual_order_id,
            )
            queried_order_id = (
                self._normalized_exchange_order_id(actual_order.get("orderId"))
                if isinstance(actual_order, dict)
                else None
            )
            if (
                not isinstance(actual_order, dict)
                or queried_order_id != actual_order_id
                or actual_order.get("symbol") != symbol
                or actual_order.get("side") != "SELL"
            ):
                raise BinanceAPIError(
                    f"Actual {role} order identity mismatch for {symbol}: "
                    f"expected_order_id={actual_order_id} response={actual_order}"
                )
            actual_status = str(actual_order.get("status", "")).upper()
            try:
                executed_quantity = Decimal(str(actual_order.get("executedQty", "0")))
            except ArithmeticError:
                executed_quantity = Decimal("0")
            filled = actual_status == "FILLED" or executed_quantity > 0
            exit_price = self._execution_price_from_payload(actual_order)
        else:
            try:
                actual_quantity = Decimal(str(algo_order.get("actualQty", "0")))
                actual_price = Decimal(str(algo_order.get("actualPrice", "0")))
            except ArithmeticError:
                actual_quantity = Decimal("0")
                actual_price = Decimal("0")
            filled = actual_quantity > 0 and algo_status in {"TRIGGERED", "FINISHED"}
            exit_price = actual_price if actual_price > 0 else None

        terminal_algo_statuses = {"CANCELED", "EXPIRED", "REJECTED", "FINISHED"}
        terminal_order_statuses = {"CANCELED", "EXPIRED", "REJECTED", "FILLED"}
        terminal_unfilled = (
            not filled
            and algo_status in terminal_algo_statuses
            and (actual_order is None or actual_status in terminal_order_statuses)
        )
        return {
            "role": role,
            "filled": filled,
            "terminal_unfilled": terminal_unfilled,
            "exit_price": exit_price,
            "algo_id": algo_order.get("algoId", algo_id),
            "client_algo_id": algo_order.get("clientAlgoId", client_algo_id),
            "algo_status": algo_status,
            "actual_order_id": actual_order_id,
            "actual_order_status": actual_status,
        }

    def resolve_closed_live_position(self, state: PositionState) -> LiveCloseResolution:
        saved_stop = (
            state.orders.get("stop")
            if isinstance(state.orders, dict)
            else None
        )
        saved_take_profit = (
            state.orders.get("take_profit")
            if isinstance(state.orders, dict)
            else None
        )
        try:
            stop = self._query_protection_execution(
                state.symbol,
                "STOP_LOSS",
                saved_stop,
            )
            take_profit = self._query_protection_execution(
                state.symbol,
                "TAKE_PROFIT",
                saved_take_profit,
            )
        except Exception as exc:
            return LiveCloseResolution(
                result="LIVE_RESULT_PENDING",
                exit_reason="LIVE_RESULT_PENDING",
                exit_price=None,
                detail={"reason": "PROTECTION_ORDER_QUERY_FAILED", "error": str(exc)},
            )

        def serializable_execution(value: dict[str, Any]) -> dict[str, Any]:
            result = dict(value)
            if isinstance(result.get("exit_price"), Decimal):
                result["exit_price"] = decimal_to_api(
                    result["exit_price"]
                )
            return result

        detail_stop = serializable_execution(stop)
        detail_take_profit = serializable_execution(take_profit)
        detail = {"stop": detail_stop, "take_profit": detail_take_profit}

        stop_cleanup_order = (
            {**saved_stop, "role": "STOP"}
            if isinstance(saved_stop, dict)
            else {}
        )
        take_profit_cleanup_order = (
            {**saved_take_profit, "role": "TAKE_PROFIT"}
            if isinstance(saved_take_profit, dict)
            else {}
        )

        def pending_after_cleanup(reason: str) -> LiveCloseResolution:
            try:
                refreshed_stop = self._query_protection_execution(
                    state.symbol,
                    "STOP_LOSS",
                    saved_stop,
                )
                refreshed_take_profit = self._query_protection_execution(
                    state.symbol,
                    "TAKE_PROFIT",
                    saved_take_profit,
                )
                detail["post_cleanup_stop"] = serializable_execution(
                    refreshed_stop
                )
                detail["post_cleanup_take_profit"] = (
                    serializable_execution(refreshed_take_profit)
                )
                stop_newly_filled = (
                    refreshed_stop["filled"] and not stop["filled"]
                )
                take_profit_newly_filled = (
                    refreshed_take_profit["filled"]
                    and not take_profit["filled"]
                )
                if (
                    refreshed_stop["filled"]
                    and refreshed_take_profit["filled"]
                    and (stop_newly_filled or take_profit_newly_filled)
                ):
                    reason = (
                        "BOTH_PROTECTION_ORDERS_FILLED_DURING_CLEANUP"
                    )
                elif stop_newly_filled or take_profit_newly_filled:
                    reason = "PROTECTION_ORDER_FILLED_DURING_CLEANUP"
            except Exception as exc:
                detail["post_cleanup_query_error"] = str(exc)
            detail["reason"] = reason
            return LiveCloseResolution(
                "LIVE_RESULT_PENDING",
                "LIVE_RESULT_PENDING",
                None,
                detail,
            )

        if stop["filled"] and not take_profit["filled"]:
            if not take_profit["terminal_unfilled"]:
                sibling_cleanup = self._cancel_placed_protection_orders(
                    state.symbol,
                    [take_profit_cleanup_order],
                )
                detail["sibling_cleanup"] = sibling_cleanup
                if not sibling_cleanup.get("confirmed_all_canceled"):
                    return pending_after_cleanup(
                        "SIBLING_PROTECTION_CANCEL_PENDING"
                    )
            return LiveCloseResolution("LOSS", "STOP_LOSS", stop["exit_price"], detail)
        if take_profit["filled"] and not stop["filled"]:
            if not stop["terminal_unfilled"]:
                sibling_cleanup = self._cancel_placed_protection_orders(
                    state.symbol,
                    [stop_cleanup_order],
                )
                detail["sibling_cleanup"] = sibling_cleanup
                if not sibling_cleanup.get("confirmed_all_canceled"):
                    return pending_after_cleanup(
                        "SIBLING_PROTECTION_CANCEL_PENDING"
                    )
            return LiveCloseResolution("WIN", "TAKE_PROFIT", take_profit["exit_price"], detail)
        if stop["filled"] and take_profit["filled"]:
            detail["reason"] = "BOTH_PROTECTION_ORDERS_FILLED"
            return LiveCloseResolution(
                "LIVE_RESULT_PENDING",
                "LIVE_RESULT_PENDING",
                None,
                detail,
            )
        if stop["terminal_unfilled"] and take_profit["terminal_unfilled"]:
            detail["reason"] = "NO_PROTECTION_ORDER_FILLED"
            return LiveCloseResolution(
                "MANUAL_OR_EXTERNAL_CLOSE",
                "MANUAL_OR_EXTERNAL_CLOSE",
                None,
                detail,
            )

        zero_position_cleanup_orders = []
        if not stop["terminal_unfilled"]:
            zero_position_cleanup_orders.append(stop_cleanup_order)
        if not take_profit["terminal_unfilled"]:
            zero_position_cleanup_orders.append(take_profit_cleanup_order)
        if zero_position_cleanup_orders:
            zero_position_cleanup = self._cancel_placed_protection_orders(
                state.symbol,
                zero_position_cleanup_orders,
            )
            detail["zero_position_protection_cleanup"] = (
                zero_position_cleanup
            )
            if not zero_position_cleanup.get("confirmed_all_canceled"):
                return pending_after_cleanup(
                    "ZERO_POSITION_PROTECTION_CANCEL_PENDING"
                )
            detail["reason"] = "NO_PROTECTION_ORDER_FILLED"
            return LiveCloseResolution(
                "MANUAL_OR_EXTERNAL_CLOSE",
                "MANUAL_OR_EXTERNAL_CLOSE",
                None,
                detail,
            )

        detail["reason"] = "PROTECTION_ORDER_STATUS_NOT_FINAL"
        return LiveCloseResolution(
            "LIVE_RESULT_PENDING",
            "LIVE_RESULT_PENDING",
            None,
            detail,
        )

    def _symbol_rules(self, symbol: str) -> SymbolRules:
        info = self.client.get_symbol_info(symbol)
        filters = {item["filterType"]: item for item in info.get("filters", [])}
        price_filter = filters["PRICE_FILTER"]
        qty_filter = filters.get("MARKET_LOT_SIZE") or filters["LOT_SIZE"]
        min_notional_filter = filters.get("MIN_NOTIONAL", {})
        return SymbolRules(
            tick_size=Decimal(str(price_filter["tickSize"])),
            min_qty=Decimal(str(qty_filter["minQty"])),
            max_qty=Decimal(str(qty_filter["maxQty"])),
            step_size=Decimal(str(qty_filter["stepSize"])),
            min_notional=Decimal(str(min_notional_filter.get("notional", "0"))),
        )

    def _get_24h_amplitude(self, symbol: str) -> tuple[Decimal, Decimal, Decimal]:
        ticker = self.client.get_24hr_ticker(symbol)
        high = Decimal(str(ticker["highPrice"]))
        low = Decimal(str(ticker["lowPrice"]))
        if low <= 0 or high < low:
            raise BinanceAPIError(f"Invalid 24h ticker for {symbol}: high={high}, low={low}")
        amplitude_pct = (high - low) / low
        return amplitude_pct, high, low

    def _stop_loss_pct_from_amplitude(self, amplitude_pct: Decimal) -> Decimal:
        raw_pct = amplitude_pct * self.config.stop_loss_amplitude_ratio
        return min(max(raw_pct, self.config.min_stop_loss_pct), self.config.max_stop_loss_pct)

    def _available_balance(self) -> Decimal:
        if self.config.dry_run and not self.client.has_credentials:
            return self.dry_run_account.balance()
        return self.client.get_available_usdt()

    def build_trade_plan(self, symbol: str, entry_price: Decimal) -> TradePlan:
        leverage = self.client.get_max_leverage(symbol)
        balance = self._available_balance()
        rules = self._symbol_rules(symbol)
        amplitude_pct, high_24h_price, low_24h_price = self._get_24h_amplitude(symbol)
        stop_loss_pct = self._stop_loss_pct_from_amplitude(amplitude_pct)
        take_profit_pct = stop_loss_pct * self.config.take_profit_r_multiple
        risk_amount = balance * self.config.risk_balance_fraction

        raw_quantity = risk_amount / (entry_price * stop_loss_pct)
        quantity = floor_to_step(raw_quantity, rules.step_size)
        if quantity <= 0 or quantity > rules.max_qty:
            raise BinanceAPIError(f"Invalid quantity for {symbol}: {quantity}")
        if quantity < rules.min_qty:
            raise BinanceAPIError(f"Risk-sized quantity is below minQty for {symbol}: {quantity} < {rules.min_qty}")

        notional_value = quantity * entry_price
        if rules.min_notional > 0 and notional_value < rules.min_notional:
            raise BinanceAPIError(
                f"Risk-sized notional is below minNotional for {symbol}: {notional_value} < {rules.min_notional}"
            )

        required_margin = notional_value / Decimal(leverage)
        max_allowed_margin = balance * self.config.max_margin_balance_fraction
        if required_margin > max_allowed_margin:
            raise BinanceAPIError(
                f"Insufficient balance for risk-sized order: required_margin={required_margin}, "
                f"max_allowed_margin={max_allowed_margin}, balance={balance}"
            )

        stop_loss_price = floor_to_step(entry_price * (Decimal("1") - stop_loss_pct), rules.tick_size)
        take_profit_price = floor_to_step(entry_price * (Decimal("1") + take_profit_pct), rules.tick_size)

        if stop_loss_price <= 0:
            raise BinanceAPIError(f"Calculated stop loss is non-positive for {symbol}: {stop_loss_price}")
        if take_profit_price <= entry_price:
            raise BinanceAPIError(f"Calculated take profit is not above entry for {symbol}: {take_profit_price}")

        return TradePlan(
            symbol=symbol,
            leverage=leverage,
            quantity=quantity,
            entry_price=floor_to_step(entry_price, rules.tick_size),
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            amplitude_24h_pct=amplitude_pct,
            high_24h_price=high_24h_price,
            low_24h_price=low_24h_price,
            risk_amount=risk_amount,
            notional_value=notional_value,
            required_margin=required_margin,
            balance=balance,
            stop_mode="amplitude",
            risk_reward_ratio=self.config.take_profit_r_multiple,
        )

    def build_amplitude_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None = None,
    ) -> TradePlan:
        if risk_reward_ratio <= 0:
            raise BinanceAPIError(f"Invalid risk/reward ratio for {symbol}: {risk_reward_ratio}")

        leverage = self.client.get_max_leverage(symbol)
        balance = self._available_balance()
        rules = self._symbol_rules(symbol)
        amplitude_pct, high_24h_price, low_24h_price = self._get_24h_amplitude(symbol)
        stop_loss_pct = self._stop_loss_pct_from_amplitude(amplitude_pct)
        normalized_entry = floor_to_step(entry_price, rules.tick_size)
        if normalized_entry <= 0:
            raise BinanceAPIError(
                f"Invalid precision-adjusted N08 entry price for {symbol}: {normalized_entry}"
            )

        stop_loss_price = floor_to_step(
            normalized_entry * (Decimal("1") - stop_loss_pct),
            rules.tick_size,
        )
        if stop_loss_price <= 0 or stop_loss_price >= normalized_entry:
            raise BinanceAPIError(
                f"Invalid precision-adjusted N08 stop for {symbol}: "
                f"entry={normalized_entry}, stop={stop_loss_price}"
            )
        risk_distance = normalized_entry - stop_loss_price
        raw_take_profit = normalized_entry + risk_reward_ratio * risk_distance
        take_profit_price = ceil_to_step(raw_take_profit, rules.tick_size)
        if take_profit_price < raw_take_profit or take_profit_price <= normalized_entry:
            raise BinanceAPIError(
                f"Invalid precision-adjusted N08 take profit for {symbol}: {take_profit_price}"
            )

        target_risk_amount = balance * self.config.risk_balance_fraction
        target_risk_qty = target_risk_amount / (normalized_entry * stop_loss_pct)
        precision_safe_risk_qty = target_risk_amount / risk_distance
        margin_cap_qty = (
            balance
            * self.config.max_margin_balance_fraction
            * Decimal(leverage)
            / normalized_entry
        )
        raw_quantity = min(
            target_risk_qty,
            precision_safe_risk_qty,
            margin_cap_qty,
            rules.max_qty,
        )
        quantity = floor_to_step(raw_quantity, rules.step_size)
        if quantity <= 0:
            raise BinanceAPIError(f"N08 margin-capped quantity is non-positive for {symbol}: {quantity}")
        if quantity < rules.min_qty:
            raise BinanceAPIError(
                f"N08 margin-capped quantity is below minQty for {symbol}: "
                f"{quantity} < {rules.min_qty}"
            )
        if quantity > rules.max_qty:
            raise BinanceAPIError(
                f"N08 margin-capped quantity exceeds maxQty for {symbol}: "
                f"{quantity} > {rules.max_qty}"
            )

        notional_value = quantity * normalized_entry
        if rules.min_notional > 0 and notional_value < rules.min_notional:
            raise BinanceAPIError(
                f"N08 margin-capped notional is below minNotional for {symbol}: "
                f"{notional_value} < {rules.min_notional}"
            )
        required_margin = notional_value / Decimal(leverage)
        max_allowed_margin = balance * self.config.max_margin_balance_fraction
        if required_margin > max_allowed_margin:
            raise BinanceAPIError(
                f"N08 margin-capped plan exceeds margin limit for {symbol}: "
                f"required_margin={required_margin}, max_allowed_margin={max_allowed_margin}"
            )
        actual_risk_amount = quantity * risk_distance
        if actual_risk_amount > target_risk_amount:
            raise BinanceAPIError(
                f"N08 margin-capped risk exceeds target for {symbol}: "
                f"actual={actual_risk_amount}, target={target_risk_amount}"
            )
        risk_capped_by_margin = margin_cap_qty < min(
            target_risk_qty,
            precision_safe_risk_qty,
            rules.max_qty,
        )

        return TradePlan(
            symbol=symbol,
            leverage=leverage,
            quantity=quantity,
            entry_price=normalized_entry,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=(take_profit_price - normalized_entry) / normalized_entry,
            amplitude_24h_pct=amplitude_pct,
            high_24h_price=high_24h_price,
            low_24h_price=low_24h_price,
            risk_amount=target_risk_amount,
            notional_value=notional_value,
            required_margin=required_margin,
            balance=balance,
            stop_mode="amplitude_margin_capped",
            risk_reward_ratio=risk_reward_ratio,
            structure_id=structure_id,
            target_risk_amount=target_risk_amount,
            actual_risk_amount=actual_risk_amount,
            risk_capped_by_margin=risk_capped_by_margin,
            pretrade_quantity=quantity,
        )

    def _n09_protection_prices(
        self,
        symbol: str,
        entry_price: Decimal,
        s1_price: Decimal,
        rules: SymbolRules,
    ) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        take_profit_price = floor_to_step(s1_price, rules.tick_size)
        if take_profit_price != s1_price:
            raise BinanceAPIError(
                f"N09 S1 target is not tick-aligned for {symbol}: {s1_price}"
            )
        if not take_profit_price > entry_price:
            raise BinanceAPIError(
                f"N09 target must be above entry for {symbol}: "
                f"target={take_profit_price}, entry={entry_price}"
            )
        raw_risk_distance = (take_profit_price - entry_price) / Decimal("5")
        raw_stop_loss = entry_price - raw_risk_distance
        stop_loss_price = ceil_to_step(raw_stop_loss, rules.tick_size)
        if not take_profit_price > entry_price > stop_loss_price > 0:
            raise BinanceAPIError(
                f"N09 invalid rounded prices for {symbol}: "
                f"tp={take_profit_price}, entry={entry_price}, sl={stop_loss_price}"
            )
        risk_distance = entry_price - stop_loss_price
        stop_pct = risk_distance / entry_price
        if not Decimal("0.01") <= stop_pct <= Decimal("0.05"):
            raise BinanceAPIError(
                f"N09_STOP_PCT_OUT_OF_RANGE:{symbol}:stop_pct={stop_pct}"
            )
        actual_r_multiple = (take_profit_price - entry_price) / risk_distance
        if actual_r_multiple < Decimal("5"):
            raise BinanceAPIError(
                f"N09 rounded risk/reward below 5 for {symbol}: {actual_r_multiple}"
            )
        return stop_loss_price, take_profit_price, stop_pct, actual_r_multiple

    def build_s1_target_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        s1_price: Decimal,
        structure_id: str | None = None,
    ) -> TradePlan:
        leverage = self.client.get_max_leverage(symbol)
        balance = self._available_balance()
        rules = self._symbol_rules(symbol)
        normalized_entry = floor_to_step(entry_price, rules.tick_size)
        if normalized_entry <= 0:
            raise BinanceAPIError(f"N09 invalid entry price for {symbol}: {normalized_entry}")
        stop_loss_price, take_profit_price, stop_pct, actual_r_multiple = (
            self._n09_protection_prices(
                symbol,
                normalized_entry,
                s1_price,
                rules,
            )
        )
        risk_distance = normalized_entry - stop_loss_price
        target_risk_amount = balance * self.config.risk_balance_fraction
        target_risk_qty = target_risk_amount / risk_distance
        margin_cap_qty = (
            balance
            * self.config.max_margin_balance_fraction
            * Decimal(leverage)
            / normalized_entry
        )
        quantity = floor_to_step(
            min(target_risk_qty, margin_cap_qty, rules.max_qty),
            rules.step_size,
        )
        self._validate_margin_capped_quantity(
            TradePlan(
                symbol=symbol,
                leverage=leverage,
                quantity=quantity,
                entry_price=normalized_entry,
                stop_loss_price=stop_loss_price,
                take_profit_price=take_profit_price,
                stop_loss_pct=stop_pct,
                take_profit_pct=(take_profit_price - normalized_entry) / normalized_entry,
                amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("0"),
                low_24h_price=Decimal("0"),
                risk_amount=target_risk_amount,
                notional_value=quantity * normalized_entry,
                required_margin=(quantity * normalized_entry) / Decimal(leverage),
                balance=balance,
                stop_mode="s1_target_margin_capped",
                structure_id=structure_id,
                structure_target_price=take_profit_price,
                target_risk_amount=target_risk_amount,
            ),
            normalized_entry,
            quantity,
            rules,
            "pretrade",
        )
        notional_value = quantity * normalized_entry
        required_margin = notional_value / Decimal(leverage)
        actual_risk_amount = quantity * risk_distance
        if actual_risk_amount > target_risk_amount:
            raise BinanceAPIError(
                f"N09 pretrade risk exceeds target for {symbol}: "
                f"actual={actual_risk_amount}, target={target_risk_amount}"
            )
        if required_margin > balance * self.config.max_margin_balance_fraction:
            raise BinanceAPIError(
                f"N09 pretrade margin exceeds limit for {symbol}: {required_margin}"
            )
        return TradePlan(
            symbol=symbol,
            leverage=leverage,
            quantity=quantity,
            entry_price=normalized_entry,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            stop_loss_pct=stop_pct,
            take_profit_pct=(take_profit_price - normalized_entry) / normalized_entry,
            amplitude_24h_pct=Decimal("0"),
            high_24h_price=Decimal("0"),
            low_24h_price=Decimal("0"),
            risk_amount=target_risk_amount,
            notional_value=notional_value,
            required_margin=required_margin,
            balance=balance,
            stop_mode="s1_target_margin_capped",
            risk_reward_ratio=actual_r_multiple,
            structure_id=structure_id,
            structure_target_price=take_profit_price,
            target_risk_amount=target_risk_amount,
            actual_risk_amount=actual_risk_amount,
            risk_capped_by_margin=margin_cap_qty < min(target_risk_qty, rules.max_qty),
            pretrade_quantity=quantity,
        )

    def build_structure_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        structure_stop_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None = None,
    ) -> TradePlan:
        if risk_reward_ratio <= 0:
            raise BinanceAPIError(f"Invalid risk/reward ratio for {symbol}: {risk_reward_ratio}")
        if structure_stop_price >= entry_price:
            raise BinanceAPIError(
                f"Invalid structure risk distance for {symbol}: entry={entry_price}, P1={structure_stop_price}"
            )

        leverage = self.client.get_max_leverage(symbol)
        balance = self._available_balance()
        rules = self._symbol_rules(symbol)
        normalized_entry = floor_to_step(entry_price, rules.tick_size)
        normalized_stop = floor_to_step(structure_stop_price, rules.tick_size)
        if normalized_entry <= 0 or normalized_stop <= 0 or normalized_stop >= normalized_entry:
            raise BinanceAPIError(
                f"Invalid precision-adjusted structure prices for {symbol}: "
                f"entry={normalized_entry}, P1={normalized_stop}"
            )

        risk_distance = normalized_entry - normalized_stop
        raw_take_profit = normalized_entry + risk_reward_ratio * risk_distance
        take_profit_price = ceil_to_step(raw_take_profit, rules.tick_size)
        if take_profit_price <= normalized_entry:
            raise BinanceAPIError(
                f"Invalid precision-adjusted structure take profit for {symbol}: {take_profit_price}"
            )

        risk_amount = balance * self.config.risk_balance_fraction
        raw_quantity = risk_amount / risk_distance
        quantity = floor_to_step(raw_quantity, rules.step_size)
        if quantity <= 0 or quantity > rules.max_qty:
            raise BinanceAPIError(f"Invalid structure risk-sized quantity for {symbol}: {quantity}")
        if quantity < rules.min_qty:
            raise BinanceAPIError(
                f"Structure risk-sized quantity is below minQty for {symbol}: {quantity} < {rules.min_qty}"
            )

        notional_value = quantity * normalized_entry
        if rules.min_notional > 0 and notional_value < rules.min_notional:
            raise BinanceAPIError(
                f"Structure risk-sized notional is below minNotional for {symbol}: "
                f"{notional_value} < {rules.min_notional}"
            )

        required_margin = notional_value / Decimal(leverage)
        max_allowed_margin = balance * self.config.max_margin_balance_fraction
        if required_margin > max_allowed_margin:
            raise BinanceAPIError(
                f"Insufficient balance for structure risk-sized order: required_margin={required_margin}, "
                f"max_allowed_margin={max_allowed_margin}, balance={balance}"
            )

        stop_loss_pct = risk_distance / normalized_entry
        take_profit_pct = (take_profit_price - normalized_entry) / normalized_entry
        return TradePlan(
            symbol=symbol,
            leverage=leverage,
            quantity=quantity,
            entry_price=normalized_entry,
            stop_loss_price=normalized_stop,
            take_profit_price=take_profit_price,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            amplitude_24h_pct=Decimal("0"),
            high_24h_price=Decimal("0"),
            low_24h_price=Decimal("0"),
            risk_amount=risk_amount,
            notional_value=notional_value,
            required_margin=required_margin,
            balance=balance,
            stop_mode="structure_p1",
            risk_reward_ratio=risk_reward_ratio,
            structure_id=structure_id,
            structure_stop_price=structure_stop_price,
            target_risk_amount=risk_amount,
            actual_risk_amount=quantity * risk_distance,
        )

    def build_margin_capped_structure_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        structure_stop_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None = None,
    ) -> TradePlan:
        if risk_reward_ratio <= 0:
            raise BinanceAPIError(f"Invalid risk/reward ratio for {symbol}: {risk_reward_ratio}")
        if structure_stop_price >= entry_price:
            raise BinanceAPIError(
                f"Invalid structure risk distance for {symbol}: entry={entry_price}, P1={structure_stop_price}"
            )

        leverage = self.client.get_max_leverage(symbol)
        balance = self._available_balance()
        rules = self._symbol_rules(symbol)
        normalized_entry = floor_to_step(entry_price, rules.tick_size)
        normalized_stop = floor_to_step(structure_stop_price, rules.tick_size)
        if normalized_entry <= 0 or normalized_stop <= 0 or normalized_stop >= normalized_entry:
            raise BinanceAPIError(
                f"Invalid precision-adjusted structure prices for {symbol}: "
                f"entry={normalized_entry}, P1={normalized_stop}"
            )

        risk_distance = normalized_entry - normalized_stop
        raw_take_profit = normalized_entry + risk_reward_ratio * risk_distance
        take_profit_price = ceil_to_step(raw_take_profit, rules.tick_size)
        if take_profit_price < raw_take_profit or take_profit_price <= normalized_entry:
            raise BinanceAPIError(
                f"Invalid precision-adjusted structure take profit for {symbol}: {take_profit_price}"
            )

        target_risk_amount = balance * self.config.risk_balance_fraction
        target_risk_qty = target_risk_amount / risk_distance
        margin_cap_qty = (
            balance
            * self.config.max_margin_balance_fraction
            * Decimal(leverage)
            / normalized_entry
        )
        raw_quantity = min(target_risk_qty, margin_cap_qty, rules.max_qty)
        quantity = floor_to_step(raw_quantity, rules.step_size)
        if quantity <= 0:
            raise BinanceAPIError(f"Margin-capped structure quantity is non-positive for {symbol}: {quantity}")
        if quantity < rules.min_qty:
            raise BinanceAPIError(
                f"Margin-capped structure quantity is below minQty for {symbol}: "
                f"{quantity} < {rules.min_qty}"
            )
        if quantity > rules.max_qty:
            raise BinanceAPIError(
                f"Margin-capped structure quantity exceeds maxQty for {symbol}: "
                f"{quantity} > {rules.max_qty}"
            )

        notional_value = quantity * normalized_entry
        if rules.min_notional > 0 and notional_value < rules.min_notional:
            raise BinanceAPIError(
                f"Margin-capped structure notional is below minNotional for {symbol}: "
                f"{notional_value} < {rules.min_notional}"
            )

        required_margin = notional_value / Decimal(leverage)
        max_allowed_margin = balance * self.config.max_margin_balance_fraction
        if required_margin > max_allowed_margin:
            raise BinanceAPIError(
                f"Margin-capped structure plan exceeds margin limit for {symbol}: "
                f"required_margin={required_margin}, max_allowed_margin={max_allowed_margin}"
            )
        actual_risk_amount = quantity * risk_distance
        if actual_risk_amount > target_risk_amount:
            raise BinanceAPIError(
                f"Margin-capped structure risk exceeds target for {symbol}: "
                f"actual={actual_risk_amount}, target={target_risk_amount}"
            )
        risk_capped_by_margin = margin_cap_qty < min(target_risk_qty, rules.max_qty)

        return TradePlan(
            symbol=symbol,
            leverage=leverage,
            quantity=quantity,
            entry_price=normalized_entry,
            stop_loss_price=normalized_stop,
            take_profit_price=take_profit_price,
            stop_loss_pct=risk_distance / normalized_entry,
            take_profit_pct=(take_profit_price - normalized_entry) / normalized_entry,
            amplitude_24h_pct=Decimal("0"),
            high_24h_price=Decimal("0"),
            low_24h_price=Decimal("0"),
            risk_amount=target_risk_amount,
            notional_value=notional_value,
            required_margin=required_margin,
            balance=balance,
            stop_mode="structure_p1_margin_capped",
            risk_reward_ratio=risk_reward_ratio,
            structure_id=structure_id,
            structure_stop_price=structure_stop_price,
            target_risk_amount=target_risk_amount,
            actual_risk_amount=actual_risk_amount,
            risk_capped_by_margin=risk_capped_by_margin,
            pretrade_quantity=quantity,
        )

    def build_sweep_low_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        sweep_low_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None = None,
        *,
        entry_min_price: Decimal,
        entry_max_price: Decimal,
    ) -> TradePlan:
        if risk_reward_ratio <= 0:
            raise BinanceAPIError(f"Invalid N10 risk/reward ratio for {symbol}: {risk_reward_ratio}")
        if (
            entry_min_price <= 0
            or entry_max_price < entry_min_price
            or entry_price < entry_min_price
            or entry_price > entry_max_price
        ):
            raise BinanceAPIError(
                f"Invalid N10 entry range for {symbol}: entry={entry_price}, "
                f"entry_min={entry_min_price}, entry_max={entry_max_price}"
            )
        leverage = self.client.get_max_leverage(symbol)
        balance = self._available_balance()
        rules = self._symbol_rules(symbol)
        normalized_entry = floor_to_step(entry_price, rules.tick_size)
        normalized_sweep_low = floor_to_step(sweep_low_price, rules.tick_size)
        stop_loss_price = normalized_sweep_low - rules.tick_size
        if (
            normalized_entry <= 0
            or normalized_sweep_low <= 0
            or stop_loss_price <= 0
            or stop_loss_price >= normalized_entry
        ):
            raise BinanceAPIError(
                f"Invalid N10 precision-adjusted prices for {symbol}: "
                f"entry={normalized_entry}, sweep_low={normalized_sweep_low}, stop={stop_loss_price}"
            )
        risk_distance = normalized_entry - stop_loss_price
        stop_loss_pct = risk_distance / normalized_entry
        if not self.config.min_stop_loss_pct <= stop_loss_pct <= self.config.max_stop_loss_pct:
            raise BinanceAPIError(
                f"N10_STOP_PCT_OUT_OF_RANGE:{symbol}:stop_pct={stop_loss_pct}"
            )
        raw_take_profit = normalized_entry + risk_reward_ratio * risk_distance
        take_profit_price = ceil_to_step(raw_take_profit, rules.tick_size)
        if take_profit_price < raw_take_profit or take_profit_price <= normalized_entry:
            raise BinanceAPIError(
                f"Invalid N10 precision-adjusted take profit for {symbol}: {take_profit_price}"
            )

        target_risk_amount = balance * self.config.risk_balance_fraction
        target_risk_qty = target_risk_amount / risk_distance
        margin_cap_qty = (
            balance
            * self.config.max_margin_balance_fraction
            * Decimal(leverage)
            / normalized_entry
        )
        quantity = floor_to_step(
            min(target_risk_qty, margin_cap_qty, rules.max_qty),
            rules.step_size,
        )
        provisional = TradePlan(
            symbol=symbol,
            leverage=leverage,
            quantity=quantity,
            entry_price=normalized_entry,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=(take_profit_price - normalized_entry) / normalized_entry,
            amplitude_24h_pct=Decimal("0"),
            high_24h_price=Decimal("0"),
            low_24h_price=Decimal("0"),
            risk_amount=target_risk_amount,
            notional_value=quantity * normalized_entry,
            required_margin=(quantity * normalized_entry) / Decimal(leverage),
            balance=balance,
            stop_mode="sweep_low_tick_margin_capped",
            risk_reward_ratio=risk_reward_ratio,
            structure_id=structure_id,
            structure_stop_price=stop_loss_price,
            target_risk_amount=target_risk_amount,
            entry_min_price=entry_min_price,
            entry_max_price=entry_max_price,
        )
        self._validate_margin_capped_quantity(
            provisional,
            normalized_entry,
            quantity,
            rules,
            "pretrade",
        )
        actual_risk_amount = quantity * risk_distance
        required_margin = provisional.required_margin
        if actual_risk_amount > target_risk_amount:
            raise BinanceAPIError(
                f"N10 pretrade risk exceeds target for {symbol}: "
                f"actual={actual_risk_amount}, target={target_risk_amount}"
            )
        if required_margin > balance * self.config.max_margin_balance_fraction:
            raise BinanceAPIError(
                f"N10 pretrade margin exceeds limit for {symbol}: {required_margin}"
            )
        return replace(
            provisional,
            actual_risk_amount=actual_risk_amount,
            risk_capped_by_margin=margin_cap_qty < min(target_risk_qty, rules.max_qty),
            pretrade_quantity=quantity,
        )

    def build_breakout_retest_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        retest_low_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None = None,
        *,
        entry_min_price: Decimal,
        entry_max_price: Decimal,
    ) -> TradePlan:
        return self._build_retest_margin_capped_trade_plan(
            symbol,
            entry_price,
            retest_low_price,
            risk_reward_ratio,
            structure_id,
            entry_min_price=entry_min_price,
            entry_max_price=entry_max_price,
            strategy_label="N11",
            stop_mode="breakout_retest_margin_capped",
        )

    def build_relative_strength_pullback_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        pullback_low_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None = None,
        *,
        entry_min_price: Decimal,
        entry_max_price: Decimal,
    ) -> TradePlan:
        return self._build_retest_margin_capped_trade_plan(
            symbol,
            entry_price,
            pullback_low_price,
            risk_reward_ratio,
            structure_id,
            entry_min_price=entry_min_price,
            entry_max_price=entry_max_price,
            strategy_label="N12",
            stop_mode="relative_strength_pullback_margin_capped",
        )

    def build_vwap_rotation_margin_capped_trade_plan(
        self, symbol: str, entry_price: Decimal, pullback_low_price: Decimal,
        risk_reward_ratio: Decimal, structure_id: str | None = None, *,
        entry_min_price: Decimal, entry_max_price: Decimal,
    ) -> TradePlan:
        return self._build_retest_margin_capped_trade_plan(
            symbol, entry_price, pullback_low_price, risk_reward_ratio, structure_id,
            entry_min_price=entry_min_price, entry_max_price=entry_max_price,
            strategy_label="N13", stop_mode="vwap_rotation_margin_capped",
        )

    def build_sell_pressure_decay_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        reaction_low_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None = None,
        *,
        entry_min_price: Decimal,
        entry_max_price: Decimal,
    ) -> TradePlan:
        return self._build_retest_margin_capped_trade_plan(
            symbol,
            entry_price,
            reaction_low_price,
            risk_reward_ratio,
            structure_id,
            entry_min_price=entry_min_price,
            entry_max_price=entry_max_price,
            strategy_label="N14",
            stop_mode="sell_pressure_decay_margin_capped",
        )

    def build_breadth_recovery_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        recovery_low_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None = None,
        *,
        entry_min_price: Decimal,
        entry_max_price: Decimal,
    ) -> TradePlan:
        return self._build_retest_margin_capped_trade_plan(
            symbol,
            entry_price,
            recovery_low_price,
            risk_reward_ratio,
            structure_id,
            entry_min_price=entry_min_price,
            entry_max_price=entry_max_price,
            strategy_label="N15",
            stop_mode="breadth_recovery_margin_capped",
        )

    def build_trend_support_continuation_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        support_low_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None = None,
        *,
        entry_min_price: Decimal,
        entry_max_price: Decimal,
    ) -> TradePlan:
        if type(risk_reward_ratio) is not Decimal or risk_reward_ratio != Decimal("5"):
            raise BinanceAPIError(
                f"Invalid N16 risk/reward ratio for {symbol}: {risk_reward_ratio}"
            )
        return self._build_retest_margin_capped_trade_plan(
            symbol,
            entry_price,
            support_low_price,
            risk_reward_ratio,
            structure_id,
            entry_min_price=entry_min_price,
            entry_max_price=entry_max_price,
            strategy_label="N16",
            stop_mode="trend_support_continuation_margin_capped",
        )

    def build_range_support_absorption_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        touch_low_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None = None,
        *,
        entry_min_price: Decimal,
        entry_max_price: Decimal,
    ) -> TradePlan:
        if type(risk_reward_ratio) is not Decimal or risk_reward_ratio != Decimal("5"):
            raise BinanceAPIError(
                f"Invalid N17 risk/reward ratio for {symbol}: {risk_reward_ratio}"
            )
        return self._build_retest_margin_capped_trade_plan(
            symbol,
            entry_price,
            touch_low_price,
            risk_reward_ratio,
            structure_id,
            entry_min_price=entry_min_price,
            entry_max_price=entry_max_price,
            strategy_label="N17",
            stop_mode=_N17_STOP_MODE,
        )

    def build_staircase_exhaustion_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        exhaustion_low_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None = None,
        *,
        entry_min_price: Decimal,
        entry_max_price: Decimal,
    ) -> TradePlan:
        if type(risk_reward_ratio) is not Decimal or risk_reward_ratio != Decimal("5"):
            raise BinanceAPIError(
                f"Invalid N19 risk/reward ratio for {symbol}: {risk_reward_ratio}"
            )
        return self._build_retest_margin_capped_trade_plan(
            symbol,
            entry_price,
            exhaustion_low_price,
            risk_reward_ratio,
            structure_id,
            entry_min_price=entry_min_price,
            entry_max_price=entry_max_price,
            strategy_label="N19",
            stop_mode=_N19_STOP_MODE,
        )

    def build_ascending_triangle_breakout_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        higher_low_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None = None,
        *,
        entry_min_price: Decimal,
        entry_max_price: Decimal,
    ) -> TradePlan:
        if type(risk_reward_ratio) is not Decimal or risk_reward_ratio != Decimal("5"):
            raise BinanceAPIError(
                f"Invalid N18 risk/reward ratio for {symbol}: {risk_reward_ratio}"
            )
        return self._build_retest_margin_capped_trade_plan(
            symbol,
            entry_price,
            higher_low_price,
            risk_reward_ratio,
            structure_id,
            entry_min_price=entry_min_price,
            entry_max_price=entry_max_price,
            strategy_label="N18",
            stop_mode=_N18_STOP_MODE,
        )

    def build_n20_relative_strength_recovery_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        p_price: Decimal,
        risk_reward_ratio: Decimal,
        *,
        structure_id: str,
        entry_min_price: Decimal,
        entry_max_price: Decimal,
    ) -> TradePlan:
        if type(risk_reward_ratio) is not Decimal or risk_reward_ratio != Decimal("5"):
            raise BinanceAPIError(
                f"Invalid N20 risk/reward ratio for {symbol}: {risk_reward_ratio}"
            )
        return self._build_retest_margin_capped_trade_plan(
            symbol,
            entry_price,
            p_price,
            risk_reward_ratio,
            structure_id=structure_id,
            entry_min_price=entry_min_price,
            entry_max_price=entry_max_price,
            strategy_label="N20",
            stop_mode=_N20_STOP_MODE,
        )

    def build_micro_observation_margin_capped_trade_plan(
        self,
        strategy_id: str,
        symbol: str,
        entry_price: Decimal,
        structural_low: Decimal,
        risk_reward_ratio: Decimal,
        *,
        structure_id: str,
        entry_min_price: Decimal,
        entry_max_price: Decimal,
    ) -> TradePlan:
        if strategy_id not in _MICRO_STRATEGY_IDS:
            raise BinanceAPIError(f"Invalid micro strategy identity: {strategy_id}")
        if type(risk_reward_ratio) is not Decimal or risk_reward_ratio != Decimal("5"):
            raise BinanceAPIError(
                f"Invalid {strategy_id} risk/reward ratio for {symbol}: {risk_reward_ratio}"
            )
        return self._build_retest_margin_capped_trade_plan(
            symbol,
            entry_price,
            structural_low,
            risk_reward_ratio,
            structure_id=structure_id,
            entry_min_price=entry_min_price,
            entry_max_price=entry_max_price,
            strategy_label=strategy_id,
            stop_mode=_MICRO_STOP_MODE,
        )

    def _build_retest_margin_capped_trade_plan(
        self,
        symbol: str,
        entry_price: Decimal,
        retest_low_price: Decimal,
        risk_reward_ratio: Decimal,
        structure_id: str | None,
        *,
        entry_min_price: Decimal,
        entry_max_price: Decimal,
        strategy_label: str,
        stop_mode: str,
    ) -> TradePlan:
        if risk_reward_ratio <= 0:
            raise BinanceAPIError(
                f"Invalid {strategy_label} risk/reward ratio for {symbol}: {risk_reward_ratio}"
            )
        if (
            entry_min_price <= 0
            or entry_max_price < entry_min_price
            or entry_price < entry_min_price
            or entry_price > entry_max_price
        ):
            raise BinanceAPIError(
                f"Invalid {strategy_label} entry range for {symbol}: entry={entry_price}, "
                f"entry_min={entry_min_price}, entry_max={entry_max_price}"
            )
        leverage = self.client.get_max_leverage(symbol)
        balance = self._available_balance()
        rules = self._symbol_rules(symbol)
        normalized_entry = floor_to_step(entry_price, rules.tick_size)
        if normalized_entry < entry_min_price or normalized_entry > entry_max_price:
            raise BinanceAPIError(
                f"Invalid {strategy_label} precision-adjusted entry range for {symbol}: "
                f"entry={normalized_entry}, entry_min={entry_min_price}, "
                f"entry_max={entry_max_price}"
            )
        normalized_retest_low = floor_to_step(retest_low_price, rules.tick_size)
        structural_stop_price = normalized_retest_low - rules.tick_size
        minimum_distance_stop = floor_to_step(
            normalized_entry * (Decimal("1") - self.config.min_stop_loss_pct),
            rules.tick_size,
        )
        stop_loss_price = min(structural_stop_price, minimum_distance_stop)
        if (
            normalized_entry <= 0
            or normalized_retest_low <= 0
            or structural_stop_price <= 0
            or stop_loss_price <= 0
            or stop_loss_price >= normalized_entry
        ):
            raise BinanceAPIError(
                f"Invalid {strategy_label} precision-adjusted prices for {symbol}: "
                f"entry={normalized_entry}, retest_low={normalized_retest_low}, "
                f"stop={stop_loss_price}"
            )
        risk_distance = normalized_entry - stop_loss_price
        stop_loss_pct = risk_distance / normalized_entry
        if not self.config.min_stop_loss_pct <= stop_loss_pct <= self.config.max_stop_loss_pct:
            raise BinanceAPIError(
                f"{strategy_label}_STOP_PCT_OUT_OF_RANGE:{symbol}:stop_pct={stop_loss_pct}"
            )
        raw_take_profit = normalized_entry + risk_reward_ratio * risk_distance
        take_profit_price = ceil_to_step(raw_take_profit, rules.tick_size)
        if take_profit_price < raw_take_profit or take_profit_price <= normalized_entry:
            raise BinanceAPIError(
                f"Invalid {strategy_label} precision-adjusted take profit for {symbol}: "
                f"{take_profit_price}"
            )

        target_risk_amount = balance * self.config.risk_balance_fraction
        target_risk_qty = target_risk_amount / risk_distance
        margin_cap_qty = (
            balance
            * self.config.max_margin_balance_fraction
            * Decimal(leverage)
            / normalized_entry
        )
        quantity = floor_to_step(
            min(target_risk_qty, margin_cap_qty, rules.max_qty),
            rules.step_size,
        )
        provisional = TradePlan(
            symbol=symbol,
            leverage=leverage,
            quantity=quantity,
            entry_price=normalized_entry,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=(take_profit_price - normalized_entry) / normalized_entry,
            amplitude_24h_pct=Decimal("0"),
            high_24h_price=Decimal("0"),
            low_24h_price=Decimal("0"),
            risk_amount=target_risk_amount,
            notional_value=quantity * normalized_entry,
            required_margin=(quantity * normalized_entry) / Decimal(leverage),
            balance=balance,
            stop_mode=stop_mode,
            risk_reward_ratio=risk_reward_ratio,
            structure_id=structure_id,
            structure_stop_price=structural_stop_price,
            target_risk_amount=target_risk_amount,
            entry_min_price=entry_min_price,
            entry_max_price=entry_max_price,
            structure_context=(
                {
                    "strategy_id": strategy_label,
                    "rule_version": f"{strategy_label}_V1",
                }
                if stop_mode == _MICRO_STOP_MODE
                else None
            ),
        )
        self._validate_margin_capped_quantity(
            provisional,
            normalized_entry,
            quantity,
            rules,
            "pretrade",
        )
        actual_risk_amount = quantity * risk_distance
        required_margin = provisional.required_margin
        if actual_risk_amount > target_risk_amount:
            raise BinanceAPIError(
                f"{strategy_label} pretrade risk exceeds target for {symbol}: "
                f"actual={actual_risk_amount}, target={target_risk_amount}"
            )
        if required_margin > balance * self.config.max_margin_balance_fraction:
            raise BinanceAPIError(
                f"{strategy_label} pretrade margin exceeds limit for {symbol}: {required_margin}"
            )
        return replace(
            provisional,
            actual_risk_amount=actual_risk_amount,
            risk_capped_by_margin=margin_cap_qty < min(target_risk_qty, rules.max_qty),
            pretrade_quantity=quantity,
        )

    def open_long_with_protection(self, symbol: str, current_price: Decimal) -> PositionState:
        plan = self.build_trade_plan(symbol, current_price)
        return self.open_long_plan_with_protection(plan)

    def _execution_price_from_payload(self, payload: dict[str, Any]) -> Decimal | None:
        try:
            average_price = Decimal(str(payload.get("avgPrice", "0")))
        except ArithmeticError:
            average_price = Decimal("0")
        if average_price > 0:
            return average_price

        try:
            executed_quantity = Decimal(str(payload.get("executedQty", "0")))
            cumulative_quote = Decimal(str(payload.get("cumQuote", "0")))
        except ArithmeticError:
            return None
        if executed_quantity > 0 and cumulative_quote > 0:
            return cumulative_quote / executed_quantity
        return None

    def _confirm_actual_entry_price(
        self,
        symbol: str,
        open_order: dict[str, Any],
        estimated_entry: Decimal,
        expected_client_order_id: str,
    ) -> tuple[Decimal, str]:
        if self.config.dry_run:
            return estimated_entry, "DRY_RUN_PLAN"

        response_price = self._execution_price_from_payload(open_order)
        if response_price is not None:
            return response_price, "MARKET_ORDER_RESPONSE"

        order_id = open_order.get("orderId")
        if order_id is not None:
            try:
                order = self.client.get_order(symbol, order_id=order_id)
                self._validate_market_order_response(
                    symbol,
                    order,
                    expected_client_order_id,
                )
                queried_price = self._execution_price_from_payload(order)
                if queried_price is not None:
                    return queried_price, "ORDER_QUERY"
            except Exception as exc:
                self.logger.warning("Failed to confirm market fill from order query | symbol=%s error=%s", symbol, exc)

        try:
            return self.client.get_long_position_entry_price(symbol), "POSITION_QUERY"
        except Exception as exc:
            raise BinanceAPIError(f"Unable to confirm actual market entry price for {symbol}: {exc}") from exc

    def _margin_capped_strategy_label(self, plan: TradePlan) -> str:
        if plan.stop_mode == "structure_p1_margin_capped":
            return "N07"
        if plan.stop_mode == "amplitude_margin_capped":
            return "N08"
        if plan.stop_mode == "s1_target_margin_capped":
            return "N09"
        if plan.stop_mode == "sweep_low_tick_margin_capped":
            return "N10"
        if plan.stop_mode == "breakout_retest_margin_capped":
            return "N11"
        if plan.stop_mode == "relative_strength_pullback_margin_capped":
            return "N12"
        if plan.stop_mode == "vwap_rotation_margin_capped":
            return "N13"
        if plan.stop_mode == "sell_pressure_decay_margin_capped":
            return "N14"
        if plan.stop_mode == "breadth_recovery_margin_capped":
            return "N15"
        if plan.stop_mode == "trend_support_continuation_margin_capped":
            return "N16"
        if plan.stop_mode == _N17_STOP_MODE:
            return "N17"
        if plan.stop_mode == _N18_STOP_MODE:
            return "N18"
        if plan.stop_mode == _N19_STOP_MODE:
            return "N19"
        if plan.stop_mode == _N20_STOP_MODE:
            return "N20"
        if plan.stop_mode == _MICRO_STOP_MODE:
            context = plan.structure_context
            strategy_id = context.get("strategy_id") if type(context) is dict else None
            if strategy_id in _MICRO_STRATEGY_IDS:
                return strategy_id
        raise BinanceAPIError(f"Unsupported margin-capped stop mode: {plan.stop_mode}")

    def _confirm_margin_capped_position_quantity(
        self,
        plan: TradePlan,
        symbol: str,
        dry_run_quantity: Decimal,
    ) -> Decimal:
        strategy_label = self._margin_capped_strategy_label(plan)
        if self.config.dry_run:
            return dry_run_quantity
        try:
            quantity = self.client.get_long_position_quantity(symbol)
        except Exception as exc:
            raise BinanceAPIError(
                f"Unable to confirm {strategy_label} long position quantity for {symbol}: {exc}"
            ) from exc
        if quantity <= 0:
            raise BinanceAPIError(
                f"Confirmed {strategy_label} long position quantity is non-positive for "
                f"{symbol}: {quantity}"
            )
        return quantity

    def _validate_margin_capped_quantity(
        self,
        plan: TradePlan,
        actual_entry: Decimal,
        quantity: Decimal,
        rules: SymbolRules,
        label: str,
    ) -> None:
        strategy_label = self._margin_capped_strategy_label(plan)
        if quantity <= 0:
            raise BinanceAPIError(
                f"{strategy_label} {label} quantity is non-positive for "
                f"{plan.symbol}: {quantity}"
            )
        if quantity < rules.min_qty:
            raise BinanceAPIError(
                f"{strategy_label} {label} quantity is below minQty for {plan.symbol}: "
                f"{quantity} < {rules.min_qty}"
            )
        if quantity > rules.max_qty:
            raise BinanceAPIError(
                f"{strategy_label} {label} quantity exceeds maxQty for {plan.symbol}: "
                f"{quantity} > {rules.max_qty}"
            )
        notional_value = quantity * actual_entry
        if rules.min_notional > 0 and notional_value < rules.min_notional:
            raise BinanceAPIError(
                f"{strategy_label} {label} notional is below minNotional for {plan.symbol}: "
                f"{notional_value} < {rules.min_notional}"
            )

    def _margin_capped_safe_quantity_after_fill(
        self,
        plan: TradePlan,
        actual_entry: Decimal,
        executed_quantity: Decimal,
    ) -> Decimal:
        strategy_label = self._margin_capped_strategy_label(plan)
        target_risk_amount = plan.target_risk_amount
        if target_risk_amount is None or target_risk_amount <= 0:
            raise BinanceAPIError(
                f"Missing {strategy_label} target risk amount for {plan.symbol}"
            )
        rules = self._symbol_rules(plan.symbol)
        if plan.stop_mode in {
            "breakout_retest_margin_capped",
            "relative_strength_pullback_margin_capped",
            "vwap_rotation_margin_capped",
            "sell_pressure_decay_margin_capped",
            "breadth_recovery_margin_capped",
            "trend_support_continuation_margin_capped",
            _N17_STOP_MODE,
            _N18_STOP_MODE,
            _N19_STOP_MODE,
            _N20_STOP_MODE,
            _MICRO_STOP_MODE,
        }:
            structural_stop = plan.structure_stop_price or plan.stop_loss_price
            minimum_distance_stop = floor_to_step(
                actual_entry * (Decimal("1") - self.config.min_stop_loss_pct),
                rules.tick_size,
            )
            stop_loss_price = floor_to_step(
                min(structural_stop, minimum_distance_stop),
                rules.tick_size,
            )
        elif plan.stop_mode in {
            "structure_p1_margin_capped",
            "sweep_low_tick_margin_capped",
        }:
            stop_reference = plan.structure_stop_price or plan.stop_loss_price
            stop_loss_price = floor_to_step(stop_reference, rules.tick_size)
        elif plan.stop_mode == "s1_target_margin_capped":
            target_price = plan.structure_target_price or plan.take_profit_price
            stop_loss_price, _, _, _ = self._n09_protection_prices(
                plan.symbol,
                actual_entry,
                target_price,
                rules,
            )
        else:
            stop_loss_price = floor_to_step(
                actual_entry * (Decimal("1") - plan.stop_loss_pct),
                rules.tick_size,
            )
        if stop_loss_price <= 0 or stop_loss_price >= actual_entry:
            raise BinanceAPIError(
                f"Invalid {strategy_label} protection distance after fill for {plan.symbol}: "
                f"entry={actual_entry}, stop={stop_loss_price}"
            )

        risk_distance = actual_entry - stop_loss_price
        max_allowed_margin = plan.balance * self.config.max_margin_balance_fraction
        target_risk_qty = target_risk_amount / risk_distance
        margin_cap_qty = max_allowed_margin * Decimal(plan.leverage) / actual_entry
        safe_quantity = floor_to_step(
            min(executed_quantity, target_risk_qty, margin_cap_qty, rules.max_qty),
            rules.step_size,
        )
        self._validate_margin_capped_quantity(
            plan,
            actual_entry,
            safe_quantity,
            rules,
            "post-fill safe",
        )
        return safe_quantity

    def _assert_margin_capped_post_fill_limits(self, plan: TradePlan) -> None:
        strategy_label = self._margin_capped_strategy_label(plan)
        target_risk_amount = plan.target_risk_amount
        if target_risk_amount is None or plan.actual_risk_amount is None:
            raise BinanceAPIError(
                f"Missing {strategy_label} post-fill risk audit for {plan.symbol}"
            )
        max_allowed_margin = plan.balance * self.config.max_margin_balance_fraction
        if plan.actual_risk_amount > target_risk_amount:
            raise BinanceAPIError(
                f"{strategy_label} post-fill risk exceeds target for {plan.symbol}: "
                f"actual={plan.actual_risk_amount}, target={target_risk_amount}"
            )
        if plan.required_margin > max_allowed_margin:
            raise BinanceAPIError(
                f"{strategy_label} post-fill margin exceeds limit for {plan.symbol}: "
                f"required={plan.required_margin}, max={max_allowed_margin}"
            )

    def _validate_margin_capped_reduction_response(
        self,
        plan: TradePlan,
        symbol: str,
        response: Any,
        requested_quantity: Decimal,
    ) -> None:
        strategy_label = self._margin_capped_strategy_label(plan)
        if self.config.dry_run:
            return
        if not isinstance(response, dict):
            raise BinanceAPIError(
                f"{strategy_label} reduction returned invalid response for {symbol}: {response}"
            )
        status = str(response.get("status", "")).upper()
        try:
            executed_quantity = Decimal(str(response.get("executedQty", "0")))
        except ArithmeticError:
            executed_quantity = Decimal("0")
        if status != "FILLED" or executed_quantity < requested_quantity:
            raise BinanceAPIError(
                f"{strategy_label} reduction was not fully filled for {symbol}: "
                f"status={status or '-'}, "
                f"executed={executed_quantity}, requested={requested_quantity}"
            )

    def _margin_capped_emergency_cleanup(
        self,
        plan: TradePlan,
        symbol: str,
        initial_quantity: Decimal,
    ) -> dict[str, Any]:
        audit: dict[str, Any] = {
            "strategy": self._margin_capped_strategy_label(plan),
            "initial_cleanup_quantity": decimal_to_api(initial_quantity),
            "attempts": [],
            "confirmed_closed": False,
        }
        cleanup_quantity = initial_quantity
        for attempt_number in range(1, 3):
            attempt: dict[str, Any] = {
                "attempt": attempt_number,
                "quantity": decimal_to_api(cleanup_quantity),
            }
            try:
                attempt["order"] = self.client.close_position_market(symbol, cleanup_quantity)
            except Exception as close_exc:
                attempt["order_error"] = str(close_exc)

            try:
                remaining_quantity = self.client.get_long_position_quantity(symbol)
                attempt["remaining_quantity"] = decimal_to_api(remaining_quantity)
            except Exception as query_exc:
                attempt["remaining_query_error"] = str(query_exc)
                audit["attempts"].append(attempt)
                audit["final_error"] = "EMERGENCY_POSITION_CONFIRMATION_FAILED"
                return audit

            audit["attempts"].append(attempt)
            if remaining_quantity <= 0:
                audit["confirmed_closed"] = True
                return audit
            cleanup_quantity = remaining_quantity

        audit["remaining_quantity"] = decimal_to_api(cleanup_quantity)
        audit["final_error"] = "EMERGENCY_POSITION_STILL_OPEN"
        return audit

    def _execution_plan_from_actual_entry(
        self,
        plan: TradePlan,
        actual_entry: Decimal,
        protected_quantity: Decimal | None = None,
    ) -> TradePlan:
        if actual_entry <= 0:
            raise BinanceAPIError(f"Invalid actual entry price for {plan.symbol}: {actual_entry}")
        self._assert_structured_actual_entry_range(plan, actual_entry)

        rules = self._symbol_rules(plan.symbol)
        if plan.stop_mode in {
            "breakout_retest_margin_capped",
            "relative_strength_pullback_margin_capped",
            "vwap_rotation_margin_capped",
            "sell_pressure_decay_margin_capped",
            "breadth_recovery_margin_capped",
            "trend_support_continuation_margin_capped",
            _N17_STOP_MODE,
            _N18_STOP_MODE,
            _N19_STOP_MODE,
            _N20_STOP_MODE,
            _MICRO_STOP_MODE,
        }:
            structural_stop = plan.structure_stop_price or plan.stop_loss_price
            minimum_distance_stop = floor_to_step(
                actual_entry * (Decimal("1") - self.config.min_stop_loss_pct),
                rules.tick_size,
            )
            stop_loss_price = floor_to_step(
                min(structural_stop, minimum_distance_stop),
                rules.tick_size,
            )
            risk_distance = actual_entry - stop_loss_price
            raw_take_profit = actual_entry + plan.risk_reward_ratio * risk_distance
            take_profit_price = ceil_to_step(raw_take_profit, rules.tick_size)
        elif plan.stop_mode in {
            "structure_p1",
            "structure_p1_margin_capped",
            "sweep_low_tick_margin_capped",
        }:
            stop_reference = plan.structure_stop_price or plan.stop_loss_price
            stop_loss_price = floor_to_step(stop_reference, rules.tick_size)
            risk_distance = actual_entry - stop_loss_price
            raw_take_profit = actual_entry + plan.risk_reward_ratio * risk_distance
            take_profit_price = ceil_to_step(raw_take_profit, rules.tick_size)
        elif plan.stop_mode == "s1_target_margin_capped":
            target_price = plan.structure_target_price or plan.take_profit_price
            stop_loss_price, take_profit_price, _, _ = self._n09_protection_prices(
                plan.symbol,
                actual_entry,
                target_price,
                rules,
            )
        else:
            stop_loss_price = floor_to_step(
                actual_entry * (Decimal("1") - plan.stop_loss_pct),
                rules.tick_size,
            )
            risk_distance = actual_entry - stop_loss_price
            raw_take_profit = actual_entry + plan.risk_reward_ratio * risk_distance
            take_profit_price = ceil_to_step(raw_take_profit, rules.tick_size)

        if stop_loss_price <= 0 or stop_loss_price >= actual_entry:
            raise BinanceAPIError(
                f"Invalid protection distance after fill for {plan.symbol}: "
                f"entry={actual_entry}, stop={stop_loss_price}"
            )

        risk_distance = actual_entry - stop_loss_price
        if plan.stop_mode in {
            "sweep_low_tick_margin_capped",
            "breakout_retest_margin_capped",
            "relative_strength_pullback_margin_capped",
            "vwap_rotation_margin_capped",
            "sell_pressure_decay_margin_capped",
            "breadth_recovery_margin_capped",
            "trend_support_continuation_margin_capped",
            _N17_STOP_MODE,
            _N18_STOP_MODE,
            _N19_STOP_MODE,
            _N20_STOP_MODE,
            _MICRO_STOP_MODE,
        }:
            stop_loss_pct = risk_distance / actual_entry
            if not self.config.min_stop_loss_pct <= stop_loss_pct <= self.config.max_stop_loss_pct:
                strategy_label = self._margin_capped_strategy_label(plan)
                raise BinanceAPIError(
                    f"{strategy_label}_STOP_PCT_OUT_OF_RANGE:"
                    f"{plan.symbol}:stop_pct={stop_loss_pct}"
                )
        if take_profit_price <= actual_entry:
            raise BinanceAPIError(
                f"Invalid take profit after fill for {plan.symbol}: "
                f"adjusted={take_profit_price}"
            )
        actual_r_multiple = (take_profit_price - actual_entry) / risk_distance
        if actual_r_multiple < Decimal("5"):
            raise BinanceAPIError(
                f"Post-fill risk/reward below 5 for {plan.symbol}: {actual_r_multiple}"
            )

        final_quantity = protected_quantity if protected_quantity is not None else plan.quantity
        notional_value = final_quantity * actual_entry
        actual_risk_amount = final_quantity * risk_distance
        return replace(
            plan,
            quantity=final_quantity,
            entry_price=actual_entry,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            stop_loss_pct=risk_distance / actual_entry,
            take_profit_pct=(take_profit_price - actual_entry) / actual_entry,
            notional_value=notional_value,
            required_margin=notional_value / Decimal(plan.leverage),
            actual_risk_amount=actual_risk_amount,
            risk_reward_ratio=actual_r_multiple,
            actual_entry_price=actual_entry,
        )

    def _assert_structured_actual_entry_range(
        self,
        plan: TradePlan,
        actual_entry: Decimal,
    ) -> None:
        labels = {
            "sweep_low_tick_margin_capped": "N10",
            "breakout_retest_margin_capped": "N11",
            "relative_strength_pullback_margin_capped": "N12",
            "vwap_rotation_margin_capped": "N13",
            "sell_pressure_decay_margin_capped": "N14",
            "breadth_recovery_margin_capped": "N15",
            "trend_support_continuation_margin_capped": "N16",
            _N17_STOP_MODE: "N17",
            _N18_STOP_MODE: "N18",
            _N19_STOP_MODE: "N19",
            _N20_STOP_MODE: "N20",
        }
        strategy_label = labels.get(plan.stop_mode)
        if plan.stop_mode == _MICRO_STOP_MODE:
            strategy_label = self._margin_capped_strategy_label(plan)
        if strategy_label is None:
            return
        if plan.entry_min_price is None or plan.entry_max_price is None:
            raise BinanceAPIError(
                f"{strategy_label}_ENTRY_RANGE_MISSING:{plan.symbol}:"
                f"entry_min={plan.entry_min_price}:entry_max={plan.entry_max_price}"
            )
        allowed_min = plan.entry_min_price * Decimal("0.995")
        allowed_max = plan.entry_max_price * Decimal("1.005")
        if actual_entry < allowed_min or actual_entry > allowed_max:
            raise BinanceAPIError(
                f"{strategy_label}_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE:{plan.symbol}:"
                f"actual_entry={actual_entry}:entry_min={plan.entry_min_price}:"
                f"entry_max={plan.entry_max_price}:allowed_min={allowed_min}:"
                f"allowed_max={allowed_max}"
            )

    def _algo_order_identity(
        self,
        order: Any,
    ) -> tuple[int | None, str | None]:
        if not isinstance(order, dict):
            return None, None
        raw_algo_id = order.get("algoId")
        algo_id: int | None = None
        if type(raw_algo_id) is int and raw_algo_id > 0:
            algo_id = raw_algo_id
        elif (
            isinstance(raw_algo_id, str)
            and raw_algo_id.isdigit()
            and int(raw_algo_id) > 0
        ):
            algo_id = int(raw_algo_id)
        raw_client_algo_id = order.get("clientAlgoId")
        client_algo_id = (
            raw_client_algo_id
            if isinstance(raw_client_algo_id, str)
            and bool(raw_client_algo_id.strip())
            else None
        )
        return algo_id, client_algo_id

    @staticmethod
    def _normalized_exchange_order_id(value: Any) -> int | None:
        if type(value) is int and value > 0:
            return value
        if (
            type(value) is str
            and value.isascii()
            and value.isdecimal()
        ):
            normalized = int(value)
            if normalized > 0 and str(normalized) == value:
                return normalized
        return None

    @staticmethod
    def _is_missing_actual_order_id(value: Any) -> bool:
        """Only Binance's literal empty field means no ordinary order exists.

        In particular, do not compare arbitrary string subclasses with ``""``:
        a hostile subclass can override ``__eq__`` and otherwise bypass the
        strict ordinary-order identity validation below.
        """
        return value is None or (type(value) is str and value == "")

    def _new_exchange_client_id(self, prefix: str) -> str:
        return f"{prefix}{uuid.uuid4().hex}"[:36]

    def _validate_market_order_response(
        self,
        symbol: str,
        response: Any,
        expected_client_order_id: str,
    ) -> tuple[Decimal, str]:
        if not isinstance(response, dict) or not response:
            raise BinanceAPIError(
                f"MARKET_ORDER_CONFIRMATION_INVALID:{symbol}:{response}"
            )
        if self.config.dry_run and response.get("dryRun") is True:
            return Decimal(str(response.get("origQty", "0"))), "FILLED"
        status = str(response.get("status", "")).upper()
        allowed_statuses = {
            "NEW",
            "PARTIALLY_FILLED",
            "FILLED",
            "CANCELED",
            "CANCELLED",
            "REJECTED",
            "EXPIRED",
        }
        try:
            executed_quantity = Decimal(str(response.get("executedQty", "0")))
        except (ArithmeticError, ValueError) as exc:
            raise BinanceAPIError(
                f"MARKET_ORDER_EXECUTED_QTY_INVALID:{symbol}:{response}"
            ) from exc
        if (
            response.get("clientOrderId") != expected_client_order_id
            or response.get("symbol") != symbol
            or response.get("side") != "BUY"
            or response.get("type", response.get("origType")) != "MARKET"
            or status not in allowed_statuses
            or not executed_quantity.is_finite()
            or executed_quantity < 0
            or (status == "FILLED" and executed_quantity <= 0)
        ):
            raise BinanceAPIError(
                f"MARKET_ORDER_IDENTITY_MISMATCH:{symbol}:"
                f"expected_client={expected_client_order_id}:response={response}"
            )
        return executed_quantity, status

    def _validate_protection_order_response(
        self,
        symbol: str,
        order_type: str,
        response: Any,
        expected_client_algo_id: str,
    ) -> dict[str, Any]:
        if not isinstance(response, dict) or not response:
            raise BinanceAPIError(
                f"INVALID_{order_type}_PROTECTION_RESPONSE:{symbol}:{response}"
            )
        if self.config.dry_run and response.get("dryRun") is True:
            return response
        algo_id, client_algo_id = self._algo_order_identity(response)
        if (
            client_algo_id != expected_client_algo_id
            or ("algoId" in response and algo_id is None)
        ):
            raise BinanceAPIError(
                f"{order_type}_PROTECTION_ID_MISMATCH:{symbol}:"
                f"expected_client={expected_client_algo_id}:response={response}"
            )
        rejected = {"REJECTED", "CANCELED", "CANCELLED", "EXPIRED", "FAILED"}
        response_status = str(
            response.get("algoStatus", response.get("status", ""))
        ).upper()
        if response_status in rejected:
            raise BinanceAPIError(
                f"{order_type}_PROTECTION_REJECTED:{symbol}:status={response_status}"
            )
        try:
            confirmed = self.client.get_algo_order(
                algo_id=algo_id,
                client_algo_id=client_algo_id,
            )
        except Exception as exc:
            raise BinanceAPIError(
                f"{order_type}_PROTECTION_CONFIRMATION_FAILED:{symbol}:{exc}"
            ) from exc
        if not isinstance(confirmed, dict) or not confirmed:
            raise BinanceAPIError(
                f"{order_type}_PROTECTION_CONFIRMATION_INVALID:{symbol}:{confirmed}"
            )
        confirmed_id, confirmed_client_id = self._algo_order_identity(confirmed)
        if confirmed_client_id != expected_client_algo_id:
            raise BinanceAPIError(
                f"{order_type}_PROTECTION_CONFIRMATION_CLIENT_ID_MISMATCH:"
                f"{symbol}:expected={expected_client_algo_id}:"
                f"confirmed={confirmed_client_id}"
            )
        if algo_id is not None and confirmed_id != algo_id:
            raise BinanceAPIError(
                f"{order_type}_PROTECTION_CONFIRMATION_ID_MISMATCH:{symbol}:"
                f"placed=({algo_id},{client_algo_id}):"
                f"confirmed=({confirmed_id},{confirmed_client_id})"
            )
        confirmed_status = str(confirmed.get("algoStatus", "")).upper()
        expected_order_type = (
            "STOP_MARKET" if order_type == "STOP" else "TAKE_PROFIT_MARKET"
        )
        if confirmed_status != "NEW":
            raise BinanceAPIError(
                f"{order_type}_PROTECTION_CONFIRMATION_NOT_ACTIVE:"
                f"{symbol}:status={confirmed_status}"
            )
        if (
            confirmed.get("symbol") != symbol
            or confirmed.get("algoType") != "CONDITIONAL"
            or confirmed.get("orderType") != expected_order_type
            or confirmed.get("side") != "SELL"
            or confirmed.get("closePosition") is not True
        ):
            raise BinanceAPIError(
                f"{order_type}_PROTECTION_CONFIRMATION_MISMATCH:{symbol}:"
                f"confirmed={confirmed}"
            )
        return confirmed

    def _cancel_placed_protection_orders(
        self,
        symbol: str,
        orders: list[dict[str, Any]],
    ) -> dict[str, Any]:
        audit: dict[str, Any] = {
            "attempts": [],
            "confirmed_all_canceled": True,
            "symbol": symbol,
        }
        updated_orders = [dict(order) for order in orders]
        confirmed_terminal: set[tuple[int | None, str | None]] = set()
        finished_non_cancelled: set[
            tuple[int | None, str | None]
        ] = set()
        canceled_statuses = {
            "CANCELED",
            "CANCELLED",
            "EXPIRED",
            "REJECTED",
        }
        deduplicated: list[dict[str, Any]] = []
        seen: set[tuple[int | None, str | None]] = set()
        for order in reversed(orders):
            algo_id, client_algo_id = self._algo_order_identity(order)
            identity = (algo_id, client_algo_id)
            if identity in seen:
                continue
            seen.add(identity)
            deduplicated.append(order)

        for order in deduplicated:
            algo_id, client_algo_id = self._algo_order_identity(order)
            attempt: dict[str, Any] = {
                "algo_id": algo_id,
                "client_algo_id": client_algo_id,
            }
            if self.config.dry_run and order.get("dryRun") is True:
                attempt["confirmed_canceled"] = True
                audit["attempts"].append(attempt)
                continue
            if algo_id is None and not client_algo_id:
                attempt["error"] = "PROTECTION_ID_MISSING"
                attempt["confirmed_canceled"] = False
                audit["confirmed_all_canceled"] = False
                audit["attempts"].append(attempt)
                continue
            cancel_error: str | None = None
            try:
                cancel_response = self.client.cancel_algo_order(
                    algo_id=algo_id,
                    client_algo_id=client_algo_id,
                )
                attempt["cancel_response"] = cancel_response
            except Exception as exc:
                cancel_error = str(exc)
                attempt["cancel_error"] = cancel_error
            try:
                confirmed = self.client.get_algo_order(
                    algo_id=algo_id,
                    client_algo_id=client_algo_id,
                )
                attempt["confirmation"] = confirmed
                confirmed_algo_id, confirmed_client_algo_id = (
                    self._algo_order_identity(confirmed)
                )
                if (
                    (algo_id is not None and confirmed_algo_id != algo_id)
                    or (
                        client_algo_id is not None
                        and confirmed_client_algo_id != client_algo_id
                    )
                ):
                    raise BinanceAPIError(
                        "PROTECTION_CANCEL_IDENTITY_MISMATCH:"
                        f"expected=({algo_id},{client_algo_id}):"
                        f"confirmed=({confirmed_algo_id},{confirmed_client_algo_id})"
                    )
                role = order.get("role")
                expected_order_type = {
                    "STOP": "STOP_MARKET",
                    "STOP_LOSS": "STOP_MARKET",
                    "TAKE_PROFIT": "TAKE_PROFIT_MARKET",
                }.get(role)
                if (
                    confirmed.get("symbol") != symbol
                    or confirmed.get("algoType") != "CONDITIONAL"
                    or confirmed.get("side") != "SELL"
                    or confirmed.get("closePosition") is not True
                    or (
                        expected_order_type is not None
                        and confirmed.get("orderType")
                        != expected_order_type
                    )
                ):
                    raise BinanceAPIError(
                        "PROTECTION_CANCEL_SEMANTICS_MISMATCH:"
                        f"symbol={symbol}:role={role}:confirmed={confirmed}"
                    )
                status = str(
                    confirmed.get("algoStatus", confirmed.get("status", ""))
                ).upper()
                if status == "FINISHED":
                    finished_non_cancelled.add((algo_id, client_algo_id))
                    finished_detail: dict[str, Any] = {
                        "algo_status": status,
                        "actual_order_id": confirmed.get("actualOrderId"),
                    }
                    raw_actual_order_id = confirmed.get("actualOrderId")
                    actual_order_id = self._normalized_exchange_order_id(
                        raw_actual_order_id
                    )
                    if not self._is_missing_actual_order_id(raw_actual_order_id):
                        if actual_order_id is None:
                            raise BinanceAPIError(
                                "PROTECTION_FINISHED_ACTUAL_ORDER_ID_INVALID:"
                                f"{raw_actual_order_id!r}"
                            )
                        actual_order = self.client.get_order(
                            symbol,
                            order_id=actual_order_id,
                        )
                        queried_order_id = (
                            self._normalized_exchange_order_id(
                                actual_order.get("orderId")
                            )
                            if isinstance(actual_order, dict)
                            else None
                        )
                        if (
                            not isinstance(actual_order, dict)
                            or queried_order_id != actual_order_id
                            or actual_order.get("symbol") != symbol
                            or actual_order.get("side") != "SELL"
                        ):
                            raise BinanceAPIError(
                                "PROTECTION_FINISHED_ACTUAL_ORDER_MISMATCH:"
                                f"expected={actual_order_id}:"
                                f"actual={actual_order}"
                            )
                        finished_detail["actual_order"] = actual_order
                    attempt["finished_execution"] = finished_detail
                    raise BinanceAPIError(
                        "PROTECTION_FINISHED_DURING_CANCEL:"
                        f"{finished_detail}"
                    )
                if status not in canceled_statuses:
                    raise BinanceAPIError(
                        f"PROTECTION_CANCEL_NOT_CONFIRMED:status={status}"
                    )
                attempt["confirmed_canceled"] = True
                confirmed_terminal.add((algo_id, client_algo_id))
            except Exception as exc:
                attempt["confirmation_error"] = str(exc)
                if cancel_error is not None:
                    attempt["error"] = (
                        f"cancel={cancel_error};confirmation={exc}"
                    )
                else:
                    attempt["error"] = str(exc)
                attempt["confirmed_canceled"] = False
                audit["confirmed_all_canceled"] = False
            audit["attempts"].append(attempt)

        try:
            open_orders = self.client.get_open_algo_orders(symbol)
            audit["open_orders_before_fallback"] = open_orders
        except Exception as exc:
            open_orders = None
            audit["open_orders_query_error"] = str(exc)
            audit["confirmed_all_canceled"] = False

        known_algo_ids = {
            algo_id
            for algo_id, _ in seen
            if algo_id is not None
        }
        known_client_ids = {
            client_id
            for _, client_id in seen
            if client_id is not None
        }

        def is_relevant_open(order: dict[str, Any]) -> bool:
            algo_id, client_id = self._algo_order_identity(order)
            return algo_id in known_algo_ids or client_id in known_client_ids

        relevant_open = (
            [order for order in open_orders if is_relevant_open(order)]
            if isinstance(open_orders, list)
            else []
        )
        if isinstance(open_orders, list):
            for updated in updated_orders:
                identity = self._algo_order_identity(updated)
                if identity in confirmed_terminal:
                    continue
                if identity in finished_non_cancelled:
                    continue
                if any(
                    self._algo_order_identity(open_order) == identity
                    or (
                        identity[1] is not None
                        and self._algo_order_identity(open_order)[1]
                        == identity[1]
                    )
                    for open_order in relevant_open
                ):
                    continue
                if updated.get("submission_attempted") is False:
                    updated["absence_confirmations"] = 2
                else:
                    previous_absence = updated.get("absence_confirmations", 0)
                    if type(previous_absence) is not int or previous_absence < 0:
                        previous_absence = 0
                    updated["absence_confirmations"] = previous_absence + 1
            absence_confirmed = all(
                self._algo_order_identity(updated)
                not in finished_non_cancelled
                and (
                    self._algo_order_identity(updated)
                    in confirmed_terminal
                    or updated.get("submission_attempted") is False
                    or updated.get("absence_confirmations", 0) >= 2
                )
                for updated in updated_orders
            )
            # A potentially submitted protection order must be absent on a
            # later reconciliation round before absence is terminal evidence.
            audit["confirmed_all_canceled"] = (
                not relevant_open and absence_confirmed
            )
            if relevant_open:
                audit["relevant_open_before_fallback"] = relevant_open
        else:
            audit["confirmed_all_canceled"] = all(
                self._algo_order_identity(updated)
                not in finished_non_cancelled
                and (
                    self._algo_order_identity(updated)
                    in confirmed_terminal
                    or updated.get("submission_attempted") is False
                )
                for updated in updated_orders
            )

        unrelated_open = (
            [order for order in open_orders if not is_relevant_open(order)]
            if isinstance(open_orders, list)
            else []
        )
        if unrelated_open:
            audit["unrelated_open_orders"] = unrelated_open
            audit["confirmed_all_canceled"] = False

        bulk_fallback_allowed = bool(
            isinstance(open_orders, list)
            and relevant_open
            and not unrelated_open
        )
        if not audit["confirmed_all_canceled"] and bulk_fallback_allowed:
            try:
                audit["cancel_all_response"] = (
                    self.client.cancel_all_algo_open_orders(symbol)
                )
                final_open_orders = self.client.get_open_algo_orders(symbol)
                audit["open_orders_after_fallback"] = final_open_orders
                if final_open_orders:
                    raise BinanceAPIError(
                        f"ALGO_OPEN_ORDERS_REMAIN:{symbol}:{final_open_orders}"
                    )
                audit["confirmed_all_canceled"] = True
                audit["fallback_used"] = True
                for updated in updated_orders:
                    updated["absence_confirmations"] = max(
                        int(updated.get("absence_confirmations", 0)),
                        2,
                    )
            except Exception as exc:
                audit["fallback_error"] = str(exc)
                audit["confirmed_all_canceled"] = False
        elif not audit["confirmed_all_canceled"]:
            audit["fallback_skipped"] = (
                "OPEN_ORDER_SCOPE_NOT_PROVEN_EXCLUSIVE"
            )
        audit["updated_orders"] = updated_orders
        return audit

    @staticmethod
    def _strategy_order_payload(
        plan: TradePlan,
        strategy_id: str | None,
    ) -> dict[str, Any] | None:
        context = plan.structure_context
        context_strategy_id = (
            context.get("strategy_id") if type(context) is dict else None
        )
        strict_modes = {
            _N16_STOP_MODE: "N16",
            _N17_STOP_MODE: "N17",
            _N18_STOP_MODE: "N18",
            _N19_STOP_MODE: "N19",
            _N20_STOP_MODE: "N20",
        }
        expected_strategy = strict_modes.get(plan.stop_mode)
        if plan.stop_mode == _MICRO_STOP_MODE:
            expected_strategy = context_strategy_id
            if expected_strategy not in _MICRO_STRATEGY_IDS:
                raise BinanceAPIError("MICRO_PLAN_STRATEGY_IDENTITY_CONFLICT")
        if expected_strategy is not None and (
            context_strategy_id != expected_strategy
            or strategy_id != expected_strategy
        ):
            raise BinanceAPIError(
                f"{expected_strategy}_PLAN_STRATEGY_IDENTITY_CONFLICT"
            )
        if expected_strategy is None and (
            context_strategy_id in {"N16", "N17", "N18", "N19", "N20", *_MICRO_STRATEGY_IDS}
            or strategy_id in {"N16", "N17", "N18", "N19", "N20", *_MICRO_STRATEGY_IDS}
        ):
            raise BinanceAPIError("STRATEGY_PLAN_IDENTITY_CONFLICT")
        if type(strategy_id) is not str or not strategy_id:
            return None
        signal_id = (
            context.get("signal_id") if type(context) is dict else None
        )
        payload: dict[str, Any] = {
            "strategy_id": strategy_id,
            "structure_id": plan.structure_id,
        }
        if type(signal_id) is int and signal_id > 0:
            payload["signal_id"] = signal_id
        elif expected_strategy is not None:
            raise BinanceAPIError(
                f"{expected_strategy}_PUBLISHED_SIGNAL_ID_MISSING"
            )
        if expected_strategy is not None and (
            type(plan.structure_id) is not str
            or len(plan.structure_id) != 24
            or any(
                character not in "0123456789abcdef"
                for character in plan.structure_id
            )
            or context.get("structure_id") != plan.structure_id
        ):
            raise BinanceAPIError(f"{expected_strategy}_STRUCTURE_ID_INVALID")
        return payload

    def _plan_payload(self, plan: TradePlan) -> dict[str, Any]:
        return {
            "stop_mode": plan.stop_mode,
            "risk_reward_ratio": decimal_to_api(plan.risk_reward_ratio),
            "structure_id": plan.structure_id,
            "structure_stop_price": decimal_to_api(plan.structure_stop_price)
            if plan.structure_stop_price is not None
            else None,
            "structure_target_price": decimal_to_api(plan.structure_target_price)
            if plan.structure_target_price is not None
            else None,
            "entry_price": decimal_to_api(plan.entry_price),
            "entry_min": decimal_to_api(plan.entry_min_price)
            if plan.entry_min_price is not None
            else None,
            "entry_max": decimal_to_api(plan.entry_max_price)
            if plan.entry_max_price is not None
            else None,
            "actual_entry": decimal_to_api(plan.actual_entry_price)
            if plan.actual_entry_price is not None
            else None,
            "stop_loss_price": decimal_to_api(plan.stop_loss_price),
            "take_profit_price": decimal_to_api(plan.take_profit_price),
            "amplitude_24h_pct": decimal_to_api(plan.amplitude_24h_pct),
            "high_24h_price": decimal_to_api(plan.high_24h_price),
            "low_24h_price": decimal_to_api(plan.low_24h_price),
            "stop_loss_pct": decimal_to_api(plan.stop_loss_pct),
            "take_profit_pct": decimal_to_api(plan.take_profit_pct),
            "risk_amount": decimal_to_api(plan.risk_amount),
            "notional_value": decimal_to_api(plan.notional_value),
            "required_margin": decimal_to_api(plan.required_margin),
            "balance": decimal_to_api(plan.balance),
            "target_risk_amount": decimal_to_api(plan.target_risk_amount)
            if plan.target_risk_amount is not None
            else None,
            "actual_risk_amount": decimal_to_api(plan.actual_risk_amount)
            if plan.actual_risk_amount is not None
            else None,
            "risk_capped_by_margin": plan.risk_capped_by_margin,
            "pretrade_quantity": decimal_to_api(plan.pretrade_quantity)
            if plan.pretrade_quantity is not None
            else None,
            "executed_quantity": decimal_to_api(plan.executed_quantity)
            if plan.executed_quantity is not None
            else None,
            "final_protected_quantity": decimal_to_api(plan.final_protected_quantity)
            if plan.final_protected_quantity is not None
            else None,
            "post_fill_actual_risk_amount": decimal_to_api(plan.post_fill_actual_risk_amount)
            if plan.post_fill_actual_risk_amount is not None
            else None,
            "post_fill_required_margin": decimal_to_api(plan.post_fill_required_margin)
            if plan.post_fill_required_margin is not None
            else None,
            "reduced_after_fill": plan.reduced_after_fill,
            "entry_candle_open_time_ms": plan.entry_candle_open_time_ms,
            "entry_deadline_ms": plan.entry_deadline_ms,
            "structure_context": plan.structure_context,
        }

    def _market_order_execution_pending_state(
        self,
        plan: TradePlan,
        client_order_id: str,
        error: Any,
        possible_protection_orders: list[dict[str, Any]],
        *,
        phase: str,
        opened_at: str | None = None,
    ) -> PositionState:
        structure_context = (
            plan.structure_context
            if isinstance(plan.structure_context, dict)
            else {}
        )
        strategy_id = structure_context.get("strategy_id")
        if not isinstance(strategy_id, str) or not strategy_id:
            try:
                strategy_id = self._margin_capped_strategy_label(plan)
            except BinanceAPIError:
                strategy_id = None
        orders: dict[str, Any] = {
            "plan": self._plan_payload(plan),
            "execution_pending": {
                "reason": "MARKET_ORDER_EXECUTION_UNKNOWN",
                "phase": phase,
                "client_order_id": client_order_id,
                "error": str(error),
                "stop_mode": plan.stop_mode,
                "structure_id": plan.structure_id,
                "possible_protection_orders": possible_protection_orders,
                "market_order_absence_confirmations": 0,
            },
        }
        strategy_payload = self._strategy_order_payload(plan, strategy_id)
        if strategy_payload is not None:
            orders["strategy"] = strategy_payload
            if strategy_payload["strategy_id"] in {
                "N16", "N17", "N18", "N19", "N20", *_MICRO_STRATEGY_IDS
            }:
                orders["execution_pending"].update(
                    {
                        "strategy_id": strategy_payload["strategy_id"],
                        "signal_id": strategy_payload["signal_id"],
                    }
                )
        return PositionState(
            symbol=plan.symbol,
            quantity=decimal_to_api(plan.quantity),
            entry_price=decimal_to_api(plan.entry_price),
            stop_loss_price=decimal_to_api(plan.stop_loss_price),
            take_profit_price=decimal_to_api(plan.take_profit_price),
            leverage=plan.leverage,
            opened_at=(
                opened_at
                if opened_at is not None
                else datetime.now(timezone.utc).isoformat()
            ),
            dry_run=False,
            orders=orders,
        )

    def _save_market_order_execution_pending(
        self,
        plan: TradePlan,
        client_order_id: str,
        error: Any,
        possible_protection_orders: list[dict[str, Any]],
        *,
        phase: str,
    ) -> PositionState:
        state = self._market_order_execution_pending_state(
            plan,
            client_order_id,
            error,
            possible_protection_orders,
            phase=phase,
        )
        self.state.save(state)
        return state

    def _advance_market_order_execution_pending(
        self,
        expected: PositionState,
        plan: TradePlan,
        client_order_id: str,
        error: Any,
        possible_protection_orders: list[dict[str, Any]],
        *,
        phase: str,
    ) -> PositionState:
        replacement = self._market_order_execution_pending_state(
            plan,
            client_order_id,
            error,
            possible_protection_orders,
            phase=phase,
            opened_at=expected.opened_at,
        )
        compare_and_save = getattr(self.state, "compare_and_save", None)
        if compare_and_save is None:
            raise OSError("state compare-and-save is unavailable")
        if not compare_and_save(expected, replacement):
            raise OSError("pre-submit reservation changed")
        return replacement

    def _clear_unsubmitted_market_order_journal(
        self,
        plan: TradePlan,
        client_order_id: str,
    ) -> None:
        try:
            state = self.state.load()
        except Exception as exc:
            raise BinanceAPIError(
                f"ENTRY_WINDOW_EXPIRED_PENDING_READ_FAILED:{plan.symbol}:{exc}"
            ) from exc
        expected_strategy = self._margin_capped_strategy_label(plan)
        expected_strategy_payload = self._strategy_order_payload(
            plan, expected_strategy
        )
        if (
            state is None
            or state.symbol != plan.symbol
            or not isinstance(state.orders, dict)
            or not isinstance(state.orders.get("strategy"), dict)
            or state.orders["strategy"].get("strategy_id") != expected_strategy
            or state.orders["strategy"].get("structure_id") != plan.structure_id
            or (
                expected_strategy in {
                    "N16", "N17", "N18", "N19", "N20", *_MICRO_STRATEGY_IDS
                }
                and (
                    type(state.orders["strategy"].get("signal_id")) is not int
                    or state.orders["strategy"].get("signal_id")
                    != expected_strategy_payload["signal_id"]
                )
            )
            or not isinstance(state.orders.get("execution_pending"), dict)
        ):
            raise BinanceAPIError(
                f"ENTRY_WINDOW_EXPIRED_PENDING_IDENTITY_MISMATCH:{plan.symbol}"
            )
        pending = state.orders["execution_pending"]
        protection = pending.get("possible_protection_orders")
        if (
            pending.get("phase") not in {
                "PRE_SUBMIT_RESERVED",
                "MARKET_ORDER_SUBMITTING",
            }
            or pending.get("client_order_id") != client_order_id
            or not isinstance(protection, list)
            or any(
                not isinstance(order, dict)
                or order.get("submission_attempted") is not False
                for order in protection
            )
        ):
            raise BinanceAPIError(
                f"ENTRY_WINDOW_EXPIRED_PENDING_NOT_CLEARABLE:{plan.symbol}"
            )
        try:
            cleared = self.state.compare_and_clear(state)
        except Exception as exc:
            raise BinanceAPIError(
                f"ENTRY_WINDOW_EXPIRED_PENDING_CLEAR_FAILED:{plan.symbol}:{exc}"
            ) from exc
        if not cleared:
            raise BinanceAPIError(
                f"ENTRY_WINDOW_EXPIRED_PENDING_CHANGED:{plan.symbol}"
            )

    def _save_execution_cleanup_resolved(
        self,
        plan: TradePlan,
        failure_reason: str,
        protection_cleanup: dict[str, Any],
        emergency_cleanup: dict[str, Any],
        placed_protection_orders: list[dict[str, Any]],
    ) -> None:
        structure_context = (
            plan.structure_context
            if isinstance(plan.structure_context, dict)
            else {}
        )
        strategy_id = structure_context.get("strategy_id")
        if not isinstance(strategy_id, str) or not strategy_id:
            try:
                strategy_id = self._margin_capped_strategy_label(plan)
            except BinanceAPIError:
                strategy_id = None
        resolved = {
            "reason": "EMERGENCY_CLEANUP_CONFIRMED_FLAT_WITHOUT_LIVE_RESULT",
            "strategy_id": strategy_id,
            "stop_mode": plan.stop_mode,
            "structure_id": plan.structure_id,
            "failure_reason": failure_reason,
            "protection_cleanup": protection_cleanup,
            "emergency_cleanup": emergency_cleanup,
            "placed_protection_orders": protection_cleanup.get(
                "updated_orders", placed_protection_orders
            ),
        }
        orders: dict[str, Any] = {
            "plan": self._plan_payload(plan),
            "execution_cleanup_resolved": resolved,
        }
        strategy_payload = self._strategy_order_payload(plan, strategy_id)
        if strategy_payload is not None:
            orders["strategy"] = strategy_payload
            if strategy_payload["strategy_id"] in {
                "N16", "N17", "N18", "N19", "N20", *_MICRO_STRATEGY_IDS
            }:
                resolved["signal_id"] = strategy_payload["signal_id"]
        self.state.save(
            PositionState(
                symbol=plan.symbol,
                quantity="0",
                entry_price=decimal_to_api(
                    plan.actual_entry_price or plan.entry_price
                ),
                stop_loss_price=decimal_to_api(plan.stop_loss_price),
                take_profit_price=decimal_to_api(plan.take_profit_price),
                leverage=plan.leverage,
                opened_at=datetime.now(timezone.utc).isoformat(),
                dry_run=False,
                orders=orders,
            )
        )

    def _save_emergency_cleanup_pending(
        self,
        plan: TradePlan,
        cleanup_quantity: Decimal,
        failure_reason: str,
        protection_cleanup: dict[str, Any],
        emergency_cleanup: dict[str, Any],
        placed_protection_orders: list[dict[str, Any]],
    ) -> None:
        structure_context = (
            plan.structure_context
            if isinstance(plan.structure_context, dict)
            else {}
        )
        strategy_id = structure_context.get("strategy_id")
        if not isinstance(strategy_id, str) or not strategy_id:
            try:
                strategy_id = self._margin_capped_strategy_label(plan)
            except BinanceAPIError:
                strategy_id = None
        orders: dict[str, Any] = {
            "plan": self._plan_payload(plan),
            "emergency_cleanup_pending": {
                "strategy_id": strategy_id,
                "stop_mode": plan.stop_mode,
                "structure_id": plan.structure_id,
                "cleanup_quantity": decimal_to_api(cleanup_quantity),
                "failure_reason": failure_reason,
                "protection_cleanup": protection_cleanup,
                "emergency_cleanup": emergency_cleanup,
                "placed_protection_orders": placed_protection_orders,
            },
        }
        strategy_payload = self._strategy_order_payload(plan, strategy_id)
        if strategy_payload is not None:
            orders["strategy"] = strategy_payload
            if strategy_payload["strategy_id"] in {
                "N16", "N17", "N18", "N19", "N20", *_MICRO_STRATEGY_IDS
            }:
                orders["emergency_cleanup_pending"]["signal_id"] = (
                    strategy_payload["signal_id"]
                )
        self.state.save(
            PositionState(
                symbol=plan.symbol,
                quantity=decimal_to_api(cleanup_quantity),
                entry_price=decimal_to_api(
                    plan.actual_entry_price or plan.entry_price
                ),
                stop_loss_price=decimal_to_api(plan.stop_loss_price),
                take_profit_price=decimal_to_api(plan.take_profit_price),
                leverage=plan.leverage,
                opened_at=datetime.now(timezone.utc).isoformat(),
                dry_run=False,
                orders=orders,
            )
        )

    def open_long_plan_with_protection(self, plan: TradePlan) -> PositionState:
        symbol = plan.symbol
        structure_context = (
            plan.structure_context
            if type(plan.structure_context) is dict
            else {}
        )
        if (
            structure_context.get("strategy_id") in {"N16", "N17", "N18", "N19", "N20", *_MICRO_STRATEGY_IDS}
            or plan.stop_mode in {_N16_STOP_MODE, _N17_STOP_MODE, _N18_STOP_MODE, _N19_STOP_MODE, _N20_STOP_MODE, _MICRO_STOP_MODE}
        ):
            # This is an audit identity gate and runs before set_leverage or
            # any other exchange call.  It does not alter pricing or strategy
            # eligibility.
            self._strategy_order_payload(
                plan,
                {
                    _N16_STOP_MODE: "N16",
                    _N17_STOP_MODE: "N17",
                    _N18_STOP_MODE: "N18",
                    _N19_STOP_MODE: "N19",
                    _N20_STOP_MODE: "N20",
                    _MICRO_STOP_MODE: structure_context.get("strategy_id"),
                }.get(plan.stop_mode),
            )
        self.logger.info(
            "Opening long plan | symbol=%s mode=%s leverage=%sx qty=%s entry=%s stop=%s take_profit=%s "
            "amplitude24h=%s stop_pct=%s take_profit_pct=%s risk=%s target_risk=%s actual_risk=%s "
            "risk_capped_by_margin=%s margin=%s balance=%s",
            plan.symbol,
            plan.stop_mode,
            plan.leverage,
            decimal_to_api(plan.quantity),
            decimal_to_api(plan.entry_price),
            decimal_to_api(plan.stop_loss_price),
            decimal_to_api(plan.take_profit_price),
            decimal_to_api(plan.amplitude_24h_pct),
            decimal_to_api(plan.stop_loss_pct),
            decimal_to_api(plan.take_profit_pct),
            decimal_to_api(plan.risk_amount),
            decimal_to_api(plan.target_risk_amount) if plan.target_risk_amount is not None else "",
            decimal_to_api(plan.actual_risk_amount) if plan.actual_risk_amount is not None else "",
            plan.risk_capped_by_margin,
            decimal_to_api(plan.required_margin),
            decimal_to_api(plan.balance),
        )

        strict_pre_submit_required = (
            not self.config.dry_run
            and plan.stop_mode in {_N16_STOP_MODE, _N17_STOP_MODE, _N18_STOP_MODE, _N19_STOP_MODE, _N20_STOP_MODE, _MICRO_STOP_MODE}
        )
        # N16 has a durable pre-submit boundary.  Its entry window is checked
        # before any exchange call; the exact reservation is then persisted
        # and checked once more before leverage can be changed.
        if strict_pre_submit_required:
            assert_entry_window_open(plan, self.clock_ms())
        else:
            self.client.set_leverage(symbol, plan.leverage)
            assert_entry_window_open(plan, self.clock_ms())
        market_client_order_id = self._new_exchange_client_id("mkt-")
        stop_client_algo_id = self._new_exchange_client_id("sl-")
        take_profit_client_algo_id = self._new_exchange_client_id("tp-")
        open_order: dict[str, Any] = {}
        emergency_close_quantity = plan.quantity
        post_fill_adjustment: dict[str, Any] | None = None
        margin_capped_audit: dict[str, Any] = {}
        placed_protection_orders: list[dict[str, Any]] = (
            [
                {
                    "role": "STOP",
                    "clientAlgoId": stop_client_algo_id,
                    "submission_attempted": False,
                    "absence_confirmations": 0,
                },
                {
                    "role": "TAKE_PROFIT",
                    "clientAlgoId": take_profit_client_algo_id,
                    "submission_attempted": False,
                    "absence_confirmations": 0,
                },
            ]
            if not self.config.dry_run
            else []
        )
        margin_capped_stop_modes = {
            "structure_p1_margin_capped",
            "amplitude_margin_capped",
            "s1_target_margin_capped",
            "sweep_low_tick_margin_capped",
            "breakout_retest_margin_capped",
            "relative_strength_pullback_margin_capped",
            "vwap_rotation_margin_capped",
            "sell_pressure_decay_margin_capped",
            "breadth_recovery_margin_capped",
            "trend_support_continuation_margin_capped",
            _N17_STOP_MODE,
            _N18_STOP_MODE,
            _N19_STOP_MODE,
            _N20_STOP_MODE,
            _MICRO_STOP_MODE,
        }
        strict_reservation: PositionState | None = None
        if strict_pre_submit_required:
            try:
                strict_reservation = self._save_market_order_execution_pending(
                    plan,
                    market_client_order_id,
                    "PRE_SUBMIT_RESERVATION",
                    placed_protection_orders,
                    phase="PRE_SUBMIT_RESERVED",
                )
            except Exception as exc:
                raise BinanceAPIError(
                    f"MARKET_ORDER_PRE_SUBMIT_JOURNAL_FAILED:{symbol}:{exc}"
                ) from exc
            try:
                assert_entry_window_open(plan, self.clock_ms())
            except EntryWindowExpiredError:
                self._clear_unsubmitted_market_order_journal(
                    plan, market_client_order_id
                )
                raise
            try:
                self._advance_market_order_execution_pending(
                    strict_reservation,
                    plan,
                    market_client_order_id,
                    "MARKET_ORDER_SUBMITTING",
                    placed_protection_orders,
                    phase="MARKET_ORDER_SUBMITTING",
                )
            except Exception as exc:
                raise BinanceAPIError(
                    f"MARKET_ORDER_PRE_SUBMIT_JOURNAL_FAILED:{symbol}:{exc}"
                ) from exc
            try:
                self.client.set_leverage(symbol, plan.leverage)
            except Exception as exc:
                # MARKET_ORDER_SUBMITTING was durably established before the
                # first exchange call.  Reconciliation therefore owns every
                # acknowledgement-loss case and the market order is not sent
                # from this invocation.
                raise BinanceAPIError(
                    f"LEVERAGE_SETUP_FAILED_WITH_PENDING_JOURNAL:{symbol}:{exc}"
                ) from exc
        if (
            not self.config.dry_run
            and plan.stop_mode in margin_capped_stop_modes
            and not strict_pre_submit_required
        ):
            try:
                self._save_market_order_execution_pending(
                    plan,
                    market_client_order_id,
                    "PRE_SUBMIT_JOURNAL",
                    placed_protection_orders,
                    phase="MARKET_ORDER_SUBMITTING",
                )
            except Exception as exc:
                raise BinanceAPIError(
                    f"MARKET_ORDER_PRE_SUBMIT_JOURNAL_FAILED:{symbol}:{exc}"
                ) from exc
            try:
                assert_entry_window_open(plan, self.clock_ms())
            except EntryWindowExpiredError:
                self._clear_unsubmitted_market_order_journal(
                    plan, market_client_order_id
                )
                raise
        try:
            try:
                open_order = self.client.place_market_order(
                    symbol,
                    "BUY",
                    plan.quantity,
                    client_order_id=market_client_order_id,
                )
                self._validate_market_order_response(
                    symbol,
                    open_order,
                    market_client_order_id,
                )
            except Exception as place_exc:
                if isinstance(place_exc, BinanceAPIError) and str(
                    place_exc
                ).startswith("MARKET_ORDER_"):
                    self._save_market_order_execution_pending(
                        plan,
                        market_client_order_id,
                        place_exc,
                        placed_protection_orders,
                        phase="MARKET_ORDER_RESPONSE_INVALID",
                    )
                    raise MarketOrderExecutionPendingError(
                        "MARKET_ORDER_RESPONSE_INVALID:"
                        f"{symbol}:client_order_id={market_client_order_id}:"
                        f"error={place_exc}"
                    ) from place_exc
                self.logger.warning(
                    "Market order response unknown; querying by client id | "
                    "symbol=%s client_order_id=%s error=%s",
                    symbol,
                    market_client_order_id,
                    place_exc,
                )
                try:
                    open_order = self.client.get_order(
                        symbol,
                        orig_client_order_id=market_client_order_id,
                    )
                    self._validate_market_order_response(
                        symbol,
                        open_order,
                        market_client_order_id,
                    )
                except Exception as query_exc:
                    try:
                        observed_quantity = (
                            self.client.get_long_position_quantity_or_zero(symbol)
                        )
                    except Exception as position_exc:
                        self._save_market_order_execution_pending(
                            plan,
                            market_client_order_id,
                            position_exc,
                            placed_protection_orders,
                            phase="MARKET_ORDER_RESPONSE_UNKNOWN",
                        )
                        raise MarketOrderExecutionPendingError(
                            "MARKET_ORDER_EXECUTION_UNKNOWN:"
                            f"{symbol}:client_order_id={market_client_order_id}:"
                            f"query_error={query_exc}:position_error={position_exc}"
                        ) from place_exc
                    self._save_market_order_execution_pending(
                        plan,
                        market_client_order_id,
                        query_exc,
                        placed_protection_orders,
                        phase="MARKET_ORDER_RESPONSE_UNKNOWN",
                    )
                    raise MarketOrderExecutionPendingError(
                        "MARKET_ORDER_EXECUTION_UNKNOWN:"
                        f"{symbol}:client_order_id={market_client_order_id}:"
                        f"query_error={query_exc}:observed_quantity="
                        f"{decimal_to_api(observed_quantity)}"
                    ) from place_exc
            executed_quantity_from_order, market_order_status = (
                self._validate_market_order_response(
                    symbol,
                    open_order,
                    market_client_order_id,
                )
            )
            if (
                executed_quantity_from_order <= 0
                and market_order_status != "FILLED"
            ):
                try:
                    observed_quantity = (
                        self.client.get_long_position_quantity_or_zero(symbol)
                    )
                except Exception as position_exc:
                    self._save_market_order_execution_pending(
                        plan,
                        market_client_order_id,
                        position_exc,
                        placed_protection_orders,
                        phase="MARKET_ORDER_NOT_YET_CONFIRMED",
                    )
                    raise MarketOrderExecutionPendingError(
                        "MARKET_ORDER_EXECUTION_UNKNOWN:"
                        f"{symbol}:client_order_id={market_client_order_id}:"
                        f"status={market_order_status}:position_error={position_exc}"
                    ) from position_exc
                if observed_quantity <= 0:
                    self._save_market_order_execution_pending(
                        plan,
                        market_client_order_id,
                        "MARKET_ORDER_NOT_YET_EXECUTED",
                        placed_protection_orders,
                        phase="MARKET_ORDER_NOT_YET_CONFIRMED",
                    )
                    raise MarketOrderExecutionPendingError(
                        "MARKET_ORDER_EXECUTION_UNKNOWN:"
                        f"{symbol}:client_order_id={market_client_order_id}:"
                        f"status={market_order_status}:observed_quantity=0"
                    )
                emergency_close_quantity = observed_quantity
                open_order = {
                    "symbol": symbol,
                    "side": "BUY",
                    "type": "MARKET",
                    "status": "FILLED",
                    "clientOrderId": market_client_order_id,
                    "executedQty": decimal_to_api(observed_quantity),
                    "executionRecoveredFromPosition": True,
                }
            actual_entry, entry_price_source = self._confirm_actual_entry_price(
                symbol,
                open_order,
                plan.entry_price,
                market_client_order_id,
            )
            if plan.stop_mode in margin_capped_stop_modes:
                strategy_label = self._margin_capped_strategy_label(plan)
                executed_quantity = self._confirm_margin_capped_position_quantity(
                    plan,
                    symbol,
                    plan.quantity,
                )
                emergency_close_quantity = executed_quantity
                margin_capped_audit = {
                    "strategy": strategy_label,
                    "pretrade_quantity": decimal_to_api(plan.quantity),
                    "executed_quantity": decimal_to_api(executed_quantity),
                }
                if plan.stop_mode in {
                    "sweep_low_tick_margin_capped",
                    "breakout_retest_margin_capped",
                    "relative_strength_pullback_margin_capped",
                    "vwap_rotation_margin_capped",
                    "sell_pressure_decay_margin_capped",
                    "breadth_recovery_margin_capped",
                    "trend_support_continuation_margin_capped",
                    _N17_STOP_MODE,
                    _N18_STOP_MODE,
                    _N19_STOP_MODE,
                    _N20_STOP_MODE,
                    _MICRO_STOP_MODE,
                }:
                    margin_capped_audit.update(
                        {
                            "actual_entry": decimal_to_api(actual_entry),
                            "entry_min": decimal_to_api(plan.entry_min_price)
                            if plan.entry_min_price is not None
                            else None,
                            "entry_max": decimal_to_api(plan.entry_max_price)
                            if plan.entry_max_price is not None
                            else None,
                        }
                    )
                self._assert_structured_actual_entry_range(plan, actual_entry)
                safe_quantity = self._margin_capped_safe_quantity_after_fill(
                    plan,
                    actual_entry,
                    executed_quantity,
                )
                reduced_after_fill = safe_quantity < executed_quantity
                excess_quantity = executed_quantity - safe_quantity
                reduction_order: dict[str, Any] | None = None
                margin_capped_audit.update({
                    "safe_quantity": decimal_to_api(safe_quantity),
                    "excess_quantity": decimal_to_api(excess_quantity),
                    "reduced_after_fill": reduced_after_fill,
                })

                if reduced_after_fill:
                    self.logger.warning(
                        "%s post-fill reduction required | symbol=%s entry=%s executed=%s "
                        "safe=%s excess=%s",
                        strategy_label,
                        symbol,
                        decimal_to_api(actual_entry),
                        decimal_to_api(executed_quantity),
                        decimal_to_api(safe_quantity),
                        decimal_to_api(excess_quantity),
                    )
                    reduction_order = self.client.close_position_market(symbol, excess_quantity)
                    margin_capped_audit["reduction_order"] = reduction_order
                    self._validate_margin_capped_reduction_response(
                        plan,
                        symbol,
                        reduction_order,
                        excess_quantity,
                    )
                    final_quantity = self._confirm_margin_capped_position_quantity(
                        plan,
                        symbol,
                        safe_quantity,
                    )
                else:
                    final_quantity = executed_quantity

                margin_capped_audit["final_confirmed_quantity"] = decimal_to_api(final_quantity)
                emergency_close_quantity = final_quantity
                if final_quantity > safe_quantity:
                    raise BinanceAPIError(
                        f"{strategy_label} remaining position exceeds safe quantity for {symbol}: "
                        f"remaining={final_quantity}, safe={safe_quantity}"
                    )
                rules = self._symbol_rules(symbol)
                self._validate_margin_capped_quantity(
                    plan,
                    actual_entry,
                    final_quantity,
                    rules,
                    "final protected",
                )
                emergency_close_quantity = final_quantity
                execution_plan = self._execution_plan_from_actual_entry(
                    plan,
                    actual_entry,
                    protected_quantity=final_quantity,
                )
                execution_plan = replace(
                    execution_plan,
                    pretrade_quantity=plan.quantity,
                    executed_quantity=executed_quantity,
                    final_protected_quantity=final_quantity,
                    post_fill_actual_risk_amount=execution_plan.actual_risk_amount,
                    post_fill_required_margin=execution_plan.required_margin,
                    reduced_after_fill=reduced_after_fill,
                )
                self._assert_margin_capped_post_fill_limits(execution_plan)
                post_fill_adjustment = {
                    **margin_capped_audit,
                    "post_fill_actual_risk_amount": decimal_to_api(
                        execution_plan.post_fill_actual_risk_amount
                    ),
                    "post_fill_required_margin": decimal_to_api(
                        execution_plan.post_fill_required_margin
                    ),
                }
                self.logger.info(
                    "%s post-fill limits verified | symbol=%s pretrade=%s executed=%s final=%s "
                    "actual_risk=%s target_risk=%s required_margin=%s max_margin=%s reduced=%s",
                    strategy_label,
                    symbol,
                    decimal_to_api(plan.quantity),
                    decimal_to_api(executed_quantity),
                    decimal_to_api(final_quantity),
                    decimal_to_api(execution_plan.actual_risk_amount),
                    decimal_to_api(execution_plan.target_risk_amount),
                    decimal_to_api(execution_plan.required_margin),
                    decimal_to_api(
                        execution_plan.balance * self.config.max_margin_balance_fraction
                    ),
                    reduced_after_fill,
                )
            else:
                execution_plan = self._execution_plan_from_actual_entry(plan, actual_entry)
            stop_cleanup_index: int | None = None
            if (
                not self.config.dry_run
                and plan.stop_mode in margin_capped_stop_modes
            ):
                stop_cleanup_index = 0
                placed_protection_orders[stop_cleanup_index][
                    "submission_attempted"
                ] = True
                self._save_market_order_execution_pending(
                    plan,
                    market_client_order_id,
                    "STOP_PROTECTION_SUBMITTING",
                    placed_protection_orders,
                    phase="STOP_PROTECTION_SUBMITTING",
                )
            stop_order = self.client.place_close_all_algo_order(
                symbol,
                "STOP_MARKET",
                execution_plan.stop_loss_price,
                client_algo_id=stop_client_algo_id,
            )
            if stop_cleanup_index is not None and isinstance(stop_order, dict):
                placed_protection_orders[stop_cleanup_index][
                    "raw_response"
                ] = dict(stop_order)
            validated_stop = self._validate_protection_order_response(
                symbol,
                "STOP",
                stop_order,
                stop_client_algo_id,
            )
            if not (self.config.dry_run and stop_order.get("dryRun") is True):
                if stop_cleanup_index is None:
                    placed_protection_orders.append(validated_stop)
                else:
                    placed_protection_orders[stop_cleanup_index] = {
                        **placed_protection_orders[stop_cleanup_index],
                        **validated_stop,
                    }
            take_profit_cleanup_index: int | None = None
            if (
                not self.config.dry_run
                and plan.stop_mode in margin_capped_stop_modes
            ):
                take_profit_cleanup_index = 1
                placed_protection_orders[take_profit_cleanup_index][
                    "submission_attempted"
                ] = True
                self._save_market_order_execution_pending(
                    plan,
                    market_client_order_id,
                    "TAKE_PROFIT_PROTECTION_SUBMITTING",
                    placed_protection_orders,
                    phase="TAKE_PROFIT_PROTECTION_SUBMITTING",
                )
            take_profit_order = self.client.place_close_all_algo_order(
                symbol,
                "TAKE_PROFIT_MARKET",
                execution_plan.take_profit_price,
                client_algo_id=take_profit_client_algo_id,
            )
            if (
                take_profit_cleanup_index is not None
                and isinstance(take_profit_order, dict)
            ):
                placed_protection_orders[take_profit_cleanup_index][
                    "raw_response"
                ] = dict(take_profit_order)
            validated_take_profit = self._validate_protection_order_response(
                symbol,
                "TAKE_PROFIT",
                take_profit_order,
                take_profit_client_algo_id,
            )
            if not (
                self.config.dry_run and take_profit_order.get("dryRun") is True
            ):
                if take_profit_cleanup_index is None:
                    placed_protection_orders.append(validated_take_profit)
                else:
                    placed_protection_orders[take_profit_cleanup_index] = {
                        **placed_protection_orders[take_profit_cleanup_index],
                        **validated_take_profit,
                    }
        except Exception as exc:
            if isinstance(exc, MarketOrderExecutionPendingError):
                raise
            self.logger.exception("Fill confirmation or protection failed, closing position immediately: %s", symbol)
            protection_cleanup = self._cancel_placed_protection_orders(
                symbol,
                placed_protection_orders,
            )
            if plan.stop_mode in margin_capped_stop_modes:
                strategy_label = self._margin_capped_strategy_label(plan)
                emergency_cleanup = self._margin_capped_emergency_cleanup(
                    plan,
                    symbol,
                    emergency_close_quantity,
                )
                final_protection_cleanup = (
                    self._cancel_placed_protection_orders(
                        symbol,
                        placed_protection_orders,
                    )
                )
                margin_capped_audit.update(
                    {
                        "strategy": strategy_label,
                        "failure_reason": str(exc),
                        "protection_cleanup_before_close": protection_cleanup,
                        "protection_cleanup": final_protection_cleanup,
                        "emergency_cleanup": emergency_cleanup,
                    }
                )
                if (
                    not emergency_cleanup.get("confirmed_closed")
                    or not final_protection_cleanup.get(
                        "confirmed_all_canceled"
                    )
                ):
                    pending_orders = final_protection_cleanup.get(
                        "updated_orders", placed_protection_orders
                    )
                    self._save_emergency_cleanup_pending(
                        plan,
                        emergency_close_quantity,
                        str(exc),
                        final_protection_cleanup,
                        emergency_cleanup,
                        pending_orders,
                    )
                else:
                    self._save_execution_cleanup_resolved(
                        plan,
                        str(exc),
                        final_protection_cleanup,
                        emergency_cleanup,
                        placed_protection_orders,
                    )
                self.logger.error(
                    "%s emergency cleanup result | symbol=%s audit=%s",
                    strategy_label,
                    symbol,
                    margin_capped_audit,
                )
                raise BinanceAPIError(
                    f"{strategy_label} post-fill adjustment or protection failed for "
                    f"{symbol}: {margin_capped_audit}"
                ) from exc

            emergency_close_order = None
            emergency_close_error = None
            try:
                emergency_close_order = self.client.close_position_market(
                    symbol,
                    emergency_close_quantity,
                )
            except Exception as close_exc:
                emergency_close_error = str(close_exc)
                self.logger.exception("Emergency close also failed: %s", symbol)
            if isinstance(exc, BinanceAPIError):
                raise
            raise BinanceAPIError(str(exc)) from exc

        orders = {
            "plan": self._plan_payload(execution_plan),
            "pretrade_plan": self._plan_payload(plan),
            "entry_price_source": entry_price_source,
            "open": open_order,
            "stop": stop_order,
            "take_profit": take_profit_order,
        }
        if post_fill_adjustment is not None:
            orders["post_fill_adjustment"] = post_fill_adjustment
        structure_context = (
            plan.structure_context
            if isinstance(plan.structure_context, dict)
            else {}
        )
        strategy_id = structure_context.get("strategy_id")
        if not isinstance(strategy_id, str) or not strategy_id:
            if plan.stop_mode in margin_capped_stop_modes:
                strategy_id = self._margin_capped_strategy_label(plan)
            else:
                strategy_id = None
        strategy_payload = self._strategy_order_payload(plan, strategy_id)
        if strategy_payload is not None:
            orders["strategy"] = strategy_payload
        state = PositionState(
            symbol=symbol,
            quantity=decimal_to_api(execution_plan.quantity),
            entry_price=decimal_to_api(execution_plan.entry_price),
            stop_loss_price=decimal_to_api(execution_plan.stop_loss_price),
            take_profit_price=decimal_to_api(execution_plan.take_profit_price),
            leverage=execution_plan.leverage,
            opened_at=datetime.now(timezone.utc).isoformat(),
            dry_run=self.config.dry_run,
            orders=orders,
        )
        self.state.save(state)
        return state
