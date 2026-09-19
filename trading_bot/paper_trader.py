from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from .precision import decimal_to_api
from .recorder import (
    PaperTradePersistenceError,
    ReviewRecorder,
    StrategyPaperTrade,
)
from .trader import TradePlan, assert_entry_window_open


@dataclass(frozen=True)
class PaperCloseResult:
    trade: StrategyPaperTrade
    result: str
    exit_reason: str
    exit_price: Decimal
    mark_price: Decimal
    r_multiple: Decimal
    conflict: bool = False


class PaperTrader:
    def __init__(
        self,
        recorder: ReviewRecorder,
        logger,
        clock_ms: Callable[[], int] | None = None,
    ):
        self.recorder = recorder
        self.logger = logger
        self.clock_ms = clock_ms or (
            lambda: int(datetime.now(timezone.utc).timestamp() * 1000)
        )

    def has_open_trade(self, strategy_id: str) -> bool:
        return self.recorder.get_open_strategy_paper_trade(strategy_id) is not None

    def open_trade(
        self,
        strategy_id: str,
        symbol: str,
        funding_rate: Decimal | None,
        plan: TradePlan,
        detail: dict,
        opened_at: str | None = None,
    ) -> int | None:
        assert_entry_window_open(plan, self.clock_ms())
        trade_id = self.recorder.open_strategy_paper_trade(
            strategy_id=strategy_id,
            symbol=symbol,
            entry_price=decimal_to_api(plan.entry_price),
            stop_loss_price=decimal_to_api(plan.stop_loss_price),
            take_profit_price=decimal_to_api(plan.take_profit_price),
            funding_rate=str(funding_rate) if funding_rate is not None else "",
            orders={
                "plan": {
                    "leverage": plan.leverage,
                    "quantity": decimal_to_api(plan.quantity),
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
                    "stop_mode": plan.stop_mode,
                    "risk_reward_ratio": decimal_to_api(plan.risk_reward_ratio),
                    "structure_id": plan.structure_id,
                    "structure_stop_price": decimal_to_api(plan.structure_stop_price)
                    if plan.structure_stop_price is not None
                    else None,
                    "structure_target_price": decimal_to_api(plan.structure_target_price)
                    if plan.structure_target_price is not None
                    else None,
                    "stop_loss_pct": decimal_to_api(plan.stop_loss_pct),
                    "take_profit_pct": decimal_to_api(plan.take_profit_pct),
                    "amplitude_24h_pct": decimal_to_api(plan.amplitude_24h_pct),
                    "risk_amount": decimal_to_api(plan.risk_amount),
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
                    "notional_value": decimal_to_api(plan.notional_value),
                    "required_margin": decimal_to_api(plan.required_margin),
                    "balance": decimal_to_api(plan.balance),
                }
            },
            detail=detail,
            opened_at=opened_at,
        )
        if trade_id is not None:
            self.logger.info("Paper trade opened | strategy=%s symbol=%s trade_id=%s", strategy_id, symbol, trade_id)
        return trade_id

    def close_triggered_open_trades(
        self,
        get_1m_klines: Callable[[str, int], list[list[Any]]],
        get_aggregate_trades: Callable[[str, int, int], list[dict[str, Any]]],
        now_ms: int | None = None,
    ) -> list[PaperCloseResult]:
        current_ms = now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
        closed: list[PaperCloseResult] = []
        trades_by_symbol: dict[str, list[StrategyPaperTrade]] = {}
        for trade in self.recorder.get_open_strategy_paper_trades():
            trades_by_symbol.setdefault(trade.symbol, []).append(trade)

        for symbol, trades in trades_by_symbol.items():
            starts = {trade.id: self._paper_check_start_ms(trade) for trade in trades}
            full_minute_starts = {
                trade.id: self._first_full_minute_start(starts[trade.id])
                for trade in trades
            }
            completed_klines: list[list[Any]] = []
            eligible_full_starts = [
                start
                for start in full_minute_starts.values()
                if start + 59999 <= current_ms
            ]
            if eligible_full_starts:
                try:
                    raw_klines = get_1m_klines(symbol, min(eligible_full_starts))
                    completed_klines = self._completed_1m_klines(raw_klines, current_ms)
                except Exception:
                    self.logger.exception("Paper 1m kline fetch failed | symbol=%s", symbol)

            partial_groups: dict[int, list[StrategyPaperTrade]] = {}
            for trade in trades:
                if trade.last_checked_at:
                    continue
                opened_ms = starts[trade.id]
                if opened_ms % 60000 == 0:
                    continue
                minute_start = (opened_ms // 60000) * 60000
                minute_end = minute_start + 59999
                if minute_end <= current_ms:
                    partial_groups.setdefault(minute_start, []).append(trade)

            partial_rows: dict[int, list[dict[str, Any]]] = {}
            failed_partial_minutes: set[int] = set()
            for minute_start, grouped_trades in partial_groups.items():
                request_start = min(starts[trade.id] for trade in grouped_trades)
                minute_end = minute_start + 59999
                try:
                    rows = get_aggregate_trades(symbol, request_start, minute_end)
                    partial_rows[minute_start] = sorted(
                        rows,
                        key=lambda item: (
                            int(item.get("T", -1)),
                            int(item.get("a", -1)),
                        ),
                    )
                except Exception:
                    failed_partial_minutes.add(minute_start)
                    self.logger.exception(
                        "Paper opening-minute aggregate trade fetch failed | symbol=%s minute=%s",
                        symbol,
                        minute_start,
                    )

            for trade in trades:
                latest_checked_ms: int | None = None
                start_ms = starts[trade.id]
                if not trade.last_checked_at and start_ms % 60000 != 0:
                    minute_start = (start_ms // 60000) * 60000
                    minute_end = minute_start + 59999
                    if minute_end > current_ms or minute_start in failed_partial_minutes:
                        continue
                    relevant_trades = [
                        row
                        for row in partial_rows.get(minute_start, [])
                        if start_ms <= int(row.get("T", -1)) <= minute_end
                    ]
                    close_result = self._close_from_aggregate_trades(trade, relevant_trades)
                    if close_result is not None:
                        closed.append(close_result)
                        continue
                    latest_checked_ms = minute_end

                relevant_klines = [
                    row
                    for row in completed_klines
                    if int(row[0]) >= full_minute_starts[trade.id]
                ]
                close_result = self._close_from_1m_klines(trade, relevant_klines)
                if close_result is not None:
                    closed.append(close_result)
                    continue

                if relevant_klines:
                    latest_checked_ms = int(relevant_klines[-1][6])
                if latest_checked_ms is not None:
                    checkpoint_saved = (
                        self.recorder.update_strategy_paper_last_checked(
                            trade.id,
                            datetime.fromtimestamp(
                                latest_checked_ms / 1000,
                                tz=timezone.utc,
                            ).isoformat(),
                        )
                    )
                    if checkpoint_saved is not True:
                        raise PaperTradePersistenceError(
                            "strategy paper checkpoint could not be confirmed"
                        )
        return closed

    def _paper_check_start_ms(self, trade: StrategyPaperTrade) -> int:
        if trade.last_checked_at:
            checked = datetime.fromisoformat(trade.last_checked_at.replace("Z", "+00:00"))
            if checked.tzinfo is None:
                checked = checked.replace(tzinfo=timezone.utc)
            return int(checked.timestamp() * 1000) + 1

        opened = datetime.fromisoformat(trade.opened_at.replace("Z", "+00:00"))
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=timezone.utc)
        opened_ms = int(opened.timestamp() * 1000)
        return opened_ms

    def _first_full_minute_start(self, start_ms: int) -> int:
        if start_ms % 60000 == 0:
            return start_ms
        return ((start_ms // 60000) + 1) * 60000

    def _completed_1m_klines(self, raw_klines: list[list[Any]], now_ms: int) -> list[list[Any]]:
        completed = [row for row in raw_klines if len(row) > 6 and int(row[6]) <= now_ms]
        completed.sort(key=lambda row: int(row[0]))
        return completed

    def _close_from_1m_klines(
        self,
        trade: StrategyPaperTrade,
        klines: list[list[Any]],
    ) -> PaperCloseResult | None:
        stop_loss_price = Decimal(trade.stop_loss_price)
        take_profit_price = Decimal(trade.take_profit_price)

        for row in klines:
            high = Decimal(str(row[2]))
            low = Decimal(str(row[3]))
            stop_hit = low <= stop_loss_price
            take_profit_hit = high >= take_profit_price
            if not stop_hit and not take_profit_hit:
                continue

            conflict = stop_hit and take_profit_hit
            if stop_hit:
                result = "LOSS"
                exit_reason = "STOP_LOSS"
                exit_price = stop_loss_price
            else:
                result = "WIN"
                exit_reason = "TAKE_PROFIT"
                exit_price = take_profit_price

            trigger_detail = {
                "source": "COMPLETED_1M_KLINE",
                "conflict": conflict,
                "stop_hit": stop_hit,
                "take_profit_hit": take_profit_hit,
                "trigger_bar": {
                    "open_time": str(row[0]),
                    "close_time": str(row[6]),
                    "open": str(row[1]),
                    "high": str(row[2]),
                    "low": str(row[3]),
                    "close": str(row[4]),
                },
            }
            return self._record_close(
                trade=trade,
                result=result,
                exit_reason=exit_reason,
                exit_price=exit_price,
                mark_price=Decimal(str(row[4])),
                conflict=conflict,
                detail=trigger_detail,
            )
        return None

    def _close_from_aggregate_trades(
        self,
        trade: StrategyPaperTrade,
        aggregate_trades: list[dict[str, Any]],
    ) -> PaperCloseResult | None:
        stop_loss_price = Decimal(trade.stop_loss_price)
        take_profit_price = Decimal(trade.take_profit_price)
        rows_by_timestamp: dict[int, list[dict[str, Any]]] = {}
        for row in aggregate_trades:
            rows_by_timestamp.setdefault(int(row.get("T", -1)), []).append(row)

        for trade_time in sorted(rows_by_timestamp):
            rows = rows_by_timestamp[trade_time]
            ids = [int(row.get("a", -1)) for row in rows]
            ordering_reliable = all(trade_id >= 0 for trade_id in ids) and len(set(ids)) == len(ids)
            if ordering_reliable:
                ordered_rows = sorted(rows, key=lambda row: int(row["a"]))
                for row in ordered_rows:
                    price = Decimal(str(row["p"]))
                    if price <= stop_loss_price:
                        return self._record_aggregate_close(
                            trade,
                            row,
                            "LOSS",
                            "STOP_LOSS",
                            stop_loss_price,
                            conflict=False,
                        )
                    if price >= take_profit_price:
                        return self._record_aggregate_close(
                            trade,
                            row,
                            "WIN",
                            "TAKE_PROFIT",
                            take_profit_price,
                            conflict=False,
                        )
                continue

            prices = [Decimal(str(row["p"])) for row in rows]
            stop_hit = any(price <= stop_loss_price for price in prices)
            take_profit_hit = any(price >= take_profit_price for price in prices)
            if not stop_hit and not take_profit_hit:
                continue
            conflict = stop_hit and take_profit_hit
            if stop_hit:
                result = "LOSS"
                exit_reason = "STOP_LOSS"
                exit_price = stop_loss_price
            else:
                result = "WIN"
                exit_reason = "TAKE_PROFIT"
                exit_price = take_profit_price
            detail = {
                "source": "OPENING_PARTIAL_AGG_TRADES",
                "conflict": conflict,
                "ordering_reliable": False,
                "trade_time": str(trade_time),
                "prices": [str(price) for price in prices],
            }
            return self._record_close(
                trade,
                result,
                exit_reason,
                exit_price,
                prices[-1],
                conflict,
                detail,
            )
        return None

    def _record_aggregate_close(
        self,
        trade: StrategyPaperTrade,
        row: dict[str, Any],
        result: str,
        exit_reason: str,
        exit_price: Decimal,
        conflict: bool,
    ) -> PaperCloseResult:
        mark_price = Decimal(str(row["p"]))
        detail = {
            "source": "OPENING_PARTIAL_AGG_TRADES",
            "conflict": conflict,
            "ordering_reliable": True,
            "trigger_trade": {
                "aggregate_trade_id": str(row.get("a", "")),
                "time": str(row.get("T", "")),
                "price": str(row["p"]),
            },
        }
        return self._record_close(
            trade,
            result,
            exit_reason,
            exit_price,
            mark_price,
            conflict,
            detail,
        )

    def _record_close(
        self,
        trade: StrategyPaperTrade,
        result: str,
        exit_reason: str,
        exit_price: Decimal,
        mark_price: Decimal,
        conflict: bool,
        detail: dict[str, Any],
    ) -> PaperCloseResult:
        entry_price = Decimal(trade.entry_price)
        stop_distance = entry_price - Decimal(trade.stop_loss_price)
        r_multiple = Decimal("0")
        if stop_distance > 0:
            r_multiple = (exit_price - entry_price) / stop_distance
        closed = self.recorder.close_strategy_paper_trade(
            trade_id=trade.id,
            result=result,
            exit_reason=exit_reason,
            exit_price=decimal_to_api(exit_price),
            r_multiple=decimal_to_api(r_multiple),
            detail=detail,
        )
        if closed is not True:
            raise PaperTradePersistenceError(
                "strategy paper close could not be confirmed"
            )
        close_result = PaperCloseResult(
            trade=trade,
            result=result,
            exit_reason=exit_reason,
            exit_price=exit_price,
            mark_price=mark_price,
            r_multiple=r_multiple,
            conflict=conflict,
        )
        self.logger.info(
            "Paper trade closed | strategy=%s symbol=%s result=%s reason=%s mark=%s r=%s conflict=%s source=%s",
            trade.strategy_id,
            trade.symbol,
            result,
            exit_reason,
            decimal_to_api(mark_price),
            decimal_to_api(r_multiple),
            conflict,
            detail.get("source"),
        )
        return close_result
