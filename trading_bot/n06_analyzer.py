from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .double_break import (
    DoubleBreakSkeleton,
    SwingSegmentConfig,
    SwingSegmentMetrics,
    StructureCandle,
    evaluate_swing_segment,
    extract_double_break_skeletons,
    find_effective_second_highs,
    find_pivot_lows,
    parse_structure_klines,
)


FIFTEEN_MINUTES_MS = 15 * 60 * 1000


@dataclass(frozen=True)
class N06Structure:
    h0: Decimal
    h1: Decimal
    p1: Decimal
    h2: Decimal
    p2: Decimal
    stable_count: int
    h0_index: int
    h1_index: int
    p1_index: int
    h2_index: int
    p2_index: int
    first_break_index: int
    second_break_index: int
    stable_confirm_index: int
    h0_time: str
    h1_time: str
    p1_time: str
    h2_time: str
    p2_time: str
    first_break_time: str
    second_break_time: str
    stable_confirm_time: str
    structure_id: str
    base_skeleton: DoubleBreakSkeleton
    second_up_complete_segment: SwingSegmentMetrics
    h2_p2_pullback_segment: SwingSegmentMetrics

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "h0": str(self.h0),
            "h1": str(self.h1),
            "p1": str(self.p1),
            "h2": str(self.h2),
            "p2": str(self.p2),
            "stable_count": self.stable_count,
            "h0_index": self.h0_index,
            "h1_index": self.h1_index,
            "p1_index": self.p1_index,
            "h2_index": self.h2_index,
            "p2_index": self.p2_index,
            "first_break_index": self.first_break_index,
            "second_break_index": self.second_break_index,
            "stable_confirm_index": self.stable_confirm_index,
            "h0_time": self.h0_time,
            "h1_time": self.h1_time,
            "p1_time": self.p1_time,
            "h2_time": self.h2_time,
            "p2_time": self.p2_time,
            "first_break_time": self.first_break_time,
            "second_break_time": self.second_break_time,
            "stable_confirm_time": self.stable_confirm_time,
            "structure_id": self.structure_id,
            "segment_config": self.base_skeleton.segment_config.to_jsonable(),
            "prior_downtrend": self.base_skeleton.prior_downtrend.to_jsonable(),
            "first_up_segment": self.base_skeleton.first_up_segment.to_jsonable(),
            "first_pullback_segment": self.base_skeleton.first_pullback_segment.to_jsonable(),
            "second_up_to_break_segment": self.base_skeleton.second_up_segment.to_jsonable(),
            "second_up_complete_segment": self.second_up_complete_segment.to_jsonable(),
            "h2_p2_pullback_segment": self.h2_p2_pullback_segment.to_jsonable(),
        }


@dataclass(frozen=True)
class N06AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    current_bullish: bool
    structure: N06Structure | None
    detail: str
    current_open_time: str | None = None
    checked_at: str | None = None
    elapsed_ms: int | None = None
    entry_window_ms: int = 120_000
    historical_missed: tuple[N06Structure, ...] = ()

    @property
    def structure_id(self) -> str | None:
        return self.structure.structure_id if self.structure else None

    def detail_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "reason": self.reason,
            "current_bullish": self.current_bullish,
            "current_open_time": self.current_open_time,
            "checked_at": self.checked_at,
            "elapsed_ms": self.elapsed_ms,
            "entry_window_ms": self.entry_window_ms,
            "entry_deadline_ms": (
                int(Decimal(self.current_open_time)) + self.entry_window_ms
                if self.current_open_time is not None
                else None
            ),
            "historical_missed_structure_ids": [
                structure.structure_id for structure in self.historical_missed
            ],
        }
        if self.structure:
            payload["structure"] = self.structure.to_jsonable()
        return payload


def _build_structure(
    candles: list[StructureCandle],
    skeleton: DoubleBreakSkeleton,
    stable_count_required: int,
    pivot_left: int,
    pivot_right: int,
    segment_config: SwingSegmentConfig,
) -> N06Structure | None:
    low_pivots = find_pivot_lows(candles, pivot_left, pivot_right)
    second_highs = find_effective_second_highs(
        candles, skeleton, pivot_left, pivot_right, segment_config
    )
    if not second_highs:
        return None
    completed: list[N06Structure] = []
    for p2 in (
        pivot for pivot in low_pivots if pivot.index > skeleton.second_break_index
    ):
        stable_confirm_index = p2.index + stable_count_required
        if stable_confirm_index >= len(candles):
            continue
        eligible_highs = [
            item for item in second_highs if item[0].index < p2.index
        ]
        if not eligible_highs:
            continue
        h2, second_up_complete = max(
            eligible_highs, key=lambda item: (item[0].price, item[0].index)
        )
        if max(
            candle.high
            for candle in candles[skeleton.second_break_index : p2.index + 1]
        ) > h2.price:
            continue
        if p2.price <= skeleton.p1:
            continue
        if any(
            candle.low <= skeleton.p1
            for candle in candles[
                skeleton.second_break_index + 1 : stable_confirm_index + 1
            ]
        ):
            continue
        pullback = evaluate_swing_segment(
            candles, h2.index, p2.index, "DOWN", segment_config
        )
        if not pullback.passed:
            continue
        if p2.price != min(
            candle.low
            for candle in candles[h2.index + 1 : stable_confirm_index + 1]
        ):
            continue
        stable_confirm_candle = candles[stable_confirm_index]
        completed.append(N06Structure(
            h0=skeleton.h0,
            h1=skeleton.h1,
            p1=skeleton.p1,
            h2=h2.price,
            p2=p2.price,
            stable_count=stable_count_required,
            h0_index=skeleton.h0_index,
            h1_index=skeleton.h1_index,
            p1_index=skeleton.p1_index,
            h2_index=h2.index,
            p2_index=p2.index,
            first_break_index=skeleton.first_break_index,
            second_break_index=skeleton.second_break_index,
            stable_confirm_index=stable_confirm_index,
            h0_time=skeleton.h0_time,
            h1_time=skeleton.h1_time,
            p1_time=skeleton.p1_time,
            h2_time=h2.open_time,
            p2_time=p2.open_time,
            first_break_time=skeleton.first_break_time,
            second_break_time=skeleton.second_break_time,
            stable_confirm_time=stable_confirm_candle.open_time,
            structure_id=skeleton.structure_id,
            base_skeleton=skeleton,
            second_up_complete_segment=second_up_complete,
            h2_p2_pullback_segment=pullback,
        ))
    return min(
        completed,
        key=lambda item: (item.stable_confirm_index, item.p2_index, item.h2_index),
    ) if completed else None


def analyze_n06_double_break_pullback(
    symbol: str,
    raw_klines: list[list[Any]],
    pivot_left: int = 2,
    pivot_right: int = 2,
    stable_count_required: int = 5,
    entry_window_seconds: int = 120,
    checked_at_ms: int | None = None,
    segment_min_bars: int = 5,
    segment_atr_period: int = 14,
    segment_min_atr_multiple: Decimal = Decimal("1.2"),
    segment_min_efficiency: Decimal = Decimal("0.35"),
) -> N06AnalysisResult:
    if pivot_left < 1 or pivot_right < 1 or stable_count_required < 1:
        raise ValueError("pivot_left, pivot_right and stable_count_required must be positive")
    if entry_window_seconds <= 0:
        raise ValueError("entry_window_seconds must be positive")

    entry_window_ms = entry_window_seconds * 1000
    segment_config = SwingSegmentConfig(
        min_bars=segment_min_bars,
        atr_period=segment_atr_period,
        min_atr_multiple=segment_min_atr_multiple,
        min_efficiency=segment_min_efficiency,
    )
    candles = parse_structure_klines(raw_klines)
    min_required = pivot_left + pivot_right + stable_count_required + 8
    if len(candles) < min_required:
        return N06AnalysisResult(
            symbol,
            False,
            "NOT_ENOUGH_KLINES",
            False,
            None,
            "not enough kline data",
            entry_window_ms=entry_window_ms,
        )

    current = candles[-1]
    closed = candles[:-1]
    current_bullish = current.close > current.open
    # Production supplies checked_at_ms. Direct analysis treats the response as
    # an immediate snapshot so deterministic fixtures do not depend on wall time.
    now_ms = checked_at_ms if checked_at_ms is not None else int(Decimal(current.open_time))
    checked_at = datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat()
    completed_by_id: dict[str, N06Structure] = {}
    skeletons = extract_double_break_skeletons(
        symbol, closed, pivot_left, pivot_right, segment_config
    )
    for skeleton in skeletons:
        structure = _build_structure(
            closed,
            skeleton,
            stable_count_required,
            pivot_left,
            pivot_right,
            segment_config,
        )
        if structure is None:
            continue
        previous = completed_by_id.get(structure.structure_id)
        if previous is None or structure.stable_confirm_index < previous.stable_confirm_index:
            completed_by_id[structure.structure_id] = structure

    completed = sorted(
        completed_by_id.values(),
        key=lambda item: (item.stable_confirm_index, item.h0_index),
    )
    if not completed:
        reason = "N06_H2_P2_NOT_CONFIRMED" if skeletons else "STRUCTURE_NOT_FOUND"
        return N06AnalysisResult(
            symbol,
            False,
            reason,
            current_bullish,
            None,
            "confirmed H1/P1 exists but effective H2/P2 is not complete"
            if skeletons
            else "double break pullback structure not found",
            current.open_time,
            checked_at,
            None,
            entry_window_ms,
        )

    latest_closed_index = len(closed) - 1
    historical_missed = tuple(
        structure
        for structure in completed
        if structure.stable_confirm_index < latest_closed_index
    )
    current_structures = [
        structure
        for structure in completed
        if structure.stable_confirm_index == latest_closed_index
    ]
    if not current_structures:
        structure = completed[-1]
        return N06AnalysisResult(
            symbol,
            False,
            "N06_HISTORICAL_STRUCTURE_MISSED",
            current_bullish,
            structure,
            f"structure={structure.structure_id} stable confirmation is historical",
            current.open_time,
            checked_at,
            None,
            entry_window_ms,
            historical_missed,
        )

    structure = max(current_structures, key=lambda item: item.h0_index)
    current_open_ms = int(Decimal(current.open_time))
    expected_current_open_ms = int(Decimal(structure.stable_confirm_time)) + FIFTEEN_MINUTES_MS
    elapsed_ms = now_ms - current_open_ms
    if current_open_ms != expected_current_open_ms:
        reason = "N06_ENTRY_CANDLE_MISMATCH_MISSED"
        passed = False
    elif elapsed_ms < 0:
        reason = "N06_ENTRY_WINDOW_NOT_OPEN"
        passed = False
    elif elapsed_ms >= entry_window_ms:
        reason = "N06_ENTRY_WINDOW_MISSED"
        passed = False
    elif not current_bullish:
        reason = "CURRENT_CANDLE_NOT_BULLISH"
        passed = False
    else:
        reason = "PASSED"
        passed = True

    return N06AnalysisResult(
        symbol,
        passed,
        reason,
        current_bullish,
        structure,
        f"structure={structure.structure_id} p1={structure.p1} p2={structure.p2} "
        f"stable={structure.stable_count} elapsed_ms={elapsed_ms}",
        current.open_time,
        checked_at,
        elapsed_ms,
        entry_window_ms,
        historical_missed,
    )
