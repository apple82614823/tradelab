from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .double_break import (
    DoubleBreakSkeleton,
    Pivot,
    SwingSegmentConfig,
    SwingSegmentMetrics,
    evaluate_swing_segment,
    extract_double_break_skeletons,
    find_effective_second_highs,
    parse_structure_klines,
)


N07_DISTANCE_MIN = Decimal("0.01")
N07_DISTANCE_MAX = Decimal("0.015")
N07_PULLBACK_MIN = Decimal("0.06")


@dataclass(frozen=True)
class N07AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    current_bullish: bool
    structure: DoubleBreakSkeleton | None
    current_price: Decimal
    distance_pct: Decimal | None
    post_break_low: Decimal | None
    detail: str
    distance_min: Decimal = N07_DISTANCE_MIN
    distance_max: Decimal = N07_DISTANCE_MAX
    pullback_pct: Decimal | None = None
    pullback_min: Decimal = N07_PULLBACK_MIN
    zone_lower: Decimal | None = None
    zone_upper: Decimal | None = None
    earlier_post_break_low: Decimal | None = None
    current_low: Decimal | None = None
    h2: Pivot | None = None
    entry_retrace_segment: SwingSegmentMetrics | None = None
    segment_config: SwingSegmentConfig = SwingSegmentConfig()

    @property
    def structure_id(self) -> str | None:
        return self.structure.structure_id if self.structure else None

    def detail_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "reason": self.reason,
            "current_bullish": self.current_bullish,
            "current_price": str(self.current_price),
            "current_low": str(self.current_low) if self.current_low is not None else None,
            "distance_pct": str(self.distance_pct) if self.distance_pct is not None else None,
            "distance_min": str(self.distance_min),
            "distance_max": str(self.distance_max),
            "pullback_pct": str(self.pullback_pct) if self.pullback_pct is not None else None,
            "pullback_min": str(self.pullback_min),
            "zone_lower": str(self.zone_lower) if self.zone_lower is not None else None,
            "zone_upper": str(self.zone_upper) if self.zone_upper is not None else None,
            "post_break_low": str(self.post_break_low) if self.post_break_low is not None else None,
            "earlier_post_break_low": (
                str(self.earlier_post_break_low)
                if self.earlier_post_break_low is not None
                else None
            ),
            "h2": self.h2.to_jsonable() if self.h2 is not None else None,
            "entry_retrace_segment": (
                self.entry_retrace_segment.to_jsonable()
                if self.entry_retrace_segment is not None
                else None
            ),
            "segment_config": self.segment_config.to_jsonable(),
        }
        if self.structure is not None:
            payload["structure"] = self.structure.to_jsonable()
        return payload


def analyze_n07_p1_retest(
    symbol: str,
    raw_klines: list[list[Any]],
    pivot_left: int = 2,
    pivot_right: int = 2,
    distance_min: Decimal = N07_DISTANCE_MIN,
    distance_max: Decimal = N07_DISTANCE_MAX,
    pullback_min: Decimal = N07_PULLBACK_MIN,
    segment_min_bars: int = 5,
    segment_atr_period: int = 14,
    segment_min_atr_multiple: Decimal = Decimal("1.2"),
    segment_min_efficiency: Decimal = Decimal("0.35"),
) -> N07AnalysisResult:
    if pivot_left < 1 or pivot_right < 1:
        raise ValueError("pivot_left and pivot_right must be positive")
    if distance_min < 0 or distance_max < distance_min:
        raise ValueError("N07 distance bounds are invalid")
    if not Decimal("0") <= pullback_min < Decimal("1"):
        raise ValueError("N07 pullback minimum is invalid")
    segment_config = SwingSegmentConfig(
        min_bars=segment_min_bars,
        atr_period=segment_atr_period,
        min_atr_multiple=segment_min_atr_multiple,
        min_efficiency=segment_min_efficiency,
    )

    candles = parse_structure_klines(raw_klines)
    if len(candles) < pivot_left + pivot_right + 8:
        return N07AnalysisResult(
            symbol, False, "NOT_ENOUGH_KLINES", False, None, Decimal("0"), None,
            None, "not enough kline data", distance_min, distance_max,
            pullback_min=pullback_min, segment_config=segment_config,
        )

    current = candles[-1]
    closed = candles[:-1]
    current_bullish = current.close > current.open
    skeletons = extract_double_break_skeletons(
        symbol, closed, pivot_left, pivot_right, segment_config
    )
    if not skeletons:
        return N07AnalysisResult(
            symbol, False, "STRUCTURE_NOT_FOUND", current_bullish, None,
            current.close, None, current.low, "double break skeleton not found",
            distance_min, distance_max, pullback_min=pullback_min,
            current_low=current.low, segment_config=segment_config,
        )

    structure = max(skeletons, key=lambda item: (item.second_break_index, item.h0_index))
    if structure.p1 <= 0 or structure.h1 <= 0:
        return N07AnalysisResult(
            symbol, False, "INVALID_P1", current_bullish, structure,
            current.close, None, current.low, "P1 and H1 must be positive",
            distance_min, distance_max, pullback_min=pullback_min,
            current_low=current.low, segment_config=segment_config,
        )

    pullback_pct = (structure.h1 - structure.p1) / structure.h1
    zone_lower = structure.p1 * (Decimal("1") + distance_min)
    zone_upper = structure.p1 * (Decimal("1") + distance_max)
    distance_pct = (current.close - structure.p1) / structure.p1
    earlier = closed[structure.second_break_index + 1 :]
    earlier_low = min((candle.low for candle in earlier), default=None)
    post_break_low = min([candle.low for candle in earlier] + [current.low])

    h2: Pivot | None = None
    retrace: SwingSegmentMetrics | None = None

    def result(passed: bool, reason: str, detail: str) -> N07AnalysisResult:
        return N07AnalysisResult(
            symbol=symbol,
            passed=passed,
            reason=reason,
            current_bullish=current_bullish,
            structure=structure,
            current_price=current.close,
            distance_pct=distance_pct,
            post_break_low=post_break_low,
            detail=detail,
            distance_min=distance_min,
            distance_max=distance_max,
            pullback_pct=pullback_pct,
            pullback_min=pullback_min,
            zone_lower=zone_lower,
            zone_upper=zone_upper,
            earlier_post_break_low=earlier_low,
            current_low=current.low,
            h2=h2,
            entry_retrace_segment=retrace,
            segment_config=segment_config,
        )

    if pullback_pct < pullback_min:
        return result(
            False,
            "N07_PULLBACK_TOO_SHALLOW",
            f"structure={structure.structure_id} pullback={pullback_pct} minimum={pullback_min}",
        )
    if earlier_low is not None and earlier_low <= structure.p1:
        return result(
            False,
            "N07_P1_TOUCHED_AFTER_SECOND_BREAK",
            f"structure={structure.structure_id} earlier_low={earlier_low} p1={structure.p1}",
        )
    if earlier_low is not None and earlier_low <= zone_upper:
        return result(
            False,
            "N07_HISTORICAL_ZONE_TOUCH_MISSED",
            f"structure={structure.structure_id} earlier_low={earlier_low} zone_upper={zone_upper}",
        )

    second_highs = find_effective_second_highs(
        closed, structure, pivot_left, pivot_right, segment_config
    )
    if second_highs:
        h2, _ = max(second_highs, key=lambda item: (item[0].price, item[0].index))
        if len(closed) - 1 > h2.index:
            candidate_retrace = evaluate_swing_segment(
                closed, h2.index, len(closed) - 1, "DOWN", segment_config
            )
            if candidate_retrace.passed:
                retrace = candidate_retrace
    if current.low <= structure.p1:
        return result(
            False,
            "N07_P1_TOUCHED_AFTER_SECOND_BREAK",
            f"structure={structure.structure_id} current_low={current.low} p1={structure.p1}",
        )
    if current.low < zone_lower:
        return result(
            False,
            "N07_ENTRY_ZONE_OVERSHOT",
            f"structure={structure.structure_id} current_low={current.low} zone_lower={zone_lower}",
        )
    if current.close < zone_lower:
        return result(
            False,
            "N07_ENTRY_PRICE_BELOW_ZONE",
            f"structure={structure.structure_id} current_close={current.close} zone_lower={zone_lower}",
        )
    if current.low <= zone_upper and current.close > zone_upper:
        return result(
            False,
            "N07_ENTRY_TOUCH_REBOUNDED",
            f"structure={structure.structure_id} current_low={current.low} current_close={current.close}",
        )
    if zone_lower <= current.close <= zone_upper:
        if retrace is None:
            return result(
                False,
                "N07_INVALID_FIRST_TOUCH_SEGMENT",
                f"structure={structure.structure_id} first zone touch lacks effective H2 retrace",
            )
        return result(
            True,
            "PASSED",
            f"structure={structure.structure_id} p1={structure.p1} distance={distance_pct} "
            f"pullback={pullback_pct} h2={h2.price if h2 else None}",
        )
    waiting_reason = (
        "N07_EFFECTIVE_RETRACE_NOT_CONFIRMED"
        if retrace is None
        else "N07_WAITING_FIRST_ZONE_TOUCH"
    )
    return result(
        False,
        waiting_reason,
        f"structure={structure.structure_id} current_low={current.low} zone_upper={zone_upper}",
    )
