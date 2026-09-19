from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class StructureCandle:
    index: int
    open_time: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal


@dataclass(frozen=True)
class Pivot:
    index: int
    price: Decimal
    open_time: str

    def to_jsonable(self) -> dict[str, Any]:
        return {"index": self.index, "price": str(self.price), "open_time": self.open_time}


@dataclass(frozen=True)
class SwingSegmentConfig:
    min_bars: int = 5
    atr_period: int = 14
    min_atr_multiple: Decimal = Decimal("1.2")
    min_efficiency: Decimal = Decimal("0.35")

    def __post_init__(self) -> None:
        if self.min_bars < 1 or self.atr_period < 1:
            raise ValueError("swing segment bar and ATR periods must be positive")
        if self.min_atr_multiple < 0:
            raise ValueError("swing segment ATR multiple cannot be negative")
        if not Decimal("0") <= self.min_efficiency <= Decimal("1"):
            raise ValueError("swing segment efficiency must be between zero and one")

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "min_bars": self.min_bars,
            "atr_period": self.atr_period,
            "min_atr_multiple": str(self.min_atr_multiple),
            "min_efficiency": str(self.min_efficiency),
        }


@dataclass(frozen=True)
class SwingSegmentMetrics:
    passed: bool
    reason: str
    direction: str
    start_index: int
    end_index: int
    start_time: str
    end_time: str
    bar_distance: int
    slope: Decimal
    atr: Decimal
    net_displacement: Decimal
    required_displacement: Decimal
    efficiency: Decimal

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "reason": self.reason,
            "direction": self.direction,
            "start_index": self.start_index,
            "end_index": self.end_index,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "bar_distance": self.bar_distance,
            "slope": str(self.slope),
            "atr": str(self.atr),
            "net_displacement": str(self.net_displacement),
            "required_displacement": str(self.required_displacement),
            "efficiency": str(self.efficiency),
        }


@dataclass(frozen=True)
class PriorDowntrend:
    highs: tuple[Pivot, Pivot, Pivot]
    lows: tuple[Pivot, Pivot, Pivot]
    segments: tuple[SwingSegmentMetrics, ...]

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "highs": [pivot.to_jsonable() for pivot in self.highs],
            "lows": [pivot.to_jsonable() for pivot in self.lows],
            "segments": [segment.to_jsonable() for segment in self.segments],
        }


@dataclass(frozen=True)
class DoubleBreakSkeleton:
    symbol: str
    h0: Decimal
    h1: Decimal
    p1: Decimal
    h0_index: int
    h1_index: int
    p1_index: int
    first_break_index: int
    second_break_index: int
    h0_time: str
    h1_time: str
    p1_time: str
    first_break_time: str
    second_break_time: str
    structure_id: str
    segment_config: SwingSegmentConfig
    prior_downtrend: PriorDowntrend
    first_up_segment: SwingSegmentMetrics
    first_pullback_segment: SwingSegmentMetrics
    second_up_segment: SwingSegmentMetrics

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "h0": str(self.h0),
            "h1": str(self.h1),
            "p1": str(self.p1),
            "h0_index": self.h0_index,
            "h1_index": self.h1_index,
            "p1_index": self.p1_index,
            "first_break_index": self.first_break_index,
            "second_break_index": self.second_break_index,
            "h0_time": self.h0_time,
            "h1_time": self.h1_time,
            "p1_time": self.p1_time,
            "first_break_time": self.first_break_time,
            "second_break_time": self.second_break_time,
            "structure_id": self.structure_id,
            "segment_config": self.segment_config.to_jsonable(),
            "prior_downtrend": self.prior_downtrend.to_jsonable(),
            "first_up_segment": self.first_up_segment.to_jsonable(),
            "first_pullback_segment": self.first_pullback_segment.to_jsonable(),
            "second_up_segment": self.second_up_segment.to_jsonable(),
        }


def parse_structure_klines(raw_klines: list[list[Any]]) -> list[StructureCandle]:
    return [
        StructureCandle(
            index=index,
            open_time=str(row[0]),
            open=Decimal(str(row[1])),
            high=Decimal(str(row[2])),
            low=Decimal(str(row[3])),
            close=Decimal(str(row[4])),
        )
        for index, row in enumerate(raw_klines)
    ]


def find_pivot_highs(candles: list[StructureCandle], left: int, right: int) -> list[Pivot]:
    pivots: list[Pivot] = []
    for index in range(left, len(candles) - right):
        candle = candles[index]
        left_highs = [item.high for item in candles[index - left : index]]
        right_highs = [item.high for item in candles[index + 1 : index + right + 1]]
        if candle.high > max(left_highs) and candle.high >= max(right_highs):
            pivots.append(Pivot(index=index, price=candle.high, open_time=candle.open_time))
    return pivots


def find_pivot_lows(candles: list[StructureCandle], left: int, right: int) -> list[Pivot]:
    pivots: list[Pivot] = []
    for index in range(left, len(candles) - right):
        candle = candles[index]
        left_lows = [item.low for item in candles[index - left : index]]
        right_lows = [item.low for item in candles[index + 1 : index + right + 1]]
        if candle.low < min(left_lows) and candle.low <= min(right_lows):
            pivots.append(Pivot(index=index, price=candle.low, open_time=candle.open_time))
    return pivots


def _linear_regression_slope(values: list[Decimal]) -> Decimal:
    count = Decimal(len(values))
    x_values = [Decimal(index) for index in range(len(values))]
    sum_x = sum(x_values, Decimal("0"))
    sum_y = sum(values, Decimal("0"))
    sum_xx = sum((value * value for value in x_values), Decimal("0"))
    sum_xy = sum((x * y for x, y in zip(x_values, values)), Decimal("0"))
    denominator = count * sum_xx - sum_x * sum_x
    if denominator == 0:
        return Decimal("0")
    return (count * sum_xy - sum_x * sum_y) / denominator


def _atr_at(candles: list[StructureCandle], end_index: int, period: int) -> Decimal:
    start_index = max(0, end_index - period + 1)
    true_ranges: list[Decimal] = []
    for index in range(start_index, end_index + 1):
        candle = candles[index]
        if index == 0:
            true_range = candle.high - candle.low
        else:
            previous_close = candles[index - 1].close
            true_range = max(
                candle.high - candle.low,
                abs(candle.high - previous_close),
                abs(candle.low - previous_close),
            )
        true_ranges.append(true_range)
    if not true_ranges:
        return Decimal("0")
    return sum(true_ranges, Decimal("0")) / Decimal(len(true_ranges))


def evaluate_swing_segment(
    candles: list[StructureCandle],
    start_index: int,
    end_index: int,
    direction: str,
    config: SwingSegmentConfig,
) -> SwingSegmentMetrics:
    if direction not in {"UP", "DOWN"}:
        raise ValueError("swing segment direction must be UP or DOWN")
    if not (0 <= start_index < len(candles)) or not (0 <= end_index < len(candles)):
        raise ValueError("swing segment indices are out of range")
    if end_index <= start_index:
        raise ValueError("swing segment end must follow start")

    closes = [candle.close for candle in candles[start_index : end_index + 1]]
    slope = _linear_regression_slope(closes)
    atr = _atr_at(candles, end_index, config.atr_period)
    displacement = abs(closes[-1] - closes[0])
    required_displacement = atr * config.min_atr_multiple
    path_distance = sum(
        (abs(closes[index] - closes[index - 1]) for index in range(1, len(closes))),
        Decimal("0"),
    )
    efficiency = displacement / path_distance if path_distance > 0 else Decimal("0")
    bar_distance = end_index - start_index
    if bar_distance < config.min_bars:
        reason = "SEGMENT_TOO_SHORT"
    elif (direction == "UP" and closes[-1] <= closes[0]) or (
        direction == "DOWN" and closes[-1] >= closes[0]
    ):
        reason = "SEGMENT_ENDPOINT_DIRECTION_MISMATCH"
    elif (direction == "UP" and slope <= 0) or (direction == "DOWN" and slope >= 0):
        reason = "SEGMENT_WRONG_DIRECTION"
    elif displacement < required_displacement:
        reason = "SEGMENT_DISPLACEMENT_TOO_SMALL"
    elif efficiency < config.min_efficiency:
        reason = "SEGMENT_EFFICIENCY_TOO_LOW"
    else:
        reason = "PASSED"
    return SwingSegmentMetrics(
        passed=reason == "PASSED",
        reason=reason,
        direction=direction,
        start_index=start_index,
        end_index=end_index,
        start_time=candles[start_index].open_time,
        end_time=candles[end_index].open_time,
        bar_distance=bar_distance,
        slope=slope,
        atr=atr,
        net_displacement=displacement,
        required_displacement=required_displacement,
        efficiency=efficiency,
    )


def _first_close_break(
    candles: list[StructureCandle], start_index: int, break_price: Decimal
) -> int | None:
    for index in range(start_index, len(candles)):
        if candles[index].close > break_price:
            return index
    return None


def _pivot_between(pivots: list[Pivot], start: int, end: int) -> Pivot | None:
    matches = [pivot for pivot in pivots if start < pivot.index < end]
    return min(matches, key=lambda pivot: pivot.price) if matches else None


def _prior_downtrend(
    h0: Pivot,
    first_break_index: int,
    high_pivots: list[Pivot],
    low_pivots: list[Pivot],
    candles: list[StructureCandle],
    config: SwingSegmentConfig,
) -> PriorDowntrend | None:
    try:
        h0_position = high_pivots.index(h0)
    except ValueError:
        return None
    if h0_position < 2:
        return None
    highs = tuple(high_pivots[h0_position - 2 : h0_position + 1])
    if not (highs[0].price > highs[1].price > highs[2].price):
        return None
    low1 = _pivot_between(low_pivots, highs[0].index, highs[1].index)
    low2 = _pivot_between(low_pivots, highs[1].index, highs[2].index)
    low3 = _pivot_between(low_pivots, highs[2].index, first_break_index)
    if low1 is None or low2 is None or low3 is None:
        return None
    lows = (low1, low2, low3)
    if not (lows[0].price > lows[1].price > lows[2].price):
        return None
    endpoints = (
        (highs[0].index, lows[0].index, "DOWN"),
        (lows[0].index, highs[1].index, "UP"),
        (highs[1].index, lows[1].index, "DOWN"),
        (lows[1].index, highs[2].index, "UP"),
        (highs[2].index, lows[2].index, "DOWN"),
    )
    segments = tuple(
        evaluate_swing_segment(candles, start, end, direction, config)
        for start, end, direction in endpoints
    )
    if not all(segment.passed for segment in segments):
        return None
    return PriorDowntrend(highs=highs, lows=lows, segments=segments)


def _structure_id(symbol: str, parts: list[Any]) -> str:
    raw = "|".join([symbol, *(str(part) for part in parts)])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def extract_double_break_skeletons(
    symbol: str,
    closed_candles: list[StructureCandle],
    pivot_left: int,
    pivot_right: int,
    segment_config: SwingSegmentConfig | None = None,
) -> list[DoubleBreakSkeleton]:
    config = segment_config or SwingSegmentConfig()
    high_pivots = find_pivot_highs(closed_candles, pivot_left, pivot_right)
    low_pivots = find_pivot_lows(closed_candles, pivot_left, pivot_right)
    skeletons: list[DoubleBreakSkeleton] = []

    for h0 in high_pivots:
        first_break_index = _first_close_break(closed_candles, h0.index + 1, h0.price)
        if first_break_index is None:
            continue
        prior = _prior_downtrend(
            h0, first_break_index, high_pivots, low_pivots, closed_candles, config
        )
        if prior is None:
            continue
        for p1 in (pivot for pivot in low_pivots if pivot.index > first_break_index):
            h1_candidates = [
                pivot
                for pivot in high_pivots
                if first_break_index <= pivot.index < p1.index
            ]
            if not h1_candidates:
                continue
            h1 = max(h1_candidates, key=lambda pivot: (pivot.price, pivot.index))
            if h1.price <= h0.price or p1.price >= h1.price:
                continue
            if max(
                candle.high for candle in closed_candles[h1.index : p1.index + 1]
            ) > h1.price:
                continue
            pullback_lows = [
                candle.low
                for candle in closed_candles[h1.index + 1 : p1.index + 1]
            ]
            pullback_min = min(pullback_lows)
            first_pullback_min_index = h1.index + 1 + pullback_lows.index(pullback_min)
            if p1.price != pullback_min or p1.index != first_pullback_min_index:
                continue
            first_up = evaluate_swing_segment(
                closed_candles, prior.lows[-1].index, h1.index, "UP", config
            )
            if not first_up.passed:
                continue
            first_pullback = evaluate_swing_segment(
                closed_candles, h1.index, p1.index, "DOWN", config
            )
            if not first_pullback.passed:
                continue
            second_break_index = _first_close_break(
                closed_candles, p1.index + 1, h1.price
            )
            if second_break_index is None:
                continue
            if min(
                candle.low
                for candle in closed_candles[p1.index : second_break_index + 1]
            ) < p1.price:
                continue
            second_up = evaluate_swing_segment(
                closed_candles, p1.index, second_break_index, "UP", config
            )
            if not second_up.passed:
                continue
            first_break = closed_candles[first_break_index]
            second_break = closed_candles[second_break_index]
            structure_id = _structure_id(
                symbol,
                [
                    h0.open_time,
                    first_break.open_time,
                    h1.open_time,
                    p1.open_time,
                    second_break.open_time,
                    h0.price,
                    h1.price,
                    p1.price,
                ],
            )
            skeletons.append(
                DoubleBreakSkeleton(
                    symbol=symbol,
                    h0=h0.price,
                    h1=h1.price,
                    p1=p1.price,
                    h0_index=h0.index,
                    h1_index=h1.index,
                    p1_index=p1.index,
                    first_break_index=first_break_index,
                    second_break_index=second_break_index,
                    h0_time=h0.open_time,
                    h1_time=h1.open_time,
                    p1_time=p1.open_time,
                    first_break_time=first_break.open_time,
                    second_break_time=second_break.open_time,
                    structure_id=structure_id,
                    segment_config=config,
                    prior_downtrend=prior,
                    first_up_segment=first_up,
                    first_pullback_segment=first_pullback,
                    second_up_segment=second_up,
                )
            )
    unique: dict[str, DoubleBreakSkeleton] = {}
    for skeleton in skeletons:
        unique.setdefault(skeleton.structure_id, skeleton)
    return list(unique.values())


def find_effective_second_highs(
    candles: list[StructureCandle],
    skeleton: DoubleBreakSkeleton,
    pivot_left: int,
    pivot_right: int,
    segment_config: SwingSegmentConfig | None = None,
) -> list[tuple[Pivot, SwingSegmentMetrics]]:
    config = segment_config or skeleton.segment_config
    results: list[tuple[Pivot, SwingSegmentMetrics]] = []
    for pivot in find_pivot_highs(candles, pivot_left, pivot_right):
        if pivot.index < skeleton.second_break_index or pivot.price <= skeleton.h1:
            continue
        segment = evaluate_swing_segment(
            candles,
            skeleton.p1_index,
            pivot.index,
            "UP",
            config,
        )
        if segment.passed:
            results.append((pivot, segment))
    return results
