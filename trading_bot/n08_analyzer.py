from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .double_break import (
    StructureCandle,
    find_pivot_highs,
    find_pivot_lows,
    parse_structure_klines,
)


FIFTEEN_MINUTES_MS = 15 * 60 * 1000


@dataclass(frozen=True)
class RangePivot:
    kind: str
    index: int
    open_time: str
    price: Decimal

    def to_jsonable(self, reference: Decimal, range_height: Decimal) -> dict[str, Any]:
        deviation = abs(self.price - reference)
        return {
            "kind": self.kind,
            "index": self.index,
            "open_time": self.open_time,
            "price": str(self.price),
            "deviation": str(deviation),
            "deviation_fraction": str(deviation / range_height),
        }


@dataclass(frozen=True)
class N08RangeStructure:
    symbol: str
    start_index: int
    end_index: int
    start_time: str
    end_time: str
    bars: int
    upper_reference: Decimal
    lower_reference: Decimal
    range_height: Decimal
    tolerance_fraction: Decimal
    upper_tolerance_boundary: Decimal
    lower_tolerance_boundary: Decimal
    high_spread: Decimal
    low_spread: Decimal
    pivot_highs: tuple[RangePivot, ...]
    pivot_lows: tuple[RangePivot, ...]
    alternating_pivots: tuple[RangePivot, ...]
    structure_id: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "structure_id": self.structure_id,
            "start_index": self.start_index,
            "end_index": self.end_index,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "bars": self.bars,
            "hours": str(Decimal(self.bars) / Decimal("4")),
            "upper_reference": str(self.upper_reference),
            "lower_reference": str(self.lower_reference),
            "range_height": str(self.range_height),
            "tolerance_fraction": str(self.tolerance_fraction),
            "upper_tolerance_boundary": str(self.upper_tolerance_boundary),
            "lower_tolerance_boundary": str(self.lower_tolerance_boundary),
            "high_spread": str(self.high_spread),
            "low_spread": str(self.low_spread),
            "high_spread_fraction": str(self.high_spread / self.range_height),
            "low_spread_fraction": str(self.low_spread / self.range_height),
            "pivot_highs": [
                pivot.to_jsonable(self.upper_reference, self.range_height)
                for pivot in self.pivot_highs
            ],
            "pivot_lows": [
                pivot.to_jsonable(self.lower_reference, self.range_height)
                for pivot in self.pivot_lows
            ],
            "alternating_pivots": [
                pivot.to_jsonable(
                    self.upper_reference if pivot.kind == "HIGH" else self.lower_reference,
                    self.range_height,
                )
                for pivot in self.alternating_pivots
            ],
            "alternating_turn_count": len(self.alternating_pivots),
        }


@dataclass(frozen=True)
class N08AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    current_bullish: bool
    structure: N08RangeStructure | None
    bullish_streak: tuple[StructureCandle, ...]
    current_price: Decimal
    current_open_time: str | None
    checked_at: str
    elapsed_seconds: Decimal | None
    entry_window_seconds: int
    detail: str

    @property
    def structure_id(self) -> str | None:
        return self.structure.structure_id if self.structure is not None else None

    def detail_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "reason": self.reason,
            "structure_id": self.structure_id,
            "current_bullish": self.current_bullish,
            "current_price": str(self.current_price),
            "current_open_time": self.current_open_time,
            "sixth_candle_open_time": self.current_open_time,
            "checked_at": self.checked_at,
            "elapsed_seconds": str(self.elapsed_seconds)
            if self.elapsed_seconds is not None
            else None,
            "entry_window_seconds": self.entry_window_seconds,
            "bullish_streak": [_candle_payload(candle) for candle in self.bullish_streak],
        }
        if self.structure is not None:
            payload["structure"] = self.structure.to_jsonable()
        return payload


def _candle_payload(candle: StructureCandle) -> dict[str, Any]:
    return {
        "index": candle.index,
        "open_time": candle.open_time,
        "open": str(candle.open),
        "high": str(candle.high),
        "low": str(candle.low),
        "close": str(candle.close),
    }


def _median(values: list[Decimal]) -> Decimal:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal("2")


def compress_alternating_pivots(pivots: list[RangePivot]) -> tuple[RangePivot, ...]:
    compressed: list[RangePivot] = []
    for pivot in sorted(pivots, key=lambda item: (item.index, item.kind)):
        if not compressed or compressed[-1].kind != pivot.kind:
            compressed.append(pivot)
            continue
        previous = compressed[-1]
        more_extreme = (
            pivot.price > previous.price
            if pivot.kind == "HIGH"
            else pivot.price < previous.price
        )
        if more_extreme:
            compressed[-1] = pivot
    return tuple(compressed)


def _structure_id(
    symbol: str,
    window: list[StructureCandle],
    upper_reference: Decimal,
    lower_reference: Decimal,
    pivots: list[RangePivot],
) -> str:
    parts = [
        symbol,
        window[0].open_time,
        window[-1].open_time,
        str(upper_reference),
        str(lower_reference),
    ]
    parts.extend(
        f"{pivot.kind}:{pivot.open_time}:{pivot.price}"
        for pivot in sorted(pivots, key=lambda item: (item.index, item.kind))
    )
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:24]


def validate_range_window(
    symbol: str,
    window: list[StructureCandle],
    pivot_left: int,
    pivot_right: int,
    tolerance_fraction: Decimal,
) -> N08RangeStructure | None:
    raw_highs = find_pivot_highs(window, pivot_left, pivot_right)
    raw_lows = find_pivot_lows(window, pivot_left, pivot_right)
    if len(raw_highs) < 2 or len(raw_lows) < 2:
        return None

    pivot_highs = tuple(
        RangePivot(
            kind="HIGH",
            index=window[pivot.index].index,
            open_time=pivot.open_time,
            price=pivot.price,
        )
        for pivot in raw_highs
    )
    pivot_lows = tuple(
        RangePivot(
            kind="LOW",
            index=window[pivot.index].index,
            open_time=pivot.open_time,
            price=pivot.price,
        )
        for pivot in raw_lows
    )
    alternating = compress_alternating_pivots([*pivot_highs, *pivot_lows])
    if len(alternating) < 4:
        return None

    high_prices = [pivot.price for pivot in pivot_highs]
    low_prices = [pivot.price for pivot in pivot_lows]
    upper_reference = _median(high_prices)
    lower_reference = _median(low_prices)
    range_height = upper_reference - lower_reference
    if range_height <= 0:
        return None

    tolerance = range_height * tolerance_fraction
    high_spread = max(high_prices) - min(high_prices)
    low_spread = max(low_prices) - min(low_prices)
    if high_spread > tolerance or low_spread > tolerance:
        return None

    upper_boundary = upper_reference + tolerance
    lower_boundary = lower_reference - tolerance
    if any(candle.high > upper_boundary or candle.low < lower_boundary for candle in window):
        return None

    all_pivots = [*pivot_highs, *pivot_lows]
    return N08RangeStructure(
        symbol=symbol,
        start_index=window[0].index,
        end_index=window[-1].index,
        start_time=window[0].open_time,
        end_time=window[-1].open_time,
        bars=len(window),
        upper_reference=upper_reference,
        lower_reference=lower_reference,
        range_height=range_height,
        tolerance_fraction=tolerance_fraction,
        upper_tolerance_boundary=upper_boundary,
        lower_tolerance_boundary=lower_boundary,
        high_spread=high_spread,
        low_spread=low_spread,
        pivot_highs=pivot_highs,
        pivot_lows=pivot_lows,
        alternating_pivots=alternating,
        structure_id=_structure_id(
            symbol,
            window,
            upper_reference,
            lower_reference,
            all_pivots,
        ),
    )


def _longest_valid_range_suffix(
    symbol: str,
    closed: list[StructureCandle],
    streak_start: int,
    range_min_bars: int,
    range_max_bars: int,
    pivot_left: int,
    pivot_right: int,
    tolerance_fraction: Decimal,
) -> N08RangeStructure | None:
    longest = min(range_max_bars, streak_start)
    for length in range(longest, range_min_bars - 1, -1):
        structure = validate_range_window(
            symbol,
            closed[streak_start - length : streak_start],
            pivot_left,
            pivot_right,
            tolerance_fraction,
        )
        if structure is not None:
            return structure
    return None


def find_n08_range_reset_open_time(
    raw_klines: list[list[Any]],
    after_open_time: str,
    upper_tolerance_boundary: Decimal,
    lower_tolerance_boundary: Decimal,
) -> str | None:
    after_ms = int(Decimal(after_open_time))
    for candle in parse_structure_klines(raw_klines):
        if int(Decimal(candle.open_time)) <= after_ms:
            continue
        if (
            candle.high > upper_tolerance_boundary
            or candle.low < lower_tolerance_boundary
        ):
            return candle.open_time
    return None


def is_same_n08_range_family(
    structure: N08RangeStructure,
    prior_upper_reference: Decimal,
    prior_lower_reference: Decimal,
    prior_upper_tolerance_boundary: Decimal,
    prior_lower_tolerance_boundary: Decimal,
) -> bool:
    prior_height = prior_upper_reference - prior_lower_reference
    if prior_height <= 0:
        return False
    prior_tolerance = max(
        prior_upper_tolerance_boundary - prior_upper_reference,
        prior_lower_reference - prior_lower_tolerance_boundary,
    )
    current_tolerance = structure.range_height * structure.tolerance_fraction
    reference_tolerance = max(prior_tolerance, current_tolerance)
    return (
        abs(structure.upper_reference - prior_upper_reference) <= reference_tolerance
        and abs(structure.lower_reference - prior_lower_reference) <= reference_tolerance
    )


def find_n08_historical_missed_events(
    symbol: str,
    raw_klines: list[list[Any]],
    pivot_left: int = 2,
    pivot_right: int = 2,
    range_min_bars: int = 20,
    range_max_bars: int = 96,
    tolerance_fraction: Decimal = Decimal("0.25"),
    bullish_streak_count: int = 5,
    entry_window_seconds: int = 120,
    checked_at_ms: int | None = None,
) -> tuple[N08AnalysisResult, ...]:
    now_ms = checked_at_ms
    if now_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    checked_at = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat()
    candles = parse_structure_klines(raw_klines)
    if len(candles) < range_min_bars + bullish_streak_count + 2:
        return ()

    closed = candles[:-1]
    latest_historical_start = len(closed) - bullish_streak_count - 1
    events: list[N08AnalysisResult] = []
    for streak_start in range(range_min_bars, latest_historical_start + 1):
        if (
            streak_start > 0
            and closed[streak_start - 1].close > closed[streak_start - 1].open
        ):
            continue
        streak = tuple(closed[streak_start : streak_start + bullish_streak_count])
        if len(streak) != bullish_streak_count:
            continue
        if not all(candle.close > candle.open for candle in streak):
            continue
        structure = _longest_valid_range_suffix(
            symbol,
            closed,
            streak_start,
            range_min_bars,
            range_max_bars,
            pivot_left,
            pivot_right,
            tolerance_fraction,
        )
        if structure is None:
            continue

        sixth = closed[streak_start + bullish_streak_count]
        expected_sixth_open = int(Decimal(streak[-1].open_time)) + FIFTEEN_MINUTES_MS
        actual_sixth_open = int(Decimal(sixth.open_time))
        reason = (
            "HISTORICAL_N08_STRUCTURE_MISSED"
            if actual_sixth_open == expected_sixth_open
            else "HISTORICAL_N08_ENTRY_CANDLE_MISMATCH"
        )
        events.append(
            N08AnalysisResult(
                symbol=symbol,
                passed=False,
                reason=reason,
                current_bullish=sixth.close > sixth.open,
                structure=structure,
                bullish_streak=streak,
                current_price=sixth.close,
                current_open_time=sixth.open_time,
                checked_at=checked_at,
                elapsed_seconds=Decimal(FIFTEEN_MINUTES_MS) / Decimal("1000"),
                entry_window_seconds=entry_window_seconds,
                detail=(
                    f"historical structure={structure.structure_id} "
                    f"sixth_open={sixth.open_time} checked_at={checked_at}"
                ),
            )
        )
    return tuple(events)


def analyze_n08_range_five_bullish(
    symbol: str,
    raw_klines: list[list[Any]],
    pivot_left: int = 2,
    pivot_right: int = 2,
    range_min_bars: int = 20,
    range_max_bars: int = 96,
    tolerance_fraction: Decimal = Decimal("0.25"),
    bullish_streak_count: int = 5,
    entry_window_seconds: int = 120,
    checked_at_ms: int | None = None,
) -> N08AnalysisResult:
    if pivot_left < 1 or pivot_right < 1:
        raise ValueError("pivot_left and pivot_right must be positive")
    if range_min_bars < 1 or range_max_bars < range_min_bars:
        raise ValueError("N08 range bar limits are invalid")
    if tolerance_fraction < 0 or bullish_streak_count < 1 or entry_window_seconds < 1:
        raise ValueError("N08 tolerance, streak and entry window must be positive")

    now_ms = checked_at_ms
    if now_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    checked_at = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat()
    candles = parse_structure_klines(raw_klines)
    current_price = candles[-1].close if candles else Decimal("0")
    current_bullish = bool(candles and candles[-1].close > candles[-1].open)
    current_open_time = candles[-1].open_time if candles else None
    minimum = range_min_bars + bullish_streak_count + 1
    if len(candles) < minimum:
        return N08AnalysisResult(
            symbol,
            False,
            "NOT_ENOUGH_KLINES",
            current_bullish,
            None,
            (),
            current_price,
            current_open_time,
            checked_at,
            None,
            entry_window_seconds,
            "not enough kline data",
        )

    current = candles[-1]
    closed = candles[:-1]
    streak_start = len(closed) - bullish_streak_count
    selected_streak = tuple(closed[streak_start:])
    if not all(candle.close > candle.open for candle in selected_streak):
        return N08AnalysisResult(
            symbol,
            False,
            "RANGE_FIVE_BULLISH_NOT_FOUND",
            current_bullish,
            None,
            (),
            current.close,
            current.open_time,
            checked_at,
            None,
            entry_window_seconds,
            "valid range followed by five bullish candles not found",
        )

    if (
        streak_start > 0
        and closed[streak_start - 1].close > closed[streak_start - 1].open
    ):
        return N08AnalysisResult(
            symbol,
            False,
            "ROLLING_BULLISH_STREAK",
            current_bullish,
            None,
            selected_streak,
            current.close,
            current.open_time,
            checked_at,
            None,
            entry_window_seconds,
            "the immediate five candles are part of a longer bullish streak",
        )

    selected_structure = _longest_valid_range_suffix(
        symbol,
        closed,
        streak_start,
        range_min_bars,
        range_max_bars,
        pivot_left,
        pivot_right,
        tolerance_fraction,
    )
    if selected_structure is None:
        return N08AnalysisResult(
            symbol,
            False,
            "RANGE_FIVE_BULLISH_NOT_FOUND",
            current_bullish,
            None,
            selected_streak,
            current.close,
            current.open_time,
            checked_at,
            None,
            entry_window_seconds,
            "the immediate five bullish candles have no valid preceding range",
        )

    if any(
        candle.low < selected_structure.lower_tolerance_boundary
        for candle in selected_streak
    ):
        return N08AnalysisResult(
            symbol,
            False,
            "BULLISH_STREAK_BROKE_LOWER_TOLERANCE",
            current_bullish,
            selected_structure,
            selected_streak,
            current.close,
            current.open_time,
            checked_at,
            None,
            entry_window_seconds,
            f"structure={selected_structure.structure_id} streak low broke range",
        )

    fifth_open_time = int(Decimal(selected_streak[-1].open_time))
    expected_current_open = fifth_open_time + FIFTEEN_MINUTES_MS
    actual_current_open = int(Decimal(current.open_time))
    if actual_current_open != expected_current_open:
        return N08AnalysisResult(
            symbol,
            False,
            "ENTRY_CANDLE_MISMATCH",
            current_bullish,
            selected_structure,
            selected_streak,
            current.close,
            current.open_time,
            checked_at,
            None,
            entry_window_seconds,
            f"expected_open={expected_current_open} actual_open={actual_current_open}",
        )

    elapsed_seconds = Decimal(now_ms - actual_current_open) / Decimal("1000")
    if elapsed_seconds < 0:
        reason = "ENTRY_WINDOW_NOT_OPEN"
        passed = False
    elif elapsed_seconds >= Decimal(entry_window_seconds):
        reason = "ENTRY_WINDOW_MISSED"
        passed = False
    else:
        reason = "PASSED"
        passed = True

    return N08AnalysisResult(
        symbol,
        passed,
        reason,
        current_bullish,
        selected_structure,
        selected_streak,
        current.close,
        current.open_time,
        checked_at,
        elapsed_seconds,
        entry_window_seconds,
        f"structure={selected_structure.structure_id} elapsed={elapsed_seconds}",
    )
