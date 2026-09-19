from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .double_break import StructureCandle, find_pivot_highs, parse_structure_klines


@dataclass(frozen=True)
class SlowDeclineMetrics:
    slope: Decimal
    r_squared: Decimal
    max_bearish_body: Decimal
    max_countertrend_rebound: Decimal
    minimum_eight_bar_net_drop: Decimal | None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "slope": str(self.slope),
            "r_squared": str(self.r_squared),
            "max_bearish_body": str(self.max_bearish_body),
            "max_countertrend_rebound": str(self.max_countertrend_rebound),
            "minimum_eight_bar_net_drop": (
                str(self.minimum_eight_bar_net_drop)
                if self.minimum_eight_bar_net_drop is not None
                else None
            ),
        }


@dataclass(frozen=True)
class N09Structure:
    symbol: str
    s1: Decimal
    l: Decimal
    depth: Decimal
    p43: Decimal
    p50: Decimal
    s1_index: int
    l_index: int
    s1_time: str
    l_time: str
    decline_bars: int
    metrics: SlowDeclineMetrics
    structure_id: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "structure_id": self.structure_id,
            "s1": str(self.s1),
            "l": str(self.l),
            "depth": str(self.depth),
            "p43": str(self.p43),
            "p50": str(self.p50),
            "s1_index": self.s1_index,
            "l_index": self.l_index,
            "s1_time": self.s1_time,
            "l_time": self.l_time,
            "decline_bars": self.decline_bars,
            "decline_hours": str(Decimal(self.decline_bars) / Decimal("4")),
            "metrics": self.metrics.to_jsonable(),
        }


@dataclass(frozen=True)
class N09TerminalEvent:
    structure: N09Structure
    reason: str
    first_touch_index: int
    first_touch_time: str
    rebound_bars: int

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "structure": self.structure.to_jsonable(),
            "reason": self.reason,
            "first_touch_index": self.first_touch_index,
            "first_touch_time": self.first_touch_time,
            "rebound_bars": self.rebound_bars,
        }


@dataclass(frozen=True)
class N09AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    structure: N09Structure | None
    current_price: Decimal
    current_open_time: str | None
    first_touch_index: int | None
    first_touch_time: str | None
    rebound_bars: int | None
    touch_observed: bool
    historical_touch: bool
    checked_at: str
    detail: str
    historical_events: tuple[N09TerminalEvent, ...] = ()

    @property
    def structure_id(self) -> str | None:
        return self.structure.structure_id if self.structure is not None else None

    @property
    def current_bullish(self) -> bool:
        return False

    def detail_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "reason": self.reason,
            "structure_id": self.structure_id,
            "current_price": str(self.current_price),
            "current_open_time": self.current_open_time,
            "first_touch_index": self.first_touch_index,
            "first_touch_time": self.first_touch_time,
            "rebound_bars": self.rebound_bars,
            "touch_observed": self.touch_observed,
            "historical_touch": self.historical_touch,
            "checked_at": self.checked_at,
            "detail": self.detail,
            "historical_events": [
                event.to_jsonable() for event in self.historical_events
            ],
        }
        if self.structure is not None:
            payload["structure"] = self.structure.to_jsonable()
        return payload


def linear_regression_stats(values: list[Decimal]) -> tuple[Decimal, Decimal]:
    if len(values) < 2:
        return Decimal("0"), Decimal("0")
    count = Decimal(len(values))
    mean_x = Decimal(len(values) - 1) / Decimal("2")
    mean_y = sum(values) / count
    denominator = sum((Decimal(index) - mean_x) ** 2 for index in range(len(values)))
    if denominator == 0:
        return Decimal("0"), Decimal("0")
    slope = sum(
        (Decimal(index) - mean_x) * (value - mean_y)
        for index, value in enumerate(values)
    ) / denominator
    intercept = mean_y - slope * mean_x
    residual = sum(
        (value - (intercept + slope * Decimal(index))) ** 2
        for index, value in enumerate(values)
    )
    total = sum((value - mean_y) ** 2 for value in values)
    r_squared = Decimal("0") if total == 0 else Decimal("1") - residual / total
    return slope, r_squared


def validate_slow_decline_segment(
    segment: list[StructureCandle],
    depth: Decimal,
    min_bars: int = 20,
    min_r_squared: Decimal = Decimal("0.65"),
    max_bearish_body_fraction: Decimal = Decimal("0.20"),
    max_countertrend_rebound_fraction: Decimal = Decimal("0.20"),
    sideways_window: int = 8,
    sideways_max_net_drop_fraction: Decimal = Decimal("0.05"),
) -> tuple[str | None, SlowDeclineMetrics]:
    closes = [candle.close for candle in segment]
    slope, r_squared = linear_regression_stats(closes)
    bearish_bodies = [max(candle.open - candle.close, Decimal("0")) for candle in segment]
    max_bearish_body = max(bearish_bodies, default=Decimal("0"))

    running_low = segment[0].low if segment else Decimal("0")
    max_rebound = Decimal("0")
    for candle in segment[1:]:
        max_rebound = max(max_rebound, candle.high - running_low)
        running_low = min(running_low, candle.low)

    net_drops = [
        closes[start] - closes[start + sideways_window - 1]
        for start in range(0, len(closes) - sideways_window + 1)
    ]
    minimum_net_drop = min(net_drops) if net_drops else None
    metrics = SlowDeclineMetrics(
        slope=slope,
        r_squared=r_squared,
        max_bearish_body=max_bearish_body,
        max_countertrend_rebound=max_rebound,
        minimum_eight_bar_net_drop=minimum_net_drop,
    )

    if len(segment) < min_bars:
        return "SLOW_DECLINE_DURATION_TOO_SHORT", metrics
    if depth <= 0 or slope >= 0 or r_squared < min_r_squared:
        return "SLOW_DECLINE_TREND_NOT_QUALIFIED", metrics
    if max_bearish_body > depth * max_bearish_body_fraction:
        return "SINGLE_CANDLE_DROP_TOO_LARGE", metrics
    if max_rebound > depth * max_countertrend_rebound_fraction:
        return "COUNTERTREND_REBOUND_TOO_LARGE", metrics
    sideways_limit = depth * sideways_max_net_drop_fraction
    if any(net_drop <= sideways_limit for net_drop in net_drops):
        return "SIDEWAYS_WINDOW_DETECTED", metrics
    return None, metrics


def _structure_id(
    symbol: str,
    s1_time: str,
    l_time: str,
    s1: Decimal,
    l: Decimal,
) -> str:
    raw = f"{symbol}|{s1_time}|{l_time}|{s1}|{l}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _result(
    symbol: str,
    reason: str,
    current_price: Decimal,
    current_open_time: str | None,
    checked_at: str,
    structure: N09Structure | None = None,
    passed: bool = False,
    first_touch: StructureCandle | None = None,
    rebound_bars: int | None = None,
    historical_touch: bool = False,
    detail: str = "",
    historical_events: tuple[N09TerminalEvent, ...] = (),
) -> N09AnalysisResult:
    return N09AnalysisResult(
        symbol=symbol,
        passed=passed,
        reason=reason,
        structure=structure,
        current_price=current_price,
        current_open_time=current_open_time,
        first_touch_index=first_touch.index if first_touch is not None else None,
        first_touch_time=first_touch.open_time if first_touch is not None else None,
        rebound_bars=rebound_bars,
        touch_observed=first_touch is not None,
        historical_touch=historical_touch,
        checked_at=checked_at,
        detail=detail or reason,
        historical_events=historical_events,
    )


def _build_structure(
    symbol: str,
    s1_pivot: Any,
    l_candle: StructureCandle,
    metrics: SlowDeclineMetrics,
    entry_floor_fraction: Decimal,
    touch_fraction: Decimal,
) -> N09Structure:
    depth = s1_pivot.price - l_candle.low
    return N09Structure(
        symbol=symbol,
        s1=s1_pivot.price,
        l=l_candle.low,
        depth=depth,
        p43=l_candle.low + entry_floor_fraction * depth,
        p50=l_candle.low + touch_fraction * depth,
        s1_index=s1_pivot.index,
        l_index=l_candle.index,
        s1_time=s1_pivot.open_time,
        l_time=l_candle.open_time,
        decline_bars=l_candle.index - s1_pivot.index + 1,
        metrics=metrics,
        structure_id=_structure_id(
            symbol,
            s1_pivot.open_time,
            l_candle.open_time,
            s1_pivot.price,
            l_candle.low,
        ),
    )


def analyze_n09_slow_decline_half_retrace(
    symbol: str,
    raw_klines: list[list[Any]],
    pivot_left: int = 2,
    pivot_right: int = 2,
    min_decline_bars: int = 20,
    min_r_squared: Decimal = Decimal("0.65"),
    max_bearish_body_fraction: Decimal = Decimal("0.20"),
    max_countertrend_rebound_fraction: Decimal = Decimal("0.20"),
    sideways_window: int = 8,
    sideways_max_net_drop_fraction: Decimal = Decimal("0.05"),
    rebound_max_bars: int = 40,
    touch_fraction: Decimal = Decimal("0.50"),
    entry_floor_fraction: Decimal = Decimal("0.43"),
    checked_at_ms: int | None = None,
) -> N09AnalysisResult:
    now_ms = checked_at_ms
    if now_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    checked_at = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat()
    candles = parse_structure_klines(raw_klines)
    current_price = candles[-1].close if candles else Decimal("0")
    current_open_time = candles[-1].open_time if candles else None
    minimum_rows = pivot_left + pivot_right + min_decline_bars + 1
    if len(candles) < minimum_rows:
        return _result(
            symbol,
            "NOT_ENOUGH_KLINES",
            current_price,
            current_open_time,
            checked_at,
        )

    current = candles[-1]
    closed = candles[:-1]
    high_pivots = find_pivot_highs(closed, pivot_left, pivot_right)
    if not high_pivots:
        return _result(
            symbol,
            "HISTORY_CONTEXT_INSUFFICIENT",
            current.close,
            current.open_time,
            checked_at,
        )

    pivots_by_index = {pivot.index: pivot for pivot in high_pivots}
    active_s1 = None
    historical_events: list[N09TerminalEvent] = []
    latest_historical_result: N09AnalysisResult | None = None

    for touch_candle in candles:
        if active_s1 is not None and touch_candle.index > active_s1.index:
            prior_candles = candles[active_s1.index + 1 : touch_candle.index]
            if prior_candles:
                l_candle = min(
                    prior_candles,
                    key=lambda candle: (candle.low, candle.index),
                )
                segment = candles[active_s1.index : l_candle.index + 1]
                depth = active_s1.price - l_candle.low
                rejection, metrics = validate_slow_decline_segment(
                    segment,
                    depth,
                    min_bars=min_decline_bars,
                    min_r_squared=min_r_squared,
                    max_bearish_body_fraction=max_bearish_body_fraction,
                    max_countertrend_rebound_fraction=max_countertrend_rebound_fraction,
                    sideways_window=sideways_window,
                    sideways_max_net_drop_fraction=sideways_max_net_drop_fraction,
                )
                structure = _build_structure(
                    symbol,
                    active_s1,
                    l_candle,
                    metrics,
                    entry_floor_fraction,
                    touch_fraction,
                )
                if rejection is None and touch_candle.high >= structure.p50:
                    rebound_bars = touch_candle.index - l_candle.index
                    historical_touch = touch_candle.index < current.index
                    if (
                        rebound_bars > rebound_max_bars
                        or rebound_bars * 2 > structure.decline_bars
                    ):
                        reason = "REBOUND_TOO_SLOW"
                    elif touch_candle.high >= structure.s1 or current.close >= structure.s1:
                        reason = "S1_REACHED"
                    elif historical_touch:
                        reason = "HISTORICAL_P50_TOUCH_MISSED"
                    elif current.close < structure.p43:
                        reason = "P43_MISSED"
                    else:
                        reason = "PASSED"

                    if historical_touch:
                        event = N09TerminalEvent(
                            structure=structure,
                            reason=reason,
                            first_touch_index=touch_candle.index,
                            first_touch_time=touch_candle.open_time,
                            rebound_bars=rebound_bars,
                        )
                        historical_events.append(event)
                        latest_historical_result = _result(
                            symbol,
                            reason,
                            current.close,
                            current.open_time,
                            checked_at,
                            structure=structure,
                            first_touch=touch_candle,
                            rebound_bars=rebound_bars,
                            historical_touch=True,
                            historical_events=tuple(historical_events),
                        )
                        active_s1 = None
                        continue

                    return _result(
                        symbol,
                        reason,
                        current.close,
                        current.open_time,
                        checked_at,
                        structure=structure,
                        passed=reason == "PASSED",
                        first_touch=touch_candle,
                        rebound_bars=rebound_bars,
                        detail=(
                            f"structure={structure.structure_id} s1={structure.s1} "
                            f"l={structure.l} p43={structure.p43} p50={structure.p50} "
                            f"rebound_bars={rebound_bars}"
                        ),
                        historical_events=tuple(historical_events),
                    )

        pivot = pivots_by_index.get(touch_candle.index)
        if pivot is not None and (
            active_s1 is None or pivot.price > active_s1.price
        ):
            active_s1 = pivot

    if active_s1 is None:
        if latest_historical_result is not None:
            return latest_historical_result
        return _result(
            symbol,
            "HISTORY_CONTEXT_INSUFFICIENT",
            current.close,
            current.open_time,
            checked_at,
            historical_events=tuple(historical_events),
        )

    remaining_closed = closed[active_s1.index + 1 :]
    if not remaining_closed:
        return _result(
            symbol,
            "SLOW_DECLINE_TREND_NOT_QUALIFIED",
            current.close,
            current.open_time,
            checked_at,
            historical_events=tuple(historical_events),
        )
    l_candle = min(
        remaining_closed,
        key=lambda candle: (candle.low, candle.index),
    )
    segment = closed[active_s1.index : l_candle.index + 1]
    depth = active_s1.price - l_candle.low
    rejection, metrics = validate_slow_decline_segment(
        segment,
        depth,
        min_bars=min_decline_bars,
        min_r_squared=min_r_squared,
        max_bearish_body_fraction=max_bearish_body_fraction,
        max_countertrend_rebound_fraction=max_countertrend_rebound_fraction,
        sideways_window=sideways_window,
        sideways_max_net_drop_fraction=sideways_max_net_drop_fraction,
    )
    structure = _build_structure(
        symbol,
        active_s1,
        l_candle,
        metrics,
        entry_floor_fraction,
        touch_fraction,
    )
    return _result(
        symbol,
        rejection or "P50_NOT_TOUCHED",
        current.close,
        current.open_time,
        checked_at,
        structure=structure,
        historical_events=tuple(historical_events),
    )
