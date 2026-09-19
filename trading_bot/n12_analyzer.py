from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from .n11_analyzer import decimal_median


FIFTEEN_MINUTES_MS = 15 * 60 * 1000


@dataclass(frozen=True)
class N12Candle:
    index: int
    open_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    quote_volume: Decimal
    taker_buy_quote_volume: Decimal

    @property
    def open_time(self) -> str:
        return str(self.open_time_ms)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "open_time": self.open_time,
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "quote_volume": str(self.quote_volume),
            "taker_buy_quote_volume": str(self.taker_buy_quote_volume),
        }


@dataclass(frozen=True)
class RelativeStrength:
    symbol: str
    return_24h: Decimal
    rank: int


@dataclass(frozen=True)
class N12UpLegMetrics:
    passed: bool
    reason: str
    bars: int
    gain_fraction: Decimal
    displacement: Decimal
    atr_reference: Decimal
    slope: Decimal
    efficiency: Decimal
    max_bullish_body: Decimal
    up_volume_median: Decimal

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "reason": self.reason,
            "bars": self.bars,
            "gain_fraction": str(self.gain_fraction),
            "displacement": str(self.displacement),
            "atr_reference": str(self.atr_reference),
            "slope": str(self.slope),
            "efficiency": str(self.efficiency),
            "max_bullish_body": str(self.max_bullish_body),
            "up_volume_median": str(self.up_volume_median),
        }


@dataclass(frozen=True)
class N12Structure:
    symbol: str
    l: Decimal
    h: Decimal
    p: Decimal
    l_time: str
    h_time: str
    p_time: str
    c_time: str
    l_index: int
    h_index: int
    p_index: int
    c_index: int
    c: N12Candle
    entry: N12Candle | None
    atr_at_h: Decimal
    atr_at_c: Decimal
    up_leg: N12UpLegMetrics
    pullback_bars: int
    pullback_depth: Decimal
    pullback_volume_median: Decimal
    c_taker_buy_ratio: Decimal
    c_volume_multiple: Decimal
    entry_min_price: Decimal
    entry_max_price: Decimal
    relative_strength_rank: int | None
    relative_strength_return: Decimal | None
    structure_id: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "structure_id": self.structure_id,
            "l": str(self.l),
            "h": str(self.h),
            "p": str(self.p),
            "l_time": self.l_time,
            "h_time": self.h_time,
            "p_time": self.p_time,
            "c_time": self.c_time,
            "l_index": self.l_index,
            "h_index": self.h_index,
            "p_index": self.p_index,
            "c_index": self.c_index,
            "c": self.c.to_jsonable(),
            "entry": self.entry.to_jsonable() if self.entry is not None else None,
            "atr_at_h": str(self.atr_at_h),
            "atr_at_c": str(self.atr_at_c),
            "up_leg": self.up_leg.to_jsonable(),
            "pullback_bars": self.pullback_bars,
            "pullback_depth": str(self.pullback_depth),
            "up_volume_median": str(self.up_leg.up_volume_median),
            "pullback_volume_median": str(self.pullback_volume_median),
            "c_taker_buy_ratio": str(self.c_taker_buy_ratio),
            "c_volume_multiple": str(self.c_volume_multiple),
            "entry_min_price": str(self.entry_min_price),
            "entry_max_price": str(self.entry_max_price),
            "relative_strength_rank": self.relative_strength_rank,
            "relative_strength_return": (
                str(self.relative_strength_return)
                if self.relative_strength_return is not None
                else None
            ),
        }


@dataclass(frozen=True)
class N12HistoricalEvent:
    structure: N12Structure
    status: str
    reason: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "structure": self.structure.to_jsonable(),
        }


@dataclass(frozen=True)
class N12StageEvent:
    l_time: str
    h_time: str
    l_index: int
    h_index: int
    terminal_index: int
    status: str
    reason: str
    detail: dict[str, Any]

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "l_time": self.l_time,
            "h_time": self.h_time,
            "l_index": self.l_index,
            "h_index": self.h_index,
            "terminal_index": self.terminal_index,
            "status": self.status,
            "reason": self.reason,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class N12AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    structure: N12Structure | None
    current_price: Decimal
    current_open_time: str | None
    checked_at: str
    elapsed_ms: int | None
    entry_window_ms: int
    consume_current: bool
    detail: str
    relative_strength_rank: int | None
    relative_strength_return: Decimal | None
    historical_events: tuple[N12HistoricalEvent, ...] = ()
    stage_events: tuple[N12StageEvent, ...] = ()

    @property
    def structure_id(self) -> str | None:
        return self.structure.structure_id if self.structure is not None else None

    @property
    def current_bullish(self) -> bool:
        if self.structure is None or self.structure.entry is None:
            return False
        return self.structure.entry.close > self.structure.entry.open

    def detail_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "reason": self.reason,
            "structure_id": self.structure_id,
            "current_price": str(self.current_price),
            "current_open_time": self.current_open_time,
            "checked_at": self.checked_at,
            "elapsed_ms": self.elapsed_ms,
            "entry_window_ms": self.entry_window_ms,
            "entry_deadline_ms": (
                int(self.current_open_time) + self.entry_window_ms
                if self.current_open_time is not None
                else None
            ),
            "consume_current": self.consume_current,
            "relative_strength_rank": self.relative_strength_rank,
            "relative_strength_return": (
                str(self.relative_strength_return)
                if self.relative_strength_return is not None
                else None
            ),
            "detail": self.detail,
            "historical_events": [event.to_jsonable() for event in self.historical_events],
            "stage_events": [event.to_jsonable() for event in self.stage_events],
        }
        if self.structure is not None:
            payload["structure"] = self.structure.to_jsonable()
        return payload


def _decimal(value: Any) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise InvalidOperation("non-finite decimal")
    return parsed


def parse_n12_klines(raw_klines: list[list[Any]]) -> list[N12Candle]:
    candles: list[N12Candle] = []
    for index, row in enumerate(raw_klines):
        if len(row) <= 10:
            raise ValueError("N12 kline requires indexes 0-10")
        open_time = _decimal(row[0])
        if open_time != open_time.to_integral_value() or open_time <= 0:
            raise ValueError("N12 kline open time is invalid")
        candle = N12Candle(
            index=index,
            open_time_ms=int(open_time),
            open=_decimal(row[1]),
            high=_decimal(row[2]),
            low=_decimal(row[3]),
            close=_decimal(row[4]),
            quote_volume=_decimal(row[7]),
            taker_buy_quote_volume=_decimal(row[10]),
        )
        if (
            min(candle.open, candle.high, candle.low, candle.close) <= 0
            or candle.high <= candle.low
            or candle.high < max(candle.open, candle.close)
            or candle.low > min(candle.open, candle.close)
            or candle.quote_volume <= 0
            or candle.taker_buy_quote_volume < 0
            or candle.taker_buy_quote_volume > candle.quote_volume
        ):
            raise ValueError("N12 kline contains invalid prices or volumes")
        candles.append(candle)
    return candles


def _continuous(candles: list[N12Candle]) -> bool:
    return all(
        right.open_time_ms - left.open_time_ms == FIFTEEN_MINUTES_MS
        for left, right in zip(candles, candles[1:])
    )


def calculate_relative_strength_return(
    raw_klines: list[list[Any]], lookback_bars: int = 96
) -> Decimal:
    candles = parse_n12_klines(raw_klines)
    closed = candles[:-1]
    if lookback_bars < 1 or len(closed) < lookback_bars:
        raise ValueError("N12 relative strength history is incomplete")
    window = closed[-lookback_bars:]
    return window[-1].close / window[0].open - Decimal("1")


def _slope(values: list[Decimal]) -> Decimal:
    count = Decimal(len(values))
    xs = [Decimal(index) for index in range(len(values))]
    sx = sum(xs, Decimal("0"))
    sy = sum(values, Decimal("0"))
    sxx = sum((value * value for value in xs), Decimal("0"))
    sxy = sum((x * y for x, y in zip(xs, values)), Decimal("0"))
    denominator = count * sxx - sx * sx
    return Decimal("0") if denominator == 0 else (count * sxy - sx * sy) / denominator


def _atr(candles: list[N12Candle], end_index: int, period: int = 14) -> Decimal:
    ranges: list[Decimal] = []
    for index in range(0, end_index + 1):
        candle = candles[index]
        if index == 0:
            ranges.append(candle.high - candle.low)
        else:
            previous_close = candles[index - 1].close
            ranges.append(max(
                candle.high - candle.low,
                abs(candle.high - previous_close),
                abs(candle.low - previous_close),
            ))
    if len(ranges) < period:
        raise ValueError("N12 ATR history is incomplete")
    atr = sum(ranges[:period], Decimal("0")) / Decimal(period)
    for true_range in ranges[period:]:
        atr = (atr * Decimal(period - 1) + true_range) / Decimal(period)
    return atr


def evaluate_n12_up_leg(
    candles: list[N12Candle],
    l_index: int,
    h_index: int,
    *,
    min_bars: int = 5,
    min_gain_fraction: Decimal = Decimal("0.03"),
    min_atr_multiple: Decimal = Decimal("3"),
    min_efficiency: Decimal = Decimal("0.50"),
    max_bullish_body_fraction: Decimal = Decimal("0.50"),
) -> N12UpLegMetrics:
    leg = candles[l_index : h_index + 1]
    l_price = candles[l_index].low
    h_price = candles[h_index].high
    displacement = h_price - l_price
    try:
        atr_reference = _atr(candles, h_index)
        atr_incomplete = False
    except ValueError:
        atr_reference = Decimal("0")
        atr_incomplete = True
    closes = [candle.close for candle in leg]
    path = sum(
        (abs(closes[index] - closes[index - 1]) for index in range(1, len(closes))),
        Decimal("0"),
    )
    efficiency = abs(closes[-1] - closes[0]) / path if path > 0 else Decimal("0")
    slope = _slope(closes)
    gain_fraction = displacement / l_price if l_price > 0 else Decimal("0")
    max_body = max(
        (max(candle.close - candle.open, Decimal("0")) for candle in leg),
        default=Decimal("0"),
    )
    volumes = [candle.quote_volume for candle in leg]
    if atr_incomplete:
        reason = "N12_ATR_HISTORY_INCOMPLETE"
    elif h_index - l_index < min_bars:
        reason = "N12_UP_LEG_TOO_SHORT"
    elif gain_fraction < min_gain_fraction:
        reason = "N12_UP_LEG_GAIN_TOO_SMALL"
    elif displacement < min_atr_multiple * atr_reference:
        reason = "N12_UP_LEG_ATR_DISPLACEMENT_TOO_SMALL"
    elif slope <= 0:
        reason = "N12_UP_LEG_SLOPE_NOT_POSITIVE"
    elif efficiency < min_efficiency:
        reason = "N12_UP_LEG_EFFICIENCY_TOO_LOW"
    elif max_body >= max_bullish_body_fraction * displacement:
        reason = "N12_UP_LEG_SINGLE_BODY_TOO_LARGE"
    else:
        reason = "PASSED"
    return N12UpLegMetrics(
        passed=reason == "PASSED",
        reason=reason,
        bars=h_index - l_index,
        gain_fraction=gain_fraction,
        displacement=displacement,
        atr_reference=atr_reference,
        slope=slope,
        efficiency=efficiency,
        max_bullish_body=max_body,
        up_volume_median=decimal_median(volumes),
    )


def _pivot_lows(candles: list[N12Candle], left: int, right: int) -> list[int]:
    return [
        index
        for index in range(left, len(candles) - right)
        if candles[index].low < min(item.low for item in candles[index - left:index])
        and candles[index].low <= min(item.low for item in candles[index + 1:index + right + 1])
    ]


def _pivot_highs(candles: list[N12Candle], left: int, right: int) -> list[int]:
    return [
        index
        for index in range(left, len(candles) - right)
        if candles[index].high > max(item.high for item in candles[index - left:index])
        and candles[index].high >= max(item.high for item in candles[index + 1:index + right + 1])
    ]


def _structure_id(
    symbol: str,
    l: N12Candle,
    h: N12Candle,
    p: N12Candle,
    c: N12Candle,
) -> str:
    raw = "|".join((symbol, l.open_time, h.open_time, p.open_time, c.open_time))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _result(
    symbol: str,
    reason: str,
    current: N12Candle | None,
    checked_at: str,
    entry_window_ms: int,
    rank: int | None,
    relative_return: Decimal | None,
    *,
    structure: N12Structure | None = None,
    elapsed_ms: int | None = None,
    consume_current: bool = False,
    passed: bool = False,
) -> N12AnalysisResult:
    return N12AnalysisResult(
        symbol=symbol,
        passed=passed,
        reason=reason,
        structure=structure,
        current_price=current.close if current is not None else Decimal("0"),
        current_open_time=current.open_time if current is not None else None,
        checked_at=checked_at,
        elapsed_ms=elapsed_ms,
        entry_window_ms=entry_window_ms,
        consume_current=consume_current,
        detail=reason,
        relative_strength_rank=rank,
        relative_strength_return=relative_return,
    )


def analyze_n12_relative_strength_first_pullback(
    symbol: str,
    raw_klines: list[list[Any]],
    *,
    relative_strength_rank: int | None,
    relative_strength_return: Decimal | None,
    historical_strength_by_entry_time: dict[int, RelativeStrength] | None = None,
    historical_rank_context_complete: set[int] | None = None,
    relative_strength_context_complete: bool = True,
    relative_strength_top_n: int = 10,
    pivot_left: int = 2,
    pivot_right: int = 2,
    up_min_bars: int = 5,
    up_min_gain_fraction: Decimal = Decimal("0.03"),
    up_min_atr_multiple: Decimal = Decimal("3"),
    up_min_efficiency: Decimal = Decimal("0.50"),
    up_max_bullish_body_fraction: Decimal = Decimal("0.50"),
    pullback_min_bars: int = 2,
    pullback_max_bars: int = 5,
    pullback_depth_min: Decimal = Decimal("0.20"),
    pullback_depth_max: Decimal = Decimal("0.45"),
    pullback_close_floor_fraction: Decimal = Decimal("0.50"),
    pullback_volume_ratio_max: Decimal = Decimal("0.70"),
    confirmation_close_location_min: Decimal = Decimal("0.75"),
    confirmation_taker_buy_ratio_min: Decimal = Decimal("0.55"),
    confirmation_volume_multiple_min: Decimal = Decimal("1.2"),
    entry_extension_atr_max: Decimal = Decimal("0.5"),
    entry_window_seconds: int = 120,
    checked_at_ms: int | None = None,
) -> N12AnalysisResult:
    if min(
        relative_strength_top_n, pivot_left, pivot_right, up_min_bars,
        pullback_min_bars, pullback_max_bars, entry_window_seconds,
    ) < 1 or pullback_min_bars > pullback_max_bars:
        raise ValueError("N12 count parameters are invalid")
    now_ms = checked_at_ms
    if now_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    checked_at = datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat()
    entry_window_ms = entry_window_seconds * 1000
    try:
        candles = parse_n12_klines(raw_klines)
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return _result(
            symbol, "N12_KLINE_DATA_INVALID", None, checked_at, entry_window_ms,
            relative_strength_rank, relative_strength_return,
        )
    current = candles[-1] if candles else None
    if len(candles) < 97:
        return _result(
            symbol, "N12_NOT_ENOUGH_HISTORY", current, checked_at, entry_window_ms,
            relative_strength_rank, relative_strength_return,
        )
    if not _continuous(candles):
        return _result(
            symbol, "N12_KLINE_SEQUENCE_INVALID", current, checked_at, entry_window_ms,
            relative_strength_rank, relative_strength_return,
        )
    closed = candles[:-1]
    closed_end = len(closed) - 1
    lows = _pivot_lows(closed, pivot_left, pivot_right)
    highs = _pivot_highs(closed, pivot_left, pivot_right)
    historical_events: list[N12HistoricalEvent] = []
    stage_events: list[N12StageEvent] = []
    current_results: list[N12AnalysisResult] = []
    latest_rejection = "N12_STRUCTURE_NOT_FOUND"

    for h_index in highs:
        l_candidates = [index for index in lows if index < h_index]
        valid_legs: list[tuple[int, N12UpLegMetrics]] = []
        for l_index in reversed(l_candidates):
            metrics = evaluate_n12_up_leg(
                closed, l_index, h_index,
                min_bars=up_min_bars,
                min_gain_fraction=up_min_gain_fraction,
                min_atr_multiple=up_min_atr_multiple,
                min_efficiency=up_min_efficiency,
                max_bullish_body_fraction=up_max_bullish_body_fraction,
            )
            if metrics.passed:
                valid_legs.append((l_index, metrics))
                break
            latest_rejection = metrics.reason
        if not valid_legs:
            continue
        l_index, up_leg = valid_legs[0]
        l_candle = closed[l_index]
        h_candle = closed[h_index]
        midpoint = l_candle.low + pullback_close_floor_fraction * up_leg.displacement
        pullback_start = h_index + 1
        if pullback_start > closed_end:
            continue

        c_index: int | None = None
        terminal_reason: str | None = None
        terminal_index: int | None = None
        max_pullback_end = min(h_index + pullback_max_bars, closed_end)
        for index in range(pullback_start, max_pullback_end + 1):
            pullback = closed[pullback_start:index + 1]
            p_low = min(candle.low for candle in pullback)
            depth = (h_candle.high - p_low) / up_leg.displacement
            if any(candle.high > h_candle.high for candle in pullback):
                terminal_reason, terminal_index = "N12_PULLBACK_BROKE_H", index
                break
            if any(candle.close < midpoint for candle in pullback):
                terminal_reason, terminal_index = "N12_PULLBACK_CLOSE_BELOW_MIDPOINT", index
                break
            if depth > pullback_depth_max:
                terminal_reason, terminal_index = "N12_PULLBACK_DEPTH_OUT_OF_RANGE", index
                break
            bars = index - h_index
            next_index = index + 1
            if next_index > closed_end:
                continue
            candidate_c = closed[next_index]
            if not (
                candidate_c.close > candidate_c.open
                and candidate_c.close > closed[index].high
            ):
                continue
            c_index = next_index
            break

        if c_index is None:
            if (
                terminal_reason is None
                and closed_end >= h_index + pullback_max_bars + 1
            ):
                terminal_reason = "N12_CONFIRMATION_NOT_FOUND"
                terminal_index = h_index + pullback_max_bars
            if terminal_reason is None or terminal_index is None:
                continue
            stage_pullback = closed[pullback_start:terminal_index + 1]
            if not stage_pullback:
                continue
            stage_p = min(
                stage_pullback, key=lambda candle: (candle.low, candle.index)
            )
            stage_depth = (
                (h_candle.high - stage_p.low) / up_leg.displacement
            )
            stage_events.append(N12StageEvent(
                l_time=l_candle.open_time,
                h_time=h_candle.open_time,
                l_index=l_index,
                h_index=h_index,
                terminal_index=terminal_index,
                status="INVALID",
                reason=terminal_reason,
                detail={
                    "l": str(l_candle.low),
                    "h": str(h_candle.high),
                    "p": str(stage_p.low),
                    "p_time": stage_p.open_time,
                    "pullback_bars": len(stage_pullback),
                    "pullback_depth": str(stage_depth),
                    "up_leg": up_leg.to_jsonable(),
                },
            ))
            latest_rejection = terminal_reason
            continue

        anchor_index = c_index
        pullback_end = min(anchor_index - 1, h_index + pullback_max_bars)
        pullback = closed[pullback_start:pullback_end + 1]
        if not pullback:
            continue
        p_candle = min(pullback, key=lambda candle: (candle.low, candle.index))
        depth = (h_candle.high - p_candle.low) / up_leg.displacement
        pullback_volume_median = decimal_median(
            [candle.quote_volume for candle in pullback]
        )
        c_candle = closed[anchor_index]
        c_ratio = c_candle.taker_buy_quote_volume / c_candle.quote_volume
        c_volume_multiple = c_candle.quote_volume / pullback_volume_median
        entry_min = c_candle.high
        atr_at_c = _atr(closed, c_candle.index)
        entry_max = c_candle.close + entry_extension_atr_max * atr_at_c
        structure = N12Structure(
            symbol=symbol,
            l=l_candle.low,
            h=h_candle.high,
            p=p_candle.low,
            l_time=l_candle.open_time,
            h_time=h_candle.open_time,
            p_time=p_candle.open_time,
            c_time=c_candle.open_time,
            l_index=l_index,
            h_index=h_index,
            p_index=p_candle.index,
            c_index=c_candle.index,
            c=c_candle,
            entry=None,
            atr_at_h=up_leg.atr_reference,
            atr_at_c=atr_at_c,
            up_leg=up_leg,
            pullback_bars=len(pullback),
            pullback_depth=depth,
            pullback_volume_median=pullback_volume_median,
            c_taker_buy_ratio=c_ratio,
            c_volume_multiple=c_volume_multiple,
            entry_min_price=entry_min,
            entry_max_price=entry_max,
            relative_strength_rank=relative_strength_rank,
            relative_strength_return=relative_strength_return,
            structure_id=_structure_id(symbol, l_candle, h_candle, p_candle, c_candle),
        )

        if terminal_reason is None:
            if len(pullback) < pullback_min_bars or len(pullback) > pullback_max_bars:
                terminal_reason = "N12_PULLBACK_DURATION_OUT_OF_RANGE"
            elif not pullback_depth_min <= depth <= pullback_depth_max:
                terminal_reason = "N12_PULLBACK_DEPTH_OUT_OF_RANGE"
            elif pullback_volume_median > up_leg.up_volume_median * pullback_volume_ratio_max:
                terminal_reason = "N12_PULLBACK_VOLUME_TOO_HIGH"
            elif c_candle.close >= h_candle.high:
                terminal_reason = "N12_CONFIRMATION_REACHED_H"
            else:
                close_location = (
                    (c_candle.close - c_candle.low) / (c_candle.high - c_candle.low)
                )
                if close_location < confirmation_close_location_min:
                    terminal_reason = "N12_CONFIRMATION_CLOSE_LOCATION_TOO_LOW"
                elif c_ratio < confirmation_taker_buy_ratio_min:
                    terminal_reason = "N12_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW"
                elif c_volume_multiple < confirmation_volume_multiple_min:
                    terminal_reason = "N12_CONFIRMATION_VOLUME_TOO_LOW"

        if terminal_reason is not None:
            event = N12HistoricalEvent(structure, "INVALID", terminal_reason)
            if anchor_index == closed_end:
                current_results.append(_result(
                    symbol, terminal_reason, current, checked_at, entry_window_ms,
                    relative_strength_rank, relative_strength_return,
                    structure=structure, consume_current=True,
                ))
            else:
                historical_events.append(event)
            continue

        entry_index = c_index + 1
        if entry_index <= closed_end:
            historical_entry = closed[entry_index]
            historical_strength = (historical_strength_by_entry_time or {}).get(
                historical_entry.open_time_ms
            )
            context_complete = historical_entry.open_time_ms in (
                historical_rank_context_complete or set()
            )
            if historical_entry.low < structure.p:
                historical_reason = "N12_HISTORICAL_ENTRY_LOW_BROKE_P"
            elif historical_entry.close < structure.entry_min_price:
                historical_reason = "N12_HISTORICAL_ENTRY_PRICE_BELOW_CONFIRMATION"
            elif historical_entry.close > structure.entry_max_price:
                historical_reason = "N12_HISTORICAL_ENTRY_PRICE_TOO_EXTENDED"
            elif not context_complete:
                historical_reason = "N12_HISTORICAL_RANK_CONTEXT_INSUFFICIENT"
            elif (
                historical_strength is None
                or historical_strength.return_24h <= 0
                or historical_strength.rank > relative_strength_top_n
            ):
                historical_reason = "N12_HISTORICAL_RELATIVE_STRENGTH_NOT_TOP10"
            else:
                historical_reason = "N12_HISTORICAL_ENTRY_MISSED"
            historical_structure = replace(
                structure,
                entry=historical_entry,
                relative_strength_rank=(
                    historical_strength.rank if historical_strength is not None else None
                ),
                relative_strength_return=(
                    historical_strength.return_24h
                    if historical_strength is not None
                    else None
                ),
            )
            historical_events.append(N12HistoricalEvent(
                historical_structure, "MISSED", historical_reason
            ))
            continue
        if entry_index != len(candles) - 1:
            continue
        entry = candles[entry_index]
        structure = replace(structure, entry=entry)
        elapsed_ms = now_ms - entry.open_time_ms
        if not relative_strength_context_complete:
            reason = "N12_RELATIVE_STRENGTH_CONTEXT_INSUFFICIENT"
        elif (
            relative_strength_rank is None
            or relative_strength_return is None
            or relative_strength_return <= 0
            or relative_strength_rank > relative_strength_top_n
        ):
            reason = "N12_RELATIVE_STRENGTH_NOT_TOP10"
        elif elapsed_ms < 0 or elapsed_ms >= entry_window_ms:
            reason = "N12_ENTRY_WINDOW_EXPIRED"
        elif entry.low < structure.p:
            reason = "N12_ENTRY_LOW_BROKE_P"
        elif entry.close < entry_min:
            reason = "N12_ENTRY_PRICE_BELOW_CONFIRMATION"
        elif entry.close > entry_max:
            reason = "N12_ENTRY_PRICE_TOO_EXTENDED"
        else:
            reason = "PASSED"
        current_results.append(_result(
            symbol, reason, entry, checked_at, entry_window_ms,
            relative_strength_rank, relative_strength_return,
            structure=structure,
            elapsed_ms=elapsed_ms,
            consume_current=(
                reason != "N12_RELATIVE_STRENGTH_CONTEXT_INSUFFICIENT"
            ),
            passed=reason == "PASSED",
        ))

    historical_by_c: dict[str, N12HistoricalEvent] = {}
    for event in historical_events:
        existing = historical_by_c.get(event.structure.c_time)
        if existing is None or (
            event.structure.h_index,
            event.structure.l_index,
        ) > (
            existing.structure.h_index,
            existing.structure.l_index,
        ):
            historical_by_c[event.structure.c_time] = event
    historical_events = list(historical_by_c.values())

    if current_results:
        selected = max(
            current_results,
            key=lambda result: (
                result.structure.c_index if result.structure else -1,
                result.structure.h_index if result.structure else -1,
                result.structure.l_index if result.structure else -1,
            ),
        )
    elif historical_events:
        latest_event = max(
            historical_events,
            key=lambda event: (
                event.structure.c_index,
                event.structure.h_index,
                event.structure.l_index,
            ),
        )
        selected = _result(
            symbol,
            latest_event.reason,
            current,
            checked_at,
            entry_window_ms,
            relative_strength_rank,
            relative_strength_return,
            structure=latest_event.structure,
        )
    else:
        latest_stage = max(
            stage_events,
            key=lambda event: (
                event.terminal_index, event.h_index, event.l_index
            ),
            default=None,
        )
        selected = _result(
            symbol,
            latest_stage.reason if latest_stage is not None else latest_rejection,
            current, checked_at, entry_window_ms,
            relative_strength_rank, relative_strength_return,
        )
    return replace(
        selected,
        historical_events=tuple(historical_events),
        stage_events=tuple(stage_events),
    )
