from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


FIFTEEN_MINUTES_MS = 15 * 60 * 1000

N10_REASON_CODES = (
    "N10_NOT_ENOUGH_SUPPORT_HISTORY",
    "N10_KLINE_DATA_INVALID",
    "N10_KLINE_SEQUENCE_INVALID",
    "N10_SUPPORT_NOT_RETESTED",
    "N10_SWEEP_DEPTH_OUT_OF_RANGE",
    "N10_SWEEP_NOT_RECLAIMED",
    "N10_VOLUME_SPIKE_NOT_CONFIRMED",
    "N10_LOWER_WICK_TOO_SMALL",
    "N10_CONFIRMATION_NOT_BROKEN_HIGH",
    "N10_STRUCTURE_LOW_BROKEN",
    "N10_TAKER_BUY_RATIO_TOO_LOW",
    "N10_ENTRY_WINDOW_EXPIRED",
    "N10_ENTRY_PRICE_BELOW_RECLAIM",
    "N10_ENTRY_PRICE_TOO_EXTENDED",
    "N10_STRUCTURE_CONSUMED",
    "N10_HISTORY_CONTEXT_INSUFFICIENT",
    "N10_STATE_READ_FAILED",
    "N10_STATE_PERSIST_FAILED",
    "N10_STOP_PCT_OUT_OF_RANGE",
    "N10_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE",
)


@dataclass(frozen=True)
class N10Candle:
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
class N10Structure:
    symbol: str
    support_price: Decimal
    support_start_time: str
    support_end_time: str
    first_touch_time: str
    second_touch_time: str
    support_touch_gap: int
    w: N10Candle
    c: N10Candle
    e: N10Candle
    volume_median_20: Decimal
    volume_spike_multiple: Decimal
    sweep_depth: Decimal
    lower_wick: Decimal
    lower_wick_ratio: Decimal
    taker_buy_ratio: Decimal
    entry_upper_price: Decimal
    structure_id: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "structure_id": self.structure_id,
            "support_price": str(self.support_price),
            "support_start_time": self.support_start_time,
            "support_end_time": self.support_end_time,
            "support_bars": 48,
            "first_touch_time": self.first_touch_time,
            "second_touch_time": self.second_touch_time,
            "support_touch_gap": self.support_touch_gap,
            "w": self.w.to_jsonable(),
            "c": self.c.to_jsonable(),
            "e": self.e.to_jsonable(),
            "volume_median_20": str(self.volume_median_20),
            "volume_spike_multiple": str(self.volume_spike_multiple),
            "sweep_depth": str(self.sweep_depth),
            "lower_wick": str(self.lower_wick),
            "lower_wick_ratio": str(self.lower_wick_ratio),
            "taker_buy_ratio": str(self.taker_buy_ratio),
            "entry_upper_price": str(self.entry_upper_price),
        }


@dataclass(frozen=True)
class N10HistoricalEvent:
    structure: N10Structure
    reason: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "structure": self.structure.to_jsonable(),
        }


@dataclass(frozen=True)
class N10AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    structure: N10Structure | None
    current_price: Decimal
    current_open_time: str | None
    checked_at: str
    elapsed_ms: int | None
    entry_window_ms: int
    consume_current: bool
    detail: str
    historical_events: tuple[N10HistoricalEvent, ...] = ()

    @property
    def structure_id(self) -> str | None:
        return self.structure.structure_id if self.structure is not None else None

    @property
    def current_bullish(self) -> bool:
        if self.structure is None:
            return False
        return self.structure.e.close > self.structure.e.open

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
            "detail": self.detail,
            "historical_events": [
                event.to_jsonable() for event in self.historical_events
            ],
        }
        if self.structure is not None:
            payload["structure"] = self.structure.to_jsonable()
        return payload


def decimal_median(values: list[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("median requires at least one value")
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal("2")


def _decimal(value: Any) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise InvalidOperation("non-finite decimal")
    return parsed


def parse_n10_klines(raw_klines: list[list[Any]]) -> list[N10Candle]:
    candles: list[N10Candle] = []
    for index, row in enumerate(raw_klines):
        if len(row) <= 10:
            raise ValueError("N10 kline requires indexes 0-10")
        open_time = _decimal(row[0])
        if open_time != open_time.to_integral_value() or open_time <= 0:
            raise ValueError("N10 kline open time is invalid")
        candle = N10Candle(
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
            or candle.quote_volume < 0
            or candle.taker_buy_quote_volume < 0
            or candle.taker_buy_quote_volume > candle.quote_volume
            or candle.high < max(candle.open, candle.close)
            or candle.low > min(candle.open, candle.close)
            or candle.high <= candle.low
        ):
            raise ValueError("N10 kline contains invalid prices or volumes")
        candles.append(candle)
    return candles


def _result(
    symbol: str,
    reason: str,
    current_price: Decimal,
    current_open_time: str | None,
    checked_at: str,
    entry_window_ms: int,
    structure: N10Structure | None = None,
    elapsed_ms: int | None = None,
    consume_current: bool = False,
    passed: bool = False,
    detail: str = "",
) -> N10AnalysisResult:
    return N10AnalysisResult(
        symbol=symbol,
        passed=passed,
        reason=reason,
        structure=structure,
        current_price=current_price,
        current_open_time=current_open_time,
        checked_at=checked_at,
        elapsed_ms=elapsed_ms,
        entry_window_ms=entry_window_ms,
        consume_current=consume_current,
        detail=detail or reason,
    )


def _structure_id(
    symbol: str,
    support: list[N10Candle],
    support_price: Decimal,
    first_touch: N10Candle,
    second_touch: N10Candle,
    w: N10Candle,
    c: N10Candle,
) -> str:
    raw = "|".join(
        (
            symbol,
            support[0].open_time,
            support[-1].open_time,
            str(support_price),
            first_touch.open_time,
            second_touch.open_time,
            w.open_time,
            str(w.low),
            str(w.high),
            c.open_time,
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _support_touches(
    support: list[N10Candle],
    support_price: Decimal,
    tolerance_fraction: Decimal,
    minimum_gap: int,
) -> tuple[N10Candle, N10Candle] | None:
    upper = support_price * (Decimal("1") + tolerance_fraction)
    support_indexes = [index for index, item in enumerate(support) if item.low == support_price]
    eligible_indexes = [
        index for index, item in enumerate(support) if support_price <= item.low <= upper
    ]
    for anchor in support_indexes:
        for other in eligible_indexes:
            if anchor == other or abs(other - anchor) < minimum_gap:
                continue
            first, second = sorted((anchor, other))
            return support[first], support[second]
    return None


def _sequence_is_continuous(candles: list[N10Candle]) -> bool:
    return all(
        right.open_time_ms - left.open_time_ms == FIFTEEN_MINUTES_MS
        for left, right in zip(candles, candles[1:])
    )


def _evaluate_event(
    symbol: str,
    candles: list[N10Candle],
    entry_index: int,
    checked_at_ms: int,
    support_bars: int,
    support_tolerance_fraction: Decimal,
    support_minimum_gap: int,
    volume_median_bars: int,
    volume_spike_multiple: Decimal,
    sweep_depth_min: Decimal,
    sweep_depth_max: Decimal,
    lower_wick_ratio_min: Decimal,
    taker_buy_ratio_min: Decimal,
    entry_extension_max: Decimal,
    entry_window_ms: int,
    historical: bool,
) -> N10AnalysisResult:
    checked_at = datetime.fromtimestamp(
        checked_at_ms / 1000, tz=timezone.utc
    ).isoformat()
    current = candles[entry_index]
    support_start = entry_index - support_bars - 2
    if support_start < 0:
        return _result(
            symbol,
            "N10_NOT_ENOUGH_SUPPORT_HISTORY",
            current.close,
            current.open_time,
            checked_at,
            entry_window_ms,
        )
    support = candles[support_start : entry_index - 2]
    w = candles[entry_index - 2]
    c = candles[entry_index - 1]
    e = current
    sequence = [*support, w, c, e]
    if len(support) != support_bars:
        return _result(
            symbol,
            "N10_NOT_ENOUGH_SUPPORT_HISTORY",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )
    if not _sequence_is_continuous(sequence):
        return _result(
            symbol,
            "N10_KLINE_SEQUENCE_INVALID",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )

    support_price = min(item.low for item in support)
    if support_price <= 0:
        return _result(
            symbol,
            "N10_KLINE_DATA_INVALID",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )
    touches = _support_touches(
        support,
        support_price,
        support_tolerance_fraction,
        support_minimum_gap,
    )
    if touches is None:
        return _result(
            symbol,
            "N10_SUPPORT_NOT_RETESTED",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )
    first_touch, second_touch = touches

    sweep_depth = (support_price - w.low) / support_price
    if not sweep_depth_min <= sweep_depth <= sweep_depth_max:
        return _result(
            symbol,
            "N10_SWEEP_DEPTH_OUT_OF_RANGE",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )
    if w.close <= support_price:
        return _result(
            symbol,
            "N10_SWEEP_NOT_RECLAIMED",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )
    if w.high <= w.low:
        return _result(
            symbol,
            "N10_KLINE_DATA_INVALID",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )
    historical_volumes = [item.quote_volume for item in support[-volume_median_bars:]]
    if len(historical_volumes) != volume_median_bars or any(
        value <= 0 for value in historical_volumes
    ) or w.quote_volume <= 0:
        return _result(
            symbol,
            "N10_KLINE_DATA_INVALID",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )
    volume_median = decimal_median(historical_volumes)
    spike_multiple = w.quote_volume / volume_median
    if spike_multiple < volume_spike_multiple:
        return _result(
            symbol,
            "N10_VOLUME_SPIKE_NOT_CONFIRMED",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )
    lower_wick = min(w.open, w.close) - w.low
    lower_wick_ratio = lower_wick / (w.high - w.low)
    if lower_wick_ratio < lower_wick_ratio_min:
        return _result(
            symbol,
            "N10_LOWER_WICK_TOO_SMALL",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )
    if c.close <= w.high:
        return _result(
            symbol,
            "N10_CONFIRMATION_NOT_BROKEN_HIGH",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )
    if c.low < w.low:
        return _result(
            symbol,
            "N10_STRUCTURE_LOW_BROKEN",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )
    if (
        c.quote_volume <= 0
        or c.taker_buy_quote_volume < 0
        or c.taker_buy_quote_volume > c.quote_volume
    ):
        return _result(
            symbol,
            "N10_KLINE_DATA_INVALID",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )
    taker_buy_ratio = c.taker_buy_quote_volume / c.quote_volume
    if taker_buy_ratio < taker_buy_ratio_min:
        return _result(
            symbol,
            "N10_TAKER_BUY_RATIO_TOO_LOW",
            e.close,
            e.open_time,
            checked_at,
            entry_window_ms,
        )

    entry_upper = w.high * (Decimal("1") + entry_extension_max)
    structure = N10Structure(
        symbol=symbol,
        support_price=support_price,
        support_start_time=support[0].open_time,
        support_end_time=support[-1].open_time,
        first_touch_time=first_touch.open_time,
        second_touch_time=second_touch.open_time,
        support_touch_gap=abs(second_touch.index - first_touch.index),
        w=w,
        c=c,
        e=e,
        volume_median_20=volume_median,
        volume_spike_multiple=spike_multiple,
        sweep_depth=sweep_depth,
        lower_wick=lower_wick,
        lower_wick_ratio=lower_wick_ratio,
        taker_buy_ratio=taker_buy_ratio,
        entry_upper_price=entry_upper,
        structure_id=_structure_id(
            symbol,
            support,
            support_price,
            first_touch,
            second_touch,
            w,
            c,
        ),
    )

    elapsed_ms = checked_at_ms - e.open_time_ms
    if historical or elapsed_ms >= entry_window_ms or elapsed_ms < 0:
        reason = "N10_ENTRY_WINDOW_EXPIRED"
    elif e.low < w.low:
        reason = "N10_STRUCTURE_LOW_BROKEN"
    elif e.close < w.high:
        reason = "N10_ENTRY_PRICE_BELOW_RECLAIM"
    elif e.close > entry_upper:
        reason = "N10_ENTRY_PRICE_TOO_EXTENDED"
    else:
        reason = "PASSED"
    return _result(
        symbol,
        reason,
        e.close,
        e.open_time,
        checked_at,
        entry_window_ms,
        structure=structure,
        elapsed_ms=elapsed_ms,
        consume_current=True,
        passed=reason == "PASSED",
        detail=(
            f"structure={structure.structure_id} support={support_price} "
            f"w={w.open_time} c={c.open_time} e={e.open_time} elapsed_ms={elapsed_ms}"
        ),
    )


def analyze_n10_volume_liquidity_sweep_reclaim(
    symbol: str,
    raw_klines: list[list[Any]],
    support_bars: int = 48,
    support_tolerance_fraction: Decimal = Decimal("0.005"),
    support_minimum_gap: int = 4,
    volume_median_bars: int = 20,
    volume_spike_multiple: Decimal = Decimal("2.5"),
    sweep_depth_min: Decimal = Decimal("0.003"),
    sweep_depth_max: Decimal = Decimal("0.015"),
    lower_wick_ratio_min: Decimal = Decimal("0.50"),
    taker_buy_ratio_min: Decimal = Decimal("0.55"),
    entry_extension_max: Decimal = Decimal("0.015"),
    entry_window_seconds: int = 120,
    checked_at_ms: int | None = None,
) -> N10AnalysisResult:
    now_ms = checked_at_ms
    if now_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    checked_at = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat()
    entry_window_ms = entry_window_seconds * 1000
    try:
        candles = parse_n10_klines(raw_klines)
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return _result(
            symbol,
            "N10_KLINE_DATA_INVALID",
            Decimal("0"),
            None,
            checked_at,
            entry_window_ms,
        )
    if len(candles) < support_bars + 3:
        current = candles[-1] if candles else None
        return _result(
            symbol,
            "N10_NOT_ENOUGH_SUPPORT_HISTORY",
            current.close if current is not None else Decimal("0"),
            current.open_time if current is not None else None,
            checked_at,
            entry_window_ms,
        )

    historical_events: list[N10HistoricalEvent] = []
    first_entry_index = support_bars + 2
    for entry_index in range(first_entry_index, len(candles) - 1):
        historical = _evaluate_event(
            symbol,
            candles,
            entry_index,
            now_ms,
            support_bars,
            support_tolerance_fraction,
            support_minimum_gap,
            volume_median_bars,
            volume_spike_multiple,
            sweep_depth_min,
            sweep_depth_max,
            lower_wick_ratio_min,
            taker_buy_ratio_min,
            entry_extension_max,
            entry_window_ms,
            True,
        )
        if historical.structure is not None:
            historical_events.append(
                N10HistoricalEvent(historical.structure, historical.reason)
            )

    current = _evaluate_event(
        symbol,
        candles,
        len(candles) - 1,
        now_ms,
        support_bars,
        support_tolerance_fraction,
        support_minimum_gap,
        volume_median_bars,
        volume_spike_multiple,
        sweep_depth_min,
        sweep_depth_max,
        lower_wick_ratio_min,
        taker_buy_ratio_min,
        entry_extension_max,
        entry_window_ms,
        False,
    )
    return replace(current, historical_events=tuple(historical_events))
