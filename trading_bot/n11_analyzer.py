from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


FIFTEEN_MINUTES_MS = 15 * 60 * 1000

N11_REASON_CODES = (
    "N11_NOT_ENOUGH_HISTORY",
    "N11_KLINE_DATA_INVALID",
    "N11_KLINE_SEQUENCE_INVALID",
    "N11_STRUCTURE_NOT_FOUND",
    "N11_TREND_FILTER_NOT_MET",
    "N11_BREAKOUT_NOT_CONFIRMED",
    "N11_BREAKOUT_BODY_TOO_SMALL",
    "N11_BREAKOUT_CLOSE_LOCATION_TOO_LOW",
    "N11_BREAKOUT_VOLUME_TOO_LOW",
    "N11_BREAKOUT_TAKER_BUY_RATIO_TOO_LOW",
    "N11_RETEST_PENDING",
    "N11_RETEST_WINDOW_EXPIRED",
    "N11_RETEST_TOO_DEEP",
    "N11_RETEST_NOT_HELD",
    "N11_RETEST_CLOSE_LOCATION_TOO_LOW",
    "N11_RETEST_VOLUME_TOO_HIGH",
    "N11_HISTORICAL_ENTRY_MISSED",
    "N11_ENTRY_WINDOW_EXPIRED",
    "N11_ENTRY_PRICE_BELOW_BREAKOUT",
    "N11_ENTRY_PRICE_TOO_EXTENDED",
    "N11_STRUCTURE_CONSUMED",
    "N11_STATE_READ_FAILED",
    "N11_STATE_PERSIST_FAILED",
    "N11_STOP_PCT_OUT_OF_RANGE",
    "N11_ENTRY_RANGE_MISSING",
    "N11_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE",
)


@dataclass(frozen=True)
class N11Candle:
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
class N11Structure:
    symbol: str
    squeeze_start_time: str
    squeeze_end_time: str
    squeeze_bars: int
    breakout_level: Decimal
    atr_reference: Decimal
    ema20: Decimal
    ema50: Decimal
    breakout: N11Candle
    retest: N11Candle | None
    entry: N11Candle | None
    retest_lower_price: Decimal
    retest_upper_price: Decimal
    entry_upper_price: Decimal | None
    volume_median_20: Decimal
    breakout_volume_multiple: Decimal
    breakout_taker_buy_ratio: Decimal
    structure_id: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "structure_id": self.structure_id,
            "squeeze_start_time": self.squeeze_start_time,
            "squeeze_end_time": self.squeeze_end_time,
            "squeeze_bars": self.squeeze_bars,
            "breakout_level": str(self.breakout_level),
            "atr_reference": str(self.atr_reference),
            "ema20": str(self.ema20),
            "ema50": str(self.ema50),
            "breakout": self.breakout.to_jsonable(),
            "retest": self.retest.to_jsonable() if self.retest is not None else None,
            "entry": self.entry.to_jsonable() if self.entry is not None else None,
            "retest_lower_price": str(self.retest_lower_price),
            "retest_upper_price": str(self.retest_upper_price),
            "entry_upper_price": (
                str(self.entry_upper_price) if self.entry_upper_price is not None else None
            ),
            "volume_median_20": str(self.volume_median_20),
            "breakout_volume_multiple": str(self.breakout_volume_multiple),
            "breakout_taker_buy_ratio": str(self.breakout_taker_buy_ratio),
        }


@dataclass(frozen=True)
class N11HistoricalEvent:
    structure: N11Structure
    status: str
    reason: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "structure": self.structure.to_jsonable(),
        }


@dataclass(frozen=True)
class N11AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    structure: N11Structure | None
    current_price: Decimal
    current_open_time: str | None
    checked_at: str
    elapsed_ms: int | None
    entry_window_ms: int
    consume_current: bool
    detail: str
    historical_events: tuple[N11HistoricalEvent, ...] = ()

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
            "detail": self.detail,
            "historical_events": [
                event.to_jsonable() for event in self.historical_events
            ],
        }
        if self.structure is not None:
            payload["structure"] = self.structure.to_jsonable()
        return payload


@dataclass(frozen=True)
class _IndicatorSeries:
    ema20: list[Decimal | None]
    ema50: list[Decimal | None]
    atr14: list[Decimal | None]


def _decimal(value: Any) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise InvalidOperation("non-finite decimal")
    return parsed


def decimal_median(values: list[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("median requires at least one value")
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal("2")


def parse_n11_klines(raw_klines: list[list[Any]]) -> list[N11Candle]:
    candles: list[N11Candle] = []
    for index, row in enumerate(raw_klines):
        if len(row) <= 10:
            raise ValueError("N11 kline requires indexes 0-10")
        open_time = _decimal(row[0])
        if open_time != open_time.to_integral_value() or open_time <= 0:
            raise ValueError("N11 kline open time is invalid")
        candle = N11Candle(
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
            raise ValueError("N11 kline contains invalid prices or volumes")
        candles.append(candle)
    return candles


def _sequence_is_continuous(candles: list[N11Candle]) -> bool:
    return all(
        right.open_time_ms - left.open_time_ms == FIFTEEN_MINUTES_MS
        for left, right in zip(candles, candles[1:])
    )


def _ema_series(values: list[Decimal], period: int) -> list[Decimal | None]:
    result: list[Decimal | None] = [None] * len(values)
    if len(values) < period:
        return result
    seed = sum(values[:period], Decimal("0")) / Decimal(period)
    result[period - 1] = seed
    multiplier = Decimal("2") / Decimal(period + 1)
    previous = seed
    for index in range(period, len(values)):
        previous = (values[index] - previous) * multiplier + previous
        result[index] = previous
    return result


def _atr_series(candles: list[N11Candle], period: int) -> list[Decimal | None]:
    result: list[Decimal | None] = [None] * len(candles)
    if len(candles) < period:
        return result
    true_ranges: list[Decimal] = []
    for index, candle in enumerate(candles):
        if index == 0:
            true_ranges.append(candle.high - candle.low)
            continue
        previous_close = candles[index - 1].close
        true_ranges.append(
            max(
                candle.high - candle.low,
                abs(candle.high - previous_close),
                abs(candle.low - previous_close),
            )
        )
    seed = sum(true_ranges[:period], Decimal("0")) / Decimal(period)
    result[period - 1] = seed
    previous = seed
    for index in range(period, len(candles)):
        previous = (
            previous * Decimal(period - 1) + true_ranges[index]
        ) / Decimal(period)
        result[index] = previous
    return result


def _indicator_series(candles: list[N11Candle]) -> _IndicatorSeries:
    closes = [candle.close for candle in candles]
    return _IndicatorSeries(
        ema20=_ema_series(closes, 20),
        ema50=_ema_series(closes, 50),
        atr14=_atr_series(candles, 14),
    )


def _is_squeeze(
    candles: list[N11Candle],
    indicators: _IndicatorSeries,
    index: int,
    bollinger_stddevs: Decimal,
    keltner_atr_multiple: Decimal,
) -> bool:
    if index < 19:
        return False
    ema20 = indicators.ema20[index]
    atr14 = indicators.atr14[index]
    if ema20 is None or atr14 is None or atr14 <= 0:
        return False
    closes = [item.close for item in candles[index - 19 : index + 1]]
    mean = sum(closes, Decimal("0")) / Decimal("20")
    variance = sum((value - mean) ** 2 for value in closes) / Decimal("20")
    standard_deviation = variance.sqrt()
    bb_upper = mean + bollinger_stddevs * standard_deviation
    bb_lower = mean - bollinger_stddevs * standard_deviation
    kc_upper = ema20 + keltner_atr_multiple * atr14
    kc_lower = ema20 - keltner_atr_multiple * atr14
    return bb_upper <= kc_upper and bb_lower >= kc_lower


def _structure_id(
    symbol: str,
    squeeze: list[N11Candle],
    breakout: N11Candle,
    breakout_level: Decimal,
) -> str:
    raw = "|".join(
        (
            symbol,
            squeeze[0].open_time,
            squeeze[-1].open_time,
            breakout.open_time,
            str(breakout_level),
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _qualify_breakout(
    symbol: str,
    candles: list[N11Candle],
    indicators: _IndicatorSeries,
    breakout_index: int,
    squeeze_bars: int,
    breakout_lookback_bars: int,
    bollinger_stddevs: Decimal,
    keltner_atr_multiple: Decimal,
    breakout_body_atr_min: Decimal,
    breakout_close_location_min: Decimal,
    volume_median_bars: int,
    breakout_volume_multiple_min: Decimal,
    breakout_taker_buy_ratio_min: Decimal,
    retest_atr_tolerance: Decimal,
) -> tuple[bool, N11Structure | None, str | None]:
    squeeze_start = breakout_index - squeeze_bars
    if squeeze_start < 0:
        return False, None, None
    squeeze_indexes = range(squeeze_start, breakout_index)
    if not all(
        _is_squeeze(
            candles,
            indicators,
            index,
            bollinger_stddevs,
            keltner_atr_multiple,
        )
        for index in squeeze_indexes
    ):
        return False, None, None

    breakout = candles[breakout_index]
    lookback_start = breakout_index - breakout_lookback_bars
    if lookback_start < 0:
        return False, None, None
    breakout_history = candles[lookback_start:breakout_index]
    breakout_level = max(item.high for item in breakout_history)
    if breakout.close <= breakout_level or breakout.close <= breakout.open:
        return False, None, None
    ema20 = indicators.ema20[breakout_index]
    ema50 = indicators.ema50[breakout_index]
    atr_reference = indicators.atr14[breakout_index - 1]
    if ema20 is None or ema50 is None or atr_reference is None or atr_reference <= 0:
        return True, None, "N11_NOT_ENOUGH_HISTORY"
    if ema20 <= ema50:
        return True, None, "N11_TREND_FILTER_NOT_MET"
    if breakout.close - breakout.open < breakout_body_atr_min * atr_reference:
        return True, None, "N11_BREAKOUT_BODY_TOO_SMALL"
    close_location = (breakout.close - breakout.low) / (breakout.high - breakout.low)
    if close_location < breakout_close_location_min:
        return True, None, "N11_BREAKOUT_CLOSE_LOCATION_TOO_LOW"

    historical_volumes = [
        item.quote_volume
        for item in candles[breakout_index - volume_median_bars : breakout_index]
    ]
    if (
        len(historical_volumes) != volume_median_bars
        or any(value <= 0 for value in historical_volumes)
        or breakout.quote_volume <= 0
    ):
        return True, None, "N11_KLINE_DATA_INVALID"
    volume_median = decimal_median(historical_volumes)
    volume_multiple = breakout.quote_volume / volume_median
    if volume_multiple < breakout_volume_multiple_min:
        return True, None, "N11_BREAKOUT_VOLUME_TOO_LOW"
    taker_buy_ratio = breakout.taker_buy_quote_volume / breakout.quote_volume
    if taker_buy_ratio < breakout_taker_buy_ratio_min:
        return True, None, "N11_BREAKOUT_TAKER_BUY_RATIO_TOO_LOW"

    squeeze = candles[squeeze_start:breakout_index]
    tolerance = retest_atr_tolerance * atr_reference
    structure = N11Structure(
        symbol=symbol,
        squeeze_start_time=squeeze[0].open_time,
        squeeze_end_time=squeeze[-1].open_time,
        squeeze_bars=squeeze_bars,
        breakout_level=breakout_level,
        atr_reference=atr_reference,
        ema20=ema20,
        ema50=ema50,
        breakout=breakout,
        retest=None,
        entry=None,
        retest_lower_price=breakout_level - tolerance,
        retest_upper_price=breakout_level + tolerance,
        entry_upper_price=None,
        volume_median_20=volume_median,
        breakout_volume_multiple=volume_multiple,
        breakout_taker_buy_ratio=taker_buy_ratio,
        structure_id=_structure_id(symbol, squeeze, breakout, breakout_level),
    )
    return True, structure, None


def _result(
    symbol: str,
    reason: str,
    current: N11Candle | None,
    checked_at: str,
    entry_window_ms: int,
    *,
    structure: N11Structure | None = None,
    elapsed_ms: int | None = None,
    consume_current: bool = False,
    passed: bool = False,
    detail: str = "",
) -> N11AnalysisResult:
    return N11AnalysisResult(
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
        detail=detail or reason,
    )


def _terminal_status(reason: str) -> str:
    if reason in {
        "N11_RETEST_TOO_DEEP",
        "N11_RETEST_NOT_HELD",
        "N11_RETEST_CLOSE_LOCATION_TOO_LOW",
        "N11_RETEST_VOLUME_TOO_HIGH",
    }:
        return "INVALID"
    return "MISSED"


def analyze_n11_volatility_squeeze_breakout_retest(
    symbol: str,
    raw_klines: list[list[Any]],
    *,
    squeeze_bars: int = 8,
    breakout_lookback_bars: int = 20,
    bollinger_stddevs: Decimal = Decimal("2"),
    keltner_atr_multiple: Decimal = Decimal("1.5"),
    breakout_body_atr_min: Decimal = Decimal("0.6"),
    breakout_close_location_min: Decimal = Decimal("0.75"),
    volume_median_bars: int = 20,
    breakout_volume_multiple_min: Decimal = Decimal("1.8"),
    breakout_taker_buy_ratio_min: Decimal = Decimal("0.55"),
    retest_max_bars: int = 6,
    retest_atr_tolerance: Decimal = Decimal("0.3"),
    retest_close_location_min: Decimal = Decimal("0.5"),
    retest_volume_ratio_max: Decimal = Decimal("0.8"),
    entry_extension_atr_max: Decimal = Decimal("0.5"),
    entry_window_seconds: int = 120,
    checked_at_ms: int | None = None,
) -> N11AnalysisResult:
    positive_counts = {
        "squeeze_bars": squeeze_bars,
        "breakout_lookback_bars": breakout_lookback_bars,
        "volume_median_bars": volume_median_bars,
        "retest_max_bars": retest_max_bars,
        "entry_window_seconds": entry_window_seconds,
    }
    if any(value < 1 for value in positive_counts.values()):
        raise ValueError(f"N11 count parameters must be positive: {positive_counts}")
    positive_decimals = {
        "bollinger_stddevs": bollinger_stddevs,
        "keltner_atr_multiple": keltner_atr_multiple,
        "breakout_volume_multiple_min": breakout_volume_multiple_min,
    }
    if any(not value.is_finite() or value <= 0 for value in positive_decimals.values()):
        raise ValueError(f"N11 positive parameters are invalid: {positive_decimals}")
    nonnegative_decimals = {
        "breakout_body_atr_min": breakout_body_atr_min,
        "retest_atr_tolerance": retest_atr_tolerance,
        "entry_extension_atr_max": entry_extension_atr_max,
    }
    if any(
        not value.is_finite() or value < 0
        for value in nonnegative_decimals.values()
    ):
        raise ValueError(f"N11 nonnegative parameters are invalid: {nonnegative_decimals}")
    unit_interval_decimals = {
        "breakout_close_location_min": breakout_close_location_min,
        "breakout_taker_buy_ratio_min": breakout_taker_buy_ratio_min,
        "retest_close_location_min": retest_close_location_min,
        "retest_volume_ratio_max": retest_volume_ratio_max,
    }
    if any(
        not value.is_finite() or value < 0 or value > 1
        for value in unit_interval_decimals.values()
    ):
        raise ValueError(
            f"N11 ratio parameters must be within [0, 1]: {unit_interval_decimals}"
        )
    now_ms = checked_at_ms
    if now_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    checked_at = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat()
    entry_window_ms = entry_window_seconds * 1000
    try:
        candles = parse_n11_klines(raw_klines)
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return _result(
            symbol,
            "N11_KLINE_DATA_INVALID",
            None,
            checked_at,
            entry_window_ms,
        )
    minimum_history = max(50, breakout_lookback_bars, volume_median_bars) + 3
    current = candles[-1] if candles else None
    if len(candles) < minimum_history:
        return _result(
            symbol,
            "N11_NOT_ENOUGH_HISTORY",
            current,
            checked_at,
            entry_window_ms,
        )
    if not _sequence_is_continuous(candles):
        return _result(
            symbol,
            "N11_KLINE_SEQUENCE_INVALID",
            current,
            checked_at,
            entry_window_ms,
        )

    indicators = _indicator_series(candles)
    closed_end = len(candles) - 2
    historical_events: list[N11HistoricalEvent] = []
    current_results: list[N11AnalysisResult] = []
    latest_breakout_failure: str | None = None
    previous_breakout_index: int | None = None

    for breakout_index in range(49, closed_end + 1):
        if (
            previous_breakout_index is not None
            and breakout_index - squeeze_bars <= previous_breakout_index
        ):
            continue
        considered, structure, failure = _qualify_breakout(
            symbol,
            candles,
            indicators,
            breakout_index,
            squeeze_bars,
            breakout_lookback_bars,
            bollinger_stddevs,
            keltner_atr_multiple,
            breakout_body_atr_min,
            breakout_close_location_min,
            volume_median_bars,
            breakout_volume_multiple_min,
            breakout_taker_buy_ratio_min,
            retest_atr_tolerance,
        )
        if not considered:
            continue
        previous_breakout_index = breakout_index
        if structure is None:
            latest_breakout_failure = failure
            continue

        first_touch: N11Candle | None = None
        last_retest_index = min(
            structure.breakout.index + retest_max_bars,
            closed_end,
        )
        for index in range(structure.breakout.index + 1, last_retest_index + 1):
            candidate_retest = candles[index]
            if candidate_retest.low <= structure.retest_upper_price:
                first_touch = candidate_retest
                break

        if first_touch is None:
            expiry_index = structure.breakout.index + retest_max_bars
            if closed_end < expiry_index:
                current_results.append(
                    _result(
                        symbol,
                        "N11_RETEST_PENDING",
                        current,
                        checked_at,
                        entry_window_ms,
                        structure=structure,
                        detail=(
                            f"structure={structure.structure_id} breakout="
                            f"{structure.breakout.open_time} waiting_first_retest"
                        ),
                    )
                )
                continue
            reason = "N11_RETEST_WINDOW_EXPIRED"
            if expiry_index == closed_end:
                current_results.append(
                    _result(
                        symbol,
                        reason,
                        current,
                        checked_at,
                        entry_window_ms,
                        structure=structure,
                        consume_current=True,
                    )
                )
            else:
                historical_events.append(
                    N11HistoricalEvent(structure, _terminal_status(reason), reason)
                )
            continue

        entry_upper = first_touch.close + entry_extension_atr_max * structure.atr_reference
        structure = replace(
            structure,
            retest=first_touch,
            entry_upper_price=entry_upper,
        )
        retest_reason: str | None = None
        if first_touch.low < structure.retest_lower_price:
            retest_reason = "N11_RETEST_TOO_DEEP"
        elif first_touch.close < structure.breakout_level:
            retest_reason = "N11_RETEST_NOT_HELD"
        else:
            close_location = (
                (first_touch.close - first_touch.low)
                / (first_touch.high - first_touch.low)
            )
            if close_location < retest_close_location_min:
                retest_reason = "N11_RETEST_CLOSE_LOCATION_TOO_LOW"
            elif first_touch.quote_volume > (
                structure.breakout.quote_volume * retest_volume_ratio_max
            ):
                retest_reason = "N11_RETEST_VOLUME_TOO_HIGH"

        if retest_reason is not None:
            if first_touch.index == closed_end:
                current_results.append(
                    _result(
                        symbol,
                        retest_reason,
                        current,
                        checked_at,
                        entry_window_ms,
                        structure=structure,
                        consume_current=True,
                    )
                )
            else:
                historical_events.append(
                    N11HistoricalEvent(
                        structure,
                        _terminal_status(retest_reason),
                        retest_reason,
                    )
                )
            continue

        entry_index = first_touch.index + 1
        if entry_index <= closed_end:
            historical_structure = replace(structure, entry=candles[entry_index])
            historical_events.append(
                N11HistoricalEvent(
                    historical_structure,
                    "MISSED",
                    "N11_HISTORICAL_ENTRY_MISSED",
                )
            )
            continue
        if entry_index != len(candles) - 1:
            continue

        entry = candles[entry_index]
        structure = replace(structure, entry=entry)
        elapsed_ms = now_ms - entry.open_time_ms
        if elapsed_ms < 0 or elapsed_ms >= entry_window_ms:
            reason = "N11_ENTRY_WINDOW_EXPIRED"
        elif entry.close < structure.breakout_level:
            reason = "N11_ENTRY_PRICE_BELOW_BREAKOUT"
        elif entry.close > entry_upper:
            reason = "N11_ENTRY_PRICE_TOO_EXTENDED"
        else:
            reason = "PASSED"
        current_results.append(
            _result(
                symbol,
                reason,
                entry,
                checked_at,
                entry_window_ms,
                structure=structure,
                elapsed_ms=elapsed_ms,
                consume_current=True,
                passed=reason == "PASSED",
                detail=(
                    f"structure={structure.structure_id} breakout="
                    f"{structure.breakout.open_time} retest={first_touch.open_time} "
                    f"entry={entry.open_time} elapsed_ms={elapsed_ms}"
                ),
            )
        )

    if current_results:
        selected = max(
            current_results,
            key=lambda item: (
                item.structure.breakout.open_time_ms
                if item.structure is not None
                else -1
            ),
        )
    else:
        selected = _result(
            symbol,
            latest_breakout_failure or "N11_STRUCTURE_NOT_FOUND",
            current,
            checked_at,
            entry_window_ms,
        )
    return replace(selected, historical_events=tuple(historical_events))
