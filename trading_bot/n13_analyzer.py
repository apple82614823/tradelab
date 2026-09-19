from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from typing import Any


INTERVAL_MS = 900_000
_N13_SIGNAL_DETAIL_MAX_BYTES = 2_048
_N13_SIGNAL_MAX_EVENTS = 4
_N13_SIGNAL_MAX_CONFIRMATION_OUTCOMES = 4
_N13_SIGNAL_TEXT_MAX = 128
_N13_SIGNAL_DECIMAL_TEXT_MAX = 64
_N13_SIGNAL_INVALID = "INVALID"
_N13_SIGNAL_MAX_TIME_MS = 9_223_372_036_854_000_000
_N13_SIGNAL_MAX_EVENT_COUNT = 10_000
_N13_SIGNAL_T_STAGE_REASONS = frozenset(
    {
        "N13_VALUE_BAND_BROKEN",
        "N13_CONFIRMATION_NOT_FOUND",
    }
)
_N13_SIGNAL_A_STAGE_REASONS = frozenset(
    {
        "N13_ARMED_WINDOW_EXPIRED",
        "N13_VALUE_ZONE_SKIPPED",
    }
)
_N13_SIGNAL_CONFIRMATION_OUTCOMES = frozenset(
    {
        "N13_VALUE_BAND_BROKEN",
        "N13_CONFIRMATION_FLAT_CANDLE",
        "N13_CONFIRMATION_CLOSE_BELOW_VWAP",
        "N13_CONFIRMATION_NOT_BULLISH",
        "N13_CONFIRMATION_CLOSE_NOT_ADVANCING",
        "N13_CONFIRMATION_CLOSE_LOCATION_TOO_LOW",
        "N13_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW",
    }
)
_N13_SIGNAL_T_STAGE_V2_KEYS = frozenset(
    {
        "schema_version",
        "symbol",
        "t_time",
        "reason",
        "a",
        "t",
        "vwap_a",
        "atr_a",
        "zone_lower",
        "zone_upper",
        "armed_expiry_time_ms",
        "armed_max_bars",
        "vwap_lookback_bars",
        "atr_period",
        "upper_atr_fraction",
        "lower_atr_fraction",
        "confirmation_max_bars",
        "confirmation_close_location_min",
        "confirmation_taker_buy_ratio_min",
        "metric_context_bars",
        "pre_touch_bars",
        "confirmation_checks",
        "failed_confirmation_reasons",
        "canonical_sha256",
    }
)


@dataclass(frozen=True)
class N13Candle:
    index: int
    open_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    base_volume: Decimal
    quote_volume: Decimal
    taker_buy_quote_volume: Decimal

    def json(self) -> dict[str, Any]:
        return {
            key: str(value) if isinstance(value, Decimal) else value
            for key, value in self.__dict__.items()
        }


@dataclass(frozen=True)
class N13BarMetric:
    vwap: Decimal
    atr: Decimal
    upper: Decimal
    lower: Decimal


@dataclass(frozen=True)
class N13Structure:
    symbol: str
    a: N13Candle
    t: N13Candle
    c: N13Candle
    entry: N13Candle | None
    p: Decimal
    vwap_c: Decimal
    atr_c: Decimal
    entry_min_price: Decimal
    entry_max_price: Decimal
    return_24h: Decimal | None
    return_rank: int | None
    positive_return_breadth: Decimal | None
    above_vwap_breadth: Decimal | None
    snapshot_context_available: bool
    zone_lower: Decimal
    zone_upper: Decimal
    armed_expiry_time_ms: int
    failed_confirmation_reasons: tuple[str, ...]
    structure_id: str

    def json(self) -> dict[str, Any]:
        return {
            "structure_id": self.structure_id, "a": self.a.json(), "t": self.t.json(), "c": self.c.json(),
            "entry": self.entry.json() if self.entry else None, "p": str(self.p),
            "vwap_c": str(self.vwap_c), "atr_c": str(self.atr_c),
            "entry_min_price": str(self.entry_min_price),
            "entry_max_price": str(self.entry_max_price),
            "return_24h": str(self.return_24h) if self.return_24h is not None else None, "return_rank": self.return_rank,
            "positive_return_breadth": str(self.positive_return_breadth) if self.positive_return_breadth is not None else None,
            "above_vwap_breadth": str(self.above_vwap_breadth) if self.above_vwap_breadth is not None else None,
            "snapshot_context_available": self.snapshot_context_available,
            "zone_lower": str(self.zone_lower), "zone_upper": str(self.zone_upper),
            "armed_expiry_time_ms": self.armed_expiry_time_ms,
            "failed_confirmation_reasons": list(self.failed_confirmation_reasons),
        }


@dataclass(frozen=True)
class N13Event:
    t_time: str
    status: str
    reason: str
    structure: N13Structure | None = None
    detail: dict[str, Any] | None = None


@dataclass(frozen=True)
class N13AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    structure: N13Structure | None
    consume_current: bool
    historical_events: tuple[N13Event, ...]
    stage_events: tuple[N13Event, ...]
    current_price: Decimal
    elapsed_ms: int | None
    return_rank: int | None
    return_24h: Decimal | None
    vwap_slope_reference: Decimal | None = None

    @property
    def structure_id(self) -> str | None:
        return self.structure.structure_id if self.structure else None

    @property
    def current_bullish(self) -> bool:
        return bool(
            self.structure
            and self.structure.entry
            and self.structure.entry.close > self.structure.entry.open
        )

    def detail_json(self) -> dict[str, Any]:
        fallback = _n13_signal_analysis_fallback(self)
        try:
            if (
                type(self.historical_events) is not tuple
                or type(self.stage_events) is not tuple
            ):
                return fallback
            historical_events = self.historical_events[-_N13_SIGNAL_MAX_EVENTS:]
            stage_events = self.stage_events[-_N13_SIGNAL_MAX_EVENTS:]
            payload = {
                "reason": _n13_signal_text(self.reason),
                "structure_id": _n13_signal_structure_id(self.structure_id),
                "consume_current": (
                    self.consume_current
                    if type(self.consume_current) is bool
                    else _N13_SIGNAL_INVALID
                ),
                "current_price": _n13_signal_decimal_value(
                    self.current_price
                ),
                "elapsed_ms": _n13_signal_elapsed_ms(self.elapsed_ms),
                "return_rank": _n13_signal_return_rank(self.return_rank),
                "return_24h": _n13_signal_optional_decimal_value(
                    self.return_24h
                ),
                "structure": _n13_signal_structure_summary(
                    self.structure,
                    vwap_slope_reference=self.vwap_slope_reference,
                ),
                "historical_event_count": _n13_signal_event_count(
                    self.historical_events
                ),
                "historical_events": [
                    _n13_signal_event(event, historical=True)
                    for event in historical_events
                ],
                "stage_event_count": _n13_signal_event_count(
                    self.stage_events
                ),
                "stage_events": [
                    _n13_signal_event(event, historical=False)
                    for event in stage_events
                ],
            }
            encoded = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            return (
                payload
                if len(encoded) <= _N13_SIGNAL_DETAIL_MAX_BYTES
                else fallback
            )
        except Exception:
            return fallback


def _d(value: Any) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise InvalidOperation
    return result


def parse_n13_klines(raw: list[list[Any]]) -> list[N13Candle]:
    result = []
    for index, row in enumerate(raw):
        if len(row) <= 10:
            raise ValueError("N13 kline fields missing")
        item = N13Candle(
            index,
            int(row[0]),
            _d(row[1]),
            _d(row[2]),
            _d(row[3]),
            _d(row[4]),
            _d(row[5]),
            _d(row[7]),
            _d(row[10]),
        )
        if (
            min(item.open, item.high, item.low, item.close) <= 0
            or item.high < item.low
            or item.high < max(item.open, item.close)
            or item.low > min(item.open, item.close)
            or min(
                item.base_volume,
                item.quote_volume,
                item.taker_buy_quote_volume,
            )
            < 0
            or item.taker_buy_quote_volume > item.quote_volume
        ):
            raise ValueError("N13 invalid kline")
        result.append(item)
    if any(
        right.open_time_ms - left.open_time_ms != INTERVAL_MS
        for left, right in zip(result, result[1:])
    ):
        raise ValueError("N13 kline gap")
    return result


def n13_metrics(
    candles: list[N13Candle],
    *,
    vwap_lookback_bars: int = 96,
    atr_period: int = 14,
    upper_atr_fraction: Decimal = Decimal("0.25"),
    lower_atr_fraction: Decimal = Decimal("0.50"),
) -> dict[int, N13BarMetric]:
    if (
        vwap_lookback_bars <= 0
        or atr_period <= 0
        or upper_atr_fraction < 0
        or lower_atr_fraction < 0
    ):
        raise ValueError("invalid N13 metric parameters")
    metrics: dict[int, N13BarMetric] = {}
    atr: Decimal | None = None
    trs: list[Decimal] = []
    for i, bar in enumerate(candles):
        prev = candles[i - 1].close if i else bar.close
        tr = max(bar.high - bar.low, abs(bar.high - prev), abs(bar.low - prev))
        trs.append(tr)
        if i == atr_period - 1:
            atr = (
                sum(trs[:atr_period], Decimal("0"))
                / Decimal(atr_period)
            )
        elif i > atr_period - 1:
            atr = (
                atr * Decimal(atr_period - 1) + tr
            ) / Decimal(atr_period)
        if i < vwap_lookback_bars - 1 or atr is None:
            continue
        window = candles[i - vwap_lookback_bars + 1:i + 1]
        base = sum((item.base_volume for item in window), Decimal("0"))
        quote = sum((item.quote_volume for item in window), Decimal("0"))
        if base <= 0 or quote <= 0:
            raise ValueError("N13 VWAP volume invalid")
        vwap = quote / base
        metrics[i] = N13BarMetric(
            vwap,
            atr,
            vwap + upper_atr_fraction * atr,
            vwap - lower_atr_fraction * atr,
        )
    return metrics


def _sid(symbol: str, t: N13Candle, c: N13Candle) -> str:
    return hashlib.sha256(f"{symbol}|{t.open_time_ms}|{c.open_time_ms}".encode()).hexdigest()[:24]


def _stable_stage_audit_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _stable_stage_audit_value(item)
            for key, item in value.items()
            if key != "index"
        }
    if isinstance(value, (list, tuple)):
        return [_stable_stage_audit_value(item) for item in value]
    return value


def _n13_signal_text(value: Any) -> str:
    return (
        value
        if type(value) is str and 0 < len(value) <= _N13_SIGNAL_TEXT_MAX
        else _N13_SIGNAL_INVALID
    )


def _n13_signal_structure_id(value: Any) -> str | None:
    if value is None:
        return None
    return _n13_signal_text(value)


def _n13_signal_decimal_text(value: Any) -> str:
    if (
        type(value) is not str
        or not 0 < len(value) <= _N13_SIGNAL_DECIMAL_TEXT_MAX
    ):
        return _N13_SIGNAL_INVALID
    try:
        parsed = Decimal(value)
    except (ArithmeticError, InvalidOperation, ValueError):
        return _N13_SIGNAL_INVALID
    return (
        value
        if parsed.is_finite() and str(parsed) == value
        else _N13_SIGNAL_INVALID
    )


def _n13_signal_decimal_value(value: Any) -> str:
    if type(value) is not Decimal or not value.is_finite():
        return _N13_SIGNAL_INVALID
    decimal_tuple = value.as_tuple()
    if (
        len(decimal_tuple.digits) > _N13_SIGNAL_DECIMAL_TEXT_MAX
        or type(decimal_tuple.exponent) is not int
        or abs(decimal_tuple.exponent) > _N13_SIGNAL_MAX_TIME_MS
    ):
        return _N13_SIGNAL_INVALID
    return _n13_signal_decimal_text(str(value))


def _n13_signal_optional_decimal_value(value: Any) -> str | None:
    return None if value is None else _n13_signal_decimal_value(value)


def _n13_signal_elapsed_ms(value: Any) -> int | None | str:
    return (
        value
        if value is None
        or type(value) is int
        and -INTERVAL_MS <= value <= INTERVAL_MS
        else _N13_SIGNAL_INVALID
    )


def _n13_signal_return_rank(value: Any) -> int | None | str:
    return (
        value
        if value is None
        or type(value) is int
        and 1 <= value <= 100
        else _N13_SIGNAL_INVALID
    )


def _n13_signal_event_count(value: Any) -> int | str:
    return (
        len(value)
        if type(value) is tuple
        and len(value) <= _N13_SIGNAL_MAX_EVENT_COUNT
        else _N13_SIGNAL_INVALID
    )


def _n13_signal_open_time(value: Any) -> int | str:
    return (
        value
        if type(value) is int
        and 0 < value <= _N13_SIGNAL_MAX_TIME_MS
        and value % INTERVAL_MS == 0
        else _N13_SIGNAL_INVALID
    )


def _n13_signal_t_time(value: Any) -> str:
    if type(value) is not str or not 0 < len(value) <= 20:
        return _N13_SIGNAL_INVALID
    try:
        parsed = int(value)
    except ValueError:
        return _N13_SIGNAL_INVALID
    return (
        value
        if parsed > 0
        and parsed % INTERVAL_MS == 0
        and str(parsed) == value
        else _N13_SIGNAL_INVALID
    )


def _n13_signal_hash(value: Any) -> str:
    return (
        value
        if type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
        else _N13_SIGNAL_INVALID
    )


def _n13_signal_candle_summary(value: Any) -> dict[str, Any] | str:
    expected = {
        "index",
        "open_time_ms",
        "open",
        "high",
        "low",
        "close",
        "base_volume",
        "quote_volume",
        "taker_buy_quote_volume",
    }
    if (
        type(value) is not dict
        or any(type(key) is not str for key in value)
        or set(value) != expected
        or type(value.get("index")) is not int
    ):
        return _N13_SIGNAL_INVALID
    open_time_ms = _n13_signal_open_time(value.get("open_time_ms"))
    decimal_values = {
        key: _n13_signal_decimal_text(value.get(key))
        for key in expected
        if key not in {"index", "open_time_ms"}
    }
    if (
        open_time_ms == _N13_SIGNAL_INVALID
        or _N13_SIGNAL_INVALID in decimal_values.values()
    ):
        return _N13_SIGNAL_INVALID
    try:
        open_price = Decimal(decimal_values["open"])
        high = Decimal(decimal_values["high"])
        low = Decimal(decimal_values["low"])
        close = Decimal(decimal_values["close"])
        base_volume = Decimal(decimal_values["base_volume"])
        quote_volume = Decimal(decimal_values["quote_volume"])
        taker_quote = Decimal(
            decimal_values["taker_buy_quote_volume"]
        )
    except (ArithmeticError, InvalidOperation):
        return _N13_SIGNAL_INVALID
    if (
        min(open_price, high, low, close) <= 0
        or high < max(open_price, close)
        or low > min(open_price, close)
        or high < low
        or min(base_volume, quote_volume, taker_quote) < 0
        or taker_quote > quote_volume
    ):
        return _N13_SIGNAL_INVALID
    return {
        "open_time_ms": open_time_ms,
        "open": decimal_values["open"],
        "high": decimal_values["high"],
        "low": decimal_values["low"],
        "close": decimal_values["close"],
        "base_volume": decimal_values["base_volume"],
        "quote_volume": decimal_values["quote_volume"],
        "taker_buy_quote_volume": decimal_values[
            "taker_buy_quote_volume"
        ],
    }


def _n13_signal_failure_count(
    value: Any,
    maximum: int = _N13_SIGNAL_MAX_CONFIRMATION_OUTCOMES,
) -> int | str:
    if (
        type(value) is not list
        or type(maximum) is not int
        or not 1 <= maximum <= _N13_SIGNAL_MAX_CONFIRMATION_OUTCOMES
        or len(value) > maximum
        or any(
            type(item) is not str
            or item not in _N13_SIGNAL_CONFIRMATION_OUTCOMES
            for item in value
        )
    ):
        return _N13_SIGNAL_INVALID
    return len(value)


def _n13_signal_confirmation_outcomes(
    value: Any,
    maximum: int,
) -> list[str] | str:
    if (
        type(value) is not list
        or type(maximum) is not int
        or not 1 <= maximum <= _N13_SIGNAL_MAX_CONFIRMATION_OUTCOMES
        or not 1 <= len(value) <= maximum
    ):
        return _N13_SIGNAL_INVALID
    outcomes: list[str] = []
    for check in value:
        if (
            type(check) is not dict
            or any(type(key) is not str for key in check)
            or set(check) != {"candle", "vwap", "outcome"}
            or _n13_signal_candle_summary(check.get("candle"))
            == _N13_SIGNAL_INVALID
            or _n13_signal_decimal_text(check.get("vwap"))
            == _N13_SIGNAL_INVALID
            or type(check.get("outcome")) is not str
            or check["outcome"] not in _N13_SIGNAL_CONFIRMATION_OUTCOMES
        ):
            return _N13_SIGNAL_INVALID
        outcomes.append(check["outcome"])
    return outcomes


def _n13_signal_invalid_stage_detail() -> dict[str, Any]:
    return {
        "audit_detail_omitted": True,
        "audit_detail_status": _N13_SIGNAL_INVALID,
    }


def _n13_signal_stage_detail(reason: Any, value: Any) -> dict[str, Any]:
    """Return a fixed-size audit summary for every runtime stage shape."""
    if (
        type(reason) is not str
        or type(value) is not dict
        or any(type(key) is not str for key in value)
    ):
        return _n13_signal_invalid_stage_detail()
    keys = frozenset(value)
    if reason in _N13_SIGNAL_T_STAGE_REASONS:
        if keys == _N13_SIGNAL_T_STAGE_V2_KEYS:
            context = value.get("metric_context_bars")
            context_count: int | str = (
                len(context)
                if type(context) is list and len(context) <= 1_000
                else _N13_SIGNAL_INVALID
            )
            checks = value.get("confirmation_checks")
            confirmation_max_bars = value.get("confirmation_max_bars")
            bounded_confirmation_max = (
                confirmation_max_bars
                if type(confirmation_max_bars) is int
                and 1
                <= confirmation_max_bars
                <= _N13_SIGNAL_MAX_CONFIRMATION_OUTCOMES
                else _N13_SIGNAL_INVALID
            )
            outcomes = _n13_signal_confirmation_outcomes(
                checks,
                bounded_confirmation_max,
            )
            return {
                "audit_detail_omitted": True,
                "audit_detail_status": "SUMMARY",
                "schema_version": (
                    2
                    if type(value.get("schema_version")) is int
                    and value["schema_version"] == 2
                    else _N13_SIGNAL_INVALID
                ),
                "canonical_sha256": _n13_signal_hash(
                    value.get("canonical_sha256")
                ),
                "metric_context_bar_count": context_count,
                "a": _n13_signal_candle_summary(value.get("a")),
                "t": _n13_signal_candle_summary(value.get("t")),
                "vwap_a": _n13_signal_decimal_text(value.get("vwap_a")),
                "atr_a": _n13_signal_decimal_text(value.get("atr_a")),
                "zone_lower": _n13_signal_decimal_text(
                    value.get("zone_lower")
                ),
                "zone_upper": _n13_signal_decimal_text(
                    value.get("zone_upper")
                ),
                "confirmation_max_bars": bounded_confirmation_max,
                "confirmation_check_count": (
                    len(checks)
                    if type(checks) is list
                    and type(bounded_confirmation_max) is int
                    and len(checks) <= bounded_confirmation_max
                    else _N13_SIGNAL_INVALID
                ),
                "confirmation_outcomes": outcomes,
                "failed_confirmation_reason_count": (
                    _n13_signal_failure_count(
                        value.get("failed_confirmation_reasons"),
                        bounded_confirmation_max,
                    )
                ),
            }
        if keys == {"t", "failed_confirmation_reasons"}:
            return {
                "audit_detail_omitted": True,
                "audit_detail_status": "LEGACY_SUMMARY",
                "t": _n13_signal_candle_summary(value.get("t")),
                "failed_confirmation_reason_count": (
                    _n13_signal_failure_count(
                        value.get("failed_confirmation_reasons")
                    )
                ),
            }
        return _n13_signal_invalid_stage_detail()
    if reason in _N13_SIGNAL_A_STAGE_REASONS:
        allowed = {
            "a",
            "zone_lower",
            "zone_upper",
            "armed_expiry_time_ms",
        }
        if not keys <= allowed or "a" not in keys:
            return _n13_signal_invalid_stage_detail()
        return {
            "audit_detail_omitted": True,
            "audit_detail_status": "STAGE_SUMMARY",
            "a": _n13_signal_candle_summary(value.get("a")),
            "zone_lower": (
                _n13_signal_decimal_text(value.get("zone_lower"))
                if "zone_lower" in value
                else None
            ),
            "zone_upper": (
                _n13_signal_decimal_text(value.get("zone_upper"))
                if "zone_upper" in value
                else None
            ),
            "armed_expiry_time_ms": (
                _n13_signal_open_time(value.get("armed_expiry_time_ms"))
                if "armed_expiry_time_ms" in value
                else None
            ),
        }
    return _n13_signal_invalid_stage_detail()


def _n13_signal_structure_summary(
    value: Any,
    *,
    vwap_slope_reference: Any = None,
) -> dict[str, Any] | str | None:
    if value is None:
        return None
    if type(value) is not N13Structure:
        return _N13_SIGNAL_INVALID
    return {
        "structure_id": _n13_signal_structure_id(value.structure_id),
        "a_open_time_ms": _n13_signal_open_time(value.a.open_time_ms),
        "t_open_time_ms": _n13_signal_open_time(value.t.open_time_ms),
        "c_open_time_ms": _n13_signal_open_time(value.c.open_time_ms),
        "entry_open_time_ms": (
            _n13_signal_open_time(value.entry.open_time_ms)
            if type(value.entry) is N13Candle
            else None
            if value.entry is None
            else _N13_SIGNAL_INVALID
        ),
        "entry_low": (
            _n13_signal_decimal_value(value.entry.low)
            if type(value.entry) is N13Candle
            else None
            if value.entry is None
            else _N13_SIGNAL_INVALID
        ),
        "p": _n13_signal_decimal_value(value.p),
        "vwap_c": _n13_signal_decimal_value(value.vwap_c),
        "vwap_slope_reference": _n13_signal_optional_decimal_value(
            vwap_slope_reference
        ),
        "atr_c": _n13_signal_decimal_value(value.atr_c),
        "entry_min_price": _n13_signal_decimal_value(
            value.entry_min_price
        ),
        "entry_max_price": _n13_signal_decimal_value(
            value.entry_max_price
        ),
        "zone_lower": _n13_signal_decimal_value(value.zone_lower),
        "zone_upper": _n13_signal_decimal_value(value.zone_upper),
        "positive_return_breadth": _n13_signal_optional_decimal_value(
            value.positive_return_breadth
        ),
        "above_vwap_breadth": _n13_signal_optional_decimal_value(
            value.above_vwap_breadth
        ),
        "armed_expiry_time_ms": _n13_signal_open_time(
            value.armed_expiry_time_ms
        ),
    }


def _n13_signal_event(event: Any, *, historical: bool) -> dict[str, Any]:
    if type(event) is not N13Event:
        return {
            "audit_event_status": _N13_SIGNAL_INVALID,
        }
    return {
        "t_time": _n13_signal_t_time(event.t_time),
        "status": _n13_signal_text(event.status),
        "reason": _n13_signal_text(event.reason),
        "structure": _n13_signal_structure_summary(event.structure),
        "detail": (
            None
            if historical and event.detail is None
            else _n13_signal_invalid_stage_detail()
            if historical
            else _n13_signal_stage_detail(event.reason, event.detail)
        ),
    }


def _n13_signal_analysis_fallback(value: Any) -> dict[str, Any]:
    try:
        reason = _n13_signal_text(value.reason)
        structure_id = _n13_signal_structure_id(value.structure_id)
        historical_count: int | str = (
            len(value.historical_events)
            if type(value.historical_events) is tuple
            and len(value.historical_events) <= _N13_SIGNAL_MAX_EVENT_COUNT
            else _N13_SIGNAL_INVALID
        )
        stage_count: int | str = (
            len(value.stage_events)
            if type(value.stage_events) is tuple
            and len(value.stage_events) <= _N13_SIGNAL_MAX_EVENT_COUNT
            else _N13_SIGNAL_INVALID
        )
    except Exception:
        reason = _N13_SIGNAL_INVALID
        structure_id = _N13_SIGNAL_INVALID
        historical_count = _N13_SIGNAL_INVALID
        stage_count = _N13_SIGNAL_INVALID
    return {
        "audit_detail_omitted": True,
        "audit_detail_status": "OVERSIZE_OR_INVALID",
        "reason": reason,
        "structure_id": structure_id,
        "historical_event_count": historical_count,
        "stage_event_count": stage_count,
    }


def _n13_t_stage_audit_detail(
    symbol: str,
    reason: str,
    a: N13Candle,
    t: N13Candle,
    vwap_a: Decimal,
    atr_a: Decimal,
    zone_lower: Decimal,
    zone_upper: Decimal,
    armed_max_bars: int,
    vwap_lookback_bars: int,
    atr_period: int,
    upper_atr_fraction: Decimal,
    lower_atr_fraction: Decimal,
    confirmation_max_bars: int,
    confirmation_close_location_min: Decimal,
    confirmation_taker_buy_ratio_min: Decimal,
    metric_context_bars: list[N13Candle],
    pre_touch_bars: list[N13Candle],
    confirmation_checks: list[dict[str, Any]],
    failed_confirmation_reasons: list[str],
) -> dict[str, Any]:
    unsigned = {
        "schema_version": 2,
        "symbol": symbol,
        "t_time": str(t.open_time_ms),
        "reason": reason,
        "a": a.json(),
        "t": t.json(),
        "vwap_a": str(vwap_a),
        "atr_a": str(atr_a),
        "zone_lower": str(zone_lower),
        "zone_upper": str(zone_upper),
        "armed_expiry_time_ms": (
            a.open_time_ms + armed_max_bars * INTERVAL_MS
        ),
        "armed_max_bars": armed_max_bars,
        "vwap_lookback_bars": vwap_lookback_bars,
        "atr_period": atr_period,
        "upper_atr_fraction": str(upper_atr_fraction),
        "lower_atr_fraction": str(lower_atr_fraction),
        "confirmation_max_bars": confirmation_max_bars,
        "confirmation_close_location_min": str(
            confirmation_close_location_min
        ),
        "confirmation_taker_buy_ratio_min": str(
            confirmation_taker_buy_ratio_min
        ),
        "metric_context_bars": [
            bar.json() for bar in metric_context_bars
        ],
        "pre_touch_bars": [bar.json() for bar in pre_touch_bars],
        "confirmation_checks": confirmation_checks,
        "failed_confirmation_reasons": list(
            failed_confirmation_reasons
        ),
    }
    canonical = json.dumps(
        _stable_stage_audit_value(unsigned),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        **unsigned,
        "canonical_sha256": hashlib.sha256(
            canonical.encode("utf-8")
        ).hexdigest(),
    }


def analyze_n13_vwap_rotation(
    symbol: str,
    raw_klines: list[list[Any]],
    *,
    return_rank: int | None,
    return_24h: Decimal | None,
    positive_return_breadth: Decimal | None,
    above_vwap_breadth: Decimal | None,
    snapshot_context_complete: bool,
    checked_at_ms: int,
    entry_window_seconds: int = 120,
    positive_breadth_min: Decimal = Decimal("0.60"),
    above_vwap_breadth_min: Decimal = Decimal("0.50"),
    return_rank_min: int = 11,
    return_rank_max: int = 60,
    vwap_lookback_bars: int = 96,
    atr_period: int = 14,
    vwap_slope_lookback_bars: int = 8,
    upper_atr_fraction: Decimal = Decimal("0.25"),
    lower_atr_fraction: Decimal = Decimal("0.50"),
    armed_max_bars: int = 8,
    confirmation_max_bars: int = 4,
    confirmation_close_location_min: Decimal = Decimal("0.60"),
    confirmation_taker_buy_ratio_min: Decimal = Decimal("0.50"),
    entry_extension_atr_max: Decimal = Decimal("0.50"),
) -> N13AnalysisResult:
    vwap_slope_reference: Decimal | None = None

    def result(
        reason,
        structure=None,
        passed=False,
        consume=False,
        historical=(),
        stages=(),
        elapsed=None,
    ):
        current = candles[-1] if candles else None
        return N13AnalysisResult(
            symbol,
            passed,
            reason,
            structure,
            consume,
            tuple(historical),
            tuple(stages),
            current.close if current else Decimal("0"),
            elapsed,
            return_rank,
            return_24h,
            vwap_slope_reference,
        )
    try:
        if (
            vwap_lookback_bars <= 0
            or atr_period <= 0
            or vwap_slope_lookback_bars <= 0
            or armed_max_bars <= 0
            or confirmation_max_bars <= 0
            or entry_window_seconds <= 0
            or upper_atr_fraction < 0
            or lower_atr_fraction < 0
            or entry_extension_atr_max < 0
            or not Decimal("0") <= confirmation_close_location_min <= Decimal("1")
            or not Decimal("0") <= confirmation_taker_buy_ratio_min <= Decimal("1")
            or not Decimal("0") <= positive_breadth_min <= Decimal("1")
            or not Decimal("0") <= above_vwap_breadth_min <= Decimal("1")
            or return_rank_min <= 0
            or return_rank_max < return_rank_min
        ):
            raise ValueError
        candles = parse_n13_klines(raw_klines)
        minimum_closed_bars = max(
            vwap_lookback_bars + vwap_slope_lookback_bars,
            atr_period,
        )
        if len(candles) < minimum_closed_bars + 1:
            raise ValueError
        closed = candles[:-1]
        metrics = n13_metrics(
            closed,
            vwap_lookback_bars=vwap_lookback_bars,
            atr_period=atr_period,
            upper_atr_fraction=upper_atr_fraction,
            lower_atr_fraction=lower_atr_fraction,
        )
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        candles = []
        return result("N13_RELATIVE_RANK_CONTEXT_INSUFFICIENT")
    if not snapshot_context_complete:
        return result("N13_RELATIVE_RANK_CONTEXT_INSUFFICIENT")
    try:
        if (
            return_rank is None
            or not isinstance(return_rank, int)
            or isinstance(return_rank, bool)
            or not 1 <= return_rank <= 100
            or return_24h is None
            or positive_return_breadth is None
            or above_vwap_breadth is None
        ):
            raise ValueError
        return_24h = _d(return_24h)
        positive_return_breadth = _d(positive_return_breadth)
        above_vwap_breadth = _d(above_vwap_breadth)
        if (
            not Decimal("0") <= positive_return_breadth <= Decimal("1")
            or not Decimal("0") <= above_vwap_breadth <= Decimal("1")
        ):
            raise ValueError
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return result("N13_RELATIVE_RANK_CONTEXT_INSUFFICIENT")
    last_index = len(closed) - 1
    reference_index = last_index - vwap_slope_lookback_bars
    if reference_index in metrics:
        vwap_slope_reference = metrics[reference_index].vwap
    if (
        positive_return_breadth < positive_breadth_min
        or above_vwap_breadth < above_vwap_breadth_min
    ):
        market_reason = "N13_MARKET_BREADTH_NOT_MET"
    elif return_24h <= 0:
        market_reason = "N13_RETURN_NOT_POSITIVE"
    elif not return_rank_min <= return_rank <= return_rank_max:
        market_reason = "N13_RETURN_RANK_OUT_OF_RANGE"
    elif (
        last_index not in metrics
        or last_index - vwap_slope_lookback_bars not in metrics
        or metrics[last_index].vwap
        <= metrics[last_index - vwap_slope_lookback_bars].vwap
    ):
        market_reason = "N13_VWAP_NOT_RISING"
    else:
        market_reason = None

    historical: list[N13Event] = []
    stages: list[N13Event] = []
    current_candidates: list[N13Structure] = []
    armed = False
    armed_a_index = None
    armed_vwap = None
    armed_atr = None
    armed_lower = None
    armed_upper = None
    pending_touch = False
    i = max(vwap_lookback_bars - 1, atr_period - 1)
    while i <= last_index:
        metric = metrics.get(i)
        if metric is None:
            i += 1; continue
        if not armed:
            if closed[i].low > metric.upper:
                armed = True
                armed_a_index = i
                armed_vwap = metric.vwap
                armed_atr = metric.atr
                armed_lower = metric.lower
                armed_upper = metric.upper
            i += 1; continue
        if i > armed_a_index + armed_max_bars:
            a = closed[armed_a_index]
            stages.append(
                N13Event(
                    str(a.open_time_ms),
                    "INVALID",
                    "N13_ARMED_WINDOW_EXPIRED",
                    detail={
                        "a": a.json(),
                        "zone_lower": str(armed_lower),
                        "zone_upper": str(armed_upper),
                        "armed_expiry_time_ms": (
                            a.open_time_ms + armed_max_bars * INTERVAL_MS
                        ),
                    },
                )
            )
            armed = False
            continue
        if closed[i].high < armed_lower:
            a = closed[armed_a_index]
            stages.append(
                N13Event(
                    str(a.open_time_ms),
                    "INVALID",
                    "N13_VALUE_ZONE_SKIPPED",
                    detail={
                        "a": a.json(),
                        "zone_lower": str(armed_lower),
                        "zone_upper": str(armed_upper),
                    },
                )
            )
            armed = False
            i += 1
            continue
        if not (closed[i].low <= armed_upper and closed[i].high >= armed_lower):
            i += 1; continue
        t_index = i
        t = closed[t_index]
        a = closed[armed_a_index]
        terminal = None
        c_index = None
        terminal_index = None
        failure_reasons = []
        confirmation_checks: list[dict[str, Any]] = []
        end = min(t_index + confirmation_max_bars - 1, last_index)
        for j in range(t_index, end + 1):
            bar, bm = closed[j], metrics[j]
            if bar.close < armed_lower:
                terminal = "N13_VALUE_BAND_BROKEN"
                terminal_index = j
                confirmation_checks.append(
                    {
                        "candle": bar.json(),
                        "vwap": str(bm.vwap),
                        "outcome": terminal,
                    }
                )
                break
            if bar.high == bar.low:
                failure_reason = "N13_CONFIRMATION_FLAT_CANDLE"
            elif bar.close < bm.vwap:
                failure_reason = "N13_CONFIRMATION_CLOSE_BELOW_VWAP"
            elif bar.close <= bar.open:
                failure_reason = "N13_CONFIRMATION_NOT_BULLISH"
            elif j == 0 or bar.close <= closed[j - 1].close:
                failure_reason = "N13_CONFIRMATION_CLOSE_NOT_ADVANCING"
            elif (
                (bar.close - bar.low) / (bar.high - bar.low)
                < confirmation_close_location_min
            ):
                failure_reason = "N13_CONFIRMATION_CLOSE_LOCATION_TOO_LOW"
            elif (
                bar.quote_volume <= 0
                or bar.taker_buy_quote_volume / bar.quote_volume
                < confirmation_taker_buy_ratio_min
            ):
                failure_reason = "N13_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW"
            else:
                failure_reason = None
            if failure_reason is not None:
                failure_reasons.append(failure_reason)
                confirmation_checks.append(
                    {
                        "candle": bar.json(),
                        "vwap": str(bm.vwap),
                        "outcome": failure_reason,
                    }
                )
                continue
            c_index = j
            break
        if (
            c_index is None
            and terminal is None
            and last_index >= t_index + confirmation_max_bars - 1
        ):
            terminal = "N13_CONFIRMATION_NOT_FOUND"
        if c_index is None:
            if terminal:
                stages.append(
                    N13Event(
                        str(t.open_time_ms),
                        "INVALID",
                        terminal,
                        detail=_n13_t_stage_audit_detail(
                            symbol,
                            terminal,
                            a,
                            t,
                            armed_vwap,
                            armed_atr,
                            armed_lower,
                            armed_upper,
                            armed_max_bars,
                            vwap_lookback_bars,
                            atr_period,
                            upper_atr_fraction,
                            lower_atr_fraction,
                            confirmation_max_bars,
                            confirmation_close_location_min,
                            confirmation_taker_buy_ratio_min,
                            closed[:t_index + len(confirmation_checks)],
                            closed[armed_a_index + 1:t_index],
                            confirmation_checks,
                            failure_reasons,
                        ),
                    )
                )
                armed = False
                i = (
                    terminal_index + 1
                    if terminal_index is not None
                    else t_index + confirmation_max_bars
                )
            else:
                pending_touch = True
                break
            continue
        c = closed[c_index]
        cm = metrics[c_index]
        p = min(item.low for item in closed[t_index:c_index + 1])
        structure = N13Structure(
            symbol,
            a,
            t,
            c,
            None,
            p,
            cm.vwap,
            cm.atr,
            c.close,
            c.close + entry_extension_atr_max * cm.atr,
            return_24h,
            return_rank,
            positive_return_breadth,
            above_vwap_breadth,
            True,
            armed_lower,
            armed_upper,
            a.open_time_ms + armed_max_bars * INTERVAL_MS,
            tuple(failure_reasons),
            _sid(symbol, t, c),
        )
        if c_index < last_index:
            historical_structure = replace(
                structure, return_24h=None, return_rank=None,
                positive_return_breadth=None, above_vwap_breadth=None,
                snapshot_context_available=False,
            )
            historical.append(N13Event(str(t.open_time_ms), "MISSED", "HISTORICAL_N13_ENTRY_MISSED", historical_structure))
        else:
            current_candidates.append(structure)
        armed = False
        i = c_index + 1
    if current_candidates:
        structure = current_candidates[-1]
        entry = candles[-1]
        structure = replace(structure, entry=entry)
        elapsed = checked_at_ms - entry.open_time_ms
        if market_reason is not None:
            return result(market_reason, structure, consume=True, historical=historical, stages=stages, elapsed=elapsed)
        if elapsed < 0 or elapsed >= entry_window_seconds * 1000:
            reason, consume = "N13_ENTRY_WINDOW_EXPIRED", True
        elif entry.low < structure.p:
            reason, consume = "N13_ENTRY_LOW_BROKE_P", True
        elif entry.close < structure.entry_min_price:
            reason, consume = "N13_WAITING_ENTRY_PRICE", False
        elif entry.close > structure.entry_max_price:
            reason, consume = "N13_ENTRY_PRICE_TOO_EXTENDED", True
        else:
            reason, consume = "PASSED", True
        return result(reason, structure, reason == "PASSED", consume, historical, stages, elapsed)
    if historical:
        return result(historical[-1].reason, historical[-1].structure, historical=historical, stages=stages)
    if stages:
        return result(stages[-1].reason, stages=stages)
    return result("N13_VALUE_TOUCH_PENDING" if (armed or pending_touch) else "N13_NOT_ARMED")
