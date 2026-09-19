from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .exchange_symbol import canonical_exchange_symbol


INTERVAL_MS = 15 * 60 * 1000
N16_SCHEMA_VERSION = 1
N16_RULE_VERSION = "N16_V1"
N16_EVIDENCE_MAX_BYTES = 128 * 1024


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _decimal(value: Any) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise InvalidOperation("non-finite decimal")
    return parsed


def _canonical_symbol(value: Any) -> str:
    try:
        return canonical_exchange_symbol(value)
    except ValueError as exc:
        raise ValueError("N16 symbol is not canonical") from exc


def _decimal_text(value: Any, name: str) -> Decimal:
    if type(value) is not str or not value or len(value) > 128:
        raise ValueError("%s is not a canonical decimal" % name)
    parsed = Decimal(value)
    if not parsed.is_finite() or str(parsed) != value:
        raise ValueError("%s is not a canonical decimal" % name)
    return parsed


def _canonical_time(value: Any, name: str) -> int:
    if (
        type(value) is not int
        or value <= 0
        or value > 9_223_372_036_854_775_807
        or value % INTERVAL_MS != 0
    ):
        raise ValueError("%s is not a canonical 15m time" % name)
    return value


@dataclass(frozen=True)
class N16Candle:
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

    def to_jsonable(self, include_index: bool = True) -> Dict[str, Any]:
        result = {
            "open_time_ms": self.open_time_ms,
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "quote_volume": str(self.quote_volume),
            "taker_buy_quote_volume": str(self.taker_buy_quote_volume),
        }
        if include_index:
            result["index"] = self.index
        return result


_CANDLE_KEYS = frozenset(
    {
        "open_time_ms",
        "open",
        "high",
        "low",
        "close",
        "quote_volume",
        "taker_buy_quote_volume",
    }
)


def _validate_candle(candle: N16Candle) -> None:
    if (
        type(candle.index) is not int
        or candle.index < 0
        or candle.open_time_ms <= 0
        or candle.open_time_ms % INTERVAL_MS != 0
        or min(candle.open, candle.high, candle.low, candle.close) <= 0
        or candle.high <= candle.low
        or candle.high < max(candle.open, candle.close)
        or candle.low > min(candle.open, candle.close)
        or candle.quote_volume <= 0
        or candle.taker_buy_quote_volume < 0
        or candle.taker_buy_quote_volume > candle.quote_volume
    ):
        raise ValueError("N16 candle semantics are invalid")


def _n16_candle_monotonic_extension(
    earlier: N16Candle,
    later: N16Candle,
) -> bool:
    """Prove that two observations belong to one still-forming 15m candle."""
    return bool(
        earlier.open_time_ms == later.open_time_ms
        and earlier.open == later.open
        and later.high >= earlier.high
        and later.low <= earlier.low
        and later.quote_volume >= earlier.quote_volume
        and later.taker_buy_quote_volume >= earlier.taker_buy_quote_volume
        and later.taker_buy_quote_volume - earlier.taker_buy_quote_volume
        <= later.quote_volume - earlier.quote_volume
    )


def _qualified_observation_payload(
    entry: N16Candle,
    observed_at_ms: int,
    entry_window_ms: int,
) -> Dict[str, Any]:
    elapsed_ms = observed_at_ms - entry.open_time_ms
    if not 0 <= elapsed_ms < entry_window_ms:
        raise ValueError("N16 qualified observation time is invalid")
    return {
        "entry": entry.to_jsonable(False),
        "observed_at_ms": observed_at_ms,
        "elapsed_ms": elapsed_ms,
        "entry_deadline_ms": entry.open_time_ms + entry_window_ms,
    }


def _candle_from_json(value: Any, index: int) -> N16Candle:
    if type(value) is not dict or frozenset(value.keys()) != _CANDLE_KEYS:
        raise ValueError("N16 evidence candle shape is invalid")
    candle = N16Candle(
        index=index,
        open_time_ms=_canonical_time(value["open_time_ms"], "open_time_ms"),
        open=_decimal_text(value["open"], "open"),
        high=_decimal_text(value["high"], "high"),
        low=_decimal_text(value["low"], "low"),
        close=_decimal_text(value["close"], "close"),
        quote_volume=_decimal_text(value["quote_volume"], "quote_volume"),
        taker_buy_quote_volume=_decimal_text(
            value["taker_buy_quote_volume"], "taker_buy_quote_volume"
        ),
    )
    _validate_candle(candle)
    return candle


def parse_n16_klines(raw_klines: Sequence[Sequence[Any]]) -> List[N16Candle]:
    candles: List[N16Candle] = []
    for index, row in enumerate(raw_klines):
        if type(row) not in (list, tuple) or len(row) <= 10:
            raise ValueError("N16 kline requires indexes 0-10")
        open_time = _decimal(row[0])
        if open_time != open_time.to_integral_value() or open_time <= 0:
            raise ValueError("N16 kline open time is invalid")
        candle = N16Candle(
            index=index,
            open_time_ms=int(open_time),
            open=_decimal(row[1]),
            high=_decimal(row[2]),
            low=_decimal(row[3]),
            close=_decimal(row[4]),
            quote_volume=_decimal(row[7]),
            taker_buy_quote_volume=_decimal(row[10]),
        )
        _validate_candle(candle)
        candles.append(candle)
    return candles


def _continuous(candles: Sequence[N16Candle]) -> bool:
    return all(
        right.open_time_ms - left.open_time_ms == INTERVAL_MS
        for left, right in zip(candles, candles[1:])
    )


def _median(values: Iterable[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("N16 median input is empty")
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal("2")


def _ema_series(candles: Sequence[N16Candle], period: int) -> List[Optional[Decimal]]:
    if type(period) is not int or period < 1:
        raise ValueError("N16 EMA period is invalid")
    result: List[Optional[Decimal]] = [None] * len(candles)
    if len(candles) < period:
        return result
    seed = sum((item.close for item in candles[:period]), Decimal("0")) / Decimal(period)
    result[period - 1] = seed
    multiplier = Decimal("2") / Decimal(period + 1)
    previous = seed
    for index in range(period, len(candles)):
        previous = (candles[index].close - previous) * multiplier + previous
        result[index] = previous
    return result


def _atr_series(candles: Sequence[N16Candle], period: int) -> List[Optional[Decimal]]:
    if type(period) is not int or period < 1:
        raise ValueError("N16 ATR period is invalid")
    ranges: List[Decimal] = []
    for index, candle in enumerate(candles):
        if index == 0:
            ranges.append(candle.high - candle.low)
        else:
            previous_close = candles[index - 1].close
            ranges.append(
                max(
                    candle.high - candle.low,
                    abs(candle.high - previous_close),
                    abs(candle.low - previous_close),
                )
            )
    result: List[Optional[Decimal]] = [None] * len(candles)
    if len(ranges) < period:
        return result
    atr = sum(ranges[:period], Decimal("0")) / Decimal(period)
    result[period - 1] = atr
    for index in range(period, len(ranges)):
        atr = (atr * Decimal(period - 1) + ranges[index]) / Decimal(period)
        result[index] = atr
    return result


def _pivot_lows(candles: Sequence[N16Candle], left: int, right: int) -> List[int]:
    return [
        index
        for index in range(left, len(candles) - right)
        if candles[index].low
        < min(item.low for item in candles[index - left : index])
        and candles[index].low
        <= min(item.low for item in candles[index + 1 : index + right + 1])
    ]


def _pivot_highs(candles: Sequence[N16Candle], left: int, right: int) -> List[int]:
    return [
        index
        for index in range(left, len(candles) - right)
        if candles[index].high
        > max(item.high for item in candles[index - left : index])
        and candles[index].high
        >= max(item.high for item in candles[index + 1 : index + right + 1])
    ]


def _path_efficiency(candles: Sequence[N16Candle]) -> Decimal:
    if len(candles) < 2:
        return Decimal("0")
    path = sum(
        (abs(right.close - left.close) for left, right in zip(candles, candles[1:])),
        Decimal("0"),
    )
    return (
        abs(candles[-1].close - candles[0].close) / path
        if path > 0
        else Decimal("0")
    )


def _anchor_id(
    domain: str,
    symbol: str,
    anchors: Sequence[N16Candle],
) -> str:
    _canonical_symbol(symbol)
    raw = "|".join(
        [N16_RULE_VERSION, domain, symbol]
        + [str(item.open_time_ms) for item in anchors]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _trend_id(symbol: str, anchors: Sequence[N16Candle]) -> str:
    return _anchor_id("TREND", symbol, anchors)


def _episode_id(symbol: str, anchors: Sequence[N16Candle]) -> str:
    return _anchor_id("EPISODE", symbol, anchors)


def _structure_id(symbol: str, anchors: Sequence[N16Candle]) -> str:
    return _anchor_id("STRUCTURE", symbol, anchors)


@dataclass(frozen=True)
class N16Structure:
    symbol: str
    l1: N16Candle
    h1: N16Candle
    l2: N16Candle
    h2: N16Candle
    a: N16Candle
    c: Optional[N16Candle]
    entry: Optional[N16Candle]
    trend_id: str
    episode_id: str
    structure_id: Optional[str]
    atr_h2: Decimal
    atr_a: Decimal
    atr_c: Optional[Decimal]
    ema20_a: Decimal
    ema50_a: Decimal
    ema50_slope_reference: Decimal
    ema20_c: Optional[Decimal]
    ema50_c: Optional[Decimal]
    up_leg_efficiency: Decimal
    up_leg_volume_median: Decimal
    pullback_volume_median: Decimal
    pullback_volume_ratio: Decimal
    pullback_depth: Decimal
    p: Decimal
    c_close_location: Optional[Decimal]
    c_taker_buy_ratio: Optional[Decimal]
    c_volume_multiple: Optional[Decimal]
    entry_min_price: Optional[Decimal]
    entry_max_price: Optional[Decimal]

    def summary_json(self) -> Dict[str, Any]:
        return {
            "trend_id": self.trend_id,
            "episode_id": self.episode_id,
            "structure_id": self.structure_id,
            "l1": self.l1.to_jsonable(False),
            "h1": self.h1.to_jsonable(False),
            "l2": self.l2.to_jsonable(False),
            "h2": self.h2.to_jsonable(False),
            "a": self.a.to_jsonable(False),
            "c": self.c.to_jsonable(False) if self.c is not None else None,
            "atr_h2": str(self.atr_h2),
            "atr_a": str(self.atr_a),
            "atr_c": str(self.atr_c) if self.atr_c is not None else None,
            "ema20_a": str(self.ema20_a),
            "ema50_a": str(self.ema50_a),
            "ema50_slope_reference": str(self.ema50_slope_reference),
            "ema20_c": str(self.ema20_c) if self.ema20_c is not None else None,
            "ema50_c": str(self.ema50_c) if self.ema50_c is not None else None,
            "up_leg_efficiency": str(self.up_leg_efficiency),
            "up_leg_volume_median": str(self.up_leg_volume_median),
            "pullback_volume_median": str(self.pullback_volume_median),
            "pullback_volume_ratio": str(self.pullback_volume_ratio),
            "pullback_depth": str(self.pullback_depth),
            "p": str(self.p),
            "c_close_location": (
                str(self.c_close_location)
                if self.c_close_location is not None
                else None
            ),
            "c_taker_buy_ratio": (
                str(self.c_taker_buy_ratio)
                if self.c_taker_buy_ratio is not None
                else None
            ),
            "c_volume_multiple": (
                str(self.c_volume_multiple)
                if self.c_volume_multiple is not None
                else None
            ),
            "entry_min_price": (
                str(self.entry_min_price)
                if self.entry_min_price is not None
                else None
            ),
            "entry_max_price": (
                str(self.entry_max_price)
                if self.entry_max_price is not None
                else None
            ),
        }


@dataclass(frozen=True)
class N16StateRecord:
    strategy_id: str
    symbol: str
    episode_id: str
    structure_id: Optional[str]
    stage: str
    reason: str
    quote_volume_rank: int
    evidence: Dict[str, Any]

    @property
    def evidence_json(self) -> str:
        return _canonical_json(self.evidence)

    @property
    def evidence_sha256(self) -> str:
        value = self.evidence.get("canonical_sha256")
        return value if type(value) is str else ""

    def with_evidence(self, evidence: Dict[str, Any]) -> "N16StateRecord":
        return replace(self, evidence=evidence)


@dataclass(frozen=True)
class N16DecodedState:
    strategy_id: str
    symbol: str
    episode_id: str
    structure_id: Optional[str]
    stage: str
    reason: str
    quote_volume_rank: int
    config: Dict[str, Any]
    seed_current_open_time_ms: int
    metric_source: Tuple[N16Candle, ...]
    terminal_entry: Optional[N16Candle]
    observed_at_ms: Optional[int]
    qualified_observation: Optional[Dict[str, Any]]
    summary: Dict[str, Any]
    canonical_sha256: str


@dataclass(frozen=True)
class N16AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    structure: Optional[N16Structure]
    current_price: Decimal
    current_open_time: Optional[str]
    checked_at: str
    elapsed_ms: Optional[int]
    entry_window_ms: int
    consume_current: bool
    detail: str
    quote_volume_rank: Optional[int]
    state_record: Optional[N16StateRecord]
    state_records: Tuple[N16StateRecord, ...] = ()

    @property
    def structure_id(self) -> Optional[str]:
        return self.structure.structure_id if self.structure is not None else None

    @property
    def current_bullish(self) -> bool:
        return bool(
            self.structure is not None
            and self.structure.entry is not None
            and self.structure.entry.close > self.structure.entry.open
        )

    def detail_json(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "schema_version": N16_SCHEMA_VERSION,
            "rule_version": N16_RULE_VERSION,
            "reason": self.reason,
            "structure_id": self.structure_id,
            "episode_id": (
                self.structure.episode_id if self.structure is not None else None
            ),
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
            "quote_volume_rank": self.quote_volume_rank,
            "state_stage": (
                self.state_record.stage if self.state_record is not None else None
            ),
            "evidence_sha256": (
                self.state_record.evidence_sha256
                if self.state_record is not None
                else None
            ),
            "detail": self.detail,
        }
        if self.structure is not None:
            payload["structure"] = self.structure.summary_json()
            if self.structure.entry is not None:
                payload["entry"] = self.structure.entry.to_jsonable(False)
        return payload


_STAGES = frozenset(
    {
        "TOUCH_LOCKED",
        "CONFIRMING",
        "CONFIRMED",
        "MISSED",
        "INVALID",
        "EXPIRED",
    }
)


def _config_payload(
    *,
    fixed_input_bars: int,
    pivot_left: int,
    pivot_right: int,
    closed_logic_bars: int,
    mature_min_bars: int,
    h2_progress_atr_min: Decimal,
    ema_fast_period: int,
    ema_slow_period: int,
    ema_slope_lookback_bars: int,
    up_leg_atr_min: Decimal,
    up_leg_efficiency_min: Decimal,
    support_min_bars: int,
    support_max_bars: int,
    support_touch_upper_atr: Decimal,
    support_close_lower_atr: Decimal,
    pullback_depth_min: Decimal,
    pullback_depth_max: Decimal,
    pullback_volume_ratio_max: Decimal,
    confirmation_max_bars: int,
    confirmation_close_location_min: Decimal,
    confirmation_taker_buy_ratio_min: Decimal,
    confirmation_volume_multiple_min: Decimal,
    entry_extension_atr_max: Decimal,
    entry_window_seconds: int,
    atr_period: int,
) -> Dict[str, Any]:
    return {
        "rule_version": N16_RULE_VERSION,
        "fixed_input_bars": fixed_input_bars,
        "pivot_left": pivot_left,
        "pivot_right": pivot_right,
        "closed_logic_bars": closed_logic_bars,
        "mature_min_bars": mature_min_bars,
        "h2_progress_atr_min": str(h2_progress_atr_min),
        "ema_fast_period": ema_fast_period,
        "ema_slow_period": ema_slow_period,
        "ema_slope_lookback_bars": ema_slope_lookback_bars,
        "up_leg_atr_min": str(up_leg_atr_min),
        "up_leg_efficiency_min": str(up_leg_efficiency_min),
        "support_min_bars": support_min_bars,
        "support_max_bars": support_max_bars,
        "support_touch_upper_atr": str(support_touch_upper_atr),
        "support_close_lower_atr": str(support_close_lower_atr),
        "pullback_depth_min": str(pullback_depth_min),
        "pullback_depth_max": str(pullback_depth_max),
        "pullback_volume_ratio_max": str(pullback_volume_ratio_max),
        "confirmation_max_bars": confirmation_max_bars,
        "confirmation_close_location_min": str(
            confirmation_close_location_min
        ),
        "confirmation_taker_buy_ratio_min": str(
            confirmation_taker_buy_ratio_min
        ),
        "confirmation_volume_multiple_min": str(
            confirmation_volume_multiple_min
        ),
        "entry_extension_atr_max": str(entry_extension_atr_max),
        "entry_window_seconds": entry_window_seconds,
        "atr_period": atr_period,
    }


_CONFIG_KEYS = frozenset(
    {
        "rule_version",
        "fixed_input_bars",
        "pivot_left",
        "pivot_right",
        "closed_logic_bars",
        "mature_min_bars",
        "h2_progress_atr_min",
        "ema_fast_period",
        "ema_slow_period",
        "ema_slope_lookback_bars",
        "up_leg_atr_min",
        "up_leg_efficiency_min",
        "support_min_bars",
        "support_max_bars",
        "support_touch_upper_atr",
        "support_close_lower_atr",
        "pullback_depth_min",
        "pullback_depth_max",
        "pullback_volume_ratio_max",
        "confirmation_max_bars",
        "confirmation_close_location_min",
        "confirmation_taker_buy_ratio_min",
        "confirmation_volume_multiple_min",
        "entry_extension_atr_max",
        "entry_window_seconds",
        "atr_period",
    }
)

_APPROVED_N16_CONFIG = _config_payload(
    fixed_input_bars=122,
    pivot_left=2,
    pivot_right=2,
    closed_logic_bars=96,
    mature_min_bars=16,
    h2_progress_atr_min=Decimal("0.25"),
    ema_fast_period=20,
    ema_slow_period=50,
    ema_slope_lookback_bars=8,
    up_leg_atr_min=Decimal("2"),
    up_leg_efficiency_min=Decimal("0.40"),
    support_min_bars=2,
    support_max_bars=8,
    support_touch_upper_atr=Decimal("0.25"),
    support_close_lower_atr=Decimal("0.20"),
    pullback_depth_min=Decimal("0.15"),
    pullback_depth_max=Decimal("0.45"),
    pullback_volume_ratio_max=Decimal("0.90"),
    confirmation_max_bars=3,
    confirmation_close_location_min=Decimal("0.65"),
    confirmation_taker_buy_ratio_min=Decimal("0.52"),
    confirmation_volume_multiple_min=Decimal("0.90"),
    entry_extension_atr_max=Decimal("0.50"),
    entry_window_seconds=120,
    atr_period=14,
)


def _confirmation_failure_reason(
    candles: Sequence[N16Candle],
    index: int,
    ema20: Sequence[Optional[Decimal]],
    ema50: Sequence[Optional[Decimal]],
    atr: Sequence[Optional[Decimal]],
    config: Dict[str, Any],
) -> Optional[str]:
    item = candles[index]
    previous = candles[index - 1]
    item_ema20 = ema20[index]
    item_atr = atr[index]
    if None in (item_ema20, item_atr):
        return "N16_CONFIRMATION_METRIC_INCOMPLETE"
    close_location = (item.close - item.low) / (item.high - item.low)
    taker_ratio = item.taker_buy_quote_volume / item.quote_volume
    prior_volume = _median(
        candle.quote_volume for candle in candles[max(0, index - 20) : index]
    )
    volume_multiple = item.quote_volume / prior_volume
    checks = (
        (item.close > item.open, "N16_CONFIRMATION_NOT_BULLISH"),
        (item.close > previous.high, "N16_CONFIRMATION_HIGH_NOT_BROKEN"),
        (item.close > item_ema20, "N16_CONFIRMATION_BELOW_EMA20"),
        (
            close_location
            >= _decimal_text(
                config["confirmation_close_location_min"],
                "confirmation_close_location_min",
            ),
            "N16_CONFIRMATION_CLOSE_LOCATION_TOO_LOW",
        ),
        (
            taker_ratio
            >= _decimal_text(
                config["confirmation_taker_buy_ratio_min"],
                "confirmation_taker_buy_ratio_min",
            ),
            "N16_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW",
        ),
        (
            volume_multiple
            >= _decimal_text(
                config["confirmation_volume_multiple_min"],
                "confirmation_volume_multiple_min",
            ),
            "N16_CONFIRMATION_VOLUME_TOO_LOW",
        ),
    )
    return next((reason for passed, reason in checks if not passed), None)


def _validate_n16_state_semantics(
    structure: N16Structure,
    stage: str,
    reason: str,
    config: Dict[str, Any],
    candles: Sequence[N16Candle],
    ema20: Sequence[Optional[Decimal]],
    ema50: Sequence[Optional[Decimal]],
    atr: Sequence[Optional[Decimal]],
    terminal_entry: Optional[N16Candle],
    observed_at_ms: Optional[int],
) -> None:
    by_time = {item.open_time_ms: item.index for item in candles}
    indexes = [
        by_time[item.open_time_ms]
        for item in (
            structure.l1,
            structure.h1,
            structure.l2,
            structure.h2,
            structure.a,
        )
    ]
    if structure.c is not None:
        indexes.append(by_time[structure.c.open_time_ms])
    if indexes != sorted(indexes) or len(set(indexes)) != len(indexes):
        raise ValueError("N16 anchor order is invalid")
    l1_index, h1_index, l2_index, h2_index, a_index = indexes[:5]
    fixed_closed = candles[: config["fixed_input_bars"] - 1]
    official_logic = fixed_closed[-config["closed_logic_bars"] :]
    official_skeleton = _latest_skeleton(
        official_logic,
        config["pivot_left"],
        config["pivot_right"],
    )
    if (
        official_skeleton is None
        or tuple(item.open_time_ms for item in official_skeleton)
        != tuple(
            item.open_time_ms
            for item in (
                structure.l1,
                structure.h1,
                structure.l2,
                structure.h2,
            )
        )
    ):
        raise ValueError("N16 evidence is not the official fixed-window structure")
    if (
        h2_index - l1_index < config["mature_min_bars"]
        or structure.l2.low <= structure.l1.low
        or atr[h2_index] is None
        or structure.h2.high - structure.h1.high
        < _decimal_text(config["h2_progress_atr_min"], "h2_progress_atr_min")
        * atr[h2_index]
        or structure.h2.high - structure.l2.low
        < _decimal_text(config["up_leg_atr_min"], "up_leg_atr_min")
        * atr[h2_index]
        or structure.up_leg_efficiency
        < _decimal_text(config["up_leg_efficiency_min"], "up_leg_efficiency_min")
    ):
        raise ValueError("N16 trend evidence does not satisfy the frozen rules")
    left = config["pivot_left"]
    right = config["pivot_right"]
    pivot_lows = set(_pivot_lows(candles, left, right))
    pivot_highs = set(_pivot_highs(candles, left, right))
    if (
        l1_index not in pivot_lows
        or l2_index not in pivot_lows
        or h1_index not in pivot_highs
        or h2_index not in pivot_highs
    ):
        raise ValueError("N16 pivot evidence is invalid")
    slope_index = a_index - config["ema_slope_lookback_bars"]
    ema_gate_reason: Optional[str] = None
    if (
        slope_index < 0
        or None in (ema20[a_index], ema50[a_index], ema50[slope_index], atr[a_index])
    ):
        raise ValueError("N16 EMA evidence is invalid")
    if ema20[a_index] <= ema50[a_index]:
        ema_gate_reason = "N16_TREND_EMA_ALIGNMENT_NOT_MET"
    elif ema50[a_index] <= ema50[slope_index]:
        ema_gate_reason = "N16_EMA50_SLOPE_NOT_RISING"
    touch_offset = a_index - h2_index
    if not config["support_min_bars"] <= touch_offset <= config["support_max_bars"]:
        raise ValueError("N16 support-touch offset is invalid")
    upper_atr = _decimal_text(
        config["support_touch_upper_atr"], "support_touch_upper_atr"
    )
    lower_atr = _decimal_text(
        config["support_close_lower_atr"], "support_close_lower_atr"
    )
    for index in range(
        h2_index + config["support_min_bars"], a_index
    ):
        if (
            candles[index].low <= ema20[index] + upper_atr * atr[index]
            and candles[index].close >= ema20[index] - lower_atr * atr[index]
        ):
            raise ValueError("N16 evidence skipped an earlier support touch")
    if (
        structure.a.low > ema20[a_index] + upper_atr * atr[a_index]
        or structure.a.close < ema20[a_index] - lower_atr * atr[a_index]
    ):
        raise ValueError("N16 frozen support touch is invalid")
    if ema_gate_reason is not None:
        if (
            stage != "INVALID"
            or reason != ema_gate_reason
            or structure.c is not None
            or terminal_entry is not None
            or observed_at_ms is not None
        ):
            raise ValueError("N16 invalid trend reason is inconsistent")
        return
    a_pullback = candles[h2_index + 1 : a_index + 1]
    a_pullback_depth = (
        structure.h2.high - min(item.low for item in a_pullback)
    ) / (structure.h2.high - structure.l2.low)
    a_pullback_volume_ratio = _median(
        item.quote_volume for item in a_pullback
    ) / _median(
        item.quote_volume for item in candles[l2_index : h2_index + 1]
    )
    pullback_gate_reason: Optional[str] = None
    if not (
        _decimal_text(config["pullback_depth_min"], "pullback_depth_min")
        <= a_pullback_depth
        <= _decimal_text(config["pullback_depth_max"], "pullback_depth_max")
    ):
        pullback_gate_reason = "N16_PULLBACK_DEPTH_OUT_OF_RANGE"
    elif a_pullback_volume_ratio > _decimal_text(
        config["pullback_volume_ratio_max"], "pullback_volume_ratio_max"
    ):
        pullback_gate_reason = "N16_PULLBACK_VOLUME_TOO_HIGH"
    if pullback_gate_reason is not None:
        if (
            stage != "INVALID"
            or reason != pullback_gate_reason
            or structure.c is not None
            or terminal_entry is not None
            or observed_at_ms is not None
        ):
            raise ValueError("N16 invalid pullback reason is inconsistent")
        return

    if structure.c is None:
        if stage == "INVALID":
            raise ValueError("N16 invalid state has no failed frozen rule")
        if terminal_entry is not None or observed_at_ms is not None:
            raise ValueError("N16 unconfirmed state has terminal observation")
        closed_after_a = len(candles) - 1 - a_index
        if stage == "TOUCH_LOCKED" and reason == "N16_TOUCH_LOCKED":
            if closed_after_a != 0:
                raise ValueError("N16 touch-locked source length is inconsistent")
            return
        if stage == "CONFIRMING" and reason == "N16_CONFIRMATION_PENDING":
            if not 1 <= closed_after_a < config["confirmation_max_bars"]:
                raise ValueError("N16 confirming source length is inconsistent")
            return
        if stage == "EXPIRED" and closed_after_a >= config["confirmation_max_bars"]:
            failures = [
                _confirmation_failure_reason(
                    candles, index, ema20, ema50, atr, config
                )
                for index in range(
                    a_index + 1,
                    a_index + config["confirmation_max_bars"] + 1,
                )
            ]
            expected_reason = failures[-1] or "N16_CONFIRMATION_NOT_FOUND"
            if reason != expected_reason or any(item is None for item in failures):
                raise ValueError("N16 confirmation expiry reason is inconsistent")
            return
        raise ValueError("N16 unconfirmed stage/reason is inconsistent")

    c_index = by_time[structure.c.open_time_ms]
    if not 1 <= c_index - a_index <= config["confirmation_max_bars"]:
        raise ValueError("N16 confirmation offset is invalid")
    for index in range(a_index + 1, c_index):
        if _confirmation_failure_reason(candles, index, ema20, ema50, atr, config) is None:
            raise ValueError("N16 evidence skipped an earlier valid confirmation")
    if _confirmation_failure_reason(candles, c_index, ema20, ema50, atr, config) is not None:
        raise ValueError("N16 frozen confirmation is invalid")
    if not (
        _decimal_text(config["pullback_depth_min"], "pullback_depth_min")
        <= structure.pullback_depth
        <= _decimal_text(config["pullback_depth_max"], "pullback_depth_max")
    ):
        if (
            stage != "INVALID"
            or reason != "N16_PULLBACK_DEPTH_OUT_OF_RANGE"
            or terminal_entry is not None
            or observed_at_ms is not None
        ):
            raise ValueError("N16 final pullback reason is inconsistent")
        return
    if stage == "INVALID":
        raise ValueError("N16 invalid state has no failed frozen rule")
    if stage == "CONFIRMED" and reason in {"PASSED", "N16_ENTRY_WAITING_PRICE"}:
        if (
            terminal_entry is not None
            or observed_at_ms is not None
            or candles[-1].open_time_ms != structure.c.open_time_ms
        ):
            raise ValueError("N16 confirmed state has terminal observation")
        return
    if terminal_entry is not None:
        if (
            terminal_entry.open_time_ms
            != structure.c.open_time_ms + INTERVAL_MS
            or observed_at_ms is None
            or observed_at_ms < terminal_entry.open_time_ms
        ):
            raise ValueError("N16 entry terminal observation is invalid")
        is_historical = reason.startswith("N16_HISTORICAL_")
        if is_historical and observed_at_ms < terminal_entry.open_time_ms + INTERVAL_MS:
            raise ValueError("N16 historical entry was not closed when observed")
        if terminal_entry.low < structure.p:
            expected_reason = (
                "N16_HISTORICAL_ENTRY_LOW_BROKE_P"
                if is_historical
                else "N16_ENTRY_LOW_BROKE_P"
            )
            expected_stage = "MISSED"
        elif not is_historical and observed_at_ms >= (
            terminal_entry.open_time_ms
            + config["entry_window_seconds"] * 1000
        ):
            expected_reason = "N16_ENTRY_WINDOW_EXPIRED"
            expected_stage = "EXPIRED"
        elif terminal_entry.close > structure.entry_max_price:
            expected_reason = (
                "N16_HISTORICAL_ENTRY_PRICE_TOO_EXTENDED"
                if is_historical
                else "N16_ENTRY_PRICE_TOO_EXTENDED"
            )
            expected_stage = "MISSED"
        elif is_historical:
            expected_reason = (
                "N16_HISTORICAL_ENTRY_PRICE_BELOW_CONFIRMATION"
                if terminal_entry.close < structure.entry_min_price
                else "N16_HISTORICAL_ENTRY_MISSED"
            )
            expected_stage = "MISSED"
        else:
            raise ValueError("N16 current entry terminal reason is invalid")
        if reason != expected_reason or stage != expected_stage:
            raise ValueError("N16 entry terminal reason is inconsistent")
        return
    raise ValueError("N16 confirmed stage/reason is inconsistent")


def _build_evidence(
    *,
    symbol: str,
    structure: N16Structure,
    stage: str,
    reason: str,
    quote_volume_rank: int,
    config: Dict[str, Any],
    seed_current_open_time_ms: int,
    metric_source: Sequence[N16Candle],
    observed_at_ms: Optional[int],
    qualified_observation: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    terminal_entry = (
        structure.entry.to_jsonable(False)
        if stage in {"MISSED", "EXPIRED"} and structure.entry is not None
        else None
    )
    unsigned = {
        "schema_version": N16_SCHEMA_VERSION,
        "strategy_id": "N16",
        "symbol": symbol,
        "episode_id": structure.episode_id,
        "structure_id": structure.structure_id,
        "stage": stage,
        "reason": reason,
        "quote_volume_rank": quote_volume_rank,
        "config": config,
        "seed_current_open_time_ms": seed_current_open_time_ms,
        "metric_source": [item.to_jsonable(False) for item in metric_source],
        "terminal_entry": terminal_entry,
        "observed_at_ms": observed_at_ms,
        "qualified_observation": qualified_observation,
        "summary": structure.summary_json(),
    }
    result = dict(unsigned)
    result["canonical_sha256"] = _sha256_json(unsigned)
    encoded = _canonical_json(result).encode("utf-8")
    if len(encoded) >= N16_EVIDENCE_MAX_BYTES:
        raise ValueError("N16 evidence exceeds 128 KiB")
    return result


def _state_record(
    symbol: str,
    structure: N16Structure,
    stage: str,
    reason: str,
    quote_volume_rank: int,
    config: Dict[str, Any],
    seed_current_open_time_ms: int,
    metric_source: Sequence[N16Candle],
    observed_at_ms: Optional[int] = None,
    qualified_observation: Optional[Dict[str, Any]] = None,
) -> N16StateRecord:
    evidence = _build_evidence(
        symbol=symbol,
        structure=structure,
        stage=stage,
        reason=reason,
        quote_volume_rank=quote_volume_rank,
        config=config,
        seed_current_open_time_ms=seed_current_open_time_ms,
        metric_source=metric_source,
        observed_at_ms=observed_at_ms,
        qualified_observation=qualified_observation,
    )
    return N16StateRecord(
        strategy_id="N16",
        symbol=symbol,
        episode_id=structure.episode_id,
        structure_id=structure.structure_id,
        stage=stage,
        reason=reason,
        quote_volume_rank=quote_volume_rank,
        evidence=evidence,
    )


def _strict_summary_structure(
    symbol: str,
    summary: Dict[str, Any],
    candles: Sequence[N16Candle],
) -> N16Structure:
    if type(summary) is not dict:
        raise ValueError("N16 summary is invalid")
    expected_keys = frozenset(N16Structure.__dataclass_fields__.keys()).difference(
        {"symbol", "entry"}
    )
    # summary has candle payloads in place of the structure's internal objects.
    if frozenset(summary.keys()) != expected_keys:
        raise ValueError("N16 summary shape is invalid")
    by_time = {item.open_time_ms: item for item in candles}

    def candle(name: str) -> Optional[N16Candle]:
        value = summary[name]
        if value is None:
            return None
        parsed = _candle_from_json(value, 0)
        actual = by_time.get(parsed.open_time_ms)
        if actual is None or actual.to_jsonable(False) != parsed.to_jsonable(False):
            raise ValueError("N16 summary candle is outside metric source")
        return actual

    l1 = candle("l1")
    h1 = candle("h1")
    l2 = candle("l2")
    h2 = candle("h2")
    a = candle("a")
    c = candle("c")
    if None in (l1, h1, l2, h2, a):
        raise ValueError("N16 summary anchors are incomplete")

    def optional_decimal(name: str) -> Optional[Decimal]:
        value = summary[name]
        return None if value is None else _decimal_text(value, name)

    return N16Structure(
        symbol=symbol,
        l1=l1,
        h1=h1,
        l2=l2,
        h2=h2,
        a=a,
        c=c,
        entry=None,
        trend_id=summary["trend_id"],
        episode_id=summary["episode_id"],
        structure_id=summary["structure_id"],
        atr_h2=_decimal_text(summary["atr_h2"], "atr_h2"),
        atr_a=_decimal_text(summary["atr_a"], "atr_a"),
        atr_c=optional_decimal("atr_c"),
        ema20_a=_decimal_text(summary["ema20_a"], "ema20_a"),
        ema50_a=_decimal_text(summary["ema50_a"], "ema50_a"),
        ema50_slope_reference=_decimal_text(
            summary["ema50_slope_reference"], "ema50_slope_reference"
        ),
        ema20_c=optional_decimal("ema20_c"),
        ema50_c=optional_decimal("ema50_c"),
        up_leg_efficiency=_decimal_text(
            summary["up_leg_efficiency"], "up_leg_efficiency"
        ),
        up_leg_volume_median=_decimal_text(
            summary["up_leg_volume_median"], "up_leg_volume_median"
        ),
        pullback_volume_median=_decimal_text(
            summary["pullback_volume_median"], "pullback_volume_median"
        ),
        pullback_volume_ratio=_decimal_text(
            summary["pullback_volume_ratio"], "pullback_volume_ratio"
        ),
        pullback_depth=_decimal_text(
            summary["pullback_depth"], "pullback_depth"
        ),
        p=_decimal_text(summary["p"], "p"),
        c_close_location=optional_decimal("c_close_location"),
        c_taker_buy_ratio=optional_decimal("c_taker_buy_ratio"),
        c_volume_multiple=optional_decimal("c_volume_multiple"),
        entry_min_price=optional_decimal("entry_min_price"),
        entry_max_price=optional_decimal("entry_max_price"),
    )


def decode_n16_state_envelope(
    value: Any,
    expected_strategy_id: str = "N16",
    expected_symbol: Optional[str] = None,
) -> N16DecodedState:
    expected_keys = frozenset(
        {
            "schema_version",
            "strategy_id",
            "symbol",
            "episode_id",
            "structure_id",
            "stage",
            "reason",
            "quote_volume_rank",
            "config",
            "seed_current_open_time_ms",
            "metric_source",
            "terminal_entry",
            "observed_at_ms",
            "qualified_observation",
            "summary",
            "canonical_sha256",
        }
    )
    if type(value) is not dict or frozenset(value.keys()) != expected_keys:
        raise ValueError("N16 evidence envelope shape is invalid")
    if (
        value["schema_version"] != N16_SCHEMA_VERSION
        or type(value["schema_version"]) is not int
        or value["strategy_id"] != expected_strategy_id
        or type(value["strategy_id"]) is not str
        or _canonical_symbol(value["symbol"]) != value["symbol"]
        or (expected_symbol is not None and value["symbol"] != expected_symbol)
        or type(value["episode_id"]) is not str
        or len(value["episode_id"]) != 24
        or any(character not in "0123456789abcdef" for character in value["episode_id"])
        or (
            value["structure_id"] is not None
            and (
                type(value["structure_id"]) is not str
                or len(value["structure_id"]) != 24
                or any(
                    character not in "0123456789abcdef"
                    for character in value["structure_id"]
                )
            )
        )
        or type(value["stage"]) is not str
        or value["stage"] not in _STAGES
        or type(value["reason"]) is not str
        or not value["reason"]
        or len(value["reason"]) > 128
        or type(value["quote_volume_rank"]) is not int
        or not 1 <= value["quote_volume_rank"] <= 100
        or type(value["config"]) is not dict
        or frozenset(value["config"].keys()) != _CONFIG_KEYS
        or _canonical_time(
            value["seed_current_open_time_ms"],
            "seed_current_open_time_ms",
        )
        != value["seed_current_open_time_ms"]
        or type(value["metric_source"]) is not list
        or not 121 <= len(value["metric_source"]) <= 125
        or (
            value["observed_at_ms"] is not None
            and (
                type(value["observed_at_ms"]) is not int
                or value["observed_at_ms"] <= 0
                or value["observed_at_ms"]
                > 9_223_372_036_854_775_807
            )
        )
        or type(value["canonical_sha256"]) is not str
        or len(value["canonical_sha256"]) != 64
        or any(
            character not in "0123456789abcdef"
            for character in value["canonical_sha256"]
        )
    ):
        raise ValueError("N16 evidence envelope identity is invalid")
    unsigned = dict(value)
    canonical_sha256 = unsigned.pop("canonical_sha256")
    if canonical_sha256 != _sha256_json(unsigned):
        raise ValueError("N16 evidence hash is invalid")
    if len(_canonical_json(value).encode("utf-8")) >= N16_EVIDENCE_MAX_BYTES:
        raise ValueError("N16 evidence exceeds 128 KiB")
    candles = tuple(
        _candle_from_json(item, index)
        for index, item in enumerate(value["metric_source"])
    )
    if not _continuous(candles):
        raise ValueError("N16 evidence candle sequence is invalid")
    if (
        candles[0].open_time_ms
        + (value["config"].get("fixed_input_bars", 0) - 1) * INTERVAL_MS
        != value["seed_current_open_time_ms"]
        or candles[120].open_time_ms
        >= value["seed_current_open_time_ms"]
    ):
        raise ValueError("N16 fixed input seed identity is invalid")
    summary_structure = _strict_summary_structure(
        value["symbol"], value["summary"], candles
    )
    anchors = [
        summary_structure.l1,
        summary_structure.h1,
        summary_structure.l2,
        summary_structure.h2,
    ]
    if summary_structure.trend_id != _trend_id(value["symbol"], anchors):
        raise ValueError("N16 trend identity is invalid")
    if summary_structure.episode_id != _episode_id(
        value["symbol"], anchors + [summary_structure.a]
    ):
        raise ValueError("N16 episode identity is invalid")
    if summary_structure.episode_id != value["episode_id"]:
        raise ValueError("N16 envelope episode identity is invalid")
    expected_structure_id = (
        _structure_id(
            value["symbol"], anchors + [summary_structure.a, summary_structure.c]
        )
        if summary_structure.c is not None
        else None
    )
    if (
        summary_structure.structure_id != expected_structure_id
        or value["structure_id"] != expected_structure_id
    ):
        raise ValueError("N16 structure identity is invalid")

    config = value["config"]
    if config["rule_version"] != N16_RULE_VERSION:
        raise ValueError("N16 evidence rule version is invalid")
    int_fields = {
        "pivot_left",
        "pivot_right",
        "fixed_input_bars",
        "closed_logic_bars",
        "mature_min_bars",
        "ema_fast_period",
        "ema_slow_period",
        "ema_slope_lookback_bars",
        "support_min_bars",
        "support_max_bars",
        "confirmation_max_bars",
        "entry_window_seconds",
        "atr_period",
    }
    if any(type(config[name]) is not int or config[name] <= 0 for name in int_fields):
        raise ValueError("N16 evidence config counts are invalid")
    for name in _CONFIG_KEYS.difference(int_fields | {"rule_version"}):
        _decimal_text(config[name], name)
    if config != _APPROVED_N16_CONFIG:
        raise ValueError("N16 evidence config is not the approved rule version")

    terminal_entry = (
        None
        if value["terminal_entry"] is None
        else _candle_from_json(value["terminal_entry"], 0)
    )
    if terminal_entry is not None:
        source_entry = next(
            (
                item
                for item in candles
                if item.open_time_ms == terminal_entry.open_time_ms
            ),
            None,
        )
        if (
            source_entry is None
            or source_entry.to_jsonable(False)
            != terminal_entry.to_jsonable(False)
        ):
            raise ValueError("N16 terminal entry is outside the fixed seed")
        terminal_entry = source_entry

    qualified_observation = value["qualified_observation"]
    if qualified_observation is not None:
        expected_observation_keys = frozenset(
            {"entry", "observed_at_ms", "elapsed_ms", "entry_deadline_ms"}
        )
        if (
            type(qualified_observation) is not dict
            or frozenset(qualified_observation.keys()) != expected_observation_keys
            or type(qualified_observation["observed_at_ms"]) is not int
            or type(qualified_observation["elapsed_ms"]) is not int
            or type(qualified_observation["entry_deadline_ms"]) is not int
        ):
            raise ValueError("N16 qualified observation shape is invalid")
        qualified_entry = _candle_from_json(
            qualified_observation["entry"], 0
        )
        qualified_observation = {
            "entry": qualified_entry.to_jsonable(False),
            "observed_at_ms": qualified_observation["observed_at_ms"],
            "elapsed_ms": qualified_observation["elapsed_ms"],
            "entry_deadline_ms": qualified_observation["entry_deadline_ms"],
        }
    else:
        qualified_entry = None

    # Recompute every persisted derived field from the immutable source.
    ema20 = _ema_series(candles, config["ema_fast_period"])
    ema50 = _ema_series(candles, config["ema_slow_period"])
    atr = _atr_series(candles, config["atr_period"])
    index_by_time = {item.open_time_ms: item.index for item in candles}
    h2_index = index_by_time[summary_structure.h2.open_time_ms]
    a_index = index_by_time[summary_structure.a.open_time_ms]
    c_index = (
        index_by_time[summary_structure.c.open_time_ms]
        if summary_structure.c is not None
        else None
    )
    if (
        None in (ema20[a_index], ema50[a_index], atr[h2_index], atr[a_index])
        or a_index - config["ema_slope_lookback_bars"] < 0
        or ema50[a_index - config["ema_slope_lookback_bars"]] is None
    ):
        raise ValueError("N16 evidence metrics are incomplete")
    pullback = candles[h2_index + 1 : a_index + 1]
    impulse_start = index_by_time[summary_structure.l2.open_time_ms]
    impulse = candles[impulse_start : h2_index + 1]
    p_end = c_index if c_index is not None else a_index
    p_value = min(item.low for item in candles[h2_index + 1 : p_end + 1])
    displacement = summary_structure.h2.high - summary_structure.l2.low
    expected = {
        "atr_h2": atr[h2_index],
        "atr_a": atr[a_index],
        "ema20_a": ema20[a_index],
        "ema50_a": ema50[a_index],
        "ema50_slope_reference": ema50[
            a_index - config["ema_slope_lookback_bars"]
        ],
        "up_leg_efficiency": _path_efficiency(impulse),
        "up_leg_volume_median": _median(
            item.quote_volume for item in impulse
        ),
        "pullback_volume_median": _median(
            item.quote_volume for item in pullback
        ),
        "p": p_value,
    }
    expected["pullback_volume_ratio"] = (
        expected["pullback_volume_median"] / expected["up_leg_volume_median"]
    )
    expected["pullback_depth"] = (
        (summary_structure.h2.high - p_value) / displacement
    )
    for name, expected_value in expected.items():
        if getattr(summary_structure, name) != expected_value:
            raise ValueError("N16 evidence derived field %s is invalid" % name)
    if summary_structure.c is not None:
        if c_index is None or None in (atr[c_index], ema20[c_index], ema50[c_index]):
            raise ValueError("N16 confirmation metrics are incomplete")
        c = summary_structure.c
        c_close_location = (c.close - c.low) / (c.high - c.low)
        c_taker_ratio = c.taker_buy_quote_volume / c.quote_volume
        prior_volume = _median(
            item.quote_volume for item in candles[max(0, c_index - 20) : c_index]
        )
        c_volume_multiple = c.quote_volume / prior_volume
        c_expected = {
            "atr_c": atr[c_index],
            "ema20_c": ema20[c_index],
            "ema50_c": ema50[c_index],
            "c_close_location": c_close_location,
            "c_taker_buy_ratio": c_taker_ratio,
            "c_volume_multiple": c_volume_multiple,
            "entry_min_price": c.close,
            "entry_max_price": c.close
            + _decimal_text(
                config["entry_extension_atr_max"], "entry_extension_atr_max"
            )
            * atr[c_index],
        }
        for name, expected_value in c_expected.items():
            if getattr(summary_structure, name) != expected_value:
                raise ValueError("N16 confirmation field %s is invalid" % name)
    elif any(
        getattr(summary_structure, name) is not None
        for name in (
            "atr_c",
            "ema20_c",
            "ema50_c",
            "c_close_location",
            "c_taker_buy_ratio",
            "c_volume_multiple",
            "entry_min_price",
            "entry_max_price",
        )
    ):
        raise ValueError("N16 unconfirmed evidence has confirmation fields")
    if qualified_observation is not None:
        if summary_structure.c is None or qualified_entry is None:
            raise ValueError("N16 qualified observation lacks confirmation")
        entry_open_time_ms = summary_structure.c.open_time_ms + INTERVAL_MS
        entry_min = _decimal_text(
            summary_structure.summary_json()["entry_min_price"],
            "entry_min_price",
        )
        entry_max = _decimal_text(
            summary_structure.summary_json()["entry_max_price"],
            "entry_max_price",
        )
        elapsed_ms = qualified_observation["elapsed_ms"]
        if (
            qualified_entry.open_time_ms != entry_open_time_ms
            or qualified_observation["observed_at_ms"]
            != entry_open_time_ms + elapsed_ms
            or qualified_observation["entry_deadline_ms"]
            != entry_open_time_ms + config["entry_window_seconds"] * 1000
            or not 0 <= elapsed_ms < config["entry_window_seconds"] * 1000
            or qualified_entry.low < summary_structure.p
            or not entry_min <= qualified_entry.close <= entry_max
        ):
            raise ValueError("N16 qualified observation is invalid")
        if terminal_entry is not None and not _n16_candle_monotonic_extension(
            qualified_entry, terminal_entry
        ):
            raise ValueError("N16 terminal entry conflicts with qualified observation")
    if value["stage"] == "CONFIRMED" and value["reason"] == "PASSED":
        if qualified_observation is None:
            raise ValueError("N16 PASSED evidence lacks qualified observation")
    elif summary_structure.c is None and qualified_observation is not None:
        raise ValueError("N16 unconfirmed evidence has qualified observation")
    _validate_n16_state_semantics(
        summary_structure,
        value["stage"],
        value["reason"],
        config,
        candles,
        ema20,
        ema50,
        atr,
        terminal_entry,
        value["observed_at_ms"],
    )
    return N16DecodedState(
        strategy_id=value["strategy_id"],
        symbol=value["symbol"],
        episode_id=value["episode_id"],
        structure_id=value["structure_id"],
        stage=value["stage"],
        reason=value["reason"],
        quote_volume_rank=value["quote_volume_rank"],
        config=dict(config),
        seed_current_open_time_ms=value["seed_current_open_time_ms"],
        metric_source=candles,
        terminal_entry=terminal_entry,
        observed_at_ms=value["observed_at_ms"],
        qualified_observation=qualified_observation,
        summary=dict(value["summary"]),
        canonical_sha256=canonical_sha256,
    )


def loads_n16_state_envelope(
    raw: str,
    expected_strategy_id: str = "N16",
    expected_symbol: Optional[str] = None,
) -> N16DecodedState:
    if type(raw) is not str or len(raw.encode("utf-8")) >= N16_EVIDENCE_MAX_BYTES:
        raise ValueError("N16 evidence JSON is invalid")

    def pairs(items: List[Tuple[str, Any]]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key, value in items:
            if type(key) is not str or key in result:
                raise ValueError("N16 evidence JSON has duplicate keys")
            result[key] = value
        return result

    parsed = json.loads(
        raw,
        object_pairs_hook=pairs,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError("N16 evidence JSON contains a non-finite value")
        ),
    )
    return decode_n16_state_envelope(
        parsed, expected_strategy_id, expected_symbol
    )


def validate_n16_passed_signal_detail(
    detail: Any,
    *,
    expected_symbol: str,
    expected_structure_id: str,
    expected_evidence_json: str,
) -> None:
    """Bind a PASSED ordinary signal to frozen C evidence and its live E."""
    _canonical_symbol(expected_symbol)
    decoded = loads_n16_state_envelope(
        expected_evidence_json, "N16", expected_symbol
    )
    required_keys = {
        "schema_version",
        "rule_version",
        "reason",
        "structure_id",
        "episode_id",
        "current_price",
        "current_open_time",
        "checked_at",
        "elapsed_ms",
        "entry_window_ms",
        "entry_deadline_ms",
        "consume_current",
        "quote_volume_rank",
        "state_stage",
        "evidence_sha256",
        "detail",
        "structure",
        "entry",
    }
    allowed_keys = required_keys | {"candidate_universe", "quote_volume"}
    if type(detail) is not dict or frozenset(detail.keys()) != allowed_keys:
        raise ValueError("N16 PASSED signal detail shape is invalid")
    if (
        detail["schema_version"] != N16_SCHEMA_VERSION
        or type(detail["schema_version"]) is not int
        or detail["rule_version"] != N16_RULE_VERSION
        or type(detail["rule_version"]) is not str
        or detail["reason"] != "PASSED"
        or detail["detail"] != "PASSED"
        or detail["structure_id"] != expected_structure_id
        or detail["structure_id"] != decoded.structure_id
        or detail["episode_id"] != decoded.episode_id
        or detail["state_stage"] != "CONFIRMED"
        or detail["evidence_sha256"] != decoded.canonical_sha256
        or detail["consume_current"] is not False
        or detail["quote_volume_rank"] != decoded.quote_volume_rank
        or type(detail["quote_volume_rank"]) is not int
        or detail["candidate_universe"] not in {
            "quote_volume_top",
            "quote_volume_top_frozen_n16",
        }
        or type(detail["candidate_universe"]) is not str
        or (
            detail["quote_volume"] is not None
            and (
                type(detail["quote_volume"]) is not str
                or len(detail["quote_volume"]) > 128
                or _decimal_text(detail["quote_volume"], "quote_volume") < 0
            )
        )
        or detail["structure"] != decoded.summary
        or type(detail["elapsed_ms"]) is not int
        or not 0 <= detail["elapsed_ms"] < 120_000
        or detail["entry_window_ms"] != 120_000
        or type(detail["entry_window_ms"]) is not int
    ):
        raise ValueError("N16 PASSED signal identity is invalid")
    entry = _candle_from_json(detail["entry"], 0)
    c_value = decoded.summary["c"]
    if type(c_value) is not dict:
        raise ValueError("N16 PASSED signal lacks a frozen confirmation")
    c = _candle_from_json(c_value, 0)
    entry_min = _decimal_text(decoded.summary["entry_min_price"], "entry_min_price")
    entry_max = _decimal_text(decoded.summary["entry_max_price"], "entry_max_price")
    p_value = _decimal_text(decoded.summary["p"], "p")
    if decoded.qualified_observation is None:
        raise ValueError("N16 PASSED lifecycle lacks its first qualified observation")
    qualified_entry = _candle_from_json(
        decoded.qualified_observation["entry"], 0
    )
    if (
        entry.open_time_ms != c.open_time_ms + INTERVAL_MS
        or detail["current_open_time"] != str(entry.open_time_ms)
        or detail["entry_deadline_ms"] != entry.open_time_ms + 120_000
        or detail["current_price"] != str(entry.close)
        or entry.low < p_value
        or not entry_min <= entry.close <= entry_max
        or not _n16_candle_monotonic_extension(qualified_entry, entry)
        or detail["elapsed_ms"]
        < decoded.qualified_observation["elapsed_ms"]
    ):
        raise ValueError("N16 PASSED entry observation is invalid")
    if type(detail["checked_at"]) is not str:
        raise ValueError("N16 PASSED checked_at is invalid")
    checked = datetime.fromisoformat(detail["checked_at"].replace("Z", "+00:00"))
    if checked.tzinfo is None:
        raise ValueError("N16 PASSED checked_at must be timezone-aware")
    checked_ms = int(checked.timestamp() * 1000)
    if checked_ms != entry.open_time_ms + detail["elapsed_ms"]:
        raise ValueError("N16 PASSED elapsed time is inconsistent")


def _result(
    symbol: str,
    reason: str,
    current: Optional[N16Candle],
    checked_at: str,
    entry_window_ms: int,
    quote_volume_rank: Optional[int],
    *,
    structure: Optional[N16Structure] = None,
    state_record: Optional[N16StateRecord] = None,
    elapsed_ms: Optional[int] = None,
    consume_current: bool = False,
    passed: bool = False,
) -> N16AnalysisResult:
    return N16AnalysisResult(
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
        quote_volume_rank=quote_volume_rank,
        state_record=state_record,
        state_records=(state_record,) if state_record is not None else (),
    )


def _latest_skeleton(
    logic: Sequence[N16Candle], pivot_left: int, pivot_right: int
) -> Optional[Tuple[N16Candle, N16Candle, N16Candle, N16Candle]]:
    lows = _pivot_lows(logic, pivot_left, pivot_right)
    highs = _pivot_highs(logic, pivot_left, pivot_right)
    for h2_index in reversed(highs):
        l2_values = [index for index in lows if index < h2_index]
        if not l2_values:
            continue
        l2_index = l2_values[-1]
        h1_values = [index for index in highs if index < l2_index]
        if not h1_values:
            continue
        h1_index = h1_values[-1]
        l1_values = [index for index in lows if index < h1_index]
        if not l1_values:
            continue
        l1_index = l1_values[-1]
        return (
            logic[l1_index],
            logic[h1_index],
            logic[l2_index],
            logic[h2_index],
        )
    return None


def _merge_frozen_source(
    decoded: N16DecodedState, current_view: Sequence[N16Candle]
) -> List[N16Candle]:
    combined = [replace(item, index=index) for index, item in enumerate(decoded.metric_source)]
    by_time = {item.open_time_ms: item for item in combined}
    for item in current_view:
        existing = by_time.get(item.open_time_ms)
        if existing is not None:
            if existing.to_jsonable(False) != item.to_jsonable(False):
                raise ValueError("N16 frozen/current candle conflict")
            continue
        if item.open_time_ms <= combined[-1].open_time_ms:
            continue
        if item.open_time_ms != combined[-1].open_time_ms + INTERVAL_MS:
            raise ValueError("N16 frozen/current source gap")
        appended = replace(item, index=len(combined))
        combined.append(appended)
        by_time[appended.open_time_ms] = appended
    return combined


def _frozen_metric_source(
    candles: Sequence[N16Candle],
    seed_current_open_time_ms: int,
    through_open_time_ms: int,
) -> Tuple[N16Candle, ...]:
    """Return the immutable fixed seed plus only required closed evidence."""
    minimum_end = seed_current_open_time_ms - INTERVAL_MS
    target_end = max(minimum_end, through_open_time_ms)
    result = tuple(
        replace(item, index=index)
        for index, item in enumerate(
            item for item in candles if item.open_time_ms <= target_end
        )
    )
    if (
        not 121 <= len(result) <= 125
        or not result
        or result[-1].open_time_ms != target_end
    ):
        raise ValueError("N16 frozen metric source extent is invalid")
    return result


def analyze_n16_mature_trend_support(
    symbol: str,
    raw_klines: Sequence[Sequence[Any]],
    *,
    quote_volume_rank: int,
    frozen_evidence: Optional[Any] = None,
    fixed_input_bars: int = 122,
    closed_logic_bars: int = 96,
    pivot_left: int = 2,
    pivot_right: int = 2,
    mature_min_bars: int = 16,
    h2_progress_atr_min: Decimal = Decimal("0.25"),
    ema_fast_period: int = 20,
    ema_slow_period: int = 50,
    ema_slope_lookback_bars: int = 8,
    atr_period: int = 14,
    up_leg_atr_min: Decimal = Decimal("2"),
    up_leg_efficiency_min: Decimal = Decimal("0.40"),
    support_min_bars: int = 2,
    support_max_bars: int = 8,
    support_touch_upper_atr: Decimal = Decimal("0.25"),
    support_close_lower_atr: Decimal = Decimal("0.20"),
    pullback_depth_min: Decimal = Decimal("0.15"),
    pullback_depth_max: Decimal = Decimal("0.45"),
    pullback_volume_ratio_max: Decimal = Decimal("0.90"),
    confirmation_max_bars: int = 3,
    confirmation_close_location_min: Decimal = Decimal("0.65"),
    confirmation_taker_buy_ratio_min: Decimal = Decimal("0.52"),
    confirmation_volume_multiple_min: Decimal = Decimal("0.90"),
    entry_extension_atr_max: Decimal = Decimal("0.50"),
    entry_window_seconds: int = 120,
    checked_at_ms: Optional[int] = None,
) -> N16AnalysisResult:
    _canonical_symbol(symbol)
    counts = (
        fixed_input_bars,
        closed_logic_bars,
        pivot_left,
        pivot_right,
        mature_min_bars,
        ema_fast_period,
        ema_slow_period,
        ema_slope_lookback_bars,
        atr_period,
        support_min_bars,
        support_max_bars,
        confirmation_max_bars,
        entry_window_seconds,
    )
    if (
        any(type(item) is not int or item < 1 for item in counts)
        or fixed_input_bars < closed_logic_bars + 1
        or support_min_bars > support_max_bars
        or type(quote_volume_rank) is not int
        or not 1 <= quote_volume_rank <= 100
    ):
        raise ValueError("N16 count/rank parameters are invalid")
    now_ms = (
        int(datetime.now(timezone.utc).timestamp() * 1000)
        if checked_at_ms is None
        else checked_at_ms
    )
    if type(now_ms) is not int:
        raise ValueError("N16 checked_at_ms is invalid")
    checked_at = datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat()
    entry_window_ms = entry_window_seconds * 1000
    if len(raw_klines) < fixed_input_bars:
        return _result(
            symbol,
            "N16_NOT_ENOUGH_HISTORY",
            None,
            checked_at,
            entry_window_ms,
            quote_volume_rank,
        )
    try:
        current_view = parse_n16_klines(raw_klines[-fixed_input_bars:])
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return _result(
            symbol,
            "N16_KLINE_DATA_INVALID",
            None,
            checked_at,
            entry_window_ms,
            quote_volume_rank,
        )
    current = current_view[-1]
    if not _continuous(current_view):
        return _result(
            symbol,
            "N16_KLINE_SEQUENCE_INVALID",
            current,
            checked_at,
            entry_window_ms,
            quote_volume_rank,
        )
    if not current.open_time_ms <= now_ms < current.open_time_ms + INTERVAL_MS:
        return _result(
            symbol,
            "N16_KLINE_AXIS_NOT_READY",
            current,
            checked_at,
            entry_window_ms,
            quote_volume_rank,
        )
    config = _config_payload(
        fixed_input_bars=fixed_input_bars,
        pivot_left=pivot_left,
        pivot_right=pivot_right,
        closed_logic_bars=closed_logic_bars,
        mature_min_bars=mature_min_bars,
        h2_progress_atr_min=h2_progress_atr_min,
        ema_fast_period=ema_fast_period,
        ema_slow_period=ema_slow_period,
        ema_slope_lookback_bars=ema_slope_lookback_bars,
        up_leg_atr_min=up_leg_atr_min,
        up_leg_efficiency_min=up_leg_efficiency_min,
        support_min_bars=support_min_bars,
        support_max_bars=support_max_bars,
        support_touch_upper_atr=support_touch_upper_atr,
        support_close_lower_atr=support_close_lower_atr,
        pullback_depth_min=pullback_depth_min,
        pullback_depth_max=pullback_depth_max,
        pullback_volume_ratio_max=pullback_volume_ratio_max,
        confirmation_max_bars=confirmation_max_bars,
        confirmation_close_location_min=confirmation_close_location_min,
        confirmation_taker_buy_ratio_min=confirmation_taker_buy_ratio_min,
        confirmation_volume_multiple_min=confirmation_volume_multiple_min,
        entry_extension_atr_max=entry_extension_atr_max,
        entry_window_seconds=entry_window_seconds,
        atr_period=atr_period,
    )
    if config != _APPROVED_N16_CONFIG:
        return _result(
            symbol,
            "N16_DEFINITION_INVALID",
            current,
            checked_at,
            entry_window_ms,
            quote_volume_rank,
        )
    frozen_decoded: Optional[N16DecodedState] = None
    qualified_observation: Optional[Dict[str, Any]] = None
    seed_current_open_time_ms = current.open_time_ms
    if frozen_evidence is not None:
        try:
            if type(frozen_evidence) is str:
                frozen_decoded = loads_n16_state_envelope(
                    frozen_evidence, "N16", symbol
                )
            else:
                frozen_decoded = decode_n16_state_envelope(
                    frozen_evidence, "N16", symbol
                )
            if frozen_decoded.config != config:
                raise ValueError("N16 frozen config mismatch")
            qualified_observation = frozen_decoded.qualified_observation
            if frozen_decoded.stage in {"MISSED", "INVALID", "EXPIRED"}:
                terminal_structure = _strict_summary_structure(
                    symbol,
                    frozen_decoded.summary,
                    frozen_decoded.metric_source,
                )
                terminal_structure = replace(
                    terminal_structure,
                    entry=frozen_decoded.terminal_entry,
                )
                terminal_record = _state_record(
                    symbol,
                    terminal_structure,
                    frozen_decoded.stage,
                    frozen_decoded.reason,
                    frozen_decoded.quote_volume_rank,
                    frozen_decoded.config,
                    frozen_decoded.seed_current_open_time_ms,
                    frozen_decoded.metric_source,
                    observed_at_ms=frozen_decoded.observed_at_ms,
                    qualified_observation=qualified_observation,
                )
                if (
                    terminal_record.evidence_sha256
                    != frozen_decoded.canonical_sha256
                ):
                    raise ValueError("N16 terminal evidence reconstruction failed")
                return _result(
                    symbol,
                    frozen_decoded.reason,
                    current,
                    checked_at,
                    entry_window_ms,
                    frozen_decoded.quote_volume_rank,
                    structure=terminal_structure,
                    state_record=terminal_record,
                    elapsed_ms=(
                        frozen_decoded.observed_at_ms
                        - frozen_decoded.terminal_entry.open_time_ms
                        if frozen_decoded.observed_at_ms is not None
                        and frozen_decoded.terminal_entry is not None
                        else None
                    ),
                    consume_current=True,
                )
            candles = _merge_frozen_source(frozen_decoded, current_view)
            seed_current_open_time_ms = frozen_decoded.seed_current_open_time_ms
        except (ArithmeticError, InvalidOperation, TypeError, ValueError):
            return _result(
                symbol,
                "N16_FROZEN_EVIDENCE_INVALID",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )
        summary = frozen_decoded.summary
        structure = _strict_summary_structure(symbol, summary, candles)
        by_time = {item.open_time_ms: item for item in candles}
        anchors = (
            structure.l1,
            structure.h1,
            structure.l2,
            structure.h2,
            structure.a,
        )
        # Remap the frozen anchors to their indexes in the extended source.
        structure = replace(
            structure,
            l1=by_time[structure.l1.open_time_ms],
            h1=by_time[structure.h1.open_time_ms],
            l2=by_time[structure.l2.open_time_ms],
            h2=by_time[structure.h2.open_time_ms],
            a=by_time[structure.a.open_time_ms],
            c=(
                by_time[structure.c.open_time_ms]
                if structure.c is not None
                else None
            ),
        )
    else:
        candles = current_view
        logic = candles[:-1][-closed_logic_bars:]
        skeleton = _latest_skeleton(logic, pivot_left, pivot_right)
        if skeleton is None:
            return _result(
                symbol,
                "N16_STRUCTURE_NOT_FOUND",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )
        l1, h1, l2, h2 = skeleton
        anchors = (l1, h1, l2, h2)
        structure = None

    ema20 = _ema_series(candles, ema_fast_period)
    ema50 = _ema_series(candles, ema_slow_period)
    atr = _atr_series(candles, atr_period)
    if frozen_decoded is None:
        l1, h1, l2, h2 = anchors
        if h2.index - l1.index < mature_min_bars:
            return _result(
                symbol,
                "N16_TREND_AGE_TOO_SHORT",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )
        if l2.low <= l1.low:
            return _result(
                symbol,
                "N16_HIGHER_LOW_NOT_CONFIRMED",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )
        atr_h2 = atr[h2.index]
        if atr_h2 is None or atr_h2 <= 0:
            return _result(
                symbol,
                "N16_METRIC_HISTORY_INCOMPLETE",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )
        if h2.high - h1.high < h2_progress_atr_min * atr_h2:
            return _result(
                symbol,
                "N16_HIGHER_HIGH_TOO_SMALL",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )
        impulse = candles[l2.index : h2.index + 1]
        displacement = h2.high - l2.low
        efficiency = _path_efficiency(impulse)
        if displacement < up_leg_atr_min * atr_h2:
            return _result(
                symbol,
                "N16_UP_LEG_ATR_TOO_SMALL",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )
        if efficiency < up_leg_efficiency_min:
            return _result(
                symbol,
                "N16_UP_LEG_EFFICIENCY_TOO_LOW",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )
        a: Optional[N16Candle] = None
        first_touch_offset: Optional[int] = None
        search_end = min(h2.index + support_max_bars, len(candles) - 2)
        for index in range(h2.index + support_min_bars, search_end + 1):
            item_atr = atr[index]
            item_ema20 = ema20[index]
            if item_atr is None or item_ema20 is None:
                continue
            if (
                candles[index].low
                <= item_ema20 + support_touch_upper_atr * item_atr
                and candles[index].close
                >= item_ema20 - support_close_lower_atr * item_atr
            ):
                a = candles[index]
                first_touch_offset = index - h2.index
                break
        if a is None:
            reason = (
                "N16_PULLBACK_WINDOW_EXPIRED"
                if len(candles) - 2 >= h2.index + support_max_bars
                else "N16_PULLBACK_PENDING"
            )
            return _result(
                symbol,
                reason,
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )
        impulse_volume = _median(item.quote_volume for item in impulse)
        pullback = candles[h2.index + 1 : a.index + 1]
        pullback_volume = _median(item.quote_volume for item in pullback)
        p_value = min(item.low for item in pullback)
        depth = (h2.high - p_value) / displacement
        slope_reference_index = a.index - ema_slope_lookback_bars
        atr_a = atr[a.index]
        ema20_a = ema20[a.index]
        ema50_a = ema50[a.index]
        ema50_slope_reference = (
            ema50[slope_reference_index]
            if slope_reference_index >= 0
            else None
        )
        if None in (atr_a, ema20_a, ema50_a, ema50_slope_reference):
            return _result(
                symbol,
                "N16_METRIC_HISTORY_INCOMPLETE",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )
        base_structure = N16Structure(
            symbol=symbol,
            l1=l1,
            h1=h1,
            l2=l2,
            h2=h2,
            a=a,
            c=None,
            entry=None,
            trend_id=_trend_id(symbol, anchors),
            episode_id=_episode_id(symbol, anchors + (a,)),
            structure_id=None,
            atr_h2=atr_h2,
            atr_a=atr_a,
            atr_c=None,
            ema20_a=ema20_a,
            ema50_a=ema50_a,
            ema50_slope_reference=ema50_slope_reference,
            ema20_c=None,
            ema50_c=None,
            up_leg_efficiency=efficiency,
            up_leg_volume_median=impulse_volume,
            pullback_volume_median=pullback_volume,
            pullback_volume_ratio=pullback_volume / impulse_volume,
            pullback_depth=depth,
            p=p_value,
            c_close_location=None,
            c_taker_buy_ratio=None,
            c_volume_multiple=None,
            entry_min_price=None,
            entry_max_price=None,
        )
        invalid_reason: Optional[str] = None
        if ema20_a <= ema50_a:
            invalid_reason = "N16_TREND_EMA_ALIGNMENT_NOT_MET"
        elif ema50_a <= ema50_slope_reference:
            invalid_reason = "N16_EMA50_SLOPE_NOT_RISING"
        elif not pullback_depth_min <= depth <= pullback_depth_max:
            invalid_reason = "N16_PULLBACK_DEPTH_OUT_OF_RANGE"
        elif pullback_volume > impulse_volume * pullback_volume_ratio_max:
            invalid_reason = "N16_PULLBACK_VOLUME_TOO_HIGH"
        if invalid_reason is not None:
            record = _state_record(
                symbol,
                base_structure,
                "INVALID",
                invalid_reason,
                quote_volume_rank,
                config,
                seed_current_open_time_ms,
                _frozen_metric_source(
                    candles,
                    seed_current_open_time_ms,
                    seed_current_open_time_ms - INTERVAL_MS,
                ),
            )
            return _result(
                symbol,
                invalid_reason,
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
                structure=base_structure,
                state_record=record,
                consume_current=True,
            )
        structure = base_structure
    else:
        assert structure is not None
        a = structure.a
        h2 = structure.h2

    # Continue the frozen episode through the first fully-qualified C.
    c: Optional[N16Candle] = structure.c
    failure_reasons: List[str] = []
    if c is None:
        max_c_index = min(a.index + confirmation_max_bars, len(candles) - 2)
        for index in range(a.index + 1, max_c_index + 1):
            item = candles[index]
            previous = candles[index - 1]
            item_ema20 = ema20[index]
            item_ema50 = ema50[index]
            item_atr = atr[index]
            if None in (item_ema20, item_ema50, item_atr):
                failure_reasons.append("N16_CONFIRMATION_METRIC_INCOMPLETE")
                continue
            p_candidate = min(
                candle.low for candle in candles[h2.index + 1 : index + 1]
            )
            close_location = (item.close - item.low) / (item.high - item.low)
            taker_ratio = item.taker_buy_quote_volume / item.quote_volume
            prior_volume = _median(
                candle.quote_volume
                for candle in candles[max(0, index - 20) : index]
            )
            volume_multiple = item.quote_volume / prior_volume
            checks = (
                (item.close > item.open, "N16_CONFIRMATION_NOT_BULLISH"),
                (item.close > previous.high, "N16_CONFIRMATION_HIGH_NOT_BROKEN"),
                (item.close > item_ema20, "N16_CONFIRMATION_BELOW_EMA20"),
                (
                    close_location >= confirmation_close_location_min,
                    "N16_CONFIRMATION_CLOSE_LOCATION_TOO_LOW",
                ),
                (
                    taker_ratio >= confirmation_taker_buy_ratio_min,
                    "N16_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW",
                ),
                (
                    volume_multiple >= confirmation_volume_multiple_min,
                    "N16_CONFIRMATION_VOLUME_TOO_LOW",
                ),
            )
            failed = next((reason for passed, reason in checks if not passed), None)
            if failed is not None:
                failure_reasons.append(failed)
                continue
            c = item
            p_value = p_candidate
            structure = replace(
                structure,
                c=c,
                structure_id=_structure_id(
                    symbol,
                    (
                        structure.l1,
                        structure.h1,
                        structure.l2,
                        structure.h2,
                        structure.a,
                        c,
                    ),
                ),
                atr_c=item_atr,
                ema20_c=item_ema20,
                ema50_c=item_ema50,
                p=p_value,
                pullback_depth=(h2.high - p_value) / (h2.high - structure.l2.low),
                c_close_location=close_location,
                c_taker_buy_ratio=taker_ratio,
                c_volume_multiple=volume_multiple,
                entry_min_price=c.close,
                entry_max_price=c.close + entry_extension_atr_max * item_atr,
            )
            break

    if c is not None:
        final_depth = structure.pullback_depth
        if not pullback_depth_min <= final_depth <= pullback_depth_max:
            reason = "N16_PULLBACK_DEPTH_OUT_OF_RANGE"
            record = _state_record(
                symbol,
                structure,
                "INVALID",
                reason,
                frozen_decoded.quote_volume_rank
                if frozen_decoded
                else quote_volume_rank,
                config,
                seed_current_open_time_ms,
                _frozen_metric_source(
                    candles,
                    seed_current_open_time_ms,
                    c.open_time_ms,
                ),
                qualified_observation=qualified_observation,
            )
            return _result(
                symbol,
                reason,
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
                structure=structure,
                state_record=record,
                consume_current=True,
            )

    if c is None:
        closed_after_a = max(0, len(candles) - 2 - a.index)
        if closed_after_a >= confirmation_max_bars:
            reason = failure_reasons[-1] if failure_reasons else "N16_CONFIRMATION_NOT_FOUND"
            stage = "EXPIRED"
            consume_current = True
        else:
            reason = (
                "N16_CONFIRMATION_PENDING"
                if closed_after_a > 0
                else "N16_TOUCH_LOCKED"
            )
            stage = "CONFIRMING" if closed_after_a > 0 else "TOUCH_LOCKED"
            consume_current = False
        record = _state_record(
            symbol,
            structure,
            stage,
            reason,
            frozen_decoded.quote_volume_rank if frozen_decoded else quote_volume_rank,
            config,
            seed_current_open_time_ms,
            _frozen_metric_source(
                candles,
                seed_current_open_time_ms,
                (
                    a.open_time_ms + confirmation_max_bars * INTERVAL_MS
                    if stage == "EXPIRED"
                    else candles[-2].open_time_ms
                ),
            ),
            qualified_observation=qualified_observation,
        )
        return _result(
            symbol,
            reason,
            current,
            checked_at,
            entry_window_ms,
            quote_volume_rank,
            structure=structure,
            state_record=record,
            consume_current=consume_current,
        )

    assert structure.structure_id is not None
    entry_index = c.index + 1
    if entry_index < len(candles) - 1:
        entry = candles[entry_index]
        historical_structure = replace(structure, entry=entry)
        if entry.low < structure.p:
            reason = "N16_HISTORICAL_ENTRY_LOW_BROKE_P"
        elif entry.close < structure.entry_min_price:
            reason = "N16_HISTORICAL_ENTRY_PRICE_BELOW_CONFIRMATION"
        elif entry.close > structure.entry_max_price:
            reason = "N16_HISTORICAL_ENTRY_PRICE_TOO_EXTENDED"
        else:
            reason = "N16_HISTORICAL_ENTRY_MISSED"
        record = _state_record(
            symbol,
            historical_structure,
            "MISSED",
            reason,
            frozen_decoded.quote_volume_rank if frozen_decoded else quote_volume_rank,
            config,
            seed_current_open_time_ms,
            _frozen_metric_source(
                candles,
                seed_current_open_time_ms,
                entry.open_time_ms,
            ),
            observed_at_ms=now_ms,
            qualified_observation=qualified_observation,
        )
        return _result(
            symbol,
            reason,
            current,
            checked_at,
            entry_window_ms,
            quote_volume_rank,
            structure=historical_structure,
            state_record=record,
            consume_current=True,
        )
    if entry_index != len(candles) - 1:
        return _result(
            symbol,
            "N16_ENTRY_CANDLE_MISMATCH",
            current,
            checked_at,
            entry_window_ms,
            quote_volume_rank,
            structure=structure,
        )
    entry = candles[entry_index]
    structure = replace(structure, entry=entry)
    elapsed_ms = now_ms - entry.open_time_ms
    if qualified_observation is not None:
        qualified_entry = _candle_from_json(
            qualified_observation["entry"], 0
        )
        if not _n16_candle_monotonic_extension(qualified_entry, entry):
            return _result(
                symbol,
                "N16_FROZEN_EVIDENCE_INVALID",
                entry,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
                structure=structure,
            )
    if entry.low < structure.p:
        reason = "N16_ENTRY_LOW_BROKE_P"
        stage = "MISSED"
        consume_current = True
    elif elapsed_ms < 0 or elapsed_ms >= entry_window_ms:
        reason = "N16_ENTRY_WINDOW_EXPIRED"
        stage = "EXPIRED"
        consume_current = True
    elif entry.close > structure.entry_max_price:
        reason = "N16_ENTRY_PRICE_TOO_EXTENDED"
        stage = "MISSED"
        consume_current = True
    elif entry.close < structure.entry_min_price:
        reason = "N16_ENTRY_WAITING_PRICE"
        stage = "CONFIRMED"
        consume_current = False
    else:
        reason = "PASSED"
        stage = "CONFIRMED"
        consume_current = False
        if qualified_observation is None:
            qualified_observation = _qualified_observation_payload(
                entry, now_ms, entry_window_ms
            )
    record = _state_record(
        symbol,
        structure,
        stage,
        reason,
        frozen_decoded.quote_volume_rank if frozen_decoded else quote_volume_rank,
        config,
        seed_current_open_time_ms,
        _frozen_metric_source(
            candles,
            seed_current_open_time_ms,
            (
                entry.open_time_ms
                if stage in {"MISSED", "EXPIRED"}
                else c.open_time_ms
            ),
        ),
        observed_at_ms=(
            now_ms if stage in {"MISSED", "EXPIRED"} else None
        ),
        qualified_observation=qualified_observation,
    )
    return _result(
        symbol,
        reason,
        entry,
        checked_at,
        entry_window_ms,
        quote_volume_rank,
        structure=structure,
        state_record=record,
        elapsed_ms=elapsed_ms,
        consume_current=consume_current,
        passed=reason == "PASSED",
    )


# The long name is the frozen evaluator identifier; keep the shorter alias for tests.
analyze_n16_mature_trend_dynamic_support = analyze_n16_mature_trend_support
