from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Sequence

from .exchange_symbol import canonical_exchange_symbol


INTERVAL_MS = 15 * 60 * 1000
N17_SCHEMA_VERSION = 1
N17_RULE_VERSION = "N17_V1"
N17_EVIDENCE_MAX_BYTES = 128 * 1024


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _strict_json_loads(value: str) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in items:
            if key in result:
                raise ValueError("N17 JSON contains a duplicate key")
            result[key] = item
        return result

    return json.loads(
        value,
        object_pairs_hook=pairs,
        parse_constant=lambda constant: (_ for _ in ()).throw(
            ValueError("N17 JSON contains a non-finite number: %s" % constant)
        ),
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _decimal(value: Any) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise InvalidOperation("non-finite decimal")
    return parsed


def _symbol(value: Any) -> str:
    try:
        return canonical_exchange_symbol(value)
    except ValueError as exc:
        raise ValueError("N17 symbol is invalid") from exc


def _median(values: Iterable[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("N17 median input is empty")
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


@dataclass(frozen=True)
class N17Candle:
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

    @property
    def range(self) -> Decimal:
        return self.high - self.low

    @property
    def close_location(self) -> Decimal:
        return (self.close - self.low) / self.range

    @property
    def taker_buy_ratio(self) -> Decimal:
        return self.taker_buy_quote_volume / self.quote_volume

    @property
    def sell_quote(self) -> Decimal:
        return self.quote_volume - self.taker_buy_quote_volume

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "open_time_ms": self.open_time_ms,
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "quote_volume": str(self.quote_volume),
            "taker_buy_quote_volume": str(self.taker_buy_quote_volume),
        }


def _validate_candle(candle: N17Candle) -> None:
    if (
        type(candle.index) is not int
        or candle.index < 0
        or type(candle.open_time_ms) is not int
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
        raise ValueError("N17 candle semantics are invalid")


def parse_n17_klines(raw_klines: Sequence[Sequence[Any]]) -> list[N17Candle]:
    candles: list[N17Candle] = []
    for index, row in enumerate(raw_klines):
        if type(row) not in (list, tuple) or len(row) <= 10:
            raise ValueError("N17 kline requires indexes 0-10")
        open_time = _decimal(row[0])
        if open_time != open_time.to_integral_value() or open_time <= 0:
            raise ValueError("N17 kline open time is invalid")
        candle = N17Candle(
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


def _continuous(candles: Sequence[N17Candle]) -> bool:
    return all(
        right.open_time_ms - left.open_time_ms == INTERVAL_MS
        for left, right in zip(candles, candles[1:])
    )


def _n17_live_candle_can_finalize(
    observed_live: N17Candle,
    finalized: N17Candle,
) -> bool:
    """Prove one Binance cumulative 15m row only evolved live -> closed."""

    quote_delta = finalized.quote_volume - observed_live.quote_volume
    taker_delta = (
        finalized.taker_buy_quote_volume
        - observed_live.taker_buy_quote_volume
    )
    return (
        finalized.open_time_ms == observed_live.open_time_ms
        and finalized.open == observed_live.open
        and finalized.high >= observed_live.high
        and finalized.low <= observed_live.low
        and quote_delta >= 0
        and taker_delta >= 0
        and taker_delta <= quote_delta
    )


def _merge_n17_frozen_source(
    frozen_source: Sequence[dict[str, Any]],
    current_candles: Sequence[N17Candle],
) -> tuple[N17Candle, ...]:
    """Keep closed evidence immutable while finalizing one prior live tail.

    Every N17 state stores the 122-row analyzer input, whose final row is the
    then-current, still mutable 15m candle.  A later scan may replace exactly
    that prior tail once a newer candle proves it has closed.  All other
    overlapping rows are immutable and any gap or conflicting evolution is
    rejected.
    """

    frozen_rows = [
        [
            item.get("open_time_ms"), item.get("open"), item.get("high"),
            item.get("low"), item.get("close"), "0", "0",
            item.get("quote_volume"), "0", "0",
            item.get("taker_buy_quote_volume"),
        ]
        for item in frozen_source
        if type(item) is dict
    ]
    if len(frozen_rows) != len(frozen_source):
        raise ValueError("N17 frozen source row is invalid")
    frozen = parse_n17_klines(frozen_rows)
    if not frozen or not _continuous(frozen) or not current_candles:
        raise ValueError("N17 frozen source axis is invalid")
    current_by_time = {
        candle.open_time_ms: candle for candle in current_candles
    }
    frozen_last_time = frozen[-1].open_time_ms
    current_tail = current_by_time.get(frozen_last_time)
    if current_tail is None:
        raise ValueError("N17 prior live tail is outside the current source")

    for prior in frozen[:-1]:
        current = current_by_time.get(prior.open_time_ms)
        if current is not None and current.to_jsonable() != prior.to_jsonable():
            raise ValueError("N17 frozen closed source changed")

    additions = tuple(
        candle for candle in current_candles
        if candle.open_time_ms > frozen_last_time
    )
    merged = list(frozen)
    if additions:
        if not _n17_live_candle_can_finalize(frozen[-1], current_tail):
            raise ValueError("N17 prior live tail did not finalize monotonically")
        merged[-1] = current_tail
    elif not _n17_live_candle_can_finalize(frozen[-1], current_tail):
        raise ValueError("N17 current live tail conflicts with frozen evidence")
    if additions:
        if additions[0].open_time_ms != frozen_last_time + INTERVAL_MS:
            raise ValueError("N17 appended source has a gap")
        merged.extend(additions)
    if not _continuous(merged):
        raise ValueError("N17 merged source is discontinuous")
    return tuple(merged)


def _atr_series(candles: Sequence[N17Candle], period: int) -> list[Decimal | None]:
    true_ranges: list[Decimal] = []
    for index, candle in enumerate(candles):
        if index == 0:
            true_ranges.append(candle.range)
        else:
            prior_close = candles[index - 1].close
            true_ranges.append(
                max(
                    candle.range,
                    abs(candle.high - prior_close),
                    abs(candle.low - prior_close),
                )
            )
    result: list[Decimal | None] = [None] * len(candles)
    if len(candles) < period:
        return result
    value = sum(true_ranges[:period], Decimal("0")) / Decimal(period)
    result[period - 1] = value
    for index in range(period, len(candles)):
        value = (value * Decimal(period - 1) + true_ranges[index]) / Decimal(period)
        result[index] = value
    return result


def _pivot_indexes(
    candles: Sequence[N17Candle], left: int, right: int, kind: str
) -> list[int]:
    indexes: list[int] = []
    for index in range(left, len(candles) - right):
        candle = candles[index]
        before = candles[index - left : index]
        after = candles[index + 1 : index + right + 1]
        if kind == "HIGH":
            valid = candle.high > max(item.high for item in before) and candle.high >= max(
                item.high for item in after
            )
        else:
            valid = candle.low < min(item.low for item in before) and candle.low <= min(
                item.low for item in after
            )
        if valid:
            indexes.append(index)
    return indexes


@dataclass(frozen=True)
class N17Pivot:
    kind: str
    open_time_ms: int
    price: Decimal

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "open_time_ms": self.open_time_ms,
            "price": str(self.price),
        }


def _compress_turns(pivots: Sequence[N17Pivot]) -> tuple[N17Pivot, ...]:
    compressed: list[N17Pivot] = []
    for pivot in sorted(pivots, key=lambda item: (item.open_time_ms, item.kind)):
        if not compressed or compressed[-1].kind != pivot.kind:
            compressed.append(pivot)
            continue
        previous = compressed[-1]
        if (pivot.kind == "HIGH" and pivot.price > previous.price) or (
            pivot.kind == "LOW" and pivot.price < previous.price
        ):
            compressed[-1] = pivot
    return tuple(compressed)


@dataclass(frozen=True)
class N17Box:
    symbol: str
    start_time_ms: int
    end_time_ms: int
    bars: int
    upper: Decimal
    lower: Decimal
    height: Decimal
    tolerance: Decimal
    upper_boundary: Decimal
    lower_boundary: Decimal
    high_spread: Decimal
    low_spread: Decimal
    net_move: Decimal
    pivot_highs: tuple[N17Pivot, ...]
    pivot_lows: tuple[N17Pivot, ...]
    alternating_turns: tuple[N17Pivot, ...]

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "start_time_ms": self.start_time_ms,
            "end_time_ms": self.end_time_ms,
            "bars": self.bars,
            "upper": str(self.upper),
            "lower": str(self.lower),
            "height": str(self.height),
            "tolerance": str(self.tolerance),
            "upper_boundary": str(self.upper_boundary),
            "lower_boundary": str(self.lower_boundary),
            "high_spread": str(self.high_spread),
            "low_spread": str(self.low_spread),
            "net_move": str(self.net_move),
            "pivot_highs": [item.to_jsonable() for item in self.pivot_highs],
            "pivot_lows": [item.to_jsonable() for item in self.pivot_lows],
            "alternating_turns": [item.to_jsonable() for item in self.alternating_turns],
        }


def validate_n17_box(
    symbol: str,
    window: Sequence[N17Candle],
    *,
    pivot_left: int = 2,
    pivot_right: int = 2,
    tolerance_fraction: Decimal = Decimal("0.25"),
    net_move_fraction_max: Decimal = Decimal("0.50"),
) -> N17Box | None:
    if len(window) < 1:
        return None
    highs = _pivot_indexes(window, pivot_left, pivot_right, "HIGH")
    lows = _pivot_indexes(window, pivot_left, pivot_right, "LOW")
    if len(highs) < 2 or len(lows) < 2:
        return None
    high_pivots = tuple(
        N17Pivot("HIGH", window[index].open_time_ms, window[index].high)
        for index in highs
    )
    low_pivots = tuple(
        N17Pivot("LOW", window[index].open_time_ms, window[index].low)
        for index in lows
    )
    turns = _compress_turns((*high_pivots, *low_pivots))
    if len(turns) < 4:
        return None
    upper = _median(item.price for item in high_pivots)
    lower = _median(item.price for item in low_pivots)
    height = upper - lower
    if height <= 0:
        return None
    tolerance = height * tolerance_fraction
    high_spread = max(item.price for item in high_pivots) - min(
        item.price for item in high_pivots
    )
    low_spread = max(item.price for item in low_pivots) - min(
        item.price for item in low_pivots
    )
    if high_spread > tolerance or low_spread > tolerance:
        return None
    upper_boundary = upper + tolerance
    lower_boundary = lower - tolerance
    if any(item.high > upper_boundary or item.low < lower_boundary for item in window):
        return None
    net_move = abs(window[-1].close - window[0].open)
    if net_move > height * net_move_fraction_max:
        return None
    return N17Box(
        symbol=symbol,
        start_time_ms=window[0].open_time_ms,
        end_time_ms=window[-1].open_time_ms,
        bars=len(window),
        upper=upper,
        lower=lower,
        height=height,
        tolerance=tolerance,
        upper_boundary=upper_boundary,
        lower_boundary=lower_boundary,
        high_spread=high_spread,
        low_spread=low_spread,
        net_move=net_move,
        pivot_highs=high_pivots,
        pivot_lows=low_pivots,
        alternating_turns=turns,
    )


def _longest_box_suffix(
    symbol: str,
    closed: Sequence[N17Candle],
    touch_index: int,
    *,
    box_min_bars: int,
    box_max_bars: int,
    pivot_left: int,
    pivot_right: int,
    tolerance_fraction: Decimal,
    net_move_fraction_max: Decimal,
) -> N17Box | None:
    maximum = min(box_max_bars, touch_index)
    for bars in range(maximum, box_min_bars - 1, -1):
        box = validate_n17_box(
            symbol,
            closed[touch_index - bars : touch_index],
            pivot_left=pivot_left,
            pivot_right=pivot_right,
            tolerance_fraction=tolerance_fraction,
            net_move_fraction_max=net_move_fraction_max,
        )
        if box is not None:
            return box
    return None


def _structure_id(symbol: str, box: N17Box, touch: N17Candle) -> str:
    payload = "|".join(
        (
            N17_RULE_VERSION,
            symbol,
            str(box.start_time_ms),
            str(box.end_time_ms),
            str(box.upper),
            str(box.lower),
            str(touch.open_time_ms),
            str(touch.low),
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True)
class N17Structure:
    symbol: str
    box: N17Box
    touch: N17Candle
    absorption: N17Candle | None
    confirmation: N17Candle | None
    entry: N17Candle | None
    atr_touch: Decimal
    atr_confirmation: Decimal | None
    touch_volume_median20: Decimal
    touch_volume_multiple: Decimal
    absorption_range_ratio: Decimal | None
    absorption_sell_quote_ratio: Decimal | None
    confirmation_close_location: Decimal | None
    confirmation_taker_buy_ratio: Decimal | None
    confirmation_volume_multiple: Decimal | None
    entry_min: Decimal | None
    entry_max: Decimal | None
    structure_id: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "structure_id": self.structure_id,
            "box": self.box.to_jsonable(),
            "touch": self.touch.to_jsonable(),
            "absorption": self.absorption.to_jsonable() if self.absorption else None,
            "confirmation": (
                self.confirmation.to_jsonable() if self.confirmation else None
            ),
            "atr_touch": str(self.atr_touch),
            "atr_confirmation": (
                str(self.atr_confirmation) if self.atr_confirmation is not None else None
            ),
            "touch_volume_median20": str(self.touch_volume_median20),
            "touch_volume_multiple": str(self.touch_volume_multiple),
            "absorption_range_ratio": (
                str(self.absorption_range_ratio)
                if self.absorption_range_ratio is not None
                else None
            ),
            "absorption_sell_quote_ratio": (
                str(self.absorption_sell_quote_ratio)
                if self.absorption_sell_quote_ratio is not None
                else None
            ),
            "confirmation_close_location": (
                str(self.confirmation_close_location)
                if self.confirmation_close_location is not None
                else None
            ),
            "confirmation_taker_buy_ratio": (
                str(self.confirmation_taker_buy_ratio)
                if self.confirmation_taker_buy_ratio is not None
                else None
            ),
            "confirmation_volume_multiple": (
                str(self.confirmation_volume_multiple)
                if self.confirmation_volume_multiple is not None
                else None
            ),
            "entry_min": str(self.entry_min) if self.entry_min is not None else None,
            "entry_max": str(self.entry_max) if self.entry_max is not None else None,
        }


@dataclass(frozen=True)
class N17StateRecord:
    strategy_id: str
    symbol: str
    family_id: str
    structure_id: str
    stage: str
    reason: str
    quote_volume_rank: int
    box_start_time_ms: int
    box_end_time_ms: int
    reset_after_time_ms: int | None
    evidence: dict[str, Any]

    @property
    def evidence_json(self) -> str:
        return _canonical_json(self.evidence)

    @property
    def evidence_sha256(self) -> str:
        return _sha256_json(self.evidence)


@dataclass(frozen=True)
class N17AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    structure: N17Structure | None
    current_price: Decimal
    current_open_time: str | None
    checked_at: str
    elapsed_ms: int | None
    entry_window_ms: int
    consume_current: bool
    quote_volume_rank: int | None
    state_record: N17StateRecord | None
    state_records: tuple[N17StateRecord, ...] = ()

    @property
    def structure_id(self) -> str | None:
        return self.structure.structure_id if self.structure else None

    @property
    def current_bullish(self) -> bool:
        return bool(self.structure and self.structure.entry and self.structure.entry.close > self.structure.entry.open)

    def detail_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": N17_SCHEMA_VERSION,
            "rule_version": N17_RULE_VERSION,
            "reason": self.reason,
            "structure_id": self.structure_id,
            "current_price": str(self.current_price),
            "current_open_time": self.current_open_time,
            "checked_at": self.checked_at,
            "elapsed_ms": self.elapsed_ms,
            "entry_window_ms": self.entry_window_ms,
            "consume_current": self.consume_current,
            "quote_volume_rank": self.quote_volume_rank,
            "state_stage": self.state_record.stage if self.state_record else None,
            "evidence_sha256": self.state_record.evidence_sha256 if self.state_record else None,
        }
        if self.structure:
            payload["structure"] = self.structure.to_jsonable()
            if self.structure.entry:
                payload["entry"] = self.structure.entry.to_jsonable()
        return payload


def decode_n17_state_evidence(
    value: str | dict[str, Any],
    *,
    expected_symbol: str | None = None,
) -> N17StateRecord:
    if type(value) is str:
        parsed = _strict_json_loads(value)
    elif type(value) is dict:
        parsed = value
    else:
        raise ValueError("N17 frozen evidence type is invalid")
    if type(parsed) is not dict:
        raise ValueError("N17 frozen evidence shape is invalid")
    expected_keys = {
        "schema_version",
        "rule_version",
        "strategy_id",
        "symbol",
        "family_id",
        "structure_id",
        "stage",
        "reason",
        "quote_volume_rank",
        "reset_after_time_ms",
        "structure",
        "source",
        "canonical_sha256",
    }
    if set(parsed) != expected_keys:
        raise ValueError("N17 frozen evidence fields are invalid")
    unsigned = dict(parsed)
    claimed = unsigned.pop("canonical_sha256")
    symbol = parsed.get("symbol")
    structure = parsed.get("structure")
    box = structure.get("box") if type(structure) is dict else None
    touch = structure.get("touch") if type(structure) is dict else None
    source = parsed.get("source")
    if (
        type(parsed.get("schema_version")) is not int
        or parsed.get("schema_version") != N17_SCHEMA_VERSION
        or parsed.get("rule_version") != N17_RULE_VERSION
        or parsed.get("strategy_id") != "N17"
        or type(symbol) is not str
        or (expected_symbol is not None and symbol != expected_symbol)
        or type(parsed.get("family_id")) is not str
        or len(parsed["family_id"]) != 24
        or type(parsed.get("structure_id")) is not str
        or len(parsed["structure_id"]) != 24
        or parsed.get("stage")
        not in {
            "TOUCH_LOCKED",
            "CONFIRMING",
            "CONFIRMED",
            "CONSUMED",
            "MISSED",
            "INVALID",
            "EXPIRED",
        }
        or type(parsed.get("reason")) is not str
        or type(parsed.get("quote_volume_rank")) is not int
        or not 1 <= parsed["quote_volume_rank"] <= 100
        or (
            parsed.get("reset_after_time_ms") is not None
            and (
                type(parsed.get("reset_after_time_ms")) is not int
                or parsed["reset_after_time_ms"] <= 0
            )
        )
        or type(claimed) is not str
        or claimed != _sha256_json(unsigned)
        or type(structure) is not dict
        or structure.get("structure_id") != parsed["structure_id"]
        or type(box) is not dict
        or type(touch) is not dict
        or type(source) is not list
        or not 122 <= len(source) <= 126
    ):
        raise ValueError("N17 frozen evidence identity is invalid")
    source_rows = [
        [
            item.get("open_time_ms"),
            item.get("open"),
            item.get("high"),
            item.get("low"),
            item.get("close"),
            "0",
            "0",
            item.get("quote_volume"),
            "0",
            "0",
            item.get("taker_buy_quote_volume"),
        ]
        for item in source
        if type(item) is dict
    ]
    if not 122 <= len(source_rows) <= 126:
        raise ValueError("N17 frozen source is invalid")
    if any(
        type(item) is not dict or type(item.get("open_time_ms")) is not int
        for item in source
    ):
        raise ValueError("N17 frozen source time type is invalid")
    candles = parse_n17_klines(source_rows)
    if not _continuous(candles):
        raise ValueError("N17 frozen source is discontinuous")
    try:
        if any(
            type(item) is not int
            for item in (
                box["start_time_ms"], box["end_time_ms"], touch["open_time_ms"]
            )
        ):
            raise ValueError("N17 frozen structure time type is invalid")
        box_start = box["start_time_ms"]
        box_end = box["end_time_ms"]
        touch_time = touch["open_time_ms"]
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("N17 frozen structure time is invalid") from exc
    by_time = {candle.open_time_ms: index for index, candle in enumerate(candles)}
    try:
        box_start_index = by_time[box_start]
        box_end_index = by_time[box_end]
        touch_index = by_time[touch_time]
        if box_end_index + 1 != touch_index:
            raise ValueError("N17 frozen box is not adjacent to touch")
        verified_box = validate_n17_box(
            symbol, candles[box_start_index : box_end_index + 1]
        )
        if verified_box is None or verified_box.to_jsonable() != box:
            raise ValueError("N17 frozen box cannot be reproduced")
        atr = _atr_series(candles, 14)
        atr_touch = atr[touch_index]
        touch_candle = candles[touch_index]
        volume20 = _median(
            candle.quote_volume for candle in candles[touch_index - 20 : touch_index]
        )
        if (
            atr_touch is None
            or structure.get("atr_touch") != str(atr_touch)
            or structure.get("touch_volume_median20") != str(volume20)
            or structure.get("touch_volume_multiple")
            != str(touch_candle.quote_volume / volume20)
        ):
            raise ValueError("N17 frozen touch metrics conflict")
        absorption = structure.get("absorption")
        if absorption is not None:
            if type(absorption.get("open_time_ms")) is not int:
                raise ValueError("N17 frozen absorption time type is invalid")
            absorption_time = absorption["open_time_ms"]
            absorption_index = by_time[absorption_time]
            absorption_candle = candles[absorption_index]
            if absorption_index != touch_index + 1:
                raise ValueError("N17 frozen absorption is not adjacent")
            if (
                absorption_candle.to_jsonable() != absorption
                or structure.get("absorption_range_ratio")
                != str(absorption_candle.range / touch_candle.range)
                or structure.get("absorption_sell_quote_ratio")
                != str(absorption_candle.sell_quote / touch_candle.sell_quote)
            ):
                raise ValueError("N17 frozen absorption metrics conflict")
        confirmation = structure.get("confirmation")
        if confirmation is not None:
            if type(confirmation.get("open_time_ms")) is not int:
                raise ValueError("N17 frozen confirmation time type is invalid")
            confirmation_time = confirmation["open_time_ms"]
            confirmation_index = by_time[confirmation_time]
            confirmation_candle = candles[confirmation_index]
            atr_confirmation = atr[confirmation_index]
            prior_volume = _median(
                candle.quote_volume
                for candle in candles[confirmation_index - 20 : confirmation_index]
            )
            if (
                absorption is None
                or not touch_index + 2 <= confirmation_index <= touch_index + 3
                or atr_confirmation is None
                or confirmation_candle.to_jsonable() != confirmation
                or structure.get("atr_confirmation") != str(atr_confirmation)
                or structure.get("confirmation_close_location")
                != str(confirmation_candle.close_location)
                or structure.get("confirmation_taker_buy_ratio")
                != str(confirmation_candle.taker_buy_ratio)
                or structure.get("confirmation_volume_multiple")
                != str(confirmation_candle.quote_volume / prior_volume)
                or structure.get("entry_min") != str(confirmation_candle.close)
                or structure.get("entry_max")
                != str(confirmation_candle.close + Decimal("0.50") * atr_confirmation)
            ):
                raise ValueError("N17 frozen confirmation metrics conflict")
    except (ArithmeticError, KeyError, TypeError, ValueError) as exc:
        raise ValueError("N17 frozen structure cannot be reproduced") from exc
    return N17StateRecord(
        strategy_id="N17",
        symbol=symbol,
        family_id=parsed["family_id"],
        structure_id=parsed["structure_id"],
        stage=parsed["stage"],
        reason=parsed["reason"],
        quote_volume_rank=parsed["quote_volume_rank"],
        box_start_time_ms=box_start,
        box_end_time_ms=box_end,
        reset_after_time_ms=parsed["reset_after_time_ms"],
        evidence=parsed,
    )


def repair_n17_legacy_live_tail_evidence(
    value: str | dict[str, Any],
    *,
    expected_symbol: str | None = None,
) -> N17StateRecord:
    """Canonically repair only the old N17 live-tail persistence defect.

    The legacy writer could append a newly closed absorption/confirmation to
    the frozen structure while retaining that candle's earlier live snapshot
    in ``source``.  A repair is accepted only when exactly one structural row
    proves a monotonic cumulative live->closed transition and the resulting
    evidence passes the unchanged strict decoder.  Box, touch, identity, hash,
    metric, multi-row, or ambiguous conflicts remain fail-closed.
    """

    try:
        return decode_n17_state_evidence(
            value, expected_symbol=expected_symbol
        )
    except ValueError as exc:
        original_error = exc
    if type(value) is str:
        parsed = _strict_json_loads(value)
    elif type(value) is dict:
        parsed = _strict_json_loads(_canonical_json(value))
    else:
        raise ValueError("N17 legacy evidence type is invalid") from original_error
    if type(parsed) is not dict:
        raise ValueError("N17 legacy evidence shape is invalid") from original_error
    unsigned = dict(parsed)
    claimed = unsigned.pop("canonical_sha256", None)
    if type(claimed) is not str or claimed != _sha256_json(unsigned):
        raise ValueError("N17 legacy evidence hash is invalid") from original_error
    structure = parsed.get("structure")
    source = parsed.get("source")
    if type(structure) is not dict or type(source) is not list:
        raise ValueError("N17 legacy evidence body is invalid") from original_error

    successful: dict[str, N17StateRecord] = {}
    for field in ("absorption", "confirmation"):
        structural_row = structure.get(field)
        if type(structural_row) is not dict:
            continue
        open_time = structural_row.get("open_time_ms")
        matches = [
            index for index, row in enumerate(source)
            if type(row) is dict and row.get("open_time_ms") == open_time
        ]
        if len(matches) != 1 or matches[0] >= len(source) - 1:
            continue
        index = matches[0]
        if source[index] == structural_row:
            continue
        try:
            old_row = source[index]
            old_candle = parse_n17_klines(
                [[
                    old_row.get("open_time_ms"), old_row.get("open"),
                    old_row.get("high"), old_row.get("low"),
                    old_row.get("close"), "0", "0",
                    old_row.get("quote_volume"), "0", "0",
                    old_row.get("taker_buy_quote_volume"),
                ]]
            )[0]
            new_candle = parse_n17_klines(
                [[
                    structural_row.get("open_time_ms"),
                    structural_row.get("open"), structural_row.get("high"),
                    structural_row.get("low"), structural_row.get("close"),
                    "0", "0", structural_row.get("quote_volume"), "0", "0",
                    structural_row.get("taker_buy_quote_volume"),
                ]]
            )[0]
            if not _n17_live_candle_can_finalize(old_candle, new_candle):
                continue
            candidate = _strict_json_loads(_canonical_json(parsed))
            candidate["source"][index] = dict(structural_row)
            candidate_unsigned = dict(candidate)
            candidate_unsigned.pop("canonical_sha256", None)
            candidate["canonical_sha256"] = _sha256_json(candidate_unsigned)
            repaired = decode_n17_state_evidence(
                candidate, expected_symbol=expected_symbol
            )
            successful[repaired.evidence_sha256] = repaired
        except (ArithmeticError, KeyError, TypeError, ValueError):
            continue
    if len(successful) != 1:
        raise ValueError(
            "N17 legacy live-tail evidence is not uniquely repairable"
        ) from original_error
    return next(iter(successful.values()))


def _frozen_structure_matches(
    frozen_record: N17StateRecord,
    structure: N17Structure,
) -> bool:
    frozen = frozen_record.evidence.get("structure")
    if type(frozen) is not dict:
        return False
    current = structure.to_jsonable()
    keys = {
        "structure_id",
        "box",
        "touch",
        "atr_touch",
        "touch_volume_median20",
        "touch_volume_multiple",
    }
    if frozen_record.stage in {"CONFIRMING", "CONFIRMED"}:
        keys.update(
            {
                "absorption",
                "absorption_range_ratio",
                "absorption_sell_quote_ratio",
            }
        )
    if frozen_record.stage == "CONFIRMED":
        keys.update(
            {
                "confirmation",
                "atr_confirmation",
                "confirmation_close_location",
                "confirmation_taker_buy_ratio",
                "confirmation_volume_multiple",
                "entry_min",
                "entry_max",
            }
        )
    return all(frozen.get(key) == current.get(key) for key in keys)


def _recompute_structure_metrics(
    structure: N17Structure,
    candles: Sequence[N17Candle],
) -> N17Structure:
    by_time = {
        candle.open_time_ms: index for index, candle in enumerate(candles)
    }
    touch_index = by_time[structure.touch.open_time_ms]
    atr = _atr_series(candles, 14)
    atr_touch = atr[touch_index]
    if atr_touch is None or touch_index < 20:
        raise ValueError("N17 touch metric source is incomplete")
    touch_volume_median = _median(
        candle.quote_volume for candle in candles[touch_index - 20 : touch_index]
    )
    absorption_range_ratio = None
    absorption_sell_quote_ratio = None
    if structure.absorption is not None:
        absorption_range_ratio = structure.absorption.range / structure.touch.range
        if structure.touch.sell_quote <= 0:
            raise ValueError("N17 touch sell quote is invalid")
        absorption_sell_quote_ratio = (
            structure.absorption.sell_quote / structure.touch.sell_quote
        )
    atr_confirmation = None
    confirmation_close_location = None
    confirmation_taker_buy_ratio = None
    confirmation_volume_multiple = None
    entry_min = None
    entry_max = None
    if structure.confirmation is not None:
        confirmation_index = by_time[structure.confirmation.open_time_ms]
        atr_confirmation = atr[confirmation_index]
        if atr_confirmation is None or confirmation_index < 20:
            raise ValueError("N17 confirmation metric source is incomplete")
        confirmation_volume_median = _median(
            candle.quote_volume
            for candle in candles[confirmation_index - 20 : confirmation_index]
        )
        confirmation_close_location = structure.confirmation.close_location
        confirmation_taker_buy_ratio = structure.confirmation.taker_buy_ratio
        confirmation_volume_multiple = (
            structure.confirmation.quote_volume / confirmation_volume_median
        )
        entry_min = structure.confirmation.close
        entry_max = entry_min + Decimal("0.50") * atr_confirmation
    return N17Structure(
        **{
            **structure.__dict__,
            "atr_touch": atr_touch,
            "atr_confirmation": atr_confirmation,
            "touch_volume_median20": touch_volume_median,
            "touch_volume_multiple": structure.touch.quote_volume
            / touch_volume_median,
            "absorption_range_ratio": absorption_range_ratio,
            "absorption_sell_quote_ratio": absorption_sell_quote_ratio,
            "confirmation_close_location": confirmation_close_location,
            "confirmation_taker_buy_ratio": confirmation_taker_buy_ratio,
            "confirmation_volume_multiple": confirmation_volume_multiple,
            "entry_min": entry_min,
            "entry_max": entry_max,
        }
    )


_APPROVED_CONFIG = {
    "fixed_input_bars": 122,
    "pivot_left": 2,
    "pivot_right": 2,
    "atr_period": 14,
    "box_min_bars": 20,
    "box_max_bars": 64,
    "box_tolerance_fraction": Decimal("0.25"),
    "box_net_move_fraction_max": Decimal("0.50"),
    "touch_breakdown_fraction": Decimal("0.003"),
    "touch_upper_atr": Decimal("0.20"),
    "touch_volume_multiple_max": Decimal("2.5"),
    "panic_body_atr_min": Decimal("0.8"),
    "panic_volume_multiple_min": Decimal("1.5"),
    "panic_taker_buy_ratio_max": Decimal("0.40"),
    "panic_close_location_max": Decimal("0.30"),
    "absorption_range_ratio_max": Decimal("0.85"),
    "absorption_sell_quote_ratio_max": Decimal("0.85"),
    "confirmation_max_bars": 2,
    "confirmation_close_location_min": Decimal("0.65"),
    "confirmation_taker_buy_ratio_min": Decimal("0.52"),
    "confirmation_volume_multiple_min": Decimal("0.80"),
    "n08_bullish_streak_count": 5,
    "entry_extension_atr_max": Decimal("0.50"),
    "entry_window_seconds": 120,
}


def _state_record(
    structure: N17Structure,
    stage: str,
    reason: str,
    quote_volume_rank: int,
    candles: Sequence[N17Candle],
    *,
    reset_after_time_ms: int | None = None,
) -> N17StateRecord:
    family_id = hashlib.sha256(
        "|".join(
            (
                N17_RULE_VERSION,
                structure.symbol,
                str(structure.box.start_time_ms),
                str(structure.box.end_time_ms),
                str(structure.box.upper),
                str(structure.box.lower),
            )
        ).encode("utf-8")
    ).hexdigest()[:24]
    evidence = {
        "schema_version": N17_SCHEMA_VERSION,
        "rule_version": N17_RULE_VERSION,
        "strategy_id": "N17",
        "symbol": structure.symbol,
        "family_id": family_id,
        "structure_id": structure.structure_id,
        "stage": stage,
        "reason": reason,
        "quote_volume_rank": quote_volume_rank,
        "reset_after_time_ms": reset_after_time_ms,
        "structure": structure.to_jsonable(),
        "source": [item.to_jsonable() for item in candles],
    }
    evidence["canonical_sha256"] = _sha256_json(evidence)
    encoded = _canonical_json(evidence).encode("utf-8")
    if len(encoded) > N17_EVIDENCE_MAX_BYTES:
        raise ValueError("N17 evidence exceeds the permanent limit")
    return N17StateRecord(
        strategy_id="N17",
        symbol=structure.symbol,
        family_id=family_id,
        structure_id=structure.structure_id,
        stage=stage,
        reason=reason,
        quote_volume_rank=quote_volume_rank,
        box_start_time_ms=structure.box.start_time_ms,
        box_end_time_ms=structure.box.end_time_ms,
        reset_after_time_ms=reset_after_time_ms,
        evidence=evidence,
    )


def _result(
    symbol: str,
    reason: str,
    current: N17Candle | None,
    checked_at: str,
    entry_window_ms: int,
    quote_volume_rank: int | None,
    *,
    structure: N17Structure | None = None,
    state_record: N17StateRecord | None = None,
    elapsed_ms: int | None = None,
    consume_current: bool = False,
    passed: bool = False,
    state_records: tuple[N17StateRecord, ...] | None = None,
) -> N17AnalysisResult:
    return N17AnalysisResult(
        symbol=symbol,
        passed=passed,
        reason=reason,
        structure=structure,
        current_price=current.close if current else Decimal("0"),
        current_open_time=current.open_time if current else None,
        checked_at=checked_at,
        elapsed_ms=elapsed_ms,
        entry_window_ms=entry_window_ms,
        consume_current=consume_current,
        quote_volume_rank=quote_volume_rank,
        state_record=state_record,
        state_records=(
            state_records
            if state_records is not None
            else ((state_record,) if state_record else ())
        ),
    )


def _structure_for_touch(
    symbol: str,
    candles: Sequence[N17Candle],
    closed: Sequence[N17Candle],
    touch_index: int,
    atr: Sequence[Decimal | None],
    config: dict[str, Any],
) -> tuple[N17Structure | None, str]:
    box = _longest_box_suffix(
        symbol,
        closed,
        touch_index,
        box_min_bars=config["box_min_bars"],
        box_max_bars=config["box_max_bars"],
        pivot_left=config["pivot_left"],
        pivot_right=config["pivot_right"],
        tolerance_fraction=config["box_tolerance_fraction"],
        net_move_fraction_max=config["box_net_move_fraction_max"],
    )
    if box is None:
        return None, "N17_BOX_NOT_FOUND"
    touch = closed[touch_index]
    atr_touch = atr[touch.index]
    if atr_touch is None or atr_touch <= 0 or touch_index < 20:
        return None, "N17_METRICS_NOT_READY"
    prior_volume = _median(item.quote_volume for item in closed[touch_index - 20 : touch_index])
    # A bar above the support band has not touched B yet.  Once the band is
    # reached, however, that first encounter owns the family even when it is
    # routed to N10/N14 or fails the N17 close requirement; a later prettier
    # candle must never replace it.
    if touch.low > box.lower + config["touch_upper_atr"] * atr_touch:
        return None, "N17_TOUCH_NOT_QUALIFIED"
    touch_multiple = touch.quote_volume / prior_volume
    structure = N17Structure(
        symbol=symbol,
        box=box,
        touch=touch,
        absorption=None,
        confirmation=None,
        entry=None,
        atr_touch=atr_touch,
        atr_confirmation=None,
        touch_volume_median20=prior_volume,
        touch_volume_multiple=touch_multiple,
        absorption_range_ratio=None,
        absorption_sell_quote_ratio=None,
        confirmation_close_location=None,
        confirmation_taker_buy_ratio=None,
        confirmation_volume_multiple=None,
        entry_min=None,
        entry_max=None,
        structure_id=_structure_id(symbol, box, touch),
    )
    if touch.low <= box.lower * (Decimal("1") - config["touch_breakdown_fraction"]):
        return structure, "N17_N10_BREAKDOWN_EXCLUDED"
    if touch.close < box.lower:
        return structure, "N17_TOUCH_CLOSED_BELOW_LOWER"
    if touch_multiple >= config["touch_volume_multiple_max"]:
        return structure, "N17_N10_VOLUME_EXCLUDED"
    bearish_body = touch.open - touch.close
    if (
        bearish_body >= config["panic_body_atr_min"] * atr_touch
        and touch_multiple >= config["panic_volume_multiple_min"]
        and touch.taker_buy_ratio <= config["panic_taker_buy_ratio_max"]
        and touch.close_location <= config["panic_close_location_max"]
    ):
        return structure, "N17_N14_PANIC_EXCLUDED"
    return structure, "N17_TOUCH_LOCKED"


def analyze_n17_range_support_rebound(
    symbol: str,
    raw_klines: Sequence[Sequence[Any]],
    *,
    quote_volume_rank: int,
    fixed_input_bars: int = 122,
    pivot_left: int = 2,
    pivot_right: int = 2,
    atr_period: int = 14,
    box_min_bars: int = 20,
    box_max_bars: int = 64,
    box_tolerance_fraction: Decimal = Decimal("0.25"),
    box_net_move_fraction_max: Decimal = Decimal("0.50"),
    touch_breakdown_fraction: Decimal = Decimal("0.003"),
    touch_upper_atr: Decimal = Decimal("0.20"),
    touch_volume_multiple_max: Decimal = Decimal("2.5"),
    panic_body_atr_min: Decimal = Decimal("0.8"),
    panic_volume_multiple_min: Decimal = Decimal("1.5"),
    panic_taker_buy_ratio_max: Decimal = Decimal("0.40"),
    panic_close_location_max: Decimal = Decimal("0.30"),
    absorption_range_ratio_max: Decimal = Decimal("0.85"),
    absorption_sell_quote_ratio_max: Decimal = Decimal("0.85"),
    confirmation_max_bars: int = 2,
    confirmation_close_location_min: Decimal = Decimal("0.65"),
    confirmation_taker_buy_ratio_min: Decimal = Decimal("0.52"),
    confirmation_volume_multiple_min: Decimal = Decimal("0.80"),
    n08_bullish_streak_count: int = 5,
    entry_extension_atr_max: Decimal = Decimal("0.50"),
    entry_window_seconds: int = 120,
    checked_at_ms: int | None = None,
    frozen_evidence: str | dict[str, Any] | None = None,
) -> N17AnalysisResult:
    _symbol(symbol)
    config = {
        "fixed_input_bars": fixed_input_bars,
        "pivot_left": pivot_left,
        "pivot_right": pivot_right,
        "atr_period": atr_period,
        "box_min_bars": box_min_bars,
        "box_max_bars": box_max_bars,
        "box_tolerance_fraction": box_tolerance_fraction,
        "box_net_move_fraction_max": box_net_move_fraction_max,
        "touch_breakdown_fraction": touch_breakdown_fraction,
        "touch_upper_atr": touch_upper_atr,
        "touch_volume_multiple_max": touch_volume_multiple_max,
        "panic_body_atr_min": panic_body_atr_min,
        "panic_volume_multiple_min": panic_volume_multiple_min,
        "panic_taker_buy_ratio_max": panic_taker_buy_ratio_max,
        "panic_close_location_max": panic_close_location_max,
        "absorption_range_ratio_max": absorption_range_ratio_max,
        "absorption_sell_quote_ratio_max": absorption_sell_quote_ratio_max,
        "confirmation_max_bars": confirmation_max_bars,
        "confirmation_close_location_min": confirmation_close_location_min,
        "confirmation_taker_buy_ratio_min": confirmation_taker_buy_ratio_min,
        "confirmation_volume_multiple_min": confirmation_volume_multiple_min,
        "n08_bullish_streak_count": n08_bullish_streak_count,
        "entry_extension_atr_max": entry_extension_atr_max,
        "entry_window_seconds": entry_window_seconds,
    }
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000) if checked_at_ms is None else checked_at_ms
    if type(now_ms) is not int or type(quote_volume_rank) is not int or not 1 <= quote_volume_rank <= 100:
        raise ValueError("N17 time/rank is invalid")
    checked_at = datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat()
    entry_window_ms = entry_window_seconds * 1000
    if config != _APPROVED_CONFIG:
        return _result(symbol, "N17_DEFINITION_INVALID", None, checked_at, entry_window_ms, quote_volume_rank)
    if len(raw_klines) < fixed_input_bars:
        return _result(symbol, "N17_NOT_ENOUGH_HISTORY", None, checked_at, entry_window_ms, quote_volume_rank)
    try:
        candles = parse_n17_klines(raw_klines[-fixed_input_bars:])
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return _result(symbol, "N17_KLINE_DATA_INVALID", None, checked_at, entry_window_ms, quote_volume_rank)
    current = candles[-1]
    if not _continuous(candles):
        return _result(symbol, "N17_KLINE_SEQUENCE_INVALID", current, checked_at, entry_window_ms, quote_volume_rank)
    if not current.open_time_ms <= now_ms < current.open_time_ms + INTERVAL_MS:
        return _result(symbol, "N17_KLINE_AXIS_NOT_READY", current, checked_at, entry_window_ms, quote_volume_rank)
    closed = candles[:-1]
    atr = _atr_series(candles, atr_period)

    frozen_record: N17StateRecord | None = None
    frozen_touch_index: int | None = None
    prior_reset_record: N17StateRecord | None = None
    required_new_box_start_after: int | None = None
    if frozen_evidence is not None:
        try:
            frozen_record = decode_n17_state_evidence(
                frozen_evidence, expected_symbol=symbol
            )
            frozen_touch_time = frozen_record.evidence["structure"]["touch"][
                "open_time_ms"
            ]
            if frozen_record.stage in {"TOUCH_LOCKED", "CONFIRMING", "CONFIRMED"}:
                matching = [
                    index
                    for index, candle in enumerate(closed)
                    if candle.open_time_ms == frozen_touch_time
                ]
                if len(matching) != 1:
                    raise ValueError("N17 active touch is outside the live window")
                frozen_touch_index = matching[0]
            else:
                frozen_box = frozen_record.evidence["structure"]["box"]
                reset_time = frozen_record.reset_after_time_ms
                if reset_time is None:
                    upper_boundary = _decimal(frozen_box["upper_boundary"])
                    lower_boundary = _decimal(frozen_box["lower_boundary"])
                    reset = next(
                        (
                            candle
                            for candle in closed
                            if candle.open_time_ms > frozen_touch_time
                            and (
                                candle.high > upper_boundary
                                or candle.low < lower_boundary
                            )
                        ),
                        None,
                    )
                    if reset is not None:
                        reset_time = reset.open_time_ms
                        updated_evidence = dict(frozen_record.evidence)
                        updated_evidence["reset_after_time_ms"] = reset_time
                        unsigned = dict(updated_evidence)
                        unsigned.pop("canonical_sha256", None)
                        updated_evidence["canonical_sha256"] = _sha256_json(unsigned)
                        prior_reset_record = N17StateRecord(
                            **{
                                **frozen_record.__dict__,
                                "reset_after_time_ms": reset_time,
                                "evidence": updated_evidence,
                            }
                        )
                if reset_time is None:
                    return _result(
                        symbol,
                        "N17_STRUCTURE_CONSUMED",
                        current,
                        checked_at,
                        entry_window_ms,
                        quote_volume_rank,
                        state_record=frozen_record,
                    )
                required_new_box_start_after = reset_time
        except Exception:
            return _result(
                symbol,
                "N17_FROZEN_EVIDENCE_INVALID",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )

    lifecycle_quote_volume_rank = (
        frozen_record.quote_volume_rank
        if frozen_record is not None and frozen_touch_index is not None
        else quote_volume_rank
    )
    lifecycle_candles: Sequence[N17Candle] = candles
    if frozen_record is not None and frozen_touch_index is not None:
        frozen_source = frozen_record.evidence["source"]
        try:
            lifecycle_candles = _merge_n17_frozen_source(
                frozen_source, candles
            )
        except (ArithmeticError, KeyError, TypeError, ValueError):
            return _result(
                symbol,
                "N17_FROZEN_EVIDENCE_INVALID",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )
        if len(lifecycle_candles) > 126:
            return _result(
                symbol,
                "N17_FROZEN_EVIDENCE_INVALID",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )

    events: list[tuple[N17Structure, N17StateRecord, int | None]] = []
    family_boxes: list[tuple[N17Box, int]] = []
    exclusion_reason: str | None = None
    # Runtime discovery is deliberately bounded to the live T/A/C horizon.
    # Older bars establish B but are never retroactively promoted into paper
    # or live opportunities on first deployment; durable coverage/state rows
    # carry every family forward once it is first observed.
    touch_indexes = (
        (frozen_touch_index,)
        if frozen_touch_index is not None
        else range(max(box_min_bars, len(closed) - 4), len(closed))
    )
    for touch_index in touch_indexes:
        structure, touch_reason = _structure_for_touch(
            symbol, candles, closed, touch_index, atr, config
        )
        if structure is None:
            continue
        if (
            required_new_box_start_after is not None
            and structure.box.start_time_ms <= required_new_box_start_after
        ):
            continue
        same_family = False
        for prior_box, prior_touch_index in family_boxes:
            reference_tolerance = max(prior_box.tolerance, structure.box.tolerance)
            if (
                abs(prior_box.upper - structure.box.upper) <= reference_tolerance
                and abs(prior_box.lower - structure.box.lower) <= reference_tolerance
            ):
                reset_indexes = [
                    index
                    for index in range(prior_touch_index + 1, touch_index)
                    if closed[index].high > prior_box.upper_boundary
                    or closed[index].low < prior_box.lower_boundary
                ]
                reset_index = reset_indexes[0] if reset_indexes else None
                if reset_index is None or structure.box.start_time_ms <= closed[reset_index].open_time_ms:
                    same_family = True
                    break
        if same_family:
            continue
        family_boxes.append((structure.box, touch_index))
        touch = closed[touch_index]
        absorption_index = touch_index + 1
        confirmation_index: int | None = None
        if touch_reason != "N17_TOUCH_LOCKED":
            stage = (
                "INVALID"
                if touch_reason == "N17_TOUCH_CLOSED_BELOW_LOWER"
                else "CONSUMED"
            )
            reason = touch_reason
            exclusion_reason = touch_reason
        elif absorption_index >= len(closed):
            stage, reason = "TOUCH_LOCKED", "N17_TOUCH_LOCKED"
        else:
            absorption = closed[absorption_index]
            range_ratio = absorption.range / touch.range
            sell_ratio = (
                absorption.sell_quote / touch.sell_quote
                if touch.sell_quote > 0
                else Decimal("Infinity")
            )
            structure = N17Structure(
                **{
                    **structure.__dict__,
                    "absorption": absorption,
                    "absorption_range_ratio": range_ratio,
                    "absorption_sell_quote_ratio": sell_ratio,
                }
            )
            if absorption.low < touch.low:
                stage, reason = "INVALID", "N17_ABSORPTION_BROKE_TOUCH_LOW"
            elif (
                absorption.close < structure.box.lower
                or range_ratio > absorption_range_ratio_max
                or sell_ratio > absorption_sell_quote_ratio_max
            ):
                stage, reason = "CONSUMED", "N17_ABSORPTION_NOT_QUALIFIED"
            else:
                stage, reason = "CONFIRMING", "N17_CONFIRMATION_WAITING"
                available = 0
                for index in range(
                    absorption_index + 1,
                    min(absorption_index + confirmation_max_bars + 1, len(closed)),
                ):
                    available += 1
                    item = closed[index]
                    if item.low < touch.low:
                        stage, reason = "INVALID", "N17_CONFIRMATION_BROKE_TOUCH_LOW"
                        break
                    prior_volume = _median(
                        c.quote_volume for c in closed[index - 20 : index]
                    )
                    prior_high = max(c.high for c in closed[touch_index:index])
                    if (
                        item.close > prior_high
                        and item.close_location >= confirmation_close_location_min
                        and item.taker_buy_ratio >= confirmation_taker_buy_ratio_min
                        and item.quote_volume >= confirmation_volume_multiple_min * prior_volume
                    ):
                        confirmation_index = index
                        atr_confirmation = atr[item.index]
                        if atr_confirmation is None or atr_confirmation <= 0:
                            stage, reason = "INVALID", "N17_CONFIRMATION_METRICS_INVALID"
                            confirmation_index = None
                            break
                        structure = N17Structure(
                            **{
                                **structure.__dict__,
                                "confirmation": item,
                                "atr_confirmation": atr_confirmation,
                                "confirmation_close_location": item.close_location,
                                "confirmation_taker_buy_ratio": item.taker_buy_ratio,
                                "confirmation_volume_multiple": item.quote_volume / prior_volume,
                                "entry_min": item.close,
                                "entry_max": item.close + entry_extension_atr_max * atr_confirmation,
                            }
                        )
                        stage, reason = "CONFIRMED", "N17_CONFIRMED"
                        break
                if confirmation_index is None and stage == "CONFIRMING" and available >= confirmation_max_bars:
                    stage, reason = "CONSUMED", "N17_CONFIRMATION_NOT_FOUND"
        if confirmation_index is not None:
            entry_index = confirmation_index + 1
            if entry_index >= len(candles) or candles[entry_index].open_time_ms != closed[confirmation_index].open_time_ms + INTERVAL_MS:
                stage, reason = "MISSED", "N17_HISTORICAL_ENTRY_MISSED"
            elif entry_index != len(candles) - 1:
                stage, reason = "MISSED", "N17_HISTORICAL_ENTRY_MISSED"
            elif all(
                item.close > item.open
                for item in closed[-n08_bullish_streak_count:]
            ):
                stage, reason = "CONSUMED", "N17_N08_BULLISH_STREAK_EXCLUDED"
            structure = N17Structure(
                **{**structure.__dict__, "entry": candles[entry_index]}
            )
        if frozen_record is not None and frozen_touch_index is not None:
            try:
                structure = _recompute_structure_metrics(
                    structure, lifecycle_candles
                )
            except Exception:
                return _result(
                    symbol,
                    "N17_FROZEN_EVIDENCE_INVALID",
                    current,
                    checked_at,
                    entry_window_ms,
                    quote_volume_rank,
                )
        record = _state_record(
            structure,
            stage,
            reason,
            lifecycle_quote_volume_rank,
            lifecycle_candles,
        )
        if frozen_record is not None and frozen_touch_index is not None and (
            record.structure_id != frozen_record.structure_id
            or not _frozen_structure_matches(frozen_record, structure)
        ):
            return _result(
                symbol,
                "N17_FROZEN_EVIDENCE_INVALID",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
            )
        if (
            frozen_record is not None
            and frozen_touch_index is not None
            and stage == frozen_record.stage
            and stage in {"TOUCH_LOCKED", "CONFIRMING", "CONFIRMED"}
        ):
            # A live cumulative tail can change on every scan while the
            # lifecycle remains in the same state.  Keep the already
            # authenticated durable record until the state actually
            # progresses; the next progression replays and authenticates the
            # entire monotonic live-to-closed source.  Re-signing an
            # unchanged state on each live-tail observation would correctly
            # be rejected by the recorder as a conflicting durable history.
            record = frozen_record
        events.append((structure, record, confirmation_index))

    if not events:
        if prior_reset_record is not None:
            return _result(
                symbol,
                "N17_STRUCTURE_CONSUMED",
                current,
                checked_at,
                entry_window_ms,
                quote_volume_rank,
                state_record=prior_reset_record,
            )
        return _result(
            symbol,
            exclusion_reason or "N17_STRUCTURE_NOT_FOUND",
            current,
            checked_at,
            entry_window_ms,
            quote_volume_rank,
        )
    all_records = (
        ((prior_reset_record,) if prior_reset_record is not None else ())
        + tuple(item[1] for item in events)
    )
    executable = [
        item
        for item in events
        if item[2] == len(closed) - 1 and item[1].stage == "CONFIRMED"
    ]
    if not executable:
        structure, record, _ = events[-1]
        return _result(
            symbol,
            record.reason,
            current,
            checked_at,
            entry_window_ms,
            quote_volume_rank,
            structure=structure,
            state_record=record,
            consume_current=record.stage in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"},
            state_records=all_records,
        )

    structure, _confirmed_record, confirmation_index = executable[0]
    elapsed_ms = now_ms - current.open_time_ms
    if elapsed_ms < 0:
        reason, stage = "N17_ENTRY_WINDOW_NOT_OPEN", "CONFIRMED"
    elif elapsed_ms >= entry_window_ms:
        reason, stage = "N17_ENTRY_WINDOW_EXPIRED", "EXPIRED"
    elif current.low < structure.touch.low:
        reason, stage = "N17_ENTRY_BROKE_TOUCH_LOW", "INVALID"
    elif current.close < structure.entry_min:
        reason, stage = "N17_ENTRY_BELOW_CONFIRMATION", "CONFIRMED"
    elif current.close > structure.entry_max:
        reason, stage = "N17_ENTRY_PRICE_TOO_EXTENDED", "MISSED"
    else:
        reason, stage = "PASSED", "CONFIRMED"
    terminal = stage in {"EXPIRED", "INVALID", "MISSED"}
    record = _state_record(
        structure,
        stage,
        reason,
        lifecycle_quote_volume_rank,
        lifecycle_candles,
    )
    if (
        frozen_record is not None
        and frozen_touch_index is not None
        and stage == frozen_record.stage
        and stage in {"TOUCH_LOCKED", "CONFIRMING", "CONFIRMED"}
    ):
        record = frozen_record
    return _result(
        symbol,
        reason,
        current,
        checked_at,
        entry_window_ms,
        quote_volume_rank,
        structure=structure,
        state_record=record,
        elapsed_ms=elapsed_ms,
        consume_current=terminal,
        passed=reason == "PASSED",
        state_records=all_records[:-1] + (record,),
    )
