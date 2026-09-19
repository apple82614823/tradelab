from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence

from .exchange_symbol import canonical_exchange_symbol


INTERVAL_MS = 15 * 60 * 1000
N18_SCHEMA_VERSION = 1
N18_RULE_VERSION = "N18_V1"
N18_EVIDENCE_MAX_BYTES = 128 * 1024

N18_TERMINAL_STAGE_REASONS: Mapping[str, frozenset[str]] = {
    "CONSUMED": frozenset({
        "N18_PREEXISTING_BREAKOUT",
        "N18_ABSORPTION_NOT_QUALIFIED",
        "N18_BREAKOUT_ATR_INVALID",
        "N18_BREAKOUT_NOT_QUALIFIED",
        "N18_BREAKOUT_NOT_FOUND",
    }),
    "INVALID": frozenset({
        "N18_BREAKOUT_BROKE_L3_LOW",
        "N18_ENTRY_TRIANGLE_INVALIDATED",
        "N18_ENTRY_BREAKOUT_NOT_HELD",
    }),
    "MISSED": frozenset({
        "N18_HISTORICAL_ENTRY_MISSED",
        "N18_ENTRY_PRICE_ABOVE_MAX",
    }),
    "EXPIRED": frozenset({"N18_ENTRY_WINDOW_EXPIRED"}),
}
_N18_TERMINAL_REASONS = frozenset().union(
    *N18_TERMINAL_STAGE_REASONS.values()
)


def _validate_n18_terminal_stage_reason(stage: Any, reason: Any) -> None:
    expected_reasons = N18_TERMINAL_STAGE_REASONS.get(stage)
    if expected_reasons is not None:
        if type(reason) is not str or reason not in expected_reasons:
            raise ValueError("N18 terminal stage and reason conflict")
    elif reason in _N18_TERMINAL_REASONS:
        raise ValueError("N18 terminal reason requires its canonical stage")


def _validate_n18_evidence_types(value: Any, *, key: str | None = None) -> None:
    """Reject JSON scalar coercions before reproducing frozen evidence."""

    if key in {"schema_version", "quote_volume_rank"} or (
        type(key) is str
        and (key.endswith("_time_ms") or key.endswith("_interval_bars"))
    ):
        if value is None and key == "terminal_cutoff_time_ms":
            return
        if type(value) is not int:
            raise ValueError("N18 evidence integer type is invalid")
        return
    if key in {
        "rule_version", "strategy_id", "symbol", "family_id", "stage", "reason",
    }:
        if type(value) is not str:
            raise ValueError("N18 evidence text type is invalid")
        return
    if key == "structure_id":
        if value is not None and type(value) is not str:
            raise ValueError("N18 evidence structure identity type is invalid")
        return
    if type(value) is dict:
        for child_key, child in value.items():
            _validate_n18_evidence_types(child, key=child_key)
    elif type(value) is list:
        for child in value:
            _validate_n18_evidence_types(child)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    )


def _strict_json_loads(value: str) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in items:
            if key in result:
                raise ValueError("N18 JSON contains a duplicate key")
            result[key] = item
        return result

    return json.loads(
        value,
        object_pairs_hook=pairs,
        parse_constant=lambda item: (_ for _ in ()).throw(
            ValueError("N18 JSON contains a non-finite value: %s" % item)
        ),
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _decimal(value: Any) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise InvalidOperation("non-finite decimal")
    return result


def _median(values: Iterable[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("N18 median input is empty")
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def _symbol(value: Any) -> str:
    try:
        return canonical_exchange_symbol(value)
    except ValueError as exc:
        raise ValueError("N18 symbol is invalid") from exc


@dataclass(frozen=True)
class N18Candle:
    index: int
    open_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    quote_volume: Decimal
    taker_buy_quote_volume: Decimal

    @property
    def range(self) -> Decimal:
        return self.high - self.low

    @property
    def body(self) -> Decimal:
        return abs(self.close - self.open)

    @property
    def close_location(self) -> Decimal:
        return (self.close - self.low) / self.range

    @property
    def taker_buy_ratio(self) -> Decimal:
        return self.taker_buy_quote_volume / self.quote_volume

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


def _validate_candle(candle: N18Candle) -> None:
    if (
        type(candle.open_time_ms) is not int
        or candle.open_time_ms <= 0
        or candle.open_time_ms % INTERVAL_MS
        or min(candle.open, candle.high, candle.low, candle.close) <= 0
        or candle.high <= candle.low
        or candle.high < max(candle.open, candle.close)
        or candle.low > min(candle.open, candle.close)
        or candle.quote_volume <= 0
        or candle.taker_buy_quote_volume < 0
        or candle.taker_buy_quote_volume > candle.quote_volume
    ):
        raise ValueError("N18 candle semantics are invalid")


def parse_n18_klines(raw_klines: Sequence[Sequence[Any]]) -> list[N18Candle]:
    result: list[N18Candle] = []
    for index, row in enumerate(raw_klines):
        if type(row) not in (list, tuple) or len(row) <= 10:
            raise ValueError("N18 kline requires indexes 0-10")
        open_time = _decimal(row[0])
        if open_time != open_time.to_integral_value():
            raise ValueError("N18 kline time is invalid")
        candle = N18Candle(
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
        result.append(candle)
    return result


def _continuous(candles: Sequence[N18Candle]) -> bool:
    return all(
        right.open_time_ms - left.open_time_ms == INTERVAL_MS
        for left, right in zip(candles, candles[1:])
    )


def _atr_series(candles: Sequence[N18Candle], period: int) -> list[Decimal | None]:
    true_ranges: list[Decimal] = []
    for index, candle in enumerate(candles):
        if index == 0:
            true_ranges.append(candle.range)
        else:
            prior = candles[index - 1].close
            true_ranges.append(
                max(candle.range, abs(candle.high - prior), abs(candle.low - prior))
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


def _ema_series(candles: Sequence[N18Candle], period: int) -> list[Decimal]:
    multiplier = Decimal("2") / Decimal(period + 1)
    result = [candles[0].close]
    for candle in candles[1:]:
        result.append((candle.close - result[-1]) * multiplier + result[-1])
    return result


@dataclass(frozen=True)
class N18Turn:
    kind: str
    candle: N18Candle


def _turns(
    candles: Sequence[N18Candle], left: int, right: int
) -> tuple[N18Turn, ...]:
    raw: list[N18Turn] = []
    for index in range(left, len(candles) - right):
        candle = candles[index]
        prior = candles[index - left:index]
        later = candles[index + 1:index + right + 1]
        if all(candle.high > item.high for item in prior) and all(
            candle.high >= item.high for item in later
        ):
            raw.append(N18Turn("HIGH", candle))
        if all(candle.low < item.low for item in prior) and all(
            candle.low <= item.low for item in later
        ):
            raw.append(N18Turn("LOW", candle))
    compressed: list[N18Turn] = []
    for turn in sorted(raw, key=lambda item: (item.candle.index, item.kind)):
        if not compressed or compressed[-1].kind != turn.kind:
            compressed.append(turn)
            continue
        prior = compressed[-1]
        if (turn.kind == "HIGH" and turn.candle.high > prior.candle.high) or (
            turn.kind == "LOW" and turn.candle.low < prior.candle.low
        ):
            compressed[-1] = turn
    return tuple(compressed)


@dataclass(frozen=True)
class N18Structure:
    symbol: str
    l1: N18Candle
    h1: N18Candle
    l2: N18Candle
    h2: N18Candle
    l3: N18Candle
    a: N18Candle
    b: N18Candle | None
    entry: N18Candle | None
    atr_a: Decimal
    atr_b: Decimal | None
    ema20_a: Decimal
    ema50_a: Decimal
    resistance: Decimal
    resistance_dispersion: Decimal
    l1_l2_atr: Decimal
    l2_l3_atr: Decimal
    initial_height_atr: Decimal
    convergence_ratio: Decimal
    a_volume_multiple: Decimal
    b_volume_multiple: Decimal | None
    entry_min: Decimal | None
    entry_max: Decimal | None
    family_id: str
    structure_id: str | None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "family_id": self.family_id,
            "structure_id": self.structure_id,
            "l1": self.l1.to_jsonable(), "h1": self.h1.to_jsonable(),
            "l2": self.l2.to_jsonable(), "h2": self.h2.to_jsonable(),
            "l3": self.l3.to_jsonable(), "a": self.a.to_jsonable(),
            "b": self.b.to_jsonable() if self.b else None,
            "entry": self.entry.to_jsonable() if self.entry else None,
            "atr_a": str(self.atr_a),
            "atr_b": str(self.atr_b) if self.atr_b is not None else None,
            "ema20_a": str(self.ema20_a), "ema50_a": str(self.ema50_a),
            "resistance": str(self.resistance),
            "resistance_band_low": str(self.resistance - Decimal("0.25") * self.atr_a),
            "resistance_band_high": str(self.resistance + Decimal("0.10") * self.atr_a),
            "resistance_dispersion": str(self.resistance_dispersion),
            "h1_h2_interval_bars": self.h2.index - self.h1.index,
            "h2_a_interval_bars": self.a.index - self.h2.index,
            "l1_l2_atr": str(self.l1_l2_atr),
            "l2_l3_atr": str(self.l2_l3_atr),
            "initial_height_atr": str(self.initial_height_atr),
            "convergence_ratio": str(self.convergence_ratio),
            "a_volume_multiple": str(self.a_volume_multiple),
            "b_volume_multiple": (
                str(self.b_volume_multiple) if self.b_volume_multiple is not None else None
            ),
            "entry_min": str(self.entry_min) if self.entry_min is not None else None,
            "entry_max": str(self.entry_max) if self.entry_max is not None else None,
        }


@dataclass(frozen=True)
class N18StateRecord:
    strategy_id: str
    symbol: str
    family_id: str
    structure_id: str | None
    stage: str
    reason: str
    quote_volume_rank: int
    l1_open_time_ms: int
    a_open_time_ms: int
    terminal_cutoff_time_ms: int | None
    evidence: dict[str, Any]

    @property
    def evidence_json(self) -> str:
        return _canonical_json(self.evidence)

    @property
    def evidence_sha256(self) -> str:
        return _sha256_json(self.evidence)


@dataclass(frozen=True)
class N18AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    structure: N18Structure | None
    current_price: Decimal
    current_open_time: str | None
    checked_at: str
    elapsed_ms: int | None
    entry_window_ms: int
    consume_current: bool
    quote_volume_rank: int | None
    state_record: N18StateRecord | None
    state_records: tuple[N18StateRecord, ...] = ()

    @property
    def structure_id(self) -> str | None:
        return self.structure.structure_id if self.structure else None

    @property
    def current_bullish(self) -> bool:
        return bool(
            self.structure and self.structure.entry
            and self.structure.entry.close > self.structure.entry.open
        )

    def detail_json(self) -> dict[str, Any]:
        payload = {
            "schema_version": N18_SCHEMA_VERSION,
            "rule_version": N18_RULE_VERSION,
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
            "evidence_sha256": (
                self.state_record.evidence_sha256 if self.state_record else None
            ),
        }
        if self.structure:
            payload["structure"] = self.structure.to_jsonable()
        if len(_canonical_json(payload).encode("utf-8")) > 16 * 1024:
            raise ValueError("N18 signal detail exceeds 16KiB")
        return payload


_APPROVED_CONFIG = {
    "fixed_input_bars": 122, "pivot_left": 2, "pivot_right": 2,
    "atr_period": 14, "ema_fast_period": 20, "ema_slow_period": 50,
    "triangle_span_min_bars": 12, "triangle_span_max_bars": 36,
    "pressure_interval_min_bars": 3,
    "pressure_dispersion_atr_max": Decimal("0.35"),
    "pressure_dispersion_fraction_max": Decimal("0.005"),
    "a_lower_atr": Decimal("0.25"), "a_upper_atr": Decimal("0.10"),
    "higher_low_progress_atr_min": Decimal("0.20"),
    "initial_height_atr_min": Decimal("2"),
    "convergence_ratio_max": Decimal("0.65"),
    "a_close_location_min": Decimal("0.60"),
    "a_taker_buy_ratio_min": Decimal("0.50"),
    "a_volume_multiple_min": Decimal("0.80"),
    "breakout_max_bars": 4, "breakout_threshold_atr": Decimal("0.10"),
    "breakout_body_atr_min": Decimal("0.40"),
    "breakout_close_location_min": Decimal("0.75"),
    "breakout_taker_buy_ratio_min": Decimal("0.55"),
    "breakout_volume_multiple_min": Decimal("1.30"),
    "entry_extension_atr_max": Decimal("0.50"), "entry_window_seconds": 120,
}


def _family_id(symbol: str, points: Sequence[N18Candle]) -> str:
    payload = "|".join(["N18", symbol] + [str(item.open_time_ms) for item in points])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _structure_id(family_id: str, b: N18Candle) -> str:
    return hashlib.sha256(
        ("N18|" + family_id + "|" + str(b.open_time_ms)).encode("utf-8")
    ).hexdigest()[:24]


def _source(candles: Sequence[N18Candle]) -> list[dict[str, Any]]:
    return [item.to_jsonable() for item in candles]


def _triangle_thresholds_pass(
    *, span: int, h1_h2: int, h2_a: int, dispersion: Decimal,
    atr_a: Decimal, resistance: Decimal, a: N18Candle,
    l1_l2: Decimal, l2_l3: Decimal, initial_height: Decimal,
    convergence: Decimal, ema20: Decimal, ema50: Decimal,
    a_volume_multiple: Decimal, config: Mapping[str, Any],
) -> bool:
    return bool(
        config["triangle_span_min_bars"] <= span <= config["triangle_span_max_bars"]
        and h1_h2 >= config["pressure_interval_min_bars"]
        and h2_a >= config["pressure_interval_min_bars"]
        and dispersion <= config["pressure_dispersion_atr_max"] * atr_a
        and dispersion <= config["pressure_dispersion_fraction_max"] * resistance
        and resistance - config["a_lower_atr"] * atr_a
        <= a.high <= resistance + config["a_upper_atr"] * atr_a
        and l1_l2 >= config["higher_low_progress_atr_min"]
        and l2_l3 >= config["higher_low_progress_atr_min"]
        and initial_height >= config["initial_height_atr_min"]
        and Decimal("0") < convergence <= config["convergence_ratio_max"]
        and ema20 >= ema50
        and a.close <= resistance
        and a.close_location >= config["a_close_location_min"]
        and a.taker_buy_ratio >= config["a_taker_buy_ratio_min"]
        and a_volume_multiple >= config["a_volume_multiple_min"]
    )


def _breakout_thresholds_pass(
    b: N18Candle, atr_b: Decimal, volume_multiple: Decimal,
    config: Mapping[str, Any],
) -> bool:
    return bool(
        b.body >= config["breakout_body_atr_min"] * atr_b
        and b.close_location >= config["breakout_close_location_min"]
        and b.taker_buy_ratio >= config["breakout_taker_buy_ratio_min"]
        and volume_multiple >= config["breakout_volume_multiple_min"]
    )


def _triangle_geometry_pass(
    *, span: int, h1_h2: int, h2_a: int, dispersion: Decimal,
    atr_a: Decimal, resistance: Decimal, a: N18Candle,
    l1_l2: Decimal, l2_l3: Decimal, initial_height: Decimal,
    convergence: Decimal, config: Mapping[str, Any],
) -> bool:
    """Recognize the immutable six-point family before A quality gates."""

    return bool(
        config["triangle_span_min_bars"] <= span <= config["triangle_span_max_bars"]
        and h1_h2 >= config["pressure_interval_min_bars"]
        and h2_a >= config["pressure_interval_min_bars"]
        and dispersion <= config["pressure_dispersion_atr_max"] * atr_a
        and dispersion <= config["pressure_dispersion_fraction_max"] * resistance
        and resistance - config["a_lower_atr"] * atr_a
        <= a.high <= resistance + config["a_upper_atr"] * atr_a
        and l1_l2 >= config["higher_low_progress_atr_min"]
        and l2_l3 >= config["higher_low_progress_atr_min"]
        and initial_height >= config["initial_height_atr_min"]
        and Decimal("0") < convergence <= config["convergence_ratio_max"]
    )


def _candidate_structures(
    symbol: str, candles: Sequence[N18Candle], closed: Sequence[N18Candle],
    atr: Sequence[Decimal | None], ema20: Sequence[Decimal],
    ema50: Sequence[Decimal], config: Mapping[str, Any],
    *, minimum_l1_time: int | None = None,
) -> list[N18Structure]:
    result: list[N18Structure] = []
    turns = _turns(closed, config["pivot_left"], config["pivot_right"])
    for offset in range(len(turns) - 5):
        window = turns[offset:offset + 6]
        if tuple(item.kind for item in window) != (
            "LOW", "HIGH", "LOW", "HIGH", "LOW", "HIGH"
        ):
            continue
        l1, h1, l2, h2, l3, a = [item.candle for item in window]
        if minimum_l1_time is not None and l1.open_time_ms <= minimum_l1_time:
            continue
        atr_a = atr[a.index]
        if atr_a is None or a.index < 20 or not (l1.low < l2.low < l3.low):
            continue
        resistance = _median((h1.high, h2.high, a.high))
        dispersion = max(abs(item.high - resistance) for item in (h1, h2, a))
        initial = resistance - l1.low
        final = resistance - l3.low
        if initial <= 0 or final <= 0:
            continue
        a_volume_multiple = a.quote_volume / _median(
            item.quote_volume for item in candles[a.index - 20:a.index]
        )
        l1_l2 = (l2.low - l1.low) / atr_a
        l2_l3 = (l3.low - l2.low) / atr_a
        if not _triangle_geometry_pass(
            span=a.index - l1.index + 1,
            h1_h2=h2.index - h1.index,
            h2_a=a.index - h2.index,
            dispersion=dispersion, atr_a=atr_a, resistance=resistance, a=a,
            l1_l2=l1_l2, l2_l3=l2_l3,
            initial_height=initial / atr_a, convergence=final / initial,
            config=config,
        ):
            continue
        family_id = _family_id(symbol, (l1, h1, l2, h2, l3, a))
        result.append(N18Structure(
            symbol, l1, h1, l2, h2, l3, a, None, None,
            atr_a, None, ema20[a.index], ema50[a.index], resistance,
            dispersion, l1_l2, l2_l3, initial / atr_a, final / initial,
            a_volume_multiple, None, None, None, family_id, None,
        ))
    return sorted(
        result,
        key=lambda item: (item.l1.open_time_ms, item.a.open_time_ms, item.family_id),
    )


def _record(
    structure: N18Structure, stage: str, reason: str, rank: int,
    source: Sequence[N18Candle], terminal_cutoff: int | None = None,
) -> N18StateRecord:
    terminal = stage in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}
    if terminal:
        if terminal_cutoff is None:
            if structure.entry is not None:
                terminal_cutoff = structure.entry.open_time_ms
            elif structure.b is not None:
                terminal_cutoff = structure.b.open_time_ms
            else:
                terminal_cutoff = source[-2].open_time_ms
        if terminal_cutoff <= structure.a.open_time_ms:
            raise ValueError("N18 terminal cutoff is invalid")
    elif terminal_cutoff is not None:
        raise ValueError("N18 active state cannot have a terminal cutoff")
    unsigned = {
        "schema_version": N18_SCHEMA_VERSION, "rule_version": N18_RULE_VERSION,
        "strategy_id": "N18", "symbol": structure.symbol,
        "family_id": structure.family_id, "structure_id": structure.structure_id,
        "stage": stage, "reason": reason, "quote_volume_rank": rank,
        "terminal_cutoff_time_ms": terminal_cutoff,
        "structure": structure.to_jsonable(), "source": _source(source),
    }
    evidence = dict(unsigned)
    evidence["canonical_sha256"] = _sha256_json(unsigned)
    if len(_canonical_json(evidence).encode("utf-8")) > N18_EVIDENCE_MAX_BYTES:
        raise ValueError("N18 evidence exceeds 128KiB")
    return N18StateRecord(
        "N18", structure.symbol, structure.family_id, structure.structure_id,
        stage, reason, rank, structure.l1.open_time_ms, structure.a.open_time_ms,
        terminal_cutoff, evidence,
    )


def decode_n18_state_evidence(
    value: str | dict[str, Any], *, expected_symbol: str | None = None,
) -> N18StateRecord:
    parsed = _strict_json_loads(value) if type(value) is str else value
    if type(parsed) is not dict:
        raise ValueError("N18 evidence shape is invalid")
    unsigned = dict(parsed)
    claimed = unsigned.pop("canonical_sha256", None)
    _validate_n18_evidence_types(unsigned)
    if (
        set(unsigned) != {
            "schema_version", "rule_version", "strategy_id", "symbol",
            "family_id", "structure_id", "stage", "reason",
            "quote_volume_rank", "terminal_cutoff_time_ms", "structure", "source",
        }
        or claimed != _sha256_json(unsigned)
        or type(unsigned["schema_version"]) is not int
        or unsigned["schema_version"] != N18_SCHEMA_VERSION
        or unsigned["rule_version"] != N18_RULE_VERSION
        or unsigned["strategy_id"] != "N18"
        or type(unsigned["symbol"]) is not str
        or (expected_symbol is not None and unsigned["symbol"] != expected_symbol)
        or type(unsigned["family_id"]) is not str
        or len(unsigned["family_id"]) != 24
        or type(unsigned["stage"]) is not str
        or type(unsigned["reason"]) is not str
        or type(unsigned["quote_volume_rank"]) is not int
        or not 1 <= unsigned["quote_volume_rank"] <= 100
        or (
            unsigned["structure_id"] is not None
            and (type(unsigned["structure_id"]) is not str or len(unsigned["structure_id"]) != 24)
        )
        or type(unsigned["source"]) is not list
        or not unsigned["source"]
    ):
        raise ValueError("N18 evidence identity is invalid")
    _validate_n18_terminal_stage_reason(unsigned["stage"], unsigned["reason"])
    source = parse_n18_klines([
        [row["open_time_ms"], row["open"], row["high"], row["low"], row["close"],
         "0", "0", row["quote_volume"], "0", "0", row["taker_buy_quote_volume"]]
        for row in unsigned["source"]
    ])
    if not _continuous(source):
        raise ValueError("N18 evidence source is discontinuous")
    structure = unsigned["structure"]
    if type(structure) is not dict:
        raise ValueError("N18 evidence structure is invalid")
    points = [structure[name] for name in ("l1", "h1", "l2", "h2", "l3", "a")]
    if any(type(item) is not dict for item in points):
        raise ValueError("N18 evidence points are invalid")
    candles_by_time = {item.open_time_ms: item for item in source}
    try:
        point_candles = [
            candles_by_time[item["open_time_ms"]] for item in points
        ]
    except (KeyError, TypeError):
        raise ValueError("N18 evidence point source is missing")
    if any(
        candle.to_jsonable() != payload
        for candle, payload in zip(point_candles, points)
    ):
        raise ValueError("N18 evidence point content conflicts with source")
    if _family_id(unsigned["symbol"], point_candles) != unsigned["family_id"]:
        raise ValueError("N18 family identity conflicts")
    b = structure.get("b")
    if b is not None:
        if type(b) is not dict or b.get("open_time_ms") not in candles_by_time:
            raise ValueError("N18 breakout evidence is missing")
        if _structure_id(
            unsigned["family_id"], candles_by_time[b["open_time_ms"]]
        ) != unsigned["structure_id"]:
            raise ValueError("N18 structure identity conflicts")
        if candles_by_time[b["open_time_ms"]].to_jsonable() != b:
            raise ValueError("N18 breakout content conflicts with source")
    entry = structure.get("entry")
    if entry is not None:
        if (
            type(entry) is not dict
            or entry.get("open_time_ms") not in candles_by_time
            or candles_by_time[entry["open_time_ms"]].to_jsonable() != entry
        ):
            raise ValueError("N18 entry content conflicts with source")
    terminal = unsigned["stage"] in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}
    cutoff = unsigned["terminal_cutoff_time_ms"]
    expected_cutoff = None
    if terminal:
        if type(entry) is dict:
            expected_cutoff = entry.get("open_time_ms")
        elif type(b) is dict:
            expected_cutoff = b.get("open_time_ms")
        elif unsigned["reason"] in {
            "N18_PREEXISTING_BREAKOUT", "N18_ABSORPTION_NOT_QUALIFIED",
        }:
            expected_cutoff = points[-1]["open_time_ms"] + 2 * INTERVAL_MS
        elif len(source) >= 2:
            expected_cutoff = source[-2].open_time_ms
    if (
        (terminal and (
            type(cutoff) is not int
            or cutoff <= points[-1]["open_time_ms"]
            or cutoff != expected_cutoff
        ))
        or (not terminal and cutoff is not None)
    ):
        raise ValueError("N18 terminal cutoff is invalid")

    atr = _atr_series(source, _APPROVED_CONFIG["atr_period"])
    ema20 = _ema_series(source, _APPROVED_CONFIG["ema_fast_period"])
    ema50 = _ema_series(source, _APPROVED_CONFIG["ema_slow_period"])
    candidates = _candidate_structures(
        unsigned["symbol"], source, source[:-1], atr, ema20, ema50,
        _APPROVED_CONFIG,
    )
    matches = [item for item in candidates if item.family_id == unsigned["family_id"]]
    if len(matches) != 1:
        raise ValueError("N18 frozen family cannot be recomputed")
    recomputed = matches[0]
    if type(b) is dict:
        b_candle = candles_by_time[b["open_time_ms"]]
        atr_b = atr[b_candle.index]
        if atr_b is None or b_candle.index < 20:
            raise ValueError("N18 frozen breakout ATR is invalid")
        volume_multiple = b_candle.quote_volume / _median(
            item.quote_volume for item in source[b_candle.index - 20:b_candle.index]
        )
        recomputed = replace(
            recomputed,
            b=b_candle,
            atr_b=atr_b,
            b_volume_multiple=volume_multiple,
            structure_id=_structure_id(recomputed.family_id, b_candle),
            entry_min=b_candle.close,
            entry_max=(
                b_candle.close
                + _APPROVED_CONFIG["entry_extension_atr_max"] * atr_b
            ),
        )
    if type(entry) is dict:
        recomputed = replace(
            recomputed, entry=candles_by_time[entry["open_time_ms"]]
        )
    if recomputed.to_jsonable() != structure:
        raise ValueError("N18 derived evidence conflicts with frozen source")
    return N18StateRecord(
        "N18", unsigned["symbol"], unsigned["family_id"], unsigned["structure_id"],
        unsigned["stage"], unsigned["reason"], unsigned["quote_volume_rank"],
        points[0]["open_time_ms"], points[-1]["open_time_ms"], cutoff, parsed,
    )


def validate_n18_source_progression(
    existing: N18StateRecord,
    updated: N18StateRecord,
) -> None:
    """Authenticate one active lifecycle update and its unique live tail."""

    if (
        type(existing) is not N18StateRecord
        or type(updated) is not N18StateRecord
        or existing.strategy_id != "N18"
        or updated.strategy_id != "N18"
        or existing.symbol != updated.symbol
        or existing.family_id != updated.family_id
        or existing.l1_open_time_ms != updated.l1_open_time_ms
        or existing.a_open_time_ms != updated.a_open_time_ms
        or existing.quote_volume_rank != updated.quote_volume_rank
    ):
        raise ValueError("N18 lifecycle identity progression is invalid")
    existing_rows = existing.evidence.get("source")
    updated_rows = updated.evidence.get("source")
    if (
        type(existing_rows) is not list
        or type(updated_rows) is not list
        or len(existing_rows) < 2
        or len(updated_rows) < len(existing_rows)
        or updated_rows[:len(existing_rows) - 1]
        != existing_rows[:-1]
    ):
        raise ValueError("N18 frozen source prefix changed")
    try:
        frozen = parse_n18_klines([
            [
                row["open_time_ms"], row["open"], row["high"], row["low"],
                row["close"], "0", "0", row["quote_volume"], "0", "0",
                row["taker_buy_quote_volume"],
            ]
            for row in existing_rows
        ])
        advanced = parse_n18_klines([
            [
                row["open_time_ms"], row["open"], row["high"], row["low"],
                row["close"], "0", "0", row["quote_volume"], "0", "0",
                row["taker_buy_quote_volume"],
            ]
            for row in updated_rows
        ])
    except Exception as exc:
        raise ValueError("N18 frozen source progression is invalid") from exc
    if (
        not _continuous(frozen)
        or not _continuous(advanced)
        or _source(frozen) != existing_rows
        or _source(advanced) != updated_rows
        or advanced[len(frozen) - 1].open_time_ms
        != frozen[-1].open_time_ms
        or advanced[len(frozen) - 1].open != frozen[-1].open
    ):
        raise ValueError("N18 frozen live tail identity changed")
    old_tail = frozen[-1]
    new_tail = advanced[len(frozen) - 1]
    if new_tail.to_jsonable() != old_tail.to_jsonable():
        quote_delta = new_tail.quote_volume - old_tail.quote_volume
        taker_delta = (
            new_tail.taker_buy_quote_volume
            - old_tail.taker_buy_quote_volume
        )
        if (
            new_tail.high < old_tail.high
            or new_tail.low > old_tail.low
            or quote_delta <= 0
            or taker_delta < 0
            or taker_delta > quote_delta
        ):
            raise ValueError("N18 frozen live tail evolution is invalid")


def _result(
    symbol: str, reason: str, current: N18Candle | None, checked_at: str,
    entry_window_ms: int, rank: int | None, *, structure: N18Structure | None = None,
    record: N18StateRecord | None = None, elapsed_ms: int | None = None,
    passed: bool = False, consume: bool = False,
    state_records: tuple[N18StateRecord, ...] | None = None,
) -> N18AnalysisResult:
    return N18AnalysisResult(
        symbol, passed, reason, structure,
        current.close if current else Decimal("0"),
        str(current.open_time_ms) if current else None,
        checked_at, elapsed_ms, entry_window_ms, consume, rank, record,
        state_records if state_records is not None else ((record,) if record else ()),
    )


def analyze_n18_ascending_triangle_breakout(
    symbol: str, raw_klines: Sequence[Sequence[Any]], *, quote_volume_rank: int,
    fixed_input_bars: int = 122, pivot_left: int = 2, pivot_right: int = 2,
    atr_period: int = 14, ema_fast_period: int = 20, ema_slow_period: int = 50,
    triangle_span_min_bars: int = 12, triangle_span_max_bars: int = 36,
    pressure_interval_min_bars: int = 3,
    pressure_dispersion_atr_max: Decimal = Decimal("0.35"),
    pressure_dispersion_fraction_max: Decimal = Decimal("0.005"),
    a_lower_atr: Decimal = Decimal("0.25"),
    a_upper_atr: Decimal = Decimal("0.10"),
    higher_low_progress_atr_min: Decimal = Decimal("0.20"),
    initial_height_atr_min: Decimal = Decimal("2"),
    convergence_ratio_max: Decimal = Decimal("0.65"),
    a_close_location_min: Decimal = Decimal("0.60"),
    a_taker_buy_ratio_min: Decimal = Decimal("0.50"),
    a_volume_multiple_min: Decimal = Decimal("0.80"),
    breakout_max_bars: int = 4,
    breakout_threshold_atr: Decimal = Decimal("0.10"),
    breakout_body_atr_min: Decimal = Decimal("0.40"),
    breakout_close_location_min: Decimal = Decimal("0.75"),
    breakout_taker_buy_ratio_min: Decimal = Decimal("0.55"),
    breakout_volume_multiple_min: Decimal = Decimal("1.30"),
    entry_extension_atr_max: Decimal = Decimal("0.50"),
    entry_window_seconds: int = 120, checked_at_ms: int | None = None,
    frozen_evidence: str | dict[str, Any] | None = None,
) -> N18AnalysisResult:
    _symbol(symbol)
    config = dict(locals())
    for name in (
        "symbol", "raw_klines", "quote_volume_rank", "checked_at_ms", "frozen_evidence"
    ):
        config.pop(name)
    now_ms = (
        int(datetime.now(timezone.utc).timestamp() * 1000)
        if checked_at_ms is None else checked_at_ms
    )
    checked_at = datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat()
    entry_window_ms = entry_window_seconds * 1000
    if type(now_ms) is not int or type(quote_volume_rank) is not int or not 1 <= quote_volume_rank <= 100:
        raise ValueError("N18 time/rank is invalid")
    if config != _APPROVED_CONFIG:
        return _result(symbol, "N18_DEFINITION_INVALID", None, checked_at, entry_window_ms, quote_volume_rank)
    if len(raw_klines) < fixed_input_bars:
        return _result(symbol, "N18_NOT_ENOUGH_HISTORY", None, checked_at, entry_window_ms, quote_volume_rank)
    try:
        candles = parse_n18_klines(raw_klines[-fixed_input_bars:])
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return _result(symbol, "N18_KLINE_DATA_INVALID", None, checked_at, entry_window_ms, quote_volume_rank)
    current = candles[-1]
    if not _continuous(candles):
        return _result(symbol, "N18_KLINE_SEQUENCE_INVALID", current, checked_at, entry_window_ms, quote_volume_rank)
    if not current.open_time_ms <= now_ms < current.open_time_ms + INTERVAL_MS:
        return _result(symbol, "N18_KLINE_AXIS_NOT_READY", current, checked_at, entry_window_ms, quote_volume_rank)

    prior: N18StateRecord | None = None
    minimum_l1_time: int | None = None
    if frozen_evidence is not None:
        try:
            prior = decode_n18_state_evidence(frozen_evidence, expected_symbol=symbol)
            terminal = prior.stage in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}
            if terminal:
                minimum_l1_time = prior.terminal_cutoff_time_ms
            else:
                frozen = parse_n18_klines([
                    [row["open_time_ms"], row["open"], row["high"], row["low"], row["close"],
                     "0", "0", row["quote_volume"], "0", "0", row["taker_buy_quote_volume"]]
                    for row in prior.evidence["source"]
                ])
                immutable_through = (
                    prior.evidence["structure"]["b"]["open_time_ms"]
                    if prior.evidence["structure"].get("b") is not None
                    else prior.a_open_time_ms
                )
                immutable = [item for item in frozen if item.open_time_ms <= immutable_through]
                if not immutable or immutable[-1].open_time_ms != immutable_through:
                    raise ValueError("N18 frozen immutable source is incomplete")
                live_by_time = {item.open_time_ms: item for item in candles}
                for item in immutable:
                    live = live_by_time.get(item.open_time_ms)
                    if live is not None and live.to_jsonable() != item.to_jsonable():
                        raise ValueError("N18 frozen source conflicts")
                additions = [item for item in candles if item.open_time_ms > immutable_through]
                if additions and additions[0].open_time_ms != immutable_through + INTERVAL_MS:
                    raise ValueError("N18 frozen source gap")
                candles = [replace(item, index=index) for index, item in enumerate(immutable + additions)]
                current = candles[-1]
                if len(candles) > 127:
                    raise ValueError("N18 active episode exceeded bounded horizon")
        except Exception:
            return _result(symbol, "N18_FROZEN_EVIDENCE_INVALID", current, checked_at, entry_window_ms, quote_volume_rank)

    closed = candles[:-1]
    atr = _atr_series(candles, atr_period)
    ema20 = _ema_series(candles, ema_fast_period)
    ema50 = _ema_series(candles, ema_slow_period)
    candidates = _candidate_structures(
        symbol, candles, closed, atr, ema20, ema50, config,
        minimum_l1_time=minimum_l1_time,
    )
    if prior is not None and prior.stage not in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}:
        candidates = [item for item in candidates if item.family_id == prior.family_id]
        if len(candidates) != 1:
            return _result(symbol, "N18_FROZEN_EVIDENCE_INVALID", current, checked_at, entry_window_ms, quote_volume_rank)
    else:
        # First installation may observe the E candle immediately after it has
        # closed.  With B allowed on A+4, A is then six closed candles behind
        # the live boundary.  Keep that bounded family solely to record the
        # permanent historical MISSED terminal; no historical execution is
        # ever exposed below because B is not the last closed candle.
        candidates = [
            item for item in candidates
            if item.a.index >= len(closed) - (breakout_max_bars + 2)
        ]
    if not candidates:
        if prior is not None and prior.stage in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}:
            return _result(symbol, "N18_STRUCTURE_CONSUMED", current, checked_at, entry_window_ms, quote_volume_rank, record=prior)
        return _result(symbol, "N18_NO_ASCENDING_TRIANGLE", current, checked_at, entry_window_ms, quote_volume_rank)
    structure = candidates[0]
    rank = prior.quote_volume_rank if prior and prior.family_id == structure.family_id else quote_volume_rank

    prebroken = any(
        item.close
        > structure.resistance + breakout_threshold_atr * structure.atr_a
        for item in closed[structure.h1.index + 1:structure.a.index]
    )
    absorption_valid = _triangle_thresholds_pass(
        span=structure.a.index - structure.l1.index + 1,
        h1_h2=structure.h2.index - structure.h1.index,
        h2_a=structure.a.index - structure.h2.index,
        dispersion=structure.resistance_dispersion,
        atr_a=structure.atr_a,
        resistance=structure.resistance,
        a=structure.a,
        l1_l2=structure.l1_l2_atr,
        l2_l3=structure.l2_l3_atr,
        initial_height=structure.initial_height_atr,
        convergence=structure.convergence_ratio,
        ema20=structure.ema20_a,
        ema50=structure.ema50_a,
        a_volume_multiple=structure.a_volume_multiple,
        config=config,
    )
    if prebroken or not absorption_valid:
        reason = (
            "N18_PREEXISTING_BREAKOUT"
            if prebroken else "N18_ABSORPTION_NOT_QUALIFIED"
        )
        cutoff = structure.a.open_time_ms + pivot_right * INTERVAL_MS
        record = _record(
            structure, "CONSUMED", reason, rank, candles,
            terminal_cutoff=cutoff,
        )
        return _result(
            symbol, reason, current, checked_at, entry_window_ms, rank,
            structure=structure, record=record, consume=True,
        )

    stage = "ABSORPTION_LOCKED" if len(closed) <= structure.a.index + 1 else "BREAKOUT_PENDING"
    reason = "N18_ABSORPTION_LOCKED" if stage == "ABSORPTION_LOCKED" else "N18_BREAKOUT_WAITING"
    b: N18Candle | None = None
    available = 0
    for index in range(structure.a.index + 1, min(structure.a.index + breakout_max_bars + 1, len(closed))):
        item = closed[index]
        available += 1
        if item.low < structure.l3.low:
            stage, reason = "INVALID", "N18_BREAKOUT_BROKE_L3_LOW"
            break
        atr_b = atr[index]
        if atr_b is None:
            stage, reason = "CONSUMED", "N18_BREAKOUT_ATR_INVALID"
            break
        if item.close > item.open and item.close > structure.resistance + breakout_threshold_atr * atr_b:
            b = item
            volume_multiple = item.quote_volume / _median(
                candle.quote_volume for candle in candles[index - 20:index]
            )
            structure = replace(
                structure, b=item, atr_b=atr_b, b_volume_multiple=volume_multiple,
                structure_id=_structure_id(structure.family_id, item),
            )
            if not _breakout_thresholds_pass(item, atr_b, volume_multiple, config):
                stage, reason = "CONSUMED", "N18_BREAKOUT_NOT_QUALIFIED"
            else:
                structure = replace(
                    structure, entry_min=item.close,
                    entry_max=item.close + entry_extension_atr_max * atr_b,
                )
                stage, reason = "CONFIRMED", "N18_CONFIRMED"
            break
    if b is None and stage in {"ABSORPTION_LOCKED", "BREAKOUT_PENDING"} and available >= breakout_max_bars:
        stage, reason = "CONSUMED", "N18_BREAKOUT_NOT_FOUND"

    if stage != "CONFIRMED":
        record = _record(structure, stage, reason, rank, candles)
        return _result(
            symbol, reason, current, checked_at, entry_window_ms, rank,
            structure=structure, record=record,
            consume=stage in {"CONSUMED", "INVALID"},
        )
    assert structure.b is not None and structure.entry_min is not None and structure.entry_max is not None
    if structure.b.index != len(closed) - 1:
        stage, reason = "MISSED", "N18_HISTORICAL_ENTRY_MISSED"
    else:
        elapsed = now_ms - current.open_time_ms
        structure = replace(structure, entry=current)
        if current.low < structure.l3.low:
            stage, reason = "INVALID", "N18_ENTRY_TRIANGLE_INVALIDATED"
        elif current.low < structure.resistance:
            stage, reason = "INVALID", "N18_ENTRY_BREAKOUT_NOT_HELD"
        elif elapsed >= entry_window_ms:
            stage, reason = "EXPIRED", "N18_ENTRY_WINDOW_EXPIRED"
        elif current.close > structure.entry_max:
            stage, reason = "MISSED", "N18_ENTRY_PRICE_ABOVE_MAX"
        elif current.close < structure.entry_min:
            record = _record(structure, "CONFIRMED", "N18_ENTRY_BELOW_MIN_WAITING", rank, candles)
            return _result(
                symbol, "N18_ENTRY_BELOW_MIN_WAITING", current, checked_at,
                entry_window_ms, rank, structure=structure, record=record,
                elapsed_ms=elapsed,
            )
        else:
            record = _record(structure, "CONFIRMED", "PASSED", rank, candles)
            return _result(
                symbol, "PASSED", current, checked_at, entry_window_ms, rank,
                structure=structure, record=record, elapsed_ms=elapsed, passed=True,
            )
    record = _record(structure, stage, reason, rank, candles)
    return _result(
        symbol, reason, current, checked_at, entry_window_ms, rank,
        structure=structure, record=record, consume=True,
    )
