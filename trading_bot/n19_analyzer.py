from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence

from .exchange_symbol import canonical_exchange_symbol


INTERVAL_MS = 15 * 60 * 1000
N19_SCHEMA_VERSION = 1
N19_RULE_VERSION = "N19_V1"
N19_EVIDENCE_MAX_BYTES = 128 * 1024

N19_TERMINAL_STAGE_REASONS: Mapping[str, frozenset[str]] = {
    "CONSUMED": frozenset({
        "N19_CONFIRMATION_NOT_QUALIFIED",
        "N19_CONFIRMATION_ATR_INVALID",
        "N19_CONFIRMATION_NOT_FOUND",
        "N19_SYSTEMIC_CRASH_VETO",
    }),
    "INVALID": frozenset({
        "N19_CONFIRMATION_BROKE_X_LOW",
        "N19_ENTRY_BROKE_X_LOW",
    }),
    "MISSED": frozenset({
        "N19_HISTORICAL_ENTRY_MISSED",
        "N19_ENTRY_PRICE_ABOVE_MAX",
    }),
    "EXPIRED": frozenset({"N19_ENTRY_WINDOW_EXPIRED"}),
}
_N19_TERMINAL_REASONS = frozenset().union(
    *N19_TERMINAL_STAGE_REASONS.values()
)
_N19_ENTRY_TERMINAL_REASONS = frozenset({
    "N19_ENTRY_BROKE_X_LOW",
    "N19_ENTRY_PRICE_ABOVE_MAX",
    "N19_ENTRY_WINDOW_EXPIRED",
})


def _validate_n19_terminal_stage_reason(stage: Any, reason: Any) -> None:
    expected_reasons = N19_TERMINAL_STAGE_REASONS.get(stage)
    if expected_reasons is not None:
        if type(reason) is not str or reason not in expected_reasons:
            raise ValueError("N19 terminal stage and reason conflict")
    elif reason in _N19_TERMINAL_REASONS:
        raise ValueError("N19 terminal reason requires its canonical stage")


def _validate_n19_evidence_types(value: Any, *, key: str | None = None) -> None:
    """Reject JSON scalar coercions before reproducing frozen evidence."""

    if key in {"schema_version", "quote_volume_rank", "member_count"} or (
        type(key) is str
        and (key.endswith("_time_ms") or key.endswith("_interval_bars"))
    ):
        if value is None and key in {
            "reset_after_time_ms", "terminal_cutoff_time_ms", "c_open_time_ms",
        }:
            return
        if type(value) is not int:
            raise ValueError("N19 evidence integer type is invalid")
        return
    if key == "complete":
        if type(value) is not bool:
            raise ValueError("N19 market context boolean type is invalid")
        return
    if key in {
        "rule_version", "strategy_id", "symbol", "family_id", "stage", "reason",
    }:
        if type(value) is not str:
            raise ValueError("N19 evidence text type is invalid")
        return
    if key == "structure_id":
        if value is not None and type(value) is not str:
            raise ValueError("N19 evidence structure identity type is invalid")
        return
    if type(value) is dict:
        for child_key, child in value.items():
            _validate_n19_evidence_types(child, key=child_key)
    elif type(value) is list:
        for child in value:
            _validate_n19_evidence_types(child)


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
                raise ValueError("N19 JSON contains a duplicate key")
            result[key] = item
        return result

    return json.loads(
        value,
        object_pairs_hook=pairs,
        parse_constant=lambda item: (_ for _ in ()).throw(
            ValueError("N19 JSON contains a non-finite value: %s" % item)
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
        raise ValueError("N19 median input is empty")
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def _symbol(value: Any) -> str:
    try:
        return canonical_exchange_symbol(value)
    except ValueError as exc:
        raise ValueError("N19 symbol is invalid") from exc


@dataclass(frozen=True)
class N19Candle:
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


def _validate_candle(candle: N19Candle) -> None:
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
        raise ValueError("N19 candle semantics are invalid")


def parse_n19_klines(raw_klines: Sequence[Sequence[Any]]) -> list[N19Candle]:
    result: list[N19Candle] = []
    for index, row in enumerate(raw_klines):
        if type(row) not in (list, tuple) or len(row) <= 10:
            raise ValueError("N19 kline requires indexes 0-10")
        open_time = _decimal(row[0])
        if open_time != open_time.to_integral_value():
            raise ValueError("N19 kline time is invalid")
        candle = N19Candle(
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


def _continuous(candles: Sequence[N19Candle]) -> bool:
    return all(
        right.open_time_ms - left.open_time_ms == INTERVAL_MS
        for left, right in zip(candles, candles[1:])
    )


def _atr_series(
    candles: Sequence[N19Candle], period: int
) -> list[Decimal | None]:
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


@dataclass(frozen=True)
class N19Turn:
    kind: str
    candle: N19Candle


def _turns(candles: Sequence[N19Candle]) -> tuple[N19Turn, ...]:
    raw: list[N19Turn] = []
    for index in range(1, len(candles) - 1):
        candle = candles[index]
        if candle.high > candles[index - 1].high and candle.high >= candles[index + 1].high:
            raw.append(N19Turn("HIGH", candle))
        if candle.low < candles[index - 1].low and candle.low <= candles[index + 1].low:
            raw.append(N19Turn("LOW", candle))
    compressed: list[N19Turn] = []
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
class N19MarketContext:
    complete: bool
    c_open_time_ms: int
    member_count: int
    down_breadth: Decimal | None
    median_return: Decimal | None
    rows: tuple[dict[str, Any], ...]
    reason: str

    @property
    def systemic_crash(self) -> bool:
        return bool(
            self.complete
            and self.down_breadth is not None
            and self.median_return is not None
            and self.down_breadth >= Decimal("0.75")
            and self.median_return <= Decimal("-0.01")
        )

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "c_open_time_ms": self.c_open_time_ms,
            "member_count": self.member_count,
            "down_breadth": (
                str(self.down_breadth) if self.down_breadth is not None else None
            ),
            "median_return": (
                str(self.median_return) if self.median_return is not None else None
            ),
            "reason": self.reason,
            "rows": list(self.rows),
        }


def build_n19_market_context(
    c_open_time_ms: int,
    expected_symbols: Sequence[str],
    raw_klines_by_symbol: Mapping[str, Sequence[Sequence[Any]]],
) -> N19MarketContext:
    if (
        type(c_open_time_ms) is not int
        or len(expected_symbols) != 100
        or len(set(expected_symbols)) != 100
    ):
        return N19MarketContext(False, c_open_time_ms, 0, None, None, (), "N19_MARKET_CONTEXT_INSUFFICIENT")
    rows: list[dict[str, Any]] = []
    returns: list[Decimal] = []
    try:
        for symbol in sorted(expected_symbols):
            _symbol(symbol)
            raw = raw_klines_by_symbol.get(symbol)
            if raw is None:
                raise ValueError("missing member")
            candles = parse_n19_klines(raw)
            if not _continuous(candles):
                raise ValueError("member axis gap")
            indexes = [
                index for index, candle in enumerate(candles)
                if candle.open_time_ms == c_open_time_ms
            ]
            if len(indexes) != 1 or indexes[0] < 4:
                raise ValueError("member C axis missing")
            index = indexes[0]
            current_return = candles[index].close / candles[index - 4].close - Decimal("1")
            returns.append(current_return)
            rows.append(
                {
                    "symbol": symbol,
                    "c_open_time_ms": c_open_time_ms,
                    "close": str(candles[index].close),
                    "close_4_bars_ago": str(candles[index - 4].close),
                    "return_1h": str(current_return),
                }
            )
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return N19MarketContext(False, c_open_time_ms, len(rows), None, None, tuple(rows), "N19_MARKET_CONTEXT_INSUFFICIENT")
    breadth = Decimal(sum(item < 0 for item in returns)) / Decimal("100")
    return N19MarketContext(
        True, c_open_time_ms, 100, breadth, _median(returns), tuple(rows), "OK"
    )


@dataclass(frozen=True)
class N19Structure:
    symbol: str
    s: N19Candle
    l1: N19Candle
    r1: N19Candle
    l2: N19Candle
    r2: N19Candle
    x: N19Candle
    c: N19Candle | None
    entry: N19Candle | None
    atr_x: Decimal
    atr_c: Decimal | None
    total_drop_atr: Decimal
    l1_l2_atr: Decimal
    l2_x_atr: Decimal
    s_r1_atr: Decimal
    r1_r2_atr: Decimal
    rebound_1: Decimal
    rebound_2: Decimal
    max_bearish_body_fraction: Decimal
    x_volume_ratio: Decimal
    x_taker_improvement: Decimal
    c_volume_multiple: Decimal | None
    entry_min: Decimal | None
    entry_max: Decimal | None
    family_id: str
    structure_id: str | None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "family_id": self.family_id,
            "structure_id": self.structure_id,
            "s": self.s.to_jsonable(), "l1": self.l1.to_jsonable(),
            "r1": self.r1.to_jsonable(), "l2": self.l2.to_jsonable(),
            "r2": self.r2.to_jsonable(), "x": self.x.to_jsonable(),
            "c": self.c.to_jsonable() if self.c else None,
            "entry": self.entry.to_jsonable() if self.entry else None,
            "atr_x": str(self.atr_x),
            "atr_c": str(self.atr_c) if self.atr_c is not None else None,
            "total_drop_atr": str(self.total_drop_atr),
            "l1_l2_atr": str(self.l1_l2_atr),
            "l2_x_atr": str(self.l2_x_atr),
            "s_r1_atr": str(self.s_r1_atr),
            "r1_r2_atr": str(self.r1_r2_atr),
            "rebound_1": str(self.rebound_1),
            "rebound_2": str(self.rebound_2),
            "max_bearish_body_fraction": str(self.max_bearish_body_fraction),
            "x_volume_ratio": str(self.x_volume_ratio),
            "x_taker_improvement": str(self.x_taker_improvement),
            "c_volume_multiple": (
                str(self.c_volume_multiple) if self.c_volume_multiple is not None else None
            ),
            "entry_min": str(self.entry_min) if self.entry_min is not None else None,
            "entry_max": str(self.entry_max) if self.entry_max is not None else None,
        }


@dataclass(frozen=True)
class N19StateRecord:
    strategy_id: str
    symbol: str
    family_id: str
    structure_id: str | None
    stage: str
    reason: str
    quote_volume_rank: int
    s_open_time_ms: int
    x_open_time_ms: int
    reset_after_time_ms: int | None
    evidence: dict[str, Any]

    @property
    def evidence_json(self) -> str:
        return _canonical_json(self.evidence)

    @property
    def evidence_sha256(self) -> str:
        return _sha256_json(self.evidence)


@dataclass(frozen=True)
class N19AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    structure: N19Structure | None
    current_price: Decimal
    current_open_time: str | None
    checked_at: str
    elapsed_ms: int | None
    entry_window_ms: int
    consume_current: bool
    quote_volume_rank: int | None
    state_record: N19StateRecord | None
    market_context: N19MarketContext | None
    state_records: tuple[N19StateRecord, ...] = ()

    @property
    def structure_id(self) -> str | None:
        if self.structure is not None:
            return self.structure.structure_id
        return self.state_record.structure_id if self.state_record else None

    @property
    def current_bullish(self) -> bool:
        return bool(self.structure and self.structure.entry and self.structure.entry.close > self.structure.entry.open)

    def detail_json(self) -> dict[str, Any]:
        payload = {
            "schema_version": N19_SCHEMA_VERSION,
            "rule_version": N19_RULE_VERSION,
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
        if self.market_context:
            payload["market_context"] = {
                "complete": self.market_context.complete,
                "member_count": self.market_context.member_count,
                "down_breadth": str(self.market_context.down_breadth),
                "median_return": str(self.market_context.median_return),
            }
        encoded = _canonical_json(payload).encode("utf-8")
        if len(encoded) > 16 * 1024:
            raise ValueError("N19 signal detail exceeds 16KiB")
        return payload


_APPROVED_CONFIG = {
    "fixed_input_bars": 122, "pivot_left": 1, "pivot_right": 1,
    "atr_period": 14, "structure_min_bars": 10, "structure_max_bars": 19,
    "total_drop_atr_min": Decimal("2"), "total_drop_atr_max": Decimal("6"),
    "lower_low_progress_atr_min": Decimal("0.35"),
    "exhaustion_extension_atr_max": Decimal("0.50"),
    "lower_high_progress_atr_min": Decimal("0.20"),
    "rebound_ratio_min": Decimal("0.20"), "rebound_ratio_max": Decimal("0.55"),
    "max_bearish_body_drop_fraction": Decimal("0.40"),
    "exhaustion_range_atr_max": Decimal("0.90"),
    "exhaustion_volume_ratio_max": Decimal("0.85"),
    "exhaustion_taker_buy_ratio_min": Decimal("0.45"),
    "exhaustion_taker_buy_improvement_min": Decimal("0.08"),
    "confirmation_max_bars": 3,
    "confirmation_close_location_min": Decimal("0.70"),
    "confirmation_taker_buy_ratio_min": Decimal("0.55"),
    "confirmation_volume_multiple_min": Decimal("0.80"),
    "crash_down_breadth_min": Decimal("0.75"),
    "crash_median_return_max": Decimal("-0.01"),
    "entry_extension_atr_max": Decimal("0.50"), "entry_window_seconds": 120,
}


def _family_id(symbol: str, points: Sequence[N19Candle]) -> str:
    payload = "|".join(["N19", symbol] + [str(item.open_time_ms) for item in points])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _structure_id(family_id: str, c: N19Candle) -> str:
    return hashlib.sha256(
        ("N19|" + family_id + "|" + str(c.open_time_ms)).encode("utf-8")
    ).hexdigest()[:24]


def _source(candles: Sequence[N19Candle]) -> list[dict[str, Any]]:
    return [item.to_jsonable() for item in candles]


def _structure_thresholds_pass(
    *,
    bars: int,
    total_atr: Decimal,
    l1_l2: Decimal,
    l2_x: Decimal,
    s_r1: Decimal,
    r1_r2: Decimal,
    rebound_1: Decimal,
    rebound_2: Decimal,
    max_body: Decimal,
    x_range: Decimal,
    atr_x: Decimal,
    x_volume_ratio: Decimal,
    x_taker_ratio: Decimal,
    x_taker_improvement: Decimal,
    config: Mapping[str, Any],
) -> bool:
    """Pure frozen-threshold gate shared by production and boundary tests."""

    return bool(
        config["structure_min_bars"] <= bars <= config["structure_max_bars"]
        and config["total_drop_atr_min"] <= total_atr <= config["total_drop_atr_max"]
        and l1_l2 >= config["lower_low_progress_atr_min"]
        and Decimal("0") < l2_x <= config["exhaustion_extension_atr_max"]
        and s_r1 >= config["lower_high_progress_atr_min"]
        and r1_r2 >= config["lower_high_progress_atr_min"]
        and config["rebound_ratio_min"] <= rebound_1 <= config["rebound_ratio_max"]
        and config["rebound_ratio_min"] <= rebound_2 <= config["rebound_ratio_max"]
        and max_body <= config["max_bearish_body_drop_fraction"]
        and x_range <= config["exhaustion_range_atr_max"] * atr_x
        and x_volume_ratio <= config["exhaustion_volume_ratio_max"]
        and x_taker_ratio >= config["exhaustion_taker_buy_ratio_min"]
        and x_taker_improvement >= config["exhaustion_taker_buy_improvement_min"]
    )


def _confirmation_thresholds_pass(
    candle: N19Candle,
    x: N19Candle,
    volume_multiple: Decimal,
    config: Mapping[str, Any],
) -> bool:
    """Return whether the already locked first bullish breakout qualifies."""

    return bool(
        candle.low >= x.low
        and candle.close_location >= config["confirmation_close_location_min"]
        and candle.taker_buy_ratio >= config["confirmation_taker_buy_ratio_min"]
        and volume_multiple >= config["confirmation_volume_multiple_min"]
    )


def _record(
    structure: N19Structure, stage: str, reason: str, rank: int,
    source: Sequence[N19Candle], context: N19MarketContext | None,
    reset_after: int | None = None,
) -> N19StateRecord:
    terminal_stages = {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}
    terminal_cutoff_time_ms = None
    if stage in terminal_stages:
        if structure.entry is not None:
            terminal_cutoff_time_ms = structure.entry.open_time_ms
        elif structure.c is not None:
            terminal_cutoff_time_ms = structure.c.open_time_ms
        else:
            if len(source) < 2:
                raise ValueError("N19 terminal cutoff source is incomplete")
            terminal_cutoff_time_ms = source[-2].open_time_ms
    unsigned = {
        "schema_version": N19_SCHEMA_VERSION,
        "rule_version": N19_RULE_VERSION,
        "strategy_id": "N19",
        "symbol": structure.symbol,
        "family_id": structure.family_id,
        "structure_id": structure.structure_id,
        "stage": stage,
        "reason": reason,
        "quote_volume_rank": rank,
        "reset_after_time_ms": reset_after,
        "terminal_cutoff_time_ms": terminal_cutoff_time_ms,
        "structure": structure.to_jsonable(),
        "source": _source(source),
        "market_context": context.to_jsonable() if context else None,
    }
    evidence = dict(unsigned)
    evidence["canonical_sha256"] = _sha256_json(unsigned)
    if len(_canonical_json(evidence).encode("utf-8")) > N19_EVIDENCE_MAX_BYTES:
        raise ValueError("N19 evidence exceeds 128KiB")
    return N19StateRecord(
        "N19", structure.symbol, structure.family_id, structure.structure_id,
        stage, reason, rank, structure.s.open_time_ms, structure.x.open_time_ms,
        reset_after, evidence,
    )


def decode_n19_state_evidence(
    value: str | dict[str, Any], *, expected_symbol: str | None = None
) -> N19StateRecord:
    parsed = _strict_json_loads(value) if type(value) is str else value
    if type(parsed) is not dict:
        raise ValueError("N19 evidence shape is invalid")
    unsigned = dict(parsed)
    claimed = unsigned.pop("canonical_sha256", None)
    _validate_n19_evidence_types(unsigned)
    expected = {
        "schema_version", "rule_version", "strategy_id", "symbol", "family_id",
        "structure_id", "stage", "reason", "quote_volume_rank",
        "reset_after_time_ms", "terminal_cutoff_time_ms", "structure",
        "source", "market_context",
    }
    reset_evidence = unsigned.get("reset_evidence")
    if reset_evidence is not None:
        expected.add("reset_evidence")
    if (
        set(unsigned) != expected
        or claimed != _sha256_json(unsigned)
        or type(unsigned["schema_version"]) is not int
        or unsigned["schema_version"] != N19_SCHEMA_VERSION
        or unsigned["rule_version"] != N19_RULE_VERSION
        or unsigned["strategy_id"] != "N19"
        or type(unsigned["symbol"]) is not str
        or (expected_symbol is not None and unsigned["symbol"] != expected_symbol)
        or type(unsigned["family_id"]) is not str
        or len(unsigned["family_id"]) != 24
        or type(unsigned["stage"]) is not str
        or type(unsigned["reason"]) is not str
        or type(unsigned["quote_volume_rank"]) is not int
        or not 1 <= unsigned["quote_volume_rank"] <= 100
        or (unsigned["structure_id"] is not None and (
            type(unsigned["structure_id"]) is not str or len(unsigned["structure_id"]) != 24
        ))
        or type(unsigned["source"]) is not list
        or not unsigned["source"]
    ):
        raise ValueError("N19 evidence identity is invalid")
    _validate_n19_terminal_stage_reason(unsigned["stage"], unsigned["reason"])
    source = parse_n19_klines([
        [row["open_time_ms"], row["open"], row["high"], row["low"], row["close"],
         "0", "0", row["quote_volume"], "0", "0", row["taker_buy_quote_volume"]]
        for row in unsigned["source"]
    ])
    if not _continuous(source):
        raise ValueError("N19 evidence source is discontinuous")
    structure = unsigned["structure"]
    if type(structure) is not dict:
        raise ValueError("N19 evidence structure is invalid")
    structure_keys = {
        "family_id", "structure_id", "s", "l1", "r1", "l2", "r2", "x",
        "c", "atr_x", "atr_c", "total_drop_atr", "l1_l2_atr",
        "l2_x_atr", "s_r1_atr", "r1_r2_atr", "rebound_1",
        "rebound_2", "max_bearish_body_fraction", "x_volume_ratio",
        "x_taker_improvement", "c_volume_multiple", "entry_min", "entry_max",
    }
    if (
        set(structure) not in (structure_keys, structure_keys | {"entry"})
        or structure.get("family_id") != unsigned["family_id"]
        or structure.get("structure_id") != unsigned["structure_id"]
    ):
        raise ValueError("N19 evidence structure identity is invalid")
    points = [structure[name] for name in ("s", "l1", "r1", "l2", "r2", "x")]
    if any(type(item) is not dict for item in points):
        raise ValueError("N19 evidence points are invalid")
    source_by_time = {
        item.open_time_ms: item.to_jsonable()
        for item in source
    }
    bound_points = points + [
        item
        for item in (structure.get("c"), structure.get("entry"))
        if item is not None
    ]
    if any(
        type(item) is not dict
        or type(item.get("open_time_ms")) is not int
        or source_by_time.get(item["open_time_ms"]) != item
        for item in bound_points
    ):
        raise ValueError("N19 evidence point conflicts with frozen source")
    terminal_stages = {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}
    terminal_cutoff = unsigned["terminal_cutoff_time_ms"]
    reset_after = unsigned["reset_after_time_ms"]
    structure_c = structure.get("c")
    structure_entry = structure.get("entry")
    expected_terminal_cutoff = None
    if unsigned["stage"] in terminal_stages:
        if unsigned["reason"] in _N19_ENTRY_TERMINAL_REASONS:
            if type(structure_entry) is not dict:
                raise ValueError("N19 entry terminal evidence is incomplete")
            expected_terminal_cutoff = structure_entry.get("open_time_ms")
        elif unsigned["reason"] == "N19_HISTORICAL_ENTRY_MISSED":
            expected_terminal_cutoff = (
                structure_c.get("open_time_ms")
                if type(structure_c) is dict
                else None
            )
        elif structure_entry is not None:
            raise ValueError("N19 pre-entry terminal evidence has an entry")
        elif type(structure_c) is dict:
            expected_terminal_cutoff = structure_c.get("open_time_ms")
        elif len(source) >= 2:
            expected_terminal_cutoff = source[-2].open_time_ms
    if (
        (
            unsigned["stage"] in terminal_stages
            and (
                type(terminal_cutoff) is not int
                or terminal_cutoff <= points[-1]["open_time_ms"]
                or terminal_cutoff != expected_terminal_cutoff
            )
        )
        or (
            unsigned["stage"] not in terminal_stages
            and (terminal_cutoff is not None or reset_after is not None)
        )
        or (
            reset_after is not None
            and (
                type(reset_after) is not int
                or type(terminal_cutoff) is not int
                or reset_after <= terminal_cutoff
            )
        )
    ):
        raise ValueError("N19 terminal cutoff is invalid")
    if reset_after is not None:
        reset_candles = [
            item for item in source if item.open_time_ms == reset_after
        ]
        if reset_evidence is None:
            if (
                len(reset_candles) != 1
                or reset_candles[0].close <= _decimal(structure["r2"]["high"])
            ):
                raise ValueError("N19 reset evidence is invalid")
        else:
            if (
                type(reset_evidence) is not dict
                or set(reset_evidence) != {
                    "mode", "reset_open_time_ms", "source", "source_sha256",
                }
                or reset_evidence["mode"] != "DISCONNECTED_WINDOW_V1"
                or type(reset_evidence["reset_open_time_ms"]) is not int
                or reset_evidence["reset_open_time_ms"] != reset_after
                or type(reset_evidence["source"]) is not list
                or len(reset_evidence["source"]) != 121
                or type(reset_evidence["source_sha256"]) is not str
                or reset_evidence["source_sha256"]
                != _sha256_json(reset_evidence["source"])
            ):
                raise ValueError("N19 disconnected reset evidence is invalid")
            reset_source = parse_n19_klines([
                [row["open_time_ms"], row["open"], row["high"], row["low"], row["close"],
                 "0", "0", row["quote_volume"], "0", "0", row["taker_buy_quote_volume"]]
                for row in reset_evidence["source"]
            ])
            first_reset = next(
                (
                    item for item in reset_source
                    if item.open_time_ms > terminal_cutoff
                    and item.close > _decimal(structure["r2"]["high"])
                ),
                None,
            )
            if (
                not _continuous(reset_source)
                or reset_source[0].open_time_ms <= terminal_cutoff
                or reset_source[0].open_time_ms
                < source[-1].open_time_ms + INTERVAL_MS
                or first_reset is None
                or first_reset.open_time_ms != reset_after
            ):
                raise ValueError("N19 disconnected reset evidence is invalid")
    elif reset_evidence is not None:
        raise ValueError("N19 disconnected reset evidence is unexpected")
    if _family_id(unsigned["symbol"], [
        next(item for item in source if item.open_time_ms == point["open_time_ms"])
        for point in points
    ]) != unsigned["family_id"]:
        raise ValueError("N19 family identity conflicts")
    c = structure.get("c")
    if c is not None:
        c_candle = next(item for item in source if item.open_time_ms == c["open_time_ms"])
        if _structure_id(unsigned["family_id"], c_candle) != unsigned["structure_id"]:
            raise ValueError("N19 structure identity conflicts")
    return N19StateRecord(
        "N19", unsigned["symbol"], unsigned["family_id"], unsigned["structure_id"],
        unsigned["stage"], unsigned["reason"], unsigned["quote_volume_rank"],
        points[0]["open_time_ms"], points[-1]["open_time_ms"],
        unsigned["reset_after_time_ms"], parsed,
    )


def _validated_historical_terminal_source(
    prior: N19StateRecord,
    value: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Allow only the frozen live tail to advance into its closed form."""

    prior_source = prior.evidence.get("source")
    structure = prior.evidence.get("structure")
    c = structure.get("c") if type(structure) is dict else None
    if (
        type(prior_source) is not list
        or len(prior_source) < 2
        or type(value) is not list
        or len(value) != len(prior_source)
        or type(c) is not dict
        or type(c.get("open_time_ms")) is not int
    ):
        raise ValueError("N19 historical terminal source is invalid")
    try:
        frozen = parse_n19_klines([
            [
                row["open_time_ms"], row["open"], row["high"], row["low"],
                row["close"], "0", "0", row["quote_volume"], "0", "0",
                row["taker_buy_quote_volume"],
            ]
            for row in prior_source
        ])
        terminal = parse_n19_klines([
            [
                row["open_time_ms"], row["open"], row["high"], row["low"],
                row["close"], "0", "0", row["quote_volume"], "0", "0",
                row["taker_buy_quote_volume"],
            ]
            for row in value
        ])
    except (ArithmeticError, InvalidOperation, KeyError, TypeError, ValueError) as exc:
        raise ValueError("N19 historical terminal source is invalid") from exc
    if (
        not _continuous(frozen)
        or not _continuous(terminal)
        or _source(frozen) != prior_source
        or _source(terminal) != value
        or value[:-1] != prior_source[:-1]
        or frozen[-1].open_time_ms != c["open_time_ms"] + INTERVAL_MS
        or terminal[-1].open_time_ms != frozen[-1].open_time_ms
        or terminal[-1].open != frozen[-1].open
    ):
        raise ValueError("N19 historical terminal source conflicts")
    if terminal[-1].to_jsonable() != frozen[-1].to_jsonable():
        quote_delta = terminal[-1].quote_volume - frozen[-1].quote_volume
        taker_delta = (
            terminal[-1].taker_buy_quote_volume
            - frozen[-1].taker_buy_quote_volume
        )
        if (
            terminal[-1].high < frozen[-1].high
            or terminal[-1].low > frozen[-1].low
            or quote_delta <= 0
            or taker_delta < 0
            or taker_delta > quote_delta
        ):
            raise ValueError("N19 frozen live tail evolution is invalid")
    return value


def _record_with_validated_closed_entry(
    prior: N19StateRecord,
    terminal_source: list[dict[str, Any]],
) -> N19StateRecord:
    """Advance only the authenticated live entry tail into its closed form."""

    source = _validated_historical_terminal_source(prior, terminal_source)
    prior_source = prior.evidence.get("source")
    structure = prior.evidence.get("structure")
    if type(prior_source) is not list or type(structure) is not dict:
        raise ValueError("N19 closed entry evidence is invalid")
    entry = structure.get("entry")
    evidence = dict(prior.evidence)
    evidence["source"] = source
    if entry is not None:
        if type(entry) is not dict or entry != prior_source[-1]:
            raise ValueError("N19 frozen entry conflicts with its source")
        closed_structure = dict(structure)
        closed_structure["entry"] = dict(source[-1])
        evidence["structure"] = closed_structure
    unsigned = dict(evidence)
    unsigned.pop("canonical_sha256", None)
    evidence["canonical_sha256"] = _sha256_json(unsigned)
    record = replace(prior, evidence=evidence)
    if decode_n19_state_evidence(
        record.evidence_json,
        expected_symbol=prior.symbol,
    ) != record:
        raise ValueError("N19 closed entry evidence is not canonical")
    return record


def _historical_missed_record_from_confirmed(
    prior: N19StateRecord,
    terminal_source: list[dict[str, Any]] | None = None,
) -> N19StateRecord:
    """Derive the only safe terminal state after a confirmed E is historical."""

    if prior.stage != "CONFIRMED" or prior.structure_id is None:
        raise ValueError("N19 historical terminal source is not confirmed")
    structure = prior.evidence.get("structure")
    if type(structure) is not dict:
        raise ValueError("N19 historical terminal structure is invalid")
    c = structure.get("c")
    if type(c) is not dict or type(c.get("open_time_ms")) is not int:
        raise ValueError("N19 historical terminal C identity is invalid")
    source_record = (
        _record_with_validated_closed_entry(prior, terminal_source)
        if terminal_source is not None
        else prior
    )
    evidence = dict(source_record.evidence)
    evidence["stage"] = "MISSED"
    evidence["reason"] = "N19_HISTORICAL_ENTRY_MISSED"
    evidence["terminal_cutoff_time_ms"] = c["open_time_ms"]
    evidence["reset_after_time_ms"] = None
    unsigned = dict(evidence)
    unsigned.pop("canonical_sha256", None)
    evidence["canonical_sha256"] = _sha256_json(unsigned)
    if len(_canonical_json(evidence).encode("utf-8")) > N19_EVIDENCE_MAX_BYTES:
        raise ValueError("N19 historical terminal evidence exceeds 128KiB")
    record = replace(
        prior,
        stage="MISSED",
        reason="N19_HISTORICAL_ENTRY_MISSED",
        reset_after_time_ms=None,
        evidence=evidence,
    )
    decoded = decode_n19_state_evidence(
        record.evidence_json,
        expected_symbol=prior.symbol,
    )
    if decoded != record:
        raise ValueError("N19 historical terminal evidence is not canonical")
    return record


def _disconnected_reset_record(
    prior: N19StateRecord,
    candles: Sequence[N19Candle],
) -> N19StateRecord | None:
    """Prove one reset from a complete, independently continuous window."""

    if (
        prior.stage not in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}
        or prior.reset_after_time_ms is not None
        or len(candles) != 122
        or not _continuous(candles)
    ):
        return None
    terminal_cutoff = prior.evidence.get("terminal_cutoff_time_ms")
    structure = prior.evidence.get("structure")
    source = prior.evidence.get("source")
    if (
        type(terminal_cutoff) is not int
        or type(structure) is not dict
        or type(structure.get("r2")) is not dict
        or type(source) is not list
        or not source
        or type(source[-1]) is not dict
        or type(source[-1].get("open_time_ms")) is not int
    ):
        raise ValueError("N19 disconnected reset source is invalid")
    closed = list(candles[:-1])
    if (
        len(closed) != 121
        or closed[0].open_time_ms <= terminal_cutoff
        or closed[0].open_time_ms
        < source[-1]["open_time_ms"] + INTERVAL_MS
    ):
        return None
    r2_high = _decimal(structure["r2"]["high"])
    reset = next(
        (
            item for item in closed
            if item.open_time_ms > terminal_cutoff and item.close > r2_high
        ),
        None,
    )
    if reset is None:
        return None
    reset_source = _source(closed)
    reset_evidence = {
        "mode": "DISCONNECTED_WINDOW_V1",
        "reset_open_time_ms": reset.open_time_ms,
        "source": reset_source,
        "source_sha256": _sha256_json(reset_source),
    }
    evidence = dict(prior.evidence)
    evidence["reset_after_time_ms"] = reset.open_time_ms
    evidence["reset_evidence"] = reset_evidence
    unsigned = dict(evidence)
    unsigned.pop("canonical_sha256", None)
    evidence["canonical_sha256"] = _sha256_json(unsigned)
    if len(_canonical_json(evidence).encode("utf-8")) > N19_EVIDENCE_MAX_BYTES:
        raise ValueError("N19 disconnected reset evidence exceeds 128KiB")
    record = replace(
        prior,
        reset_after_time_ms=reset.open_time_ms,
        evidence=evidence,
    )
    if decode_n19_state_evidence(
        record.evidence_json,
        expected_symbol=prior.symbol,
    ) != record:
        raise ValueError("N19 disconnected reset evidence is not canonical")
    return record


def _result(
    symbol: str, reason: str, current: N19Candle | None, checked_at: str,
    entry_window_ms: int, rank: int | None, *, structure: N19Structure | None = None,
    record: N19StateRecord | None = None, context: N19MarketContext | None = None,
    elapsed_ms: int | None = None, passed: bool = False, consume: bool = False,
    state_records: tuple[N19StateRecord, ...] | None = None,
) -> N19AnalysisResult:
    return N19AnalysisResult(
        symbol, passed, reason, structure,
        current.close if current else Decimal("0"),
        str(current.open_time_ms) if current else None,
        checked_at, elapsed_ms, entry_window_ms, consume, rank, record, context,
        (
            state_records
            if state_records is not None
            else ((record,) if record is not None else ())
        ),
    )


def _candidate_structures(
    symbol: str, candles: Sequence[N19Candle], closed: Sequence[N19Candle],
    atr: Sequence[Decimal | None], config: Mapping[str, Any],
    *, minimum_s_time: int | None = None,
) -> list[N19Structure]:
    result: list[N19Structure] = []
    turns = _turns(closed)
    for offset in range(len(turns) - 4):
        window = turns[offset : offset + 5]
        if tuple(item.kind for item in window) != ("HIGH", "LOW", "HIGH", "LOW", "HIGH"):
            continue
        s, l1, r1, l2, r2 = [item.candle for item in window]
        if minimum_s_time is not None and s.open_time_ms <= minimum_s_time:
            continue
        x = next((item for item in closed[r2.index + 1 :] if item.low < l2.low), None)
        if x is None:
            continue
        bars = x.index - s.index + 1
        atr_x = atr[x.index]
        if atr_x is None:
            continue
        if not (l1.low > l2.low > x.low and s.high > r1.high > r2.high):
            continue
        total_drop = s.high - x.low
        if total_drop <= 0:
            continue
        total_atr = total_drop / atr_x
        l1_l2 = (l1.low - l2.low) / atr_x
        l2_x = (l2.low - x.low) / atr_x
        s_r1 = (s.high - r1.high) / atr_x
        r1_r2 = (r1.high - r2.high) / atr_x
        rebound_1 = (r1.high - l1.low) / (s.high - l1.low)
        rebound_2 = (r2.high - l2.low) / (r1.high - l2.low)
        max_body = max(
            (item.open - item.close for item in closed[s.index : x.index + 1] if item.close < item.open),
            default=Decimal("0"),
        ) / total_drop
        tail = list(closed[max(r1.index + 1, l2.index - 2) : l2.index + 1])
        if not tail:
            continue
        volume_median = _median(item.quote_volume for item in tail)
        taker_median = _median(item.taker_buy_ratio for item in tail)
        x_volume_ratio = x.quote_volume / volume_median
        x_taker_improvement = x.taker_buy_ratio - taker_median
        if not _structure_thresholds_pass(
            bars=bars,
            total_atr=total_atr,
            l1_l2=l1_l2,
            l2_x=l2_x,
            s_r1=s_r1,
            r1_r2=r1_r2,
            rebound_1=rebound_1,
            rebound_2=rebound_2,
            max_body=max_body,
            x_range=x.range,
            atr_x=atr_x,
            x_volume_ratio=x_volume_ratio,
            x_taker_ratio=x.taker_buy_ratio,
            x_taker_improvement=x_taker_improvement,
            config=config,
        ):
            continue
        family = _family_id(symbol, (s, l1, r1, l2, r2, x))
        result.append(N19Structure(
            symbol, s, l1, r1, l2, r2, x, None, None, atr_x, None,
            total_atr, l1_l2, l2_x, s_r1, r1_r2, rebound_1, rebound_2,
            max_body, x_volume_ratio, x_taker_improvement, None, None, None,
            family, None,
        ))
    return sorted(result, key=lambda item: (item.s.open_time_ms, item.x.open_time_ms, item.family_id))


def analyze_n19_staircase_exhaustion_reversal(
    symbol: str,
    raw_klines: Sequence[Sequence[Any]],
    *,
    quote_volume_rank: int,
    market_symbols: Sequence[str] = (),
    market_klines_by_symbol: Mapping[str, Sequence[Sequence[Any]]] | None = None,
    fixed_input_bars: int = 122,
    pivot_left: int = 1,
    pivot_right: int = 1,
    atr_period: int = 14,
    structure_min_bars: int = 10,
    structure_max_bars: int = 19,
    total_drop_atr_min: Decimal = Decimal("2"),
    total_drop_atr_max: Decimal = Decimal("6"),
    lower_low_progress_atr_min: Decimal = Decimal("0.35"),
    exhaustion_extension_atr_max: Decimal = Decimal("0.50"),
    lower_high_progress_atr_min: Decimal = Decimal("0.20"),
    rebound_ratio_min: Decimal = Decimal("0.20"),
    rebound_ratio_max: Decimal = Decimal("0.55"),
    max_bearish_body_drop_fraction: Decimal = Decimal("0.40"),
    exhaustion_range_atr_max: Decimal = Decimal("0.90"),
    exhaustion_volume_ratio_max: Decimal = Decimal("0.85"),
    exhaustion_taker_buy_ratio_min: Decimal = Decimal("0.45"),
    exhaustion_taker_buy_improvement_min: Decimal = Decimal("0.08"),
    confirmation_max_bars: int = 3,
    confirmation_close_location_min: Decimal = Decimal("0.70"),
    confirmation_taker_buy_ratio_min: Decimal = Decimal("0.55"),
    confirmation_volume_multiple_min: Decimal = Decimal("0.80"),
    crash_down_breadth_min: Decimal = Decimal("0.75"),
    crash_median_return_max: Decimal = Decimal("-0.01"),
    entry_extension_atr_max: Decimal = Decimal("0.50"),
    entry_window_seconds: int = 120,
    checked_at_ms: int | None = None,
    frozen_evidence: str | dict[str, Any] | None = None,
    market_context_cache: dict[int, N19MarketContext] | None = None,
) -> N19AnalysisResult:
    _symbol(symbol)
    config = dict(locals())
    for name in (
        "symbol",
        "raw_klines",
        "quote_volume_rank",
        "market_symbols",
        "market_klines_by_symbol",
        "checked_at_ms",
        "frozen_evidence",
        "market_context_cache",
    ):
        config.pop(name)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000) if checked_at_ms is None else checked_at_ms
    checked_at = datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat()
    entry_window_ms = entry_window_seconds * 1000
    if type(now_ms) is not int or type(quote_volume_rank) is not int or not 1 <= quote_volume_rank <= 100:
        raise ValueError("N19 time/rank is invalid")
    if config != _APPROVED_CONFIG:
        return _result(symbol, "N19_DEFINITION_INVALID", None, checked_at, entry_window_ms, quote_volume_rank)
    if len(raw_klines) < fixed_input_bars:
        return _result(symbol, "N19_NOT_ENOUGH_HISTORY", None, checked_at, entry_window_ms, quote_volume_rank)
    try:
        candles = parse_n19_klines(raw_klines[-fixed_input_bars:])
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return _result(symbol, "N19_KLINE_DATA_INVALID", None, checked_at, entry_window_ms, quote_volume_rank)
    current = candles[-1]
    if not _continuous(candles):
        return _result(symbol, "N19_KLINE_SEQUENCE_INVALID", current, checked_at, entry_window_ms, quote_volume_rank)
    if not current.open_time_ms <= now_ms < current.open_time_ms + INTERVAL_MS:
        return _result(symbol, "N19_KLINE_AXIS_NOT_READY", current, checked_at, entry_window_ms, quote_volume_rank)

    prior: N19StateRecord | None = None
    prior_reset_record: N19StateRecord | None = None
    minimum_s_time: int | None = None
    if frozen_evidence is not None:
        try:
            prior = decode_n19_state_evidence(frozen_evidence, expected_symbol=symbol)
            terminal_stages = {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}
            source = prior.evidence["source"]
            frozen = parse_n19_klines([
                [row["open_time_ms"], row["open"], row["high"], row["low"], row["close"],
                 "0", "0", row["quote_volume"], "0", "0", row["taker_buy_quote_volume"]]
                for row in source
            ])
            structure_payload = prior.evidence["structure"]
            c_payload = structure_payload.get("c")
            immutable_through = (
                int(c_payload["open_time_ms"])
                if type(c_payload) is dict
                else prior.x_open_time_ms
            )
            frozen_immutable = [
                item for item in frozen
                if item.open_time_ms <= immutable_through
            ]
            if (
                not frozen_immutable
                or frozen_immutable[-1].open_time_ms != immutable_through
            ):
                raise ValueError("frozen immutable source is incomplete")
            live_by_time = {item.open_time_ms: item for item in candles}
            for item in frozen_immutable:
                live = live_by_time.get(item.open_time_ms)
                if live is not None and live.to_jsonable() != item.to_jsonable():
                    raise ValueError("frozen immutable source conflicts")
            if prior.stage == "CONFIRMED":
                if (
                    type(c_payload) is not dict
                    or type(c_payload.get("open_time_ms")) is not int
                ):
                    raise ValueError("confirmed frozen C identity is invalid")
                if candles[-2].open_time_ms > c_payload["open_time_ms"]:
                    terminal_source = list(source)
                    frozen_tail = frozen[-1]
                    closed_tail = next(
                        (
                            item for item in candles[:-1]
                            if item.open_time_ms == frozen_tail.open_time_ms
                        ),
                        None,
                    )
                    if closed_tail is not None:
                        terminal_source[-1] = closed_tail.to_jsonable()
                    historical = _historical_missed_record_from_confirmed(
                        prior,
                        terminal_source,
                    )
                    return _result(
                        symbol,
                        historical.reason,
                        current,
                        checked_at,
                        entry_window_ms,
                        historical.quote_volume_rank,
                        record=historical,
                        consume=True,
                    )
            reset_prior = prior
            if (
                prior.stage in terminal_stages
                and prior.reason in _N19_ENTRY_TERMINAL_REASONS
                and prior.reset_after_time_ms is None
            ):
                entry_payload = structure_payload.get("entry")
                frozen_tail = frozen[-1]
                if (
                    type(entry_payload) is not dict
                    or type(entry_payload.get("open_time_ms")) is not int
                    or frozen_tail.open_time_ms
                    != entry_payload["open_time_ms"]
                ):
                    raise ValueError(
                        "N19 frozen live entry tail identity is invalid"
                    )
                closed_tail = next(
                    (
                        item for item in candles[:-1]
                        if item.open_time_ms == frozen_tail.open_time_ms
                    ),
                    None,
                )
                if closed_tail is not None:
                    terminal_source = list(source)
                    terminal_source[-1] = closed_tail.to_jsonable()
                    reset_prior = _record_with_validated_closed_entry(
                        prior,
                        terminal_source,
                    )
            if prior.stage in terminal_stages and prior.reset_after_time_ms is not None:
                minimum_s_time = prior.reset_after_time_ms
            else:
                additions = [
                    item for item in candles
                    if item.open_time_ms > immutable_through
                ]
                if prior.stage in terminal_stages:
                    # A terminal family is permanent evidence, not an active
                    # history-coverage requirement.  A later bounded window
                    # may no longer overlap the immutable C/X tail; without a
                    # continuous suffix no reset can be proven, so preserve
                    # the terminal record byte-for-byte and keep the family
                    # consumed.  Active families still take the strict gap
                    # path below.
                    if (
                        additions
                        and additions[0].open_time_ms
                        != immutable_through + INTERVAL_MS
                    ):
                        prior_reset_record = _disconnected_reset_record(
                            reset_prior,
                            candles,
                        )
                        if prior_reset_record is None:
                            return _result(
                                symbol,
                                "N19_STRUCTURE_CONSUMED",
                                current,
                                checked_at,
                                entry_window_ms,
                                prior.quote_volume_rank,
                                record=prior,
                            )
                        minimum_s_time = (
                            prior_reset_record.reset_after_time_ms
                        )
                        current = candles[-1]
                        additions = []
                    combined = frozen_immutable + additions
                    if len(combined) > 126:
                        prior_reset_record = _disconnected_reset_record(
                            reset_prior,
                            candles,
                        )
                        if prior_reset_record is None:
                            return _result(
                                symbol,
                                "N19_STRUCTURE_CONSUMED",
                                current,
                                checked_at,
                                entry_window_ms,
                                prior.quote_volume_rank,
                                record=prior,
                            )
                        minimum_s_time = (
                            prior_reset_record.reset_after_time_ms
                        )
                    elif prior_reset_record is None:
                        observed_current = current
                        candles = [
                            replace(item, index=index)
                            for index, item in enumerate(combined)
                        ]
                        r2_high = _decimal(
                            reset_prior.evidence["structure"]["r2"]["high"]
                        )
                        terminal_cutoff = reset_prior.evidence[
                            "terminal_cutoff_time_ms"
                        ]
                        if type(terminal_cutoff) is not int:
                            raise ValueError("terminal cutoff is invalid")
                        reset = next(
                            (
                                item for item in candles[:-1]
                                if item.open_time_ms > terminal_cutoff
                                and item.close > r2_high
                            ),
                            None,
                        )
                        if reset is None:
                            return _result(
                                symbol,
                                "N19_STRUCTURE_CONSUMED",
                                observed_current,
                                checked_at,
                                entry_window_ms,
                                prior.quote_volume_rank,
                                record=prior,
                            )
                        updated_evidence = dict(reset_prior.evidence)
                        updated_evidence["reset_after_time_ms"] = reset.open_time_ms
                        updated_evidence["source"] = _source(candles)
                        unsigned = dict(updated_evidence)
                        unsigned.pop("canonical_sha256", None)
                        updated_evidence["canonical_sha256"] = _sha256_json(unsigned)
                        if len(_canonical_json(updated_evidence).encode("utf-8")) > N19_EVIDENCE_MAX_BYTES:
                            raise ValueError("N19 reset evidence exceeds 128KiB")
                        prior_reset_record = replace(
                            reset_prior,
                            reset_after_time_ms=reset.open_time_ms,
                            evidence=updated_evidence,
                        )
                        decode_n19_state_evidence(
                            prior_reset_record.evidence_json,
                            expected_symbol=symbol,
                        )
                        minimum_s_time = reset.open_time_ms
                        current = observed_current
                else:
                    if (
                        additions
                        and additions[0].open_time_ms
                        != immutable_through + INTERVAL_MS
                    ):
                        raise ValueError("frozen source gap")
                    candles = [
                        replace(item, index=index)
                        for index, item in enumerate(
                            frozen_immutable + additions
                        )
                    ]
                    current = candles[-1]
                    if len(candles) > 126:
                        raise ValueError("active episode exceeded bounded horizon")
        except Exception:
            return _result(symbol, "N19_FROZEN_EVIDENCE_INVALID", current, checked_at, entry_window_ms, quote_volume_rank)

    closed = candles[:-1]
    atr = _atr_series(candles, atr_period)
    candidates = _candidate_structures(symbol, candles, closed, atr, config, minimum_s_time=minimum_s_time)
    if prior is not None and prior.stage not in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}:
        candidates = [item for item in candidates if item.family_id == prior.family_id]
        if len(candidates) != 1:
            return _result(symbol, "N19_FROZEN_EVIDENCE_INVALID", current, checked_at, entry_window_ms, quote_volume_rank)
    else:
        candidates = [item for item in candidates if item.x.index >= len(closed) - 4]
    if not candidates:
        if prior_reset_record is not None:
            return _result(
                symbol, "N19_STRUCTURE_CONSUMED", current, checked_at,
                entry_window_ms, quote_volume_rank, record=prior_reset_record,
            )
        return _result(symbol, "N19_NO_STAIRCASE_EXHAUSTION", current, checked_at, entry_window_ms, quote_volume_rank)
    structure = candidates[0]
    rank = prior.quote_volume_rank if prior and prior.family_id == structure.family_id else quote_volume_rank

    c: N19Candle | None = None
    stage, reason = "CONFIRMATION_PENDING", "N19_CONFIRMATION_WAITING"
    available = 0
    for index in range(structure.x.index + 1, min(structure.x.index + confirmation_max_bars + 1, len(closed))):
        item = closed[index]
        available += 1
        if item.low < structure.x.low:
            stage, reason = "INVALID", "N19_CONFIRMATION_BROKE_X_LOW"
            break
        if item.close > item.open and item.close > structure.x.high:
            c = item
            volume20 = _median(previous.quote_volume for previous in candles[index - 20 : index])
            c_multiple = item.quote_volume / volume20
            atr_c = atr[index]
            structure = N19Structure(**{
                **structure.__dict__,
                "c": item,
                "atr_c": atr_c,
                "c_volume_multiple": c_multiple,
                "structure_id": _structure_id(structure.family_id, item),
            })
            if not _confirmation_thresholds_pass(
                item,
                structure.x,
                c_multiple,
                config,
            ):
                stage, reason = "CONSUMED", "N19_CONFIRMATION_NOT_QUALIFIED"
            else:
                if atr_c is None:
                    stage, reason = "CONSUMED", "N19_CONFIRMATION_ATR_INVALID"
                else:
                    structure = N19Structure(**{
                        **structure.__dict__,
                        "entry_min": item.close,
                        "entry_max": item.close + entry_extension_atr_max * atr_c,
                    })
                    stage, reason = "CONFIRMED", "N19_CONFIRMED"
            break
    if c is None and stage == "CONFIRMATION_PENDING" and available >= confirmation_max_bars:
        stage, reason = "CONSUMED", "N19_CONFIRMATION_NOT_FOUND"

    context: N19MarketContext | None = None
    if stage == "CONFIRMED" and structure.c is not None:
        context = (
            market_context_cache.get(structure.c.open_time_ms)
            if market_context_cache is not None
            else None
        )
        if context is None:
            context = build_n19_market_context(
                structure.c.open_time_ms, market_symbols,
                market_klines_by_symbol or {},
            )
            if market_context_cache is not None:
                market_context_cache[structure.c.open_time_ms] = context
        if not context.complete:
            reason = "N19_MARKET_CONTEXT_INSUFFICIENT"
        elif (
            context.down_breadth is not None
            and context.median_return is not None
            and context.down_breadth >= crash_down_breadth_min
            and context.median_return <= crash_median_return_max
        ):
            stage, reason = "CONSUMED", "N19_SYSTEMIC_CRASH_VETO"

    if stage != "CONFIRMED" or reason == "N19_MARKET_CONTEXT_INSUFFICIENT":
        record = _record(structure, stage, reason, rank, candles, context)
        return _result(symbol, reason, current, checked_at, entry_window_ms, rank, structure=structure, record=record, context=context, consume=stage in {"CONSUMED", "INVALID"}, state_records=((prior_reset_record,) if prior_reset_record is not None else ()) + (record,))

    assert structure.c is not None and structure.entry_min is not None and structure.entry_max is not None
    if structure.c.index != len(closed) - 1:
        stage, reason = "MISSED", "N19_HISTORICAL_ENTRY_MISSED"
    else:
        elapsed = now_ms - current.open_time_ms
        structure = N19Structure(**{**structure.__dict__, "entry": current})
        if current.low < structure.x.low:
            stage, reason = "INVALID", "N19_ENTRY_BROKE_X_LOW"
        elif elapsed >= entry_window_ms:
            stage, reason = "EXPIRED", "N19_ENTRY_WINDOW_EXPIRED"
        elif current.close > structure.entry_max:
            stage, reason = "MISSED", "N19_ENTRY_PRICE_ABOVE_MAX"
        elif current.close < structure.entry_min:
            record = _record(structure, "CONFIRMED", "N19_ENTRY_BELOW_MIN_WAITING", rank, candles, context)
            return _result(symbol, "N19_ENTRY_BELOW_MIN_WAITING", current, checked_at, entry_window_ms, rank, structure=structure, record=record, context=context, elapsed_ms=elapsed, state_records=((prior_reset_record,) if prior_reset_record is not None else ()) + (record,))
        else:
            record = _record(structure, "CONFIRMED", "PASSED", rank, candles, context)
            return _result(symbol, "PASSED", current, checked_at, entry_window_ms, rank, structure=structure, record=record, context=context, elapsed_ms=elapsed, passed=True, state_records=((prior_reset_record,) if prior_reset_record is not None else ()) + (record,))
    record = _record(structure, stage, reason, rank, candles, context)
    return _result(symbol, reason, current, checked_at, entry_window_ms, rank, structure=structure, record=record, context=context, consume=True, state_records=((prior_reset_record,) if prior_reset_record is not None else ()) + (record,))
