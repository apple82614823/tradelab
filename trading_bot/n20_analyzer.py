from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import zlib
from typing import Any, Iterable, Mapping, Sequence

from .exchange_symbol import canonical_exchange_symbol


INTERVAL_MS = 15 * 60 * 1000
N20_SCHEMA_VERSION = 1
N20_RULE_VERSION = "N20_V1"
N20_CANONICAL_MAX_BYTES = 8 * 1024 * 1024
N20_COMPRESSED_MAX_BYTES = 2 * 1024 * 1024


def _validate_n20_evidence_types(value: Any, *, key: str | None = None) -> None:
    """Reject bool/float coercions in permanent N20 integer identities."""

    if key in {"schema_version", "rank", "member_count"} or (
        type(key) is str and key.endswith("_time_ms")
    ):
        if value is None and key in {
            "c_open_time_ms", "terminal_cutoff_time_ms",
        }:
            return
        if type(value) is not int:
            raise ValueError("N20 evidence integer type is invalid")
        return
    if key in {
        "rule_version", "strategy_id", "symbol", "stage", "reason",
        "episode_id", "winner_symbol",
    }:
        if value is not None and type(value) is not str:
            raise ValueError("N20 evidence text type is invalid")
        return
    if key == "structure_id":
        if value is not None and type(value) is not str:
            raise ValueError("N20 structure identity type is invalid")
        return
    if type(value) is dict:
        for child_key, child in value.items():
            _validate_n20_evidence_types(child, key=child_key)
    elif type(value) is list:
        for child in value:
            _validate_n20_evidence_types(child)


class _N20FrozenSourceConflict(ValueError):
    pass


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("N20 JSON contains a duplicate key")
        result[key] = value
    return result


def _strict_loads(value: str) -> Any:
    if type(value) is not str:
        raise ValueError("N20 JSON must be text")
    return json.loads(
        value,
        object_pairs_hook=_pairs,
        parse_float=Decimal,
        parse_constant=lambda token: (_ for _ in ()).throw(
            ValueError("N20 JSON contains a non-finite number: %s" % token)
        ),
    )


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    )


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def compress_n20_evidence(value: Mapping[str, Any]) -> tuple[bytes, int, str]:
    if type(value) is not dict:
        raise ValueError("N20 evidence must be a built-in dictionary")
    raw = canonical_json(value).encode("utf-8")
    if len(raw) > N20_CANONICAL_MAX_BYTES:
        raise ValueError("N20 canonical evidence exceeds 8MiB")
    blob = zlib.compress(raw, level=9)
    if len(blob) > N20_COMPRESSED_MAX_BYTES:
        raise ValueError("N20 compressed evidence exceeds 2MiB")
    return blob, len(raw), hashlib.sha256(raw).hexdigest()


def decompress_n20_evidence(
    blob: bytes, expected_size: int, expected_sha256: str,
) -> dict[str, Any]:
    if (
        type(blob) is not bytes or not 0 < len(blob) <= N20_COMPRESSED_MAX_BYTES
        or type(expected_size) is not int
        or not 0 < expected_size <= N20_CANONICAL_MAX_BYTES
        or type(expected_sha256) is not str or len(expected_sha256) != 64
    ):
        raise ValueError("N20 compressed envelope is invalid")
    decoder = zlib.decompressobj()
    try:
        raw = decoder.decompress(blob, N20_CANONICAL_MAX_BYTES + 1)
        raw += decoder.flush()
    except zlib.error as exc:
        raise ValueError("N20 compressed evidence is invalid") from exc
    if (
        not decoder.eof or decoder.unused_data or decoder.unconsumed_tail
        or len(raw) != expected_size or len(raw) > N20_CANONICAL_MAX_BYTES
        or hashlib.sha256(raw).hexdigest() != expected_sha256
    ):
        raise ValueError("N20 compressed evidence identity conflicts")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("N20 canonical evidence is not UTF-8") from exc
    value = _strict_loads(text)
    try:
        canonical = canonical_json(value).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("N20 canonical evidence scalar type is invalid") from exc
    if type(value) is not dict or canonical != raw:
        raise ValueError("N20 canonical evidence is not canonical")
    return value


def _d(value: Any) -> Decimal:
    if type(value) is bool:
        raise ValueError("N20 decimal cannot be bool")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("N20 decimal is not finite")
    return result


def _median(values: Iterable[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("N20 median source is empty")
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2


@dataclass(frozen=True)
class N20Candle:
    index: int
    open_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    base_volume: Decimal
    quote_volume: Decimal
    taker_buy_quote_volume: Decimal

    @property
    def taker_ratio(self) -> Decimal:
        return self.taker_buy_quote_volume / self.quote_volume

    @property
    def close_location(self) -> Decimal:
        span = self.high - self.low
        return (self.close - self.low) / span if span > 0 else Decimal("0")

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "open_time_ms": self.open_time_ms, "open": str(self.open),
            "high": str(self.high), "low": str(self.low), "close": str(self.close),
            "base_volume": str(self.base_volume), "quote_volume": str(self.quote_volume),
            "taker_buy_quote_volume": str(self.taker_buy_quote_volume),
        }


def parse_n20_klines(raw: Sequence[Sequence[Any]]) -> list[N20Candle]:
    result: list[N20Candle] = []
    for index, row in enumerate(raw):
        if type(row) not in (list, tuple) or len(row) <= 10:
            raise ValueError("N20 kline row shape is invalid")
        open_time = int(_d(row[0]))
        if _d(row[0]) != Decimal(open_time) or open_time <= 0:
            raise ValueError("N20 open time is invalid")
        item = N20Candle(
            index, open_time, _d(row[1]), _d(row[2]), _d(row[3]), _d(row[4]),
            _d(row[5]), _d(row[7]), _d(row[10]),
        )
        if (
            item.open <= 0 or item.low <= 0 or item.high < item.low
            or not item.low <= item.open <= item.high
            or not item.low <= item.close <= item.high
            or item.base_volume <= 0 or item.quote_volume <= 0
            or not Decimal("0") <= item.taker_buy_quote_volume <= item.quote_volume
        ):
            raise ValueError("N20 kline values are invalid")
        result.append(item)
    if any(
        right.open_time_ms != left.open_time_ms + INTERVAL_MS
        for left, right in zip(result, result[1:])
    ):
        raise ValueError("N20 kline axis is not continuous")
    return result


_N20_SOURCE_ROW_KEYS = {
    "open_time_ms", "open", "high", "low", "close", "base_volume",
    "quote_volume", "taker_buy_quote_volume",
}


def _parse_n20_evidence_source_rows(value: Any) -> list[N20Candle]:
    if type(value) is not list or not value:
        raise ValueError("N20 evidence source rows are invalid")
    result: list[N20Candle] = []
    for index, row in enumerate(value):
        if (
            type(row) is not dict
            or set(row) != _N20_SOURCE_ROW_KEYS
            or type(row["open_time_ms"]) is not int
        ):
            raise ValueError("N20 evidence source row shape is invalid")
        item = N20Candle(
            index, row["open_time_ms"], _d(row["open"]), _d(row["high"]),
            _d(row["low"]), _d(row["close"]), _d(row["base_volume"]),
            _d(row["quote_volume"]), _d(row["taker_buy_quote_volume"]),
        )
        if (
            item.open_time_ms <= 0 or item.open <= 0 or item.low <= 0
            or item.high < item.low or not item.low <= item.open <= item.high
            or not item.low <= item.close <= item.high
            or item.base_volume <= 0 or item.quote_volume <= 0
            or not Decimal("0") <= item.taker_buy_quote_volume <= item.quote_volume
            or item.to_jsonable() != row
        ):
            raise ValueError("N20 evidence source row values are invalid")
        result.append(item)
    if any(
        right.open_time_ms != left.open_time_ms + INTERVAL_MS
        for left, right in zip(result, result[1:])
    ):
        raise ValueError("N20 evidence source axis is not continuous")
    return result


def validate_n20_source_progression(
    existing_source: Any, updated_source: Any,
) -> None:
    """Prove that every frozen member source is immutable and append-only."""

    if (
        type(existing_source) is not dict or type(updated_source) is not dict
        or set(existing_source) != set(updated_source)
    ):
        raise ValueError("N20 evidence source membership conflicts")
    for symbol in existing_source:
        try:
            canonical_exchange_symbol(symbol)
        except ValueError as exc:
            raise ValueError("N20 evidence source symbol is invalid") from exc
        existing_rows = existing_source[symbol]
        updated_rows = updated_source[symbol]
        _parse_n20_evidence_source_rows(existing_rows)
        _parse_n20_evidence_source_rows(updated_rows)
        if (
            len(updated_rows) < len(existing_rows)
            or updated_rows[:len(existing_rows)] != existing_rows
        ):
            raise ValueError("N20 evidence source is not an exact append-only prefix")


def _merge_n20_frozen_source(
    prior_source: Any,
    current_candles: Mapping[str, Sequence[N20Candle]],
) -> dict[str, list[dict[str, Any]]]:
    if type(prior_source) is not dict or set(prior_source) != set(current_candles):
        raise _N20FrozenSourceConflict("N20 frozen source membership conflicts")
    merged: dict[str, list[dict[str, Any]]] = {}
    try:
        for symbol, items in current_candles.items():
            prior_rows = prior_source[symbol]
            prior_candles = _parse_n20_evidence_source_rows(prior_rows)
            current_rows = [item.to_jsonable() for item in items]
            current_by_time = {row["open_time_ms"]: row for row in current_rows}
            prior_last_time = prior_candles[-1].open_time_ms
            if (
                len(current_by_time) != len(current_rows)
                or prior_last_time not in current_by_time
                or items[-1].open_time_ms < prior_last_time
            ):
                raise ValueError("N20 frozen source overlap is missing")
            prior_by_time = {row["open_time_ms"]: row for row in prior_rows}
            for open_time, row in current_by_time.items():
                if open_time <= prior_last_time and prior_by_time.get(open_time) != row:
                    raise ValueError("N20 frozen source overlap conflicts")
            appended = [
                row for row in current_rows if row["open_time_ms"] > prior_last_time
            ]
            updated_rows = list(prior_rows) + appended
            validate_n20_source_progression(
                {symbol: prior_rows}, {symbol: updated_rows},
            )
            merged[symbol] = updated_rows
    except Exception as exc:
        raise _N20FrozenSourceConflict("N20 frozen source identity conflicts") from exc
    return merged


def _atr(candles: Sequence[N20Candle], period: int = 14) -> list[Decimal | None]:
    values: list[Decimal | None] = [None] * len(candles)
    true_ranges: list[Decimal] = []
    for index, candle in enumerate(candles):
        tr = candle.high - candle.low if index == 0 else max(
            candle.high - candle.low,
            abs(candle.high - candles[index - 1].close),
            abs(candle.low - candles[index - 1].close),
        )
        true_ranges.append(tr)
        if index == period - 1:
            values[index] = sum(true_ranges[:period], Decimal("0")) / period
        elif index >= period:
            previous = values[index - 1]
            assert previous is not None
            values[index] = (previous * (period - 1) + tr) / period
    return values


def _vwap(candles: Sequence[N20Candle]) -> Decimal:
    base = sum((item.base_volume for item in candles), Decimal("0"))
    quote = sum((item.quote_volume for item in candles), Decimal("0"))
    if base <= 0 or quote <= 0:
        raise ValueError("N20 VWAP source is invalid")
    return quote / base


@dataclass(frozen=True)
class N20Winner:
    symbol: str
    quote_volume_rank: int
    baseline_rank: int
    resilience_rank: int
    recovery_ratio: Decimal
    relative_resilience: Decimal
    taker_ratio: Decimal
    p: Decimal
    p_open_time_ms: int
    c: N20Candle
    entry: N20Candle
    atr_c: Decimal
    entry_min: Decimal
    entry_max: Decimal
    episode_id: str
    structure_id: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol, "quote_volume_rank": self.quote_volume_rank,
            "baseline_rank": self.baseline_rank, "resilience_rank": self.resilience_rank,
            "recovery_ratio": str(self.recovery_ratio),
            "relative_resilience": str(self.relative_resilience),
            "taker_ratio": str(self.taker_ratio), "p": str(self.p),
            "p_open_time_ms": self.p_open_time_ms, "c": self.c.to_jsonable(),
            "entry": self.entry.to_jsonable(), "atr_c": str(self.atr_c),
            "entry_min": str(self.entry_min), "entry_max": str(self.entry_max),
            "episode_id": self.episode_id, "structure_id": self.structure_id,
        }


@dataclass(frozen=True)
class N20StateRecord:
    strategy_id: str
    episode_id: str
    stage: str
    reason: str
    m0_open_time_ms: int
    d1_open_time_ms: int
    c_open_time_ms: int | None
    winner_symbol: str | None
    structure_id: str | None
    terminal_cutoff_time_ms: int | None
    evidence: dict[str, Any]

    @property
    def encoded(self) -> tuple[bytes, int, str]:
        return compress_n20_evidence(self.evidence)


@dataclass(frozen=True)
class N20AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    winner: N20Winner | None
    current_price: Decimal
    current_open_time: str | None
    checked_at: str
    elapsed_ms: int | None
    entry_window_ms: int
    consume_current: bool
    quote_volume_rank: int | None
    state_record: N20StateRecord | None
    candidate_detail: dict[str, Any] | None = None
    state_evidence_sha256: str | None = None

    @property
    def structure_id(self) -> str | None:
        return self.winner.structure_id if self.winner and self.symbol == self.winner.symbol else None

    @property
    def current_bullish(self) -> bool:
        return bool(self.winner and self.winner.entry.close > self.winner.entry.open)

    def detail_json(self) -> dict[str, Any]:
        payload = {
            "schema_version": N20_SCHEMA_VERSION, "rule_version": N20_RULE_VERSION,
            "reason": self.reason, "structure_id": self.structure_id,
            "current_price": str(self.current_price),
            "current_open_time": self.current_open_time, "checked_at": self.checked_at,
            "elapsed_ms": self.elapsed_ms, "entry_window_ms": self.entry_window_ms,
            "consume_current": self.consume_current,
            "quote_volume_rank": self.quote_volume_rank,
            "episode_id": self.state_record.episode_id if self.state_record else None,
            "episode_evidence_sha256": (
                (
                    self.state_evidence_sha256
                    if self.state_evidence_sha256 is not None
                    else self.state_record.encoded[2]
                )
                if self.state_record else None
            ),
            "candidate": self.candidate_detail,
            "winner": self.winner.to_jsonable() if self.winner else None,
        }
        if len(canonical_json(payload).encode("utf-8")) > 16 * 1024:
            raise ValueError("N20 signal detail exceeds 16KiB")
        return payload


@dataclass(frozen=True)
class N20MarketAnalysis:
    complete: bool
    reason: str
    results: dict[str, N20AnalysisResult]
    state_record: N20StateRecord | None
    frozen_symbols: tuple[str, ...]


_APPROVED_CONFIG = {
    "fixed_input_bars": 122, "atr_period": 14, "vwap_long_period": 96,
    "vwap_short_period": 20, "baseline_rank_max": 20,
    "resilience_rank_max": 20, "m0_positive_breadth_min": Decimal("0.65"),
    "m0_above_vwap_breadth_min": Decimal("0.60"),
    "d1_down_breadth_min": Decimal("0.55"),
    "d1_median_return_max": Decimal("-0.0015"), "pullback_min_bars": 2,
    "pullback_max_bars": 6, "pullback_depth_min": Decimal("0.004"),
    "pullback_depth_max": Decimal("0.025"),
    "crash_down_breadth_min": Decimal("0.80"),
    "crash_depth_min": Decimal("0.030"),
    "recovery_up_breadth_min": Decimal("0.55"),
    "recovery_breadth_improvement_min": Decimal("0.20"),
    "relative_resilience_min": Decimal("0.50"),
    "candidate_drawdown_atr_max": Decimal("1.50"),
    "candidate_recovery_ratio_min": Decimal("0.50"),
    "candidate_close_location_min": Decimal("0.65"),
    "candidate_taker_buy_ratio_min": Decimal("0.52"),
    "candidate_volume_multiple_min": Decimal("0.80"),
    "entry_extension_atr_max": Decimal("0.50"), "entry_window_seconds": 120,
}

_APPROVED_EVIDENCE_CONFIG = {
    key: str(value) if type(value) is Decimal else value
    for key, value in _APPROVED_CONFIG.items()
}

N20_STAGE_REASONS = {
    "PULLBACK_ACTIVE": frozenset({"N20_PULLBACK_ACTIVE"}),
    "RECOVERY_FROZEN": frozenset({"N20_RECOVERY_FROZEN"}),
    "ENTRY_WAITING": frozenset({"N20_ENTRY_BELOW_MIN_WAITING"}),
    "CONFIRMED": frozenset({"PASSED"}),
    "CONSUMED": frozenset({
        "N20_PULLBACK_TOO_DEEP", "N20_NO_QUALIFIED_LEADER",
        "N20_EPISODE_CONSUMED",
    }),
    "MISSED": frozenset({
        "N20_HISTORICAL_ENTRY_MISSED", "N20_ENTRY_PRICE_ABOVE_MAX",
    }),
    "INVALID": frozenset({"N20_ENTRY_BROKE_P"}),
    "CRASH_VETO": frozenset({"N20_SYSTEMIC_CRASH_VETO"}),
    "EXPIRED": frozenset({
        "N20_PULLBACK_WINDOW_EXPIRED", "N20_ENTRY_WINDOW_EXPIRED",
    }),
}


def validate_n20_stage_reason(stage: Any, reason: Any) -> None:
    if (
        type(stage) is not str or type(reason) is not str
        or reason not in N20_STAGE_REASONS.get(stage, ())
    ):
        raise ValueError("N20 stage and reason are inconsistent")


def _validate_n20_evidence_config(value: Any) -> None:
    if type(value) is not dict or set(value) != set(_APPROVED_EVIDENCE_CONFIG):
        raise ValueError("N20 evidence config shape is invalid")
    for key, expected in _APPROVED_EVIDENCE_CONFIG.items():
        actual = value[key]
        if type(expected) is int:
            if type(actual) is not int or actual != expected:
                raise ValueError("N20 evidence integer config is invalid")
        elif (
            type(expected) is not str or type(actual) is not str
            or actual != expected
        ):
            raise ValueError("N20 evidence decimal config is invalid")


_N20_MARKET_KEYS = {
    "m0_positive_breadth", "m0_above_vwap_breadth", "d1_down_breadth",
    "d1_median_return", "timeline",
}
_N20_TIMELINE_KEYS = {
    "open_time_ms", "pullback_bars", "depth", "down_breadth",
    "up_breadth", "prior_up_breadth", "breadth_improvement",
    "median_step_return",
}
_N20_CANDIDATE_KEYS = {
    "symbol", "quote_volume_rank", "baseline_rank", "resilience_rank",
    "return96", "vwap96", "normalized_pullback_return",
    "relative_resilience", "drawdown_atr", "recovery_ratio", "vwap20",
    "close_location", "taker_ratio", "volume_multiple", "p",
    "p_open_time_ms", "c_open_time_ms",
}
_N20_WINNER_KEYS = {
    "symbol", "quote_volume_rank", "baseline_rank", "resilience_rank",
    "recovery_ratio", "relative_resilience", "taker_ratio", "p",
    "p_open_time_ms", "c", "entry", "atr_c", "entry_min", "entry_max",
    "episode_id", "structure_id",
}


def _require_n20_decimal_text(value: Any) -> Decimal:
    if type(value) is not str:
        raise ValueError("N20 decimal evidence must be canonical text")
    parsed = _d(value)
    if str(parsed) != value:
        raise ValueError("N20 decimal evidence is not canonical")
    return parsed


def _validate_n20_evidence_shapes(value: Mapping[str, Any]) -> None:
    members = value["members"]
    if any(type(item) is not dict or set(item) != {"symbol", "rank"} for item in members):
        raise ValueError("N20 member evidence shape is invalid")
    market = value["market"]
    if type(market) is not dict or set(market) != _N20_MARKET_KEYS:
        raise ValueError("N20 market evidence shape is invalid")
    for key in _N20_MARKET_KEYS - {"timeline"}:
        _require_n20_decimal_text(market[key])
    timeline = market["timeline"]
    if type(timeline) is not list:
        raise ValueError("N20 market timeline is invalid")
    prior_time = None
    for row in timeline:
        if type(row) is not dict or set(row) != _N20_TIMELINE_KEYS:
            raise ValueError("N20 market timeline row shape is invalid")
        if (
            type(row["open_time_ms"]) is not int
            or type(row["pullback_bars"]) is not int
            or not 1 <= row["pullback_bars"] <= _APPROVED_CONFIG["pullback_max_bars"]
            or (prior_time is not None and row["open_time_ms"] != prior_time + INTERVAL_MS)
        ):
            raise ValueError("N20 market timeline identity is invalid")
        prior_time = row["open_time_ms"]
        for key in _N20_TIMELINE_KEYS - {"open_time_ms", "pullback_bars"}:
            _require_n20_decimal_text(row[key])
    candidates = value["qualified_candidates"]
    if type(candidates) is not list:
        raise ValueError("N20 qualified candidates are invalid")
    for item in candidates:
        if type(item) is not dict or set(item) != _N20_CANDIDATE_KEYS:
            raise ValueError("N20 candidate evidence shape is invalid")
        try:
            canonical_exchange_symbol(item["symbol"])
        except ValueError as exc:
            raise ValueError("N20 candidate symbol is invalid") from exc
        for key in ("quote_volume_rank", "baseline_rank", "resilience_rank"):
            if type(item[key]) is not int or not 1 <= item[key] <= 100:
                raise ValueError("N20 candidate rank is invalid")
        for key in ("p_open_time_ms", "c_open_time_ms"):
            if type(item[key]) is not int or item[key] <= 0:
                raise ValueError("N20 candidate time is invalid")
        for key in _N20_CANDIDATE_KEYS - {
            "symbol", "quote_volume_rank", "baseline_rank", "resilience_rank",
            "p_open_time_ms", "c_open_time_ms",
        }:
            _require_n20_decimal_text(item[key])
    winner = value["winner"]
    if winner is None:
        if value["winner_symbol"] is not None or value["structure_id"] is not None:
            raise ValueError("N20 absent winner identity conflicts")
    else:
        if type(winner) is not dict or set(winner) != _N20_WINNER_KEYS:
            raise ValueError("N20 winner evidence shape is invalid")
        if (
            canonical_exchange_symbol(winner["symbol"])
            != value["winner_symbol"]
            or type(winner["structure_id"]) is not str
            or winner["structure_id"] != value["structure_id"]
            or winner["episode_id"] != value["episode_id"]
        ):
            raise ValueError("N20 winner identity conflicts")
        for key in ("quote_volume_rank", "baseline_rank", "resilience_rank"):
            if type(winner[key]) is not int or not 1 <= winner[key] <= 100:
                raise ValueError("N20 winner rank is invalid")
        if type(winner["p_open_time_ms"]) is not int or winner["p_open_time_ms"] <= 0:
            raise ValueError("N20 winner P time is invalid")
        for key in (
            "recovery_ratio", "relative_resilience", "taker_ratio", "p", "atr_c",
            "entry_min", "entry_max",
        ):
            _require_n20_decimal_text(winner[key])
        for key in ("c", "entry"):
            _parse_n20_evidence_source_rows([winner[key]])
        if (
            type(value["c_open_time_ms"]) is not int
            or winner["c"]["open_time_ms"] != value["c_open_time_ms"]
            or winner["entry"]["open_time_ms"] != value["c_open_time_ms"] + INTERVAL_MS
            or winner["structure_id"] != _structure_id(
                value["episode_id"], winner["symbol"],
                winner["p_open_time_ms"], value["c_open_time_ms"],
            )
            or not any(
                item["symbol"] == winner["symbol"]
                and item["c_open_time_ms"] == value["c_open_time_ms"]
                and item["p_open_time_ms"] == winner["p_open_time_ms"]
                for item in candidates
            )
        ):
            raise ValueError("N20 winner derivation conflicts")
    reason = value["reason"]
    winner_required = {
        "PASSED", "N20_ENTRY_BELOW_MIN_WAITING",
        "N20_HISTORICAL_ENTRY_MISSED", "N20_ENTRY_PRICE_ABOVE_MAX",
        "N20_ENTRY_BROKE_P", "N20_ENTRY_WINDOW_EXPIRED",
        "N20_EPISODE_CONSUMED",
    }
    no_winner = {
        "N20_PULLBACK_ACTIVE", "N20_RECOVERY_FROZEN",
        "N20_PULLBACK_TOO_DEEP", "N20_NO_QUALIFIED_LEADER",
        "N20_SYSTEMIC_CRASH_VETO", "N20_PULLBACK_WINDOW_EXPIRED",
    }
    if (reason in winner_required and winner is None) or (
        reason in no_winner and winner is not None
    ):
        raise ValueError("N20 stage evidence shape conflicts")


def _context_thresholds_pass(
    *, positive_breadth: Decimal, above_vwap_breadth: Decimal,
    d1_down_breadth: Decimal, d1_median_return: Decimal,
    config: Mapping[str, Any],
) -> bool:
    return config == _APPROVED_CONFIG and (
        positive_breadth >= config["m0_positive_breadth_min"]
        and above_vwap_breadth >= config["m0_above_vwap_breadth_min"]
        and d1_down_breadth >= config["d1_down_breadth_min"]
        and d1_median_return <= config["d1_median_return_max"]
    )


def _recovery_thresholds_pass(
    *, up_breadth: Decimal, breadth_improvement: Decimal,
    median_step_return: Decimal, pullback_bars: int,
    config: Mapping[str, Any],
) -> bool:
    return config == _APPROVED_CONFIG and (
        config["pullback_min_bars"] <= pullback_bars <= config["pullback_max_bars"]
        and up_breadth >= config["recovery_up_breadth_min"]
        and breadth_improvement >= config["recovery_breadth_improvement_min"]
        and median_step_return > 0
    )


def _candidate_thresholds_pass(
    *, baseline_rank: int, return96: Decimal, above_vwap96: bool,
    relative_resilience: Decimal, drawdown_atr: Decimal,
    resilience_rank: int, bullish: bool, broke_prior_high: bool,
    above_vwap20: bool, recovery_ratio: Decimal,
    close_location: Decimal, taker_ratio: Decimal,
    volume_multiple: Decimal, config: Mapping[str, Any],
) -> bool:
    return config == _APPROVED_CONFIG and (
        baseline_rank <= config["baseline_rank_max"] and return96 > 0
        and above_vwap96
        and relative_resilience >= config["relative_resilience_min"]
        and drawdown_atr <= config["candidate_drawdown_atr_max"]
        and resilience_rank <= config["resilience_rank_max"]
        and bullish and broke_prior_high and above_vwap20
        and recovery_ratio >= config["candidate_recovery_ratio_min"]
        and close_location >= config["candidate_close_location_min"]
        and taker_ratio >= config["candidate_taker_buy_ratio_min"]
        and volume_multiple >= config["candidate_volume_multiple_min"]
    )


def _pullback_classification(
    depth: Decimal, down_breadth: Decimal, config: Mapping[str, Any],
) -> str:
    if config != _APPROVED_CONFIG:
        return "INVALID"
    if depth > config["pullback_depth_max"]:
        if (
            down_breadth >= config["crash_down_breadth_min"]
            and depth >= config["crash_depth_min"]
        ):
            return "CRASH"
        return "TOO_DEEP"
    if depth < config["pullback_depth_min"]:
        return "WAIT"
    return "ELIGIBLE"


def _episode_id(m0: int, d1: int) -> str:
    return hashlib.sha256(("N20|%d|%d" % (m0, d1)).encode()).hexdigest()[:24]


def _structure_id(episode: str, symbol: str, p_time: int, c_time: int) -> str:
    return hashlib.sha256(
        ("N20|%s|%s|%d|%d" % (episode, symbol, p_time, c_time)).encode()
    ).hexdigest()[:24]


def _winner_sort_key(item: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        -_d(item["recovery_ratio"]), item["resilience_rank"],
        item["baseline_rank"], -_d(item["taker_ratio"]),
        item["quote_volume_rank"], item["symbol"],
    )


def _result(
    symbol: str, reason: str, candles: Sequence[N20Candle] | None,
    checked_at: str, window: int, rank: int | None,
    *, winner: N20Winner | None = None, state: N20StateRecord | None = None,
    passed: bool = False, elapsed: int | None = None, consume: bool = False,
    detail: dict[str, Any] | None = None,
    state_evidence_sha256: str | None = None,
) -> N20AnalysisResult:
    current = candles[-1] if candles else None
    return N20AnalysisResult(
        symbol, passed, reason, winner,
        current.close if current else Decimal("0"),
        str(current.open_time_ms) if current else None,
        checked_at, elapsed, window, consume, rank, state, detail,
        state_evidence_sha256,
    )


def decode_n20_state_evidence(
    blob: bytes, size: int, sha256: str,
) -> N20StateRecord:
    value = decompress_n20_evidence(blob, size, sha256)
    unsigned = dict(value)
    claimed = unsigned.pop("canonical_sha256", None)
    _validate_n20_evidence_types(unsigned)
    if claimed != canonical_sha256(unsigned):
        raise ValueError("N20 inner canonical digest conflicts")
    required = {
        "schema_version", "rule_version", "strategy_id", "episode_id", "stage",
        "reason", "m0_open_time_ms", "d1_open_time_ms", "c_open_time_ms",
        "winner_symbol", "structure_id", "terminal_cutoff_time_ms", "members",
        "source", "market", "qualified_candidates", "winner", "config",
    }
    if set(unsigned) != required or (
        type(unsigned["schema_version"]) is not int
        or unsigned["schema_version"] != 1
        or type(unsigned["rule_version"]) is not str
        or unsigned["rule_version"] != N20_RULE_VERSION
        or type(unsigned["strategy_id"]) is not str
        or unsigned["strategy_id"] != "N20"
    ):
        raise ValueError("N20 evidence schema is invalid")
    validate_n20_stage_reason(unsigned["stage"], unsigned["reason"])
    _validate_n20_evidence_config(unsigned["config"])
    episode = unsigned["episode_id"]
    m0, d1 = unsigned["m0_open_time_ms"], unsigned["d1_open_time_ms"]
    if (
        type(episode) is not str or len(episode) != 24
        or type(m0) is not int or type(d1) is not int or d1 != m0 + INTERVAL_MS
        or episode != _episode_id(m0, d1)
        or type(unsigned["stage"]) is not str or type(unsigned["reason"]) is not str
        or (
            unsigned["c_open_time_ms"] is not None
            and type(unsigned["c_open_time_ms"]) is not int
        )
        or type(unsigned["members"]) is not list or len(unsigned["members"]) != 100
        or any(
            type(item) is not dict
            or type(item.get("symbol")) is not str
            or canonical_exchange_symbol(item.get("symbol"))
            != item.get("symbol")
            or type(item.get("rank")) is not int
            for item in unsigned["members"]
        )
        or {item.get("rank") for item in unsigned["members"] if type(item) is dict}
        != set(range(1, 101))
        or len({item.get("symbol") for item in unsigned["members"] if type(item) is dict}) != 100
        or type(unsigned["source"]) is not dict
        or set(unsigned["source"]) != {item["symbol"] for item in unsigned["members"]}
    ):
        raise ValueError("N20 episode identity is invalid")
    for source_rows in unsigned["source"].values():
        _parse_n20_evidence_source_rows(source_rows)
    _validate_n20_evidence_shapes(unsigned)
    cutoff = unsigned["terminal_cutoff_time_ms"]
    if cutoff is not None and (type(cutoff) is not int or cutoff < d1):
        raise ValueError("N20 terminal cutoff is invalid")
    return N20StateRecord(
        "N20", episode, unsigned["stage"], unsigned["reason"], m0, d1,
        unsigned["c_open_time_ms"], unsigned["winner_symbol"],
        unsigned["structure_id"], cutoff, value,
    )


def terminalize_consumed_n20_state(record: N20StateRecord) -> N20StateRecord:
    """Seal one already-published winner without rebuilding market evidence."""

    if (
        type(record) is not N20StateRecord
        or record.strategy_id != "N20"
        or record.stage != "CONFIRMED"
        or record.structure_id is None
        or type(record.evidence) is not dict
    ):
        raise ValueError("N20 consumed state source is invalid")
    evidence = dict(record.evidence)
    winner = evidence.get("winner")
    if type(winner) is not dict:
        raise ValueError("N20 consumed winner evidence is missing")
    entry = winner.get("entry")
    if type(entry) is not dict or type(entry.get("open_time_ms")) is not int:
        raise ValueError("N20 consumed entry cutoff is invalid")
    cutoff = entry["open_time_ms"]
    evidence["stage"] = "CONSUMED"
    evidence["reason"] = "N20_EPISODE_CONSUMED"
    evidence["terminal_cutoff_time_ms"] = cutoff
    unsigned = dict(evidence)
    unsigned.pop("canonical_sha256", None)
    evidence["canonical_sha256"] = canonical_sha256(unsigned)
    sealed = N20StateRecord(
        "N20", record.episode_id, "CONSUMED", "N20_EPISODE_CONSUMED",
        record.m0_open_time_ms, record.d1_open_time_ms, record.c_open_time_ms,
        record.winner_symbol, record.structure_id, cutoff, evidence,
    )
    decode_n20_state_evidence(*sealed.encoded)
    return sealed


def analyze_n20_market_episode(
    members: Sequence[tuple[str, int]],
    raw_by_symbol: Mapping[str, Sequence[Sequence[Any]]],
    *, checked_at_ms: int,
    fixed_input_bars: int = 122, atr_period: int = 14,
    vwap_long_period: int = 96, vwap_short_period: int = 20,
    baseline_rank_max: int = 20, resilience_rank_max: int = 20,
    m0_positive_breadth_min: Decimal = Decimal("0.65"),
    m0_above_vwap_breadth_min: Decimal = Decimal("0.60"),
    d1_down_breadth_min: Decimal = Decimal("0.55"),
    d1_median_return_max: Decimal = Decimal("-0.0015"),
    pullback_min_bars: int = 2, pullback_max_bars: int = 6,
    pullback_depth_min: Decimal = Decimal("0.004"),
    pullback_depth_max: Decimal = Decimal("0.025"),
    crash_down_breadth_min: Decimal = Decimal("0.80"),
    crash_depth_min: Decimal = Decimal("0.030"),
    recovery_up_breadth_min: Decimal = Decimal("0.55"),
    recovery_breadth_improvement_min: Decimal = Decimal("0.20"),
    relative_resilience_min: Decimal = Decimal("0.50"),
    candidate_drawdown_atr_max: Decimal = Decimal("1.50"),
    candidate_recovery_ratio_min: Decimal = Decimal("0.50"),
    candidate_close_location_min: Decimal = Decimal("0.65"),
    candidate_taker_buy_ratio_min: Decimal = Decimal("0.52"),
    candidate_volume_multiple_min: Decimal = Decimal("0.80"),
    entry_extension_atr_max: Decimal = Decimal("0.50"),
    entry_window_seconds: int = 120,
    frozen_blob: bytes | None = None, frozen_size: int | None = None,
    frozen_sha256: str | None = None,
) -> N20MarketAnalysis:
    config = dict(locals())
    for name in ("members", "raw_by_symbol", "checked_at_ms", "frozen_blob", "frozen_size", "frozen_sha256"):
        config.pop(name)
    checked_at = datetime.fromtimestamp(checked_at_ms / 1000, timezone.utc).isoformat()
    window = entry_window_seconds * 1000
    if type(checked_at_ms) is not int or config != _APPROVED_CONFIG:
        return N20MarketAnalysis(False, "N20_DEFINITION_INVALID", {}, None, ())
    try:
        if (
            type(members) not in (list, tuple) or len(members) != 100
            or any(type(item) not in (list, tuple) or len(item) != 2 for item in members)
        ):
            raise ValueError("N20 requires exactly 100 members")
        normalized = tuple((item[0], item[1]) for item in members)
        if (
            any(
                canonical_exchange_symbol(symbol) != symbol
                or type(rank) is not int
                for symbol, rank in normalized
            )
            or len({symbol for symbol, _ in normalized}) != 100
            or {rank for _, rank in normalized} != set(range(1, 101))
        ):
            raise ValueError("N20 frozen membership is invalid")
        prior = None
        if frozen_blob is not None:
            if frozen_size is None or frozen_sha256 is None:
                raise ValueError("N20 frozen envelope is incomplete")
            prior = decode_n20_state_evidence(frozen_blob, frozen_size, frozen_sha256)
            normalized = tuple(
                (item["symbol"], item["rank"]) for item in prior.evidence["members"]
            )
        candles = {
            symbol: parse_n20_klines(raw_by_symbol[symbol][-fixed_input_bars:])
            for symbol, _ in normalized
        }
        if any(len(items) != fixed_input_bars for items in candles.values()):
            raise ValueError("N20 member history is incomplete")
        axes = {items[-1].open_time_ms for items in candles.values()}
        if len(axes) != 1:
            raise ValueError("N20 current axis is inconsistent")
        current_time = next(iter(axes))
        if not current_time <= checked_at_ms < current_time + INTERVAL_MS:
            raise ValueError("N20 current E axis is not live")
        closed_candles = {
            symbol: rows[:-1] for symbol, rows in candles.items()
        }
        source_payload = (
            _merge_n20_frozen_source(prior.evidence["source"], closed_candles)
            if prior is not None
            else {
                symbol: [item.to_jsonable() for item in rows]
                for symbol, rows in closed_candles.items()
            }
        )
    except Exception as exc:
        if isinstance(exc, _N20FrozenSourceConflict):
            reason = "N20_FROZEN_SOURCE_CONFLICT"
        else:
            reason = "N20_FROZEN_MEMBER_MISSING" if frozen_blob is not None else "N20_MARKET_CONTEXT_INSUFFICIENT"
        return N20MarketAnalysis(False, reason, {}, None, tuple(item[0] for item in members if type(item) in (list, tuple) and item))

    ranks = dict(normalized)
    by_time = {symbol: {item.open_time_ms: item for item in rows} for symbol, rows in candles.items()}
    if prior is None:
        d1_time = current_time - INTERVAL_MS
        m0_time = d1_time - INTERVAL_MS
        minimum_new_d1 = None
        # A terminal row may be supplied by the scheduler only as a cutoff,
        # never as a source from which membership is rebuilt.
    else:
        d1_time, m0_time = prior.d1_open_time_ms, prior.m0_open_time_ms
        if prior.stage in {"CONSUMED", "MISSED", "INVALID", "CRASH_VETO", "EXPIRED"}:
            results = {
                symbol: _result(
                    symbol,
                    "N20_EPISODE_CONSUMED",
                    rows,
                    checked_at,
                    window,
                    ranks[symbol],
                    state=prior,
                    state_evidence_sha256=frozen_sha256,
                )
                for symbol, rows in candles.items()
            }
            return N20MarketAnalysis(True, "N20_EPISODE_CONSUMED", results, prior, tuple(ranks))
    try:
        m0 = {symbol: by_time[symbol][m0_time] for symbol in ranks}
        d1 = {symbol: by_time[symbol][d1_time] for symbol in ranks}
        m0_indices = {symbol: m0[symbol].index for symbol in ranks}
        if any(index < vwap_long_period for index in m0_indices.values()):
            raise ValueError
        returns96 = {
            symbol: m0[symbol].close / candles[symbol][m0_indices[symbol] - vwap_long_period].close - 1
            for symbol in ranks
        }
        vwap96 = {
            symbol: _vwap(candles[symbol][m0_indices[symbol] - vwap_long_period + 1:m0_indices[symbol] + 1])
            for symbol in ranks
        }
        positive_breadth = Decimal(sum(value > 0 for value in returns96.values())) / 100
        above_breadth = Decimal(sum(m0[symbol].close > vwap96[symbol] for symbol in ranks)) / 100
        d1_returns = {symbol: d1[symbol].close / m0[symbol].close - 1 for symbol in ranks}
        d1_down = Decimal(sum(value < 0 for value in d1_returns.values())) / 100
        d1_median = _median(d1_returns.values())
    except Exception:
        return N20MarketAnalysis(False, "N20_MARKET_CONTEXT_INSUFFICIENT", {}, None, tuple(ranks))
    if prior is None and (
        positive_breadth < m0_positive_breadth_min
        or above_breadth < m0_above_vwap_breadth_min
    ):
        results = {symbol: _result(symbol, "N20_BULL_CONTEXT_NOT_MET", rows, checked_at, window, ranks[symbol]) for symbol, rows in candles.items()}
        return N20MarketAnalysis(True, "N20_BULL_CONTEXT_NOT_MET", results, None, tuple(ranks))
    if prior is None and (d1_down < d1_down_breadth_min or d1_median > d1_median_return_max):
        results = {symbol: _result(symbol, "N20_PULLBACK_NOT_STARTED", rows, checked_at, window, ranks[symbol]) for symbol, rows in candles.items()}
        return N20MarketAnalysis(True, "N20_PULLBACK_NOT_STARTED", results, None, tuple(ranks))

    episode = _episode_id(m0_time, d1_time)
    ordered_baseline = sorted(ranks, key=lambda symbol: (-returns96[symbol], ranks[symbol], symbol))
    baseline_rank = {symbol: index + 1 for index, symbol in enumerate(ordered_baseline)}
    terminal_stage = None
    terminal_reason = None
    c_time = None
    market_rows: list[dict[str, Any]] = []
    last_closed_time = current_time - INTERVAL_MS
    available_after_d1 = (last_closed_time - d1_time) // INTERVAL_MS
    for offset in range(1, min(available_after_d1, pullback_max_bars) + 1):
        candidate_time = d1_time + offset * INTERVAL_MS
        previous_time = candidate_time - INTERVAL_MS
        cumulative = {
            symbol: by_time[symbol][previous_time].close / m0[symbol].close - 1
            for symbol in ranks
        }
        depth = -_median(cumulative.values())
        down = Decimal(sum(value < 0 for value in cumulative.values())) / 100
        up = Decimal(sum(by_time[symbol][candidate_time].close > by_time[symbol][previous_time].close for symbol in ranks)) / 100
        prior_previous = previous_time - INTERVAL_MS
        prior_up = Decimal(sum(by_time[symbol][previous_time].close > by_time[symbol][prior_previous].close for symbol in ranks)) / 100
        median_step = _median(
            by_time[symbol][candidate_time].close / by_time[symbol][previous_time].close - 1
            for symbol in ranks
        )
        market_rows.append({
            "open_time_ms": candidate_time, "pullback_bars": offset,
            "depth": str(depth), "down_breadth": str(down),
            "up_breadth": str(up), "prior_up_breadth": str(prior_up),
            "breadth_improvement": str(up - prior_up), "median_step_return": str(median_step),
        })
        classification = _pullback_classification(depth, down, config)
        if classification in {"CRASH", "TOO_DEEP"}:
            terminal_stage = "CRASH_VETO" if classification == "CRASH" else "CONSUMED"
            terminal_reason = "N20_SYSTEMIC_CRASH_VETO" if terminal_stage == "CRASH_VETO" else "N20_PULLBACK_TOO_DEEP"
            break
        if offset < pullback_min_bars or classification == "WAIT":
            continue
        if _recovery_thresholds_pass(
            up_breadth=up, breadth_improvement=up - prior_up,
            median_step_return=median_step, pullback_bars=offset,
            config=config,
        ):
            c_time = candidate_time
            break
    if c_time is None and terminal_stage is None and available_after_d1 >= pullback_max_bars:
        terminal_stage, terminal_reason = "EXPIRED", "N20_PULLBACK_WINDOW_EXPIRED"

    qualified: list[dict[str, Any]] = []
    winner: N20Winner | None = None
    if c_time is not None:
        d_times = list(range(d1_time, c_time, INTERVAL_MS))
        normalized_pullbacks: dict[str, Decimal] = {}
        atr_m0: dict[str, Decimal] = {}
        for symbol in ranks:
            source = candles[symbol]
            atrs = _atr(source, atr_period)
            m0_atr = atrs[m0[symbol].index]
            if m0_atr is None or m0_atr <= 0:
                return N20MarketAnalysis(False, "N20_MARKET_CONTEXT_INSUFFICIENT", {}, None, tuple(ranks))
            atr_m0[symbol] = m0_atr
            low = min(by_time[symbol][time].low for time in d_times)
            normalized_pullbacks[symbol] = (low - m0[symbol].close) / m0_atr
        normalized_median = _median(normalized_pullbacks.values())
        resilience_order = sorted(ranks, key=lambda symbol: (-normalized_pullbacks[symbol], baseline_rank[symbol], ranks[symbol], symbol))
        resilience_rank = {symbol: index + 1 for index, symbol in enumerate(resilience_order)}
        for symbol in ranks:
            c = by_time[symbol][c_time]
            prior_d = by_time[symbol][c_time - INTERVAL_MS]
            source = candles[symbol]
            atr_c = _atr(source, atr_period)[c.index]
            if atr_c is None or atr_c <= 0 or c.index < vwap_short_period:
                continue
            pullback_candles = [by_time[symbol][time] for time in d_times]
            p_candles = pullback_candles + [c]
            p = min(item.low for item in p_candles)
            p_time = min(item.open_time_ms for item in p_candles if item.low == p)
            denominator = m0[symbol].close - min(item.low for item in pullback_candles)
            if denominator <= 0:
                continue
            recovery = (c.close - min(item.low for item in pullback_candles)) / denominator
            relative = normalized_pullbacks[symbol] - normalized_median
            drawdown = (m0[symbol].close - min(item.low for item in pullback_candles)) / atr_m0[symbol]
            vwap20 = _vwap(source[c.index - vwap_short_period + 1:c.index + 1])
            volume_median = _median(item.quote_volume for item in source[c.index - 20:c.index])
            volume_multiple = c.quote_volume / volume_median
            detail = {
                "symbol": symbol, "quote_volume_rank": ranks[symbol],
                "baseline_rank": baseline_rank[symbol], "resilience_rank": resilience_rank[symbol],
                "return96": str(returns96[symbol]), "vwap96": str(vwap96[symbol]),
                "normalized_pullback_return": str(normalized_pullbacks[symbol]),
                "relative_resilience": str(relative), "drawdown_atr": str(drawdown),
                "recovery_ratio": str(recovery), "vwap20": str(vwap20),
                "close_location": str(c.close_location), "taker_ratio": str(c.taker_ratio),
                "volume_multiple": str(volume_multiple), "p": str(p),
                "p_open_time_ms": p_time, "c_open_time_ms": c_time,
            }
            if _candidate_thresholds_pass(
                baseline_rank=baseline_rank[symbol], return96=returns96[symbol],
                above_vwap96=m0[symbol].close > vwap96[symbol],
                relative_resilience=relative, drawdown_atr=drawdown,
                resilience_rank=resilience_rank[symbol], bullish=c.close > c.open,
                broke_prior_high=c.close > prior_d.high,
                above_vwap20=c.close > vwap20, recovery_ratio=recovery,
                close_location=c.close_location, taker_ratio=c.taker_ratio,
                volume_multiple=volume_multiple, config=config,
            ):
                qualified.append(detail)
        if qualified:
            selected = min(qualified, key=_winner_sort_key)
            symbol = selected["symbol"]
            c = by_time[symbol][c_time]
            current = candles[symbol][-1]
            atr_c = _atr(candles[symbol], atr_period)[c.index]
            assert atr_c is not None
            structure = _structure_id(episode, symbol, selected["p_open_time_ms"], c_time)
            winner = N20Winner(
                symbol, ranks[symbol], selected["baseline_rank"], selected["resilience_rank"],
                _d(selected["recovery_ratio"]), _d(selected["relative_resilience"]),
                _d(selected["taker_ratio"]), _d(selected["p"]), selected["p_open_time_ms"],
                c, current, atr_c, c.close, c.close + entry_extension_atr_max * atr_c,
                episode, structure,
            )
        else:
            terminal_stage, terminal_reason = "CONSUMED", "N20_NO_QUALIFIED_LEADER"

    winner_stage = None
    winner_reason = None
    winner_passed = False
    winner_elapsed = None
    if winner is not None:
        winner_elapsed = checked_at_ms - winner.entry.open_time_ms
        winner_stage, winner_reason, winner_passed = "CONFIRMED", "PASSED", True
        if winner.c.open_time_ms != winner.entry.open_time_ms - INTERVAL_MS:
            winner_stage, winner_reason, winner_passed = "MISSED", "N20_HISTORICAL_ENTRY_MISSED", False
        elif winner.entry.low < winner.p:
            winner_stage, winner_reason, winner_passed = "INVALID", "N20_ENTRY_BROKE_P", False
        elif winner_elapsed >= window:
            winner_stage, winner_reason, winner_passed = "EXPIRED", "N20_ENTRY_WINDOW_EXPIRED", False
        elif winner.entry.close > winner.entry_max:
            winner_stage, winner_reason, winner_passed = "MISSED", "N20_ENTRY_PRICE_ABOVE_MAX", False
        elif winner.entry.close < winner.entry_min:
            winner_stage, winner_reason, winner_passed = "ENTRY_WAITING", "N20_ENTRY_BELOW_MIN_WAITING", False

    stage = terminal_stage or winner_stage or ("RECOVERY_FROZEN" if c_time is not None else "PULLBACK_ACTIVE")
    reason = terminal_reason or winner_reason or ("N20_RECOVERY_FROZEN" if c_time is not None else "N20_PULLBACK_ACTIVE")
    terminal_cutoff = (
        last_closed_time if terminal_stage
        else winner.entry.open_time_ms if winner_stage in {"MISSED", "INVALID", "EXPIRED"}
        else None
    )
    evidence_unsigned = {
        "schema_version": 1, "rule_version": N20_RULE_VERSION, "strategy_id": "N20",
        "episode_id": episode, "stage": stage, "reason": reason,
        "m0_open_time_ms": m0_time, "d1_open_time_ms": d1_time,
        "c_open_time_ms": c_time, "winner_symbol": winner.symbol if winner else None,
        "structure_id": winner.structure_id if winner else None,
        "terminal_cutoff_time_ms": terminal_cutoff,
        "members": [{"symbol": symbol, "rank": ranks[symbol]} for symbol in sorted(ranks, key=ranks.get)],
        "source": source_payload,
        "market": {
            "m0_positive_breadth": str(positive_breadth),
            "m0_above_vwap_breadth": str(above_breadth),
            "d1_down_breadth": str(d1_down), "d1_median_return": str(d1_median),
            "timeline": market_rows,
        },
        "qualified_candidates": qualified,
        "winner": winner.to_jsonable() if winner else None,
        "config": {key: str(value) if type(value) is Decimal else value for key, value in config.items()},
    }
    evidence = dict(evidence_unsigned)
    evidence["canonical_sha256"] = canonical_sha256(evidence_unsigned)
    state = N20StateRecord(
        "N20", episode, stage, reason, m0_time, d1_time, c_time,
        winner.symbol if winner else None, winner.structure_id if winner else None,
        terminal_cutoff, evidence,
    )
    try:
        state_encoded = state.encoded
    except ValueError:
        return N20MarketAnalysis(False, "N20_EVIDENCE_SIZE_INVALID", {}, None, tuple(ranks))

    results: dict[str, N20AnalysisResult] = {}
    qualified_by_symbol = {item["symbol"]: item for item in qualified}
    for symbol, rows in candles.items():
        if terminal_stage is not None:
            results[symbol] = _result(
                symbol, reason, rows, checked_at, window, ranks[symbol],
                state=state, consume=True,
                state_evidence_sha256=state_encoded[2],
            )
        elif c_time is None:
            results[symbol] = _result(
                symbol, reason, rows, checked_at, window, ranks[symbol],
                state=state, state_evidence_sha256=state_encoded[2],
            )
        elif winner is None or symbol != winner.symbol:
            candidate_reason = "N20_NOT_WINNER" if symbol in qualified_by_symbol else "N20_CANDIDATE_NOT_QUALIFIED"
            results[symbol] = _result(
                symbol, candidate_reason, rows, checked_at, window,
                ranks[symbol], state=state,
                detail=qualified_by_symbol.get(symbol),
                state_evidence_sha256=state_encoded[2],
            )
        else:
            assert winner_reason is not None and winner_elapsed is not None
            results[symbol] = _result(
                symbol, winner_reason, rows, checked_at, window, ranks[symbol],
                winner=winner, state=state, passed=winner_passed, elapsed=winner_elapsed,
                consume=winner_stage in {"MISSED", "INVALID", "EXPIRED"},
                detail=qualified_by_symbol.get(symbol),
                state_evidence_sha256=state_encoded[2],
            )
    return N20MarketAnalysis(True, reason, results, state, tuple(ranks))
