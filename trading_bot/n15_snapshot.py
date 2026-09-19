from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping


INTERVAL_MS = 900_000
N15_SNAPSHOT_SCHEMA_VERSION = 2
N15_MIN_RUNTIME_CANDLES = 122
N15_METRIC_SOURCE_CANDLES = N15_MIN_RUNTIME_CANDLES - 2


class N15SnapshotSourceUnavailableError(ValueError):
    pass


class N15HistoricalSourceUnavailableError(
    N15SnapshotSourceUnavailableError
):
    pass


def _d(value: Any) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise InvalidOperation
    return result


def _exact(left: Any, right: Any) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(
            _exact(left[key], right[key]) for key in left
        )
    if isinstance(left, list):
        return len(left) == len(right) and all(
            _exact(a, b) for a, b in zip(left, right)
        )
    return left == right


def _hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class N15Candle:
    open_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    quote_volume: Decimal
    taker_buy_quote_volume: Decimal

    def json(self) -> dict[str, Any]:
        return {
            "open_time_ms": self.open_time_ms,
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "quote_volume": str(self.quote_volume),
            "taker_buy_quote_volume": str(self.taker_buy_quote_volume),
        }


@dataclass(frozen=True)
class N15Row:
    symbol: str
    quote_volume_rank: int
    b: N15Candle
    c: N15Candle
    atr_b_pre: Decimal
    atr_c_pre: Decimal
    v20_c: Decimal
    move_b: Decimal
    move_c: Decimal
    rank_b: int
    rank_c: int
    close_location_c: Decimal | None
    taker_ratio_c: Decimal | None
    p: Decimal
    eligible: bool
    reason: str

    def json(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "quote_volume_rank": self.quote_volume_rank,
            "b": self.b.json(),
            "c": self.c.json(),
            "atr_b_pre": str(self.atr_b_pre),
            "atr_c_pre": str(self.atr_c_pre),
            "v20_c": str(self.v20_c),
            "move_b": str(self.move_b),
            "move_c": str(self.move_c),
            "rank_b": self.rank_b,
            "rank_c": self.rank_c,
            "close_location_c": (
                str(self.close_location_c)
                if self.close_location_c is not None else None
            ),
            "taker_ratio_c": (
                str(self.taker_ratio_c)
                if self.taker_ratio_c is not None else None
            ),
            "p": str(self.p),
            "eligible": self.eligible,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class N15Snapshot:
    strategy_id: str
    e_open_time_ms: int
    config_signature: dict[str, Any]
    rows: dict[str, N15Row]
    up_b: Decimal
    down_b: Decimal
    median_move_b: Decimal
    up_c: Decimal
    down_c: Decimal
    median_move_c: Decimal
    systemic_crash_veto: bool
    winner_symbol: str | None
    canonical_sha256: str


def parse_n15_klines(raw: list[list[Any]]) -> list[N15Candle]:
    candles: list[N15Candle] = []
    for row in raw:
        if len(row) <= 10 or type(row[0]) is not int or row[0] <= 0:
            raise ValueError("N15 kline fields invalid")
        candle = N15Candle(
            row[0],
            _d(row[1]),
            _d(row[2]),
            _d(row[3]),
            _d(row[4]),
            _d(row[7]),
            _d(row[10]),
        )
        if (
            min(candle.open, candle.high, candle.low, candle.close) <= 0
            or candle.high < candle.low
            or candle.high < max(candle.open, candle.close)
            or candle.low > min(candle.open, candle.close)
            or candle.quote_volume < 0
            or candle.taker_buy_quote_volume < 0
            or candle.taker_buy_quote_volume > candle.quote_volume
        ):
            raise ValueError("N15 kline values invalid")
        candles.append(candle)
    if any(
        right.open_time_ms - left.open_time_ms != INTERVAL_MS
        for left, right in zip(candles, candles[1:])
    ):
        raise ValueError("N15 kline sequence invalid")
    return candles


def wilder_atr(candles: list[N15Candle], period: int = 14) -> list[Decimal | None]:
    if type(period) is not int or period <= 0:
        raise ValueError("N15 ATR period invalid")
    values: list[Decimal | None] = [None] * len(candles)
    trs: list[Decimal] = []
    atr: Decimal | None = None
    for index, candle in enumerate(candles):
        previous = candles[index - 1].close if index else candle.close
        tr = max(
            candle.high - candle.low,
            abs(candle.high - previous),
            abs(candle.low - previous),
        )
        trs.append(tr)
        if index == period - 1:
            atr = sum(trs[:period], Decimal("0")) / Decimal(period)
        elif index >= period and atr is not None:
            atr = (atr * Decimal(period - 1) + tr) / Decimal(period)
        values[index] = atr
    return values


def exact_median(values: Iterable[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("N15 median input empty")
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def n15_config_signature(strategy: Any) -> dict[str, Any]:
    return {
        "volume_top_n": strategy.volume_top_n,
        "entry_window_seconds": strategy.entry_window_seconds,
        "atr_period": strategy.n15_atr_period,
        "volume_median_bars": strategy.n15_volume_median_bars,
        "weak_down_breadth_min": str(strategy.n15_weak_down_breadth_min),
        "weak_median_move_max": str(strategy.n15_weak_median_move_max),
        "crash_down_breadth_min": str(strategy.n15_crash_down_breadth_min),
        "crash_median_move_max": str(strategy.n15_crash_median_move_max),
        "b_rank_max": strategy.n15_b_rank_max,
        "b_move_min": str(strategy.n15_b_move_min),
        "recovery_up_breadth_min": str(strategy.n15_recovery_up_breadth_min),
        "recovery_improvement_min": str(strategy.n15_recovery_improvement_min),
        "c_rank_max": strategy.n15_c_rank_max,
        "c_close_location_min": str(strategy.n15_c_close_location_min),
        "c_taker_buy_ratio_min": str(strategy.n15_c_taker_buy_ratio_min),
        "c_volume_median_multiple_min": str(
            strategy.n15_c_volume_median_multiple_min
        ),
        "entry_extension_atr_max": str(strategy.n15_entry_extension_atr_max),
    }


def _validate_config(strategy: Any) -> None:
    integer_fields = {
        "volume_top_n": (1, 100),
        "entry_window_seconds": (1, 900),
        "n15_atr_period": (1, 122),
        "n15_volume_median_bars": (1, 119),
        "n15_b_rank_max": (1, 100),
        "n15_c_rank_max": (1, 100),
    }
    for name, (minimum, maximum) in integer_fields.items():
        value = getattr(strategy, name)
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError(f"N15 config {name} invalid")
    decimal_fields = (
        "n15_weak_down_breadth_min",
        "n15_weak_median_move_max",
        "n15_crash_down_breadth_min",
        "n15_crash_median_move_max",
        "n15_b_move_min",
        "n15_recovery_up_breadth_min",
        "n15_recovery_improvement_min",
        "n15_c_close_location_min",
        "n15_c_taker_buy_ratio_min",
        "n15_c_volume_median_multiple_min",
        "n15_entry_extension_atr_max",
    )
    for name in decimal_fields:
        value = getattr(strategy, name)
        if type(value) is not Decimal or not value.is_finite():
            raise ValueError(f"N15 config {name} invalid")
    for name in (
        "n15_weak_down_breadth_min",
        "n15_crash_down_breadth_min",
        "n15_recovery_up_breadth_min",
        "n15_recovery_improvement_min",
        "n15_c_close_location_min",
        "n15_c_taker_buy_ratio_min",
    ):
        if not Decimal("0") <= getattr(strategy, name) <= Decimal("1"):
            raise ValueError(f"N15 config {name} out of range")
    if (
        strategy.n15_c_volume_median_multiple_min < 0
        or strategy.n15_entry_extension_atr_max < 0
    ):
        raise ValueError("N15 config multiplier invalid")


def _members(value: Iterable[Any]) -> list[tuple[str, int]]:
    result: list[tuple[str, int]] = []
    for item in value:
        if isinstance(item, tuple):
            symbol, rank = item
        else:
            symbol, rank = item.symbol, item.quote_volume_rank
        if (
            type(symbol) is not str
            or not symbol
            or type(rank) is not int
            or not 1 <= rank <= 100
        ):
            raise ValueError("N15 member invalid")
        result.append((symbol, rank))
    if (
        len(result) != 100
        or len({symbol for symbol, _ in result}) != 100
        or {rank for _, rank in result} != set(range(1, 101))
    ):
        raise ValueError("N15 Top100 incomplete")
    return sorted(result, key=lambda item: (item[1], item[0]))


def _rank(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    ordered = sorted(
        rows,
        key=lambda row: (-row[field], row["quote_volume_rank"], row["symbol"]),
    )
    return {row["symbol"]: index for index, row in enumerate(ordered, 1)}


def _metric_source_from_raw(
    members: Iterable[Any],
    raw_by_symbol: Mapping[str, list[list[Any]]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for symbol, _ in _members(members):
        candles = parse_n15_klines(raw_by_symbol[symbol])
        if len(candles) != N15_MIN_RUNTIME_CANDLES:
            raise ValueError("N15 metric source window invalid")
        source = candles[:-2]
        if not source:
            raise ValueError("N15 metric source incomplete")
        rows.append({
            "symbol": symbol,
            "start_open_time_ms": source[0].open_time_ms,
            "candles": [
                [
                    str(candle.open),
                    str(candle.high),
                    str(candle.low),
                    str(candle.close),
                    str(candle.quote_volume),
                ]
                for candle in source
            ],
        })
    return {"rows": rows}


def _raw_from_metric_source(
    payload: dict[str, Any],
    strategy: Any,
) -> dict[str, list[list[Any]]]:
    metric_source = payload.get("metric_source")
    source_keys = {"rows"}
    source_row_keys = {"symbol", "start_open_time_ms", "candles"}
    if type(metric_source) is not dict or set(metric_source) != source_keys:
        raise ValueError("N15 metric source shape invalid")
    source_rows = metric_source["rows"]
    payload_rows = payload.get("rows")
    if (
        type(source_rows) is not list
        or type(payload_rows) is not list
        or len(source_rows) != 100
        or len(payload_rows) != 100
    ):
        raise ValueError("N15 metric source rows invalid")
    expected_symbols = [row.get("symbol") for row in payload_rows]
    if [row.get("symbol") if type(row) is dict else None for row in source_rows] != expected_symbols:
        raise ValueError("N15 metric source members invalid")
    payload_by_symbol = {row["symbol"]: row for row in payload_rows}
    result: dict[str, list[list[Any]]] = {}
    for source_row in source_rows:
        if type(source_row) is not dict or set(source_row) != source_row_keys:
            raise ValueError("N15 metric source row invalid")
        symbol = source_row["symbol"]
        start = source_row["start_open_time_ms"]
        values = source_row["candles"]
        if (
            type(symbol) is not str
            or not symbol
            or type(start) is not int
            or start <= 0
            or start % INTERVAL_MS != 0
            or type(values) is not list
            or len(values) != N15_METRIC_SOURCE_CANDLES
            or start
            != payload["e_open_time_ms"]
            - (N15_MIN_RUNTIME_CANDLES - 1) * INTERVAL_MS
        ):
            raise ValueError("N15 metric source identity invalid")
        raw: list[list[Any]] = []
        for index, value in enumerate(values):
            if (
                type(value) is not list
                or len(value) != 5
                or any(type(item) is not str for item in value)
            ):
                raise ValueError("N15 metric source candle invalid")
            open_price, high, low, close, quote_volume = map(_d, value)
            if (
                min(open_price, high, low, close) <= 0
                or high < max(open_price, close)
                or low > min(open_price, close)
                or quote_volume < 0
            ):
                raise ValueError("N15 metric source candle values invalid")
            open_time = start + index * INTERVAL_MS
            raw.append([
                open_time,
                value[0],
                value[1],
                value[2],
                value[3],
                "0",
                open_time + INTERVAL_MS - 1,
                value[4],
                "0",
                "0",
                "0",
                "0",
            ])
        frozen = payload_by_symbol[symbol]
        b = frozen["b"]
        c = frozen["c"]
        if (
            raw[-1][0] != b["open_time_ms"]
            or raw[-1][1:5]
            != [b["open"], b["high"], b["low"], b["close"]]
            or raw[-1][7] != b["quote_volume"]
        ):
            raise ValueError("N15 metric source B mismatch")
        raw[-1][10] = b["taker_buy_quote_volume"]
        raw.append([
            c["open_time_ms"],
            c["open"],
            c["high"],
            c["low"],
            c["close"],
            "0",
            c["open_time_ms"] + INTERVAL_MS - 1,
            c["quote_volume"],
            "0",
            "0",
            c["taker_buy_quote_volume"],
            "0",
        ])
        e_time = payload["e_open_time_ms"]
        raw.append([
            e_time,
            c["close"],
            c["close"],
            c["close"],
            c["close"],
            "0",
            e_time + INTERVAL_MS - 1,
            "0",
            "0",
            "0",
            "0",
            "0",
        ])
        parse_n15_klines(raw)
        result[symbol] = raw
    return result


def _unsigned_snapshot(
    strategy: Any,
    members: Iterable[Any],
    raw_by_symbol: Mapping[str, list[list[Any]]],
) -> dict[str, Any]:
    _validate_config(strategy)
    member_rows = _members(members)
    calculated: list[dict[str, Any]] = []
    e_times: set[int] = set()
    b_times: set[int] = set()
    c_times: set[int] = set()
    for symbol, quote_rank in member_rows:
        candles = parse_n15_klines(raw_by_symbol[symbol])
        if len(candles) < max(strategy.n15_volume_median_bars + 3, strategy.n15_atr_period + 3):
            raise ValueError("N15 history incomplete")
        b_index, c_index = len(candles) - 3, len(candles) - 2
        b, c, e = candles[b_index], candles[c_index], candles[-1]
        if (
            c.open_time_ms != b.open_time_ms + INTERVAL_MS
            or e.open_time_ms != c.open_time_ms + INTERVAL_MS
        ):
            raise ValueError("N15 B/C/E sequence invalid")
        atrs = wilder_atr(candles, strategy.n15_atr_period)
        atr_b_pre = atrs[b_index - 1]
        atr_c_pre = atrs[c_index - 1]
        if atr_b_pre is None or atr_c_pre is None or atr_b_pre <= 0 or atr_c_pre <= 0:
            raise ValueError("N15 ATR invalid")
        volume_window = candles[c_index - strategy.n15_volume_median_bars:c_index]
        if len(volume_window) != strategy.n15_volume_median_bars:
            raise ValueError("N15 V20 incomplete")
        v20 = exact_median(item.quote_volume for item in volume_window)
        target_metrics_valid = (
            v20 > 0 and c.quote_volume > 0 and c.high > c.low
        )
        taker_ratio = (
            c.taker_buy_quote_volume / c.quote_volume
            if c.quote_volume > 0 else None
        )
        close_location = (
            (c.close - c.low) / (c.high - c.low)
            if c.high > c.low else None
        )
        if taker_ratio is not None and not Decimal("0") <= taker_ratio <= Decimal("1"):
            raise ValueError("N15 taker ratio invalid")
        calculated.append({
            "symbol": symbol,
            "quote_volume_rank": quote_rank,
            "b": b,
            "c": c,
            "atr_b_pre": atr_b_pre,
            "atr_c_pre": atr_c_pre,
            "v20_c": v20,
            "move_b": (b.close - b.open) / atr_b_pre,
            "move_c": (c.close - c.open) / atr_c_pre,
            "close_location_c": close_location,
            "taker_ratio_c": taker_ratio,
            "target_metrics_valid": target_metrics_valid,
            "p": min(b.low, c.low),
        })
        b_times.add(b.open_time_ms)
        c_times.add(c.open_time_ms)
        e_times.add(e.open_time_ms)
    if len(b_times) != 1 or len(c_times) != 1 or len(e_times) != 1:
        raise ValueError("N15 cross-symbol time axis invalid")

    rank_b = _rank(calculated, "move_b")
    rank_c = _rank(calculated, "move_c")
    count = Decimal("100")
    up_b = Decimal(sum(row["move_b"] > 0 for row in calculated)) / count
    down_b = Decimal(sum(row["move_b"] < 0 for row in calculated)) / count
    up_c = Decimal(sum(row["move_c"] > 0 for row in calculated)) / count
    down_c = Decimal(sum(row["move_c"] < 0 for row in calculated)) / count
    median_b = exact_median(row["move_b"] for row in calculated)
    median_c = exact_median(row["move_c"] for row in calculated)
    crash = (
        down_b >= strategy.n15_crash_down_breadth_min
        and median_b <= strategy.n15_crash_median_move_max
    )
    weak = (
        down_b >= strategy.n15_weak_down_breadth_min
        and median_b <= strategy.n15_weak_median_move_max
    )
    recovery = (
        up_c >= strategy.n15_recovery_up_breadth_min
        and up_c - up_b >= strategy.n15_recovery_improvement_min
    )
    rows: list[N15Row] = []
    for item in calculated:
        b, c = item["b"], item["c"]
        reason = "N15_CANDIDATE"
        if crash:
            reason = "N15_SYSTEMIC_CRASH_VETO"
        elif not weak:
            reason = "N15_WEAK_MARKET_NOT_MET"
        elif not recovery:
            reason = "N15_MARKET_RECOVERY_NOT_MET"
        elif not item["target_metrics_valid"]:
            reason = "N15_C_TARGET_METRICS_INVALID"
        elif rank_b[item["symbol"]] > strategy.n15_b_rank_max:
            reason = "N15_B_RANK_OUT_OF_RANGE"
        elif item["move_b"] < strategy.n15_b_move_min:
            reason = "N15_B_NOT_RESILIENT"
        elif c.close <= c.open:
            reason = "N15_C_NOT_BULLISH"
        elif c.low < b.low:
            reason = "N15_C_LOW_BROKE_B"
        elif c.close <= b.high:
            reason = "N15_C_NOT_BREAKOUT"
        elif rank_c[item["symbol"]] > strategy.n15_c_rank_max:
            reason = "N15_C_RANK_OUT_OF_RANGE"
        elif item["close_location_c"] < strategy.n15_c_close_location_min:
            reason = "N15_C_CLOSE_LOCATION_TOO_LOW"
        elif item["taker_ratio_c"] < strategy.n15_c_taker_buy_ratio_min:
            reason = "N15_C_TAKER_BUY_RATIO_TOO_LOW"
        elif c.quote_volume < strategy.n15_c_volume_median_multiple_min * item["v20_c"]:
            reason = "N15_C_VOLUME_TOO_LOW"
        rows.append(N15Row(
            item["symbol"], item["quote_volume_rank"], b, c,
            item["atr_b_pre"], item["atr_c_pre"], item["v20_c"],
            item["move_b"], item["move_c"], rank_b[item["symbol"]],
            rank_c[item["symbol"]], item["close_location_c"],
            item["taker_ratio_c"], item["p"], reason == "N15_CANDIDATE", reason,
        ))
    eligible = [row for row in rows if row.eligible]
    winner = min(
        eligible,
        key=lambda row: (
            -row.move_c,
            row.rank_b,
            -row.taker_ratio_c,
            row.quote_volume_rank,
            row.symbol,
        ),
        default=None,
    )
    return {
        "schema_version": 1,
        "strategy_id": strategy.strategy_id,
        "e_open_time_ms": next(iter(e_times)),
        "config_signature": n15_config_signature(strategy),
        "up_b": str(up_b),
        "down_b": str(down_b),
        "median_move_b": str(median_b),
        "up_c": str(up_c),
        "down_c": str(down_c),
        "median_move_c": str(median_c),
        "systemic_crash_veto": crash,
        "winner_symbol": winner.symbol if winner else None,
        "rows": [row.json() for row in rows],
    }


def _snapshot_from_payload(payload: dict[str, Any]) -> N15Snapshot:
    rows: dict[str, N15Row] = {}
    for value in payload["rows"]:
        def candle(name: str) -> N15Candle:
            item = value[name]
            return N15Candle(
                item["open_time_ms"], _d(item["open"]), _d(item["high"]),
                _d(item["low"]), _d(item["close"]), _d(item["quote_volume"]),
                _d(item["taker_buy_quote_volume"]),
            )
        row = N15Row(
            value["symbol"], value["quote_volume_rank"], candle("b"), candle("c"),
            _d(value["atr_b_pre"]), _d(value["atr_c_pre"]), _d(value["v20_c"]),
            _d(value["move_b"]), _d(value["move_c"]), value["rank_b"],
            value["rank_c"], (
                _d(value["close_location_c"])
                if value["close_location_c"] is not None else None
            ), (
                _d(value["taker_ratio_c"])
                if value["taker_ratio_c"] is not None else None
            ), _d(value["p"]), value["eligible"],
            value["reason"],
        )
        rows[row.symbol] = row
    return N15Snapshot(
        payload["strategy_id"], payload["e_open_time_ms"],
        payload["config_signature"], rows, _d(payload["up_b"]),
        _d(payload["down_b"]), _d(payload["median_move_b"]),
        _d(payload["up_c"]), _d(payload["down_c"]),
        _d(payload["median_move_c"]), payload["systemic_crash_veto"],
        payload["winner_symbol"], payload["canonical_sha256"],
    )


def build_n15_snapshot(
    strategy: Any,
    members: Iterable[Any],
    raw_by_symbol: Mapping[str, list[list[Any]]],
) -> tuple[dict[str, Any], N15Snapshot]:
    frozen_members = list(members)
    unsigned = _unsigned_snapshot(strategy, frozen_members, raw_by_symbol)
    unsigned["schema_version"] = N15_SNAPSHOT_SCHEMA_VERSION
    unsigned["metric_source"] = _metric_source_from_raw(
        frozen_members, raw_by_symbol
    )
    payload = {**unsigned, "canonical_sha256": _hash(unsigned)}
    return payload, _snapshot_from_payload(payload)


def validate_n15_snapshot_envelope(
    payload: dict[str, Any],
    strategy_id: str,
    expected_e_open_time_ms: int,
    expected_config_signature: dict[str, Any] | None = None,
    strategy: Any | None = None,
) -> N15Snapshot:
    try:
        if strategy is None:
            from .strategies import N15_STRATEGY
            strategy = N15_STRATEGY
        if expected_config_signature is None:
            expected_config_signature = n15_config_signature(strategy)
        common_keys = {
            "schema_version", "strategy_id", "e_open_time_ms",
            "config_signature", "up_b", "down_b", "median_move_b",
            "up_c", "down_c", "median_move_c", "systemic_crash_veto",
            "winner_symbol", "rows", "canonical_sha256",
        }
        schema_version = (
            payload.get("schema_version") if type(payload) is dict else None
        )
        expected_keys = (
            common_keys | {"metric_source"}
            if schema_version == N15_SNAPSHOT_SCHEMA_VERSION
            else common_keys
        )
        if type(payload) is not dict or set(payload) != expected_keys:
            raise ValueError
        unsigned = {
            key: value for key, value in payload.items()
            if key != "canonical_sha256"
        }
        if (
            type(payload["schema_version"]) is not int
            or payload["schema_version"] not in {
                1, N15_SNAPSHOT_SCHEMA_VERSION
            }
            or type(payload["strategy_id"]) is not str
            or payload["strategy_id"] != strategy_id
            or type(payload["e_open_time_ms"]) is not int
            or payload["e_open_time_ms"] != expected_e_open_time_ms
            or expected_e_open_time_ms <= 0
            or expected_e_open_time_ms % INTERVAL_MS != 0
            or type(payload["config_signature"]) is not dict
            or not _exact(payload["config_signature"], expected_config_signature)
            or type(payload["systemic_crash_veto"]) is not bool
            or type(payload["winner_symbol"]) not in (str, type(None))
            or type(payload["rows"]) is not list
            or len(payload["rows"]) != 100
            or type(payload["canonical_sha256"]) is not str
            or payload["canonical_sha256"] != _hash(unsigned)
        ):
            raise ValueError
        for name in ("up_b", "down_b", "up_c", "down_c"):
            if type(payload[name]) is not str:
                raise ValueError
            if not Decimal("0") <= _d(payload[name]) <= Decimal("1"):
                raise ValueError
        for name in ("median_move_b", "median_move_c"):
            if type(payload[name]) is not str:
                raise ValueError
            _d(payload[name])
        row_keys = {
            "symbol", "quote_volume_rank", "b", "c", "atr_b_pre",
            "atr_c_pre", "v20_c", "move_b", "move_c", "rank_b",
            "rank_c", "close_location_c", "taker_ratio_c", "p",
            "eligible", "reason",
        }
        candle_keys = {
            "open_time_ms", "open", "high", "low", "close",
            "quote_volume", "taker_buy_quote_volume",
        }
        symbols: set[str] = set()
        qv_ranks: set[int] = set()
        b_ranks: set[int] = set()
        c_ranks: set[int] = set()
        for row in payload["rows"]:
            if type(row) is not dict or set(row) != row_keys:
                raise ValueError
            symbol = row["symbol"]
            qv_rank, b_rank, c_rank = (
                row["quote_volume_rank"], row["rank_b"], row["rank_c"]
            )
            if (
                type(symbol) is not str or not symbol
                or type(qv_rank) is not int or type(b_rank) is not int
                or type(c_rank) is not int
                or not 1 <= qv_rank <= 100
                or not 1 <= b_rank <= 100 or not 1 <= c_rank <= 100
                or type(row["eligible"]) is not bool
                or type(row["reason"]) is not str or not row["reason"]
            ):
                raise ValueError
            symbols.add(symbol)
            qv_ranks.add(qv_rank)
            b_ranks.add(b_rank)
            c_ranks.add(c_rank)
            for candle_name, expected_time in (
                ("b", expected_e_open_time_ms - 2 * INTERVAL_MS),
                ("c", expected_e_open_time_ms - INTERVAL_MS),
            ):
                candle = row[candle_name]
                if type(candle) is not dict or set(candle) != candle_keys:
                    raise ValueError
                if (
                    type(candle["open_time_ms"]) is not int
                    or candle["open_time_ms"] != expected_time
                    or any(
                        type(candle[name]) is not str
                        for name in (
                            "open", "high", "low", "close",
                            "quote_volume", "taker_buy_quote_volume",
                        )
                    )
                ):
                    raise ValueError
                open_price, high, low, close = (
                    _d(candle[name]) for name in ("open", "high", "low", "close")
                )
                volume = _d(candle["quote_volume"])
                taker = _d(candle["taker_buy_quote_volume"])
                if (
                    min(open_price, high, low, close) <= 0
                    or high < max(open_price, close)
                    or low > min(open_price, close)
                    or volume < 0 or taker < 0 or taker > volume
                ):
                    raise ValueError
            if any(
                type(row[name]) is not str
                for name in (
                    "atr_b_pre", "atr_c_pre", "v20_c", "move_b",
                    "move_c", "p",
                )
            ):
                raise ValueError
            if _d(row["atr_b_pre"]) <= 0 or _d(row["atr_c_pre"]) <= 0:
                raise ValueError
            if _d(row["v20_c"]) < 0 or _d(row["p"]) <= 0:
                raise ValueError
            _d(row["move_b"])
            _d(row["move_c"])
            for name in ("close_location_c", "taker_ratio_c"):
                if row[name] is not None:
                    if type(row[name]) is not str:
                        raise ValueError
                    if not Decimal("0") <= _d(row[name]) <= Decimal("1"):
                        raise ValueError
            if _d(row["p"]) != min(_d(row["b"]["low"]), _d(row["c"]["low"])):
                raise ValueError
        full_ranks = set(range(1, 101))
        if (
            len(symbols) != 100 or qv_ranks != full_ranks
            or b_ranks != full_ranks or c_ranks != full_ranks
        ):
            raise ValueError
        rows = payload["rows"]
        recalculated = []
        for row in rows:
            atr_b = _d(row["atr_b_pre"])
            atr_c = _d(row["atr_c_pre"])
            b_open = _d(row["b"]["open"])
            b_close = _d(row["b"]["close"])
            c_open = _d(row["c"]["open"])
            c_high = _d(row["c"]["high"])
            c_low = _d(row["c"]["low"])
            c_close = _d(row["c"]["close"])
            c_volume = _d(row["c"]["quote_volume"])
            c_taker = _d(row["c"]["taker_buy_quote_volume"])
            v20 = _d(row["v20_c"])
            move_b = (b_close - b_open) / atr_b
            move_c = (c_close - c_open) / atr_c
            metrics_valid = v20 > 0 and c_volume > 0 and c_high > c_low
            location = (
                (c_close - c_low) / (c_high - c_low)
                if c_high > c_low else None
            )
            taker_ratio = c_taker / c_volume if c_volume > 0 else None
            if (
                _d(row["move_b"]) != move_b
                or _d(row["move_c"]) != move_c
                or (location is None and row["close_location_c"] is not None)
                or (
                    location is not None
                    and (
                        row["close_location_c"] is None
                        or _d(row["close_location_c"]) != location
                    )
                )
                or (taker_ratio is None and row["taker_ratio_c"] is not None)
                or (
                    taker_ratio is not None
                    and (
                        row["taker_ratio_c"] is None
                        or _d(row["taker_ratio_c"]) != taker_ratio
                    )
                )
            ):
                raise ValueError
            recalculated.append({
                "symbol": row["symbol"],
                "quote_volume_rank": row["quote_volume_rank"],
                "move_b": move_b,
                "move_c": move_c,
                "b": row["b"],
                "c": row["c"],
                "v20": v20,
                "metrics_valid": metrics_valid,
                "location": location,
                "taker_ratio": taker_ratio,
            })
        rank_b = _rank(recalculated, "move_b")
        rank_c = _rank(recalculated, "move_c")
        count = Decimal("100")
        up_b = Decimal(sum(item["move_b"] > 0 for item in recalculated)) / count
        down_b = Decimal(sum(item["move_b"] < 0 for item in recalculated)) / count
        up_c = Decimal(sum(item["move_c"] > 0 for item in recalculated)) / count
        down_c = Decimal(sum(item["move_c"] < 0 for item in recalculated)) / count
        median_b = exact_median(item["move_b"] for item in recalculated)
        median_c = exact_median(item["move_c"] for item in recalculated)
        if (
            _d(payload["up_b"]) != up_b
            or _d(payload["down_b"]) != down_b
            or _d(payload["up_c"]) != up_c
            or _d(payload["down_c"]) != down_c
            or _d(payload["median_move_b"]) != median_b
            or _d(payload["median_move_c"]) != median_c
        ):
            raise ValueError
        cfg = payload["config_signature"]
        crash = (
            down_b >= _d(cfg["crash_down_breadth_min"])
            and median_b <= _d(cfg["crash_median_move_max"])
        )
        weak = (
            down_b >= _d(cfg["weak_down_breadth_min"])
            and median_b <= _d(cfg["weak_median_move_max"])
        )
        recovery = (
            up_c >= _d(cfg["recovery_up_breadth_min"])
            and up_c - up_b >= _d(cfg["recovery_improvement_min"])
        )
        if payload["systemic_crash_veto"] is not crash:
            raise ValueError
        eligible_rows = []
        row_by_symbol = {row["symbol"]: row for row in rows}
        for item in recalculated:
            b, c = item["b"], item["c"]
            reason = "N15_CANDIDATE"
            if crash:
                reason = "N15_SYSTEMIC_CRASH_VETO"
            elif not weak:
                reason = "N15_WEAK_MARKET_NOT_MET"
            elif not recovery:
                reason = "N15_MARKET_RECOVERY_NOT_MET"
            elif not item["metrics_valid"]:
                reason = "N15_C_TARGET_METRICS_INVALID"
            elif rank_b[item["symbol"]] > cfg["b_rank_max"]:
                reason = "N15_B_RANK_OUT_OF_RANGE"
            elif item["move_b"] < _d(cfg["b_move_min"]):
                reason = "N15_B_NOT_RESILIENT"
            elif _d(c["close"]) <= _d(c["open"]):
                reason = "N15_C_NOT_BULLISH"
            elif _d(c["low"]) < _d(b["low"]):
                reason = "N15_C_LOW_BROKE_B"
            elif _d(c["close"]) <= _d(b["high"]):
                reason = "N15_C_NOT_BREAKOUT"
            elif rank_c[item["symbol"]] > cfg["c_rank_max"]:
                reason = "N15_C_RANK_OUT_OF_RANGE"
            elif item["location"] < _d(cfg["c_close_location_min"]):
                reason = "N15_C_CLOSE_LOCATION_TOO_LOW"
            elif item["taker_ratio"] < _d(cfg["c_taker_buy_ratio_min"]):
                reason = "N15_C_TAKER_BUY_RATIO_TOO_LOW"
            elif _d(c["quote_volume"]) < _d(cfg["c_volume_median_multiple_min"]) * item["v20"]:
                reason = "N15_C_VOLUME_TOO_LOW"
            row = row_by_symbol[item["symbol"]]
            if (
                row["rank_b"] != rank_b[item["symbol"]]
                or row["rank_c"] != rank_c[item["symbol"]]
                or row["reason"] != reason
                or row["eligible"] is not (reason == "N15_CANDIDATE")
            ):
                raise ValueError
            if row["eligible"]:
                eligible_rows.append(row)
        expected_winner = min(
            eligible_rows,
            key=lambda row: (
                -_d(row["move_c"]), row["rank_b"],
                -_d(row["taker_ratio_c"]), row["quote_volume_rank"],
                row["symbol"],
            ),
            default=None,
        )
        winner = payload["winner_symbol"]
        if winner != (expected_winner["symbol"] if expected_winner else None):
            raise ValueError
        snapshot = _snapshot_from_payload(payload)
        if payload["schema_version"] == N15_SNAPSHOT_SCHEMA_VERSION:
            if not _exact(
                expected_config_signature,
                n15_config_signature(strategy),
            ):
                raise ValueError
            members = [
                (row["symbol"], row["quote_volume_rank"])
                for row in payload["rows"]
            ]
            metric_raw = _raw_from_metric_source(payload, strategy)
            expected_unsigned = _unsigned_snapshot(
                strategy, members, metric_raw
            )
            expected_unsigned["schema_version"] = (
                N15_SNAPSHOT_SCHEMA_VERSION
            )
            expected_unsigned["metric_source"] = payload["metric_source"]
            expected_payload = {
                **expected_unsigned,
                "canonical_sha256": _hash(expected_unsigned),
            }
            if not _exact(payload, expected_payload):
                raise ValueError
        return snapshot
    except (ArithmeticError, InvalidOperation, KeyError, TypeError, ValueError) as exc:
        raise ValueError("N15 snapshot envelope inconsistent") from exc


def decode_n15_snapshot(
    payload: dict[str, Any],
    strategy: Any,
    raw_by_symbol: Mapping[str, list[list[Any]]],
    expected_e_open_time_ms: int,
) -> N15Snapshot:
    try:
        snapshot = validate_n15_snapshot_envelope(
            payload,
            strategy.strategy_id,
            expected_e_open_time_ms,
            n15_config_signature(strategy),
            strategy,
        )
        members = []
        for row in payload["rows"]:
            if type(row) is not dict:
                raise ValueError
            members.append((row.get("symbol"), row.get("quote_volume_rank")))
        parsed_current: dict[str, list[N15Candle]] = {}
        for symbol, _ in members:
            source = raw_by_symbol.get(symbol)
            if source is None:
                raise N15SnapshotSourceUnavailableError(
                    "N15 frozen member kline missing"
                )
            try:
                candles = parse_n15_klines(source)
            except (
                ArithmeticError,
                InvalidOperation,
                KeyError,
                TypeError,
                ValueError,
            ) as exc:
                raise N15SnapshotSourceUnavailableError(
                    "N15 frozen member kline invalid"
                ) from exc
            if (
                not candles
                or candles[-1].open_time_ms != expected_e_open_time_ms
            ):
                raise N15SnapshotSourceUnavailableError(
                    "N15 frozen member E unavailable"
                )
            if (
                payload["schema_version"] == 1
                and len(candles) < N15_MIN_RUNTIME_CANDLES
            ):
                raise N15SnapshotSourceUnavailableError(
                    "N15 legacy source window incomplete"
                )
            parsed_current[symbol] = candles
        if payload["schema_version"] == N15_SNAPSHOT_SCHEMA_VERSION:
            metric_raw = _raw_from_metric_source(payload, strategy)
            for symbol, _ in members:
                current_by_time = {
                    candle.open_time_ms: candle
                    for candle in parsed_current[symbol]
                }
                frozen = parse_n15_klines(metric_raw[symbol])
                required_times = {
                    candle.open_time_ms for candle in frozen[:-1]
                }
                if not required_times.issubset(current_by_time):
                    raise N15SnapshotSourceUnavailableError(
                        "N15 frozen metric source window incomplete"
                    )
                for candle in frozen[:-2]:
                    current_candle = current_by_time[candle.open_time_ms]
                    if (
                        candle.open != current_candle.open
                        or candle.high != current_candle.high
                        or candle.low != current_candle.low
                        or candle.close != current_candle.close
                        or candle.quote_volume
                        != current_candle.quote_volume
                    ):
                        raise ValueError
                row = snapshot.rows[symbol]
                if (
                    current_by_time.get(row.b.open_time_ms) != row.b
                    or current_by_time.get(row.c.open_time_ms) != row.c
                ):
                    raise ValueError
            return snapshot
        try:
            expected_unsigned = _unsigned_snapshot(
                strategy, members, raw_by_symbol
            )
        except (
            ArithmeticError,
            InvalidOperation,
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            raise N15SnapshotSourceUnavailableError(
                "N15 frozen market context unavailable"
            ) from exc
        expected = {
            **expected_unsigned,
            "canonical_sha256": _hash(expected_unsigned),
        }
        actual = payload
        if not _exact(actual, expected):
            raise ValueError
        return snapshot
    except N15SnapshotSourceUnavailableError:
        raise
    except (ArithmeticError, InvalidOperation, KeyError, TypeError, ValueError) as exc:
        raise ValueError("N15 snapshot inconsistent") from exc


def validate_n15_snapshot_upgrade_pair(
    legacy_payload: dict[str, Any],
    upgraded_payload: dict[str, Any],
    strategy_id: str,
    expected_e_open_time_ms: int,
    strategy: Any | None = None,
) -> None:
    try:
        if strategy is None:
            from .strategies import N15_STRATEGY
            strategy = N15_STRATEGY
        if (
            type(legacy_payload) is not dict
            or legacy_payload.get("schema_version") != 1
            or type(upgraded_payload) is not dict
            or upgraded_payload.get("schema_version")
            != N15_SNAPSHOT_SCHEMA_VERSION
        ):
            raise ValueError
        signature = n15_config_signature(strategy)
        validate_n15_snapshot_envelope(
            legacy_payload,
            strategy_id,
            expected_e_open_time_ms,
            signature,
            strategy,
        )
        validate_n15_snapshot_envelope(
            upgraded_payload,
            strategy_id,
            expected_e_open_time_ms,
            signature,
            strategy,
        )
        legacy_unsigned = {
            key: value for key, value in upgraded_payload.items()
            if key not in {"metric_source", "canonical_sha256"}
        }
        legacy_unsigned["schema_version"] = 1
        legacy_projection = {
            **legacy_unsigned,
            "canonical_sha256": _hash(legacy_unsigned),
        }
        if not _exact(legacy_payload, legacy_projection):
            raise ValueError
    except (
        ArithmeticError,
        InvalidOperation,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        raise ValueError("N15 snapshot upgrade pair inconsistent") from exc


def upgrade_n15_snapshot_payload(
    payload: dict[str, Any],
    strategy: Any,
    raw_by_symbol: Mapping[str, list[list[Any]]],
    expected_e_open_time_ms: int,
) -> tuple[dict[str, Any], N15Snapshot]:
    try:
        if type(payload) is not dict or payload.get("schema_version") != 1:
            raise ValueError
        decode_n15_snapshot(
            payload, strategy, raw_by_symbol, expected_e_open_time_ms
        )
        members = [
            (row["symbol"], row["quote_volume_rank"])
            for row in payload["rows"]
        ]
        if any(
            len(parse_n15_klines(raw_by_symbol[symbol]))
            != N15_MIN_RUNTIME_CANDLES
            for symbol, _ in members
        ):
            raise N15SnapshotSourceUnavailableError(
                "N15 legacy upgrade requires fixed 122-candle source"
            )
        upgraded, snapshot = build_n15_snapshot(
            strategy, members, raw_by_symbol
        )
        validate_n15_snapshot_upgrade_pair(
            payload,
            upgraded,
            strategy.strategy_id,
            expected_e_open_time_ms,
            strategy,
        )
        return upgraded, snapshot
    except N15SnapshotSourceUnavailableError:
        raise
    except (ArithmeticError, InvalidOperation, KeyError, TypeError, ValueError) as exc:
        raise ValueError("N15 legacy snapshot cannot be upgraded") from exc


def _true_range(candle: N15Candle, previous_close: Decimal) -> Decimal:
    return max(
        candle.high - candle.low,
        abs(candle.high - previous_close),
        abs(candle.low - previous_close),
    )


def _validate_one_seed_legacy_replay(
    snapshot: N15Snapshot,
    strategy: Any,
    available_prefixes: Mapping[str, list[list[Any]]],
    expected_e_open_time_ms: int,
) -> None:
    period = strategy.n15_atr_period
    volume_bars = strategy.n15_volume_median_bars
    if period < 2:
        raise ValueError
    for symbol, row in snapshot.rows.items():
        candles = parse_n15_klines(available_prefixes[symbol])
        if (
            len(candles) != 121
            or candles[0].open_time_ms
            != expected_e_open_time_ms - 120 * INTERVAL_MS
            or candles[-1].open_time_ms != expected_e_open_time_ms
            or candles[-3] != row.b
            or candles[-2] != row.c
        ):
            raise ValueError
        c_index = len(candles) - 2
        volume_window = candles[c_index - volume_bars:c_index]
        if (
            len(volume_window) != volume_bars
            or exact_median(
                candle.quote_volume for candle in volume_window
            ) != row.v20_c
        ):
            raise ValueError
        b_true_range = _true_range(row.b, candles[-4].close)
        expected_atr_c = (
            row.atr_b_pre * Decimal(period - 1) + b_true_range
        ) / Decimal(period)
        if expected_atr_c != row.atr_c_pre:
            raise ValueError
        minimum_seed_sum = (
            candles[0].high - candles[0].low
        ) + sum(
            (
                _true_range(
                    candles[index], candles[index - 1].close,
                )
                for index in range(1, period - 1)
            ),
            Decimal("0"),
        )
        minimum_atr_b_pre = minimum_seed_sum / Decimal(period)
        for index in range(period - 1, 118):
            minimum_atr_b_pre = (
                minimum_atr_b_pre * Decimal(period - 1)
                + _true_range(candles[index], candles[index - 1].close)
            ) / Decimal(period)
        if (
            not minimum_atr_b_pre.is_finite()
            or minimum_atr_b_pre < 0
            or row.atr_b_pre < minimum_atr_b_pre
        ):
            raise ValueError


def decode_n15_historical_snapshot(
    payload: dict[str, Any],
    strategy: Any,
    raw_by_symbol: Mapping[str, list[list[Any]]],
    expected_e_open_time_ms: int,
) -> N15Snapshot:
    try:
        snapshot = validate_n15_snapshot_envelope(
            payload,
            strategy.strategy_id,
            expected_e_open_time_ms,
            n15_config_signature(strategy),
            strategy,
        )
        available_prefixes: dict[str, list[list[Any]]] = {}
        expired = 0
        for row in payload["rows"]:
            symbol = row["symbol"]
            source = raw_by_symbol.get(symbol)
            if source is None:
                raise N15HistoricalSourceUnavailableError(
                    "N15 historical frozen member unavailable"
                )
            try:
                parsed = parse_n15_klines(source)
            except (ArithmeticError, InvalidOperation, KeyError, TypeError, ValueError) as exc:
                raise N15HistoricalSourceUnavailableError(
                    "N15 historical market data invalid"
                ) from exc
            matches = [
                index for index, candle in enumerate(parsed)
                if candle.open_time_ms == expected_e_open_time_ms
            ]
            if len(matches) == 1:
                available_prefixes[symbol] = source[:matches[0] + 1]
            elif (
                matches
                or not parsed
                or parsed[0].open_time_ms <= expected_e_open_time_ms
            ):
                raise N15HistoricalSourceUnavailableError(
                    "N15 historical E unavailable"
                )
            else:
                expired += 1
        if available_prefixes and expired:
            raise N15HistoricalSourceUnavailableError(
                "N15 historical window partially available"
            )
        if payload["schema_version"] == 1:
            if expired == 100:
                raise N15HistoricalSourceUnavailableError(
                    "N15 legacy seed evidence unavailable"
                )
            if len(available_prefixes) != 100:
                raise N15HistoricalSourceUnavailableError(
                    "N15 historical frozen members incomplete"
                )
            one_seed_shift = all(
                len(prefix) == 121
                and type(prefix[0][0]) is int
                and prefix[0][0]
                == expected_e_open_time_ms - 120 * INTERVAL_MS
                for prefix in available_prefixes.values()
            )
            if not one_seed_shift:
                try:
                    return decode_n15_snapshot(
                        payload,
                        strategy,
                        available_prefixes,
                        expected_e_open_time_ms,
                    )
                except N15SnapshotSourceUnavailableError as exc:
                    raise N15HistoricalSourceUnavailableError(
                        str(exc)
                    ) from exc
            _validate_one_seed_legacy_replay(
                snapshot,
                strategy,
                available_prefixes,
                expected_e_open_time_ms,
            )
            return snapshot
        if expired == 100:
            return snapshot
        if len(available_prefixes) != 100:
            raise N15HistoricalSourceUnavailableError(
                "N15 historical frozen members incomplete"
            )
        metric_raw = _raw_from_metric_source(payload, strategy)
        for symbol, prefix in available_prefixes.items():
            current = parse_n15_klines(prefix)
            frozen = parse_n15_klines(metric_raw[symbol])
            if len(current) < 3:
                raise N15HistoricalSourceUnavailableError(
                    "N15 historical prefix incomplete"
                )
            current_by_time = {
                candle.open_time_ms: candle for candle in current
            }
            frozen_source = frozen[:-2]
            overlaps = [
                candle for candle in frozen_source
                if candle.open_time_ms in current_by_time
            ]
            if (
                not overlaps
                or frozen_source[-1].open_time_ms
                not in current_by_time
            ):
                raise ValueError
            for candle in overlaps:
                current_candle = current_by_time[candle.open_time_ms]
                if (
                    candle.open != current_candle.open
                    or candle.high != current_candle.high
                    or candle.low != current_candle.low
                    or candle.close != current_candle.close
                    or candle.quote_volume
                    != current_candle.quote_volume
                ):
                    raise ValueError
            row = snapshot.rows[symbol]
            if (
                current_by_time.get(row.b.open_time_ms) != row.b
                or current_by_time.get(row.c.open_time_ms) != row.c
                or current[-1].open_time_ms != expected_e_open_time_ms
            ):
                raise ValueError
        return snapshot
    except N15HistoricalSourceUnavailableError:
        raise
    except (ArithmeticError, InvalidOperation, KeyError, TypeError, ValueError) as exc:
        raise ValueError("N15 historical snapshot inconsistent") from exc
