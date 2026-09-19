from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .n15_snapshot import N15Candle, N15Snapshot, parse_n15_klines


@dataclass(frozen=True)
class N15Structure:
    symbol: str
    b: N15Candle
    c: N15Candle
    entry: N15Candle
    p: Decimal
    atr_b_pre: Decimal
    atr_c_pre: Decimal
    v20_c: Decimal
    move_b: Decimal
    move_c: Decimal
    rank_b: int
    rank_c: int
    quote_volume_rank: int
    entry_min_price: Decimal
    entry_max_price: Decimal
    structure_id: str

    def json(self) -> dict[str, Any]:
        return {
            "structure_id": self.structure_id,
            "symbol": self.symbol,
            "b": self.b.json(), "c": self.c.json(), "entry": self.entry.json(),
            "p": str(self.p), "atr_b_pre": str(self.atr_b_pre),
            "atr_c_pre": str(self.atr_c_pre), "v20_c": str(self.v20_c),
            "move_b": str(self.move_b), "move_c": str(self.move_c),
            "rank_b": self.rank_b, "rank_c": self.rank_c,
            "quote_volume_rank": self.quote_volume_rank,
            "candidate_universe": "quote_volume_top_frozen_n15",
            "entry_min_price": str(self.entry_min_price),
            "entry_max_price": str(self.entry_max_price),
        }


@dataclass(frozen=True)
class N15AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    structure: N15Structure | None
    consume_current: bool
    elapsed_ms: int | None
    winner_symbol: str | None

    @property
    def structure_id(self) -> str | None:
        return self.structure.structure_id if self.structure else None

    @property
    def current_bullish(self) -> bool:
        return bool(self.structure and self.structure.entry.close > self.structure.entry.open)

    def detail_json(self) -> dict[str, Any]:
        return {
            "event_type": "CURRENT_ENTRY",
            "strategy_id": "N15",
            "e_open_time_ms": (
                self.structure.entry.open_time_ms if self.structure else None
            ),
            "symbol": self.symbol,
            "reason": self.reason,
            "consume_current": self.consume_current,
            "elapsed_ms": self.elapsed_ms,
            "winner_symbol": self.winner_symbol,
            "structure_id": self.structure_id,
            "structure": self.structure.json() if self.structure else None,
        }


N15_CURRENT_MISSED_REASONS = {
    "N15_ENTRY_WINDOW_EXPIRED",
    "N15_ENTRY_LOW_BROKE_P",
    "N15_ENTRY_PRICE_TOO_EXTENDED",
}


def historical_n15_missed_detail(
    strategy_id: str,
    e_open_time_ms: int,
    symbol: str,
    structure_id: str,
    row: Any,
) -> dict[str, Any]:
    return {
        "event_type": "HISTORICAL_MISSED",
        "strategy_id": strategy_id,
        "e_open_time_ms": e_open_time_ms,
        "symbol": symbol,
        "reason": "HISTORICAL_N15_ENTRY_MISSED",
        "structure_id": structure_id,
        "winner_symbol": symbol,
        "structure": {
            "structure_id": structure_id,
            "symbol": symbol,
            "b": row.b.json(),
            "c": row.c.json(),
            "entry_open_time_ms": e_open_time_ms,
            "p": str(row.p),
            "quote_volume_rank": row.quote_volume_rank,
            "candidate_universe": "quote_volume_top_frozen_n15",
        },
    }


def validate_n15_state_envelope(
    strategy_id: str,
    e_time: str,
    symbol: str,
    structure_id: str,
    status: str,
    reason: str,
    detail: dict[str, Any],
) -> None:
    try:
        e_open_time_ms = int(e_time)
        if (
            str(e_open_time_ms) != e_time
            or e_open_time_ms <= 0
            or e_open_time_ms % 900_000 != 0
        ):
            raise ValueError
        current_detail_keys = {
            "event_type", "strategy_id", "e_open_time_ms", "symbol",
            "reason", "consume_current", "elapsed_ms", "winner_symbol",
            "structure_id", "structure",
        }
        historical_detail_keys = {
            "event_type", "strategy_id", "e_open_time_ms", "symbol",
            "reason", "structure_id", "winner_symbol", "structure",
        }
        if (
            strategy_id != "N15"
            or type(symbol) is not str or not symbol
            or type(structure_id) is not str or not structure_id
            or type(detail) is not dict
            or detail.get("strategy_id") != strategy_id
            or detail.get("e_open_time_ms") != e_open_time_ms
            or detail.get("symbol") != symbol
            or detail.get("structure_id") != structure_id
            or detail.get("winner_symbol") != symbol
            or detail.get("reason") != reason
        ):
            raise ValueError
        structure = detail.get("structure")
        if (
            type(structure) is not dict
            or structure.get("structure_id") != structure_id
        ):
            raise ValueError
        expected_id = n15_structure_id(
            strategy_id,
            symbol,
            structure["b"]["open_time_ms"],
            structure["c"]["open_time_ms"],
        )
        if expected_id != structure_id:
            raise ValueError

        def candle(value: Any, expected_time: int) -> dict[str, Decimal]:
            keys = {
                "open_time_ms", "open", "high", "low", "close",
                "quote_volume", "taker_buy_quote_volume",
            }
            if type(value) is not dict or set(value) != keys:
                raise ValueError
            if (
                type(value["open_time_ms"]) is not int
                or value["open_time_ms"] != expected_time
            ):
                raise ValueError
            parsed = {}
            for name in (
                "open", "high", "low", "close", "quote_volume",
                "taker_buy_quote_volume",
            ):
                if type(value[name]) is not str:
                    raise ValueError
                parsed[name] = Decimal(value[name])
                if not parsed[name].is_finite():
                    raise ValueError
            if (
                min(parsed[name] for name in ("open", "high", "low", "close")) <= 0
                or parsed["high"] < max(parsed["open"], parsed["close"])
                or parsed["low"] > min(parsed["open"], parsed["close"])
                or parsed["quote_volume"] < 0
                or parsed["taker_buy_quote_volume"] < 0
                or parsed["taker_buy_quote_volume"] > parsed["quote_volume"]
            ):
                raise ValueError
            return parsed

        b = candle(structure["b"], e_open_time_ms - 2 * 900_000)
        c = candle(structure["c"], e_open_time_ms - 900_000)
        expected_p = min(b["low"], c["low"])
        if type(structure.get("p")) is not str or Decimal(structure["p"]) != expected_p:
            raise ValueError
        if detail.get("event_type") == "CURRENT_ENTRY":
            current_structure_keys = {
                "structure_id", "symbol", "b", "c", "entry", "p",
                "atr_b_pre", "atr_c_pre", "v20_c", "move_b", "move_c",
                "rank_b", "rank_c", "quote_volume_rank",
                "candidate_universe",
                "entry_min_price", "entry_max_price",
            }
            if set(detail) != current_detail_keys or set(structure) != current_structure_keys:
                raise ValueError
            entry = candle(structure["entry"], e_open_time_ms)
            decimal_names = (
                "atr_b_pre", "atr_c_pre", "v20_c", "move_b", "move_c",
                "entry_min_price", "entry_max_price",
            )
            decimals = {}
            for name in decimal_names:
                if type(structure[name]) is not str:
                    raise ValueError
                decimals[name] = Decimal(structure[name])
                if not decimals[name].is_finite():
                    raise ValueError
            if decimals["atr_b_pre"] <= 0 or decimals["atr_c_pre"] <= 0 or decimals["v20_c"] <= 0:
                raise ValueError
            for name in ("rank_b", "rank_c", "quote_volume_rank"):
                if type(structure[name]) is not int or not 1 <= structure[name] <= 100:
                    raise ValueError
            elapsed = detail.get("elapsed_ms")
            if (
                (status == "CONSUMED" and reason != "PASSED")
                or (status == "MISSED" and reason not in N15_CURRENT_MISSED_REASONS)
                or status not in {"CONSUMED", "MISSED"}
                or structure.get("symbol") != symbol
                or detail.get("consume_current") is not True
                or type(structure.get("candidate_universe")) is not str
                or structure.get("candidate_universe")
                != "quote_volume_top_frozen_n15"
                or type(elapsed) is not int
                or elapsed < 0
                or (reason == "N15_ENTRY_WINDOW_EXPIRED" and elapsed < 120_000)
                or (reason != "N15_ENTRY_WINDOW_EXPIRED" and elapsed >= 120_000)
                or decimals["move_b"] != (b["close"] - b["open"]) / decimals["atr_b_pre"]
                or decimals["move_c"] != (c["close"] - c["open"]) / decimals["atr_c_pre"]
                or decimals["entry_min_price"] != c["close"]
                or decimals["entry_max_price"] != c["close"] + Decimal("0.50") * decimals["atr_c_pre"]
                or (reason == "N15_ENTRY_LOW_BROKE_P" and entry["low"] >= expected_p)
                or (
                    reason == "N15_ENTRY_PRICE_TOO_EXTENDED"
                    and (
                        entry["low"] < expected_p
                        or entry["close"] <= decimals["entry_max_price"]
                    )
                )
                or (
                    reason == "PASSED"
                    and not (
                        entry["low"] >= expected_p
                        and decimals["entry_min_price"] <= entry["close"] <= decimals["entry_max_price"]
                        and elapsed < 120_000
                    )
                )
            ):
                raise ValueError
        elif detail.get("event_type") == "HISTORICAL_MISSED":
            historical_structure_keys = {
                "structure_id", "symbol", "b", "c",
                "entry_open_time_ms", "p", "quote_volume_rank",
                "candidate_universe",
            }
            if (
                set(detail) != historical_detail_keys
                or set(structure) != historical_structure_keys
                or status != "MISSED"
                or reason != "HISTORICAL_N15_ENTRY_MISSED"
                or structure.get("symbol") != symbol
                or type(structure.get("entry_open_time_ms")) is not int
                or structure.get("entry_open_time_ms") != e_open_time_ms
                or type(structure.get("quote_volume_rank")) is not int
                or not 1 <= structure["quote_volume_rank"] <= 100
                or type(structure.get("candidate_universe")) is not str
                or structure.get("candidate_universe")
                != "quote_volume_top_frozen_n15"
            ):
                raise ValueError
        else:
            raise ValueError
    except (ArithmeticError, KeyError, TypeError, ValueError) as exc:
        raise ValueError("N15 state envelope inconsistent") from exc


def n15_structure_id(strategy_id: str, symbol: str, b_time: int, c_time: int) -> str:
    raw = f"{strategy_id}|{symbol}|{b_time}|{c_time}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def analyze_n15_breadth_recovery_leader(
    symbol: str,
    raw_klines: list[list[Any]],
    snapshot: N15Snapshot | None,
    *,
    snapshot_context_complete: bool,
    checked_at_ms: int,
    entry_window_seconds: int = 120,
    entry_extension_atr_max: Decimal = Decimal("0.50"),
) -> N15AnalysisResult:
    def result(reason, structure=None, passed=False, consume=False, elapsed=None):
        return N15AnalysisResult(
            symbol, passed, reason, structure, consume, elapsed,
            snapshot.winner_symbol if snapshot else None,
        )
    if not snapshot_context_complete or snapshot is None:
        return result("N15_MARKET_CONTEXT_INSUFFICIENT")
    if snapshot.winner_symbol is None:
        return result("N15_NO_WINNER")
    if symbol != snapshot.winner_symbol:
        return result("N15_NOT_WINNER")
    try:
        candles = parse_n15_klines(raw_klines)
        entry = candles[-1]
        row = snapshot.rows[symbol]
        if entry.open_time_ms != snapshot.e_open_time_ms:
            raise ValueError
        entry_max = row.c.close + entry_extension_atr_max * row.atr_c_pre
        structure = N15Structure(
            symbol, row.b, row.c, entry, row.p, row.atr_b_pre,
            row.atr_c_pre, row.v20_c, row.move_b, row.move_c,
            row.rank_b, row.rank_c, row.quote_volume_rank,
            row.c.close, entry_max,
            n15_structure_id(snapshot.strategy_id, symbol, row.b.open_time_ms, row.c.open_time_ms),
        )
    except (ArithmeticError, KeyError, TypeError, ValueError):
        return result("N15_MARKET_CONTEXT_INSUFFICIENT")
    elapsed = checked_at_ms - entry.open_time_ms
    if elapsed < 0:
        return result("N15_MARKET_CONTEXT_INSUFFICIENT", structure, elapsed=elapsed)
    if elapsed >= entry_window_seconds * 1000:
        return result("N15_ENTRY_WINDOW_EXPIRED", structure, consume=True, elapsed=elapsed)
    if entry.low < structure.p:
        return result("N15_ENTRY_LOW_BROKE_P", structure, consume=True, elapsed=elapsed)
    if entry.close < structure.entry_min_price:
        return result("N15_WAITING_ENTRY_PRICE", structure, elapsed=elapsed)
    if entry.close > structure.entry_max_price:
        return result("N15_ENTRY_PRICE_TOO_EXTENDED", structure, consume=True, elapsed=elapsed)
    return result("PASSED", structure, passed=True, consume=True, elapsed=elapsed)
