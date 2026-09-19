from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class Candle:
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal

    @property
    def bullish(self) -> bool:
        return self.close > self.open


@dataclass(frozen=True)
class AnalysisResult:
    symbol: str
    passed: bool
    trend_slope: Decimal
    pattern: str | None
    current_bullish: bool
    detail: str
    matched_patterns: tuple[str, ...] = ()


PATTERN_NAMES = {
    "A": "A_3_BULLISH_CANDLES",
    "B": "B_5_CLOSES_UPTREND",
    "C": "C_UP_PULLBACK_BOUNCE",
}


def linear_regression_slope(values: list[Decimal]) -> Decimal:
    n = len(values)
    if n < 2:
        return Decimal("0")
    xs = [Decimal(i) for i in range(n)]
    mean_x = sum(xs) / Decimal(n)
    mean_y = sum(values) / Decimal(n)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, values))
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        return Decimal("0")
    return numerator / denominator


def parse_klines(raw_klines: list[list[Any]]) -> list[Candle]:
    candles = []
    for row in raw_klines:
        candles.append(
            Candle(
                open=Decimal(str(row[1])),
                high=Decimal(str(row[2])),
                low=Decimal(str(row[3])),
                close=Decimal(str(row[4])),
            )
        )
    return candles


def _pattern_a(closed: list[Candle]) -> bool:
    return len(closed) >= 3 and all(candle.bullish for candle in closed[-3:])


def _pattern_b(closed: list[Candle]) -> bool:
    return len(closed) >= 5 and linear_regression_slope([c.close for c in closed[-5:]]) > 0


def _pattern_c(closed: list[Candle]) -> bool:
    if len(closed) < 20 or not all(c.bullish for c in closed[-2:]):
        return False

    for window in range(10, 21):
        segment = closed[-window:]
        first_end = max(3, window // 3)
        second_end = max(first_end + 3, (window * 2) // 3)
        first = segment[:first_end]
        middle = segment[first_end:second_end]
        last = segment[second_end:]
        if len(last) < 2:
            continue

        first_slope = linear_regression_slope([c.close for c in first])
        last_slope = linear_regression_slope([c.close for c in last])
        first_high = max(c.close for c in first)
        middle_low = min(c.close for c in middle)
        last_close = segment[-1].close

        if first_slope > 0 and middle_low < first_high and last_slope > 0 and last_close > middle_low:
            return True
    return False


def analyze_symbol(
    symbol: str,
    raw_klines: list[list[Any]],
    current_price: Decimal,
    trend_window: int,
    allowed_patterns: set[str] | tuple[str, ...] | None = None,
) -> AnalysisResult:
    candles = parse_klines(raw_klines)
    if len(candles) < trend_window + 1:
        return AnalysisResult(symbol, False, Decimal("0"), None, False, "not enough kline data")

    current = candles[-1]
    closed = candles[:-1]
    trend_closes = [c.close for c in closed[-trend_window:]]
    trend_slope = linear_regression_slope(trend_closes)
    trend_passed = trend_slope > 0

    pattern_a = _pattern_a(closed)
    pattern_b = _pattern_b(closed)
    pattern_c = _pattern_c(closed)
    matched_patterns = tuple(
        pattern
        for pattern, matched in (
            ("A", pattern_a),
            ("B", pattern_b),
            ("C", pattern_c),
        )
        if matched
    )

    pattern = None
    if pattern_a:
        pattern = PATTERN_NAMES["A"]
    elif pattern_c:
        pattern = PATTERN_NAMES["C"]
    elif pattern_b:
        pattern = PATTERN_NAMES["B"]

    current_bullish = current_price > current.open
    allowed_pattern_set = set(allowed_patterns or ("A", "B", "C"))
    entry_pattern_passed = bool(set(matched_patterns) & allowed_pattern_set)
    passed = trend_passed and entry_pattern_passed and current_bullish
    detail = (
        f"trend={trend_passed} slope={trend_slope} "
        f"pattern={pattern or 'NONE'} entry_pattern={entry_pattern_passed} "
        f"current_bullish={current_bullish} matched_patterns={','.join(matched_patterns) or 'NONE'} "
        f"allowed_patterns={','.join(sorted(allowed_pattern_set)) or 'NONE'}"
    )
    return AnalysisResult(symbol, passed, trend_slope, pattern, current_bullish, detail, matched_patterns)
