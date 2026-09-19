from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Any, Mapping

from .monitor import FundingCandidate
from .n14_analyzer import (
    N14Candle,
    N14MarketScenario,
    N14ShockAssessment,
    decimal_median,
    evaluate_n14_shock,
    n14_market_s_reason,
    n14_wilder_atr,
    parse_n14_klines,
)
from .strategies import N14_STRATEGY, StrategyConfig


INTERVAL_MS = 900_000


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _decimal_string(value: Any, *, positive: bool = False) -> Decimal:
    if not isinstance(value, str):
        raise ValueError("N14 snapshot decimal must be a string")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("N14 snapshot decimal invalid") from exc
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise ValueError("N14 snapshot decimal invalid")
    return parsed


def _native_int(value: Any, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("N14 snapshot integer invalid")
    if minimum is not None and value < minimum:
        raise ValueError("N14 snapshot integer invalid")
    return value


@dataclass(frozen=True)
class N14FrozenConfig:
    atr_period: int
    volume_median_bars: int
    one_hour_bars: int
    market_1h_median_min: Decimal
    rank_down_min: int
    rank_down_max: int
    residual_atr_multiple: Decimal
    systemic_red_breadth_min: Decimal
    systemic_median_return_atr_multiple: Decimal
    shock_body_atr_min: Decimal
    shock_volume_multiple_min: Decimal
    shock_taker_buy_ratio_max: Decimal
    shock_close_location_max: Decimal
    prior_shock_lookback: int
    absorption_volume_multiple_min: Decimal
    absorption_taker_buy_ratio_max: Decimal
    absorption_range_ratio_max: Decimal
    absorption_low_atr_tolerance: Decimal
    absorption_close_atr_tolerance: Decimal
    confirmation_max_bars: int
    confirmation_shock_midpoint_fraction: Decimal
    confirmation_close_location_min: Decimal
    confirmation_taker_buy_ratio_min: Decimal
    confirmation_volume_multiple_min: Decimal
    cascade_breadth_min: Decimal
    cascade_improvement_min: Decimal
    entry_extension_atr_max: Decimal
    entry_window_seconds: int

    @classmethod
    def from_strategy(cls, strategy: StrategyConfig) -> "N14FrozenConfig":
        return cls(
            atr_period=strategy.n14_atr_period,
            volume_median_bars=strategy.n14_volume_median_bars,
            one_hour_bars=strategy.n14_one_hour_bars,
            market_1h_median_min=strategy.n14_market_1h_median_min,
            rank_down_min=strategy.n14_rank_down_min,
            rank_down_max=strategy.n14_rank_down_max,
            residual_atr_multiple=strategy.n14_residual_atr_multiple,
            systemic_red_breadth_min=strategy.n14_systemic_red_breadth_min,
            systemic_median_return_atr_multiple=(
                strategy.n14_systemic_median_return_atr_multiple
            ),
            shock_body_atr_min=strategy.n14_shock_body_atr_min,
            shock_volume_multiple_min=strategy.n14_shock_volume_multiple_min,
            shock_taker_buy_ratio_max=strategy.n14_shock_taker_buy_ratio_max,
            shock_close_location_max=strategy.n14_shock_close_location_max,
            prior_shock_lookback=strategy.n14_prior_shock_lookback,
            absorption_volume_multiple_min=(
                strategy.n14_absorption_volume_multiple_min
            ),
            absorption_taker_buy_ratio_max=(
                strategy.n14_absorption_taker_buy_ratio_max
            ),
            absorption_range_ratio_max=strategy.n14_absorption_range_ratio_max,
            absorption_low_atr_tolerance=(
                strategy.n14_absorption_low_atr_tolerance
            ),
            absorption_close_atr_tolerance=(
                strategy.n14_absorption_close_atr_tolerance
            ),
            confirmation_max_bars=strategy.n14_confirmation_max_bars,
            confirmation_shock_midpoint_fraction=(
                strategy.n14_confirmation_shock_midpoint_fraction
            ),
            confirmation_close_location_min=(
                strategy.n14_confirmation_close_location_min
            ),
            confirmation_taker_buy_ratio_min=(
                strategy.n14_confirmation_taker_buy_ratio_min
            ),
            confirmation_volume_multiple_min=(
                strategy.n14_confirmation_volume_multiple_min
            ),
            cascade_breadth_min=strategy.n14_cascade_breadth_min,
            cascade_improvement_min=strategy.n14_cascade_improvement_min,
            entry_extension_atr_max=strategy.n14_entry_extension_atr_max,
            entry_window_seconds=strategy.entry_window_seconds,
        )

    def payload(self) -> dict[str, Any]:
        return {
            key: str(value) if isinstance(value, Decimal) else value
            for key, value in self.__dict__.items()
        }

    @classmethod
    def from_payload(cls, payload: Any) -> "N14FrozenConfig":
        expected = set(cls.__dataclass_fields__)
        if not isinstance(payload, dict) or set(payload) != expected:
            raise ValueError("N14 snapshot config invalid")
        integer_fields = {
            "atr_period",
            "volume_median_bars",
            "one_hour_bars",
            "rank_down_min",
            "rank_down_max",
            "prior_shock_lookback",
            "confirmation_max_bars",
            "entry_window_seconds",
        }
        values: dict[str, Any] = {}
        for key in expected:
            value = payload[key]
            values[key] = (
                _native_int(value, 0 if key == "prior_shock_lookback" else 1)
                if key in integer_fields
                else _decimal_string(value)
            )
        config = cls(**values)
        if (
            config.one_hour_bars != 4
            or config.confirmation_max_bars != 2
            or config.rank_down_max < config.rank_down_min
            or config.rank_down_max > 100
            or config != cls.from_strategy(N14_STRATEGY)
        ):
            raise ValueError("N14 snapshot config invalid")
        return config

    def analyzer_kwargs(self) -> dict[str, Any]:
        return {
            "atr_period": self.atr_period,
            "volume_median_bars": self.volume_median_bars,
            "one_hour_bars": self.one_hour_bars,
            "market_1h_median_min": self.market_1h_median_min,
            "rank_min": self.rank_down_min,
            "rank_max": self.rank_down_max,
            "residual_atr_multiple": self.residual_atr_multiple,
            "systemic_red_breadth_min": self.systemic_red_breadth_min,
            "systemic_median_return_atr_multiple": (
                self.systemic_median_return_atr_multiple
            ),
            "shock_body_atr_min": self.shock_body_atr_min,
            "shock_volume_multiple_min": self.shock_volume_multiple_min,
            "shock_taker_buy_ratio_max": self.shock_taker_buy_ratio_max,
            "shock_close_location_max": self.shock_close_location_max,
            "prior_shock_lookback": self.prior_shock_lookback,
            "absorption_volume_multiple_min": self.absorption_volume_multiple_min,
            "absorption_taker_buy_ratio_max": self.absorption_taker_buy_ratio_max,
            "absorption_range_ratio_max": self.absorption_range_ratio_max,
            "absorption_low_atr_tolerance": self.absorption_low_atr_tolerance,
            "absorption_close_atr_tolerance": self.absorption_close_atr_tolerance,
            "confirmation_max_bars": self.confirmation_max_bars,
            "confirmation_shock_midpoint_fraction": (
                self.confirmation_shock_midpoint_fraction
            ),
            "confirmation_close_location_min": (
                self.confirmation_close_location_min
            ),
            "confirmation_taker_buy_ratio_min": (
                self.confirmation_taker_buy_ratio_min
            ),
            "confirmation_volume_multiple_min": (
                self.confirmation_volume_multiple_min
            ),
            "cascade_breadth_min": self.cascade_breadth_min,
            "cascade_improvement_min": self.cascade_improvement_min,
            "entry_extension_atr_max": self.entry_extension_atr_max,
            "entry_window_seconds": self.entry_window_seconds,
        }


@dataclass(frozen=True)
class N14SnapshotRow:
    symbol: str
    quote_volume_rank: int
    r1h: Decimal
    rank_down: int
    atr_pct: Decimal
    residual: Decimal
    bar_return_s: Decimal
    red_s: bool
    bullish_s: bool
    local_shock_qualified: bool
    market_filter_reason: str | None
    prior_context_complete: bool
    prior_full_shock_present: bool
    full_s_qualified: bool
    shock_assessment: N14ShockAssessment
    s_candle: N14Candle


@dataclass(frozen=True)
class N14Snapshot:
    s_open_time_ms: int
    config: N14FrozenConfig
    config_signature: str
    rows: dict[str, N14SnapshotRow]
    market_1h_median: Decimal
    red_breadth_s: Decimal
    median_bar_return_s: Decimal
    median_atr_pct_s: Decimal
    bullish_breadth_s: Decimal

    def scenario(
        self,
        symbol: str,
        bullish_breadth_c: Decimal,
        *,
        prior_full_shock_present: bool | None = None,
    ) -> N14MarketScenario | None:
        row = self.rows.get(symbol)
        if row is None:
            return None
        return N14MarketScenario(
            s_open_time_ms=self.s_open_time_ms,
            quote_volume_rank=row.quote_volume_rank,
            r1h=row.r1h,
            rank_down=row.rank_down,
            atr_pct=row.atr_pct,
            residual=row.residual,
            market_1h_median=self.market_1h_median,
            red_breadth_s=self.red_breadth_s,
            median_bar_return_s=self.median_bar_return_s,
            median_atr_pct_s=self.median_atr_pct_s,
            bullish_breadth_s=self.bullish_breadth_s,
            bullish_breadth_c=bullish_breadth_c,
            prior_context_complete=row.prior_context_complete,
            prior_full_shock_present=(
                row.prior_full_shock_present
                if prior_full_shock_present is None
                else prior_full_shock_present
            ),
        )


def build_n14_snapshot(
    strategy: StrategyConfig,
    candidates: list[FundingCandidate],
    raw_by_symbol: Mapping[str, list[list[Any]]],
    prior_snapshots: Mapping[int, N14Snapshot] | None = None,
) -> tuple[dict[str, Any], N14Snapshot, int]:
    if len(candidates) != 100:
        raise ValueError("N14 snapshot requires exactly 100 candidates")
    symbols = [candidate.symbol for candidate in candidates]
    ranks = [candidate.quote_volume_rank for candidate in candidates]
    if (
        any(not isinstance(symbol, str) or not symbol for symbol in symbols)
        or len(set(symbols)) != 100
        or any(type(rank) is not int for rank in ranks)
        or set(ranks) != set(range(1, 101))
    ):
        raise ValueError("N14 snapshot candidate set invalid")

    config = N14FrozenConfig.from_strategy(strategy)
    calculated: list[dict[str, Any]] = []
    current_open_time: int | None = None
    s_open_time: int | None = None
    for candidate in candidates:
        raw = raw_by_symbol.get(candidate.symbol)
        if raw is None:
            raise ValueError("N14 snapshot shared kline missing")
        candles = parse_n14_klines(raw)
        if len(candles) < max(22, config.atr_period + 2):
            raise ValueError("N14 snapshot kline history insufficient")
        if current_open_time is None:
            current_open_time = candles[-1].open_time_ms
            s_open_time = candles[-2].open_time_ms
        if (
            candles[-1].open_time_ms != current_open_time
            or candles[-2].open_time_ms != s_open_time
            or s_open_time + INTERVAL_MS != current_open_time
        ):
            raise ValueError("N14 snapshot kline axes are not aligned")
        s_index = len(candles) - 2
        atr = n14_wilder_atr(candles[:-1], config.atr_period).get(s_index - 1)
        if atr is None or atr <= 0:
            raise ValueError("N14 snapshot ATR invalid")
        s = candles[s_index]
        previous = candles[s_index - 1]
        r1h = s.close / candles[s_index - 3].open - Decimal("1")
        atr_pct = atr / previous.close
        bar_return = s.close / previous.close - Decimal("1")
        calculated.append(
            {
                "symbol": candidate.symbol,
                "quote_volume_rank": candidate.quote_volume_rank,
                "r1h": r1h,
                "atr_pct": atr_pct,
                "bar_return_s": bar_return,
                "red_s": bar_return < 0,
                "bullish_s": bar_return > 0,
                "s": s,
                "candles": candles,
                "atr_by_index": n14_wilder_atr(candles[:-1], config.atr_period),
            }
        )
    assert current_open_time is not None and s_open_time is not None

    market_median = decimal_median([item["r1h"] for item in calculated])
    ordered = sorted(
        calculated,
        key=lambda item: (
            item["r1h"],
            item["quote_volume_rank"],
            item["symbol"],
        ),
    )
    ranks_down = {
        item["symbol"]: rank for rank, item in enumerate(ordered, start=1)
    }
    hundred = Decimal("100")
    red_breadth = Decimal(sum(item["red_s"] for item in calculated)) / hundred
    bullish_breadth = (
        Decimal(sum(item["bullish_s"] for item in calculated)) / hundred
    )
    median_bar_return = decimal_median(
        [item["bar_return_s"] for item in calculated]
    )
    median_atr_pct = decimal_median([item["atr_pct"] for item in calculated])
    prior_snapshots = prior_snapshots or {}
    rows = []
    for item in calculated:
        prior_times = [s_open_time - offset * INTERVAL_MS for offset in range(1, 4)]
        prior_context_complete = all(
            prior_time in prior_snapshots for prior_time in prior_times
        )
        prior_rows = [
            prior_snapshots[prior_time].rows.get(item["symbol"])
            for prior_time in prior_times
            if prior_time in prior_snapshots
        ]
        prior_full = bool(
            prior_context_complete
            and any(
                row is not None and row.full_s_qualified
                for row in prior_rows
            )
        )
        shock = evaluate_n14_shock(
            item["candles"][:-1],
            item["atr_by_index"],
            len(item["candles"]) - 2,
            volume_median_bars=config.volume_median_bars,
            body_atr_min=config.shock_body_atr_min,
            volume_multiple_min=config.shock_volume_multiple_min,
            taker_buy_ratio_max=config.shock_taker_buy_ratio_max,
            close_location_max=config.shock_close_location_max,
        )
        scenario = N14MarketScenario(
            s_open_time_ms=s_open_time,
            quote_volume_rank=item["quote_volume_rank"],
            r1h=item["r1h"],
            rank_down=ranks_down[item["symbol"]],
            atr_pct=item["atr_pct"],
            residual=item["r1h"] - market_median,
            market_1h_median=market_median,
            red_breadth_s=red_breadth,
            median_bar_return_s=median_bar_return,
            median_atr_pct_s=median_atr_pct,
            bullish_breadth_s=bullish_breadth,
            bullish_breadth_c=Decimal("0"),
            prior_context_complete=prior_context_complete,
            prior_full_shock_present=prior_full,
        )
        market_reason = n14_market_s_reason(
            scenario,
            market_1h_median_min=config.market_1h_median_min,
            rank_min=config.rank_down_min,
            rank_max=config.rank_down_max,
            residual_atr_multiple=config.residual_atr_multiple,
            systemic_red_breadth_min=config.systemic_red_breadth_min,
            systemic_median_return_atr_multiple=(
                config.systemic_median_return_atr_multiple
            ),
        )
        full_s_qualified = bool(
            prior_context_complete
            and not prior_full
            and shock.passed
            and market_reason is None
        )
        rows.append({
            "symbol": item["symbol"],
            "quote_volume_rank": item["quote_volume_rank"],
            "r1h": str(item["r1h"]),
            "rank_down": ranks_down[item["symbol"]],
            "atr_pct": str(item["atr_pct"]),
            "residual": str(item["r1h"] - market_median),
            "bar_return_s": str(item["bar_return_s"]),
            "red_s": item["red_s"],
            "bullish_s": item["bullish_s"],
            "local_shock_qualified": shock.passed,
            "market_filter_reason": market_reason,
            "prior_context_complete": prior_context_complete,
            "prior_full_shock_present": prior_full,
            "full_s_qualified": full_s_qualified,
            "shock_assessment": shock.json(),
            "s_candle": {
                key: value
                for key, value in item["s"].json().items()
                if key != "index"
            },
        })
    config_payload = config.payload()
    unsigned = {
        "snapshot_schema_version": 1,
        "strategy_id": strategy.strategy_id,
        "s_open_time_ms": s_open_time,
        "config": config_payload,
        "config_signature": _sha(config_payload),
        "market": {
            "market_1h_median": str(market_median),
            "red_breadth_s": str(red_breadth),
            "median_bar_return_s": str(median_bar_return),
            "median_atr_pct_s": str(median_atr_pct),
            "bullish_breadth_s": str(bullish_breadth),
        },
        "rows": rows,
    }
    payload = {**unsigned, "canonical_sha256": _sha(unsigned)}
    return payload, decode_n14_snapshot(payload, strategy.strategy_id), current_open_time


def decode_n14_snapshot(payload: Any, strategy_id: str) -> N14Snapshot:
    expected_top = {
        "snapshot_schema_version",
        "strategy_id",
        "s_open_time_ms",
        "config",
        "config_signature",
        "market",
        "rows",
        "canonical_sha256",
    }
    if not isinstance(payload, dict) or set(payload) != expected_top:
        raise ValueError("N14 snapshot payload invalid")
    unsigned = {key: payload[key] for key in expected_top - {"canonical_sha256"}}
    if (
        payload["snapshot_schema_version"] != 1
        or type(payload["snapshot_schema_version"]) is not int
        or payload["strategy_id"] != strategy_id
        or not isinstance(payload["canonical_sha256"], str)
        or payload["canonical_sha256"] != _sha(unsigned)
    ):
        raise ValueError("N14 snapshot envelope invalid")
    s_time = _native_int(payload["s_open_time_ms"], 1)
    config = N14FrozenConfig.from_payload(payload["config"])
    if (
        not isinstance(payload["config_signature"], str)
        or payload["config_signature"] != _sha(payload["config"])
    ):
        raise ValueError("N14 snapshot config signature invalid")
    rows_payload = payload["rows"]
    if not isinstance(rows_payload, list) or len(rows_payload) != 100:
        raise ValueError("N14 snapshot row count invalid")
    expected_row = {
        "symbol",
        "quote_volume_rank",
        "r1h",
        "rank_down",
        "atr_pct",
        "residual",
        "bar_return_s",
        "red_s",
        "bullish_s",
        "local_shock_qualified",
        "market_filter_reason",
        "prior_context_complete",
        "prior_full_shock_present",
        "full_s_qualified",
        "shock_assessment",
        "s_candle",
    }
    rows: dict[str, N14SnapshotRow] = {}
    quote_ranks: set[int] = set()
    down_ranks: set[int] = set()
    for item in rows_payload:
        if not isinstance(item, dict) or set(item) != expected_row:
            raise ValueError("N14 snapshot row invalid")
        symbol = item["symbol"]
        if not isinstance(symbol, str) or not symbol or symbol in rows:
            raise ValueError("N14 snapshot symbol invalid")
        quote_rank = _native_int(item["quote_volume_rank"], 1)
        down_rank = _native_int(item["rank_down"], 1)
        if quote_rank > 100 or down_rank > 100:
            raise ValueError("N14 snapshot rank invalid")
        r1h = _decimal_string(item["r1h"])
        atr_pct = _decimal_string(item["atr_pct"], positive=True)
        residual = _decimal_string(item["residual"])
        bar_return = _decimal_string(item["bar_return_s"])
        red_s = item["red_s"]
        bullish_s = item["bullish_s"]
        local_shock = item["local_shock_qualified"]
        market_reason = item["market_filter_reason"]
        prior_complete = item["prior_context_complete"]
        prior_full = item["prior_full_shock_present"]
        full_s = item["full_s_qualified"]
        shock_payload = item["shock_assessment"]
        s_payload = item["s_candle"]
        if (
            not isinstance(shock_payload, dict)
            or set(shock_payload) != {
                "passed",
                "reason",
                "atr_reference",
                "volume_median",
                "body",
                "body_atr_multiple",
                "volume_multiple",
                "taker_buy_ratio",
                "close_location",
            }
            or type(shock_payload["passed"]) is not bool
            or not isinstance(shock_payload["reason"], str)
        ):
            raise ValueError("N14 snapshot shock assessment invalid")

        expected_s = {
            "open_time_ms",
            "open",
            "high",
            "low",
            "close",
            "quote_volume",
            "taker_buy_quote_volume",
        }
        if not isinstance(s_payload, dict) or set(s_payload) != expected_s:
            raise ValueError("N14 snapshot S candle invalid")
        s_open_time = _native_int(s_payload["open_time_ms"], 1)
        s_open = _decimal_string(s_payload["open"], positive=True)
        s_high = _decimal_string(s_payload["high"], positive=True)
        s_low = _decimal_string(s_payload["low"], positive=True)
        s_close = _decimal_string(s_payload["close"], positive=True)
        s_quote_volume = _decimal_string(s_payload["quote_volume"])
        s_taker_quote = _decimal_string(
            s_payload["taker_buy_quote_volume"]
        )
        if (
            s_open_time != s_time
            or s_quote_volume < 0
            or s_taker_quote < 0
            or s_taker_quote > s_quote_volume
            or s_high < max(s_open, s_close)
            or s_low > min(s_open, s_close)
            or s_high < s_low
        ):
            raise ValueError("N14 snapshot S candle invalid")
        s_candle = N14Candle(
            index=0,
            open_time_ms=s_open_time,
            open=s_open,
            high=s_high,
            low=s_low,
            close=s_close,
            quote_volume=s_quote_volume,
            taker_buy_quote_volume=s_taker_quote,
        )

        def optional_decimal(value: Any) -> Decimal | None:
            return None if value is None else _decimal_string(value)

        shock_assessment = N14ShockAssessment(
            passed=shock_payload["passed"],
            reason=shock_payload["reason"],
            atr_reference=_decimal_string(
                shock_payload["atr_reference"], positive=True
            ),
            volume_median=_decimal_string(shock_payload["volume_median"]),
            body=_decimal_string(shock_payload["body"]),
            body_atr_multiple=_decimal_string(
                shock_payload["body_atr_multiple"]
            ),
            volume_multiple=_decimal_string(shock_payload["volume_multiple"]),
            taker_buy_ratio=optional_decimal(shock_payload["taker_buy_ratio"]),
            close_location=optional_decimal(shock_payload["close_location"]),
        )
        if shock_assessment.volume_median < 0:
            raise ValueError("N14 snapshot shock volume median invalid")
        expected_body = s_candle.open - s_candle.close
        expected_body_multiple = (
            expected_body / shock_assessment.atr_reference
        )
        expected_volume_multiple = (
            s_candle.quote_volume / shock_assessment.volume_median
            if shock_assessment.volume_median > 0
            else Decimal("0")
        )
        expected_taker_ratio = s_candle.taker_buy_ratio
        expected_close_location = s_candle.close_location
        if (
            shock_assessment.volume_median <= 0
            or expected_body
            < config.shock_body_atr_min * shock_assessment.atr_reference
        ):
            expected_shock_reason = "N14_NOT_SHOCK"
            expected_shock_passed = False
        elif (
            s_candle.quote_volume <= 0
            or s_candle.quote_volume
            < config.shock_volume_multiple_min
            * shock_assessment.volume_median
        ):
            expected_shock_reason = "N14_SHOCK_VOLUME_TOO_LOW"
            expected_shock_passed = False
        elif (
            expected_taker_ratio is None
            or expected_taker_ratio > config.shock_taker_buy_ratio_max
        ):
            expected_shock_reason = "N14_SHOCK_TAKER_BUY_TOO_HIGH"
            expected_shock_passed = False
        elif (
            expected_close_location is None
            or expected_close_location > config.shock_close_location_max
        ):
            expected_shock_reason = "N14_SHOCK_CLOSE_LOCATION_TOO_HIGH"
            expected_shock_passed = False
        else:
            expected_shock_reason = "PASSED"
            expected_shock_passed = True
        if (
            shock_assessment.body != expected_body
            or shock_assessment.body_atr_multiple != expected_body_multiple
            or shock_assessment.volume_multiple != expected_volume_multiple
            or shock_assessment.taker_buy_ratio != expected_taker_ratio
            or shock_assessment.close_location != expected_close_location
            or shock_assessment.reason != expected_shock_reason
            or shock_assessment.passed != expected_shock_passed
        ):
            raise ValueError("N14 snapshot shock assessment inconsistent")
        if (
            type(red_s) is not bool
            or type(bullish_s) is not bool
            or type(local_shock) is not bool
            or (market_reason is not None and not isinstance(market_reason, str))
            or type(prior_complete) is not bool
            or type(prior_full) is not bool
            or type(full_s) is not bool
            or red_s != (bar_return < 0)
            or bullish_s != (bar_return > 0)
            or (red_s and bullish_s)
        ):
            raise ValueError("N14 snapshot breadth row invalid")
        quote_ranks.add(quote_rank)
        down_ranks.add(down_rank)
        rows[symbol] = N14SnapshotRow(
            symbol,
            quote_rank,
            r1h,
            down_rank,
            atr_pct,
            residual,
            bar_return,
            red_s,
            bullish_s,
            local_shock,
            market_reason,
            prior_complete,
            prior_full,
            full_s,
            shock_assessment,
            s_candle,
        )
    if quote_ranks != set(range(1, 101)) or down_ranks != set(range(1, 101)):
        raise ValueError("N14 snapshot ranks incomplete")
    ordered = sorted(
        rows.values(),
        key=lambda row: (row.r1h, row.quote_volume_rank, row.symbol),
    )
    if any(row.rank_down != rank for rank, row in enumerate(ordered, 1)):
        raise ValueError("N14 snapshot downside ranks invalid")

    market_payload = payload["market"]
    expected_market = {
        "market_1h_median",
        "red_breadth_s",
        "median_bar_return_s",
        "median_atr_pct_s",
        "bullish_breadth_s",
    }
    if not isinstance(market_payload, dict) or set(market_payload) != expected_market:
        raise ValueError("N14 snapshot market payload invalid")
    market_median = _decimal_string(market_payload["market_1h_median"])
    red_breadth = _decimal_string(market_payload["red_breadth_s"])
    median_bar_return = _decimal_string(market_payload["median_bar_return_s"])
    median_atr_pct = _decimal_string(
        market_payload["median_atr_pct_s"], positive=True
    )
    bullish_breadth = _decimal_string(market_payload["bullish_breadth_s"])
    expected_median = decimal_median([row.r1h for row in rows.values()])
    expected_red = Decimal(sum(row.red_s for row in rows.values())) / Decimal("100")
    expected_bullish = (
        Decimal(sum(row.bullish_s for row in rows.values())) / Decimal("100")
    )
    expected_bar_median = decimal_median(
        [row.bar_return_s for row in rows.values()]
    )
    expected_atr_median = decimal_median([row.atr_pct for row in rows.values()])
    if (
        market_median != expected_median
        or red_breadth != expected_red
        or bullish_breadth != expected_bullish
        or median_bar_return != expected_bar_median
        or median_atr_pct != expected_atr_median
        or any(row.residual != row.r1h - market_median for row in rows.values())
        or not Decimal("0") <= red_breadth <= Decimal("1")
        or not Decimal("0") <= bullish_breadth <= Decimal("1")
    ):
        raise ValueError("N14 snapshot market metrics inconsistent")
    for row in rows.values():
        scenario = N14MarketScenario(
            s_open_time_ms=s_time,
            quote_volume_rank=row.quote_volume_rank,
            r1h=row.r1h,
            rank_down=row.rank_down,
            atr_pct=row.atr_pct,
            residual=row.residual,
            market_1h_median=market_median,
            red_breadth_s=red_breadth,
            median_bar_return_s=median_bar_return,
            median_atr_pct_s=median_atr_pct,
            bullish_breadth_s=bullish_breadth,
            bullish_breadth_c=Decimal("0"),
            prior_context_complete=row.prior_context_complete,
            prior_full_shock_present=row.prior_full_shock_present,
        )
        expected_reason = n14_market_s_reason(
            scenario,
            market_1h_median_min=config.market_1h_median_min,
            rank_min=config.rank_down_min,
            rank_max=config.rank_down_max,
            residual_atr_multiple=config.residual_atr_multiple,
            systemic_red_breadth_min=config.systemic_red_breadth_min,
            systemic_median_return_atr_multiple=(
                config.systemic_median_return_atr_multiple
            ),
        )
        if (
            row.market_filter_reason != expected_reason
            or (row.prior_full_shock_present and not row.prior_context_complete)
            or row.full_s_qualified
            != (
                row.prior_context_complete
                and not row.prior_full_shock_present
                and row.local_shock_qualified
                and expected_reason is None
            )
            or row.local_shock_qualified != row.shock_assessment.passed
        ):
            raise ValueError("N14 snapshot S qualification inconsistent")
    return N14Snapshot(
        s_open_time_ms=s_time,
        config=config,
        config_signature=payload["config_signature"],
        rows=rows,
        market_1h_median=market_median,
        red_breadth_s=red_breadth,
        median_bar_return_s=median_bar_return,
        median_atr_pct_s=median_atr_pct,
        bullish_breadth_s=bullish_breadth,
    )


def n14_bullish_breadth_for_current_c(
    snapshot: N14Snapshot,
    raw_by_symbol: Mapping[str, list[list[Any]]],
    current_open_time_ms: int,
) -> Decimal:
    if len(snapshot.rows) != 100:
        raise ValueError("N14 frozen member set incomplete")
    bullish = 0
    for symbol in snapshot.rows:
        raw = raw_by_symbol.get(symbol)
        if raw is None:
            raise ValueError("N14 frozen member kline missing")
        candles = parse_n14_klines(raw)
        if (
            len(candles) < 3
            or candles[-1].open_time_ms != current_open_time_ms
            or candles[-2].open_time_ms + INTERVAL_MS != current_open_time_ms
        ):
            raise ValueError("N14 frozen member kline axis invalid")
        if candles[-2].close > candles[-3].close:
            bullish += 1
    return Decimal(bullish) / Decimal("100")
