from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


INTERVAL_MS = 900_000


class N14KlineDataError(ValueError):
    pass


class N14KlineSequenceError(ValueError):
    pass


@dataclass(frozen=True)
class N14Candle:
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
    def taker_buy_ratio(self) -> Decimal | None:
        if self.quote_volume <= 0:
            return None
        return self.taker_buy_quote_volume / self.quote_volume

    @property
    def close_location(self) -> Decimal | None:
        if self.range <= 0:
            return None
        return (self.close - self.low) / self.range

    def json(self) -> dict[str, Any]:
        return {
            key: str(value) if isinstance(value, Decimal) else value
            for key, value in self.__dict__.items()
        }


@dataclass(frozen=True)
class N14MarketScenario:
    s_open_time_ms: int
    quote_volume_rank: int
    r1h: Decimal
    rank_down: int
    atr_pct: Decimal
    residual: Decimal
    market_1h_median: Decimal
    red_breadth_s: Decimal
    median_bar_return_s: Decimal
    median_atr_pct_s: Decimal
    bullish_breadth_s: Decimal
    bullish_breadth_c: Decimal
    prior_context_complete: bool = True
    prior_full_shock_present: bool = False

    def json(self) -> dict[str, Any]:
        return {
            key: str(value) if isinstance(value, Decimal) else value
            for key, value in self.__dict__.items()
        }


@dataclass(frozen=True)
class N14ShockAssessment:
    passed: bool
    reason: str
    atr_reference: Decimal
    volume_median: Decimal
    body: Decimal
    body_atr_multiple: Decimal
    volume_multiple: Decimal
    taker_buy_ratio: Decimal | None
    close_location: Decimal | None

    def json(self) -> dict[str, Any]:
        return {
            key: str(value) if isinstance(value, Decimal) else value
            for key, value in self.__dict__.items()
        }


@dataclass(frozen=True)
class N14Structure:
    symbol: str
    s: N14Candle
    a: N14Candle
    c: N14Candle
    entry: N14Candle | None
    p: Decimal
    atr_s_reference: Decimal
    atr_c: Decimal
    volume_median_s: Decimal
    shock_body_atr_multiple: Decimal
    shock_volume_multiple: Decimal
    tb_s: Decimal
    tb_a: Decimal
    tb_c: Decimal
    close_location_s: Decimal
    close_location_c: Decimal
    flow_flip: Decimal
    entry_min_price: Decimal
    entry_max_price: Decimal
    quote_volume_rank: int | None
    market_scenario: N14MarketScenario | None
    market_context_available: bool
    failed_confirmation_reasons: tuple[str, ...]
    structure_id: str

    def json(self) -> dict[str, Any]:
        return {
            "structure_id": self.structure_id,
            "s": self.s.json(),
            "a": self.a.json(),
            "c": self.c.json(),
            "entry": self.entry.json() if self.entry else None,
            "p": str(self.p),
            "atr_s_reference": str(self.atr_s_reference),
            "atr_c": str(self.atr_c),
            "volume_median_s": str(self.volume_median_s),
            "shock_body_atr_multiple": str(self.shock_body_atr_multiple),
            "shock_volume_multiple": str(self.shock_volume_multiple),
            "tb_s": str(self.tb_s),
            "tb_a": str(self.tb_a),
            "tb_c": str(self.tb_c),
            "close_location_s": str(self.close_location_s),
            "close_location_c": str(self.close_location_c),
            "flow_flip": str(self.flow_flip),
            "entry_min_price": str(self.entry_min_price),
            "entry_max_price": str(self.entry_max_price),
            "quote_volume_rank": self.quote_volume_rank,
            "market_scenario": (
                self.market_scenario.json() if self.market_scenario else None
            ),
            "market_context_available": self.market_context_available,
            "failed_confirmation_reasons": list(
                self.failed_confirmation_reasons
            ),
        }


@dataclass(frozen=True)
class N14Event:
    s_time: str
    status: str
    reason: str
    structure: N14Structure | None = None
    detail: dict[str, Any] | None = None


@dataclass(frozen=True)
class N14AnalysisResult:
    symbol: str
    passed: bool
    reason: str
    structure: N14Structure | None
    consume_current: bool
    historical_events: tuple[N14Event, ...]
    stage_events: tuple[N14Event, ...]
    current_price: Decimal
    elapsed_ms: int | None
    active_event: N14Event | None = None

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

    @property
    def flow_flip(self) -> Decimal:
        return self.structure.flow_flip if self.structure else Decimal("0")

    @property
    def confirmation_close_location(self) -> Decimal:
        return (
            self.structure.close_location_c
            if self.structure
            else Decimal("0")
        )

    def detail_json(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "structure_id": self.structure_id,
            "consume_current": self.consume_current,
            "current_price": str(self.current_price),
            "elapsed_ms": self.elapsed_ms,
            "active_event": (
                {
                    "s_time": self.active_event.s_time,
                    "status": self.active_event.status,
                    "reason": self.active_event.reason,
                    "detail": self.active_event.detail,
                }
                if self.active_event
                else None
            ),
            "structure": self.structure.json() if self.structure else None,
            "historical_events": [
                {
                    "s_time": event.s_time,
                    "status": event.status,
                    "reason": event.reason,
                    "structure": (
                        event.structure.json() if event.structure else None
                    ),
                    "detail": event.detail,
                }
                for event in self.historical_events
            ],
            "stage_events": [
                {
                    "s_time": event.s_time,
                    "status": event.status,
                    "reason": event.reason,
                    "structure": (
                        event.structure.json() if event.structure else None
                    ),
                    "detail": event.detail,
                }
                for event in self.stage_events
            ],
        }


def _d(value: Any) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise InvalidOperation
    return parsed


def decimal_median(values: list[Decimal]) -> Decimal:
    if not values:
        raise ValueError("median requires values")
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def parse_n14_klines(raw: list[list[Any]]) -> list[N14Candle]:
    candles: list[N14Candle] = []
    try:
        for index, row in enumerate(raw):
            if len(row) <= 10:
                raise N14KlineDataError("N14 kline fields missing")
            open_time_raw = row[0]
            if (
                isinstance(open_time_raw, bool)
                or not isinstance(open_time_raw, int)
                or open_time_raw <= 0
            ):
                raise N14KlineDataError("N14 kline open time invalid")
            candle = N14Candle(
                index=index,
                open_time_ms=open_time_raw,
                open=_d(row[1]),
                high=_d(row[2]),
                low=_d(row[3]),
                close=_d(row[4]),
                quote_volume=_d(row[7]),
                taker_buy_quote_volume=_d(row[10]),
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
                raise N14KlineDataError("N14 kline values invalid")
            candles.append(candle)
    except N14KlineDataError:
        raise
    except (ArithmeticError, InvalidOperation, TypeError, ValueError) as exc:
        raise N14KlineDataError("N14 kline values invalid") from exc
    if any(
        right.open_time_ms - left.open_time_ms != INTERVAL_MS
        for left, right in zip(candles, candles[1:])
    ):
        raise N14KlineSequenceError("N14 kline sequence invalid")
    return candles


def n14_wilder_atr(
    candles: list[N14Candle],
    period: int = 14,
) -> dict[int, Decimal]:
    if period <= 0:
        raise ValueError("N14 ATR period invalid")
    result: dict[int, Decimal] = {}
    atr: Decimal | None = None
    true_ranges: list[Decimal] = []
    for index, candle in enumerate(candles):
        previous_close = candles[index - 1].close if index else candle.close
        true_range = max(
            candle.high - candle.low,
            abs(candle.high - previous_close),
            abs(candle.low - previous_close),
        )
        true_ranges.append(true_range)
        if index == period - 1:
            atr = sum(true_ranges[:period], Decimal("0")) / Decimal(period)
        elif index >= period:
            atr = (atr * Decimal(period - 1) + true_range) / Decimal(period)
        if atr is not None:
            result[index] = atr
    return result


def _structure_id(
    symbol: str,
    s: N14Candle,
    a: N14Candle,
    c: N14Candle,
) -> str:
    raw = f"{symbol}|{s.open_time_ms}|{a.open_time_ms}|{c.open_time_ms}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def evaluate_n14_shock(
    candles: list[N14Candle],
    atr_by_index: Mapping[int, Decimal],
    s_index: int,
    *,
    volume_median_bars: int = 20,
    body_atr_min: Decimal = Decimal("0.8"),
    volume_multiple_min: Decimal = Decimal("1.5"),
    taker_buy_ratio_max: Decimal = Decimal("0.40"),
    close_location_max: Decimal = Decimal("0.30"),
) -> N14ShockAssessment:
    empty = N14ShockAssessment(
        False,
        "N14_NOT_SHOCK",
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        None,
        None,
    )
    if s_index < volume_median_bars or s_index - 1 not in atr_by_index:
        return empty
    s = candles[s_index]
    atr_reference = atr_by_index[s_index - 1]
    prior_volumes = [
        candle.quote_volume
        for candle in candles[s_index - volume_median_bars:s_index]
    ]
    if atr_reference <= 0:
        return replace(empty, atr_reference=atr_reference)
    volume_median = decimal_median(prior_volumes)
    if volume_median <= 0:
        return replace(
            empty,
            atr_reference=atr_reference,
            volume_median=volume_median,
        )
    body = s.open - s.close
    body_multiple = body / atr_reference
    volume_multiple = (
        s.quote_volume / volume_median
        if volume_median > 0
        else Decimal("0")
    )
    taker_ratio = s.taker_buy_ratio
    close_location = s.close_location
    assessment = N14ShockAssessment(
        False,
        "N14_NOT_SHOCK",
        atr_reference,
        volume_median,
        body,
        body_multiple,
        volume_multiple,
        taker_ratio,
        close_location,
    )
    if body < body_atr_min * atr_reference:
        return assessment
    if s.quote_volume <= 0 or s.quote_volume < volume_multiple_min * volume_median:
        return replace(assessment, reason="N14_SHOCK_VOLUME_TOO_LOW")
    if taker_ratio is None or taker_ratio > taker_buy_ratio_max:
        return replace(assessment, reason="N14_SHOCK_TAKER_BUY_TOO_HIGH")
    if close_location is None or close_location > close_location_max:
        return replace(assessment, reason="N14_SHOCK_CLOSE_LOCATION_TOO_HIGH")
    return replace(assessment, passed=True, reason="PASSED")


def n14_market_s_reason(
    scenario: N14MarketScenario,
    *,
    market_1h_median_min: Decimal,
    rank_min: int,
    rank_max: int,
    residual_atr_multiple: Decimal,
    systemic_red_breadth_min: Decimal,
    systemic_median_return_atr_multiple: Decimal,
) -> str | None:
    if scenario.market_1h_median < market_1h_median_min:
        return "N14_MARKET_1H_MEDIAN_TOO_LOW"
    if scenario.r1h >= 0:
        return "N14_NOT_SHOCK"
    if not rank_min <= scenario.rank_down <= rank_max:
        return "N14_RELATIVE_DOWNSIDE_RANK_OUT_OF_RANGE"
    if scenario.residual > -residual_atr_multiple * scenario.atr_pct:
        return "N14_RESIDUAL_DROP_TOO_SMALL"
    if (
        scenario.red_breadth_s >= systemic_red_breadth_min
        and scenario.median_bar_return_s
        <= -systemic_median_return_atr_multiple * scenario.median_atr_pct_s
    ):
        return "N14_SYSTEMIC_CRASH_VETO"
    return None


def _absorption_reason(
    s: N14Candle,
    a: N14Candle,
    assessment: N14ShockAssessment,
    *,
    volume_multiple_min: Decimal,
    taker_buy_ratio_max: Decimal,
    range_ratio_max: Decimal,
    low_atr_tolerance: Decimal,
    close_atr_tolerance: Decimal,
) -> str | None:
    if (
        a.quote_volume <= 0
        or a.quote_volume < volume_multiple_min * assessment.volume_median
    ):
        return "N14_ABSORPTION_VOLUME_TOO_LOW"
    if a.taker_buy_ratio is None or a.taker_buy_ratio > taker_buy_ratio_max:
        return "N14_ABSORPTION_TAKER_BUY_TOO_HIGH"
    if a.range > range_ratio_max * s.range:
        return "N14_ABSORPTION_RANGE_TOO_LARGE"
    if a.low < s.low - low_atr_tolerance * assessment.atr_reference:
        return "N14_ABSORPTION_LOW_TOO_LOW"
    if a.close < s.close - close_atr_tolerance * assessment.atr_reference:
        return "N14_ABSORPTION_CLOSE_TOO_LOW"
    return None


def _confirmation_reason(
    previous: N14Candle,
    s: N14Candle,
    a: N14Candle,
    c: N14Candle,
    p: Decimal,
    volume_median_s: Decimal,
    *,
    shock_midpoint_fraction: Decimal,
    close_location_min: Decimal,
    taker_buy_ratio_min: Decimal,
    volume_multiple_min: Decimal,
) -> str | None:
    if c.low < p:
        return "N14_CONFIRMATION_LOW_BROKE_P"
    if c.close <= c.open:
        return "N14_CONFIRMATION_NOT_BULLISH"
    if c.close <= previous.close:
        return "N14_CONFIRMATION_CLOSE_NOT_ADVANCING"
    if c.close <= a.high:
        return "N14_CONFIRMATION_DID_NOT_BREAK_A_HIGH"
    midpoint = s.close + shock_midpoint_fraction * (s.open - s.close)
    if c.close < midpoint:
        return "N14_CONFIRMATION_SHOCK_MIDPOINT_NOT_RECLAIMED"
    if c.close_location is None or c.close_location < close_location_min:
        return "N14_CONFIRMATION_CLOSE_LOCATION_TOO_LOW"
    if c.taker_buy_ratio is None or c.taker_buy_ratio < taker_buy_ratio_min:
        return "N14_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW"
    if (
        c.quote_volume <= 0
        or c.quote_volume < volume_multiple_min * volume_median_s
    ):
        return "N14_CONFIRMATION_VOLUME_TOO_LOW"
    return None


def analyze_n14_sell_pressure_decay_reversal(
    symbol: str,
    raw_klines: list[list[Any]],
    *,
    market_scenarios: Mapping[int, N14MarketScenario] | None,
    market_context_complete: bool,
    quote_volume_rank: int | None = None,
    checked_at_ms: int,
    atr_period: int = 14,
    volume_median_bars: int = 20,
    one_hour_bars: int = 4,
    market_1h_median_min: Decimal = Decimal("-0.0075"),
    rank_min: int = 1,
    rank_max: int = 20,
    residual_atr_multiple: Decimal = Decimal("1.0"),
    systemic_red_breadth_min: Decimal = Decimal("0.75"),
    systemic_median_return_atr_multiple: Decimal = Decimal("0.5"),
    shock_body_atr_min: Decimal = Decimal("0.8"),
    shock_volume_multiple_min: Decimal = Decimal("1.5"),
    shock_taker_buy_ratio_max: Decimal = Decimal("0.40"),
    shock_close_location_max: Decimal = Decimal("0.30"),
    prior_shock_lookback: int = 3,
    absorption_volume_multiple_min: Decimal = Decimal("0.8"),
    absorption_taker_buy_ratio_max: Decimal = Decimal("0.45"),
    absorption_range_ratio_max: Decimal = Decimal("0.8"),
    absorption_low_atr_tolerance: Decimal = Decimal("0.20"),
    absorption_close_atr_tolerance: Decimal = Decimal("0.20"),
    confirmation_max_bars: int = 2,
    confirmation_shock_midpoint_fraction: Decimal = Decimal("0.50"),
    confirmation_close_location_min: Decimal = Decimal("0.70"),
    confirmation_taker_buy_ratio_min: Decimal = Decimal("0.55"),
    confirmation_volume_multiple_min: Decimal = Decimal("0.8"),
    cascade_breadth_min: Decimal = Decimal("0.40"),
    cascade_improvement_min: Decimal = Decimal("0.15"),
    entry_extension_atr_max: Decimal = Decimal("0.50"),
    entry_window_seconds: int = 120,
    target_s_open_time_ms: int | None = None,
    frozen_shock_assessment: N14ShockAssessment | None = None,
) -> N14AnalysisResult:
    candles: list[N14Candle] = []

    def result(
        reason: str,
        structure: N14Structure | None = None,
        *,
        passed: bool = False,
        consume: bool = False,
        historical: list[N14Event] | tuple[N14Event, ...] = (),
        stages: list[N14Event] | tuple[N14Event, ...] = (),
        elapsed: int | None = None,
        active: N14Event | None = None,
    ) -> N14AnalysisResult:
        current = candles[-1] if candles else None
        return N14AnalysisResult(
            symbol=symbol,
            passed=passed,
            reason=reason,
            structure=structure,
            consume_current=consume,
            historical_events=tuple(historical),
            stage_events=tuple(stages),
            current_price=current.close if current else Decimal("0"),
            elapsed_ms=elapsed,
            active_event=active,
        )

    try:
        decimal_parameters = (
            shock_body_atr_min,
            shock_volume_multiple_min,
            shock_taker_buy_ratio_max,
            shock_close_location_max,
            absorption_volume_multiple_min,
            absorption_taker_buy_ratio_max,
            absorption_range_ratio_max,
            absorption_low_atr_tolerance,
            absorption_close_atr_tolerance,
            confirmation_shock_midpoint_fraction,
            confirmation_close_location_min,
            confirmation_taker_buy_ratio_min,
            confirmation_volume_multiple_min,
            cascade_breadth_min,
            cascade_improvement_min,
            entry_extension_atr_max,
            systemic_red_breadth_min,
            systemic_median_return_atr_multiple,
            residual_atr_multiple,
        )
        if (
            atr_period <= 0
            or volume_median_bars <= 0
            or one_hour_bars != 4
            or prior_shock_lookback < 0
            or confirmation_max_bars != 2
            or entry_window_seconds <= 0
            or rank_min <= 0
            or rank_max < rank_min
            or any(value < 0 for value in decimal_parameters)
            or any(
                not Decimal("0") <= value <= Decimal("1")
                for value in (
                    shock_taker_buy_ratio_max,
                    shock_close_location_max,
                    absorption_taker_buy_ratio_max,
                    confirmation_shock_midpoint_fraction,
                    confirmation_close_location_min,
                    confirmation_taker_buy_ratio_min,
                    cascade_breadth_min,
                    cascade_improvement_min,
                    systemic_red_breadth_min,
                )
            )
        ):
            raise ValueError
        candles = parse_n14_klines(raw_klines)
        if len(candles) < max(volume_median_bars + 4, atr_period + 4):
            raise N14KlineDataError
        atr_by_index = n14_wilder_atr(candles[:-1], atr_period)
    except N14KlineSequenceError:
        return result("N14_KLINE_SEQUENCE_INVALID")
    except (ArithmeticError, InvalidOperation, N14KlineDataError, TypeError, ValueError):
        return result("N14_KLINE_DATA_INVALID")

    if not market_context_complete or market_scenarios is None:
        return result("N14_MARKET_CONTEXT_INSUFFICIENT")
    closed = candles[:-1]
    last_closed_index = len(closed) - 1
    current_s_indexes = {last_closed_index - 3, last_closed_index - 2}
    historical: list[N14Event] = []
    stages: list[N14Event] = []
    current_candidates: list[N14Structure] = []
    assessment_cache: dict[int, N14ShockAssessment] = {}

    def assessment_at(index: int) -> N14ShockAssessment:
        if index not in assessment_cache:
            if (
                frozen_shock_assessment is not None
                and target_s_open_time_ms is not None
                and closed[index].open_time_ms == target_s_open_time_ms
            ):
                assessment_cache[index] = frozen_shock_assessment
            else:
                assessment_cache[index] = evaluate_n14_shock(
                    closed,
                    atr_by_index,
                    index,
                    volume_median_bars=volume_median_bars,
                    body_atr_min=shock_body_atr_min,
                    volume_multiple_min=shock_volume_multiple_min,
                    taker_buy_ratio_max=shock_taker_buy_ratio_max,
                    close_location_max=shock_close_location_max,
                )
        return assessment_cache[index]

    def prior_full_shock(index: int) -> int | None:
        scenario = market_scenarios.get(closed[index].open_time_ms)
        if target_s_open_time_ms is not None and scenario is not None:
            return index - 1 if scenario.prior_full_shock_present else None
        for prior in range(max(0, index - prior_shock_lookback), index):
            prior_assessment = assessment_at(prior)
            if not prior_assessment.passed:
                continue
            scenario = market_scenarios.get(closed[prior].open_time_ms)
            if scenario is None:
                return prior
            if n14_market_s_reason(
                scenario,
                market_1h_median_min=market_1h_median_min,
                rank_min=rank_min,
                rank_max=rank_max,
                residual_atr_multiple=residual_atr_multiple,
                systemic_red_breadth_min=systemic_red_breadth_min,
                systemic_median_return_atr_multiple=(
                    systemic_median_return_atr_multiple
                ),
            ) is None:
                return prior
        return None

    start_index = max(volume_median_bars, atr_period)
    maximum_index = last_closed_index
    if target_s_open_time_ms is not None:
        target_indexes = [
            candle.index
            for candle in closed
            if candle.open_time_ms == target_s_open_time_ms
        ]
        if not target_indexes:
            return result("N14_KLINE_SEQUENCE_INVALID")
        start_index = target_indexes[0]
        maximum_index = start_index
    index = start_index
    pending_reason: str | None = None
    active_event: N14Event | None = None
    while index <= maximum_index:
        shock = assessment_at(index)
        if not shock.passed:
            index += 1
            continue
        earlier = prior_full_shock(index)
        if earlier is not None:
            return result("N14_NOT_SHOCK")
        s = closed[index]
        scenario = market_scenarios.get(s.open_time_ms)
        if target_s_open_time_ms is not None or index in current_s_indexes:
            if scenario is None:
                return result("N14_MARKET_CONTEXT_INSUFFICIENT")
            if not scenario.prior_context_complete:
                return result("N14_MARKET_CONTEXT_INSUFFICIENT")
            if (
                type(scenario.quote_volume_rank) is not int
                or not 1 <= scenario.quote_volume_rank <= 100
            ):
                return result("N14_MARKET_CONTEXT_INSUFFICIENT")
            market_reason = n14_market_s_reason(
                scenario,
                market_1h_median_min=market_1h_median_min,
                rank_min=rank_min,
                rank_max=rank_max,
                residual_atr_multiple=residual_atr_multiple,
                systemic_red_breadth_min=systemic_red_breadth_min,
                systemic_median_return_atr_multiple=(
                    systemic_median_return_atr_multiple
                ),
            )
            if market_reason is not None:
                if market_reason == "N14_SYSTEMIC_CRASH_VETO":
                    stages.append(
                        N14Event(
                            str(s.open_time_ms),
                            "INVALID",
                            market_reason,
                            detail={
                                "s": s.json(),
                                "shock": shock.json(),
                                "market_scenario": scenario.json(),
                            },
                        )
                    )
                return result(market_reason, stages=stages)

        a_index = index + 1
        if a_index > last_closed_index:
            pending_reason = "N14_ABSORPTION_PENDING"
            active_event = N14Event(
                str(s.open_time_ms),
                "S_LOCKED",
                pending_reason,
                detail={
                    "s": s.json(),
                    "shock": shock.json(),
                    "market_scenario": scenario.json() if scenario else None,
                },
            )
            break
        a = closed[a_index]
        absorption_reason = _absorption_reason(
            s,
            a,
            shock,
            volume_multiple_min=absorption_volume_multiple_min,
            taker_buy_ratio_max=absorption_taker_buy_ratio_max,
            range_ratio_max=absorption_range_ratio_max,
            low_atr_tolerance=absorption_low_atr_tolerance,
            close_atr_tolerance=absorption_close_atr_tolerance,
        )
        if absorption_reason is not None:
            stages.append(
                N14Event(
                    str(s.open_time_ms),
                    "INVALID",
                    absorption_reason,
                    detail={
                        "s": s.json(),
                        "a": a.json(),
                        "shock": shock.json(),
                        "market_scenario": scenario.json() if scenario else None,
                    },
                )
            )
            index = a_index + 1
            continue

        p = min(s.low, a.low)
        c_index: int | None = None
        failed_confirmation_reasons: list[str] = []
        confirmation_low_broken = False
        confirmation_terminal_index: int | None = None
        available_c_end = min(
            a_index + confirmation_max_bars,
            last_closed_index,
        )
        for candidate_index in range(a_index + 1, available_c_end + 1):
            c_candidate = closed[candidate_index]
            confirmation_reason = _confirmation_reason(
                closed[candidate_index - 1],
                s,
                a,
                c_candidate,
                p,
                shock.volume_median,
                shock_midpoint_fraction=confirmation_shock_midpoint_fraction,
                close_location_min=confirmation_close_location_min,
                taker_buy_ratio_min=confirmation_taker_buy_ratio_min,
                volume_multiple_min=confirmation_volume_multiple_min,
            )
            if confirmation_reason == "N14_CONFIRMATION_LOW_BROKE_P":
                failed_confirmation_reasons.append(confirmation_reason)
                confirmation_low_broken = True
                confirmation_terminal_index = candidate_index
                break
            if confirmation_reason is not None:
                failed_confirmation_reasons.append(confirmation_reason)
                continue
            c_index = candidate_index
            break

        if confirmation_low_broken:
            stages.append(
                N14Event(
                    str(s.open_time_ms),
                    "INVALID",
                    "N14_CONFIRMATION_LOW_BROKE_P",
                    detail={
                        "s": s.json(),
                        "a": a.json(),
                        "p": str(p),
                        "shock": shock.json(),
                        "market_scenario": scenario.json() if scenario else None,
                        "failed_confirmation_reasons": failed_confirmation_reasons,
                    },
                )
            )
            index = (confirmation_terminal_index or available_c_end) + 1
            continue
        if c_index is None:
            if last_closed_index >= a_index + confirmation_max_bars:
                stages.append(
                    N14Event(
                        str(s.open_time_ms),
                        "INVALID",
                        "N14_CONFIRMATION_NOT_FOUND",
                        detail={
                            "s": s.json(),
                            "a": a.json(),
                            "p": str(p),
                            "shock": shock.json(),
                            "market_scenario": scenario.json() if scenario else None,
                            "failed_confirmation_reasons": (
                                failed_confirmation_reasons
                            ),
                        },
                    )
                )
                index = a_index + confirmation_max_bars + 1
                continue
            pending_reason = "N14_CONFIRMATION_PENDING"
            active_event = N14Event(
                str(s.open_time_ms),
                "A_CONFIRMED",
                pending_reason,
                detail={
                    "s": s.json(),
                    "a": a.json(),
                    "p": str(p),
                    "shock": shock.json(),
                    "market_scenario": scenario.json() if scenario else None,
                    "failed_confirmation_reasons": failed_confirmation_reasons,
                },
            )
            break

        c = closed[c_index]
        atr_c = atr_by_index.get(c_index)
        if atr_c is None or atr_c <= 0:
            return result("N14_KLINE_DATA_INVALID")
        tb_s = s.taker_buy_ratio
        tb_a = a.taker_buy_ratio
        tb_c = c.taker_buy_ratio
        close_location_s = s.close_location
        close_location_c = c.close_location
        if None in (tb_s, tb_a, tb_c, close_location_s, close_location_c):
            return result("N14_KLINE_DATA_INVALID")
        structure = N14Structure(
            symbol=symbol,
            s=s,
            a=a,
            c=c,
            entry=None,
            p=p,
            atr_s_reference=shock.atr_reference,
            atr_c=atr_c,
            volume_median_s=shock.volume_median,
            shock_body_atr_multiple=shock.body_atr_multiple,
            shock_volume_multiple=shock.volume_multiple,
            tb_s=tb_s,
            tb_a=tb_a,
            tb_c=tb_c,
            close_location_s=close_location_s,
            close_location_c=close_location_c,
            flow_flip=tb_c - tb_a,
            entry_min_price=c.close,
            entry_max_price=c.close + entry_extension_atr_max * atr_c,
            quote_volume_rank=(
                scenario.quote_volume_rank
                if scenario is not None and c_index == last_closed_index
                else None
            ),
            market_scenario=scenario if c_index == last_closed_index else None,
            market_context_available=c_index == last_closed_index,
            failed_confirmation_reasons=tuple(failed_confirmation_reasons),
            structure_id=_structure_id(symbol, s, a, c),
        )
        if c_index < last_closed_index:
            historical_structure = replace(
                structure,
                quote_volume_rank=None,
                market_scenario=None,
                market_context_available=False,
            )
            historical.append(
                N14Event(
                    str(s.open_time_ms),
                    "MISSED",
                    "HISTORICAL_N14_ENTRY_MISSED",
                    structure=historical_structure,
                    detail={"historical_market_context": "context_unavailable"},
                )
            )
        else:
            current_candidates.append(structure)
        index = c_index + 1

    if current_candidates:
        structure = current_candidates[0]
        scenario = market_scenarios.get(structure.s.open_time_ms)
        if scenario is None:
            return result(
                "N14_MARKET_CONTEXT_INSUFFICIENT",
                structure,
                historical=historical,
                stages=stages,
            )
        structure = replace(
            structure,
            market_scenario=scenario,
            market_context_available=True,
            quote_volume_rank=scenario.quote_volume_rank,
            entry=candles[-1],
        )
        active_event = N14Event(
            str(structure.s.open_time_ms),
            "C_CONFIRMED",
            "N14_ENTRY_PENDING",
            structure=replace(structure, entry=None),
            detail={"structure": replace(structure, entry=None).json()},
        )
        elapsed = checked_at_ms - candles[-1].open_time_ms
        if not (
            scenario.bullish_breadth_c >= cascade_breadth_min
            or scenario.bullish_breadth_c - scenario.bullish_breadth_s
            >= cascade_improvement_min
        ):
            return result(
                "N14_MARKET_CASCADE_NOT_STABILIZED",
                structure,
                consume=True,
                historical=historical,
                stages=stages,
                elapsed=elapsed,
                active=active_event,
            )
        entry = candles[-1]
        if elapsed < 0 or elapsed >= entry_window_seconds * 1000:
            reason, consume = "N14_ENTRY_WINDOW_EXPIRED", True
        elif entry.low < structure.p:
            reason, consume = "N14_ENTRY_LOW_BROKE_P", True
        elif entry.close < structure.entry_min_price:
            reason, consume = "N14_WAITING_ENTRY_PRICE", False
        elif entry.close > structure.entry_max_price:
            reason, consume = "N14_ENTRY_PRICE_TOO_EXTENDED", True
        else:
            reason, consume = "PASSED", True
        return result(
            reason,
            structure,
            passed=reason == "PASSED",
            consume=consume,
            historical=historical,
            stages=stages,
            elapsed=elapsed,
            active=active_event if not consume else None,
        )
    if historical:
        event = historical[-1]
        return result(
            event.reason,
            event.structure,
            historical=historical,
            stages=stages,
        )
    if stages:
        return result(stages[-1].reason, stages=stages)
    if pending_reason:
        return result(pending_reason, active=active_event)
    return result("N14_NOT_SHOCK")
