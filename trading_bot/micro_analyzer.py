from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import hashlib
from statistics import median
from typing import Any, Mapping

from .micro_observation import (
    MicroIncrement,
    MicroObservation,
    MicroObservationContext,
    MicroObservationError,
    MicroObservationWindow,
    canonical_json,
    derive_increment,
    validate_n22_market_context,
)


MICRO_SCHEMA_VERSION = 1
MICRO_EVIDENCE_MAX_BYTES = 8 * 1024
MICRO_SIGNAL_MAX_BYTES = 16 * 1024
MICRO_STRATEGY_IDS = ("N21", "N22", "N23", "N24", "N25")


def _quote_rate_at_least(
    left: MicroIncrement,
    right: MicroIncrement,
    multiplier: Decimal,
) -> bool:
    """Compare cumulative quote rates without rounded repeating division."""

    left_delta = left.after.quote_volume - left.before.quote_volume
    right_delta = right.after.quote_volume - right.before.quote_volume
    return (
        left_delta * Decimal(right.interval_ms)
        >= multiplier * right_delta * Decimal(left.interval_ms)
    )


def _trade_rate_greater(left: MicroIncrement, right: MicroIncrement) -> bool:
    """Compare integral trade rates as exact rational numbers."""

    left_delta = left.after.trade_count - left.before.trade_count
    right_delta = right.after.trade_count - right.before.trade_count
    return (
        left_delta * right.interval_ms
        > right_delta * left.interval_ms
    )


def _quote_rate_at_least_two_rate_median(
    candidate: MicroIncrement,
    first: MicroIncrement,
    second: MicroIncrement,
) -> bool:
    candidate_delta = candidate.after.quote_volume - candidate.before.quote_volume
    first_delta = first.after.quote_volume - first.before.quote_volume
    second_delta = second.after.quote_volume - second.before.quote_volume
    common = Decimal(first.interval_ms * second.interval_ms)
    return (
        Decimal(2) * candidate_delta * common
        >= Decimal(candidate.interval_ms)
        * (
            first_delta * Decimal(second.interval_ms)
            + second_delta * Decimal(first.interval_ms)
        )
    )


def _trade_rate_at_least_two_rate_median(
    candidate: MicroIncrement,
    first: MicroIncrement,
    second: MicroIncrement,
) -> bool:
    candidate_delta = candidate.after.trade_count - candidate.before.trade_count
    first_delta = first.after.trade_count - first.before.trade_count
    second_delta = second.after.trade_count - second.before.trade_count
    return (
        2 * candidate_delta * first.interval_ms * second.interval_ms
        >= candidate.interval_ms
        * (
            first_delta * second.interval_ms
            + second_delta * first.interval_ms
        )
    )


@dataclass(frozen=True)
class MicroStructure:
    strategy_id: str
    symbol: str
    structure_id: str
    kline_open_time_ms: int
    confirmation_observed_at_ms: int
    deadline_ms: int
    entry_price: Decimal
    entry_min: Decimal
    entry_max: Decimal
    structural_low: Decimal
    atr: Decimal
    quote_volume_rank: int
    winner_key: tuple[Any, ...]
    source_chain_sha256: str


@dataclass(frozen=True)
class MicroAnalysisResult:
    strategy_id: str
    symbol: str
    passed: bool
    reason: str
    structure: MicroStructure | None
    evidence: dict[str, Any] | None
    metrics: dict[str, str]

    @property
    def structure_id(self) -> str | None:
        return self.structure.structure_id if self.structure else None

    @property
    def current_bullish(self) -> bool:
        return bool(self.structure)

    @property
    def quote_volume_rank(self) -> int | None:
        return self.structure.quote_volume_rank if self.structure else None

    def detail_json(self) -> dict[str, Any]:
        payload = {
            "schema_version": MICRO_SCHEMA_VERSION,
            "rule_version": f"{self.strategy_id}_V1",
            "strategy_id": self.strategy_id, "symbol": self.symbol,
            "passed": self.passed, "reason": self.reason,
            "structure_id": self.structure_id, "metrics": self.metrics,
            "source_chain_sha256": (
                self.structure.source_chain_sha256 if self.structure else None
            ),
        }
        if self.structure:
            payload["entry"] = {
                "kline_open_time_ms": self.structure.kline_open_time_ms,
                "confirmation_observed_at_ms": self.structure.confirmation_observed_at_ms,
                "deadline_ms": self.structure.deadline_ms,
                "entry_price": str(self.structure.entry_price),
                "entry_min": str(self.structure.entry_min),
                "entry_max": str(self.structure.entry_max),
                "structural_low": str(self.structure.structural_low),
                "atr": str(self.structure.atr),
                "quote_volume_rank": self.structure.quote_volume_rank,
            }
            if self.evidence is None:
                raise ValueError("passed micro analysis is missing evidence")
            payload["evidence"] = self.evidence
        encoded = canonical_json(payload).encode("utf-8")
        if len(encoded) > MICRO_SIGNAL_MAX_BYTES:
            raise ValueError("micro signal detail exceeds 16KiB")
        return payload


def _compact_observation(item: MicroObservation) -> list[Any]:
    return [
        item.symbol, item.scan_id, item.generation, item.observed_at_ms,
        item.premium_observed_at_ms, item.kline_open_time_ms,
        str(item.open), str(item.high), str(item.low), str(item.close),
        str(item.quote_volume), item.trade_count, str(item.taker_buy_quote),
        str(item.mark_price), str(item.index_price), str(item.funding_rate),
        str(item.premium), item.quote_volume_rank, item.universe_sha256,
        str(item.atr), str(item.quote_rate_baseline), item.source_sha256,
    ]


def _source_chain(items: tuple[MicroObservation, ...]) -> str:
    return hashlib.sha256(canonical_json([i.source_sha256 for i in items]).encode()).hexdigest()


def _result(strategy_id: str, symbol: str, reason: str,
            metrics: dict[str, str] | None = None) -> MicroAnalysisResult:
    return MicroAnalysisResult(strategy_id, symbol, False, reason, None, None, metrics or {})


def _freeze(strategy_id: str, window: MicroObservationWindow,
            extension: Decimal, winner_key: tuple[Any, ...],
            metrics: dict[str, Decimal | int | str], checked_at_ms: int,
            extra_evidence: Mapping[str, Any] | None = None,
            *, confirmation_observed_at_ms: int | None = None) -> MicroAnalysisResult:
    observations = window.observations
    trigger = observations[-1]
    confirmation = (
        trigger.observed_at_ms
        if confirmation_observed_at_ms is None
        else confirmation_observed_at_ms
    )
    if type(confirmation) is not int or confirmation < trigger.observed_at_ms:
        return _result(
            strategy_id,
            window.symbol,
            f"{strategy_id}_MARKET_CONTEXT_INSUFFICIENT",
        )
    deadline = confirmation + 120_000
    if trigger.kline_close_time_ms - confirmation + 1 < 120_000:
        return _result(strategy_id, window.symbol, f"{strategy_id}_CANDLE_REMAINING_INSUFFICIENT")
    if checked_at_ms >= deadline:
        return _result(strategy_id, window.symbol, f"{strategy_id}_ENTRY_WINDOW_EXPIRED")
    structure_id = hashlib.sha256(
        f"{strategy_id}|{window.symbol}|{trigger.kline_open_time_ms}".encode()
    ).hexdigest()[:24]
    chain = _source_chain(observations)
    evidence: dict[str, Any] = {
        "schema_version": 1, "rule_version": f"{strategy_id}_V1",
        "strategy_id": strategy_id, "symbol": window.symbol,
        "structure_id": structure_id,
        "kline_open_time_ms": trigger.kline_open_time_ms,
        "confirmation_observed_at_ms": confirmation,
        "deadline_ms": deadline, "source_chain_sha256": chain,
        "observations": [_compact_observation(item) for item in observations],
        "metrics": {key: str(value) for key, value in metrics.items()},
        "entry_min": str(trigger.close),
        "entry_max": str(trigger.close + extension * trigger.atr),
        "structural_low": str(trigger.low), "atr": str(trigger.atr),
    }
    if extra_evidence:
        evidence.update(extra_evidence)
    if len(canonical_json(evidence).encode("utf-8")) > MICRO_EVIDENCE_MAX_BYTES:
        return _result(strategy_id, window.symbol, f"{strategy_id}_EVIDENCE_TOO_LARGE")
    structure = MicroStructure(
        strategy_id, window.symbol, structure_id, trigger.kline_open_time_ms,
        confirmation, deadline, trigger.close, trigger.close,
        trigger.close + extension * trigger.atr, trigger.low, trigger.atr,
        trigger.quote_volume_rank, winner_key, chain,
    )
    return MicroAnalysisResult(strategy_id, window.symbol, True, "PASSED",
                               structure, evidence,
                               {key: str(value) for key, value in metrics.items()})


def _needs(window: MicroObservationWindow | None, count: int,
           strategy_id: str, symbol: str) -> MicroAnalysisResult | None:
    if window is None or len(window.observations) < count:
        return _result(strategy_id, symbol, f"{strategy_id}_MICRO_OBSERVATION_COLD_START")
    try:
        window.increments
    except MicroObservationError:
        return _result(strategy_id, symbol, f"{strategy_id}_MICRO_OBSERVATION_INVALID")
    return None


def analyze_n21(window: MicroObservationWindow | None, checked_at_ms: int) -> MicroAnalysisResult:
    symbol = window.symbol if window else ""
    missing = _needs(window, 4, "N21", symbol)
    if missing: return missing
    assert window is not None
    i1, i2, i3 = window.increments[-3:]
    d1, d2, d3 = i1.delta_ratio, i2.delta_ratio, i3.delta_ratio
    o0, o1, o2, o3 = window.observations[-4:]
    progress = (o3.close - o0.close) / o3.atr
    location = (o3.close - o3.low) / (o3.high - o3.low) if o3.high > o3.low else Decimal(0)
    if d1 < 0 or d2 < d1 + Decimal("0.04") or d3 < d2 + Decimal("0.04") or d3 < Decimal("0.12"):
        return _result("N21", symbol, "N21_FLOW_PERSISTENCE_NOT_MET")
    if min(i1.quote_rate, i2.quote_rate, i3.quote_rate) <= 0 or i3.quote_rate < Decimal("0.75") * o3.quote_rate_baseline:
        return _result("N21", symbol, "N21_QUOTE_RATE_NOT_MET")
    if not _trade_rate_at_least_two_rate_median(i3, i1, i2):
        return _result("N21", symbol, "N21_TRADE_RATE_NOT_MET")
    if o3.close <= max(o1.close, o2.close) or not Decimal("0.10") <= progress <= Decimal("0.80") or location < Decimal("0.50"):
        return _result("N21", symbol, "N21_PRICE_CONFIRMATION_NOT_MET")
    metrics = {"d1": d1, "d2": d2, "d3": d3, "progress": progress,
               "quote_rate3": i3.quote_rate, "trade_rate3": i3.trade_rate}
    return _freeze("N21", window, Decimal("0.25"),
                   (-d3, -(d3-d1), -progress, o3.quote_volume_rank, symbol),
                   metrics, checked_at_ms)


def _cohort_for_observation(
    windows: Mapping[str, MicroObservationWindow],
    target: MicroObservation,
) -> Mapping[str, MicroObservation] | None:
    if type(windows) is MicroObservationContext:
        return windows.cohort(target.generation, target.universe_sha256)
    if len(windows) != 100:
        return None
    cohort: dict[str, MicroObservation] = {}
    for symbol, window in windows.items():
        match = next((
            item for item in window.observations
            if item.generation == target.generation
            and item.universe_sha256 == target.universe_sha256
        ), None)
        if match is None:
            return None
        cohort[symbol] = match
    return cohort


def _latest_market_returns(
    windows: Mapping[str, MicroObservationWindow],
    before_target: MicroObservation,
    after_target: MicroObservation,
) -> tuple[dict[str, Decimal], int, dict[str, Any]] | None:
    if type(windows) is MicroObservationContext:
        context = windows.n22_market_context(
            before_target.generation,
            after_target.generation,
            before_target.universe_sha256,
        )
        if context is None:
            return None
        try:
            validate_n22_market_context(context)
        except MicroObservationError:
            return None
        return (
            {entry.symbol: entry.price_return for entry in context.entries},
            context.confirmation_observed_at_ms,
            {
                "after_generation": context.after_generation,
                "before_generation": context.before_generation,
                "source_sha256": context.source_sha256,
                "universe_sha256": context.universe_sha256,
            },
        )
    cohort = _cohort_for_observation(windows, before_target)
    if cohort is None or len(cohort) != 100:
        return None
    result: dict[str, Decimal] = {}
    confirmation_times: list[int] = []
    sources: list[list[Any]] = []
    for symbol in sorted(cohort):
        previous = cohort[symbol]
        window = windows.get(symbol)
        if window is None:
            return None
        current = next((
            item for item in reversed(window.observations)
            if item.generation == after_target.generation
        ), None)
        if current is None:
            return None
        try:
            inc = derive_increment(previous, current)
        except MicroObservationError:
            return None
        result[symbol] = inc.price_return
        confirmation_times.append(current.observed_at_ms)
        sources.append([
            symbol,
            previous.source_sha256,
            current.source_sha256,
            str(inc.price_return),
            current.observed_at_ms,
        ])
    return result, max(confirmation_times), {
        "after_generation": after_target.generation,
        "before_generation": before_target.generation,
        "source_sha256": hashlib.sha256(canonical_json({
            "after_generation": after_target.generation,
            "before_generation": before_target.generation,
            "entries": sources,
            "universe_sha256": before_target.universe_sha256,
        }).encode("utf-8")).hexdigest(),
        "universe_sha256": before_target.universe_sha256,
    }


def _n22_complete_generation(
    window: MicroObservationWindow,
    windows: Mapping[str, MicroObservationWindow],
) -> tuple[
    MicroObservationWindow, dict[str, Decimal], int, dict[str, Any],
] | None:
    """Select the newest fully provable N22 Top100 return generation."""

    observations = window.observations
    for end in range(len(observations) - 1, 2, -1):
        candidate_window = MicroObservationWindow(
            window.symbol,
            observations[end - 3:end + 1],
            None,
        )
        try:
            candidate_window.increments
        except MicroObservationError:
            continue
        market_context = _latest_market_returns(
            windows,
            candidate_window.observations[-2],
            candidate_window.observations[-1],
        )
        if market_context is not None:
            market, confirmation, identity = market_context
            return candidate_window, market, confirmation, identity
    return None


def analyze_n22(window: MicroObservationWindow | None,
                windows: Mapping[str, MicroObservationWindow],
                checked_at_ms: int) -> MicroAnalysisResult:
    symbol = window.symbol if window else ""
    missing = _needs(window, 4, "N22", symbol)
    if missing: return missing
    assert window is not None
    generation_context = _n22_complete_generation(window, windows)
    if generation_context is None:
        return _result("N22", symbol, "N22_MARKET_CONTEXT_WARMING")
    (
        analysis_window,
        market,
        market_confirmation,
        market_identity,
    ) = generation_context
    if "BTCUSDT" not in market or "ETHUSDT" not in market:
        return _result("N22", symbol, "N22_MARKET_CONTEXT_INSUFFICIENT")
    i1, i2, i3 = analysis_window.increments[-3:]
    o0, o1, o2, o3 = analysis_window.observations[-4:]
    total_drop = (o0.close - o2.close) / o3.atr
    if not (i1.price_return < 0 and i2.price_return < 0 and abs(i2.price_return) < abs(i1.price_return)):
        return _result("N22", symbol, "N22_DECLINE_SHAPE_NOT_MET")
    if not Decimal("0.05") <= total_drop <= Decimal("0.40"):
        return _result("N22", symbol, "N22_DECLINE_SCALE_NOT_MET")
    if not (i1.delta_ratio < 0 and i2.delta_ratio < 0
            and i2.delta_ratio >= i1.delta_ratio + Decimal("0.10")
            and _quote_rate_at_least(i2, i1, Decimal(1))):
        return _result("N22", symbol, "N22_DELTA_DIVERGENCE_NOT_MET")
    if not (i3.delta_ratio >= Decimal("0.08") and o3.close >= o1.close
            and _quote_rate_at_least_two_rate_median(i3, i1, i2)
            and o3.low == o2.low):
        return _result("N22", symbol, "N22_RECOVERY_CONFIRMATION_NOT_MET")
    market_median = median(market.values())
    if (market["BTCUSDT"] <= Decimal("-0.0015")
            and market["ETHUSDT"] <= Decimal("-0.0015")
            and market_median <= Decimal("-0.0015")):
        return _result("N22", symbol, "N22_SYSTEMIC_CASCADE")
    recovery = (o3.close - o2.close) / o3.atr
    improve = i2.delta_ratio - i1.delta_ratio
    metrics = {"delta_improvement": improve, "recovery": recovery,
               "q3": i3.quote_rate, "market_median": market_median}
    return _freeze("N22", analysis_window, Decimal("0.25"),
                   (-improve, -recovery, -i3.quote_rate, o3.quote_volume_rank, symbol),
                   metrics, checked_at_ms,
                   {
                       "market_context_identity": market_identity,
                       "market_i3_returns": [
                           [s, str(v)] for s, v in sorted(market.items())
                       ],
                   },
                   confirmation_observed_at_ms=market_confirmation)


def analyze_n23(window: MicroObservationWindow | None, checked_at_ms: int) -> MicroAnalysisResult:
    symbol = window.symbol if window else ""
    missing = _needs(window, 4, "N23", symbol)
    if missing: return missing
    assert window is not None
    i1, i2, i3 = window.increments[-3:]
    o0, o1, o2, o3 = window.observations[-4:]
    progress = (o3.close - o0.close) / o3.atr
    location = (o3.close-o3.low)/(o3.high-o3.low) if o3.high > o3.low else Decimal(0)
    if (not _quote_rate_at_least(i2, i1, Decimal("1.25"))
            or not _quote_rate_at_least(i3, i2, Decimal("1.25"))
            or not (_trade_rate_greater(i3, i1)
                    and _trade_rate_greater(i3, i2))):
        return _result("N23", symbol, "N23_RATE_ACCELERATION_NOT_MET")
    if i1.delta_ratio > 0 and i2.delta_ratio > 0:
        return _result("N23", symbol, "N23_ROUTED_TO_N21")
    if i3.delta_ratio < Decimal("0.12"):
        return _result("N23", symbol, "N23_ACTIVE_BUY_NOT_MET")
    if o3.close <= max(o0.close, o1.close, o2.close) or not Decimal("0.15") <= progress <= Decimal("0.90") or o3.high <= max(o0.high, o1.high, o2.high) or location < Decimal("0.50"):
        return _result("N23", symbol, "N23_PRICE_IGNITION_NOT_MET")
    acceleration = i3.quote_rate / i1.quote_rate
    metrics = {"quote_acceleration": acceleration, "d3": i3.delta_ratio,
               "progress": progress, "n3": i3.trade_rate}
    return _freeze("N23", window, Decimal("0.30"),
                   (-acceleration, -i3.delta_ratio, -progress,
                    o3.quote_volume_rank, symbol), metrics, checked_at_ms)


def analyze_n24(window: MicroObservationWindow | None,
                windows: Mapping[str, MicroObservationWindow],
                checked_at_ms: int) -> MicroAnalysisResult:
    symbol = window.symbol if window else ""
    if symbol in {"BTCUSDT", "ETHUSDT"}:
        return _result("N24", symbol, "N24_ANCHOR_NOT_CANDIDATE")
    missing = _needs(window, 3, "N24", symbol)
    if missing: return missing
    btc, eth = windows.get("BTCUSDT"), windows.get("ETHUSDT")
    if _needs(btc, 3, "N24", symbol) or _needs(eth, 3, "N24", symbol):
        return _result("N24", symbol, "N24_ANCHOR_CONTEXT_INSUFFICIENT")
    assert window and btc and eth
    cobs, bobs, eobs = window.observations[-3:], btc.observations[-3:], eth.observations[-3:]
    if not all((cobs[i].generation == bobs[i].generation == eobs[i].generation
                and cobs[i].kline_open_time_ms
                == bobs[i].kline_open_time_ms
                == eobs[i].kline_open_time_ms
                and max(cobs[i].observed_at_ms,bobs[i].observed_at_ms,eobs[i].observed_at_ms)
                - min(cobs[i].observed_at_ms,bobs[i].observed_at_ms,eobs[i].observed_at_ms) <= 75_000)
               for i in range(3)):
        return _result("N24", symbol, "N24_ANCHOR_SAMPLE_SKEW")
    ci1, ci2 = window.increments[-2:]
    bi1, bi2 = btc.increments[-2:]
    ei1, ei2 = eth.increments[-2:]
    btc_lead = bi1.price_return >= Decimal("0.0015") and bi1.delta_ratio >= Decimal("0.08")
    eth_lead = ei1.price_return >= Decimal("0.0015") and ei1.delta_ratio >= Decimal("0.08")
    btc_ok = bi1.price_return >= 0 and bi1.delta_ratio >= Decimal("-0.02")
    eth_ok = ei1.price_return >= 0 and ei1.delta_ratio >= Decimal("-0.02")
    if not ((btc_lead and eth_ok) or (eth_lead and btc_ok)) or ci1.price_return > 0 or ci1.delta_ratio > 0:
        return _result("N24", symbol, "N24_LEADER_LAG_SETUP_NOT_MET")
    if ((bi1.price_return > 0 and (bobs[2].close/bobs[0].close-1) < bi1.price_return/2)
            or (ei1.price_return > 0 and (eobs[2].close/eobs[0].close-1) < ei1.price_return/2)
            or (bi1.price_return == 0 and bi2.price_return < 0)
            or (ei1.price_return == 0 and ei2.price_return < 0)):
        return _result("N24", symbol, "N24_ANCHOR_GAIN_NOT_RETAINED")
    if not (ci2.delta_ratio >= Decimal("0.08") and cobs[2].close > cobs[0].close
            and _quote_rate_at_least(ci2, ci1, Decimal("1.15"))
            and _trade_rate_greater(ci2, ci1)
            and cobs[2].low == cobs[1].low):
        return _result("N24", symbol, "N24_LAG_RECOVERY_NOT_MET")
    metrics = {"d2": ci2.delta_ratio, "quote_ratio": ci2.quote_rate/ci1.quote_rate,
               "candidate_return": cobs[2].close/cobs[0].close-1}
    return _freeze("N24", window, Decimal("0.25"),
                   (-ci2.delta_ratio, -(ci2.quote_rate/ci1.quote_rate),
                    cobs[2].quote_volume_rank, symbol), metrics, checked_at_ms,
                   {"anchor_observations": {
                       "BTCUSDT": [_compact_observation(o) for o in bobs],
                       "ETHUSDT": [_compact_observation(o) for o in eobs]}},
                   confirmation_observed_at_ms=max(
                       cobs[-1].observed_at_ms,
                       bobs[-1].observed_at_ms,
                       eobs[-1].observed_at_ms,
                   ))


def analyze_n25(window: MicroObservationWindow | None,
                windows: Mapping[str, MicroObservationWindow],
                checked_at_ms: int) -> MicroAnalysisResult:
    symbol = window.symbol if window else ""
    missing = _needs(window, 3, "N25", symbol)
    if missing: return missing
    assert window is not None
    obs = window.observations[-3:]
    p0, p1, p2 = (item.premium for item in obs)
    p0_generation = obs[0].generation
    market_p0: list[tuple[str, Decimal]] = []
    cohort = _cohort_for_observation(windows, obs[0])
    if cohort is None or len(cohort) != 100:
        return _result("N25", symbol, "N25_MARKET_CONTEXT_INSUFFICIENT")
    for market_symbol, match in cohort.items():
        if (
            match.generation != p0_generation
            or match.universe_sha256 != obs[0].universe_sha256
        ):
            return _result("N25", symbol, "N25_MARKET_CONTEXT_INSUFFICIENT")
        market_p0.append((market_symbol, match.premium))
    bottom20 = {s for s, _ in sorted(market_p0, key=lambda item: (item[1], item[0]))[:20]}
    if p0 > Decimal("-0.0002") or symbol not in bottom20:
        return _result("N25", symbol, "N25_PREMIUM_ANOMALY_NOT_MET")
    if not (p0 < p1 < p2 < 0 and p1-p0 >= Decimal("0.00005")
            and p2-p1 >= Decimal("0.00005") and abs(p2) <= Decimal("0.80")*abs(p0)):
        return _result("N25", symbol, "N25_PREMIUM_COMPRESSION_NOT_MET")
    i1, i2 = window.increments[-2:]
    if not (i2.delta_ratio >= Decimal("0.08")
            and _quote_rate_at_least(i2, i1, Decimal(1))
            and obs[2].close > max(obs[0].close, obs[1].close)
            and obs[2].high > max(obs[0].high, obs[1].high)
            and obs[2].low == obs[1].low):
        return _result("N25", symbol, "N25_PRICE_FLOW_CONFIRMATION_NOT_MET")
    compression = abs(p0)-abs(p2)
    metrics = {"p0": p0, "p1": p1, "p2": p2,
               "compression": compression, "d2": i2.delta_ratio}
    return _freeze("N25", window, Decimal("0.20"),
                   (p0, -compression, -i2.delta_ratio,
                    obs[2].quote_volume_rank, symbol), metrics, checked_at_ms,
                   {"p0_market_premiums": [[s, str(p)] for s, p in sorted(market_p0)]},
                   confirmation_observed_at_ms=max(
                       obs[-1].observed_at_ms,
                       max(item.observed_at_ms for item in cohort.values()),
                   ))


def analyze_micro_strategy(strategy_id: str, symbol: str,
                           windows: Mapping[str, MicroObservationWindow],
                           checked_at_ms: int) -> MicroAnalysisResult:
    window = windows.get(symbol)
    if strategy_id == "N21": return analyze_n21(window, checked_at_ms)
    if strategy_id == "N22": return analyze_n22(window, windows, checked_at_ms)
    if strategy_id == "N23": return analyze_n23(window, checked_at_ms)
    if strategy_id == "N24": return analyze_n24(window, windows, checked_at_ms)
    if strategy_id == "N25": return analyze_n25(window, windows, checked_at_ms)
    raise ValueError("unsupported micro strategy")
