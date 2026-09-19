from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Any, Callable, Mapping, Union

from .analyzer import AnalysisResult, analyze_symbol
from .monitor import FundingCandidate
from .exchange_symbol import (
    attest_authenticated_symbol_set,
    canonical_exchange_symbol,
)
from .n06_analyzer import N06AnalysisResult, analyze_n06_double_break_pullback
from .n07_analyzer import N07AnalysisResult, analyze_n07_p1_retest
from .n08_analyzer import (
    N08AnalysisResult,
    analyze_n08_range_five_bullish,
    find_n08_range_reset_open_time,
    find_n08_historical_missed_events,
    is_same_n08_range_family,
)
from .n09_analyzer import N09AnalysisResult, analyze_n09_slow_decline_half_retrace
from .n10_analyzer import (
    N10AnalysisResult,
    analyze_n10_volume_liquidity_sweep_reclaim,
)
from .n11_analyzer import (
    N11AnalysisResult,
    analyze_n11_volatility_squeeze_breakout_retest,
)
from .n12_analyzer import (
    FIFTEEN_MINUTES_MS,
    N12AnalysisResult,
    RelativeStrength,
    analyze_n12_relative_strength_first_pullback,
    calculate_relative_strength_return,
    parse_n12_klines,
)
from .n13_analyzer import (
    N13AnalysisResult,
    N13Candle,
    N13Event,
    N13Structure,
    analyze_n13_vwap_rotation,
    n13_metrics,
    parse_n13_klines,
)
from .n14_analyzer import (
    N14AnalysisResult,
    N14Event,
    N14Structure,
    analyze_n14_sell_pressure_decay_reversal,
    parse_n14_klines,
)
from .n14_snapshot import (
    INTERVAL_MS as N14_INTERVAL_MS,
    N14Snapshot,
    build_n14_snapshot,
    decode_n14_snapshot,
    n14_bullish_breadth_for_current_c,
)
from .n15_analyzer import (
    N15AnalysisResult,
    analyze_n15_breadth_recovery_leader,
    n15_structure_id,
)
from .n15_snapshot import (
    N15HistoricalSourceUnavailableError,
    N15_MIN_RUNTIME_CANDLES,
    N15SnapshotSourceUnavailableError,
    N15Snapshot,
    build_n15_snapshot,
    decode_n15_historical_snapshot,
    decode_n15_snapshot,
    parse_n15_klines,
    upgrade_n15_snapshot_payload,
)
from .n16_analyzer import (
    N16AnalysisResult,
    analyze_n16_mature_trend_support,
    loads_n16_state_envelope,
)
from .n17_analyzer import (
    N17AnalysisResult,
    analyze_n17_range_support_rebound,
)
from .n18_analyzer import (
    N18AnalysisResult,
    analyze_n18_ascending_triangle_breakout,
)
from .n19_analyzer import (
    N19AnalysisResult,
    analyze_n19_staircase_exhaustion_reversal,
)
from .n20_analyzer import (
    N20AnalysisResult,
    N20MarketAnalysis,
    decode_n20_state_evidence,
    terminalize_consumed_n20_state,
    analyze_n20_market_episode,
)
from .micro_analyzer import (
    MICRO_STRATEGY_IDS,
    MicroAnalysisResult,
    analyze_micro_strategy,
)
from .micro_observation import MicroObservationWindow
from .recorder import (
    HISTORY_COVERAGE_STRATEGY_IDS,
    HistoryCoverageProposal,
    N08HistoryCoverage,
    N08StructureState,
    ReviewRecorder,
    StrategyState,
    _decode_n14_terminal_envelope,
    _n13_strict_json_loads,
    utc_now,
)
from .strategies import (
    N16_STRATEGY,
    N16StrategyDefinition,
    N17_STRATEGY,
    N17StrategyDefinition,
    N18_STRATEGY,
    N18StrategyDefinition,
    N19_STRATEGY,
    N19StrategyDefinition,
    N20_STRATEGY,
    N20StrategyDefinition,
    MicroStrategyDefinition,
    StrategyConfig,
    funding_rate_passes,
)


StrategyAnalysis = Union[
    AnalysisResult,
    N06AnalysisResult,
    N07AnalysisResult,
    N08AnalysisResult,
    N09AnalysisResult,
    N10AnalysisResult,
    N11AnalysisResult,
    N12AnalysisResult,
    N13AnalysisResult,
    N14AnalysisResult,
    N15AnalysisResult,
    N16AnalysisResult,
    N17AnalysisResult,
    N18AnalysisResult,
    N19AnalysisResult,
    N20AnalysisResult,
    MicroAnalysisResult,
]


_HISTORY_COVERAGE_COOLDOWN_BLOCKING_REASONS = frozenset(
    {
        "N17_FROZEN_EVIDENCE_INVALID",
        "N17_KLINE_SEQUENCE_INVALID",
        "N18_FROZEN_EVIDENCE_INVALID",
        "N18_KLINE_SEQUENCE_INVALID",
        "N19_FROZEN_EVIDENCE_INVALID",
        "N19_KLINE_SEQUENCE_INVALID",
        "N19_MARKET_CONTEXT_INSUFFICIENT",
    }
)


def _same_persisted_state_record(left: Any, right: Any) -> bool:
    """Match independently reconstructed immutable state records by full value."""

    return (
        left is not None
        and right is not None
        and type(left) is type(right)
        and left == right
    )


_N13_CURRENT_TERMINAL_STATE_PAIRS = {
    ("CONSUMED", "PASSED"),
    ("MISSED", "N13_MARKET_BREADTH_NOT_MET"),
    ("MISSED", "N13_RETURN_NOT_POSITIVE"),
    ("MISSED", "N13_RETURN_RANK_OUT_OF_RANGE"),
    ("MISSED", "N13_VWAP_NOT_RISING"),
    ("MISSED", "N13_ENTRY_WINDOW_EXPIRED"),
    ("MISSED", "N13_ENTRY_LOW_BROKE_P"),
    ("MISSED", "N13_ENTRY_PRICE_TOO_EXTENDED"),
}
_N13_INTERVAL_MS = 900_000

_N13_WINDOW_RELATIVE_STATE_KEYS = frozenset(
    {
        "atr_c",
        "entry_max_price",
        "zone_lower",
        "zone_upper",
    }
)
_N13_CANDLE_STATE_KEYS = frozenset(
    {
        "open_time_ms",
        "open",
        "high",
        "low",
        "close",
        "base_volume",
        "quote_volume",
        "taker_buy_quote_volume",
    }
)
_N13_HISTORICAL_STRUCTURE_STABLE_KEYS = frozenset(
    {
        "structure_id",
        "a",
        "t",
        "c",
        "entry",
        "p",
        "vwap_c",
        "entry_min_price",
        "return_24h",
        "return_rank",
        "positive_return_breadth",
        "above_vwap_breadth",
        "snapshot_context_available",
        "armed_expiry_time_ms",
        "failed_confirmation_reasons",
    }
)
_N13_CURRENT_TERMINAL_EVIDENCE_KEYS = frozenset(
    {
        "structure_id",
        "a",
        "t",
        "c",
        "p",
        "vwap_c",
        "atr_c",
        "entry_min_price",
        "entry_max_price",
        "zone_lower",
        "zone_upper",
        "armed_expiry_time_ms",
        "failed_confirmation_reasons",
    }
)
_N13_CURRENT_TERMINAL_ENVELOPE_KEYS = frozenset(
    {
        "schema_version",
        "strategy_id",
        "symbol",
        "t_time",
        "structure_id",
        "status",
        "reason",
        "evidence",
        "canonical_sha256",
    }
)
_N13_STAGE_STATE_DETAIL_KEYS = {
    "N13_ARMED_WINDOW_EXPIRED": (
        frozenset({"a", "armed_expiry_time_ms"}),
        frozenset(
            {"a", "zone_lower", "zone_upper", "armed_expiry_time_ms"}
        ),
    ),
    "N13_VALUE_ZONE_SKIPPED": (
        frozenset({"a"}),
        frozenset({"a", "zone_lower", "zone_upper"}),
    ),
}
_N13_T_STAGE_REASONS = frozenset(
    {
        "N13_VALUE_BAND_BROKEN",
        "N13_CONFIRMATION_NOT_FOUND",
    }
)
_N13_CONFIRMATION_FAILURE_REASONS = frozenset(
    {
        "N13_CONFIRMATION_FLAT_CANDLE",
        "N13_CONFIRMATION_CLOSE_BELOW_VWAP",
        "N13_CONFIRMATION_NOT_BULLISH",
        "N13_CONFIRMATION_CLOSE_NOT_ADVANCING",
        "N13_CONFIRMATION_CLOSE_LOCATION_TOO_LOW",
        "N13_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW",
    }
)
_N13_LEGACY_T_STAGE_DETAIL_KEYS = frozenset(
    {"t", "failed_confirmation_reasons"}
)
_N13_T_STAGE_DETAIL_KEYS = frozenset(
    {
        "schema_version",
        "symbol",
        "t_time",
        "reason",
        "a",
        "t",
        "vwap_a",
        "atr_a",
        "zone_lower",
        "zone_upper",
        "armed_expiry_time_ms",
        "armed_max_bars",
        "vwap_lookback_bars",
        "atr_period",
        "upper_atr_fraction",
        "lower_atr_fraction",
        "confirmation_max_bars",
        "confirmation_close_location_min",
        "confirmation_taker_buy_ratio_min",
        "metric_context_bars",
        "pre_touch_bars",
        "confirmation_checks",
        "failed_confirmation_reasons",
        "canonical_sha256",
    }
)
_N13_CONFIRMATION_CHECK_KEYS = frozenset(
    {"candle", "vwap", "outcome"}
)
_N13_EVENT_MATCH_EXACT = "EXACT"
_N13_EVENT_MATCH_ISOLATED = "ISOLATED_HISTORICAL_COLLISION"
_N13_EVENT_MATCH_CONFLICT = "CONFLICT"
_N13_ISOLATED_WARNING_FINGERPRINT_CACHE_MAX = 512
_OFFBOARD_LIFECYCLE_ONLY_STRATEGIES = frozenset(
    {"N14", "N15", "N16", "N17", "N18", "N19", "N20"}
)


def _d13(value: Any) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ValueError("non-finite N13 snapshot decimal")
    return parsed


def _n13_snapshot_config_signature(strategy: StrategyConfig) -> dict[str, Any]:
    return {
        "volume_top_n": strategy.volume_top_n,
        "entry_window_seconds": strategy.entry_window_seconds,
        "positive_breadth_min": str(strategy.n13_positive_breadth_min),
        "above_vwap_breadth_min": str(strategy.n13_above_vwap_breadth_min),
        "rank_min": strategy.n13_rank_min,
        "rank_max": strategy.n13_rank_max,
        "vwap_lookback_bars": strategy.n13_vwap_lookback_bars,
        "atr_period": strategy.n13_atr_period,
        "vwap_slope_lookback_bars": strategy.n13_vwap_slope_lookback_bars,
        "upper_atr_fraction": str(strategy.n13_upper_atr_fraction),
        "lower_atr_fraction": str(strategy.n13_lower_atr_fraction),
        "armed_max_bars": strategy.n13_armed_max_bars,
        "confirmation_max_bars": strategy.n13_confirmation_max_bars,
        "confirmation_close_location_min": str(
            strategy.n13_confirmation_close_location_min
        ),
        "confirmation_taker_buy_ratio_min": str(
            strategy.n13_confirmation_taker_buy_ratio_min
        ),
        "entry_extension_atr_max": str(strategy.n13_entry_extension_atr_max),
    }


def _n13_exact_json_value(actual: Any, expected: Any) -> bool:
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            _n13_exact_json_value(actual[key], expected[key])
            for key in expected
        )
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(
            _n13_exact_json_value(left, right)
            for left, right in zip(actual, expected)
        )
    return actual == expected


def _n13_audit_collision_fingerprint(
    existing_event: tuple[Any, ...],
    replay_record: dict[str, Any],
) -> str:
    """Fingerprint the exact immutable row and replay evidence for logging."""
    payload = {
        "existing": {
            "strategy_id": existing_event[1],
            "symbol": existing_event[2],
            "t_time": existing_event[3],
            "structure_id": existing_event[4],
            "status": existing_event[5],
            "reason": existing_event[6],
            "detail_json": existing_event[7],
        },
        "replay": {
            "strategy_id": replay_record.get("strategy_id"),
            "symbol": replay_record.get("symbol"),
            "t_time": replay_record.get("t_time"),
            "structure_id": replay_record.get("structure_id"),
            "status": replay_record.get("status"),
            "reason": replay_record.get("reason"),
            "detail": replay_record.get("detail"),
        },
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _n13_finite_decimal_text(value: Any) -> bool:
    if type(value) is not str or not value:
        return False
    try:
        parsed = Decimal(value)
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return False
    return parsed.is_finite()


def _n13_canonical_t_time(value: Any) -> int | None:
    if type(value) is not str or not value:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if (
        parsed <= 0
        or str(parsed) != value
        or parsed % _N13_INTERVAL_MS != 0
    ):
        return None
    return parsed


def _n13_candle_state_is_valid(value: Any) -> bool:
    if (
        type(value) is not dict
        or value.keys() != _N13_CANDLE_STATE_KEYS
        or type(value["open_time_ms"]) is not int
        or value["open_time_ms"] <= 0
        or value["open_time_ms"] % _N13_INTERVAL_MS != 0
        or not all(
            _n13_finite_decimal_text(value[key])
            for key in _N13_CANDLE_STATE_KEYS
            if key != "open_time_ms"
        )
    ):
        return False
    return _n13_kline_values_are_valid(
        *(Decimal(value[key]) for key in (
            "open",
            "high",
            "low",
            "close",
            "base_volume",
            "quote_volume",
            "taker_buy_quote_volume",
        ))
    )


def _n13_kline_values_are_valid(
    open_price: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    base_volume: Decimal,
    quote_volume: Decimal,
    taker_buy_quote_volume: Decimal,
) -> bool:
    values = (
        open_price,
        high,
        low,
        close,
        base_volume,
        quote_volume,
        taker_buy_quote_volume,
    )
    return (
        all(type(value) is Decimal and value.is_finite() for value in values)
        and min(open_price, high, low, close) > 0
        and high >= low
        and high >= max(open_price, close)
        and low <= min(open_price, close)
        and min(base_volume, quote_volume, taker_buy_quote_volume) >= 0
        and taker_buy_quote_volume <= quote_volume
    )


def _n13_armed_window_is_valid(
    a: Any,
    t: Any,
    armed_expiry_time_ms: Any,
    armed_max_bars: Any,
) -> bool:
    if (
        type(a) is not dict
        or type(t) is not dict
        or type(armed_expiry_time_ms) is not int
        or type(armed_max_bars) is not int
        or armed_max_bars <= 0
    ):
        return False
    a_time = a.get("open_time_ms")
    t_time = t.get("open_time_ms")
    return (
        type(a_time) is int
        and type(t_time) is int
        and armed_expiry_time_ms
        == a_time + armed_max_bars * _N13_INTERVAL_MS
        and a_time < t_time <= armed_expiry_time_ms
    )


def _n13_pricing_evidence_is_valid(
    value: Any,
    entry_extension_atr_max: Any,
    *,
    require_extension_fields: bool,
) -> bool:
    if (
        type(value) is not dict
        or type(entry_extension_atr_max) is not Decimal
        or not entry_extension_atr_max.is_finite()
        or entry_extension_atr_max < 0
        or not _n13_candle_state_is_valid(value.get("t"))
        or not _n13_candle_state_is_valid(value.get("c"))
        or not all(
            _n13_finite_decimal_text(value.get(key))
            for key in ("p", "vwap_c", "entry_min_price")
        )
    ):
        return False
    p = Decimal(value["p"])
    vwap_c = Decimal(value["vwap_c"])
    entry_min = Decimal(value["entry_min_price"])
    c_close = Decimal(value["c"]["close"])
    if (
        p <= 0
        or p
        > min(
            Decimal(value["t"]["low"]),
            Decimal(value["c"]["low"]),
        )
        or vwap_c <= 0
        or vwap_c > c_close
        or entry_min <= 0
        or entry_min != c_close
    ):
        return False
    has_atr = "atr_c" in value
    has_entry_max = "entry_max_price" in value
    if has_atr != has_entry_max or (
        require_extension_fields and not has_atr
    ):
        return False
    if not has_atr:
        return True
    if not all(
        _n13_finite_decimal_text(value.get(key))
        for key in ("atr_c", "entry_max_price")
    ):
        return False
    atr_c = Decimal(value["atr_c"])
    entry_max = Decimal(value["entry_max_price"])
    return (
        atr_c > 0
        and entry_max
        == entry_min + entry_extension_atr_max * atr_c
    )


def _n13_structure_zone_evidence_is_valid(
    value: Any,
    *,
    require_zone_fields: bool,
) -> bool:
    if type(value) is not dict:
        return False
    has_lower = "zone_lower" in value
    has_upper = "zone_upper" in value
    if has_lower != has_upper or (require_zone_fields and not has_lower):
        return False
    if not has_lower:
        return True
    a = value.get("a")
    t = value.get("t")
    if (
        not _n13_candle_state_is_valid(a)
        or not _n13_candle_state_is_valid(t)
        or not _n13_finite_decimal_text(value.get("zone_lower"))
        or not _n13_finite_decimal_text(value.get("zone_upper"))
    ):
        return False
    zone_lower = Decimal(value["zone_lower"])
    zone_upper = Decimal(value["zone_upper"])
    return (
        zone_lower <= zone_upper
        and zone_upper > 0
        and Decimal(a["low"]) > zone_upper
        and Decimal(t["low"]) <= zone_upper
        and Decimal(t["high"]) >= zone_lower
    )


def _n13_structure_identity_is_valid(
    symbol: Any,
    t_time: Any,
    structure_id: Any,
    a: Any,
    t: Any,
    c: Any,
) -> bool:
    parsed_t_time = _n13_canonical_t_time(t_time)
    if (
        type(symbol) is not str
        or not symbol
        or parsed_t_time is None
        or type(structure_id) is not str
        or not structure_id
        or not all(_n13_candle_state_is_valid(item) for item in (a, t, c))
    ):
        return False
    a_time = a["open_time_ms"]
    t_time_ms = t["open_time_ms"]
    c_time = c["open_time_ms"]
    if (
        parsed_t_time != t_time_ms
        or not a_time < t_time_ms <= c_time
        or (t_time_ms - a_time) % _N13_INTERVAL_MS != 0
        or (c_time - t_time_ms) % _N13_INTERVAL_MS != 0
    ):
        return False
    expected_sid = hashlib.sha256(
        f"{symbol}|{t_time_ms}|{c_time}".encode("utf-8")
    ).hexdigest()[:24]
    return structure_id == expected_sid


def _n13_reason_list_is_valid(value: Any) -> bool:
    return type(value) is list and all(
        type(item) is str
        and item in _N13_CONFIRMATION_FAILURE_REASONS
        for item in value
    )


def _n13_current_terminal_detail_is_valid(
    detail: Any,
    strategy_id: str,
    symbol: str,
    t_time: str,
    structure_id: str,
    status: str,
    reason: str,
    armed_max_bars: int,
    entry_extension_atr_max: Decimal,
) -> bool:
    parsed_t_time = _n13_canonical_t_time(t_time)
    if (
        type(strategy_id) is not str
        or not strategy_id
        or type(symbol) is not str
        or not symbol
        or parsed_t_time is None
        or type(detail) is not dict
        or detail.keys() != _N13_CURRENT_TERMINAL_ENVELOPE_KEYS
        or type(detail["schema_version"]) is not int
        or detail["schema_version"] != 1
        or type(detail["strategy_id"]) is not str
        or detail["strategy_id"] != strategy_id
        or type(detail["symbol"]) is not str
        or detail["symbol"] != symbol
        or type(detail["t_time"]) is not str
        or detail["t_time"] != t_time
        or type(detail["structure_id"]) is not str
        or detail["structure_id"] != structure_id
        or type(detail["status"]) is not str
        or detail["status"] != status
        or type(detail["reason"]) is not str
        or detail["reason"] != reason
        or type(detail["canonical_sha256"]) is not str
        or len(detail["canonical_sha256"]) != 64
    ):
        return False
    evidence = detail["evidence"]
    if (
        type(evidence) is not dict
        or evidence.keys() != _N13_CURRENT_TERMINAL_EVIDENCE_KEYS
        or type(evidence["structure_id"]) is not str
        or evidence["structure_id"] != structure_id
        or not _n13_structure_identity_is_valid(
            symbol,
            t_time,
            structure_id,
            evidence["a"],
            evidence["t"],
            evidence["c"],
        )
        or not _n13_armed_window_is_valid(
            evidence["a"],
            evidence["t"],
            evidence["armed_expiry_time_ms"],
            armed_max_bars,
        )
        or not _n13_pricing_evidence_is_valid(
            evidence,
            entry_extension_atr_max,
            require_extension_fields=True,
        )
        or not _n13_structure_zone_evidence_is_valid(
            evidence,
            require_zone_fields=True,
        )
        or not all(
            _n13_finite_decimal_text(evidence[key])
            for key in (
                "p",
                "vwap_c",
                "atr_c",
                "entry_min_price",
                "entry_max_price",
                "zone_lower",
                "zone_upper",
            )
        )
        or type(evidence["armed_expiry_time_ms"]) is not int
        or evidence["armed_expiry_time_ms"] <= 0
        or not _n13_reason_list_is_valid(
            evidence["failed_confirmation_reasons"]
        )
    ):
        return False
    unsigned = {
        key: detail[key]
        for key in detail
        if key != "canonical_sha256"
    }
    canonical = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    expected_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return detail["canonical_sha256"] == expected_hash


def _n13_existing_current_terminal_is_valid(
    row: Any,
    strategy_id: str,
    symbol: str,
    t_time: str,
    structure: Any,
    armed_max_bars: int,
    entry_extension_atr_max: Decimal,
) -> bool:
    if (
        row is None
        or row[1] != strategy_id
        or row[2] != symbol
        or row[3] != t_time
        or row[4] != structure.structure_id
        or (row[5], row[6]) not in _N13_CURRENT_TERMINAL_STATE_PAIRS
    ):
        return False
    try:
        detail = _n13_strict_json_loads(row[7])
    except (TypeError, ValueError):
        return False
    if not _n13_current_terminal_detail_is_valid(
        detail,
        strategy_id,
        symbol,
        t_time,
        structure.structure_id,
        row[5],
        row[6],
        armed_max_bars,
        entry_extension_atr_max,
    ):
        return False
    expected = _n13_current_terminal_envelope(
        strategy_id,
        symbol,
        t_time,
        structure,
        row[5],
        row[6],
    )
    if structure.entry is not None:
        return _n13_exact_json_value(detail, expected)
    # A current terminal is immutable once written.  When the fixed-length
    # kline window advances, Wilder ATR can select a different valid A for the
    # same official T/C episode.  Both envelopes are validated independently;
    # the historical-only comparator keeps T/C and every stable field exact.
    return _n13_historical_terminal_evidence_matches(
        detail["evidence"],
        expected["evidence"],
        armed_max_bars,
        entry_extension_atr_max,
    )


def _stable_n13_state_detail(value: Any) -> Any:
    """Project persistent N13 evidence onto fixed-window-stable fields."""
    if isinstance(value, dict):
        return {
            key: _stable_n13_state_detail(item)
            for key, item in value.items()
            if key != "index" and key not in _N13_WINDOW_RELATIVE_STATE_KEYS
        }
    if isinstance(value, list):
        return [_stable_n13_state_detail(item) for item in value]
    if isinstance(value, tuple):
        return [_stable_n13_state_detail(item) for item in value]
    return value


def _n13_historical_terminal_projection(value: Any) -> Any:
    stable = _stable_n13_state_detail(value)
    if type(stable) is not dict:
        return stable
    return {
        key: item
        for key, item in stable.items()
        if key not in {"a", "armed_expiry_time_ms"}
    }


def _n13_historical_terminal_evidence_matches(
    actual: Any,
    expected: Any,
    armed_max_bars: int,
    entry_extension_atr_max: Decimal,
) -> bool:
    if type(actual) is not dict or type(expected) is not dict:
        return False
    actual_a = actual.get("a")
    expected_a = expected.get("a")
    if type(actual_a) is not dict or type(expected_a) is not dict:
        return False
    if actual_a.get("open_time_ms") == expected_a.get("open_time_ms"):
        return _n13_exact_json_value(
            _stable_n13_state_detail(actual),
            _stable_n13_state_detail(expected),
        )
    if not all(
        _n13_historical_antecedent_is_self_consistent(
            evidence,
            armed_max_bars,
            entry_extension_atr_max,
        )
        for evidence in (actual, expected)
    ):
        return False
    return _n13_exact_json_value(
        _n13_historical_terminal_projection(actual),
        _n13_historical_terminal_projection(expected),
    )


def _n13_historical_antecedent_is_self_consistent(
    evidence: Any,
    armed_max_bars: int,
    entry_extension_atr_max: Decimal,
) -> bool:
    if (
        type(evidence) is not dict
        or not _N13_WINDOW_RELATIVE_STATE_KEYS.issubset(evidence.keys())
    ):
        return False
    a = evidence.get("a")
    t = evidence.get("t")
    if (
        not _n13_candle_state_is_valid(a)
        or not _n13_candle_state_is_valid(t)
        or not _n13_armed_window_is_valid(
            a,
            t,
            evidence.get("armed_expiry_time_ms"),
            armed_max_bars,
        )
        or not _n13_pricing_evidence_is_valid(
            evidence,
            entry_extension_atr_max,
            require_extension_fields=True,
        )
        or not all(
            _n13_finite_decimal_text(evidence.get(key))
            for key in _N13_WINDOW_RELATIVE_STATE_KEYS
        )
    ):
        return False
    return _n13_structure_zone_evidence_is_valid(
        evidence,
        require_zone_fields=True,
    )


def _n13_state_detail_without_indexes(value: Any) -> Any:
    """Keep the first frozen N13 audit intact while removing local indexes."""
    if isinstance(value, dict):
        return {
            key: _n13_state_detail_without_indexes(item)
            for key, item in value.items()
            if key != "index"
        }
    if isinstance(value, (list, tuple)):
        return [_n13_state_detail_without_indexes(item) for item in value]
    return value


def _n13_historical_structure_state_is_valid(
    value: Any,
    symbol: str,
    t_time: str,
    structure_id: str,
    armed_max_bars: int,
    entry_extension_atr_max: Decimal,
) -> bool:
    if type(value) is not dict:
        return False
    allowed_keys = (
        _N13_HISTORICAL_STRUCTURE_STABLE_KEYS,
        _N13_HISTORICAL_STRUCTURE_STABLE_KEYS
        | _N13_WINDOW_RELATIVE_STATE_KEYS,
    )
    if value.keys() not in allowed_keys:
        return False
    if (
        type(value["structure_id"]) is not str
        or value["structure_id"] != structure_id
        or not _n13_structure_identity_is_valid(
            symbol,
            t_time,
            structure_id,
            value["a"],
            value["t"],
            value["c"],
        )
        or value["entry"] is not None
        or not _n13_armed_window_is_valid(
            value["a"],
            value["t"],
            value["armed_expiry_time_ms"],
            armed_max_bars,
        )
        or not _n13_pricing_evidence_is_valid(
            value,
            entry_extension_atr_max,
            require_extension_fields=(value.keys() == allowed_keys[1]),
        )
        or not _n13_structure_zone_evidence_is_valid(
            value,
            require_zone_fields=(value.keys() == allowed_keys[1]),
        )
        or not all(
            _n13_finite_decimal_text(value[key])
            for key in ("p", "vwap_c", "entry_min_price")
        )
        or value["return_24h"] is not None
        or value["return_rank"] is not None
        or value["positive_return_breadth"] is not None
        or value["above_vwap_breadth"] is not None
        or type(value["snapshot_context_available"]) is not bool
        or value["snapshot_context_available"]
        or type(value["armed_expiry_time_ms"]) is not int
        or value["armed_expiry_time_ms"] <= 0
        or not _n13_reason_list_is_valid(
            value["failed_confirmation_reasons"]
        )
    ):
        return False
    if value.keys() == allowed_keys[1] and not all(
        _n13_finite_decimal_text(value[key])
        for key in _N13_WINDOW_RELATIVE_STATE_KEYS
    ):
        return False
    return True


def _n13_confirmation_outcome(
    candle: dict[str, Any],
    previous: dict[str, Any],
    vwap: Decimal,
    zone_lower: Decimal,
    close_location_min: Decimal,
    taker_buy_ratio_min: Decimal,
) -> str | None:
    close = Decimal(candle["close"])
    high = Decimal(candle["high"])
    low = Decimal(candle["low"])
    open_price = Decimal(candle["open"])
    quote_volume = Decimal(candle["quote_volume"])
    taker_buy_quote = Decimal(candle["taker_buy_quote_volume"])
    if close < zone_lower:
        return "N13_VALUE_BAND_BROKEN"
    if high == low:
        return "N13_CONFIRMATION_FLAT_CANDLE"
    if close < vwap:
        return "N13_CONFIRMATION_CLOSE_BELOW_VWAP"
    if close <= open_price:
        return "N13_CONFIRMATION_NOT_BULLISH"
    if close <= Decimal(previous["close"]):
        return "N13_CONFIRMATION_CLOSE_NOT_ADVANCING"
    if (close - low) / (high - low) < close_location_min:
        return "N13_CONFIRMATION_CLOSE_LOCATION_TOO_LOW"
    if (
        quote_volume <= 0
        or taker_buy_quote / quote_volume < taker_buy_ratio_min
    ):
        return "N13_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW"
    return None


def _n13_legacy_t_stage_detail_is_valid(
    reason: str,
    value: Any,
    confirmation_max_bars: int,
) -> bool:
    if (
        reason not in _N13_T_STAGE_REASONS
        or type(value) is not dict
        or value.keys() != _N13_LEGACY_T_STAGE_DETAIL_KEYS
        or type(confirmation_max_bars) is not int
        or confirmation_max_bars <= 0
        or not _n13_candle_state_is_valid(value["t"])
        or not _n13_reason_list_is_valid(
            value["failed_confirmation_reasons"]
        )
    ):
        return False
    failure_count = len(value["failed_confirmation_reasons"])
    if reason == "N13_CONFIRMATION_NOT_FOUND":
        return failure_count == confirmation_max_bars
    return failure_count < confirmation_max_bars


def _n13_t_stage_replays_from_metric_context(
    value: dict[str, Any],
    symbol: str,
    vwap_lookback_bars: int,
    atr_period: int,
    upper_atr_fraction: Decimal,
    lower_atr_fraction: Decimal,
    armed_max_bars: int,
    confirmation_max_bars: int,
    confirmation_close_location_min: Decimal,
    confirmation_taker_buy_ratio_min: Decimal,
) -> bool:
    try:
        context = value["metric_context_bars"]

        def raw_bar(item: dict[str, Any]) -> list[Any]:
            return [
                item["open_time_ms"],
                item["open"],
                item["high"],
                item["low"],
                item["close"],
                item["base_volume"],
                item["open_time_ms"] + _N13_INTERVAL_MS - 1,
                item["quote_volume"],
                "0",
                "0",
                item["taker_buy_quote_volume"],
                "0",
            ]

        raw = [raw_bar(item) for item in context]
        synthetic_current = dict(context[-1])
        synthetic_current["open_time_ms"] += _N13_INTERVAL_MS
        raw.append(raw_bar(synthetic_current))
        replay = analyze_n13_vwap_rotation(
            symbol,
            raw,
            return_rank=11,
            return_24h=Decimal("0.01"),
            positive_return_breadth=Decimal("1"),
            above_vwap_breadth=Decimal("1"),
            snapshot_context_complete=True,
            checked_at_ms=(
                synthetic_current["open_time_ms"] + 30_000
            ),
            vwap_lookback_bars=vwap_lookback_bars,
            atr_period=atr_period,
            vwap_slope_lookback_bars=1,
            upper_atr_fraction=upper_atr_fraction,
            lower_atr_fraction=lower_atr_fraction,
            armed_max_bars=armed_max_bars,
            confirmation_max_bars=confirmation_max_bars,
            confirmation_close_location_min=(
                confirmation_close_location_min
            ),
            confirmation_taker_buy_ratio_min=(
                confirmation_taker_buy_ratio_min
            ),
        )
        matches = [
            event
            for event in replay.stage_events
            if event.t_time == value["t_time"]
            and event.status == "INVALID"
            and event.reason == value["reason"]
        ]
        return (
            len(matches) == 1
            and _n13_exact_json_value(
                _n13_state_detail_without_indexes(matches[0].detail),
                value,
            )
        )
    except (
        ArithmeticError,
        InvalidOperation,
        KeyError,
        TypeError,
        ValueError,
    ):
        return False


def _n13_t_stage_detail_is_valid(
    reason: str,
    value: Any,
    symbol: str,
    t_time: str,
    armed_max_bars: int,
    vwap_lookback_bars: int,
    atr_period: int,
    upper_atr_fraction: Decimal,
    lower_atr_fraction: Decimal,
    confirmation_max_bars: int,
    confirmation_close_location_min: Decimal,
    confirmation_taker_buy_ratio_min: Decimal,
) -> bool:
    parsed_t_time = _n13_canonical_t_time(t_time)
    if (
        reason not in _N13_T_STAGE_REASONS
        or type(value) is not dict
        or value.keys() != _N13_T_STAGE_DETAIL_KEYS
        or type(symbol) is not str
        or not symbol
        or parsed_t_time is None
        or type(armed_max_bars) is not int
        or armed_max_bars <= 0
        or type(vwap_lookback_bars) is not int
        or vwap_lookback_bars <= 0
        or type(atr_period) is not int
        or atr_period <= 0
        or type(upper_atr_fraction) is not Decimal
        or not upper_atr_fraction.is_finite()
        or upper_atr_fraction < 0
        or type(lower_atr_fraction) is not Decimal
        or not lower_atr_fraction.is_finite()
        or lower_atr_fraction < 0
        or type(confirmation_max_bars) is not int
        or confirmation_max_bars <= 0
        or type(confirmation_close_location_min) is not Decimal
        or not confirmation_close_location_min.is_finite()
        or not Decimal("0")
        <= confirmation_close_location_min
        <= Decimal("1")
        or type(confirmation_taker_buy_ratio_min) is not Decimal
        or not confirmation_taker_buy_ratio_min.is_finite()
        or not Decimal("0")
        <= confirmation_taker_buy_ratio_min
        <= Decimal("1")
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 2
        or type(value["symbol"]) is not str
        or value["symbol"] != symbol
        or type(value["t_time"]) is not str
        or value["t_time"] != t_time
        or type(value["reason"]) is not str
        or value["reason"] != reason
        or type(value["armed_max_bars"]) is not int
        or value["armed_max_bars"] != armed_max_bars
        or type(value["vwap_lookback_bars"]) is not int
        or value["vwap_lookback_bars"] != vwap_lookback_bars
        or type(value["atr_period"]) is not int
        or value["atr_period"] != atr_period
        or type(value["upper_atr_fraction"]) is not str
        or value["upper_atr_fraction"] != str(upper_atr_fraction)
        or type(value["lower_atr_fraction"]) is not str
        or value["lower_atr_fraction"] != str(lower_atr_fraction)
        or type(value["confirmation_max_bars"]) is not int
        or value["confirmation_max_bars"] != confirmation_max_bars
        or type(value["confirmation_close_location_min"]) is not str
        or value["confirmation_close_location_min"]
        != str(confirmation_close_location_min)
        or type(value["confirmation_taker_buy_ratio_min"]) is not str
        or value["confirmation_taker_buy_ratio_min"]
        != str(confirmation_taker_buy_ratio_min)
        or type(value["canonical_sha256"]) is not str
        or len(value["canonical_sha256"]) != 64
        or not _n13_candle_state_is_valid(value["a"])
        or not _n13_candle_state_is_valid(value["t"])
        or value["t"]["open_time_ms"] != parsed_t_time
        or not _n13_armed_window_is_valid(
            value["a"],
            value["t"],
            value["armed_expiry_time_ms"],
            armed_max_bars,
        )
        or not _n13_finite_decimal_text(value["vwap_a"])
        or not _n13_finite_decimal_text(value["atr_a"])
        or not _n13_finite_decimal_text(value["zone_lower"])
        or not _n13_finite_decimal_text(value["zone_upper"])
        or not _n13_reason_list_is_valid(
            value["failed_confirmation_reasons"]
        )
        or type(value["pre_touch_bars"]) is not list
        or type(value["confirmation_checks"]) is not list
        or type(value["metric_context_bars"]) is not list
    ):
        return False
    unsigned = {
        key: value[key]
        for key in value
        if key != "canonical_sha256"
    }
    canonical = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    if value["canonical_sha256"] != hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest():
        return False

    context = value["metric_context_bars"]
    if (
        len(context) < max(vwap_lookback_bars, atr_period)
        or not all(_n13_candle_state_is_valid(item) for item in context)
        or any(
            right["open_time_ms"] - left["open_time_ms"]
            != _N13_INTERVAL_MS
            for left, right in zip(context, context[1:])
        )
    ):
        return False
    try:
        metric_candles = [
            N13Candle(
                index,
                item["open_time_ms"],
                Decimal(item["open"]),
                Decimal(item["high"]),
                Decimal(item["low"]),
                Decimal(item["close"]),
                Decimal(item["base_volume"]),
                Decimal(item["quote_volume"]),
                Decimal(item["taker_buy_quote_volume"]),
            )
            for index, item in enumerate(context)
        ]
        recomputed_metrics = n13_metrics(
            metric_candles,
            vwap_lookback_bars=vwap_lookback_bars,
            atr_period=atr_period,
            upper_atr_fraction=upper_atr_fraction,
            lower_atr_fraction=lower_atr_fraction,
        )
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return False
    context_index_by_time = {
        item["open_time_ms"]: index
        for index, item in enumerate(context)
    }
    if len(context_index_by_time) != len(context):
        return False

    a = value["a"]
    t = value["t"]
    a_index = context_index_by_time.get(a["open_time_ms"])
    t_index = context_index_by_time.get(t["open_time_ms"])
    if (
        a_index is None
        or t_index is None
        or not _n13_exact_json_value(context[a_index], a)
        or not _n13_exact_json_value(context[t_index], t)
    ):
        return False
    a_metric = recomputed_metrics.get(a_index)
    if (
        a_metric is None
        or value["vwap_a"] != str(a_metric.vwap)
        or value["atr_a"] != str(a_metric.atr)
        or value["zone_lower"] != str(a_metric.lower)
        or value["zone_upper"] != str(a_metric.upper)
    ):
        return False
    zone_lower = a_metric.lower
    zone_upper = a_metric.upper
    if (
        a_metric.vwap <= 0
        or a_metric.atr <= 0
        or zone_lower > zone_upper
        or zone_upper <= 0
        or Decimal(a["low"]) <= zone_upper
        or Decimal(t["low"]) > zone_upper
        or Decimal(t["high"]) < zone_lower
    ):
        return False

    a_time = a["open_time_ms"]
    expected_pre_touch_count = (
        (parsed_t_time - a_time) // _N13_INTERVAL_MS - 1
    )
    pre_touch = value["pre_touch_bars"]
    if (
        expected_pre_touch_count < 0
        or len(pre_touch) != expected_pre_touch_count
        or not all(_n13_candle_state_is_valid(item) for item in pre_touch)
        or not _n13_exact_json_value(
            pre_touch,
            context[a_index + 1:t_index],
        )
    ):
        return False
    previous = a
    for item in pre_touch:
        if (
            item["open_time_ms"]
            != previous["open_time_ms"] + _N13_INTERVAL_MS
            or Decimal(item["low"]) <= zone_upper
        ):
            return False
        previous = item
    if previous["open_time_ms"] + _N13_INTERVAL_MS != parsed_t_time:
        return False

    checks = value["confirmation_checks"]
    if not 1 <= len(checks) <= confirmation_max_bars:
        return False
    outcomes: list[str] = []
    previous = pre_touch[-1] if pre_touch else a
    for offset, check in enumerate(checks):
        if (
            type(check) is not dict
            or check.keys() != _N13_CONFIRMATION_CHECK_KEYS
            or not _n13_candle_state_is_valid(check["candle"])
            or check["candle"]["open_time_ms"]
            != parsed_t_time + offset * _N13_INTERVAL_MS
            or previous["open_time_ms"] + _N13_INTERVAL_MS
            != check["candle"]["open_time_ms"]
            or not _n13_finite_decimal_text(check["vwap"])
            or Decimal(check["vwap"]) <= 0
            or type(check["outcome"]) is not str
        ):
            return False
        check_index = t_index + offset
        check_metric = recomputed_metrics.get(check_index)
        if (
            check_index >= len(context)
            or check_metric is None
            or not _n13_exact_json_value(
                check["candle"], context[check_index]
            )
            or check["vwap"] != str(check_metric.vwap)
            or (
                offset == 0
                and not _n13_exact_json_value(check["candle"], t)
            )
        ):
            return False
        outcome = _n13_confirmation_outcome(
            check["candle"],
            previous,
            Decimal(check["vwap"]),
            zone_lower,
            confirmation_close_location_min,
            confirmation_taker_buy_ratio_min,
        )
        if outcome is None or check["outcome"] != outcome:
            return False
        outcomes.append(outcome)
        previous = check["candle"]

    if not _n13_exact_json_value(
        context[-1], checks[-1]["candle"]
    ):
        return False

    failed = value["failed_confirmation_reasons"]
    if reason == "N13_CONFIRMATION_NOT_FOUND":
        terminal_valid = (
            len(checks) == confirmation_max_bars
            and all(
                outcome in _N13_CONFIRMATION_FAILURE_REASONS
                for outcome in outcomes
            )
            and failed == outcomes
        )
    else:
        terminal_valid = (
            outcomes[-1] == "N13_VALUE_BAND_BROKEN"
            and all(
                outcome in _N13_CONFIRMATION_FAILURE_REASONS
                for outcome in outcomes[:-1]
            )
            and failed == outcomes[:-1]
        )
    return terminal_valid and _n13_t_stage_replays_from_metric_context(
        value,
        symbol,
        vwap_lookback_bars,
        atr_period,
        upper_atr_fraction,
        lower_atr_fraction,
        armed_max_bars,
        confirmation_max_bars,
        confirmation_close_location_min,
        confirmation_taker_buy_ratio_min,
    )


def _n13_stage_state_detail_is_valid(
    reason: str,
    value: Any,
    symbol: str,
    t_time: str,
    armed_max_bars: int,
    vwap_lookback_bars: int,
    atr_period: int,
    upper_atr_fraction: Decimal,
    lower_atr_fraction: Decimal,
    confirmation_max_bars: int,
    confirmation_close_location_min: Decimal,
    confirmation_taker_buy_ratio_min: Decimal,
    *,
    allow_legacy_t_stage: bool,
) -> bool:
    if reason in _N13_T_STAGE_REASONS:
        if type(value) is dict and value.keys() == _N13_T_STAGE_DETAIL_KEYS:
            return _n13_t_stage_detail_is_valid(
                reason,
                value,
                symbol,
                t_time,
                armed_max_bars,
                vwap_lookback_bars,
                atr_period,
                upper_atr_fraction,
                lower_atr_fraction,
                confirmation_max_bars,
                confirmation_close_location_min,
                confirmation_taker_buy_ratio_min,
            )
        return allow_legacy_t_stage and _n13_legacy_t_stage_detail_is_valid(
            reason,
            value,
            confirmation_max_bars,
        )

    allowed_shapes = _N13_STAGE_STATE_DETAIL_KEYS.get(reason)
    if type(value) is not dict or allowed_shapes is None:
        return False
    if value.keys() not in allowed_shapes:
        return False
    if not _n13_candle_state_is_valid(value["a"]):
        return False
    if "armed_expiry_time_ms" in value and (
        type(value["armed_expiry_time_ms"]) is not int
        or value["armed_expiry_time_ms"] <= 0
        or value["armed_expiry_time_ms"]
        != value["a"]["open_time_ms"]
        + armed_max_bars * _N13_INTERVAL_MS
    ):
        return False
    if "zone_lower" in value and not all(
        _n13_finite_decimal_text(value[key])
        for key in ("zone_lower", "zone_upper")
    ):
        return False
    if "zone_lower" in value:
        zone_lower = Decimal(value["zone_lower"])
        zone_upper = Decimal(value["zone_upper"])
        if (
            zone_lower > zone_upper
            or zone_upper <= 0
            or Decimal(value["a"]["low"]) <= zone_upper
        ):
            return False
    return True


def _n13_event_state_detail_is_valid(
    value: Any,
    strategy_id: Any,
    symbol: Any,
    status: str,
    reason: str,
    structure_id: str | None,
    t_time: Any,
    armed_max_bars: int,
    vwap_lookback_bars: int,
    atr_period: int,
    upper_atr_fraction: Decimal,
    lower_atr_fraction: Decimal,
    confirmation_max_bars: int,
    confirmation_close_location_min: Decimal,
    confirmation_taker_buy_ratio_min: Decimal,
    entry_extension_atr_max: Decimal,
    *,
    allow_legacy_t_stage: bool,
) -> bool:
    parsed_t_time = _n13_canonical_t_time(t_time)
    if (
        type(strategy_id) is not str
        or not strategy_id
        or type(symbol) is not str
        or not symbol
        or type(status) is not str
        or not status
        or type(reason) is not str
        or not reason
        or parsed_t_time is None
        or type(value) is not dict
        or value.keys() != {"event", "structure", "detail"}
        or type(value["event"]) is not str
        or value["event"] != reason
    ):
        return False
    if reason == "HISTORICAL_N13_ENTRY_MISSED":
        return (
            status == "MISSED"
            and type(structure_id) is str
            and bool(structure_id)
            and value["detail"] is None
            and _n13_historical_structure_state_is_valid(
                value["structure"],
                symbol,
                t_time,
                structure_id,
                armed_max_bars,
                entry_extension_atr_max,
            )
        )
    valid_stage = (
        status == "INVALID"
        and structure_id is None
        and value["structure"] is None
        and _n13_stage_state_detail_is_valid(
            reason,
            value["detail"],
            symbol,
            t_time,
            armed_max_bars,
            vwap_lookback_bars,
            atr_period,
            upper_atr_fraction,
            lower_atr_fraction,
            confirmation_max_bars,
            confirmation_close_location_min,
            confirmation_taker_buy_ratio_min,
            allow_legacy_t_stage=allow_legacy_t_stage,
        )
    )
    if not valid_stage:
        return False
    anchor_key = (
        "a"
        if reason in {
            "N13_ARMED_WINDOW_EXPIRED",
            "N13_VALUE_ZONE_SKIPPED",
        }
        else "t"
    )
    return value["detail"][anchor_key]["open_time_ms"] == parsed_t_time


def _n13_runtime_candle_is_valid(value: Any) -> bool:
    return (
        type(value) is N13Candle
        and type(value.index) is int
        and value.index >= 0
        and type(value.open_time_ms) is int
        and value.open_time_ms > 0
        and value.open_time_ms % _N13_INTERVAL_MS == 0
        and all(
            type(getattr(value, field)) is Decimal
            and getattr(value, field).is_finite()
            for field in (
                "open",
                "high",
                "low",
                "close",
                "base_volume",
                "quote_volume",
                "taker_buy_quote_volume",
            )
        )
        and _n13_kline_values_are_valid(
            value.open,
            value.high,
            value.low,
            value.close,
            value.base_volume,
            value.quote_volume,
            value.taker_buy_quote_volume,
        )
    )


def _n13_runtime_structure_is_valid(
    value: Any,
    symbol: Any,
    armed_max_bars: int,
    entry_extension_atr_max: Decimal,
) -> bool:
    if (
        type(value) is not N13Structure
        or type(value.symbol) is not str
        or value.symbol != symbol
        or not all(
            _n13_runtime_candle_is_valid(candle)
            for candle in (value.a, value.t, value.c)
        )
        or (
            value.entry is not None
            and not _n13_runtime_candle_is_valid(value.entry)
        )
        or type(value.structure_id) is not str
        or not value.structure_id
        or not all(
            type(getattr(value, field)) is Decimal
            and getattr(value, field).is_finite()
            for field in (
                "p",
                "vwap_c",
                "atr_c",
                "entry_min_price",
                "entry_max_price",
                "zone_lower",
                "zone_upper",
            )
        )
        or any(
            item is not None
            and (type(item) is not Decimal or not item.is_finite())
            for item in (
                value.return_24h,
                value.positive_return_breadth,
                value.above_vwap_breadth,
            )
        )
        or (
            value.return_rank is not None
            and type(value.return_rank) is not int
        )
        or type(value.snapshot_context_available) is not bool
        or type(value.armed_expiry_time_ms) is not int
        or value.armed_expiry_time_ms <= 0
        or type(value.failed_confirmation_reasons) is not tuple
        or not all(
            type(reason) is str and bool(reason)
            for reason in value.failed_confirmation_reasons
        )
    ):
        return False
    a = _n13_state_detail_without_indexes(value.a.json())
    t = _n13_state_detail_without_indexes(value.t.json())
    c = _n13_state_detail_without_indexes(value.c.json())
    pricing_evidence = _n13_state_detail_without_indexes(value.json())
    return (
        _n13_structure_identity_is_valid(
            symbol,
            str(value.t.open_time_ms),
            value.structure_id,
            a,
            t,
            c,
        )
        and _n13_armed_window_is_valid(
            a,
            t,
            value.armed_expiry_time_ms,
            armed_max_bars,
        )
        and _n13_pricing_evidence_is_valid(
            pricing_evidence,
            entry_extension_atr_max,
            require_extension_fields=True,
        )
        and _n13_structure_zone_evidence_is_valid(
            pricing_evidence,
            require_zone_fields=True,
        )
    )


def _n13_runtime_event_is_valid(
    strategy_id: Any,
    symbol: Any,
    event: Any,
    armed_max_bars: int,
    vwap_lookback_bars: int,
    atr_period: int,
    upper_atr_fraction: Decimal,
    lower_atr_fraction: Decimal,
    confirmation_max_bars: int,
    confirmation_close_location_min: Decimal,
    confirmation_taker_buy_ratio_min: Decimal,
    entry_extension_atr_max: Decimal,
) -> bool:
    if type(event) is not N13Event:
        return False
    structure = event.structure
    if structure is not None and (
        not _n13_runtime_structure_is_valid(
            structure,
            symbol,
            armed_max_bars,
            entry_extension_atr_max,
        )
    ):
        return False
    if event.detail is not None and type(event.detail) is not dict:
        return False
    detail = _n13_state_detail_without_indexes(
        {
            "event": event.reason,
            "structure": structure.json() if structure is not None else None,
            "detail": event.detail,
        }
    )
    return _n13_event_state_detail_is_valid(
        detail,
        strategy_id,
        symbol,
        event.status,
        event.reason,
        structure.structure_id if structure is not None else None,
        event.t_time,
        armed_max_bars,
        vwap_lookback_bars,
        atr_period,
        upper_atr_fraction,
        lower_atr_fraction,
        confirmation_max_bars,
        confirmation_close_location_min,
        confirmation_taker_buy_ratio_min,
        entry_extension_atr_max,
        allow_legacy_t_stage=False,
    )


def _n13_analysis_state_contract_is_valid(
    strategy_id: Any,
    symbol: Any,
    analysis: Any,
    armed_max_bars: int,
    vwap_lookback_bars: int,
    atr_period: int,
    upper_atr_fraction: Decimal,
    lower_atr_fraction: Decimal,
    confirmation_max_bars: int,
    confirmation_close_location_min: Decimal,
    confirmation_taker_buy_ratio_min: Decimal,
    entry_extension_atr_max: Decimal,
) -> bool:
    """Reject analyzer-contract breaches before reading or writing N13 state."""
    if (
        type(strategy_id) is not str
        or not strategy_id
        or type(symbol) is not str
        or not symbol
        or type(analysis) is not N13AnalysisResult
        or type(armed_max_bars) is not int
        or armed_max_bars <= 0
        or type(vwap_lookback_bars) is not int
        or vwap_lookback_bars <= 0
        or type(atr_period) is not int
        or atr_period <= 0
        or type(upper_atr_fraction) is not Decimal
        or not upper_atr_fraction.is_finite()
        or upper_atr_fraction < 0
        or type(lower_atr_fraction) is not Decimal
        or not lower_atr_fraction.is_finite()
        or lower_atr_fraction < 0
        or type(confirmation_max_bars) is not int
        or confirmation_max_bars <= 0
        or type(confirmation_close_location_min) is not Decimal
        or not confirmation_close_location_min.is_finite()
        or not Decimal("0")
        <= confirmation_close_location_min
        <= Decimal("1")
        or type(confirmation_taker_buy_ratio_min) is not Decimal
        or not confirmation_taker_buy_ratio_min.is_finite()
        or not Decimal("0")
        <= confirmation_taker_buy_ratio_min
        <= Decimal("1")
        or type(entry_extension_atr_max) is not Decimal
        or not entry_extension_atr_max.is_finite()
        or entry_extension_atr_max < 0
        or type(analysis.symbol) is not str
        or analysis.symbol != symbol
        or type(analysis.passed) is not bool
        or type(analysis.consume_current) is not bool
        or type(analysis.reason) is not str
        or not analysis.reason
        or type(analysis.stage_events) is not tuple
        or type(analysis.historical_events) is not tuple
        or not all(
            event.structure is None
            and event.reason != "HISTORICAL_N13_ENTRY_MISSED"
            for event in analysis.stage_events
        )
        or not all(
            event.structure is not None
            and event.reason == "HISTORICAL_N13_ENTRY_MISSED"
            for event in analysis.historical_events
        )
        or not all(
            _n13_runtime_event_is_valid(
                strategy_id,
                symbol,
                event,
                armed_max_bars,
                vwap_lookback_bars,
                atr_period,
                upper_atr_fraction,
                lower_atr_fraction,
                confirmation_max_bars,
                confirmation_close_location_min,
                confirmation_taker_buy_ratio_min,
                entry_extension_atr_max,
            )
            for event in (*analysis.stage_events, *analysis.historical_events)
        )
        or (analysis.reason == "PASSED") != analysis.passed
    ):
        return False
    current_status = "CONSUMED" if analysis.passed else "MISSED"
    terminal_pair = (current_status, analysis.reason) in (
        _N13_CURRENT_TERMINAL_STATE_PAIRS
    )
    waiting = (
        not analysis.passed
        and analysis.reason == "N13_WAITING_ENTRY_PRICE"
    )
    structure = analysis.structure
    if structure is None:
        return (
            not terminal_pair
            and not waiting
            and not analysis.consume_current
        )
    if not _n13_runtime_structure_is_valid(
        structure,
        symbol,
        armed_max_bars,
        entry_extension_atr_max,
    ):
        return False
    if structure.entry is None:
        return (
            not terminal_pair
            and not waiting
            and not analysis.consume_current
        )
    entry = _n13_state_detail_without_indexes(structure.entry.json())
    c = _n13_state_detail_without_indexes(structure.c.json())
    if (
        not _n13_candle_state_is_valid(entry)
        or entry["open_time_ms"] != c["open_time_ms"] + _N13_INTERVAL_MS
    ):
        return False
    if terminal_pair:
        return analysis.consume_current
    if waiting:
        return not analysis.consume_current
    return False


def _n13_t_stage_immutable_candles(
    detail: dict[str, Any],
) -> dict[int, dict[str, Any]] | None:
    candles: dict[int, dict[str, Any]] = {}
    values = [
        *detail["metric_context_bars"],
        detail["a"],
        *detail["pre_touch_bars"],
    ]
    values.extend(
        check["candle"] for check in detail["confirmation_checks"]
    )
    for candle in values:
        open_time_ms = candle["open_time_ms"]
        existing = candles.get(open_time_ms)
        if existing is not None and not _n13_exact_json_value(
            existing, candle
        ):
            return None
        candles[open_time_ms] = candle
    return candles


def _n13_t_stage_collision_is_compatible(
    actual: dict[str, Any],
    expected: dict[str, Any],
) -> bool:
    actual_detail = actual["detail"]
    expected_detail = expected["detail"]
    if (
        expected_detail.keys() != _N13_T_STAGE_DETAIL_KEYS
        or not _n13_exact_json_value(
            actual_detail["t"], expected_detail["t"]
        )
    ):
        return False
    if actual_detail.keys() == _N13_LEGACY_T_STAGE_DETAIL_KEYS:
        return True
    if actual_detail.keys() != _N13_T_STAGE_DETAIL_KEYS:
        return False
    for key in (
        "symbol",
        "t_time",
        "armed_max_bars",
        "vwap_lookback_bars",
        "atr_period",
        "upper_atr_fraction",
        "lower_atr_fraction",
        "confirmation_max_bars",
        "confirmation_close_location_min",
        "confirmation_taker_buy_ratio_min",
    ):
        if actual_detail[key] != expected_detail[key]:
            return False
    actual_candles = _n13_t_stage_immutable_candles(actual_detail)
    expected_candles = _n13_t_stage_immutable_candles(expected_detail)
    if actual_candles is None or expected_candles is None:
        return False
    for open_time_ms in actual_candles.keys() & expected_candles.keys():
        if not _n13_exact_json_value(
            actual_candles[open_time_ms],
            expected_candles[open_time_ms],
        ):
            return False
    return True


def _n13_existing_event_state_match(
    row: Any,
    strategy_id: str,
    symbol: str,
    event: Any,
    armed_max_bars: int,
    vwap_lookback_bars: int,
    atr_period: int,
    upper_atr_fraction: Decimal,
    lower_atr_fraction: Decimal,
    confirmation_max_bars: int,
    confirmation_close_location_min: Decimal,
    confirmation_taker_buy_ratio_min: Decimal,
    entry_extension_atr_max: Decimal,
) -> str:
    event_structure_id = (
        event.structure.structure_id if event.structure is not None else None
    )
    if (
        row is None
        or row[1] != strategy_id
        or row[2] != symbol
        or row[3] != event.t_time
        or row[4] != event_structure_id
        or row[5] != event.status
    ):
        return _N13_EVENT_MATCH_CONFLICT
    try:
        actual = _n13_strict_json_loads(row[7])
    except (TypeError, ValueError):
        return _N13_EVENT_MATCH_CONFLICT
    if not _n13_event_state_detail_is_valid(
        actual,
        strategy_id,
        symbol,
        row[5],
        row[6],
        event_structure_id,
        event.t_time,
        armed_max_bars,
        vwap_lookback_bars,
        atr_period,
        upper_atr_fraction,
        lower_atr_fraction,
        confirmation_max_bars,
        confirmation_close_location_min,
        confirmation_taker_buy_ratio_min,
        entry_extension_atr_max,
        allow_legacy_t_stage=True,
    ):
        return _N13_EVENT_MATCH_CONFLICT
    expected = _n13_state_detail_without_indexes(
        {
            "event": event.reason,
            "structure": (
                event.structure.json()
                if event.structure is not None
                else None
            ),
            "detail": event.detail,
        }
    )
    if not _n13_event_state_detail_is_valid(
        expected,
        strategy_id,
        symbol,
        event.status,
        event.reason,
        event_structure_id,
        event.t_time,
        armed_max_bars,
        vwap_lookback_bars,
        atr_period,
        upper_atr_fraction,
        lower_atr_fraction,
        confirmation_max_bars,
        confirmation_close_location_min,
        confirmation_taker_buy_ratio_min,
        entry_extension_atr_max,
        allow_legacy_t_stage=False,
    ):
        return _N13_EVENT_MATCH_CONFLICT
    if row[6] == event.reason and _n13_exact_json_value(actual, expected):
        return _N13_EVENT_MATCH_EXACT
    if (
        row[6] in _N13_T_STAGE_REASONS
        and event.reason in _N13_T_STAGE_REASONS
        and event_structure_id is None
        and _n13_t_stage_collision_is_compatible(actual, expected)
    ):
        return _N13_EVENT_MATCH_ISOLATED
    if row[6] != event.reason:
        return _N13_EVENT_MATCH_CONFLICT
    if event.reason == "HISTORICAL_N13_ENTRY_MISSED":
        return (
            _N13_EVENT_MATCH_EXACT
            if _n13_historical_terminal_evidence_matches(
            actual["structure"],
            expected["structure"],
            armed_max_bars,
            entry_extension_atr_max,
            )
            else _N13_EVENT_MATCH_CONFLICT
        )
    return (
        _N13_EVENT_MATCH_EXACT
        if _n13_exact_json_value(
            _stable_n13_state_detail(actual),
            _stable_n13_state_detail(expected),
        )
        else _N13_EVENT_MATCH_CONFLICT
    )


def _deduplicate_n13_state_records(
    records: list[dict[str, Any]],
    armed_max_bars: int,
    vwap_lookback_bars: int,
    atr_period: int,
    upper_atr_fraction: Decimal,
    lower_atr_fraction: Decimal,
    confirmation_max_bars: int,
    confirmation_close_location_min: Decimal,
    confirmation_taker_buy_ratio_min: Decimal,
    entry_extension_atr_max: Decimal,
) -> list[dict[str, Any]] | None:
    """Fold one batch by stable event identity without discarding its first audit."""
    unique: list[dict[str, Any]] = []
    by_key: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    for record in records:
        key = (
            record["strategy_id"],
            record["symbol"],
            record["t_time"],
        )
        detail = record["detail"]
        is_event = (
            type(detail) is dict
            and detail.keys() == {"event", "structure", "detail"}
        )
        if is_event:
            if not _n13_event_state_detail_is_valid(
                detail,
                record["strategy_id"],
                record["symbol"],
                record["status"],
                record["reason"],
                record.get("structure_id"),
                record["t_time"],
                armed_max_bars,
                vwap_lookback_bars,
                atr_period,
                upper_atr_fraction,
                lower_atr_fraction,
                confirmation_max_bars,
                confirmation_close_location_min,
                confirmation_taker_buy_ratio_min,
                entry_extension_atr_max,
                allow_legacy_t_stage=False,
            ):
                return None
        else:
            structure_id = record.get("structure_id")
            if (
                type(structure_id) is not str
                or not structure_id
                or (record["status"], record["reason"])
                not in _N13_CURRENT_TERMINAL_STATE_PAIRS
                or not _n13_current_terminal_detail_is_valid(
                    detail,
                    record["strategy_id"],
                    record["symbol"],
                    record["t_time"],
                    structure_id,
                    record["status"],
                    record["reason"],
                    armed_max_bars,
                    entry_extension_atr_max,
                )
            ):
                return None
        if key in by_key:
            if not _n13_validated_state_records_match(
                by_key[key],
                record,
                armed_max_bars,
                entry_extension_atr_max,
            ):
                return None
            if record.get("require_new"):
                by_key[key]["require_new"] = True
            continue
        normalized = dict(record)
        by_key[key] = normalized
        unique.append(normalized)
    return unique


def _n13_validated_state_records_match(
    actual: dict[str, Any],
    expected: dict[str, Any],
    armed_max_bars: int,
    entry_extension_atr_max: Decimal,
) -> bool:
    if any(
        actual.get(key) != expected.get(key)
        for key in ("structure_id", "status", "reason")
    ):
        return False
    actual_detail = actual["detail"]
    expected_detail = expected["detail"]
    actual_is_event = (
        type(actual_detail) is dict
        and actual_detail.keys() == {"event", "structure", "detail"}
    )
    expected_is_event = (
        type(expected_detail) is dict
        and expected_detail.keys() == {"event", "structure", "detail"}
    )
    if actual_is_event != expected_is_event:
        return False
    if actual_is_event:
        if (
            actual_detail["event"] == "HISTORICAL_N13_ENTRY_MISSED"
            and expected_detail["event"]
            == "HISTORICAL_N13_ENTRY_MISSED"
        ):
            return _n13_historical_terminal_evidence_matches(
                actual_detail["structure"],
                expected_detail["structure"],
                armed_max_bars,
                entry_extension_atr_max,
            )
        return _n13_exact_json_value(
            _stable_n13_state_detail(actual_detail),
            _stable_n13_state_detail(expected_detail),
        )
    return _n13_exact_json_value(actual_detail, expected_detail)


def _n13_current_terminal_evidence(structure: Any) -> dict[str, Any]:
    """Return frozen episode evidence; the live E candle is deliberately absent."""
    return {
        "structure_id": structure.structure_id,
        "a": _n13_state_detail_without_indexes(structure.a.json()),
        "t": _n13_state_detail_without_indexes(structure.t.json()),
        "c": _n13_state_detail_without_indexes(structure.c.json()),
        "p": str(structure.p),
        "vwap_c": str(structure.vwap_c),
        "atr_c": str(structure.atr_c),
        "entry_min_price": str(structure.entry_min_price),
        "entry_max_price": str(structure.entry_max_price),
        "zone_lower": str(structure.zone_lower),
        "zone_upper": str(structure.zone_upper),
        "armed_expiry_time_ms": structure.armed_expiry_time_ms,
        "failed_confirmation_reasons": list(
            structure.failed_confirmation_reasons
        ),
    }


def _n13_current_terminal_envelope(
    strategy_id: str,
    symbol: str,
    t_time: str,
    structure: Any,
    status: str,
    reason: str,
) -> dict[str, Any]:
    unsigned = {
        "schema_version": 1,
        "strategy_id": strategy_id,
        "symbol": symbol,
        "t_time": t_time,
        "structure_id": structure.structure_id,
        "status": status,
        "reason": reason,
        "evidence": _n13_current_terminal_evidence(structure),
    }
    canonical = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        **unsigned,
        "canonical_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    }


def _stable_n14_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _stable_n14_value(item)
            for key, item in value.items()
            if key != "index"
        }
    if isinstance(value, (list, tuple)):
        return [_stable_n14_value(item) for item in value]
    return value


def _n14_stable_market_scenario(value: Any) -> Any:
    stable = _stable_n14_value(value)
    if isinstance(stable, dict):
        stable = dict(stable)
        stable.pop("bullish_breadth_c", None)
    return stable


def _n14_terminal_evidence(
    structure: N14Structure | None,
    event_detail: dict[str, Any] | None,
) -> dict[str, Any]:
    if structure is not None:
        scenario = structure.market_scenario
        bullish_breadth_c = (
            scenario.bullish_breadth_c if scenario is not None else None
        )
        if scenario is None:
            cascade_gate = None
        elif bullish_breadth_c >= Decimal("0.40"):
            cascade_gate = "BREADTH_MIN"
        elif (
            bullish_breadth_c - scenario.bullish_breadth_s
            >= Decimal("0.15")
        ):
            cascade_gate = "IMPROVEMENT"
        else:
            cascade_gate = "FAILED"
        return {
            "locked_stage": "C_CONFIRMED",
            "s": _stable_n14_value(structure.s.json()),
            "a": _stable_n14_value(structure.a.json()),
            "c": _stable_n14_value(structure.c.json()),
            "p": str(structure.p),
            "atr_s_reference": str(structure.atr_s_reference),
            "volume_median_s": str(structure.volume_median_s),
            "market_scenario": _n14_stable_market_scenario(
                structure.market_scenario.json()
                if structure.market_scenario
                else None
            ),
            "structure_id": structure.structure_id,
            "entry_min_price": str(structure.entry_min_price),
            "entry_max_price": str(structure.entry_max_price),
            "failed_confirmation_reasons": list(
                structure.failed_confirmation_reasons
            ),
            "bullish_breadth_c": str(bullish_breadth_c)
            if bullish_breadth_c is not None
            else None,
            "cascade_gate": cascade_gate,
        }
    detail = _stable_n14_value(event_detail or {})
    shock = detail.get("shock") if isinstance(detail, dict) else None
    locked_stage = "S_LOCKED"
    if isinstance(detail, dict) and detail.get("a") is not None:
        locked_stage = (
            "A_CONFIRMED" if detail.get("p") is not None else "S_LOCKED"
        )
    return {
        "locked_stage": locked_stage,
        "s": detail.get("s") if isinstance(detail, dict) else None,
        "a": detail.get("a") if isinstance(detail, dict) else None,
        "c": None,
        "p": detail.get("p") if isinstance(detail, dict) else None,
        "atr_s_reference": shock.get("atr_reference")
        if isinstance(shock, dict)
        else None,
        "volume_median_s": shock.get("volume_median")
        if isinstance(shock, dict)
        else None,
        "market_scenario": _n14_stable_market_scenario(
            detail.get("market_scenario") if isinstance(detail, dict) else None
        ),
        "structure_id": None,
        "entry_min_price": None,
        "entry_max_price": None,
        "failed_confirmation_reasons": list(
            detail.get("failed_confirmation_reasons", [])
            if isinstance(detail, dict)
            else []
        ),
        "bullish_breadth_c": None,
        "cascade_gate": None,
    }


def _n14_active_envelope(
    strategy_id: str,
    symbol: str,
    event: N14Event,
    config_signature: str,
) -> dict[str, Any]:
    evidence = _n14_terminal_evidence(event.structure, event.detail)
    evidence["locked_stage"] = event.status
    unsigned = {
        "schema_version": 1,
        "strategy_id": strategy_id,
        "symbol": symbol,
        "s_time": event.s_time,
        "stage": event.status,
        "config_signature": config_signature,
        "evidence": evidence,
    }
    canonical = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        **unsigned,
        "canonical_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    }


def _n14_terminal_envelope(
    strategy_id: str,
    symbol: str,
    s_time: str,
    structure_id: str | None,
    status: str,
    reason: str,
    config_signature: str,
    structure: N14Structure | None,
    event_detail: dict[str, Any] | None,
) -> dict[str, Any]:
    unsigned = {
        "schema_version": 1,
        "strategy_id": strategy_id,
        "symbol": symbol,
        "s_time": s_time,
        "structure_id": structure_id,
        "status": status,
        "reason": reason,
        "config_signature": config_signature,
        "evidence": _n14_terminal_evidence(structure, event_detail),
    }
    canonical = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        **unsigned,
        "canonical_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    }


def _n14_terminal_from_active(
    active: dict[str, Any],
    status: str,
    reason: str,
) -> dict[str, Any]:
    unsigned = {
        "schema_version": 1,
        "strategy_id": active["strategy_id"],
        "symbol": active["symbol"],
        "s_time": active["s_time"],
        "structure_id": active["evidence"].get("structure_id"),
        "status": status,
        "reason": reason,
        "config_signature": active["config_signature"],
        "evidence": active["evidence"],
    }
    canonical = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        **unsigned,
        "canonical_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    }


def _n14_existing_terminal_matches(row: Any, record: dict[str, Any]) -> bool:
    if (
        row is None
        or row[1] != record["strategy_id"]
        or row[2] != record["symbol"]
        or row[3] != record["s_time"]
        or row[4] != record.get("structure_id")
        or row[5] != record["status"]
        or row[6] != record["reason"]
    ):
        return False
    try:
        stored = json.loads(row[7])
    except (TypeError, ValueError):
        return False
    return _n13_exact_json_value(stored, record["detail"])


def _n14_stored_terminal_is_self_consistent(row: Any) -> bool:
    if row is None:
        return False
    try:
        detail = json.loads(row[7])
        decoded = _decode_n14_terminal_envelope(
            detail,
            row[1],
            row[2],
            row[3],
        )
        return (
            decoded["structure_id"] == row[4]
            and decoded["status"] == row[5]
            and decoded["reason"] == row[6]
        )
    except (KeyError, RuntimeError, TypeError, ValueError):
        return False


@dataclass(frozen=True)
class StrategySignalDecision:
    strategy: Any
    candidate: FundingCandidate
    analysis: StrategyAnalysis | None
    passed: bool
    decision: str
    reason: str
    signal_id: int | None = None


@dataclass(frozen=True)
class LiveTradeCandidate:
    signal: StrategySignalDecision
    state: StrategyState | None = None


@dataclass(frozen=True)
class SchedulerResult:
    signals: list[StrategySignalDecision]
    passed_signals: list[StrategySignalDecision]
    live_candidates: list[LiveTradeCandidate]
    signal_batch_published: bool = False
    signal_audit_failures: tuple["SignalAuditFailure", ...] = ()
    signal_audit_failure_overflow: int = 0


@dataclass(frozen=True, order=True)
class SignalAuditFailure:
    code: str
    strategy_id: str | None = None
    symbol: str | None = None

    def log_payload(self) -> dict[str, str]:
        payload = {"code": self.code}
        if self.strategy_id is not None:
            payload["strategy_id"] = self.strategy_id
        if self.symbol is not None:
            payload["symbol"] = self.symbol
        return payload


@dataclass(frozen=True)
class N08CoverageAssessment:
    coverage: N08HistoryCoverage
    gap_detected: bool


@dataclass(frozen=True)
class N12BatchContext:
    current_by_symbol: dict[str, RelativeStrength]
    historical_by_symbol: dict[str, dict[int, RelativeStrength]]
    historical_complete_times: set[int]
    current_context_complete: bool


@dataclass(frozen=True)
class N13MarketRow:
    return_24h: Decimal
    return_rank: int
    quote_volume_rank: int
    vwap_c: Decimal
    vwap_slope_reference: Decimal
    atr_c: Decimal
    close_above_vwap: bool


@dataclass(frozen=True)
class N13BatchContext:
    rows: dict[str, N13MarketRow]
    positive_breadth: Decimal | None
    above_vwap_breadth: Decimal | None
    complete: bool
    current_open_time_ms: int | None = None


@dataclass(frozen=True)
class N14SnapshotRuntime:
    snapshot: N14Snapshot
    bullish_breadth_c: Decimal | None
    complete: bool


@dataclass(frozen=True)
class N14BatchContext:
    snapshots: dict[int, N14SnapshotRuntime]
    current_open_time_ms: int | None
    complete: bool


@dataclass(frozen=True)
class N15BatchContext:
    snapshot: N15Snapshot | None
    current_open_time_ms: int | None
    complete: bool
    audit_complete: bool = True


def _n15_source_readiness_failure(detail: str) -> bool:
    """Separate authentic unavailable market input from durable-state faults."""

    return any(
        detail.startswith(prefix)
        for prefix in (
            "N15 current kline history incomplete",
            "N15 current E axis incomplete",
            "N15 current E timing invalid",
            "N15 frozen member kline missing",
            "N15 frozen member kline invalid",
            "N15 frozen member E unavailable",
            "N15 legacy source window incomplete",
            "N15 frozen metric source window incomplete",
            "N15 frozen market context unavailable",
            "N15 historical frozen member unavailable",
            "N15 historical market data invalid",
            "N15 historical E unavailable",
            "N15 legacy seed evidence unavailable",
            "N15 historical frozen members incomplete",
            "N15 historical prefix incomplete",
        )
    )


@dataclass(frozen=True)
class N20BatchContext:
    analysis: N20MarketAnalysis | None
    complete: bool
    failure_code: str | None = None
    deferred_reasons: Mapping[str, str] | None = None


class StrategyScheduler:
    def __init__(
        self,
        strategies: tuple[Any, ...],
        trend_window: int,
        recorder: ReviewRecorder,
        logger,
    ):
        self.strategies = tuple(strategy for strategy in strategies if strategy.enabled)
        self.trend_window = trend_window
        self.recorder = recorder
        self.logger = logger
        # This cache only limits repeated diagnostics.  Every replay still
        # passes through the strict matcher and atomic DB guard below.
        self._n13_isolated_warning_fingerprints = OrderedDict()
        self._n13_isolated_warning_suppressed = 0
        self._n13_isolated_warning_suppressed_fingerprints: set[str] = set()
        self._n13_isolated_warning_fingerprint_overflow = False

    def _warn_n13_isolated_collision(
        self,
        existing_event: tuple[Any, ...],
        replay_record: dict[str, Any],
    ) -> None:
        fingerprint = _n13_audit_collision_fingerprint(
            existing_event,
            replay_record,
        )
        if fingerprint in self._n13_isolated_warning_fingerprints:
            self._n13_isolated_warning_fingerprints.move_to_end(fingerprint)
            self._n13_isolated_warning_suppressed += 1
            if (
                len(self._n13_isolated_warning_suppressed_fingerprints)
                < _N13_ISOLATED_WARNING_FINGERPRINT_CACHE_MAX
            ):
                self._n13_isolated_warning_suppressed_fingerprints.add(
                    fingerprint
                )
            elif (
                fingerprint
                not in self._n13_isolated_warning_suppressed_fingerprints
            ):
                self._n13_isolated_warning_fingerprint_overflow = True
            return

        self._n13_isolated_warning_fingerprints[fingerprint] = None
        if (
            len(self._n13_isolated_warning_fingerprints)
            > _N13_ISOLATED_WARNING_FINGERPRINT_CACHE_MAX
        ):
            self._n13_isolated_warning_fingerprints.popitem(last=False)
        self.logger.warning(
            "N13 historical stage audit collision isolated; "
            "preserving immutable first audit | strategy=%s "
            "symbol=%s t_time=%s existing_reason=%s "
            "replay_reason=%s fingerprint=%s",
            replay_record.get("strategy_id"),
            replay_record.get("symbol"),
            replay_record.get("t_time"),
            existing_event[6],
            replay_record.get("reason"),
            fingerprint,
        )

    def _flush_n13_isolated_warning_summary(
        self,
        scan_id: int | None,
    ) -> None:
        suppressed = self._n13_isolated_warning_suppressed
        if suppressed <= 0:
            return
        tracked = len(self._n13_isolated_warning_suppressed_fingerprints)
        unique_fingerprints = (
            f"{tracked}+"
            if self._n13_isolated_warning_fingerprint_overflow
            else str(tracked)
        )
        self._n13_isolated_warning_suppressed = 0
        self._n13_isolated_warning_suppressed_fingerprints.clear()
        self._n13_isolated_warning_fingerprint_overflow = False
        self.logger.warning(
            "N13 historical stage audit collision replay summary; "
            "strict validation still executed for every replay | "
            "scan_id=%s suppressed=%s unique_fingerprints=%s",
            scan_id,
            suppressed,
            unique_fingerprints,
        )

    def evaluate(
        self,
        scan_id: int | None,
        candidates: list[FundingCandidate] | Mapping[str, list[FundingCandidate]],
        raw_klines_by_symbol: dict[str, list[list[Any]]],
        checked_at_ms: int | None = None,
        micro_windows: Mapping[str, MicroObservationWindow] | None = None,
        micro_window_provider: Callable[
            [], tuple[Mapping[str, MicroObservationWindow], int]
        ] | None = None,
        micro_context_complete: bool = True,
        micro_context_failure_code: str | None = None,
        micro_context_failure_symbol: str | None = None,
        authenticated_symbols: frozenset[str] | None = None,
        authenticated_symbols_sha256: str | None = None,
    ) -> SchedulerResult:
        if isinstance(self.recorder, ReviewRecorder):
            with self.recorder.strategy_round_runtime_scope():
                return self._evaluate_round(
                    scan_id,
                    candidates,
                    raw_klines_by_symbol,
                    checked_at_ms=checked_at_ms,
                    micro_windows=micro_windows,
                    micro_window_provider=micro_window_provider,
                    micro_context_complete=micro_context_complete,
                    micro_context_failure_code=micro_context_failure_code,
                    micro_context_failure_symbol=micro_context_failure_symbol,
                    authenticated_symbols=authenticated_symbols,
                    authenticated_symbols_sha256=(
                        authenticated_symbols_sha256
                    ),
                )
        return self._evaluate_round(
            scan_id,
            candidates,
            raw_klines_by_symbol,
            checked_at_ms=checked_at_ms,
            micro_windows=micro_windows,
            micro_window_provider=micro_window_provider,
            micro_context_complete=micro_context_complete,
            micro_context_failure_code=micro_context_failure_code,
            micro_context_failure_symbol=micro_context_failure_symbol,
            authenticated_symbols=authenticated_symbols,
            authenticated_symbols_sha256=authenticated_symbols_sha256,
        )

    def _evaluate_round(
        self,
        scan_id: int | None,
        candidates: list[FundingCandidate] | Mapping[str, list[FundingCandidate]],
        raw_klines_by_symbol: dict[str, list[list[Any]]],
        checked_at_ms: int | None = None,
        micro_windows: Mapping[str, MicroObservationWindow] | None = None,
        micro_window_provider: Callable[
            [], tuple[Mapping[str, MicroObservationWindow], int]
        ] | None = None,
        micro_context_complete: bool = True,
        micro_context_failure_code: str | None = None,
        micro_context_failure_symbol: str | None = None,
        authenticated_symbols: frozenset[str] | None = None,
        authenticated_symbols_sha256: str | None = None,
    ) -> SchedulerResult:
        candidate_groups = self._candidate_groups(candidates)
        current_candidates = tuple(
            candidate
            for group in candidate_groups.values()
            for candidate in group
        )
        try:
            if authenticated_symbols is not None:
                authenticated_registry = attest_authenticated_symbol_set(
                    authenticated_symbols,
                    authenticated_symbols_sha256,
                )
            else:
                authenticated_registry = None
            for candidate in current_candidates:
                symbol = canonical_exchange_symbol(candidate.symbol)
                if (
                    authenticated_registry is not None
                    and symbol not in authenticated_registry
                ):
                    raise ValueError(
                        "candidate is absent from authenticated exchangeInfo"
                    )
        except Exception:
            return SchedulerResult(
                [],
                [],
                [],
                False,
                (SignalAuditFailure("EXCHANGE_SYMBOL_IDENTITY_INVALID"),),
                0,
            )
        signals: list[StrategySignalDecision] = []
        passed_signals: list[StrategySignalDecision] = []
        live_candidates: list[LiveTradeCandidate] = []
        signal_audit_complete = True
        signal_audit_failures: list[SignalAuditFailure] = []
        signal_audit_failure_keys: set[tuple[str, str | None, str | None]] = set()
        signal_audit_failure_overflow = 0
        history_coverage_proposals: list[HistoryCoverageProposal] = []
        n08_history_coverages: list[Any] | None = (
            [] if isinstance(self.recorder, ReviewRecorder) else None
        )
        n08_states_by_symbol: dict[str, list[Any]] | None = (
            {} if isinstance(self.recorder, ReviewRecorder) else None
        )
        n13_pending_records: list[dict[str, Any]] | None = (
            [] if isinstance(self.recorder, ReviewRecorder) else None
        )
        n13_pending_guards: list[tuple[Any, ...]] | None = (
            [] if isinstance(self.recorder, ReviewRecorder) else None
        )
        pending_signal_decisions: list[
            tuple[
                StrategySignalDecision,
                StrategySignalDecision,
                bool,
            ]
        ] = []

        def fail_signal_audit(
            code: str,
            strategy_id: str | None = None,
            symbol: str | None = None,
        ) -> None:
            nonlocal signal_audit_complete, signal_audit_failure_overflow
            signal_audit_complete = False
            key = (code, strategy_id, symbol)
            if key in signal_audit_failure_keys:
                return
            signal_audit_failure_keys.add(key)
            if len(signal_audit_failures) < 64:
                signal_audit_failures.append(
                    SignalAuditFailure(code, strategy_id, symbol)
                )
            else:
                signal_audit_failure_overflow += 1

        global_cooldown_symbols: set[str] = set()
        round_cooldown_symbols = {
            candidate.symbol
            for group in candidate_groups.values()
            for candidate in group
        }
        # The main loop fetches one shared Kline union containing both current
        # candidates and every durable frozen member that may be restored by
        # N13-N20 below.  Candidate expansion is therefore strictly bounded by
        # this union.  Snapshot all of it before any analyzer can expose a
        # dropped member to paper/live selection.
        round_cooldown_symbols.update(raw_klines_by_symbol)
        global_cooldown_scope = frozenset(round_cooldown_symbols)
        global_cooldowns = self.recorder.active_symbol_cooldowns(
            global_cooldown_scope
        )
        strategy_cooldown_requirements = {
            (strategy.strategy_id, symbol): strategy.loss_symbol_cooldown_hours
            for strategy in self.strategies
            if strategy.loss_symbol_cooldown_hours > 0
            for symbol in global_cooldown_scope
        }
        strategy_cooldown_scope = frozenset(
            strategy_cooldown_requirements
        )
        strategy_cooldowns = self.recorder.active_strategy_symbol_cooldowns(
            strategy_cooldown_requirements
        )
        micro_windows = micro_windows or {}
        micro_analyses: dict[tuple[str, str], MicroAnalysisResult] = {}
        micro_suppressed: set[tuple[str, str]] = set()
        micro_analysis_persist_failed: set[tuple[str, str]] = set()
        micro_strategies = [s for s in self.strategies if s.strategy_id in MICRO_STRATEGY_IDS]
        micro_candidates = list(candidate_groups.get("quote_volume_top", []))
        micro_analysis_checked_at_ms = checked_at_ms or 0
        micro_analyses_prepared = False

        def prepare_micro_analyses() -> None:
            nonlocal micro_windows, micro_context_complete
            nonlocal micro_context_failure_code
            nonlocal micro_context_failure_symbol
            nonlocal micro_analysis_checked_at_ms
            nonlocal micro_analyses_prepared
            nonlocal micro_analysis_persist_failed
            if micro_analyses_prepared:
                return
            micro_analyses_prepared = True
            if micro_window_provider is not None:
                try:
                    provided_windows, provided_checked_at_ms = (
                        micro_window_provider()
                    )
                    if (
                        not isinstance(provided_windows, Mapping)
                        or type(provided_checked_at_ms) is not int
                        or provided_checked_at_ms <= 0
                    ):
                        raise RuntimeError("micro sampler snapshot is invalid")
                    micro_windows = provided_windows
                    micro_analysis_checked_at_ms = provided_checked_at_ms
                except Exception:
                    micro_windows = {}
                    micro_context_complete = False
                    micro_context_failure_code = (
                        "MICRO_SAMPLER_SNAPSHOT_FAILED"
                    )
                    micro_context_failure_symbol = None
            if not micro_strategies:
                return
            if not micro_context_complete:
                allowed_micro_context_codes = {
                    "MICRO_PREMIUM_TIME_UNAVAILABLE",
                    "MICRO_TOP100_SOURCE_INCOMPLETE",
                    "MICRO_OBSERVATION_BUILD_FAILED",
                    "MICRO_CACHE_PROPOSAL_FAILED",
                    "MICRO_SAMPLER_SNAPSHOT_FAILED",
                }
                fail_signal_audit(
                    micro_context_failure_code
                    if micro_context_failure_code in allowed_micro_context_codes
                    else "MICRO_CONTEXT_INCOMPLETE",
                    symbol=(
                        micro_context_failure_symbol
                        if type(micro_context_failure_symbol) is str
                        and micro_context_failure_symbol
                        else None
                    ),
                )
            for strategy in micro_strategies:
                for candidate in micro_candidates:
                    try:
                        micro_analyses[(strategy.strategy_id, candidate.symbol)] = (
                            analyze_micro_strategy(
                                strategy.strategy_id, candidate.symbol, micro_windows,
                                micro_analysis_checked_at_ms,
                            )
                        )
                    except Exception:
                        micro_analyses[(strategy.strategy_id, candidate.symbol)] = (
                            MicroAnalysisResult(
                                strategy.strategy_id, candidate.symbol, False,
                                f"{strategy.strategy_id}_ANALYSIS_INVALID",
                                None, None, {},
                            )
                        )
                        fail_signal_audit(
                            "MICRO_ANALYZER_EXCEPTION",
                            strategy.strategy_id,
                            candidate.symbol,
                        )
            priority = {"N25": 0, "N24": 1, "N22": 2, "N23": 3, "N21": 4}
            by_symbol: dict[str, list[str]] = {}
            for (strategy_id, symbol), analysis in micro_analyses.items():
                if analysis.passed and analysis.structure is not None:
                    by_symbol.setdefault(symbol, []).append(strategy_id)
            for symbol, strategy_ids in by_symbol.items():
                if len(strategy_ids) > 1:
                    winner = min(strategy_ids, key=lambda item: priority[item])
                    micro_suppressed.update(
                        (strategy_id, symbol) for strategy_id in strategy_ids
                        if strategy_id != winner
                    )
            passed_analyses = tuple(
                analysis
                for _identity, analysis in sorted(micro_analyses.items())
                if analysis.passed
                and analysis.symbol in global_cooldown_scope
                and global_cooldowns.get(analysis.symbol) is None
            )
            if passed_analyses and not self.recorder.record_micro_passed_analyses(
                scan_id, passed_analyses
            ):
                micro_analysis_persist_failed = {
                    (analysis.strategy_id, analysis.symbol)
                    for analysis in passed_analyses
                }
                for strategy_id, symbol in sorted(
                    micro_analysis_persist_failed
                ):
                    fail_signal_audit(
                        "MICRO_PASSED_ANALYSIS_PERSIST_FAILED",
                        strategy_id,
                        symbol,
                    )
        active_strategy_ids = {
            strategy.strategy_id for strategy in self.strategies
        }
        n12_context = (
            self._build_n12_batch_context(
                candidate_groups, raw_klines_by_symbol, checked_at_ms
            )
            if "N12" in active_strategy_ids
            else N12BatchContext({}, {}, set(), True)
        )
        n13_context = (
            self._build_n13_batch_context(
                candidate_groups, raw_klines_by_symbol, checked_at_ms
            )
            if "N13" in active_strategy_ids
            else N13BatchContext({}, None, None, True)
        )
        n14_context = (
            self._build_n14_batch_context(
                candidate_groups, raw_klines_by_symbol, checked_at_ms
            )
            if "N14" in active_strategy_ids
            else N14BatchContext({}, None, True)
        )
        n15_context = (
            self._build_n15_batch_context(
                candidate_groups, raw_klines_by_symbol, checked_at_ms
            )
            if "N15" in active_strategy_ids
            else N15BatchContext(None, None, True)
        )
        # Older test/integration adapters may expose only the original
        # snapshot/current/complete context fields.  Production contexts are
        # the typed N15BatchContext above; only its explicit False denotes a
        # durable lifecycle failure that owns the round-wide audit gate.
        if getattr(n15_context, "audit_complete", True) is False:
            fail_signal_audit("N15_LIFECYCLE_PERSIST_FAILED", "N15")
        history_source_presence: dict[str, frozenset[str]] = {}
        short_history_symbols = ({
            candidate.symbol
            for candidate in candidate_groups.get("quote_volume_top", [])
            if type(raw_klines_by_symbol.get(candidate.symbol)) is list
            and len(raw_klines_by_symbol[candidate.symbol]) < 122
        } if active_strategy_ids.intersection({"N17", "N18", "N19", "N20"}) else set())
        history_source_presence_complete = True
        if short_history_symbols:
            try:
                history_source_presence = (
                    self.recorder.history_coverage_presence(
                        short_history_symbols
                    )
                )
            except Exception:
                history_source_presence_complete = False
                history_source_presence = {}
        n20_context = (
            self._build_n20_batch_context(
                candidate_groups,
                raw_klines_by_symbol,
                checked_at_ms,
                history_source_presence,
                history_source_presence_complete,
            )
            if "N20" in active_strategy_ids
            else N20BatchContext(None, True)
        )
        if any(
            strategy.evaluator_type
            == "bull_market_pullback_relative_strength_recovery"
            for strategy in self.strategies
        ) and not n20_context.complete:
            fail_signal_audit(
                n20_context.failure_code or "N20_CONTEXT_INCOMPLETE", "N20"
            )
        n16_states_by_symbol: dict[str, Any] = {}
        n16_restore_complete = True
        if any(
            strategy.evaluator_type
            == "mature_trend_dynamic_support_continuation"
            for strategy in self.strategies
        ):
            try:
                active_n16_states = self.recorder.get_active_n16_states("N16")
                for state in active_n16_states:
                    if state.symbol in n16_states_by_symbol:
                        raise RuntimeError(
                            "multiple active N16 episodes share one symbol"
                        )
                    n16_states_by_symbol[state.symbol] = state
                missing_frozen_raw = sorted(
                    set(n16_states_by_symbol).difference(raw_klines_by_symbol)
                )
                if missing_frozen_raw:
                    raise RuntimeError(
                        "N16 frozen members are missing shared klines: %s"
                        % ",".join(missing_frozen_raw)
                    )
            except Exception as exc:
                self.logger.warning("Unable to restore N16 states: %s", exc)
                n16_restore_complete = False
                n16_states_by_symbol = {}
                # Restoration is a round-wide audit prerequisite.  N16 may
                # have zero current Top100 candidates while a frozen dropped
                # member still exists, so relying on a per-candidate rejection
                # would let another strategy publish and execute past a failed
                # N16 lifecycle read.
                fail_signal_audit("N16_RESTORE_FAILED", "N16")

        n17_states_by_symbol: dict[str, Any] = {}
        n17_restore_complete = True
        if any(
            strategy.evaluator_type == "range_lower_support_absorption_rebound"
            for strategy in self.strategies
        ):
            try:
                active_n17_states = self.recorder.get_active_n17_states("N17")
                active_by_symbol: dict[str, Any] = {}
                for state in active_n17_states:
                    if state.symbol in active_by_symbol:
                        raise RuntimeError(
                            "multiple active N17 families share one symbol"
                        )
                    active_by_symbol[state.symbol] = state
                missing = sorted(
                    set(active_by_symbol).difference(raw_klines_by_symbol)
                )
                if missing:
                    raise RuntimeError(
                        "N17 frozen members are missing shared klines: %s"
                        % ",".join(missing)
                    )
                n17_states_by_symbol = self.recorder.get_latest_n17_states(
                    set(raw_klines_by_symbol)
                )
                if any(
                    n17_states_by_symbol.get(symbol) is None
                    or n17_states_by_symbol[symbol].structure_id
                    != active.structure_id
                    for symbol, active in active_by_symbol.items()
                ):
                    raise RuntimeError(
                        "N17 active family is not the latest durable state"
                    )
            except Exception as exc:
                self.logger.warning("Unable to restore N17 states: %s", exc)
                n17_restore_complete = False
                n17_states_by_symbol = {}
                fail_signal_audit("N17_RESTORE_FAILED", "N17")

        n19_states_by_symbol: dict[str, Any] = {}
        n19_market_context_cache: dict[int, Any] = {}
        n19_restore_complete = True
        top100_candidates = list(candidate_groups.get("quote_volume_top", []))
        n19_market_symbols = tuple(
            candidate.symbol
            for candidate in sorted(
                top100_candidates,
                key=lambda item: (
                    item.quote_volume_rank
                    if item.quote_volume_rank is not None
                    else 10**9,
                    item.symbol,
                ),
            )
            if type(candidate.quote_volume_rank) is int
            and 1 <= candidate.quote_volume_rank <= 100
        )
        if (
            len(n19_market_symbols) != 100
            or len(set(n19_market_symbols)) != 100
            or {
                candidate.quote_volume_rank
                for candidate in top100_candidates
                if type(candidate.quote_volume_rank) is int
            }
            != set(range(1, 101))
        ):
            n19_market_symbols = ()
        if any(
            strategy.evaluator_type
            == "medium_staircase_decline_exhaustion_reversal"
            for strategy in self.strategies
        ):
            try:
                active_n19_states = self.recorder.get_active_n19_states("N19")
                active_by_symbol: dict[str, Any] = {}
                for state in active_n19_states:
                    if state.symbol in active_by_symbol:
                        raise RuntimeError(
                            "multiple active N19 families share one symbol"
                        )
                    active_by_symbol[state.symbol] = state
                missing = sorted(
                    set(active_by_symbol).difference(raw_klines_by_symbol)
                )
                if missing:
                    raise RuntimeError(
                        "N19 frozen members are missing shared klines: %s"
                        % ",".join(missing)
                    )
                n19_states_by_symbol = self.recorder.get_latest_n19_states(
                    set(raw_klines_by_symbol)
                )
                if any(
                    n19_states_by_symbol.get(symbol) is None
                    or n19_states_by_symbol[symbol].family_id
                    != active.family_id
                    for symbol, active in active_by_symbol.items()
                ):
                    raise RuntimeError(
                        "N19 active family is not the latest durable state"
                    )
            except Exception as exc:
                self.logger.warning("Unable to restore N19 states: %s", exc)
                n19_restore_complete = False
                n19_states_by_symbol = {}
                fail_signal_audit("N19_RESTORE_FAILED", "N19")

        n18_states_by_symbol: dict[str, Any] = {}
        n18_restore_complete = True
        if any(
            strategy.evaluator_type
            == "ascending_triangle_pressure_absorption_breakout"
            for strategy in self.strategies
        ):
            try:
                active_n18_states = self.recorder.get_active_n18_states("N18")
                active_by_symbol: dict[str, Any] = {}
                for state in active_n18_states:
                    if state.symbol in active_by_symbol:
                        raise RuntimeError(
                            "multiple active N18 families share one symbol"
                        )
                    active_by_symbol[state.symbol] = state
                missing = sorted(set(active_by_symbol).difference(raw_klines_by_symbol))
                if missing:
                    raise RuntimeError(
                        "N18 frozen members are missing shared klines: %s"
                        % ",".join(missing)
                    )
                n18_states_by_symbol = self.recorder.get_latest_n18_states(
                    set(raw_klines_by_symbol)
                )
                if any(
                    n18_states_by_symbol.get(symbol) is None
                    or n18_states_by_symbol[symbol].family_id != active.family_id
                    for symbol, active in active_by_symbol.items()
                ):
                    raise RuntimeError(
                        "N18 active family is not the latest durable state"
                    )
            except Exception as exc:
                self.logger.warning("Unable to restore N18 states: %s", exc)
                n18_restore_complete = False
                n18_states_by_symbol = {}
                fail_signal_audit("N18_RESTORE_FAILED", "N18")

        for strategy in self.strategies:
            strategy_candidates = self._candidates_for_strategy(
                strategy, candidate_groups
            )
            ordinary_candidate_symbols = {
                candidate.symbol for candidate in strategy_candidates
            }
            if strategy.evaluator_type == "broad_market_vwap_rotation":
                strategy_candidates = self._n13_candidates_for_frozen_snapshot(
                    strategy_candidates,
                    n13_context,
                    raw_klines_by_symbol,
                )
            elif strategy.evaluator_type == "localized_sell_pressure_decay_reversal":
                strategy_candidates = self._n14_candidates_for_frozen_snapshots(
                    strategy_candidates,
                    n14_context,
                    raw_klines_by_symbol,
                )
            elif strategy.evaluator_type == "market_breadth_recovery_leader":
                strategy_candidates = self._n15_candidates_for_frozen_snapshot(
                    strategy_candidates,
                    n15_context,
                    raw_klines_by_symbol,
                )
            elif (
                strategy.evaluator_type
                == "mature_trend_dynamic_support_continuation"
                and n16_restore_complete
            ):
                strategy_candidates = self._n16_candidates_for_frozen_states(
                    strategy_candidates,
                    n16_states_by_symbol,
                    raw_klines_by_symbol,
                )
            elif (
                strategy.evaluator_type
                == "range_lower_support_absorption_rebound"
                and n17_restore_complete
            ):
                strategy_candidates = self._n17_candidates_for_frozen_states(
                    strategy_candidates,
                    n17_states_by_symbol,
                    raw_klines_by_symbol,
                )
            elif (
                strategy.evaluator_type
                == "medium_staircase_decline_exhaustion_reversal"
                and n19_restore_complete
            ):
                strategy_candidates = self._n19_candidates_for_frozen_states(
                    strategy_candidates,
                    n19_states_by_symbol,
                    raw_klines_by_symbol,
                )
            elif (
                strategy.evaluator_type
                == "ascending_triangle_pressure_absorption_breakout"
                and n18_restore_complete
            ):
                strategy_candidates = self._n18_candidates_for_frozen_states(
                    strategy_candidates,
                    n18_states_by_symbol,
                    raw_klines_by_symbol,
                )
            elif (
                strategy.evaluator_type
                == "bull_market_pullback_relative_strength_recovery"
                and n20_context.analysis is not None
            ):
                strategy_candidates = self._n20_candidates_for_frozen_episode(
                    strategy_candidates,
                    n20_context.analysis,
                    raw_klines_by_symbol,
                )
            uncovered_cooldown_symbols = sorted(
                {
                    candidate.symbol
                    for candidate in strategy_candidates
                    if candidate.symbol not in global_cooldown_scope
                }
            )
            for symbol in uncovered_cooldown_symbols:
                fail_signal_audit(
                    "GLOBAL_COOLDOWN_SNAPSHOT_INCOMPLETE",
                    strategy.strategy_id,
                    symbol,
                )
            n14_intrinsic_analyses: dict[str, N14AnalysisResult] = {}
            if strategy.strategy_id == "N14":
                for candidate in strategy_candidates:
                    raw_klines = raw_klines_by_symbol.get(candidate.symbol)
                    if raw_klines is None:
                        continue
                    analysis = self._probe_n14_candidate(
                        strategy,
                        candidate,
                        raw_klines,
                        checked_at_ms,
                        n14_context,
                    )
                    if analysis is not None:
                        n14_intrinsic_analyses[candidate.symbol] = analysis
            if strategy.strategy_id in MICRO_STRATEGY_IDS:
                prepare_micro_analyses()
                raw_decisions = []
                for candidate in strategy_candidates:
                    if candidate.symbol not in global_cooldown_scope:
                        raw_decisions.append(
                            self._rejected(
                                strategy,
                                candidate,
                                "GLOBAL_COOLDOWN_SNAPSHOT_INCOMPLETE",
                            )
                        )
                        continue
                    global_cooldown = global_cooldowns.get(candidate.symbol)
                    if global_cooldown is not None:
                        raw_decisions.append(
                            self._rejected(
                                strategy,
                                candidate,
                                "GLOBAL_SYMBOL_COOLDOWN_UNTIL:"
                                f"{global_cooldown.cooldown_until}",
                            )
                        )
                        continue
                    analysis = micro_analyses.get((strategy.strategy_id, candidate.symbol))
                    if analysis is None:
                        raw_decisions.append(self._rejected(
                            strategy, candidate,
                            f"{strategy.strategy_id}_MARKET_CONTEXT_INSUFFICIENT",
                        ))
                        continue
                    if analysis.structure is not None:
                        candidate = replace(candidate, mark_price=analysis.structure.entry_price)
                    if (strategy.strategy_id, candidate.symbol) in micro_suppressed:
                        raw_decisions.append(StrategySignalDecision(
                            strategy, candidate, analysis, False, "REJECTED",
                            f"{strategy.strategy_id}_OVERLAP_SUPPRESSED",
                        ))
                    elif analysis.passed:
                        raw_decisions.append(StrategySignalDecision(
                            strategy, candidate, analysis, True, "PASSED", "PASSED"
                        ))
                    else:
                        raw_decisions.append(self._rejected(
                            strategy, candidate, analysis.reason, analysis
                        ))
            else:
                raw_decisions = [
                self._evaluate_candidate(
                    strategy,
                    candidate,
                    raw_klines_by_symbol,
                    checked_at_ms,
                    n12_context,
                    n13_context,
                    n14_context,
                    n15_context,
                    n16_states_by_symbol,
                    n16_restore_complete,
                    n17_states_by_symbol,
                    n17_restore_complete,
                    n19_states_by_symbol,
                    n19_restore_complete,
                    n19_market_symbols,
                    n18_states_by_symbol,
                    n18_restore_complete,
                    n20_context,
                    history_coverage_proposals,
                    global_cooldowns,
                    global_cooldown_scope,
                    n19_market_context_cache,
                    history_source_presence,
                    history_source_presence_complete,
                    strategy_cooldowns,
                    strategy_cooldown_scope,
                    n08_history_coverages,
                    n08_states_by_symbol,
                    n13_pending_records,
                    n13_pending_guards,
                )
                for candidate in strategy_candidates
                ]

            if (
                strategy.strategy_id == "N08"
                and n08_history_coverages is not None
                and n08_history_coverages
            ):
                persisted_symbols = {
                    coverage.symbol for coverage in n08_history_coverages
                }
                if not self.recorder.upsert_n08_history_coverages(
                    tuple(n08_history_coverages)
                ):
                    raw_decisions = [
                        replace(
                            decision,
                            passed=False,
                            decision="REJECTED",
                            reason="N08_HISTORY_COVERAGE_PERSIST_FAILED",
                        )
                        if decision.candidate.symbol in persisted_symbols
                        else decision
                        for decision in raw_decisions
                    ]
                n08_history_coverages.clear()

            if (
                strategy.strategy_id == "N13"
                and n13_pending_records is not None
                and n13_pending_guards is not None
                and (n13_pending_records or n13_pending_guards)
            ):
                state_result = self.recorder.record_n13_rotation_states_atomically(
                    n13_pending_records,
                    existing_guards=n13_pending_guards,
                )
                if state_result != "OK":
                    raw_decisions = [
                        replace(
                            decision,
                            passed=False,
                            decision="REJECTED",
                            reason=state_result,
                        )
                        for decision in raw_decisions
                    ]
                n13_pending_records.clear()
                n13_pending_guards.clear()

            micro_execution_winner: StrategySignalDecision | None = None
            micro_execution_consumed = False
            if strategy.strategy_id in MICRO_STRATEGY_IDS:
                intrinsic_micro = [
                    item for item in raw_decisions
                    if item.passed and isinstance(item.analysis, MicroAnalysisResult)
                    and item.analysis.structure is not None
                ]
                if intrinsic_micro:
                    micro_execution_winner = min(
                        intrinsic_micro,
                        key=lambda item: item.analysis.structure.winner_key,
                    )
                    try:
                        micro_claim_status = self.recorder.inspect_passed_structure(
                            strategy.strategy_id,
                            micro_execution_winner.candidate.symbol,
                            micro_execution_winner.analysis.structure_id,
                        )
                    except Exception:
                        micro_claim_status = "INCONSISTENT"
                        fail_signal_audit(
                            "MICRO_CLAIM_INSPECTION_EXCEPTION",
                            strategy.strategy_id,
                            micro_execution_winner.candidate.symbol,
                        )
                    if micro_claim_status == "CONSUMED":
                        micro_execution_consumed = True
                    elif micro_claim_status != "MISSING":
                        micro_execution_consumed = True
                        fail_signal_audit(
                            "MICRO_CLAIM_INCONSISTENT",
                            strategy.strategy_id,
                            micro_execution_winner.candidate.symbol,
                        )

            n16_execution_winner: StrategySignalDecision | None = None
            if strategy.strategy_id == "N16":
                intrinsic = [
                    decision
                    for decision in raw_decisions
                    if decision.candidate.symbol in ordinary_candidate_symbols
                    if isinstance(decision.analysis, N16AnalysisResult)
                    and decision.analysis.passed
                    and decision.analysis.structure_id is not None
                    and decision.reason != "N16_STRUCTURE_CONSUMED"
                ]
                if intrinsic:
                    n16_execution_winner = min(
                        intrinsic,
                        key=lambda decision: (
                            decision.analysis.quote_volume_rank,
                            decision.candidate.symbol,
                            decision.analysis.structure_id,
                        ),
                    )

            n17_execution_winner: StrategySignalDecision | None = None
            if strategy.strategy_id == "N17":
                intrinsic_n17 = [
                    decision
                    for decision in raw_decisions
                    if decision.candidate.symbol in ordinary_candidate_symbols
                    if isinstance(decision.analysis, N17AnalysisResult)
                    and decision.analysis.passed
                    and decision.analysis.structure is not None
                ]
                if intrinsic_n17:
                    n17_execution_winner = min(
                        intrinsic_n17,
                        key=lambda decision: (
                            abs(
                                decision.analysis.structure.touch.low
                                - decision.analysis.structure.box.lower
                            ),
                            decision.analysis.structure.absorption_sell_quote_ratio,
                            decision.analysis.quote_volume_rank,
                            decision.candidate.symbol,
                            decision.analysis.structure_id,
                        ),
                    )

            n19_execution_winner: StrategySignalDecision | None = None
            if strategy.strategy_id == "N19":
                intrinsic_n19 = [
                    decision
                    for decision in raw_decisions
                    if decision.candidate.symbol in ordinary_candidate_symbols
                    if isinstance(decision.analysis, N19AnalysisResult)
                    and decision.analysis.passed
                    and decision.analysis.structure_id is not None
                ]
                if intrinsic_n19:
                    n19_execution_winner = min(
                        intrinsic_n19,
                        key=lambda decision: (
                            decision.analysis.quote_volume_rank,
                            decision.candidate.symbol,
                            decision.analysis.structure_id,
                        ),
                    )

            n18_execution_winner: StrategySignalDecision | None = None
            if strategy.strategy_id == "N18":
                intrinsic_n18 = [
                    decision for decision in raw_decisions
                    if decision.candidate.symbol in ordinary_candidate_symbols
                    if isinstance(decision.analysis, N18AnalysisResult)
                    and decision.analysis.passed
                    and decision.analysis.structure_id is not None
                ]
                if intrinsic_n18:
                    n18_execution_winner = min(
                        intrinsic_n18,
                        key=lambda decision: (
                            decision.analysis.quote_volume_rank,
                            decision.candidate.symbol,
                            decision.analysis.structure_id,
                        ),
                    )

            n14_execution_winner: StrategySignalDecision | None = None
            if strategy.strategy_id == "N14":
                # Freeze the strategy-internal winner from the analyzer result,
                # before state or signal persistence can fail.  The pure probe
                # has no DB writes/reads; retaining the decision analysis as a
                # fallback also keeps this gate testable in isolation.  A
                # failed audit for the best setup must never promote the
                # runner-up, while an unrelated candidate failure must not
                # disable a healthy winner.
                raw_passed: list[
                    tuple[StrategySignalDecision, N14AnalysisResult]
                ] = []
                for decision in raw_decisions:
                    if decision.candidate.symbol not in ordinary_candidate_symbols:
                        continue
                    analysis: N14AnalysisResult | None = None
                    if (
                        decision.passed
                        and isinstance(
                            decision.analysis, N14AnalysisResult
                        )
                        and decision.analysis.passed
                    ):
                        # The formal analysis is authoritative for valid
                        # business state, including a C_CONFIRMED episode whose
                        # breadth/gate were frozen on an earlier poll.
                        analysis = decision.analysis
                    elif decision.reason in {
                        "N14_STATE_READ_FAILED",
                        "N14_STATE_INCONSISTENT",
                        "N14_STATE_PERSIST_FAILED",
                    }:
                        # Only state-integrity failures may borrow the pure
                        # market probe.  Normal business rejections such as an
                        # already-consumed episode must never re-enter ranking.
                        analysis = n14_intrinsic_analyses.get(
                            decision.candidate.symbol
                        )
                        if analysis is None and isinstance(
                            decision.analysis, N14AnalysisResult
                        ):
                            analysis = decision.analysis
                    if analysis is not None and analysis.passed:
                        raw_passed.append((decision, analysis))

                def raw_n14_key(
                    item: tuple[
                        StrategySignalDecision, N14AnalysisResult
                    ],
                ):
                    decision, analysis = item
                    rank = (
                        analysis.structure.quote_volume_rank
                        if analysis.structure is not None
                        else None
                    )
                    return (
                        -analysis.flow_flip,
                        -analysis.confirmation_close_location,
                        rank if rank is not None else 10**9,
                        decision.candidate.symbol,
                    )

                if raw_passed:
                    n14_execution_winner = min(
                        raw_passed, key=raw_n14_key
                    )[0]

            for raw_decision in raw_decisions:
                micro_persist_failed = (
                    strategy.strategy_id,
                    raw_decision.candidate.symbol,
                ) in micro_analysis_persist_failed
                effective_decision = raw_decision
                if (
                    micro_persist_failed
                ):
                    effective_decision = replace(
                        raw_decision,
                        passed=False,
                        decision="REJECTED",
                        reason=(
                            f"{strategy.strategy_id}_PASSED_ANALYSIS_"
                            "PERSIST_FAILED"
                        ),
                    )
                elif (
                    strategy.strategy_id in MICRO_STRATEGY_IDS
                    and raw_decision.passed
                    and isinstance(raw_decision.analysis, MicroAnalysisResult)
                    and raw_decision is not micro_execution_winner
                ):
                    effective_decision = replace(
                        raw_decision, passed=False, decision="REJECTED",
                        reason=f"{strategy.strategy_id}_NOT_REPRESENTATIVE",
                    )
                elif (
                    strategy.strategy_id in MICRO_STRATEGY_IDS
                    and raw_decision is micro_execution_winner
                    and micro_execution_consumed
                ):
                    effective_decision = replace(
                        raw_decision,
                        passed=False,
                        decision="REJECTED",
                        reason=f"{strategy.strategy_id}_STRUCTURE_CONSUMED",
                    )
                if (
                    strategy.strategy_id == "N16"
                    and isinstance(raw_decision.analysis, N16AnalysisResult)
                    and raw_decision.passed
                    and raw_decision.analysis.passed
                    and raw_decision is not n16_execution_winner
                ):
                    # Preserve the complete intrinsic PASSED analysis in the
                    # permanent N16 lifecycle row, but only the frozen
                    # representative may claim the executable PASSED ledger.
                    # This prevents a non-executed runner from being consumed
                    # and also prevents fallback when the representative later
                    # fails plan construction.
                    effective_decision = replace(
                        raw_decision,
                        passed=False,
                        decision="REJECTED",
                        reason="N16_NOT_REPRESENTATIVE",
                    )
                if (
                    strategy.strategy_id == "N17"
                    and isinstance(raw_decision.analysis, N17AnalysisResult)
                    and raw_decision.passed
                    and raw_decision.analysis.passed
                    and raw_decision is not n17_execution_winner
                ):
                    effective_decision = replace(
                        raw_decision,
                        passed=False,
                        decision="REJECTED",
                        reason="N17_NOT_REPRESENTATIVE",
                    )
                if (
                    strategy.strategy_id == "N19"
                    and isinstance(raw_decision.analysis, N19AnalysisResult)
                    and raw_decision.passed
                    and raw_decision.analysis.passed
                    and raw_decision is not n19_execution_winner
                ):
                    effective_decision = replace(
                        raw_decision,
                        passed=False,
                        decision="REJECTED",
                        reason="N19_NOT_REPRESENTATIVE",
                    )
                if (
                    strategy.strategy_id == "N18"
                    and isinstance(raw_decision.analysis, N18AnalysisResult)
                    and raw_decision.passed
                    and raw_decision.analysis.passed
                    and raw_decision is not n18_execution_winner
                ):
                    effective_decision = replace(
                        raw_decision,
                        passed=False,
                        decision="REJECTED",
                        reason="N18_NOT_REPRESENTATIVE",
                    )
                execution_selected = not (
                    (
                        strategy.strategy_id in MICRO_STRATEGY_IDS
                        and raw_decision is not micro_execution_winner
                    )
                    or (
                        strategy.strategy_id == "N17"
                        and raw_decision is not n17_execution_winner
                    )
                    or (
                        strategy.strategy_id == "N14"
                        and raw_decision is not n14_execution_winner
                    )
                    or (
                        strategy.strategy_id == "N16"
                        and raw_decision is not n16_execution_winner
                    )
                    or (
                        strategy.strategy_id == "N19"
                        and raw_decision is not n19_execution_winner
                    )
                    or (
                        strategy.strategy_id == "N18"
                        and raw_decision is not n18_execution_winner
                    )
                )
                if (
                    strategy.strategy_id
                    not in _OFFBOARD_LIFECYCLE_ONLY_STRATEGIES
                    or raw_decision.candidate.symbol
                    in ordinary_candidate_symbols
                ):
                    # Frozen/offboard members are deliberately evaluated before
                    # this boundary.  Their lifecycle, coverage proposal and
                    # every critical failure therefore remain authoritative,
                    # while only the current strategy candidate universe may
                    # enter ordinary signals or execution selection.
                    pending_signal_decisions.append(
                        (
                            raw_decision,
                            effective_decision,
                            execution_selected,
                        )
                    )
                if (
                    strategy.strategy_id in MICRO_STRATEGY_IDS
                    and raw_decision.reason in {
                        f"{strategy.strategy_id}_ANALYSIS_INVALID",
                        f"{strategy.strategy_id}_MARKET_CONTEXT_INSUFFICIENT",
                        f"{strategy.strategy_id}_MICRO_OBSERVATION_INVALID",
                        f"{strategy.strategy_id}_EVIDENCE_TOO_LARGE",
                    }
                ):
                    fail_signal_audit(
                        raw_decision.reason,
                        strategy.strategy_id,
                        raw_decision.candidate.symbol,
                    )
                if (
                    strategy.strategy_id == "N16"
                    and raw_decision.reason in {
                        "N16_DEFINITION_INVALID",
                        "N16_STATE_READ_FAILED",
                        "N16_STATE_INCONSISTENT",
                        "N16_STATE_PERSIST_FAILED",
                        "N16_FROZEN_EVIDENCE_INVALID",
                        "N16_ENTRY_CANDLE_MISMATCH",
                    }
                ):
                    fail_signal_audit(
                        raw_decision.reason,
                        strategy.strategy_id,
                        raw_decision.candidate.symbol,
                    )
                if (
                    strategy.strategy_id == "N17"
                    and raw_decision.reason in {
                        "N17_DEFINITION_INVALID",
                        "N17_STATE_READ_FAILED",
                        "N17_STATE_INCONSISTENT",
                        "N17_STATE_PERSIST_FAILED",
                        "N17_FROZEN_EVIDENCE_INVALID",
                        "N17_KLINE_SEQUENCE_INVALID",
                        "N17_HISTORY_COVERAGE_PERSIST_FAILED",
                        "N17_HISTORY_COVERAGE_READ_FAILED",
                        "N17_HISTORY_COVERAGE_INCONSISTENT",
                        "N17_HISTORY_COVERAGE_GAP_BLOCKED",
                    }
                ):
                    fail_signal_audit(
                        raw_decision.reason,
                        strategy.strategy_id,
                        raw_decision.candidate.symbol,
                    )
                if (
                    strategy.strategy_id == "N19"
                    and raw_decision.reason in {
                        "N19_DEFINITION_INVALID",
                        "N19_STATE_READ_FAILED",
                        "N19_STATE_INCONSISTENT",
                        "N19_STATE_PERSIST_FAILED",
                        "N19_FROZEN_EVIDENCE_INVALID",
                        "N19_KLINE_SEQUENCE_INVALID",
                        "N19_HISTORY_COVERAGE_PERSIST_FAILED",
                        "N19_HISTORY_COVERAGE_READ_FAILED",
                        "N19_HISTORY_COVERAGE_INCONSISTENT",
                        "N19_HISTORY_COVERAGE_GAP_BLOCKED",
                        "N19_MARKET_CONTEXT_INSUFFICIENT",
                    }
                ):
                    fail_signal_audit(
                        raw_decision.reason,
                        strategy.strategy_id,
                        raw_decision.candidate.symbol,
                    )
                if (
                    strategy.strategy_id == "N18"
                    and raw_decision.reason in {
                        "N18_DEFINITION_INVALID", "N18_STATE_READ_FAILED",
                        "N18_STATE_INCONSISTENT", "N18_STATE_PERSIST_FAILED",
                        "N18_FROZEN_EVIDENCE_INVALID",
                        "N18_KLINE_SEQUENCE_INVALID",
                        "N18_HISTORY_COVERAGE_PERSIST_FAILED",
                        "N18_HISTORY_COVERAGE_READ_FAILED",
                        "N18_HISTORY_COVERAGE_INCONSISTENT",
                        "N18_HISTORY_COVERAGE_GAP_BLOCKED",
                    }
                ):
                    fail_signal_audit(
                        raw_decision.reason,
                        strategy.strategy_id,
                        raw_decision.candidate.symbol,
                    )
        batch_write = (
            self.recorder.record_strategy_signals(
                scan_id,
                tuple(
                    self._signal_record_payload(effective_decision)
                    for _, effective_decision, _ in pending_signal_decisions
                ),
            )
            if pending_signal_decisions
            else None
        )
        if batch_write is not None and not batch_write.complete:
            failure = (
                pending_signal_decisions[batch_write.failed_index]
                if type(batch_write.failed_index) is int
                and 0 <= batch_write.failed_index
                < len(pending_signal_decisions)
                else None
            )
            fail_signal_audit(
                "SIGNAL_RECORD_PERSIST_FAILED",
                (
                    failure[1].strategy.strategy_id
                    if failure is not None
                    else None
                ),
                (
                    failure[1].candidate.symbol
                    if failure is not None
                    else None
                ),
            )
        if batch_write is not None and batch_write.complete and len(
            batch_write.signal_ids
        ) != len(
            pending_signal_decisions
        ):
            fail_signal_audit("SIGNAL_RECORD_RESULT_INCOMPLETE")
        signal_ids = (
            batch_write.signal_ids
            if batch_write is not None
            and batch_write.complete
            and len(batch_write.signal_ids) == len(pending_signal_decisions)
            else (None,) * len(pending_signal_decisions)
        )

        for (
            raw_decision,
            effective_decision,
            execution_selected,
        ), signal_id in zip(pending_signal_decisions, signal_ids):
            decision = replace(effective_decision, signal_id=signal_id)
            if decision.passed and signal_id is None:
                decision = replace(
                    decision,
                    passed=False,
                    decision="REJECTED",
                    reason="SIGNAL_AUDIT_PERSIST_FAILED",
                )
            signals.append(decision)
            if (
                decision.reason.startswith("GLOBAL_SYMBOL_COOLDOWN_UNTIL:")
                and decision.candidate.symbol not in global_cooldown_symbols
            ):
                global_cooldown_symbols.add(decision.candidate.symbol)
                self.recorder.record_event(
                    "global_symbol_cooldown_skip",
                    {
                        "scan_id": scan_id,
                        "cooldown_until": decision.reason.split(":", 1)[1],
                        "scope": "PAPER_AND_LIVE",
                    },
                    decision.candidate.symbol,
                )
            if not decision.passed or not execution_selected:
                continue
            passed_signals.append(decision)
            if self.recorder.strategy_activity_mode(
                decision.strategy.strategy_id
            ) == "IDLE":
                live_candidates.append(LiveTradeCandidate(decision))

        signal_batch_published = False
        if signal_audit_complete:
            try:
                signal_batch_published = self.recorder.publish_strategy_signal_batch(
                    scan_id,
                    len(signals),
                    tuple(history_coverage_proposals),
                )
            except Exception as exc:
                fail_signal_audit("SIGNAL_BATCH_PUBLISH_EXCEPTION")
                self.logger.warning(
                    "Unable to publish complete strategy signal batch: %s",
                    exc,
                )
            if not signal_batch_published and signal_audit_complete:
                fail_signal_audit("SIGNAL_BATCH_PUBLISH_REJECTED")
        if signal_audit_failures:
            self.logger.error(
                "Strategy signal audit incomplete | scan_id=%s failures=%s "
                "overflow=%s",
                scan_id,
                json.dumps(
                    [failure.log_payload() for failure in signal_audit_failures],
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                signal_audit_failure_overflow,
            )
        if not signal_batch_published:
            signals = [
                replace(
                    signal,
                    passed=False,
                    decision="REJECTED",
                    reason="SIGNAL_BATCH_AUDIT_INCOMPLETE",
                )
                if signal.passed
                else signal
                for signal in signals
            ]
            passed_signals = []
            live_candidates = []

        def n14_signal_key(signal: StrategySignalDecision):
            analysis = signal.analysis
            if not isinstance(analysis, N14AnalysisResult):
                return (Decimal("0"), Decimal("0"), 10**9, signal.candidate.symbol)
            rank = (
                analysis.structure.quote_volume_rank
                if analysis.structure is not None
                else None
            )
            return (
                -analysis.flow_flip,
                -analysis.confirmation_close_location,
                rank if rank is not None else 10**9,
                signal.candidate.symbol,
            )

        n14_passed = sorted(
            (
                signal
                for signal in passed_signals
                if signal.strategy.strategy_id == "N14"
            ),
            key=n14_signal_key,
        )
        passed_signals = [
            signal for signal in passed_signals if signal.strategy.strategy_id != "N14"
        ] + n14_passed[:1]
        n14_live = sorted(
            (
                item
                for item in live_candidates
                if item.signal.strategy.strategy_id == "N14"
            ),
            key=lambda item: n14_signal_key(item.signal),
        )
        live_candidates = [
            item
            for item in live_candidates
            if item.signal.strategy.strategy_id != "N14"
        ] + n14_live[:1]
        self._flush_n13_isolated_warning_summary(scan_id)
        return SchedulerResult(
            signals,
            passed_signals,
            live_candidates,
            signal_batch_published,
            tuple(signal_audit_failures),
            signal_audit_failure_overflow,
        )

    def _candidate_groups(
        self,
        candidates: list[FundingCandidate] | Mapping[str, list[FundingCandidate]],
    ) -> Mapping[str, list[FundingCandidate]]:
        if isinstance(candidates, Mapping):
            return candidates
        return {
            "negative_funding": candidates,
            "quote_volume_top": candidates,
        }

    def _candidates_for_strategy(
        self,
        strategy: StrategyConfig,
        candidate_groups: Mapping[str, list[FundingCandidate]],
    ) -> list[FundingCandidate]:
        candidates = list(candidate_groups.get(strategy.market_filter, []))
        if strategy.market_filter == "quote_volume_top" and strategy.volume_top_n is not None:
            return [
                candidate
                for candidate in candidates
                if candidate.quote_volume_rank is None or candidate.quote_volume_rank <= strategy.volume_top_n
            ]
        return candidates

    def _n13_candidates_for_frozen_snapshot(
        self,
        current_candidates: list[FundingCandidate],
        context: N13BatchContext,
        raw_klines_by_symbol: dict[str, list[list[Any]]],
    ) -> list[FundingCandidate]:
        """Evaluate frozen old members only when their shared raw is already present."""
        result_by_symbol = {
            candidate.symbol: candidate for candidate in current_candidates
        }
        if not context.complete or context.current_open_time_ms is None:
            return list(result_by_symbol.values())
        for symbol, row in context.rows.items():
            if symbol in result_by_symbol:
                continue
            raw = raw_klines_by_symbol.get(symbol)
            if raw is None:
                continue
            try:
                parsed = parse_n13_klines(raw)
                if (
                    len(parsed) < 2
                    or parsed[-1].open_time_ms != context.current_open_time_ms
                    or parsed[-2].open_time_ms + FIFTEEN_MINUTES_MS
                    != context.current_open_time_ms
                ):
                    continue
            except (ArithmeticError, TypeError, ValueError):
                continue
            result_by_symbol[symbol] = FundingCandidate(
                symbol=symbol,
                funding_rate=None,
                mark_price=parsed[-1].close,
                quote_volume=None,
                quote_volume_rank=row.quote_volume_rank,
                candidate_universe="quote_volume_top_frozen_n13",
            )
        return sorted(
            result_by_symbol.values(),
            key=lambda item: (
                item.quote_volume_rank
                if item.quote_volume_rank is not None
                else 10**9,
                item.symbol,
            ),
        )

    def _n14_candidates_for_frozen_snapshots(
        self,
        current_candidates: list[FundingCandidate],
        context: N14BatchContext,
        raw_klines_by_symbol: dict[str, list[list[Any]]],
    ) -> list[FundingCandidate]:
        result_by_symbol = {
            candidate.symbol: candidate for candidate in current_candidates
        }
        if context.current_open_time_ms is None:
            return list(result_by_symbol.values())
        for runtime in context.snapshots.values():
            for symbol, row in runtime.snapshot.rows.items():
                if symbol in result_by_symbol or symbol not in raw_klines_by_symbol:
                    continue
                try:
                    candles = parse_n14_klines(raw_klines_by_symbol[symbol])
                    if (
                        len(candles) < 2
                        or candles[-1].open_time_ms != context.current_open_time_ms
                        or candles[-2].open_time_ms + N14_INTERVAL_MS
                        != context.current_open_time_ms
                    ):
                        continue
                except (ArithmeticError, TypeError, ValueError):
                    continue
                result_by_symbol[symbol] = FundingCandidate(
                    symbol=symbol,
                    funding_rate=None,
                    mark_price=candles[-1].close,
                    quote_volume=None,
                    quote_volume_rank=row.quote_volume_rank,
                    candidate_universe="quote_volume_top_frozen_n14",
                )
        return sorted(
            result_by_symbol.values(),
            key=lambda item: (
                item.quote_volume_rank
                if item.quote_volume_rank is not None
                else 10**9,
                item.symbol,
            ),
        )

    def _n15_candidates_for_frozen_snapshot(
        self,
        current_candidates: list[FundingCandidate],
        context: N15BatchContext,
        raw_klines_by_symbol: dict[str, list[list[Any]]],
    ) -> list[FundingCandidate]:
        result = {item.symbol: item for item in current_candidates}
        if not context.complete or context.snapshot is None:
            return list(result.values())
        for symbol, row in context.snapshot.rows.items():
            lifecycle_only = symbol not in result
            raw = raw_klines_by_symbol.get(symbol)
            if raw is None:
                continue
            try:
                candle = parse_n15_klines(raw)[-1]
            except (ArithmeticError, TypeError, ValueError):
                continue
            if candle.open_time_ms != context.snapshot.e_open_time_ms:
                continue
            result[symbol] = FundingCandidate(
                symbol=symbol,
                funding_rate=None,
                mark_price=candle.close,
                quote_volume=None,
                quote_volume_rank=row.quote_volume_rank,
                candidate_universe=(
                    "quote_volume_top_frozen_n15_offboard"
                    if lifecycle_only
                    else "quote_volume_top_frozen_n15"
                ),
            )
        return sorted(
            result.values(),
            key=lambda item: (
                item.quote_volume_rank if item.quote_volume_rank is not None else 10**9,
                item.symbol,
            ),
        )

    def _n16_candidates_for_frozen_states(
        self,
        current_candidates: list[FundingCandidate],
        states_by_symbol: Mapping[str, Any],
        raw_klines_by_symbol: dict[str, list[list[Any]]],
    ) -> list[FundingCandidate]:
        result = {item.symbol: item for item in current_candidates}
        for symbol, state in states_by_symbol.items():
            if (
                type(symbol) is not str
                or not symbol
                or state.symbol != symbol
                or type(state.quote_volume_rank) is not int
                or not 1 <= state.quote_volume_rank <= 100
            ):
                raise RuntimeError("N16 frozen candidate identity is invalid")
            current = result.get(symbol)
            mark_price = current.mark_price if current is not None else Decimal("0")
            raw = raw_klines_by_symbol.get(symbol)
            if raw is not None:
                try:
                    mark_price = Decimal(str(raw[-1][4]))
                except (ArithmeticError, IndexError, TypeError, ValueError):
                    mark_price = Decimal("0")
            result[symbol] = FundingCandidate(
                symbol=symbol,
                funding_rate=None,
                mark_price=mark_price,
                quote_volume=(current.quote_volume if current is not None else None),
                quote_volume_rank=state.quote_volume_rank,
                candidate_universe="quote_volume_top_frozen_n16",
            )
        return sorted(
            result.values(),
            key=lambda item: (
                item.quote_volume_rank
                if item.quote_volume_rank is not None
                else 10**9,
                item.symbol,
            ),
        )

    def _n17_candidates_for_frozen_states(
        self,
        current_candidates: list[FundingCandidate],
        states_by_symbol: Mapping[str, Any],
        raw_klines_by_symbol: dict[str, list[list[Any]]],
    ) -> list[FundingCandidate]:
        result = {item.symbol: item for item in current_candidates}
        for symbol, state in states_by_symbol.items():
            if (
                type(symbol) is not str
                or not symbol
                or state.symbol != symbol
                or type(state.quote_volume_rank) is not int
                or not 1 <= state.quote_volume_rank <= 100
            ):
                raise RuntimeError("N17 frozen candidate identity is invalid")
            current = result.get(symbol)
            mark_price = current.mark_price if current is not None else Decimal("0")
            raw = raw_klines_by_symbol.get(symbol)
            if raw is not None:
                try:
                    mark_price = Decimal(str(raw[-1][4]))
                except (ArithmeticError, IndexError, TypeError, ValueError):
                    mark_price = Decimal("0")
            result[symbol] = FundingCandidate(
                symbol=symbol,
                funding_rate=None,
                mark_price=mark_price,
                quote_volume=current.quote_volume if current is not None else None,
                quote_volume_rank=state.quote_volume_rank,
                candidate_universe="quote_volume_top_frozen_n17",
            )
        return sorted(
            result.values(),
            key=lambda item: (
                item.quote_volume_rank
                if item.quote_volume_rank is not None
                else 10**9,
                item.symbol,
            ),
        )

    def _n19_candidates_for_frozen_states(
        self,
        current_candidates: list[FundingCandidate],
        states_by_symbol: Mapping[str, Any],
        raw_klines_by_symbol: dict[str, list[list[Any]]],
    ) -> list[FundingCandidate]:
        result = {item.symbol: item for item in current_candidates}
        for symbol, state in states_by_symbol.items():
            if (
                type(symbol) is not str
                or not symbol
                or state.symbol != symbol
                or type(state.quote_volume_rank) is not int
                or not 1 <= state.quote_volume_rank <= 100
            ):
                raise RuntimeError("N19 frozen candidate identity is invalid")
            current = result.get(symbol)
            mark_price = current.mark_price if current is not None else Decimal("0")
            raw = raw_klines_by_symbol.get(symbol)
            if raw is not None:
                try:
                    mark_price = Decimal(str(raw[-1][4]))
                except (ArithmeticError, IndexError, TypeError, ValueError):
                    mark_price = Decimal("0")
            result[symbol] = FundingCandidate(
                symbol=symbol,
                funding_rate=None,
                mark_price=mark_price,
                quote_volume=current.quote_volume if current is not None else None,
                quote_volume_rank=state.quote_volume_rank,
                candidate_universe="quote_volume_top_frozen_n19",
            )
        return sorted(
            result.values(),
            key=lambda item: (
                item.quote_volume_rank
                if item.quote_volume_rank is not None
                else 10**9,
                item.symbol,
            ),
        )

    def _n18_candidates_for_frozen_states(
        self,
        current_candidates: list[FundingCandidate],
        states_by_symbol: Mapping[str, Any],
        raw_klines_by_symbol: dict[str, list[list[Any]]],
    ) -> list[FundingCandidate]:
        result = {item.symbol: item for item in current_candidates}
        for symbol, state in states_by_symbol.items():
            if (
                type(symbol) is not str or not symbol or state.symbol != symbol
                or type(state.quote_volume_rank) is not int
                or not 1 <= state.quote_volume_rank <= 100
            ):
                raise RuntimeError("N18 frozen candidate identity is invalid")
            current = result.get(symbol)
            mark_price = current.mark_price if current is not None else Decimal("0")
            raw = raw_klines_by_symbol.get(symbol)
            if raw is not None:
                try:
                    mark_price = Decimal(str(raw[-1][4]))
                except (ArithmeticError, IndexError, TypeError, ValueError):
                    mark_price = Decimal("0")
            result[symbol] = FundingCandidate(
                symbol=symbol,
                funding_rate=None,
                mark_price=mark_price,
                quote_volume=current.quote_volume if current is not None else None,
                quote_volume_rank=state.quote_volume_rank,
                candidate_universe="quote_volume_top_frozen_n18",
            )
        return sorted(
            result.values(),
            key=lambda item: (
                item.quote_volume_rank if item.quote_volume_rank is not None else 10**9,
                item.symbol,
            ),
        )

    def _short_history_rejection(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        history_source_presence: Mapping[str, frozenset[str]] | None,
        history_source_presence_complete: bool,
    ) -> StrategySignalDecision:
        if not history_source_presence_complete:
            return self._rejected(
                strategy,
                candidate,
                f"{strategy.strategy_id}_HISTORY_COVERAGE_READ_FAILED",
            )
        presence = history_source_presence or {}
        owners = presence.get(candidate.symbol)
        if owners is None:
            return self._rejected(
                strategy,
                candidate,
                f"{strategy.strategy_id}_HISTORY_COVERAGE_INCONSISTENT",
            )
        if strategy.strategy_id in owners:
            return self._rejected(
                strategy,
                candidate,
                f"{strategy.strategy_id}_HISTORY_COVERAGE_INCONSISTENT",
            )
        return self._rejected(
            strategy,
            candidate,
            f"{strategy.strategy_id}_HISTORY_SOURCE_INSUFFICIENT",
        )

    @staticmethod
    def _history_coverage_proposal(
        strategy_id: str,
        symbol: str,
        raw_klines: list[list[Any]],
        fixed_input_bars: int,
    ) -> HistoryCoverageProposal:
        if (
            strategy_id not in {"N17", "N18", "N19"}
            or type(symbol) is not str
            or not symbol
            or type(raw_klines) is not list
            or type(fixed_input_bars) is not int
            or fixed_input_bars != 122
            or len(raw_klines) < fixed_input_bars
        ):
            raise ValueError("history coverage source identity is invalid")
        source = raw_klines[-fixed_input_bars:]
        open_times: list[int] = []
        for row in source:
            if type(row) is not list or len(row) < 7 or type(row[0]) is bool:
                raise ValueError("history coverage Kline shape is invalid")
            timestamp = Decimal(str(row[0]))
            if (
                not timestamp.is_finite()
                or timestamp != timestamp.to_integral_value()
                or timestamp <= 0
            ):
                raise ValueError("history coverage Kline time is invalid")
            open_times.append(int(timestamp))
        if any(
            later != earlier + 900_000
            for earlier, later in zip(open_times, open_times[1:])
        ):
            raise ValueError("history coverage Kline axis is discontinuous")
        source_sha256 = hashlib.sha256(
            json.dumps(
                # Coverage ends at open_times[-2].  The final row is the
                # still-open Kline and may legitimately evolve between
                # minute scans; it becomes immutable evidence only after the
                # next Kline opens.
                source[:-1],
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        return HistoryCoverageProposal(
            strategy_id,
            symbol,
            open_times[0],
            open_times[-2],
            open_times[-1],
            source_sha256,
        )

    def _evaluate_candidate(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        raw_klines_by_symbol: dict[str, list[list[Any]]],
        checked_at_ms: int | None,
        n12_context: N12BatchContext,
        n13_context: N13BatchContext,
        n14_context: N14BatchContext | None = None,
        n15_context: N15BatchContext | None = None,
        n16_states_by_symbol: Mapping[str, Any] | None = None,
        n16_restore_complete: bool = True,
        n17_states_by_symbol: Mapping[str, Any] | None = None,
        n17_restore_complete: bool = True,
        n19_states_by_symbol: Mapping[str, Any] | None = None,
        n19_restore_complete: bool = True,
        n19_market_symbols: tuple[str, ...] = (),
        n18_states_by_symbol: Mapping[str, Any] | None = None,
        n18_restore_complete: bool = True,
        n20_context: N20BatchContext | None = None,
        history_coverage_proposals: list[HistoryCoverageProposal] | None = None,
        global_cooldowns: Mapping[str, Any] | None = None,
        global_cooldown_scope: frozenset[str] | None = None,
        n19_market_context_cache: dict[int, Any] | None = None,
        history_source_presence: Mapping[str, frozenset[str]] | None = None,
        history_source_presence_complete: bool = True,
        strategy_cooldowns: Mapping[tuple[str, str], str] | None = None,
        strategy_cooldown_scope: frozenset[tuple[str, str]] | None = None,
        n08_history_coverages: list[Any] | None = None,
        n08_states_by_symbol: dict[str, list[Any]] | None = None,
        n13_pending_records: list[dict[str, Any]] | None = None,
        n13_pending_guards: list[tuple[Any, ...]] | None = None,
    ) -> StrategySignalDecision:
        deferred_history_cooldown_reason: str | None = None
        if global_cooldowns is not None:
            if (
                global_cooldown_scope is None
                or candidate.symbol not in global_cooldown_scope
            ):
                return self._rejected(
                    strategy,
                    candidate,
                    "GLOBAL_COOLDOWN_SNAPSHOT_INCOMPLETE",
                )
            global_cooldown = global_cooldowns.get(candidate.symbol)
        else:
            global_cooldown = self.recorder.active_symbol_cooldown(
                candidate.symbol
            )
        if global_cooldown is not None:
            cooldown_reason = (
                "GLOBAL_SYMBOL_COOLDOWN_UNTIL:"
                f"{global_cooldown.cooldown_until}"
            )
            if strategy.strategy_id in HISTORY_COVERAGE_STRATEGY_IDS:
                # N17-N19 coverage and frozen lifecycle evidence are durable
                # market-data responsibilities, not trading eligibility.  A
                # cooldown must still reject the ordinary decision, but only
                # after the fixed 122-row source and any lifecycle transition
                # have been authenticated below.
                deferred_history_cooldown_reason = cooldown_reason
            else:
                return self._rejected(
                    strategy,
                    candidate,
                    cooldown_reason,
                )

        if strategy.market_filter == "negative_funding":
            if strategy.funding_threshold is None or candidate.funding_rate is None:
                return self._rejected(strategy, candidate, "MISSING_FUNDING_RATE")
            if not funding_rate_passes(candidate.funding_rate, strategy.funding_threshold):
                return self._rejected(strategy, candidate, "FUNDING_THRESHOLD_NOT_MET")
        elif strategy.market_filter == "quote_volume_top":
            if candidate.quote_volume_rank is None:
                return self._rejected(strategy, candidate, "MISSING_QUOTE_VOLUME_RANK")
            if strategy.volume_top_n is not None and candidate.quote_volume_rank > strategy.volume_top_n:
                return self._rejected(strategy, candidate, "OUTSIDE_QUOTE_VOLUME_TOP_N")
        else:
            return self._rejected(strategy, candidate, "UNSUPPORTED_MARKET_FILTER")

        if (
            deferred_history_cooldown_reason is None
            and strategy.loss_symbol_cooldown_hours > 0
        ):
            cooldown_key = (strategy.strategy_id, candidate.symbol)
            if strategy_cooldowns is not None:
                if (
                    strategy_cooldown_scope is None
                    or cooldown_key not in strategy_cooldown_scope
                ):
                    return self._rejected(
                        strategy,
                        candidate,
                        "STRATEGY_COOLDOWN_SNAPSHOT_INCOMPLETE",
                    )
                cooldown_until = strategy_cooldowns.get(cooldown_key)
            else:
                cooldown_until = self.recorder.active_strategy_symbol_cooldown(
                    strategy.strategy_id,
                    candidate.symbol,
                    strategy.loss_symbol_cooldown_hours,
                )
            if cooldown_until:
                cooldown_reason = f"SYMBOL_COOLDOWN_UNTIL:{cooldown_until}"
                if strategy.strategy_id in HISTORY_COVERAGE_STRATEGY_IDS:
                    deferred_history_cooldown_reason = cooldown_reason
                else:
                    return self._rejected(
                        strategy,
                        candidate,
                        cooldown_reason,
                    )

        raw_klines = raw_klines_by_symbol.get(candidate.symbol)
        if raw_klines is None:
            return self._rejected(strategy, candidate, "MISSING_KLINES")

        if strategy.evaluator_type == "localized_sell_pressure_decay_reversal":
            if n14_context is None:
                return self._rejected(
                    strategy,
                    candidate,
                    "N14_MARKET_CONTEXT_INSUFFICIENT",
                )
            return self._evaluate_n14_candidate(
                strategy,
                candidate,
                raw_klines,
                checked_at_ms,
                n14_context,
            )

        if strategy.evaluator_type == "bull_market_pullback_relative_strength_recovery":
            if not isinstance(strategy, N20StrategyDefinition) or strategy != N20_STRATEGY:
                return self._rejected(strategy, candidate, "N20_DEFINITION_INVALID")
            if n20_context is None or not n20_context.complete:
                return self._rejected(strategy, candidate, "N20_MARKET_CONTEXT_INSUFFICIENT")
            if n20_context.analysis is None:
                deferred = n20_context.deferred_reasons or {}
                return self._rejected(
                    strategy,
                    candidate,
                    deferred.get(
                        candidate.symbol,
                        "N20_MARKET_CONTEXT_INSUFFICIENT",
                    ),
                )
            analysis = n20_context.analysis.results.get(candidate.symbol)
            if analysis is None:
                return self._rejected(strategy, candidate, "N20_FROZEN_MEMBER_MISSING")
            candidate = replace(candidate, mark_price=analysis.current_price)
            if not analysis.passed:
                return self._rejected(strategy, candidate, analysis.reason, analysis)
            return StrategySignalDecision(
                strategy=strategy, candidate=candidate, analysis=analysis,
                passed=True, decision="PASSED", reason="PASSED",
            )

        if strategy.evaluator_type == "abc_patterns":
            analysis: StrategyAnalysis = analyze_symbol(
                candidate.symbol,
                raw_klines,
                candidate.mark_price,
                self.trend_window,
                allowed_patterns=strategy.allowed_patterns,
            )
            if not analysis.passed:
                return self._rejected(strategy, candidate, "ANALYSIS_NOT_PASSED", analysis)
        elif strategy.evaluator_type in {"double_break_pullback", "double_break_p1_retest"}:
            try:
                current_kline_close = Decimal(str(raw_klines[-1][4]))
            except (ArithmeticError, IndexError, TypeError, ValueError):
                return self._rejected(strategy, candidate, "INVALID_CURRENT_KLINE")
            candidate = replace(candidate, mark_price=current_kline_close)
            if strategy.evaluator_type == "double_break_pullback":
                analysis = analyze_n06_double_break_pullback(
                    candidate.symbol,
                    raw_klines,
                    pivot_left=strategy.pivot_left,
                    pivot_right=strategy.pivot_right,
                    stable_count_required=strategy.stable_candle_count,
                    entry_window_seconds=strategy.entry_window_seconds,
                    checked_at_ms=checked_at_ms,
                    segment_min_bars=strategy.swing_segment_min_bars,
                    segment_atr_period=strategy.swing_segment_atr_period,
                    segment_min_atr_multiple=strategy.swing_segment_min_atr_multiple,
                    segment_min_efficiency=strategy.swing_segment_min_efficiency,
                )
            else:
                analysis = analyze_n07_p1_retest(
                    candidate.symbol,
                    raw_klines,
                    pivot_left=strategy.pivot_left,
                    pivot_right=strategy.pivot_right,
                    distance_min=strategy.entry_distance_min,
                    distance_max=strategy.entry_distance_max,
                    pullback_min=strategy.pullback_min_fraction,
                    segment_min_bars=strategy.swing_segment_min_bars,
                    segment_atr_period=strategy.swing_segment_atr_period,
                    segment_min_atr_multiple=strategy.swing_segment_min_atr_multiple,
                    segment_min_efficiency=strategy.swing_segment_min_efficiency,
                )
            terminal_result = self._apply_n06_n07_terminal_state(
                strategy, candidate, analysis
            )
            if terminal_result is not None:
                return terminal_result
            if not analysis.passed:
                return self._rejected(strategy, candidate, analysis.reason, analysis)
        elif strategy.evaluator_type == "range_five_bullish":
            try:
                current_kline_close = Decimal(str(raw_klines[-1][4]))
            except (ArithmeticError, IndexError, TypeError, ValueError):
                return self._rejected(strategy, candidate, "INVALID_CURRENT_KLINE")
            candidate = replace(candidate, mark_price=current_kline_close)
            coverage_assessment = self._assess_n08_history_coverage(
                strategy,
                candidate,
                raw_klines,
            )
            if coverage_assessment is None:
                return self._rejected(strategy, candidate, "N08_HISTORY_COVERAGE_INVALID")
            if not self._backfill_n08_history(
                strategy,
                candidate,
                raw_klines,
                checked_at_ms,
                n08_states_by_symbol,
            ):
                return self._rejected(strategy, candidate, "N08_HISTORY_BACKFILL_FAILED")
            if n08_history_coverages is None:
                if not self.recorder.upsert_n08_history_coverage(
                    coverage_assessment.coverage
                ):
                    return self._rejected(
                        strategy,
                        candidate,
                        "N08_HISTORY_COVERAGE_PERSIST_FAILED",
                    )
            else:
                n08_history_coverages.append(coverage_assessment.coverage)
            if coverage_assessment.gap_detected:
                self.recorder.record_event(
                    "n08_history_coverage_gap",
                    {
                        "strategy_id": strategy.strategy_id,
                        "continuous_from_open_time": (
                            coverage_assessment.coverage.continuous_from_open_time
                        ),
                        "continuous_until_open_time": (
                            coverage_assessment.coverage.continuous_until_open_time
                        ),
                        "gap_from_open_time": (
                            coverage_assessment.coverage.last_gap_from_open_time
                        ),
                        "gap_to_open_time": coverage_assessment.coverage.last_gap_to_open_time,
                    },
                    candidate.symbol,
                )
            analysis = analyze_n08_range_five_bullish(
                candidate.symbol,
                raw_klines,
                pivot_left=strategy.pivot_left,
                pivot_right=strategy.pivot_right,
                range_min_bars=strategy.range_min_bars,
                range_max_bars=strategy.range_max_bars,
                tolerance_fraction=strategy.range_tolerance_fraction,
                bullish_streak_count=strategy.bullish_streak_count,
                entry_window_seconds=strategy.entry_window_seconds,
                checked_at_ms=checked_at_ms,
            )
            active_n08_states = self._active_n08_states_after_resets(
                strategy,
                candidate,
                raw_klines,
                analysis.structure.start_time if analysis.structure is not None else None,
                n08_states_by_symbol,
            )
            if active_n08_states is None:
                return self._rejected(strategy, candidate, "N08_STATE_PERSIST_FAILED")
            if analysis.structure is not None:
                matching_state = self._matching_n08_state(analysis, active_n08_states)
                if matching_state is not None:
                    reason = (
                        matching_state.reason
                        if matching_state.reason.startswith("HISTORICAL_N08_")
                        else "N08_STRUCTURE_CONSUMED"
                    )
                    return self._rejected(strategy, candidate, reason, analysis)
                if analysis.passed and self._n08_history_context_incomplete(
                    strategy,
                    analysis,
                    raw_klines,
                    coverage_assessment.coverage,
                ):
                    reason = "HISTORICAL_N08_CONTEXT_INCOMPLETE"
                    if not self._consume_n08_structure(
                        strategy,
                        candidate,
                        analysis,
                        reason,
                    ):
                        return self._rejected(
                            strategy,
                            candidate,
                            "N08_STATE_PERSIST_FAILED",
                            analysis,
                        )
                    return self._rejected(strategy, candidate, reason, analysis)
                try:
                    passed_structure = self.recorder.inspect_passed_structure(
                        strategy.strategy_id,
                        candidate.symbol,
                        analysis.structure.structure_id,
                    )
                except Exception as exc:
                    self.logger.warning("Unable to read N08 passed ledger: %s", exc)
                    return self._rejected(
                        strategy, candidate, "N08_STATE_READ_FAILED", analysis
                    )
                if passed_structure == "INCONSISTENT":
                    return self._rejected(
                        strategy, candidate, "N08_STATE_INCONSISTENT", analysis
                    )
                if passed_structure == "CONSUMED":
                    if not self._consume_n08_structure(
                        strategy,
                        candidate,
                        analysis,
                        "LEGACY_DUPLICATE_STRUCTURE",
                    ):
                        return self._rejected(
                            strategy,
                            candidate,
                            "N08_STATE_PERSIST_FAILED",
                            analysis,
                        )
                    return self._rejected(strategy, candidate, "DUPLICATE_STRUCTURE", analysis)
                if analysis.reason != "ENTRY_WINDOW_NOT_OPEN" and not self._consume_n08_structure(
                    strategy,
                    candidate,
                    analysis,
                    analysis.reason,
                ):
                    return self._rejected(
                        strategy,
                        candidate,
                        "N08_STATE_PERSIST_FAILED",
                        analysis,
                    )
            if not analysis.passed:
                return self._rejected(strategy, candidate, analysis.reason, analysis)
        elif strategy.evaluator_type == "slow_decline_half_retrace":
            try:
                current_kline_close = Decimal(str(raw_klines[-1][4]))
            except (ArithmeticError, IndexError, TypeError, ValueError):
                return self._rejected(strategy, candidate, "INVALID_CURRENT_KLINE")
            candidate = replace(candidate, mark_price=current_kline_close)
            analysis = analyze_n09_slow_decline_half_retrace(
                candidate.symbol,
                raw_klines,
                pivot_left=strategy.pivot_left,
                pivot_right=strategy.pivot_right,
                min_decline_bars=strategy.slow_decline_min_bars,
                min_r_squared=strategy.slow_decline_min_r_squared,
                max_bearish_body_fraction=(
                    strategy.slow_decline_max_bearish_body_fraction
                ),
                max_countertrend_rebound_fraction=(
                    strategy.slow_decline_max_rebound_fraction
                ),
                sideways_window=strategy.slow_decline_sideways_window,
                sideways_max_net_drop_fraction=(
                    strategy.slow_decline_sideways_max_net_drop_fraction
                ),
                rebound_max_bars=strategy.slow_decline_rebound_max_bars,
                touch_fraction=strategy.retrace_touch_fraction,
                entry_floor_fraction=strategy.retrace_entry_floor_fraction,
                checked_at_ms=checked_at_ms,
            )
            newly_backfilled_structure_ids: set[str] = set()
            for event in analysis.historical_events:
                try:
                    historical_state = self.recorder.get_n09_structure_state_for_s1(
                        strategy.strategy_id,
                        candidate.symbol,
                        event.structure.s1_time,
                    )
                except Exception as exc:
                    self.logger.warning("Unable to read N09 historical state: %s", exc)
                    return self._rejected(
                        strategy, candidate, "N09_STATE_READ_FAILED", analysis
                    )
                if historical_state is not None:
                    continue
                persisted = self.recorder.record_n09_structure_consumed(
                    strategy_id=strategy.strategy_id,
                    symbol=candidate.symbol,
                    structure_id=event.structure.structure_id,
                    s1_time=event.structure.s1_time,
                    s1_price=str(event.structure.s1),
                    l_time=event.structure.l_time,
                    l_price=str(event.structure.l),
                    first_touch_time=event.first_touch_time,
                    reason=event.reason,
                    detail={"historical_backfill": event.to_jsonable()},
                )
                if not persisted:
                    return self._rejected(
                        strategy,
                        candidate,
                        "N09_STATE_PERSIST_FAILED",
                        analysis,
                    )
                newly_backfilled_structure_ids.add(event.structure.structure_id)
            if analysis.structure is not None:
                try:
                    consumed = self.recorder.get_n09_structure_state_for_s1(
                        strategy.strategy_id,
                        candidate.symbol,
                        analysis.structure.s1_time,
                    )
                except Exception as exc:
                    self.logger.warning("Unable to read N09 structure state: %s", exc)
                    return self._rejected(strategy, candidate, "N09_STATE_READ_FAILED", analysis)
                if (
                    consumed is not None
                    and analysis.structure.structure_id
                    not in newly_backfilled_structure_ids
                ):
                    return self._rejected(strategy, candidate, "STRUCTURE_CONSUMED", analysis)
                if (
                    analysis.touch_observed
                    and analysis.structure.structure_id
                    not in newly_backfilled_structure_ids
                ):
                    persisted = self.recorder.record_n09_structure_consumed(
                        strategy_id=strategy.strategy_id,
                        symbol=candidate.symbol,
                        structure_id=analysis.structure.structure_id,
                        s1_time=analysis.structure.s1_time,
                        s1_price=str(analysis.structure.s1),
                        l_time=analysis.structure.l_time,
                        l_price=str(analysis.structure.l),
                        first_touch_time=analysis.first_touch_time or "",
                        reason=analysis.reason,
                        detail=analysis.detail_json(),
                    )
                    if not persisted:
                        return self._rejected(
                            strategy,
                            candidate,
                            "N09_STATE_PERSIST_FAILED",
                            analysis,
                        )
            if not analysis.passed:
                return self._rejected(strategy, candidate, analysis.reason, analysis)
        elif strategy.evaluator_type == "volume_liquidity_sweep_reclaim":
            try:
                current_kline_close = Decimal(str(raw_klines[-1][4]))
            except (ArithmeticError, IndexError, TypeError, ValueError):
                return self._rejected(strategy, candidate, "N10_KLINE_DATA_INVALID")
            candidate = replace(candidate, mark_price=current_kline_close)
            analysis = analyze_n10_volume_liquidity_sweep_reclaim(
                candidate.symbol,
                raw_klines,
                support_bars=strategy.support_window_bars,
                support_tolerance_fraction=strategy.support_tolerance_fraction,
                support_minimum_gap=strategy.support_touch_min_gap,
                volume_median_bars=strategy.volume_median_bars,
                volume_spike_multiple=strategy.volume_spike_multiple,
                sweep_depth_min=strategy.sweep_depth_min,
                sweep_depth_max=strategy.sweep_depth_max,
                lower_wick_ratio_min=strategy.lower_wick_ratio_min,
                taker_buy_ratio_min=strategy.taker_buy_ratio_min,
                entry_extension_max=strategy.entry_extension_max,
                entry_window_seconds=strategy.entry_window_seconds,
                checked_at_ms=checked_at_ms,
            )
            newly_consumed_ids: set[str] = set()
            for event in analysis.historical_events:
                try:
                    state = self.recorder.get_n10_structure_state(
                        strategy.strategy_id,
                        event.structure.structure_id,
                    )
                except Exception as exc:
                    self.logger.warning("Unable to read N10 historical state: %s", exc)
                    return self._rejected(
                        strategy, candidate, "N10_STATE_READ_FAILED", analysis
                    )
                if state is not None:
                    continue
                structure = event.structure
                if not self.recorder.record_n10_structure_consumed(
                    strategy.strategy_id,
                    candidate.symbol,
                    structure.structure_id,
                    structure.support_start_time,
                    structure.support_end_time,
                    str(structure.support_price),
                    structure.w.open_time,
                    structure.c.open_time,
                    structure.e.open_time,
                    event.reason,
                    {"historical_backfill": event.to_jsonable()},
                ):
                    return self._rejected(
                        strategy, candidate, "N10_STATE_PERSIST_FAILED", analysis
                    )
                newly_consumed_ids.add(structure.structure_id)

            if analysis.structure is not None:
                structure = analysis.structure
                try:
                    state = self.recorder.get_n10_structure_state(
                        strategy.strategy_id,
                        structure.structure_id,
                    )
                except Exception as exc:
                    self.logger.warning("Unable to read N10 structure state: %s", exc)
                    return self._rejected(
                        strategy, candidate, "N10_STATE_READ_FAILED", analysis
                    )
                if state is not None and structure.structure_id not in newly_consumed_ids:
                    return self._rejected(
                        strategy, candidate, "N10_STRUCTURE_CONSUMED", analysis
                    )
                if analysis.consume_current and structure.structure_id not in newly_consumed_ids:
                    if not self.recorder.record_n10_structure_consumed(
                        strategy.strategy_id,
                        candidate.symbol,
                        structure.structure_id,
                        structure.support_start_time,
                        structure.support_end_time,
                        str(structure.support_price),
                        structure.w.open_time,
                        structure.c.open_time,
                        structure.e.open_time,
                        analysis.reason,
                        analysis.detail_json(),
                    ):
                        return self._rejected(
                            strategy,
                            candidate,
                            "N10_STATE_PERSIST_FAILED",
                            analysis,
                        )
            if not analysis.passed:
                return self._rejected(strategy, candidate, analysis.reason, analysis)
        elif strategy.evaluator_type == "volatility_squeeze_breakout_retest":
            try:
                current_kline_close = Decimal(str(raw_klines[-1][4]))
            except (ArithmeticError, IndexError, TypeError, ValueError):
                return self._rejected(strategy, candidate, "N11_KLINE_DATA_INVALID")
            candidate = replace(candidate, mark_price=current_kline_close)
            analysis = analyze_n11_volatility_squeeze_breakout_retest(
                candidate.symbol,
                raw_klines,
                squeeze_bars=strategy.squeeze_bars,
                breakout_lookback_bars=strategy.breakout_lookback_bars,
                bollinger_stddevs=strategy.bollinger_stddevs,
                keltner_atr_multiple=strategy.keltner_atr_multiple,
                breakout_body_atr_min=strategy.breakout_body_atr_min,
                breakout_close_location_min=strategy.breakout_close_location_min,
                volume_median_bars=strategy.volume_median_bars,
                breakout_volume_multiple_min=(
                    strategy.breakout_volume_multiple_min
                ),
                breakout_taker_buy_ratio_min=(
                    strategy.breakout_taker_buy_ratio_min
                ),
                retest_max_bars=strategy.retest_max_bars,
                retest_atr_tolerance=strategy.retest_atr_tolerance,
                retest_close_location_min=strategy.retest_close_location_min,
                retest_volume_ratio_max=strategy.retest_volume_ratio_max,
                entry_extension_atr_max=strategy.entry_extension_atr_max,
                entry_window_seconds=strategy.entry_window_seconds,
                checked_at_ms=checked_at_ms,
            )
            terminal_result = self._apply_n11_terminal_state(
                strategy,
                candidate,
                analysis,
            )
            if terminal_result is not None:
                return terminal_result
            if not analysis.passed:
                return self._rejected(strategy, candidate, analysis.reason, analysis)
        elif strategy.evaluator_type == "relative_strength_first_pullback":
            try:
                current_kline_close = Decimal(str(raw_klines[-1][4]))
            except (ArithmeticError, IndexError, TypeError, ValueError):
                return self._rejected(strategy, candidate, "N12_KLINE_DATA_INVALID")
            candidate = replace(candidate, mark_price=current_kline_close)
            strength = n12_context.current_by_symbol.get(candidate.symbol)
            analysis = analyze_n12_relative_strength_first_pullback(
                candidate.symbol,
                raw_klines,
                relative_strength_rank=strength.rank if strength else None,
                relative_strength_return=strength.return_24h if strength else None,
                historical_strength_by_entry_time=(
                    n12_context.historical_by_symbol.get(candidate.symbol, {})
                ),
                historical_rank_context_complete=(
                    n12_context.historical_complete_times
                ),
                relative_strength_context_complete=(
                    n12_context.current_context_complete
                ),
                relative_strength_top_n=strategy.relative_strength_top_n,
                pivot_left=strategy.pivot_left,
                pivot_right=strategy.pivot_right,
                up_min_bars=strategy.n12_up_min_bars,
                up_min_gain_fraction=strategy.n12_up_min_gain_fraction,
                up_min_atr_multiple=strategy.n12_up_min_atr_multiple,
                up_min_efficiency=strategy.n12_up_min_efficiency,
                up_max_bullish_body_fraction=(
                    strategy.n12_up_max_bullish_body_fraction
                ),
                pullback_min_bars=strategy.n12_pullback_min_bars,
                pullback_max_bars=strategy.n12_pullback_max_bars,
                pullback_depth_min=strategy.n12_pullback_depth_min,
                pullback_depth_max=strategy.n12_pullback_depth_max,
                pullback_close_floor_fraction=(
                    strategy.n12_pullback_close_floor_fraction
                ),
                pullback_volume_ratio_max=strategy.n12_pullback_volume_ratio_max,
                confirmation_close_location_min=(
                    strategy.n12_confirmation_close_location_min
                ),
                confirmation_taker_buy_ratio_min=(
                    strategy.n12_confirmation_taker_buy_ratio_min
                ),
                confirmation_volume_multiple_min=(
                    strategy.n12_confirmation_volume_multiple_min
                ),
                entry_extension_atr_max=strategy.n12_entry_extension_atr_max,
                entry_window_seconds=strategy.entry_window_seconds,
                checked_at_ms=checked_at_ms,
            )
            terminal_result = self._apply_n12_terminal_state(
                strategy, candidate, analysis
            )
            if terminal_result is not None:
                return terminal_result
            if not analysis.passed:
                return self._rejected(strategy, candidate, analysis.reason, analysis)
        elif strategy.evaluator_type == "broad_market_vwap_rotation":
            row = n13_context.rows.get(candidate.symbol)
            analysis = analyze_n13_vwap_rotation(
                candidate.symbol,
                raw_klines,
                return_rank=row.return_rank if row else None,
                return_24h=row.return_24h if row else None,
                positive_return_breadth=n13_context.positive_breadth,
                above_vwap_breadth=n13_context.above_vwap_breadth,
                snapshot_context_complete=n13_context.complete and row is not None,
                checked_at_ms=checked_at_ms or 0,
                entry_window_seconds=strategy.entry_window_seconds,
                positive_breadth_min=strategy.n13_positive_breadth_min,
                above_vwap_breadth_min=strategy.n13_above_vwap_breadth_min,
                return_rank_min=strategy.n13_rank_min,
                return_rank_max=strategy.n13_rank_max,
                vwap_lookback_bars=strategy.n13_vwap_lookback_bars,
                atr_period=strategy.n13_atr_period,
                vwap_slope_lookback_bars=(
                    strategy.n13_vwap_slope_lookback_bars
                ),
                upper_atr_fraction=strategy.n13_upper_atr_fraction,
                lower_atr_fraction=strategy.n13_lower_atr_fraction,
                armed_max_bars=strategy.n13_armed_max_bars,
                confirmation_max_bars=strategy.n13_confirmation_max_bars,
                confirmation_close_location_min=(
                    strategy.n13_confirmation_close_location_min
                ),
                confirmation_taker_buy_ratio_min=(
                    strategy.n13_confirmation_taker_buy_ratio_min
                ),
                entry_extension_atr_max=strategy.n13_entry_extension_atr_max,
            )
            if analysis.structure and analysis.structure.entry:
                candidate = replace(candidate, mark_price=analysis.structure.entry.close)
            terminal_result = self._apply_n13_state(
                strategy,
                candidate,
                analysis,
                pending_records=n13_pending_records,
                pending_guards=n13_pending_guards,
            )
            if terminal_result is not None:
                return terminal_result
            if not analysis.passed:
                return self._rejected(strategy, candidate, analysis.reason, analysis)
        elif strategy.evaluator_type == "market_breadth_recovery_leader":
            context = n15_context or N15BatchContext(None, None, False)
            snapshot = context.snapshot
            analysis = analyze_n15_breadth_recovery_leader(
                candidate.symbol,
                raw_klines,
                snapshot,
                snapshot_context_complete=(
                    context.complete
                    and snapshot is not None
                    and candidate.symbol in snapshot.rows
                ),
                checked_at_ms=checked_at_ms or 0,
                entry_window_seconds=strategy.entry_window_seconds,
                entry_extension_atr_max=strategy.n15_entry_extension_atr_max,
            )
            if analysis.structure is not None:
                candidate = replace(
                    candidate, mark_price=analysis.structure.entry.close
                )
            if (
                analysis.passed
                and candidate.candidate_universe
                == "quote_volume_top_frozen_n15_offboard"
            ):
                # A departed frozen winner remains lifecycle evidence, not an
                # executable current-universe opportunity.  Keep waiting until
                # an authentic terminal condition (most commonly the frozen
                # 120s deadline) can be persisted; never manufacture an entry
                # or consume it as a PASSED trade while it is offboard.
                return self._rejected(
                    strategy,
                    candidate,
                    "N15_OFFBOARD_ENTRY_UNAVAILABLE",
                    analysis,
                )
            terminal_result = self._apply_n15_state(
                strategy, candidate, analysis
            )
            if terminal_result is not None:
                return terminal_result
            if not analysis.passed:
                return self._rejected(
                    strategy, candidate, analysis.reason, analysis
                )
        elif (
            strategy.evaluator_type
            == "mature_trend_dynamic_support_continuation"
        ):
            if (
                not isinstance(strategy, N16StrategyDefinition)
                or strategy != N16_STRATEGY
            ):
                return self._rejected(
                    strategy, candidate, "N16_DEFINITION_INVALID"
                )
            if not n16_restore_complete:
                return self._rejected(
                    strategy, candidate, "N16_STATE_READ_FAILED"
                )
            frozen_state = (
                n16_states_by_symbol.get(candidate.symbol)
                if n16_states_by_symbol is not None
                else None
            )
            analysis = analyze_n16_mature_trend_support(
                candidate.symbol,
                raw_klines,
                quote_volume_rank=candidate.quote_volume_rank,
                frozen_evidence=(
                    frozen_state.evidence_json
                    if frozen_state is not None
                    else None
                ),
                fixed_input_bars=strategy.fixed_input_bars,
                closed_logic_bars=strategy.closed_logic_bars,
                pivot_left=strategy.pivot_left,
                pivot_right=strategy.pivot_right,
                mature_min_bars=strategy.mature_min_bars,
                h2_progress_atr_min=strategy.h2_progress_atr_min,
                ema_fast_period=strategy.ema_fast_period,
                ema_slow_period=strategy.ema_slow_period,
                ema_slope_lookback_bars=strategy.ema_slope_lookback_bars,
                atr_period=strategy.atr_period,
                up_leg_atr_min=strategy.up_leg_atr_min,
                up_leg_efficiency_min=strategy.up_leg_efficiency_min,
                support_min_bars=strategy.support_min_bars,
                support_max_bars=strategy.support_max_bars,
                support_touch_upper_atr=strategy.support_touch_upper_atr,
                support_close_lower_atr=strategy.support_close_lower_atr,
                pullback_depth_min=strategy.pullback_depth_min,
                pullback_depth_max=strategy.pullback_depth_max,
                pullback_volume_ratio_max=strategy.pullback_volume_ratio_max,
                confirmation_max_bars=strategy.confirmation_max_bars,
                confirmation_close_location_min=(
                    strategy.confirmation_close_location_min
                ),
                confirmation_taker_buy_ratio_min=(
                    strategy.confirmation_taker_buy_ratio_min
                ),
                confirmation_volume_multiple_min=(
                    strategy.confirmation_volume_multiple_min
                ),
                entry_extension_atr_max=strategy.entry_extension_atr_max,
                entry_window_seconds=strategy.entry_window_seconds,
                checked_at_ms=checked_at_ms,
            )
            if analysis.structure is not None and analysis.structure.entry is not None:
                candidate = replace(
                    candidate, mark_price=analysis.structure.entry.close
                )
            if analysis.state_record is not None:
                persisted = self.recorder.record_n16_state(
                    analysis.state_record
                )
                if persisted == "N16_EPISODE_CONSUMED":
                    return self._rejected(
                        strategy,
                        candidate,
                        "N16_STRUCTURE_CONSUMED",
                        analysis,
                    )
                if persisted == "N16_STATE_INCONSISTENT":
                    return self._rejected(
                        strategy,
                        candidate,
                        "N16_STATE_INCONSISTENT",
                        analysis,
                    )
                if persisted not in {"INSERTED", "UPDATED", "UNCHANGED"}:
                    return self._rejected(
                        strategy,
                        candidate,
                        "N16_STATE_PERSIST_FAILED",
                        analysis,
                    )
            elif analysis.structure is not None:
                return self._rejected(
                    strategy,
                    candidate,
                    "N16_STATE_INCONSISTENT",
                    analysis,
                )
            if not analysis.passed:
                return self._rejected(
                    strategy, candidate, analysis.reason, analysis
                )
        elif (
            strategy.evaluator_type
            == "range_lower_support_absorption_rebound"
        ):
            if (
                not isinstance(strategy, N17StrategyDefinition)
                or strategy != N17_STRATEGY
            ):
                return self._rejected(
                    strategy, candidate, "N17_DEFINITION_INVALID"
                )
            if not n17_restore_complete:
                return self._rejected(
                    strategy, candidate, "N17_STATE_READ_FAILED"
                )
            if len(raw_klines) < strategy.fixed_input_bars:
                return self._short_history_rejection(
                    strategy,
                    candidate,
                    history_source_presence,
                    history_source_presence_complete,
                )
            active_state = (
                n17_states_by_symbol.get(candidate.symbol)
                if n17_states_by_symbol is not None
                else None
            )
            analysis = analyze_n17_range_support_rebound(
                candidate.symbol,
                raw_klines,
                quote_volume_rank=candidate.quote_volume_rank,
                fixed_input_bars=strategy.fixed_input_bars,
                pivot_left=strategy.pivot_left,
                pivot_right=strategy.pivot_right,
                atr_period=strategy.atr_period,
                box_min_bars=strategy.box_min_bars,
                box_max_bars=strategy.box_max_bars,
                box_tolerance_fraction=strategy.box_tolerance_fraction,
                box_net_move_fraction_max=strategy.box_net_move_fraction_max,
                touch_breakdown_fraction=strategy.touch_breakdown_fraction,
                touch_upper_atr=strategy.touch_upper_atr,
                touch_volume_multiple_max=strategy.touch_volume_multiple_max,
                panic_body_atr_min=strategy.panic_body_atr_min,
                panic_volume_multiple_min=strategy.panic_volume_multiple_min,
                panic_taker_buy_ratio_max=strategy.panic_taker_buy_ratio_max,
                panic_close_location_max=strategy.panic_close_location_max,
                absorption_range_ratio_max=strategy.absorption_range_ratio_max,
                absorption_sell_quote_ratio_max=(
                    strategy.absorption_sell_quote_ratio_max
                ),
                confirmation_max_bars=strategy.confirmation_max_bars,
                confirmation_close_location_min=(
                    strategy.confirmation_close_location_min
                ),
                confirmation_taker_buy_ratio_min=(
                    strategy.confirmation_taker_buy_ratio_min
                ),
                confirmation_volume_multiple_min=(
                    strategy.confirmation_volume_multiple_min
                ),
                n08_bullish_streak_count=strategy.n08_bullish_streak_count,
                entry_extension_atr_max=strategy.entry_extension_atr_max,
                entry_window_seconds=strategy.entry_window_seconds,
                checked_at_ms=checked_at_ms,
                frozen_evidence=(
                    active_state.evidence_json
                    if active_state is not None
                    else None
                ),
            )
            if analysis.structure is not None and analysis.structure.entry is not None:
                candidate = replace(
                    candidate, mark_price=analysis.structure.entry.close
                )
            try:
                coverage_proposal = self._history_coverage_proposal(
                    "N17",
                    candidate.symbol,
                    raw_klines,
                    strategy.fixed_input_bars,
                )
                coverage_status = self.recorder.prepare_history_coverage_proposal(
                    coverage_proposal
                )
            except Exception:
                coverage_status = "READ_FAILED"
                coverage_proposal = None
            if coverage_status == "GAP_BLOCKED":
                return self._rejected(
                    strategy,
                    candidate,
                    "N17_HISTORY_COVERAGE_GAP_BLOCKED",
                    analysis,
                )
            if coverage_status not in {
                "NEW", "NO_CHANGE", "CONTIGUOUS", "GAP"
            }:
                return self._rejected(
                    strategy,
                    candidate,
                    (
                        "N17_HISTORY_COVERAGE_READ_FAILED"
                        if coverage_status == "READ_FAILED"
                        else "N17_HISTORY_COVERAGE_INCONSISTENT"
                    ),
                    analysis,
                )
            if (
                coverage_proposal is None
                or history_coverage_proposals is None
            ):
                return self._rejected(
                    strategy,
                    candidate,
                    "N17_HISTORY_COVERAGE_PERSIST_FAILED",
                    analysis,
                )
            history_coverage_proposals.append(coverage_proposal)
            if (
                active_state is not None
                and active_state.stage
                in {"TOUCH_LOCKED", "CONFIRMING", "CONFIRMED"}
                and analysis.structure_id != active_state.structure_id
            ):
                return self._rejected(
                    strategy, candidate, "N17_FROZEN_EVIDENCE_INVALID", analysis
                )
            if analysis.state_records:
                for state_record in analysis.state_records:
                    persisted = self.recorder.record_n17_state(state_record)
                    if (
                        persisted == "N17_STRUCTURE_CONSUMED"
                        and _same_persisted_state_record(
                            state_record,
                            analysis.state_record,
                        )
                    ):
                        return self._rejected(
                            strategy, candidate, "N17_STRUCTURE_CONSUMED", analysis
                        )
                    if persisted == "N17_STATE_INCONSISTENT":
                        return self._rejected(
                            strategy, candidate, "N17_STATE_INCONSISTENT", analysis
                        )
                    if persisted not in {
                        "INSERTED",
                        "UPDATED",
                        "UNCHANGED",
                        "N17_STRUCTURE_CONSUMED",
                    }:
                        return self._rejected(
                            strategy, candidate, "N17_STATE_PERSIST_FAILED", analysis
                        )
            elif analysis.structure is not None:
                return self._rejected(
                    strategy, candidate, "N17_STATE_INCONSISTENT", analysis
                )
            if (
                not analysis.passed
                and analysis.reason
                in _HISTORY_COVERAGE_COOLDOWN_BLOCKING_REASONS
            ):
                return self._rejected(
                    strategy, candidate, analysis.reason, analysis
                )
            if deferred_history_cooldown_reason is not None:
                return self._rejected(
                    strategy,
                    candidate,
                    deferred_history_cooldown_reason,
                    analysis,
                )
            if not analysis.passed:
                return self._rejected(
                    strategy, candidate, analysis.reason, analysis
                )
        elif (
            strategy.evaluator_type
            == "ascending_triangle_pressure_absorption_breakout"
        ):
            if (
                not isinstance(strategy, N18StrategyDefinition)
                or strategy != N18_STRATEGY
            ):
                return self._rejected(strategy, candidate, "N18_DEFINITION_INVALID")
            if not n18_restore_complete:
                return self._rejected(strategy, candidate, "N18_STATE_READ_FAILED")
            if len(raw_klines) < strategy.fixed_input_bars:
                return self._short_history_rejection(
                    strategy,
                    candidate,
                    history_source_presence,
                    history_source_presence_complete,
                )
            active_state = (
                n18_states_by_symbol.get(candidate.symbol)
                if n18_states_by_symbol is not None else None
            )
            analysis = analyze_n18_ascending_triangle_breakout(
                candidate.symbol,
                raw_klines,
                quote_volume_rank=candidate.quote_volume_rank,
                fixed_input_bars=strategy.fixed_input_bars,
                pivot_left=strategy.pivot_left,
                pivot_right=strategy.pivot_right,
                atr_period=strategy.atr_period,
                ema_fast_period=strategy.ema_fast_period,
                ema_slow_period=strategy.ema_slow_period,
                triangle_span_min_bars=strategy.triangle_span_min_bars,
                triangle_span_max_bars=strategy.triangle_span_max_bars,
                pressure_interval_min_bars=strategy.pressure_interval_min_bars,
                pressure_dispersion_atr_max=strategy.pressure_dispersion_atr_max,
                pressure_dispersion_fraction_max=strategy.pressure_dispersion_fraction_max,
                a_lower_atr=strategy.a_lower_atr,
                a_upper_atr=strategy.a_upper_atr,
                higher_low_progress_atr_min=strategy.higher_low_progress_atr_min,
                initial_height_atr_min=strategy.initial_height_atr_min,
                convergence_ratio_max=strategy.convergence_ratio_max,
                a_close_location_min=strategy.a_close_location_min,
                a_taker_buy_ratio_min=strategy.a_taker_buy_ratio_min,
                a_volume_multiple_min=strategy.a_volume_multiple_min,
                breakout_max_bars=strategy.breakout_max_bars,
                breakout_threshold_atr=strategy.breakout_threshold_atr,
                breakout_body_atr_min=strategy.breakout_body_atr_min,
                breakout_close_location_min=strategy.breakout_close_location_min,
                breakout_taker_buy_ratio_min=strategy.breakout_taker_buy_ratio_min,
                breakout_volume_multiple_min=strategy.breakout_volume_multiple_min,
                entry_extension_atr_max=strategy.entry_extension_atr_max,
                entry_window_seconds=strategy.entry_window_seconds,
                checked_at_ms=checked_at_ms,
                frozen_evidence=(
                    active_state.evidence_json if active_state is not None else None
                ),
            )
            if analysis.structure is not None and analysis.structure.entry is not None:
                candidate = replace(candidate, mark_price=analysis.structure.entry.close)
            try:
                coverage_proposal = self._history_coverage_proposal(
                    "N18",
                    candidate.symbol,
                    raw_klines,
                    strategy.fixed_input_bars,
                )
                coverage_status = self.recorder.prepare_history_coverage_proposal(
                    coverage_proposal
                )
            except Exception:
                coverage_status = "READ_FAILED"
                coverage_proposal = None
            if coverage_status == "GAP_BLOCKED":
                return self._rejected(
                    strategy,
                    candidate,
                    "N18_HISTORY_COVERAGE_GAP_BLOCKED",
                    analysis,
                )
            if coverage_status not in {
                "NEW", "NO_CHANGE", "CONTIGUOUS", "GAP"
            }:
                return self._rejected(
                    strategy,
                    candidate,
                    (
                        "N18_HISTORY_COVERAGE_READ_FAILED"
                        if coverage_status == "READ_FAILED"
                        else "N18_HISTORY_COVERAGE_INCONSISTENT"
                    ),
                    analysis,
                )
            if coverage_proposal is None or history_coverage_proposals is None:
                return self._rejected(
                    strategy,
                    candidate,
                    "N18_HISTORY_COVERAGE_PERSIST_FAILED",
                    analysis,
                )
            history_coverage_proposals.append(coverage_proposal)
            if (
                active_state is not None
                and active_state.stage in {
                    "TRIANGLE_ARMED", "ABSORPTION_LOCKED",
                    "BREAKOUT_PENDING", "CONFIRMED",
                }
                and analysis.state_record is not None
                and analysis.state_record.family_id != active_state.family_id
            ):
                return self._rejected(
                    strategy, candidate, "N18_FROZEN_EVIDENCE_INVALID", analysis
                )
            if analysis.state_records:
                for state_record in analysis.state_records:
                    persisted = self.recorder.record_n18_state(state_record)
                    if (
                        persisted == "N18_STRUCTURE_CONSUMED"
                        and _same_persisted_state_record(
                            state_record,
                            analysis.state_record,
                        )
                    ):
                        return self._rejected(
                            strategy, candidate, "N18_STRUCTURE_CONSUMED", analysis
                        )
                    if persisted == "N18_STATE_INCONSISTENT":
                        return self._rejected(
                            strategy, candidate, "N18_STATE_INCONSISTENT", analysis
                        )
                    if persisted not in {
                        "INSERTED", "UPDATED", "UNCHANGED", "N18_STRUCTURE_CONSUMED",
                    }:
                        return self._rejected(
                            strategy, candidate, "N18_STATE_PERSIST_FAILED", analysis
                        )
            elif analysis.structure is not None:
                return self._rejected(
                    strategy, candidate, "N18_STATE_INCONSISTENT", analysis
                )
            if (
                not analysis.passed
                and analysis.reason
                in _HISTORY_COVERAGE_COOLDOWN_BLOCKING_REASONS
            ):
                return self._rejected(
                    strategy, candidate, analysis.reason, analysis
                )
            if deferred_history_cooldown_reason is not None:
                return self._rejected(
                    strategy,
                    candidate,
                    deferred_history_cooldown_reason,
                    analysis,
                )
            if not analysis.passed:
                return self._rejected(strategy, candidate, analysis.reason, analysis)
        elif (
            strategy.evaluator_type
            == "medium_staircase_decline_exhaustion_reversal"
        ):
            if (
                not isinstance(strategy, N19StrategyDefinition)
                or strategy != N19_STRATEGY
            ):
                return self._rejected(
                    strategy, candidate, "N19_DEFINITION_INVALID"
                )
            if not n19_restore_complete:
                return self._rejected(
                    strategy, candidate, "N19_STATE_READ_FAILED"
                )
            if len(raw_klines) < strategy.fixed_input_bars:
                return self._short_history_rejection(
                    strategy,
                    candidate,
                    history_source_presence,
                    history_source_presence_complete,
                )
            active_state = (
                n19_states_by_symbol.get(candidate.symbol)
                if n19_states_by_symbol is not None
                else None
            )
            analysis = analyze_n19_staircase_exhaustion_reversal(
                candidate.symbol,
                raw_klines,
                quote_volume_rank=candidate.quote_volume_rank,
                market_symbols=n19_market_symbols,
                market_klines_by_symbol=raw_klines_by_symbol,
                fixed_input_bars=strategy.fixed_input_bars,
                pivot_left=strategy.pivot_left,
                pivot_right=strategy.pivot_right,
                atr_period=strategy.atr_period,
                structure_min_bars=strategy.structure_min_bars,
                structure_max_bars=strategy.structure_max_bars,
                total_drop_atr_min=strategy.total_drop_atr_min,
                total_drop_atr_max=strategy.total_drop_atr_max,
                lower_low_progress_atr_min=strategy.lower_low_progress_atr_min,
                exhaustion_extension_atr_max=strategy.exhaustion_extension_atr_max,
                lower_high_progress_atr_min=strategy.lower_high_progress_atr_min,
                rebound_ratio_min=strategy.rebound_ratio_min,
                rebound_ratio_max=strategy.rebound_ratio_max,
                max_bearish_body_drop_fraction=(
                    strategy.max_bearish_body_drop_fraction
                ),
                exhaustion_range_atr_max=strategy.exhaustion_range_atr_max,
                exhaustion_volume_ratio_max=strategy.exhaustion_volume_ratio_max,
                exhaustion_taker_buy_ratio_min=(
                    strategy.exhaustion_taker_buy_ratio_min
                ),
                exhaustion_taker_buy_improvement_min=(
                    strategy.exhaustion_taker_buy_improvement_min
                ),
                confirmation_max_bars=strategy.confirmation_max_bars,
                confirmation_close_location_min=(
                    strategy.confirmation_close_location_min
                ),
                confirmation_taker_buy_ratio_min=(
                    strategy.confirmation_taker_buy_ratio_min
                ),
                confirmation_volume_multiple_min=(
                    strategy.confirmation_volume_multiple_min
                ),
                crash_down_breadth_min=strategy.crash_down_breadth_min,
                crash_median_return_max=strategy.crash_median_return_max,
                entry_extension_atr_max=strategy.entry_extension_atr_max,
                entry_window_seconds=strategy.entry_window_seconds,
                checked_at_ms=checked_at_ms,
                frozen_evidence=(
                    active_state.evidence_json
                    if active_state is not None
                    else None
                ),
                market_context_cache=n19_market_context_cache,
            )
            if analysis.structure is not None and analysis.structure.entry is not None:
                candidate = replace(
                    candidate, mark_price=analysis.structure.entry.close
                )
            try:
                coverage_proposal = self._history_coverage_proposal(
                    "N19",
                    candidate.symbol,
                    raw_klines,
                    strategy.fixed_input_bars,
                )
                if (
                    active_state is not None
                    and active_state.stage == "CONFIRMED"
                    and analysis.state_record is not None
                    and analysis.state_record.stage == "MISSED"
                    and analysis.state_record.reason
                    == "N19_HISTORICAL_ENTRY_MISSED"
                    and analysis.state_record.family_id
                    == active_state.family_id
                ):
                    coverage_proposal = replace(
                        coverage_proposal,
                        n19_terminal_family_id=(
                            analysis.state_record.family_id
                        ),
                        n19_terminal_structure_id=(
                            analysis.state_record.structure_id
                        ),
                        n19_terminal_evidence_sha256=(
                            analysis.state_record.evidence_sha256
                        ),
                        n19_terminal_state_record=analysis.state_record,
                    )
                coverage_status = self.recorder.prepare_history_coverage_proposal(
                    coverage_proposal
                )
            except Exception:
                coverage_status = "READ_FAILED"
                coverage_proposal = None
            if coverage_status == "GAP_BLOCKED":
                return self._rejected(
                    strategy,
                    candidate,
                    "N19_HISTORY_COVERAGE_GAP_BLOCKED",
                    analysis,
                )
            if coverage_status not in {
                "NEW", "NO_CHANGE", "CONTIGUOUS", "GAP"
            }:
                return self._rejected(
                    strategy,
                    candidate,
                    (
                        "N19_HISTORY_COVERAGE_READ_FAILED"
                        if coverage_status == "READ_FAILED"
                        else "N19_HISTORY_COVERAGE_INCONSISTENT"
                    ),
                    analysis,
                )
            if coverage_proposal is None or history_coverage_proposals is None:
                return self._rejected(
                    strategy,
                    candidate,
                    "N19_HISTORY_COVERAGE_PERSIST_FAILED",
                    analysis,
                )
            history_coverage_proposals.append(coverage_proposal)
            if (
                active_state is not None
                and active_state.stage in {
                    "EXHAUSTION_LOCKED", "CONFIRMATION_PENDING", "CONFIRMED"
                }
                and analysis.state_record is not None
                and analysis.state_record.family_id != active_state.family_id
            ):
                return self._rejected(
                    strategy, candidate, "N19_FROZEN_EVIDENCE_INVALID", analysis
                )
            if analysis.state_records:
                for state_record in analysis.state_records:
                    if (
                        coverage_proposal.n19_terminal_state_record is not None
                        and type(
                            coverage_proposal.n19_terminal_state_record
                        ) is type(state_record)
                        and coverage_proposal.n19_terminal_state_record
                        == state_record
                        and coverage_proposal.n19_terminal_family_id
                        == state_record.family_id
                        and coverage_proposal.n19_terminal_structure_id
                        == state_record.structure_id
                        and coverage_proposal.n19_terminal_evidence_sha256
                        == state_record.evidence_sha256
                    ):
                        # This exact-value transition is committed with its
                        # coverage receipt and CURRENT pointer.  It may be a
                        # separately reconstructed but equal frozen record.
                        continue
                    persisted = self.recorder.record_n19_state(state_record)
                    if (
                        persisted == "N19_STRUCTURE_CONSUMED"
                        and _same_persisted_state_record(
                            state_record,
                            analysis.state_record,
                        )
                    ):
                        return self._rejected(
                            strategy, candidate, "N19_STRUCTURE_CONSUMED", analysis
                        )
                    if persisted == "N19_STATE_INCONSISTENT":
                        return self._rejected(
                            strategy, candidate, "N19_STATE_INCONSISTENT", analysis
                        )
                    if persisted not in {
                        "INSERTED", "UPDATED", "UNCHANGED",
                        "N19_STRUCTURE_CONSUMED",
                    }:
                        return self._rejected(
                            strategy, candidate, "N19_STATE_PERSIST_FAILED", analysis
                        )
            elif analysis.structure is not None:
                return self._rejected(
                    strategy, candidate, "N19_STATE_INCONSISTENT", analysis
                )
            if (
                not analysis.passed
                and analysis.reason
                in _HISTORY_COVERAGE_COOLDOWN_BLOCKING_REASONS
            ):
                return self._rejected(
                    strategy, candidate, analysis.reason, analysis
                )
            if deferred_history_cooldown_reason is not None:
                return self._rejected(
                    strategy,
                    candidate,
                    deferred_history_cooldown_reason,
                    analysis,
                )
            if not analysis.passed:
                return self._rejected(
                    strategy, candidate, analysis.reason, analysis
                )
        else:
            return self._rejected(strategy, candidate, "UNSUPPORTED_EVALUATOR")

        return StrategySignalDecision(
            strategy=strategy,
            candidate=candidate,
            analysis=analysis,
            passed=True,
            decision="PASSED",
            reason="PASSED",
        )

    def _build_n12_batch_context(
        self,
        candidate_groups: Mapping[str, list[FundingCandidate]],
        raw_klines_by_symbol: dict[str, list[list[Any]]],
        checked_at_ms: int | None,
    ) -> N12BatchContext:
        n12_strategy = next((
            strategy
            for strategy in self.strategies
            if strategy.evaluator_type == "relative_strength_first_pullback"
        ), None)
        if n12_strategy is None:
            return N12BatchContext({}, {}, set(), False)
        candidates = [
            candidate
            for candidate in candidate_groups.get("quote_volume_top", [])
            if candidate.quote_volume_rank is not None
            and candidate.quote_volume_rank <= 100
        ]
        if not candidates or checked_at_ms is None:
            return N12BatchContext({}, {}, set(), False)
        parsed: dict[str, list[Any]] = {}
        for candidate in candidates:
            raw = raw_klines_by_symbol.get(candidate.symbol)
            if raw is None:
                return N12BatchContext({}, {}, set(), False)
            try:
                parsed[candidate.symbol] = parse_n12_klines(raw)
            except (ArithmeticError, TypeError, ValueError):
                return N12BatchContext({}, {}, set(), False)
        if any(len(candles) < 97 for candles in parsed.values()):
            return N12BatchContext({}, {}, set(), False)
        if any(
            any(
                right.open_time_ms - left.open_time_ms != FIFTEEN_MINUTES_MS
                for left, right in zip(candles, candles[1:])
            )
            for candles in parsed.values()
        ):
            return N12BatchContext({}, {}, set(), False)
        current_times = {candles[-1].open_time_ms for candles in parsed.values()}
        last_closed_times = {candles[-2].open_time_ms for candles in parsed.values()}
        if len(current_times) != 1 or len(last_closed_times) != 1:
            return N12BatchContext({}, {}, set(), False)
        current_time = next(iter(current_times))
        last_closed_time = next(iter(last_closed_times))
        if (
            last_closed_time + FIFTEEN_MINUTES_MS != current_time
            or checked_at_ms < current_time
            or checked_at_ms >= current_time + FIFTEEN_MINUTES_MS
        ):
            return N12BatchContext({}, {}, set(), False)

        def ranked(rows: list[tuple[FundingCandidate, Decimal]]) -> dict[str, RelativeStrength]:
            positive = [(candidate, value) for candidate, value in rows if value > 0]
            positive.sort(key=lambda item: (
                -item[1],
                item[0].quote_volume_rank or 10**9,
                item[0].symbol,
            ))
            return {
                candidate.symbol: RelativeStrength(candidate.symbol, value, rank)
                for rank, (candidate, value) in enumerate(positive, start=1)
            }

        def load_snapshot(payload: dict[str, Any]) -> dict[str, RelativeStrength]:
            try:
                snapshot_time = payload["current_open_time_ms"]
                if (
                    isinstance(snapshot_time, bool)
                    or not isinstance(snapshot_time, int)
                    or snapshot_time != current_time
                ):
                    raise ValueError("N12 rank snapshot time mismatch")
                snapshot_rows = payload["rows"]
                if (
                    not isinstance(snapshot_rows, list)
                    or not 1 <= len(snapshot_rows) <= 100
                ):
                    raise ValueError("N12 rank snapshot rows are invalid")
                decoded: list[tuple[str, Decimal, int, int | None]] = []
                seen: set[str] = set()
                seen_quote_ranks: set[int] = set()
                for row in snapshot_rows:
                    if not isinstance(row, dict):
                        raise ValueError("N12 rank snapshot row is invalid")
                    symbol = row["symbol"]
                    value = Decimal(str(row["return_24h"]))
                    quote_rank = row["quote_volume_rank"]
                    rank_value = row.get("rank")
                    if (
                        not isinstance(symbol, str)
                        or not symbol
                        or symbol in seen
                        or not value.is_finite()
                        or isinstance(quote_rank, bool)
                        or not isinstance(quote_rank, int)
                        or not 1 <= quote_rank <= 100
                        or quote_rank in seen_quote_ranks
                        or (
                            rank_value is not None
                            and (
                                isinstance(rank_value, bool)
                                or not isinstance(rank_value, int)
                                or rank_value < 1
                            )
                        )
                    ):
                        raise ValueError("N12 rank snapshot row values are invalid")
                    seen.add(symbol)
                    seen_quote_ranks.add(quote_rank)
                    decoded.append((symbol, value, quote_rank, rank_value))
                expected = sorted(
                    (row for row in decoded if row[1] > 0),
                    key=lambda row: (-row[1], row[2], row[0]),
                )
                expected_ranks = {
                    row[0]: rank for rank, row in enumerate(expected, start=1)
                }
                if any(
                    rank_value != expected_ranks.get(symbol)
                    for symbol, _, _, rank_value in decoded
                ):
                    raise ValueError("N12 rank snapshot ranks are inconsistent")
                return {
                    symbol: RelativeStrength(symbol, value, rank_value)
                    for symbol, value, _, rank_value in decoded
                    if rank_value is not None
                }
            except (ArithmeticError, KeyError, TypeError, ValueError):
                raise ValueError("N12 rank snapshot payload is invalid")

        try:
            snapshot = self.recorder.get_n12_rank_snapshot(
                n12_strategy.strategy_id,
                str(current_time),
            )
        except Exception as exc:
            self.logger.warning("Unable to read N12 rank snapshot: %s", exc)
            return N12BatchContext({}, {}, set(), False)
        if snapshot is not None:
            try:
                current_by_symbol = load_snapshot(snapshot)
            except ValueError as exc:
                self.logger.warning("Unable to parse N12 rank snapshot: %s", exc)
                return N12BatchContext({}, {}, set(), False)
        else:
            current_rows: list[tuple[FundingCandidate, Decimal]] = []
            for candidate in candidates:
                try:
                    value = calculate_relative_strength_return(
                        raw_klines_by_symbol[candidate.symbol], 96
                    )
                except (ArithmeticError, TypeError, ValueError):
                    return N12BatchContext({}, {}, set(), False)
                current_rows.append((candidate, value))
            current_by_symbol = ranked(current_rows)
            snapshot_payload = {
                "current_open_time_ms": current_time,
                "rows": [
                    {
                        "symbol": candidate.symbol,
                        "return_24h": str(value),
                        "quote_volume_rank": candidate.quote_volume_rank,
                        "rank": (
                            current_by_symbol[candidate.symbol].rank
                            if candidate.symbol in current_by_symbol
                            else None
                        ),
                    }
                    for candidate, value in current_rows
                ],
            }
            if not self.recorder.record_n12_rank_snapshot(
                n12_strategy.strategy_id,
                str(current_time),
                snapshot_payload,
            ):
                return N12BatchContext({}, {}, set(), False)

        indexes_by_symbol = {
            symbol: {candle.open_time_ms: index for index, candle in enumerate(candles)}
            for symbol, candles in parsed.items()
        }
        all_times = sorted({
            candle.open_time_ms
            for candles in parsed.values()
            for candle in candles[96:]
        })
        historical_by_symbol: dict[str, dict[int, RelativeStrength]] = {
            candidate.symbol: {} for candidate in candidates
        }
        complete_times: set[int] = set()
        for entry_time in all_times:
            rows: list[tuple[FundingCandidate, Decimal]] = []
            complete = True
            for candidate in candidates:
                index = indexes_by_symbol[candidate.symbol].get(entry_time)
                candles = parsed[candidate.symbol]
                if index is None or index < 96:
                    complete = False
                    break
                window = candles[index - 96:index]
                if (
                    len(window) != 96
                    or window[0].open_time_ms
                    != entry_time - 96 * FIFTEEN_MINUTES_MS
                    or window[-1].open_time_ms
                    != entry_time - FIFTEEN_MINUTES_MS
                    or any(
                        right.open_time_ms - left.open_time_ms
                        != FIFTEEN_MINUTES_MS
                        for left, right in zip(window, window[1:])
                    )
                ):
                    complete = False
                    break
                rows.append((
                    candidate,
                    window[-1].close / window[0].open - Decimal("1"),
                ))
            if not complete:
                continue
            complete_times.add(entry_time)
            for symbol, strength in ranked(rows).items():
                historical_by_symbol[symbol][entry_time] = strength
        return N12BatchContext(
            current_by_symbol,
            historical_by_symbol,
            complete_times,
            True,
        )

    def _build_n13_batch_context(
        self,
        candidate_groups: Mapping[str, list[FundingCandidate]],
        raw_by_symbol: dict[str, list[list[Any]]],
        checked_at_ms: int | None,
    ) -> N13BatchContext:
        strategy = next((s for s in self.strategies if s.evaluator_type == "broad_market_vwap_rotation"), None)
        if strategy is None:
            return N13BatchContext({}, None, None, False)
        candidates = []
        for candidate in candidate_groups.get("quote_volume_top", []):
            rank = candidate.quote_volume_rank
            if (
                isinstance(rank, int)
                and not isinstance(rank, bool)
                and 1 <= rank <= 100
            ):
                candidates.append(candidate)
        symbols = [candidate.symbol for candidate in candidates]
        quote_volume_ranks = [candidate.quote_volume_rank for candidate in candidates]
        if (
            checked_at_ms is None
            or len(candidates) != 100
            or any(
                not isinstance(symbol, str) or not symbol
                for symbol in symbols
            )
            or len(set(symbols)) != 100
            or set(quote_volume_ranks) != set(range(1, 101))
        ):
            return N13BatchContext({}, None, None, False)
        parsed = {}
        try:
            for candidate in candidates:
                parsed[candidate.symbol] = parse_n13_klines(raw_by_symbol[candidate.symbol])
            minimum_closed_bars = max(
                strategy.n13_vwap_lookback_bars
                + strategy.n13_vwap_slope_lookback_bars,
                strategy.n13_atr_period,
            )
            if any(
                len(items) < minimum_closed_bars + 1
                for items in parsed.values()
            ):
                raise ValueError
            current_times = {items[-1].open_time_ms for items in parsed.values()}
            closed_times = {items[-2].open_time_ms for items in parsed.values()}
            if len(current_times) != 1 or len(closed_times) != 1:
                raise ValueError
            current_time = next(iter(current_times))
            if (
                next(iter(closed_times)) + 900_000 != current_time
                or checked_at_ms >= current_time + 900_000
            ):
                raise ValueError
        except (KeyError, ArithmeticError, TypeError, ValueError):
            return N13BatchContext({}, None, None, False)

        def decode(payload):
            payload_time = payload.get("current_open_time_ms")
            payload_rows = payload.get("rows")
            expected_signature = _n13_snapshot_config_signature(strategy)
            if (
                type(payload.get("snapshot_schema_version")) is not int
                or payload.get("snapshot_schema_version") != 1
                or not _n13_exact_json_value(
                    payload.get("config_signature"), expected_signature
                )
                or type(payload_time) is not int
                or payload_time != current_time
                or not isinstance(payload_rows, list)
                or len(payload_rows) != 100
            ):
                raise ValueError
            rows = {}
            quote_ranks = set()
            return_ranks = set()
            for item in payload_rows:
                if not isinstance(item, dict):
                    raise ValueError
                symbol = item.get("symbol")
                quote_rank = item.get("quote_volume_rank")
                rank = item.get("return_rank")
                above = item.get("close_above_vwap")
                if (
                    type(symbol) is not str
                    or not symbol
                    or symbol in rows
                    or type(quote_rank) is not int
                    or not 1 <= quote_rank <= 100
                    or quote_rank in quote_ranks
                    or type(rank) is not int
                    or not 1 <= rank <= 100
                    or rank in return_ranks
                    or type(above) is not bool
                ):
                    raise ValueError
                quote_ranks.add(quote_rank)
                return_ranks.add(rank)
                return_24h = _d13(item["return_24h"])
                vwap_c = _d13(item["vwap_c"])
                vwap_slope_reference = _d13(item["vwap_slope_reference"])
                atr_c = _d13(item["atr_c"])
                if vwap_c <= 0 or vwap_slope_reference <= 0 or atr_c <= 0:
                    raise ValueError
                rows[symbol] = N13MarketRow(
                    return_24h,
                    rank,
                    quote_rank,
                    vwap_c,
                    vwap_slope_reference,
                    atr_c,
                    above,
                )
            if quote_ranks != set(range(1, 101)) or return_ranks != set(range(1, 101)):
                raise ValueError
            pb, ab = _d13(payload["positive_return_breadth"]), _d13(payload["above_vwap_breadth"])
            if not Decimal("0") <= pb <= Decimal("1") or not Decimal("0") <= ab <= Decimal("1"):
                raise ValueError
            expected = sorted(
                rows.items(),
                key=lambda item: (
                    -item[1].return_24h,
                    next(
                        row["quote_volume_rank"]
                        for row in payload_rows
                        if row["symbol"] == item[0]
                    ),
                    item[0],
                ),
            )
            if any(rows[symbol].return_rank != index for index, (symbol, _) in enumerate(expected, 1)):
                raise ValueError
            expected_positive_breadth = (
                Decimal(sum(1 for row in rows.values() if row.return_24h > 0))
                / Decimal("100")
            )
            expected_above_vwap_breadth = (
                Decimal(sum(1 for row in rows.values() if row.close_above_vwap))
                / Decimal("100")
            )
            if pb != expected_positive_breadth or ab != expected_above_vwap_breadth:
                raise ValueError
            return rows, pb, ab

        try:
            snapshot = self.recorder.get_n13_market_snapshot(strategy.strategy_id, str(current_time))
            if snapshot is not None:
                rows, pb, ab = decode(snapshot)
                # The frozen membership is authoritative for the whole current
                # 15m candle.  If an old member dropped out of the live Top100,
                # it may only be evaluated from an already-shared Kline payload;
                # otherwise the entire N13 cross-section is incomplete.
                for symbol in set(rows) - set(parsed):
                    frozen_raw = raw_by_symbol.get(symbol)
                    if frozen_raw is None:
                        raise ValueError
                    frozen = parse_n13_klines(frozen_raw)
                    if (
                        len(frozen) < minimum_closed_bars + 1
                        or frozen[-1].open_time_ms != current_time
                        or frozen[-2].open_time_ms + FIFTEEN_MINUTES_MS
                        != current_time
                    ):
                        raise ValueError
                return N13BatchContext(rows, pb, ab, True, current_time)
        except Exception:
            return N13BatchContext({}, None, None, False)

        calculated = []
        try:
            for candidate in candidates:
                closed = parsed[candidate.symbol][:-1]
                metrics = n13_metrics(
                    closed,
                    vwap_lookback_bars=strategy.n13_vwap_lookback_bars,
                    atr_period=strategy.n13_atr_period,
                    upper_atr_fraction=strategy.n13_upper_atr_fraction,
                    lower_atr_fraction=strategy.n13_lower_atr_fraction,
                )
                last = len(closed) - 1
                metric = metrics[last]
                old = metrics[last - strategy.n13_vwap_slope_lookback_bars]
                ret = (
                    closed[-1].close
                    / closed[-strategy.n13_vwap_lookback_bars].open
                    - Decimal("1")
                )
                calculated.append(
                    (
                        candidate,
                        ret,
                        metric,
                        old,
                        closed[-1].close > metric.vwap,
                    )
                )
            ordered = sorted(
                calculated,
                key=lambda item: (
                    -item[1],
                    item[0].quote_volume_rank,
                    item[0].symbol,
                ),
            )
            rank_map = {
                item[0].symbol: index
                for index, item in enumerate(ordered, 1)
            }
            count = Decimal(len(calculated))
            pb = Decimal(sum(1 for item in calculated if item[1] > 0)) / count
            ab = Decimal(sum(1 for item in calculated if item[4])) / count
            payload = {
                "snapshot_schema_version": 1,
                "config_signature": _n13_snapshot_config_signature(strategy),
                "current_open_time_ms": current_time,
                "positive_return_breadth": str(pb),
                "above_vwap_breadth": str(ab),
                "rows": [
                    {
                        "symbol": candidate.symbol,
                        "quote_volume_rank": candidate.quote_volume_rank,
                        "return_24h": str(ret),
                        "return_rank": rank_map[candidate.symbol],
                        "vwap_c": str(metric.vwap),
                        "vwap_slope_reference": str(old.vwap),
                        "atr_c": str(metric.atr),
                        "close_above_vwap": above,
                    }
                    for candidate, ret, metric, old, above in calculated
                ],
            }
            if not self.recorder.record_n13_market_snapshot(
                strategy.strategy_id,
                str(current_time),
                payload,
            ):
                raise ValueError
            rows, pb, ab = decode(payload)
            return N13BatchContext(rows, pb, ab, True, current_time)
        except Exception:
            return N13BatchContext({}, None, None, False)

    def _quarantine_n15_snapshot(
        self,
        strategy_id: str,
        e_time: int,
        reason: str,
    ) -> bool:
        deleted = self.recorder.delete_n15_market_snapshot(
            strategy_id, str(e_time)
        )
        self.recorder.record_event(
            (
                "n15_snapshot_quarantined"
                if deleted else "n15_snapshot_quarantine_failed"
            ),
            {
                "strategy_id": strategy_id,
                "e_time": str(e_time),
                "reason": reason,
                "retained": not deleted,
            },
        )
        return deleted

    def _build_n20_batch_context(
        self,
        candidate_groups: Mapping[str, list[FundingCandidate]],
        raw_by_symbol: dict[str, list[list[Any]]],
        checked_at_ms: int | None,
        history_source_presence: Mapping[str, frozenset[str]] | None = None,
        history_source_presence_complete: bool = True,
    ) -> N20BatchContext:
        strategy = next((
            item for item in self.strategies
            if item.evaluator_type == "bull_market_pullback_relative_strength_recovery"
        ), None)
        if strategy is None:
            return N20BatchContext(None, True)
        if not isinstance(strategy, N20StrategyDefinition) or strategy != N20_STRATEGY or checked_at_ms is None:
            return N20BatchContext(None, False, "N20_CONTEXT_IDENTITY_INVALID")
        candidates = sorted(
            candidate_groups.get("quote_volume_top", []),
            key=lambda item: (
                item.quote_volume_rank if item.quote_volume_rank is not None else 10**9,
                item.symbol,
            ),
        )
        if (
            len(candidates) != 100
            or len({item.symbol for item in candidates}) != 100
            or {item.quote_volume_rank for item in candidates} != set(range(1, 101))
        ):
            return N20BatchContext(None, False, "N20_TOP100_CONTEXT_INCOMPLETE")
        try:
            active = self.recorder.get_active_n20_episode()
            short_symbols = {
                item.symbol
                for item in candidates
                if type(raw_by_symbol.get(item.symbol)) is list
                and len(raw_by_symbol[item.symbol]) < strategy.fixed_input_bars
            }
            if short_symbols and active is None:
                if not history_source_presence_complete:
                    return N20BatchContext(
                        None, False, "N20_HISTORY_COVERAGE_READ_FAILED"
                    )
                presence = history_source_presence or {}
                new_symbols = {
                    symbol for symbol in short_symbols
                    if presence.get(symbol, frozenset()) == frozenset()
                }
                if new_symbols != short_symbols:
                    return N20BatchContext(
                        None, False, "N20_MARKET_CONTEXT_INSUFFICIENT"
                    )
                return N20BatchContext(
                    None,
                    True,
                    deferred_reasons={
                        item.symbol: (
                            "N20_HISTORY_SOURCE_INSUFFICIENT"
                            if item.symbol in new_symbols
                            else "N20_MARKET_CONTEXT_DEFERRED_NEW_MEMBER"
                        )
                        for item in candidates
                    },
                )
            analysis = analyze_n20_market_episode(
                [(item.symbol, item.quote_volume_rank) for item in candidates],
                raw_by_symbol,
                checked_at_ms=checked_at_ms,
                fixed_input_bars=strategy.fixed_input_bars,
                atr_period=strategy.atr_period,
                vwap_long_period=strategy.vwap_long_period,
                vwap_short_period=strategy.vwap_short_period,
                baseline_rank_max=strategy.baseline_rank_max,
                resilience_rank_max=strategy.resilience_rank_max,
                m0_positive_breadth_min=strategy.m0_positive_breadth_min,
                m0_above_vwap_breadth_min=strategy.m0_above_vwap_breadth_min,
                d1_down_breadth_min=strategy.d1_down_breadth_min,
                d1_median_return_max=strategy.d1_median_return_max,
                pullback_min_bars=strategy.pullback_min_bars,
                pullback_max_bars=strategy.pullback_max_bars,
                pullback_depth_min=strategy.pullback_depth_min,
                pullback_depth_max=strategy.pullback_depth_max,
                crash_down_breadth_min=strategy.crash_down_breadth_min,
                crash_depth_min=strategy.crash_depth_min,
                recovery_up_breadth_min=strategy.recovery_up_breadth_min,
                recovery_breadth_improvement_min=strategy.recovery_breadth_improvement_min,
                relative_resilience_min=strategy.relative_resilience_min,
                candidate_drawdown_atr_max=strategy.candidate_drawdown_atr_max,
                candidate_recovery_ratio_min=strategy.candidate_recovery_ratio_min,
                candidate_close_location_min=strategy.candidate_close_location_min,
                candidate_taker_buy_ratio_min=strategy.candidate_taker_buy_ratio_min,
                candidate_volume_multiple_min=strategy.candidate_volume_multiple_min,
                entry_extension_atr_max=strategy.entry_extension_atr_max,
                entry_window_seconds=strategy.entry_window_seconds,
                frozen_blob=active.evidence_blob if active is not None else None,
                frozen_size=active.evidence_size if active is not None else None,
                frozen_sha256=active.evidence_sha256 if active is not None else None,
            )
            if not analysis.complete:
                return N20BatchContext(
                    analysis,
                    False,
                    analysis.reason
                    if type(analysis.reason) is str and analysis.reason
                    else "N20_MARKET_CONTEXT_INSUFFICIENT",
                )
            if analysis.state_record is not None:
                saved = self.recorder.record_n20_episode(analysis.state_record)
                if saved == "N20_EPISODE_CONSUMED":
                    if analysis.state_record.stage == "CONFIRMED":
                        sealed = terminalize_consumed_n20_state(
                            analysis.state_record
                        )
                    else:
                        latest = self.recorder.get_latest_n20_episode()
                        if (
                            latest is None
                            or latest.episode_id
                            != analysis.state_record.episode_id
                            or latest.stage != "CONSUMED"
                            or latest.reason != "N20_EPISODE_CONSUMED"
                            or (
                                analysis.state_record.structure_id is not None
                                and latest.structure_id
                                != analysis.state_record.structure_id
                            )
                        ):
                            return N20BatchContext(
                                analysis, False, "N20_STATE_INCONSISTENT"
                            )
                        sealed = decode_n20_state_evidence(
                            latest.evidence_blob,
                            latest.evidence_size,
                            latest.evidence_sha256,
                        )
                    sealed_result = self.recorder.record_n20_episode(sealed)
                    if sealed_result not in {
                        "UPDATED", "UNCHANGED", "N20_EPISODE_CONSUMED",
                    }:
                        return N20BatchContext(
                            analysis, False, "N20_STATE_PERSIST_FAILED"
                        )
                    analysis = replace(
                        analysis,
                        reason="N20_EPISODE_CONSUMED",
                        state_record=sealed,
                        results={
                            symbol: replace(
                                result, passed=False,
                                reason="N20_EPISODE_CONSUMED",
                                state_record=sealed,
                            )
                            for symbol, result in analysis.results.items()
                        },
                    )
                elif saved not in {"INSERTED", "UPDATED", "UNCHANGED"}:
                    return N20BatchContext(
                        analysis, False, "N20_STATE_PERSIST_FAILED"
                    )
            return N20BatchContext(analysis, True)
        except Exception as exc:
            self.logger.warning("Unable to freeze/restore N20 market episode: %s", exc)
            return N20BatchContext(None, False, "N20_CONTEXT_EXCEPTION")

    @staticmethod
    def _n20_candidates_for_frozen_episode(
        current_candidates: list[FundingCandidate],
        analysis: N20MarketAnalysis,
        raw_by_symbol: dict[str, list[list[Any]]],
    ) -> list[FundingCandidate]:
        current_by_symbol = {item.symbol: item for item in current_candidates}
        # The frozen hundred remains the authoritative episode universe and is
        # evaluated in full.  Current Top100 members are also retained so the
        # ordinary publication boundary remains exactly the current hundred;
        # members not belonging to the frozen episode deterministically reject
        # as N20_FROZEN_MEMBER_MISSING and can never replace its winner.
        result: dict[str, FundingCandidate] = dict(current_by_symbol)
        rank_by_symbol: dict[str, int] = {}
        if analysis.state_record is not None:
            rank_by_symbol = {
                item["symbol"]: item["rank"]
                for item in analysis.state_record.evidence["members"]
            }
        else:
            rank_by_symbol = {
                item.symbol: item.quote_volume_rank for item in current_candidates
                if type(item.quote_volume_rank) is int
            }
        for symbol in analysis.frozen_symbols:
            rank = rank_by_symbol.get(symbol)
            if type(rank) is not int or not 1 <= rank <= 100:
                raise RuntimeError("N20 frozen candidate identity is invalid")
            current = current_by_symbol.get(symbol)
            raw = raw_by_symbol.get(symbol)
            if raw is None:
                raise RuntimeError("N20 frozen candidate raw history is missing")
            mark = Decimal(str(raw[-1][4]))
            if current is None:
                result[symbol] = FundingCandidate(
                    symbol=symbol, funding_rate=None, mark_price=mark,
                    quote_volume=None,
                    quote_volume_rank=rank,
                    candidate_universe="quote_volume_top_frozen_n20",
                )
        return sorted(result.values(), key=lambda item: (
            item.quote_volume_rank if item.quote_volume_rank is not None else 10**9,
            item.symbol,
        ))

    def _build_n15_batch_context(
        self,
        candidate_groups: Mapping[str, list[FundingCandidate]],
        raw_by_symbol: dict[str, list[list[Any]]],
        checked_at_ms: int | None,
    ) -> N15BatchContext:
        strategy = next(
            (
                item for item in self.strategies
                if item.evaluator_type == "market_breadth_recovery_leader"
            ),
            None,
        )
        if strategy is None or checked_at_ms is None:
            return N15BatchContext(None, None, False)
        candidates = list(candidate_groups.get("quote_volume_top", []))
        if (
            len(candidates) != 100
            or any(
                type(item.symbol) is not str
                or not item.symbol
                or type(item.quote_volume_rank) is not int
                or not 1 <= item.quote_volume_rank <= 100
                for item in candidates
            )
            or len({item.symbol for item in candidates}) != 100
            or {item.quote_volume_rank for item in candidates} != set(range(1, 101))
        ):
            return N15BatchContext(None, None, False)
        try:
            n15_raw_by_symbol = {
                symbol: source[-N15_MIN_RUNTIME_CANDLES:]
                for symbol, source in raw_by_symbol.items()
            }
            parsed = {
                item.symbol: parse_n15_klines(
                    n15_raw_by_symbol[item.symbol]
                )
                for item in candidates
            }
            if any(
                len(items) < N15_MIN_RUNTIME_CANDLES
                for items in parsed.values()
            ):
                raise ValueError("N15 current kline history incomplete")
            e_times = {items[-1].open_time_ms for items in parsed.values()}
            if len(e_times) != 1:
                raise ValueError("N15 current E axis incomplete")
            e_time = next(iter(e_times))
            if not e_time <= checked_at_ms < e_time + FIFTEEN_MINUTES_MS:
                raise ValueError("N15 current E timing invalid")
            try:
                existing = self.recorder.get_n15_market_snapshot(
                    strategy.strategy_id, str(e_time)
                )
            except ValueError:
                self._quarantine_n15_snapshot(
                    strategy.strategy_id,
                    e_time,
                    "N15_SNAPSHOT_ENVELOPE_INVALID",
                )
                return N15BatchContext(None, e_time, False, False)
            if (
                existing is not None
                and existing.get("schema_version") == 1
            ):
                try:
                    upgraded, _ = upgrade_n15_snapshot_payload(
                        existing, strategy, n15_raw_by_symbol, e_time
                    )
                except N15SnapshotSourceUnavailableError as exc:
                    raise ValueError(str(exc)) from exc
                except ValueError:
                    if not self._quarantine_n15_snapshot(
                        strategy.strategy_id,
                        e_time,
                        "N15_SNAPSHOT_RAW_SEMANTICS_INVALID",
                    ):
                        raise ValueError("N15_STATE_PERSIST_FAILED")
                    return N15BatchContext(None, e_time, False, False)
                upgrade_result = self.recorder.upgrade_n15_market_snapshot(
                    strategy.strategy_id,
                    str(e_time),
                    existing,
                    upgraded,
                )
                if upgrade_result != "OK":
                    raise ValueError(upgrade_result)
                existing = upgraded
            prior_snapshots = self.recorder.list_n15_market_snapshots(
                strategy.strategy_id
            )
        except Exception as exc:
            detail = str(exc) or type(exc).__name__
            self.logger.warning(
                "Unable to prepare N15 snapshot: %s", detail
            )
            return N15BatchContext(
                None,
                None,
                False,
                _n15_source_readiness_failure(detail),
            )
        try:
            for prior_time_text, prior_payload in prior_snapshots:
                prior_time = int(prior_time_text)
                if prior_time >= e_time:
                    continue
                try:
                    prior = decode_n15_historical_snapshot(
                        prior_payload,
                        strategy,
                        n15_raw_by_symbol,
                        prior_time,
                    )
                except N15HistoricalSourceUnavailableError as exc:
                    raise ValueError(str(exc)) from exc
                except ValueError:
                    if not self._quarantine_n15_snapshot(
                        strategy.strategy_id,
                        prior_time,
                        "N15_SNAPSHOT_RAW_SEMANTICS_INVALID",
                    ):
                        raise ValueError("N15_STATE_PERSIST_FAILED")
                    return N15BatchContext(None, e_time, False, False)
                if prior.winner_symbol is not None:
                    winner = prior.rows[prior.winner_symbol]
                    structure_id = n15_structure_id(
                        strategy.strategy_id,
                        winner.symbol,
                        winner.b.open_time_ms,
                        winner.c.open_time_ms,
                    )
                    prior_state = self.recorder.get_n15_entry_state(
                        strategy.strategy_id, str(prior_time)
                    )
                    if prior_state is not None and (
                        prior_state[3] != winner.symbol
                        or prior_state[4] != structure_id
                    ):
                        raise ValueError("N15_STATE_INCONSISTENT")
            if existing is None:
                payload, snapshot = build_n15_snapshot(
                    strategy, candidates, n15_raw_by_symbol
                )
                write_result = self.recorder.record_n15_market_snapshot(
                    strategy.strategy_id, str(e_time), payload
                )
                if write_result != "OK":
                    raise ValueError(write_result)
            else:
                try:
                    snapshot = decode_n15_snapshot(
                        existing, strategy, n15_raw_by_symbol, e_time
                    )
                except N15SnapshotSourceUnavailableError as exc:
                    raise ValueError(str(exc)) from exc
                except ValueError:
                    if not self._quarantine_n15_snapshot(
                        strategy.strategy_id,
                        e_time,
                        "N15_SNAPSHOT_RAW_SEMANTICS_INVALID",
                    ):
                        raise ValueError("N15_STATE_PERSIST_FAILED")
                    return N15BatchContext(None, e_time, False, False)
            if prior_snapshots and not self.recorder.delete_n15_market_snapshots_before(
                strategy.strategy_id, str(e_time)
            ):
                raise ValueError("N15_STATE_PERSIST_FAILED")
            return N15BatchContext(snapshot, e_time, True)
        except Exception as exc:
            detail = str(exc) or type(exc).__name__
            self.logger.warning(
                "Unable to freeze/restore N15 snapshot: %s", detail
            )
            return N15BatchContext(
                None,
                e_time,
                False,
                _n15_source_readiness_failure(detail),
            )

    def _build_n14_batch_context(
        self,
        candidate_groups: Mapping[str, list[FundingCandidate]],
        raw_by_symbol: dict[str, list[list[Any]]],
        checked_at_ms: int | None,
    ) -> N14BatchContext:
        strategy = next(
            (
                item
                for item in self.strategies
                if item.evaluator_type
                == "localized_sell_pressure_decay_reversal"
            ),
            None,
        )
        if strategy is None or checked_at_ms is None:
            return N14BatchContext({}, None, False)
        current_open_time = checked_at_ms // N14_INTERVAL_MS * N14_INTERVAL_MS
        if not current_open_time <= checked_at_ms < current_open_time + N14_INTERVAL_MS:
            return N14BatchContext({}, None, False)
        stale_result = self.recorder.expire_stale_n14_active_episodes(
            strategy.strategy_id,
            current_open_time,
        )
        if stale_result != "OK":
            self.logger.warning("Unable to retire stale N14 episodes: %s", stale_result)
            return N14BatchContext({}, current_open_time, False)
        candidates = [
            candidate
            for candidate in candidate_groups.get("quote_volume_top", [])
            if type(candidate.quote_volume_rank) is int
            and 1 <= candidate.quote_volume_rank <= 100
        ]
        latest_s_time = current_open_time - N14_INTERVAL_MS
        try:
            prior_snapshots: dict[int, N14Snapshot] = {}
            for offset in range(1, 4):
                prior_time = latest_s_time - offset * N14_INTERVAL_MS
                prior_payload = self.recorder.get_n14_market_snapshot(
                    strategy.strategy_id,
                    str(prior_time),
                )
                if prior_payload is not None:
                    prior_snapshots[prior_time] = decode_n14_snapshot(
                        prior_payload,
                        strategy.strategy_id,
                    )
            existing_latest = self.recorder.get_n14_market_snapshot(
                strategy.strategy_id,
                str(latest_s_time),
            )
        except Exception as exc:
            self.logger.warning("Unable to read N14 S snapshot: %s", exc)
            return N14BatchContext({}, current_open_time, False)
        if existing_latest is None:
            try:
                payload, snapshot, observed_current = build_n14_snapshot(
                    strategy,
                    candidates,
                    raw_by_symbol,
                    prior_snapshots=prior_snapshots,
                )
                if (
                    observed_current != current_open_time
                    or snapshot.s_open_time_ms != latest_s_time
                ):
                    raise ValueError("N14 current axis mismatch")
            except (ArithmeticError, TypeError, ValueError) as exc:
                self.logger.warning("Unable to build N14 S snapshot: %s", exc)
                return N14BatchContext({}, current_open_time, False)
            write_result = self.recorder.record_n14_market_snapshot(
                strategy.strategy_id,
                str(latest_s_time),
                payload,
            )
            if write_result != "OK":
                self.logger.warning("Unable to freeze N14 S snapshot: %s", write_result)
                return N14BatchContext({}, current_open_time, False)

        if not self.recorder.delete_expired_n14_market_snapshots(
            strategy.strategy_id,
            str(current_open_time - 4 * N14_INTERVAL_MS),
        ):
            return N14BatchContext({}, current_open_time, False)
        try:
            active_s_times = {
                int(value)
                for value in self.recorder.list_n14_active_snapshot_times(
                    strategy.strategy_id
                )
            }
        except Exception as exc:
            self.logger.warning("Unable to list N14 active snapshots: %s", exc)
            return N14BatchContext({}, current_open_time, False)

        snapshots: dict[int, N14SnapshotRuntime] = {}
        all_complete = True
        for s_time in sorted({latest_s_time, *active_s_times}):
            try:
                if s_time >= current_open_time:
                    raise ValueError("N14 snapshot is not closed")
                payload = self.recorder.get_n14_market_snapshot(
                    strategy.strategy_id,
                    str(s_time),
                )
                if payload is None:
                    raise ValueError("N14 active snapshot missing")
                snapshot = decode_n14_snapshot(payload, strategy.strategy_id)
                if snapshot.s_open_time_ms != s_time:
                    raise ValueError("N14 snapshot identity mismatch")
                breadth_c = n14_bullish_breadth_for_current_c(
                    snapshot,
                    raw_by_symbol,
                    current_open_time,
                )
                runtime = N14SnapshotRuntime(snapshot, breadth_c, True)
            except (ArithmeticError, TypeError, ValueError) as exc:
                self.logger.warning("Unable to restore N14 S snapshot: %s", exc)
                all_complete = False
                continue
            snapshots[s_time] = runtime
        if latest_s_time not in snapshots:
            all_complete = False
        return N14BatchContext(snapshots, current_open_time, all_complete)

    def _probe_n14_candidate(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        raw_klines: list[list[Any]],
        checked_at_ms: int | None,
        context: N14BatchContext,
    ) -> N14AnalysisResult | None:
        """Return the intrinsic N14 setup used to freeze the batch winner.

        This probe deliberately performs no recorder reads or writes.  The
        winner therefore cannot change merely because persistence for one
        candidate failed after the market analysis was already known.
        """
        if (
            not context.complete
            or context.current_open_time_ms is None
            or checked_at_ms is None
        ):
            return None
        runtimes = [
            runtime
            for _, runtime in sorted(context.snapshots.items())
            if candidate.symbol in runtime.snapshot.rows
            and runtime.complete
            and runtime.bullish_breadth_c is not None
        ]
        if not runtimes:
            return None
        scenarios = {
            runtime.snapshot.s_open_time_ms: runtime.snapshot.scenario(
                candidate.symbol,
                runtime.bullish_breadth_c or Decimal("0"),
            )
            for runtime in runtimes
        }
        scenarios = {
            key: value for key, value in scenarios.items() if value is not None
        }
        try:
            parsed = parse_n14_klines(raw_klines)
        except (ArithmeticError, TypeError, ValueError):
            return None
        analyses: list[tuple[int, N14AnalysisResult]] = []
        for runtime in runtimes:
            snapshot = runtime.snapshot
            scenario = scenarios.get(snapshot.s_open_time_ms)
            if scenario is None:
                continue
            try:
                raw_s = next(
                    candle
                    for candle in parsed
                    if candle.open_time_ms == snapshot.s_open_time_ms
                )
                frozen_s = snapshot.rows[candidate.symbol].s_candle
                if any(
                    getattr(raw_s, field) != getattr(frozen_s, field)
                    for field in (
                        "open_time_ms",
                        "open",
                        "high",
                        "low",
                        "close",
                        "quote_volume",
                        "taker_buy_quote_volume",
                    )
                ):
                    return None
                analysis = analyze_n14_sell_pressure_decay_reversal(
                    candidate.symbol,
                    raw_klines,
                    market_scenarios=scenarios,
                    market_context_complete=True,
                    quote_volume_rank=scenario.quote_volume_rank,
                    checked_at_ms=checked_at_ms,
                    target_s_open_time_ms=snapshot.s_open_time_ms,
                    frozen_shock_assessment=(
                        snapshot.rows[candidate.symbol].shock_assessment
                    ),
                    **snapshot.config.analyzer_kwargs(),
                )
            except (ArithmeticError, StopIteration, TypeError, ValueError):
                return None
            analyses.append((snapshot.s_open_time_ms, analysis))
        if not analyses:
            return None

        def priority(item: tuple[int, N14AnalysisResult]):
            s_time, analysis = item
            if analysis.passed:
                level = 5
            elif (
                analysis.structure is not None
                and analysis.structure.entry is not None
            ):
                level = 4
            elif analysis.active_event is not None:
                level = {
                    "C_CONFIRMED": 3,
                    "A_CONFIRMED": 2,
                    "S_LOCKED": 1,
                }.get(analysis.active_event.status, 0)
            else:
                level = 0
            return (level, -s_time)

        return max(analyses, key=priority)[1]

    def _n14_event_record(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        event: N14Event,
        config_signature: str,
        *,
        require_new: bool = False,
    ) -> dict[str, Any]:
        structure_id = event.structure.structure_id if event.structure else None
        detail = _n14_terminal_envelope(
            strategy.strategy_id,
            candidate.symbol,
            event.s_time,
            structure_id,
            event.status,
            event.reason,
            config_signature,
            event.structure,
            event.detail,
        )
        return {
            "strategy_id": strategy.strategy_id,
            "symbol": candidate.symbol,
            "s_time": event.s_time,
            "structure_id": structure_id,
            "status": event.status,
            "reason": event.reason,
            "detail": detail,
            "require_new": require_new,
        }

    def _evaluate_n14_candidate(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        raw_klines: list[list[Any]],
        checked_at_ms: int | None,
        context: N14BatchContext,
    ) -> StrategySignalDecision:
        if (
            not context.complete
            or context.current_open_time_ms is None
            or checked_at_ms is None
        ):
            return self._rejected(
                strategy,
                candidate,
                "N14_MARKET_CONTEXT_INSUFFICIENT",
            )
        try:
            recent_terminal_rows = self.recorder.list_recent_n14_sell_impact_states(
                strategy.strategy_id,
                candidate.symbol,
                str(context.current_open_time_ms - 4 * N14_INTERVAL_MS),
            )
        except Exception:
            return self._rejected(
                strategy, candidate, "N14_STATE_READ_FAILED"
            )
        if any(
            not _n14_stored_terminal_is_self_consistent(row)
            for row in recent_terminal_rows
        ):
            return self._rejected(
                strategy, candidate, "N14_STATE_INCONSISTENT"
            )
        runtimes = [
            runtime
            for _, runtime in sorted(context.snapshots.items())
            if candidate.symbol in runtime.snapshot.rows
            and runtime.complete
            and runtime.bullish_breadth_c is not None
        ]
        if not runtimes:
            if recent_terminal_rows:
                return self._rejected(
                    strategy, candidate, "N14_EPISODE_CONSUMED"
                )
            return self._rejected(
                strategy,
                candidate,
                "N14_MARKET_CONTEXT_INSUFFICIENT",
            )

        scenarios = {
            runtime.snapshot.s_open_time_ms: runtime.snapshot.scenario(
                candidate.symbol,
                runtime.bullish_breadth_c or Decimal("0"),
            )
            for runtime in runtimes
        }
        scenarios = {
            key: value for key, value in scenarios.items() if value is not None
        }
        analyses: list[tuple[int, N14AnalysisResult]] = []
        terminal_records: list[dict[str, Any]] = []
        active_records: list[dict[str, Any]] = []
        consumed_times: set[str] = set()

        for runtime in runtimes:
            snapshot = runtime.snapshot
            scenario = scenarios.get(snapshot.s_open_time_ms)
            if scenario is None:
                continue
            s_time = str(snapshot.s_open_time_ms)
            try:
                existing_active = self.recorder.get_validated_n14_active_episode(
                    strategy.strategy_id,
                    candidate.symbol,
                    s_time,
                )
            except Exception:
                return self._rejected(
                    strategy,
                    candidate,
                    "N14_STATE_INCONSISTENT",
                )
            analysis_scenarios = scenarios
            if (
                existing_active is not None
                and existing_active.get("stage") == "C_CONFIRMED"
            ):
                try:
                    evidence = existing_active["evidence"]
                    frozen_breadth_c = Decimal(
                        str(evidence["bullish_breadth_c"])
                    )
                    if (
                        not frozen_breadth_c.is_finite()
                        or not Decimal("0")
                        <= frozen_breadth_c
                        <= Decimal("1")
                        or evidence.get("market_scenario")
                        != _n14_stable_market_scenario(scenario.json())
                    ):
                        raise ValueError("invalid frozen C breadth")
                    if frozen_breadth_c >= Decimal("0.40"):
                        frozen_gate = "BREADTH_MIN"
                    elif (
                        frozen_breadth_c - scenario.bullish_breadth_s
                        >= Decimal("0.15")
                    ):
                        frozen_gate = "IMPROVEMENT"
                    else:
                        frozen_gate = "FAILED"
                    if evidence.get("cascade_gate") != frozen_gate:
                        raise ValueError("invalid frozen cascade gate")
                    frozen_scenario = replace(
                        scenario,
                        bullish_breadth_c=frozen_breadth_c,
                    )
                    analysis_scenarios = {
                        **scenarios,
                        snapshot.s_open_time_ms: frozen_scenario,
                    }
                    scenario = frozen_scenario
                except (ArithmeticError, KeyError, TypeError, ValueError):
                    return self._rejected(
                        strategy,
                        candidate,
                        "N14_STATE_INCONSISTENT",
                    )
            if (
                existing_active is not None
                and context.current_open_time_ms
                > snapshot.s_open_time_ms + 4 * N14_INTERVAL_MS
            ):
                detail = _n14_terminal_from_active(
                    existing_active,
                    "MISSED",
                    "HISTORICAL_N14_ENTRY_MISSED",
                )
                terminal_records.append(
                    {
                        "strategy_id": strategy.strategy_id,
                        "symbol": candidate.symbol,
                        "s_time": s_time,
                        "structure_id": detail["structure_id"],
                        "status": "MISSED",
                        "reason": "HISTORICAL_N14_ENTRY_MISSED",
                        "detail": detail,
                    }
                )
                analyses.append(
                    (
                        snapshot.s_open_time_ms,
                        N14AnalysisResult(
                            symbol=candidate.symbol,
                            passed=False,
                            reason="HISTORICAL_N14_ENTRY_MISSED",
                            structure=None,
                            consume_current=False,
                            historical_events=(),
                            stage_events=(),
                            current_price=Decimal(str(raw_klines[-1][4])),
                            elapsed_ms=None,
                        ),
                    )
                )
                continue
            try:
                parsed_candidate_candles = parse_n14_klines(raw_klines)
                raw_s = next(
                    candle
                    for candle in parsed_candidate_candles
                    if candle.open_time_ms == snapshot.s_open_time_ms
                )
                frozen_s = snapshot.rows[candidate.symbol].s_candle
                if any(
                    getattr(raw_s, field) != getattr(frozen_s, field)
                    for field in (
                        "open_time_ms",
                        "open",
                        "high",
                        "low",
                        "close",
                        "quote_volume",
                        "taker_buy_quote_volume",
                    )
                ):
                    raise ValueError("N14 frozen S candle changed")
            except (StopIteration, TypeError, ValueError):
                return self._rejected(
                    strategy,
                    candidate,
                    "N14_STATE_INCONSISTENT",
                )
            analysis = analyze_n14_sell_pressure_decay_reversal(
                candidate.symbol,
                raw_klines,
                market_scenarios=analysis_scenarios,
                market_context_complete=True,
                quote_volume_rank=scenario.quote_volume_rank,
                checked_at_ms=checked_at_ms,
                target_s_open_time_ms=snapshot.s_open_time_ms,
                frozen_shock_assessment=(
                    snapshot.rows[candidate.symbol].shock_assessment
                ),
                **snapshot.config.analyzer_kwargs(),
            )
            analyses.append((snapshot.s_open_time_ms, analysis))
            try:
                existing_target = self.recorder.get_n14_sell_impact_state(
                    strategy.strategy_id,
                    candidate.symbol,
                    s_time,
                )
            except Exception:
                return self._rejected(
                    strategy,
                    candidate,
                    "N14_STATE_READ_FAILED",
                    analysis,
                )
            if existing_target is not None:
                if not _n14_stored_terminal_is_self_consistent(existing_target):
                    return self._rejected(
                        strategy,
                        candidate,
                        "N14_STATE_INCONSISTENT",
                        analysis,
                    )
                consumed_times.add(s_time)
                continue
            events = [*analysis.stage_events, *analysis.historical_events]
            if analysis.structure is not None and analysis.consume_current:
                events.append(
                    N14Event(
                        str(analysis.structure.s.open_time_ms),
                        "CONSUMED" if analysis.passed else "MISSED",
                        analysis.reason,
                        structure=replace(analysis.structure, entry=None),
                    )
                )
            for event in events:
                if (
                    event.reason == "HISTORICAL_N14_ENTRY_MISSED"
                    and existing_active is not None
                ):
                    detail = _n14_terminal_from_active(
                        existing_active,
                        "MISSED",
                        "HISTORICAL_N14_ENTRY_MISSED",
                    )
                    record = {
                        "strategy_id": strategy.strategy_id,
                        "symbol": candidate.symbol,
                        "s_time": event.s_time,
                        "structure_id": detail["structure_id"],
                        "status": "MISSED",
                        "reason": "HISTORICAL_N14_ENTRY_MISSED",
                        "detail": detail,
                    }
                else:
                    event_for_record = event
                    if (
                        event.reason == "HISTORICAL_N14_ENTRY_MISSED"
                        and event.structure is not None
                    ):
                        event_for_record = replace(
                            event,
                            structure=replace(
                                event.structure,
                                market_scenario=scenario,
                                quote_volume_rank=scenario.quote_volume_rank,
                            ),
                        )
                    record = self._n14_event_record(
                        strategy,
                        candidate,
                        event_for_record,
                        snapshot.config_signature,
                        require_new=(
                            analysis.structure is not None
                            and event.s_time
                            == str(analysis.structure.s.open_time_ms)
                            and analysis.consume_current
                        ),
                    )
                terminal_records.append(record)
            if analysis.active_event is not None and not events:
                event = analysis.active_event
                active_records.append(
                    {
                        "strategy_id": strategy.strategy_id,
                        "symbol": candidate.symbol,
                        "s_time": event.s_time,
                        "stage": event.status,
                        "detail": _n14_active_envelope(
                            strategy.strategy_id,
                            candidate.symbol,
                            event,
                            snapshot.config_signature,
                        ),
                    }
                )
            elif existing_active is not None and not events:
                active_records.append(
                    {
                        "strategy_id": strategy.strategy_id,
                        "symbol": candidate.symbol,
                        "s_time": s_time,
                        "stage": existing_active["stage"],
                        "detail": existing_active,
                    }
                )

        state_result = self.recorder.record_n14_state_batch_atomically(
            terminal_records,
            active_records,
        )
        if state_result != "OK":
            return self._rejected(
                strategy,
                candidate,
                state_result,
                analyses[-1][1] if analyses else None,
            )
        if not analyses:
            return self._rejected(
                strategy,
                candidate,
                "N14_MARKET_CONTEXT_INSUFFICIENT",
            )

        has_new_episode_evidence = any(
            analysis.passed
            or analysis.active_event is not None
            or analysis.consume_current
            or analysis.stage_events
            or analysis.historical_events
            for _, analysis in analyses
        )
        if recent_terminal_rows and not has_new_episode_evidence:
            return self._rejected(
                strategy,
                candidate,
                "N14_EPISODE_CONSUMED",
                analyses[-1][1],
            )

        def priority(item: tuple[int, N14AnalysisResult]):
            s_time, analysis = item
            if analysis.passed:
                level = 5
            elif analysis.structure is not None and analysis.structure.entry is not None:
                level = 4
            elif analysis.active_event is not None:
                level = {
                    "C_CONFIRMED": 3,
                    "A_CONFIRMED": 2,
                    "S_LOCKED": 1,
                }.get(analysis.active_event.status, 0)
            else:
                level = 0
            return (level, -s_time)

        selected_time, selected = max(analyses, key=priority)
        selected_s_time = (
            str(selected.structure.s.open_time_ms)
            if selected.structure is not None
            else selected.active_event.s_time
            if selected.active_event is not None
            else str(selected_time)
        )
        if selected_s_time in consumed_times:
            return self._rejected(
                strategy,
                candidate,
                "N14_EPISODE_CONSUMED",
                selected,
            )
        if selected.structure is not None and selected.structure.entry is not None:
            candidate = replace(
                candidate,
                mark_price=selected.structure.entry.close,
                quote_volume_rank=selected.structure.quote_volume_rank,
            )
        if not selected.passed:
            return self._rejected(
                strategy,
                candidate,
                selected.reason,
                selected,
            )
        return StrategySignalDecision(
            strategy=strategy,
            candidate=candidate,
            analysis=selected,
            passed=True,
            decision="PASSED",
            reason="PASSED",
        )

    def _apply_n13_state(
        self,
        strategy,
        candidate,
        analysis: N13AnalysisResult,
        *,
        pending_records: list[dict[str, Any]] | None = None,
        pending_guards: list[tuple[Any, ...]] | None = None,
    ):
        try:
            analysis_contract_valid = _n13_analysis_state_contract_is_valid(
                strategy.strategy_id,
                candidate.symbol,
                analysis,
                strategy.n13_armed_max_bars,
                strategy.n13_vwap_lookback_bars,
                strategy.n13_atr_period,
                strategy.n13_upper_atr_fraction,
                strategy.n13_lower_atr_fraction,
                strategy.n13_confirmation_max_bars,
                strategy.n13_confirmation_close_location_min,
                strategy.n13_confirmation_taker_buy_ratio_min,
                strategy.n13_entry_extension_atr_max,
            )
        except Exception:
            analysis_contract_valid = False
        if not analysis_contract_valid:
            return self._rejected(
                strategy,
                candidate,
                "N13_STATE_INCONSISTENT",
                analysis,
            )
        events = (*analysis.stage_events, *analysis.historical_events)
        event_records: list[dict[str, Any]] = []
        events_by_key: dict[tuple[str, str, str], Any] = {}
        for event in events:
            event_structure_id = (
                event.structure.structure_id if event.structure else None
            )
            record = {
                "strategy_id": strategy.strategy_id,
                "symbol": candidate.symbol,
                "t_time": event.t_time,
                "structure_id": event_structure_id,
                "status": event.status,
                "reason": event.reason,
                "detail": _n13_state_detail_without_indexes(
                    {
                        "event": event.reason,
                        "structure": (
                            event.structure.json()
                            if event.structure
                            else None
                        ),
                        "detail": event.detail,
                    }
                ),
            }
            event_records.append(record)
            events_by_key.setdefault(
                (
                    strategy.strategy_id,
                    candidate.symbol,
                    event.t_time,
                ),
                event,
            )
        unique_event_records = _deduplicate_n13_state_records(
            event_records,
            strategy.n13_armed_max_bars,
            strategy.n13_vwap_lookback_bars,
            strategy.n13_atr_period,
            strategy.n13_upper_atr_fraction,
            strategy.n13_lower_atr_fraction,
            strategy.n13_confirmation_max_bars,
            strategy.n13_confirmation_close_location_min,
            strategy.n13_confirmation_taker_buy_ratio_min,
            strategy.n13_entry_extension_atr_max,
        )
        if unique_event_records is None:
            return self._rejected(
                strategy,
                candidate,
                "N13_STATE_INCONSISTENT",
                analysis,
            )

        records: list[dict[str, Any]] = []
        existing_guards: list[tuple[Any, ...]] = []
        current_already_consumed = False
        for record in unique_event_records:
            event = events_by_key[
                (
                    record["strategy_id"],
                    record["symbol"],
                    record["t_time"],
                )
            ]
            event_structure_id = record.get("structure_id")
            try:
                existing_event = self.recorder.get_n13_rotation_state(
                    record["strategy_id"],
                    record["symbol"],
                    record["t_time"],
                )
            except Exception:
                return self._rejected(
                    strategy,
                    candidate,
                    "N13_STATE_READ_FAILED",
                    analysis,
                )
            if existing_event is None:
                records.append(record)
                continue
            # A structure already consumed while it was current must not be
            # rewritten as a historical miss on a later fixed window.
            current_terminal_dominates = (
                event.reason == "HISTORICAL_N13_ENTRY_MISSED"
                and event_structure_id is not None
                and _n13_existing_current_terminal_is_valid(
                    existing_event,
                    strategy.strategy_id,
                    candidate.symbol,
                    event.t_time,
                    event.structure,
                    strategy.n13_armed_max_bars,
                    strategy.n13_entry_extension_atr_max,
                )
            )
            if current_terminal_dominates:
                existing_guards.append(tuple(existing_event))
                continue
            event_match = _n13_existing_event_state_match(
                existing_event,
                strategy.strategy_id,
                candidate.symbol,
                event,
                strategy.n13_armed_max_bars,
                strategy.n13_vwap_lookback_bars,
                strategy.n13_atr_period,
                strategy.n13_upper_atr_fraction,
                strategy.n13_lower_atr_fraction,
                strategy.n13_confirmation_max_bars,
                strategy.n13_confirmation_close_location_min,
                strategy.n13_confirmation_taker_buy_ratio_min,
                strategy.n13_entry_extension_atr_max,
            )
            if event_match == _N13_EVENT_MATCH_EXACT:
                existing_guards.append(tuple(existing_event))
                continue
            if event_match == _N13_EVENT_MATCH_ISOLATED:
                existing_guards.append(tuple(existing_event))
                self._warn_n13_isolated_collision(
                    tuple(existing_event),
                    record,
                )
                continue
            conflict_fingerprint = _n13_audit_collision_fingerprint(
                tuple(existing_event),
                record,
            )
            self.logger.warning(
                "N13 rotation state conflict detected; candidate rejected "
                "| strategy=%s symbol=%s t_time=%s "
                "existing_structure_id=%s replay_structure_id=%s "
                "existing_status=%s replay_status=%s "
                "existing_reason=%s replay_reason=%s fingerprint=%s",
                strategy.strategy_id,
                candidate.symbol,
                event.t_time,
                existing_event[4],
                event_structure_id,
                existing_event[5],
                event.status,
                existing_event[6],
                event.reason,
                conflict_fingerprint,
            )
            return self._rejected(
                strategy,
                candidate,
                "N13_STATE_INCONSISTENT",
                analysis,
            )

        structure = analysis.structure
        current_structure = (
            structure
            if structure is not None and structure.entry is not None
            else None
        )
        if current_structure is not None:
            try:
                existing = self.recorder.get_n13_rotation_state(
                    strategy.strategy_id,
                    candidate.symbol,
                    str(current_structure.t.open_time_ms),
                )
            except Exception:
                return self._rejected(strategy, candidate, "N13_STATE_READ_FAILED", analysis)
            if existing is not None:
                if not _n13_existing_current_terminal_is_valid(
                    existing,
                    strategy.strategy_id,
                    candidate.symbol,
                    str(current_structure.t.open_time_ms),
                    current_structure,
                    strategy.n13_armed_max_bars,
                    strategy.n13_entry_extension_atr_max,
                ):
                    return self._rejected(
                        strategy,
                        candidate,
                        "N13_STATE_INCONSISTENT",
                        analysis,
                    )
                current_already_consumed = True
                existing_guards.append(tuple(existing))
            elif analysis.consume_current:
                current_status = "CONSUMED" if analysis.passed else "MISSED"
                records.append(
                    {
                        "strategy_id": strategy.strategy_id,
                        "symbol": candidate.symbol,
                        "t_time": str(current_structure.t.open_time_ms),
                        "structure_id": current_structure.structure_id,
                        "status": current_status,
                        "reason": analysis.reason,
                        "detail": _n13_current_terminal_envelope(
                            strategy.strategy_id,
                            candidate.symbol,
                            str(current_structure.t.open_time_ms),
                            current_structure,
                            current_status,
                            analysis.reason,
                        ),
                        "require_new": True,
                    }
                )

        unique_records = _deduplicate_n13_state_records(
            records,
            strategy.n13_armed_max_bars,
            strategy.n13_vwap_lookback_bars,
            strategy.n13_atr_period,
            strategy.n13_upper_atr_fraction,
            strategy.n13_lower_atr_fraction,
            strategy.n13_confirmation_max_bars,
            strategy.n13_confirmation_close_location_min,
            strategy.n13_confirmation_taker_buy_ratio_min,
            strategy.n13_entry_extension_atr_max,
        )
        if unique_records is None:
            return self._rejected(
                strategy,
                candidate,
                "N13_STATE_INCONSISTENT",
                analysis,
            )
        if (pending_records is None) != (pending_guards is None):
            return self._rejected(
                strategy,
                candidate,
                "N13_STATE_INCONSISTENT",
                analysis,
            )
        if pending_records is not None and pending_guards is not None:
            pending_records.extend(unique_records)
            pending_guards.extend(existing_guards)
            state_result = "OK"
        elif existing_guards:
            state_result = self.recorder.record_n13_rotation_states_atomically(
                unique_records,
                existing_guards=existing_guards,
            )
        else:
            state_result = self.recorder.record_n13_rotation_states_atomically(
                unique_records
            )
        if state_result != "OK":
            return self._rejected(strategy, candidate, state_result, analysis)
        if current_already_consumed:
            return self._rejected(
                strategy,
                candidate,
                "N13_EPISODE_CONSUMED",
                analysis,
            )
        return None

    def _apply_n15_state(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        analysis: N15AnalysisResult,
    ) -> StrategySignalDecision | None:
        structure = analysis.structure
        if structure is None:
            return None
        e_time = str(structure.entry.open_time_ms)
        try:
            existing = self.recorder.get_n15_entry_state(
                strategy.strategy_id, e_time
            )
        except ValueError:
            return self._rejected(
                strategy, candidate, "N15_STATE_INCONSISTENT", analysis
            )
        except Exception:
            return self._rejected(
                strategy, candidate, "N15_STATE_READ_FAILED", analysis
            )
        if existing is not None:
            valid = (
                existing[3] == candidate.symbol
                and existing[4] == structure.structure_id
            )
            return self._rejected(
                strategy,
                candidate,
                "N15_OPPORTUNITY_CONSUMED" if valid else "N15_STATE_INCONSISTENT",
                analysis,
            )
        if not analysis.consume_current:
            return None
        write_result = self.recorder.record_n15_entry_state(
            strategy.strategy_id,
            e_time,
            candidate.symbol,
            structure.structure_id,
            "CONSUMED" if analysis.passed else "MISSED",
            analysis.reason,
            analysis.detail_json(),
        )
        if write_result == "EXISTS":
            return self._rejected(
                strategy, candidate, "N15_OPPORTUNITY_CONSUMED", analysis
            )
        if write_result != "INSERTED":
            return self._rejected(
                strategy, candidate, write_result, analysis
            )
        return None

    def _apply_n12_terminal_state(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        analysis: N12AnalysisResult,
    ) -> StrategySignalDecision | None:
        newly_backfilled_stages: set[tuple[str, str]] = set()
        newly_backfilled_structures: set[str] = set()
        selected_stage = None
        if analysis.structure is None and analysis.stage_events:
            selected_stage = max(
                analysis.stage_events,
                key=lambda item: (
                    item.terminal_index, item.h_index, item.l_index
                ),
            )
        for event in analysis.stage_events:
            stage_key = (event.l_time, event.h_time)
            try:
                stage = self.recorder.get_n12_stage_state(
                    strategy.strategy_id,
                    candidate.symbol,
                    *stage_key,
                )
            except Exception as exc:
                self.logger.warning("Unable to read N12 stage event state: %s", exc)
                return self._rejected(
                    strategy, candidate, "N12_STATE_READ_FAILED", analysis
                )
            if stage is not None:
                if event is selected_stage:
                    return self._rejected(
                        strategy, candidate, "N12_STAGE_CONSUMED", analysis
                    )
                continue
            if not self.recorder.record_n12_stage_terminal(
                strategy.strategy_id,
                candidate.symbol,
                event.l_time,
                event.h_time,
                event.status,
                event.reason,
                {"stage_event": event.to_jsonable()},
            ):
                return self._rejected(
                    strategy, candidate, "N12_STATE_PERSIST_FAILED", analysis
                )
            newly_backfilled_stages.add(stage_key)

        for event in analysis.historical_events:
            structure = event.structure
            stage_key = (structure.l_time, structure.h_time)
            try:
                stage = self.recorder.get_n12_stage_state(
                    strategy.strategy_id,
                    candidate.symbol,
                    *stage_key,
                )
                terminal = self.recorder.get_strategy_structure_terminal_state(
                    strategy.strategy_id,
                    structure.structure_id,
                )
            except Exception as exc:
                self.logger.warning("Unable to read N12 historical state: %s", exc)
                return self._rejected(
                    strategy, candidate, "N12_STATE_READ_FAILED", analysis
                )
            if stage is not None or terminal is not None:
                reconciliation = (
                    self.recorder.reconcile_n12_stage_and_structure_terminal(
                        strategy.strategy_id,
                        candidate.symbol,
                        structure.l_time,
                        structure.h_time,
                        structure.structure_id,
                    )
                )
                if reconciliation == "FAILED":
                    return self._rejected(
                        strategy, candidate, "N12_STATE_PERSIST_FAILED", analysis
                    )
                if reconciliation == "INCONSISTENT":
                    return self._rejected(
                        strategy, candidate, "N12_STATE_INCONSISTENT", analysis
                    )
                continue
            if stage is None and terminal is None:
                if not self.recorder.record_n12_stage_and_structure_terminal(
                    strategy.strategy_id,
                    candidate.symbol,
                    structure.l_time,
                    structure.h_time,
                    structure.structure_id,
                    event.status,
                    event.reason,
                    {"historical_backfill": event.to_jsonable()},
                ):
                    return self._rejected(
                        strategy, candidate, "N12_STATE_PERSIST_FAILED", analysis
                    )
                if stage is None:
                    newly_backfilled_stages.add(stage_key)
                if terminal is None:
                    newly_backfilled_structures.add(structure.structure_id)

        structure = analysis.structure
        if structure is None:
            return None
        stage_key = (structure.l_time, structure.h_time)
        try:
            stage = self.recorder.get_n12_stage_state(
                strategy.strategy_id,
                candidate.symbol,
                *stage_key,
            )
            terminal = self.recorder.get_strategy_structure_terminal_state(
                strategy.strategy_id,
                structure.structure_id,
            )
            passed_structure = self.recorder.inspect_passed_structure(
                strategy.strategy_id,
                candidate.symbol,
                structure.structure_id,
            )
        except Exception as exc:
            self.logger.warning("Unable to read N12 current state: %s", exc)
            return self._rejected(
                strategy, candidate, "N12_STATE_READ_FAILED", analysis
            )
        if stage is not None or terminal is not None:
            reconciliation = (
                self.recorder.reconcile_n12_stage_and_structure_terminal(
                    strategy.strategy_id,
                    candidate.symbol,
                    structure.l_time,
                    structure.h_time,
                    structure.structure_id,
                )
            )
            if reconciliation == "FAILED":
                return self._rejected(
                    strategy, candidate, "N12_STATE_PERSIST_FAILED", analysis
                )
            if reconciliation == "INCONSISTENT":
                return self._rejected(
                    strategy, candidate, "N12_STATE_INCONSISTENT", analysis
                )
        if stage is not None and stage_key not in newly_backfilled_stages:
            return self._rejected(
                strategy, candidate, "N12_STAGE_CONSUMED", analysis
            )
        if passed_structure == "INCONSISTENT":
            return self._rejected(
                strategy, candidate, "N12_STATE_INCONSISTENT", analysis
            )
        if (
            terminal is not None
            and structure.structure_id not in newly_backfilled_structures
        ) or passed_structure == "CONSUMED":
            return self._rejected(
                strategy, candidate, "N12_STRUCTURE_CONSUMED", analysis
            )
        if not analysis.consume_current:
            return None
        status = "CONSUMED" if analysis.passed else "MISSED"
        if analysis.reason.startswith("N12_PULLBACK_") or analysis.reason.startswith(
            "N12_CONFIRMATION_"
        ):
            status = "INVALID"
        if not self.recorder.record_n12_stage_and_structure_terminal(
            strategy.strategy_id,
            candidate.symbol,
            structure.l_time,
            structure.h_time,
            structure.structure_id,
            status,
            analysis.reason,
            analysis.detail_json(),
        ):
            return self._rejected(
                strategy, candidate, "N12_STATE_PERSIST_FAILED", analysis
            )
        return None

    def _apply_n11_terminal_state(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        analysis: N11AnalysisResult,
    ) -> StrategySignalDecision | None:
        newly_backfilled: set[str] = set()
        for historical in analysis.historical_events:
            structure_id = historical.structure.structure_id
            try:
                existing = self.recorder.get_strategy_structure_terminal_state(
                    strategy.strategy_id,
                    structure_id,
                )
            except Exception as exc:
                self.logger.warning("Unable to read N11 historical state: %s", exc)
                return self._rejected(
                    strategy,
                    candidate,
                    "N11_STATE_READ_FAILED",
                    analysis,
                )
            if existing is not None:
                continue
            if not self.recorder.record_strategy_structure_terminal(
                strategy.strategy_id,
                candidate.symbol,
                structure_id,
                historical.status,
                historical.reason,
                {"historical_backfill": historical.to_jsonable()},
            ):
                return self._rejected(
                    strategy,
                    candidate,
                    "N11_STATE_PERSIST_FAILED",
                    analysis,
                )
            newly_backfilled.add(structure_id)

        structure_id = analysis.structure_id
        if not structure_id:
            return None
        try:
            terminal = self.recorder.get_strategy_structure_terminal_state(
                strategy.strategy_id,
                structure_id,
            )
        except Exception as exc:
            self.logger.warning("Unable to read N11 structure state: %s", exc)
            return self._rejected(
                strategy,
                candidate,
                "N11_STATE_READ_FAILED",
                analysis,
            )
        if terminal is not None and structure_id not in newly_backfilled:
            return self._rejected(
                strategy,
                candidate,
                "N11_STRUCTURE_CONSUMED",
                analysis,
            )

        try:
            passed_structure = self.recorder.inspect_passed_structure(
                strategy.strategy_id,
                candidate.symbol,
                structure_id,
            )
        except Exception as exc:
            self.logger.warning("Unable to read N11 legacy structure signal: %s", exc)
            return self._rejected(
                strategy,
                candidate,
                "N11_STATE_READ_FAILED",
                analysis,
            )
        if passed_structure == "INCONSISTENT":
            return self._rejected(
                strategy,
                candidate,
                "N11_STATE_INCONSISTENT",
                analysis,
            )
        if passed_structure == "CONSUMED":
            if terminal is None and not self.recorder.record_strategy_structure_terminal(
                strategy.strategy_id,
                candidate.symbol,
                structure_id,
                "CONSUMED",
                "N11_LEGACY_DUPLICATE_STRUCTURE",
                analysis.detail_json(),
            ):
                return self._rejected(
                    strategy,
                    candidate,
                    "N11_STATE_PERSIST_FAILED",
                    analysis,
                )
            return self._rejected(
                strategy,
                candidate,
                "N11_STRUCTURE_CONSUMED",
                analysis,
            )

        if not analysis.consume_current or structure_id in newly_backfilled:
            return None
        if analysis.passed:
            status = "CONSUMED"
        elif analysis.reason in {
            "N11_RETEST_TOO_DEEP",
            "N11_RETEST_NOT_HELD",
            "N11_RETEST_CLOSE_LOCATION_TOO_LOW",
            "N11_RETEST_VOLUME_TOO_HIGH",
        }:
            status = "INVALID"
        else:
            status = "MISSED"
        if terminal is None and not self.recorder.record_strategy_structure_terminal(
            strategy.strategy_id,
            candidate.symbol,
            structure_id,
            status,
            analysis.reason,
            analysis.detail_json(),
        ):
            return self._rejected(
                strategy,
                candidate,
                "N11_STATE_PERSIST_FAILED",
                analysis,
            )
        return None

    def _apply_n06_n07_terminal_state(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        analysis: N06AnalysisResult | N07AnalysisResult,
    ) -> StrategySignalDecision | None:
        newly_backfilled: set[str] = set()
        if isinstance(analysis, N06AnalysisResult):
            for historical in analysis.historical_missed:
                try:
                    existing = self.recorder.get_strategy_structure_terminal_state(
                        strategy.strategy_id, historical.structure_id
                    )
                except Exception as exc:
                    self.logger.warning("Unable to read N06 terminal state: %s", exc)
                    return self._rejected(strategy, candidate, "N06_STATE_READ_FAILED", analysis)
                if existing is None:
                    if not self.recorder.record_strategy_structure_terminal(
                        strategy.strategy_id,
                        candidate.symbol,
                        historical.structure_id,
                        "MISSED",
                        "N06_HISTORICAL_STRUCTURE_MISSED",
                        {"structure": historical.to_jsonable(), "backfilled": True},
                    ):
                        return self._rejected(
                            strategy, candidate, "N06_STATE_PERSIST_FAILED", analysis
                        )
                    newly_backfilled.add(historical.structure_id)

        structure_id = analysis.structure_id
        if not structure_id:
            return None
        prefix = strategy.strategy_id
        try:
            terminal = self.recorder.get_strategy_structure_terminal_state(
                strategy.strategy_id, structure_id
            )
        except Exception as exc:
            self.logger.warning("Unable to read %s terminal state: %s", prefix, exc)
            return self._rejected(
                strategy, candidate, f"{prefix}_STATE_READ_FAILED", analysis
            )
        if terminal is not None and structure_id not in newly_backfilled:
            return self._rejected(
                strategy, candidate, f"{prefix}_STRUCTURE_CONSUMED", analysis
            )

        try:
            passed_structure = self.recorder.inspect_passed_structure(
                strategy.strategy_id,
                candidate.symbol,
                structure_id,
            )
        except Exception as exc:
            self.logger.warning("Unable to read %s passed ledger: %s", prefix, exc)
            return self._rejected(
                strategy, candidate, f"{prefix}_STATE_READ_FAILED", analysis
            )
        if passed_structure == "INCONSISTENT":
            return self._rejected(
                strategy, candidate, f"{prefix}_STATE_INCONSISTENT", analysis
            )
        if passed_structure == "CONSUMED":
            if not self.recorder.record_strategy_structure_terminal(
                strategy.strategy_id,
                candidate.symbol,
                structure_id,
                "TRADED",
                "LEGACY_DUPLICATE_STRUCTURE",
                analysis.detail_json(),
            ):
                return self._rejected(
                    strategy, candidate, f"{prefix}_STATE_PERSIST_FAILED", analysis
                )
            return self._rejected(strategy, candidate, "DUPLICATE_STRUCTURE", analysis)

        terminal_status: str | None = None
        if isinstance(analysis, N06AnalysisResult) and analysis.reason in {
            "N06_HISTORICAL_STRUCTURE_MISSED",
            "N06_ENTRY_CANDLE_MISMATCH_MISSED",
            "N06_ENTRY_WINDOW_MISSED",
        }:
            terminal_status = "MISSED"
        elif isinstance(analysis, N07AnalysisResult):
            if analysis.reason in {
                "N07_PULLBACK_TOO_SHALLOW",
                "N07_P1_TOUCHED_AFTER_SECOND_BREAK",
            }:
                terminal_status = "INVALID"
            elif analysis.reason in {
                "N07_HISTORICAL_ZONE_TOUCH_MISSED",
                "N07_ENTRY_ZONE_OVERSHOT",
                "N07_ENTRY_PRICE_BELOW_ZONE",
                "N07_ENTRY_TOUCH_REBOUNDED",
                "N07_INVALID_FIRST_TOUCH_SEGMENT",
            }:
                terminal_status = "MISSED"
        if terminal_status is None:
            return None
        if terminal is None and not self.recorder.record_strategy_structure_terminal(
            strategy.strategy_id,
            candidate.symbol,
            structure_id,
            terminal_status,
            analysis.reason,
            analysis.detail_json(),
        ):
            return self._rejected(
                strategy, candidate, f"{prefix}_STATE_PERSIST_FAILED", analysis
            )
        return self._rejected(strategy, candidate, analysis.reason, analysis)

    def _assess_n08_history_coverage(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        raw_klines: list[list[Any]],
    ) -> N08CoverageAssessment | None:
        try:
            open_times = [int(Decimal(str(row[0]))) for row in raw_klines]
        except (ArithmeticError, IndexError, TypeError, ValueError):
            return None
        if not open_times:
            return None

        try:
            existing = self.recorder.get_n08_history_coverage(
                strategy.strategy_id,
                candidate.symbol,
            )
        except Exception as exc:
            self.logger.warning("Unable to read N08 history coverage: %s", exc)
            return None

        gap_index = None
        for index in range(1, len(open_times)):
            if open_times[index] - open_times[index - 1] != 15 * 60 * 1000:
                gap_index = index

        response_first = open_times[0]
        response_last = open_times[-1]
        gap_detected = gap_index is not None
        if gap_index is not None:
            continuous_from = open_times[gap_index]
            continuous_until = response_last
            gap_from = open_times[gap_index - 1]
            gap_to = open_times[gap_index]
        elif existing is None:
            continuous_from = response_first
            continuous_until = response_last
            gap_from = None
            gap_to = None
        else:
            existing_from = int(Decimal(existing.continuous_from_open_time))
            existing_until = int(Decimal(existing.continuous_until_open_time))
            if response_first <= existing_until + 15 * 60 * 1000:
                continuous_from = min(existing_from, response_first)
                continuous_until = max(existing_until, response_last)
                gap_from = (
                    int(Decimal(existing.last_gap_from_open_time))
                    if existing.last_gap_from_open_time is not None
                    else None
                )
                gap_to = (
                    int(Decimal(existing.last_gap_to_open_time))
                    if existing.last_gap_to_open_time is not None
                    else None
                )
            else:
                continuous_from = response_first
                continuous_until = response_last
                gap_from = existing_until
                gap_to = response_first
                gap_detected = True

        return N08CoverageAssessment(
            coverage=N08HistoryCoverage(
                strategy_id=strategy.strategy_id,
                symbol=candidate.symbol,
                continuous_from_open_time=str(continuous_from),
                continuous_until_open_time=str(continuous_until),
                last_response_first_open_time=str(response_first),
                last_response_last_open_time=str(response_last),
                last_gap_from_open_time=str(gap_from) if gap_from is not None else None,
                last_gap_to_open_time=str(gap_to) if gap_to is not None else None,
                updated_at=utc_now(),
            ),
            gap_detected=gap_detected,
        )

    def _backfill_n08_history(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        raw_klines: list[list[Any]],
        checked_at_ms: int | None,
        n08_states_by_symbol: dict[str, list[Any]] | None = None,
    ) -> bool:
        events = find_n08_historical_missed_events(
            candidate.symbol,
            raw_klines,
            pivot_left=strategy.pivot_left,
            pivot_right=strategy.pivot_right,
            range_min_bars=strategy.range_min_bars,
            range_max_bars=strategy.range_max_bars,
            tolerance_fraction=strategy.range_tolerance_fraction,
            bullish_streak_count=strategy.bullish_streak_count,
            entry_window_seconds=strategy.entry_window_seconds,
            checked_at_ms=checked_at_ms,
        )
        for event in events:
            structure = event.structure
            if structure is None:
                continue
            try:
                if self.recorder.has_n08_structure_state(
                    strategy.strategy_id,
                    structure.structure_id,
                ):
                    continue
            except Exception as exc:
                self.logger.warning("Unable to read N08 historical state: %s", exc)
                return False
            active_states = self._active_n08_states_after_resets(
                strategy,
                candidate,
                raw_klines,
                structure.start_time,
                n08_states_by_symbol,
            )
            if active_states is None:
                return False
            if self._matching_n08_state(event, active_states) is not None:
                continue
            if not self._consume_n08_structure(
                strategy,
                candidate,
                event,
                event.reason,
            ):
                return False
            if n08_states_by_symbol is not None:
                try:
                    n08_states_by_symbol[candidate.symbol] = (
                        self.recorder.get_current_n08_structure_states(
                            strategy.strategy_id,
                            candidate.symbol,
                        )
                    )
                except Exception as exc:
                    self.logger.warning(
                        "Unable to confirm same-round N08 historical state: %s",
                        exc,
                    )
                    return False
        return True

    def _matching_n08_state(
        self,
        analysis: N08AnalysisResult,
        states: list[N08StructureState],
    ) -> N08StructureState | None:
        structure = analysis.structure
        if structure is None:
            return None
        for state in states:
            if state.structure_id == structure.structure_id or is_same_n08_range_family(
                structure,
                Decimal(state.upper_reference),
                Decimal(state.lower_reference),
                Decimal(state.upper_tolerance_boundary),
                Decimal(state.lower_tolerance_boundary),
            ):
                return state
        return None

    def _n08_history_context_incomplete(
        self,
        strategy: StrategyConfig,
        analysis: N08AnalysisResult,
        raw_klines: list[list[Any]],
        coverage: N08HistoryCoverage,
    ) -> bool:
        if analysis.structure is None or not raw_klines:
            return False
        full_window_size = (
            strategy.range_max_bars + strategy.bullish_streak_count + 1
        )
        if len(raw_klines) < full_window_size:
            return False
        try:
            structure_start_time = int(Decimal(analysis.structure.start_time))
            continuous_from = int(Decimal(coverage.continuous_from_open_time))
            continuous_until = int(Decimal(coverage.continuous_until_open_time))
            response_last = int(Decimal(str(raw_klines[-1][0])))
        except (ArithmeticError, IndexError, TypeError, ValueError):
            return True
        return not (
            continuous_from < structure_start_time
            and continuous_until >= response_last
        )

    def _active_n08_states_after_resets(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        raw_klines: list[list[Any]],
        candidate_range_start_time: str | None,
        n08_states_by_symbol: dict[str, list[Any]] | None = None,
    ) -> list[N08StructureState] | None:
        try:
            if (
                n08_states_by_symbol is not None
                and candidate.symbol in n08_states_by_symbol
            ):
                states = list(n08_states_by_symbol[candidate.symbol])
            else:
                states = self.recorder.get_active_n08_structure_states(
                    strategy.strategy_id,
                    candidate.symbol,
                )
                if n08_states_by_symbol is not None:
                    n08_states_by_symbol[candidate.symbol] = list(states)
        except Exception as exc:
            self.logger.warning("Unable to read N08 structure state: %s", exc)
            return None

        active: list[N08StructureState] = []
        for state in states:
            try:
                reset_open_time = state.reset_open_time or find_n08_range_reset_open_time(
                    raw_klines,
                    state.first_streak_end_time,
                    Decimal(state.upper_tolerance_boundary),
                    Decimal(state.lower_tolerance_boundary),
                )
            except (ArithmeticError, TypeError, ValueError) as exc:
                self.logger.warning("Invalid persisted N08 structure state: %s", exc)
                return None
            if reset_open_time is None:
                active.append(state)
                continue
            if (
                candidate_range_start_time is not None
                and int(Decimal(candidate_range_start_time)) > int(Decimal(reset_open_time))
            ):
                if not self.recorder.retire_n08_structure_state(
                    state.id,
                    reset_open_time,
                    "NEW_RANGE_AFTER_TOLERANCE_RESET",
                ):
                    return None
                self.recorder.record_event(
                    "n08_structure_retired",
                    {
                        "strategy_id": strategy.strategy_id,
                        "structure_id": state.structure_id,
                        "reset_open_time": reset_open_time,
                        "new_range_start_time": candidate_range_start_time,
                        "reason": "NEW_RANGE_AFTER_TOLERANCE_RESET",
                    },
                    candidate.symbol,
                )
                continue
            if state.reset_open_time is None:
                if not self.recorder.mark_n08_structure_reset(
                    state.id,
                    reset_open_time,
                    "RANGE_TOLERANCE_BOUNDARY_BROKEN",
                ):
                    return None
                self.recorder.record_event(
                    "n08_structure_reset_observed",
                    {
                        "strategy_id": strategy.strategy_id,
                        "structure_id": state.structure_id,
                        "reset_open_time": reset_open_time,
                        "reason": "RANGE_TOLERANCE_BOUNDARY_BROKEN",
                    },
                    candidate.symbol,
                )
                if n08_states_by_symbol is not None:
                    state = replace(state, reset_open_time=reset_open_time)
            active.append(state)
        if n08_states_by_symbol is not None:
            n08_states_by_symbol[candidate.symbol] = list(active)
        return active

    def _consume_n08_structure(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        analysis: N08AnalysisResult,
        reason: str,
    ) -> bool:
        structure = analysis.structure
        if structure is None or not analysis.bullish_streak:
            return False
        reset_open_time = next(
            (
                candle.open_time
                for candle in analysis.bullish_streak
                if candle.low < structure.lower_tolerance_boundary
            ),
            None,
        )
        persisted = self.recorder.record_n08_structure_consumed(
            strategy_id=strategy.strategy_id,
            symbol=candidate.symbol,
            structure_id=structure.structure_id,
            range_start_time=structure.start_time,
            range_end_time=structure.end_time,
            upper_reference=str(structure.upper_reference),
            lower_reference=str(structure.lower_reference),
            upper_tolerance_boundary=str(structure.upper_tolerance_boundary),
            lower_tolerance_boundary=str(structure.lower_tolerance_boundary),
            first_streak_start_time=analysis.bullish_streak[0].open_time,
            first_streak_end_time=analysis.bullish_streak[-1].open_time,
            reason=reason,
            detail=analysis.detail_json(),
            reset_open_time=reset_open_time,
        )
        if persisted:
            self.recorder.record_event(
                "n08_structure_consumed",
                {
                    "strategy_id": strategy.strategy_id,
                    "structure_id": structure.structure_id,
                    "reason": reason,
                    "first_streak_start_time": analysis.bullish_streak[0].open_time,
                    "first_streak_end_time": analysis.bullish_streak[-1].open_time,
                },
                candidate.symbol,
            )
        return persisted

    def _rejected(
        self,
        strategy: StrategyConfig,
        candidate: FundingCandidate,
        reason: str,
        analysis: StrategyAnalysis | None = None,
    ) -> StrategySignalDecision:
        return StrategySignalDecision(
            strategy=strategy,
            candidate=candidate,
            analysis=analysis,
            passed=False,
            decision="REJECTED",
            reason=reason,
        )

    def _signal_record_payload(
        self,
        decision: StrategySignalDecision,
    ) -> dict[str, Any]:
        analysis = decision.analysis
        matched_patterns: tuple[str, ...] = ()
        trend_slope = ""
        current_bullish = False
        structure_id = None
        detail: dict[str, Any] = {
            "candidate_universe": decision.candidate.candidate_universe,
            "quote_volume": str(decision.candidate.quote_volume)
            if decision.candidate.quote_volume is not None
            else None,
            "quote_volume_rank": decision.candidate.quote_volume_rank,
        }

        if isinstance(analysis, AnalysisResult):
            matched_patterns = analysis.matched_patterns
            trend_slope = str(analysis.trend_slope)
            current_bullish = analysis.current_bullish
            detail["analysis"] = analysis.detail
        elif isinstance(analysis, (N06AnalysisResult, N07AnalysisResult)):
            current_bullish = analysis.current_bullish
            structure_id = analysis.structure_id
            detail.update(analysis.detail_json())
            if analysis.structure is not None:
                risk_distance = decision.candidate.mark_price - analysis.structure.p1
                detail.update(
                    {
                        "entry_price": str(decision.candidate.mark_price),
                        "stop_loss_price": str(analysis.structure.p1),
                        "take_profit_price": str(
                            decision.candidate.mark_price
                            + decision.strategy.risk_reward_ratio * risk_distance
                        ),
                    }
                )
        elif isinstance(
            analysis,
            (
                N08AnalysisResult,
                N09AnalysisResult,
                N10AnalysisResult,
                N11AnalysisResult,
                N12AnalysisResult,
                N13AnalysisResult,
                N14AnalysisResult,
                N15AnalysisResult,
                N16AnalysisResult,
                N17AnalysisResult,
                N18AnalysisResult,
                N19AnalysisResult,
                N20AnalysisResult,
                MicroAnalysisResult,
            ),
        ):
            current_bullish = analysis.current_bullish
            structure_id = analysis.structure_id
            detail.update(analysis.detail_json())

        funding_rate = (
            str(decision.candidate.funding_rate)
            if decision.candidate.funding_rate is not None
            else ""
        )
        return {
            "strategy_id": decision.strategy.strategy_id,
            "symbol": decision.candidate.symbol,
            "funding_rate": funding_rate,
            "matched_patterns": matched_patterns,
            "trend_slope": trend_slope,
            "current_bullish": current_bullish,
            "passed": decision.passed,
            "decision": decision.decision,
            "reason": decision.reason,
            "structure_id": structure_id,
            "detail": detail,
        }

    def _record_signal(
        self,
        scan_id: int | None,
        decision: StrategySignalDecision,
    ) -> int | None:
        return self.recorder.record_strategy_signal(
            scan_id=scan_id,
            **self._signal_record_payload(decision),
        )

    def choose_live_candidate(
        self,
        live_candidates: list[LiveTradeCandidate],
        live_blocked: bool,
    ) -> LiveTradeCandidate | None:
        if live_blocked or not live_candidates:
            return None

        eligible_candidates = [
            candidate
            for candidate in live_candidates
            if candidate.state is None
            or not candidate.state.live_result_pending
        ]
        if not eligible_candidates:
            return None

        def priority(candidate: LiveTradeCandidate):
            base: tuple[Any, ...] = (candidate.signal.strategy.strategy_id,)
            analysis = candidate.signal.analysis
            if isinstance(analysis, N14AnalysisResult):
                rank = (
                    analysis.structure.quote_volume_rank
                    if analysis.structure is not None
                    else None
                )
                return (
                    *base,
                    -analysis.flow_flip,
                    -analysis.confirmation_close_location,
                    rank if rank is not None else 10**9,
                    candidate.signal.candidate.symbol,
                )
            return (*base, candidate.signal.candidate.symbol)

        return min(eligible_candidates, key=priority)
