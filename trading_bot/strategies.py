from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class StrategyConfig:
    strategy_id: str
    name: str
    allowed_patterns: tuple[str, ...]
    funding_threshold: Decimal | None
    loss_symbol_cooldown_hours: int
    enabled: bool = True
    market_filter: str = "negative_funding"
    evaluator_type: str = "abc_patterns"
    stop_mode: str = "amplitude"
    risk_reward_ratio: Decimal = Decimal("5")
    volume_top_n: int | None = None
    pivot_left: int = 2
    pivot_right: int = 2
    stable_candle_count: int = 5
    entry_distance_min: Decimal = Decimal("0.01")
    entry_distance_max: Decimal = Decimal("0.015")
    pullback_min_fraction: Decimal = Decimal("0.06")
    swing_segment_min_bars: int = 5
    swing_segment_atr_period: int = 14
    swing_segment_min_atr_multiple: Decimal = Decimal("1.2")
    swing_segment_min_efficiency: Decimal = Decimal("0.35")
    range_min_bars: int = 20
    range_max_bars: int = 96
    range_tolerance_fraction: Decimal = Decimal("0.25")
    bullish_streak_count: int = 5
    entry_window_seconds: int = 120
    slow_decline_min_bars: int = 20
    slow_decline_min_r_squared: Decimal = Decimal("0.65")
    slow_decline_max_bearish_body_fraction: Decimal = Decimal("0.20")
    slow_decline_max_rebound_fraction: Decimal = Decimal("0.20")
    slow_decline_sideways_window: int = 8
    slow_decline_sideways_max_net_drop_fraction: Decimal = Decimal("0.05")
    slow_decline_rebound_max_bars: int = 40
    retrace_touch_fraction: Decimal = Decimal("0.50")
    retrace_entry_floor_fraction: Decimal = Decimal("0.43")
    support_window_bars: int = 48
    support_tolerance_fraction: Decimal = Decimal("0.005")
    support_touch_min_gap: int = 4
    volume_median_bars: int = 20
    volume_spike_multiple: Decimal = Decimal("2.5")
    sweep_depth_min: Decimal = Decimal("0.003")
    sweep_depth_max: Decimal = Decimal("0.015")
    lower_wick_ratio_min: Decimal = Decimal("0.50")
    taker_buy_ratio_min: Decimal = Decimal("0.55")
    entry_extension_max: Decimal = Decimal("0.015")
    squeeze_bars: int = 8
    breakout_lookback_bars: int = 20
    bollinger_stddevs: Decimal = Decimal("2")
    keltner_atr_multiple: Decimal = Decimal("1.5")
    breakout_body_atr_min: Decimal = Decimal("0.6")
    breakout_close_location_min: Decimal = Decimal("0.75")
    breakout_volume_multiple_min: Decimal = Decimal("1.8")
    breakout_taker_buy_ratio_min: Decimal = Decimal("0.55")
    retest_max_bars: int = 6
    retest_atr_tolerance: Decimal = Decimal("0.3")
    retest_close_location_min: Decimal = Decimal("0.5")
    retest_volume_ratio_max: Decimal = Decimal("0.8")
    entry_extension_atr_max: Decimal = Decimal("0.5")
    relative_strength_lookback_bars: int = 96
    relative_strength_top_n: int = 10
    n12_up_min_bars: int = 5
    n12_up_min_gain_fraction: Decimal = Decimal("0.03")
    n12_up_min_atr_multiple: Decimal = Decimal("3")
    n12_up_min_efficiency: Decimal = Decimal("0.50")
    n12_up_max_bullish_body_fraction: Decimal = Decimal("0.50")
    n12_pullback_min_bars: int = 2
    n12_pullback_max_bars: int = 5
    n12_pullback_depth_min: Decimal = Decimal("0.20")
    n12_pullback_depth_max: Decimal = Decimal("0.45")
    n12_pullback_close_floor_fraction: Decimal = Decimal("0.50")
    n12_pullback_volume_ratio_max: Decimal = Decimal("0.70")
    n12_confirmation_close_location_min: Decimal = Decimal("0.75")
    n12_confirmation_taker_buy_ratio_min: Decimal = Decimal("0.55")
    n12_confirmation_volume_multiple_min: Decimal = Decimal("1.2")
    n12_entry_extension_atr_max: Decimal = Decimal("0.5")
    n13_positive_breadth_min: Decimal = Decimal("0.60")
    n13_above_vwap_breadth_min: Decimal = Decimal("0.50")
    n13_rank_min: int = 11
    n13_rank_max: int = 60
    n13_vwap_lookback_bars: int = 96
    n13_atr_period: int = 14
    n13_vwap_slope_lookback_bars: int = 8
    n13_upper_atr_fraction: Decimal = Decimal("0.25")
    n13_lower_atr_fraction: Decimal = Decimal("0.50")
    n13_armed_max_bars: int = 8
    n13_confirmation_max_bars: int = 4
    n13_confirmation_close_location_min: Decimal = Decimal("0.60")
    n13_confirmation_taker_buy_ratio_min: Decimal = Decimal("0.50")
    n13_entry_extension_atr_max: Decimal = Decimal("0.50")
    n14_atr_period: int = 14
    n14_volume_median_bars: int = 20
    n14_one_hour_bars: int = 4
    n14_market_1h_median_min: Decimal = Decimal("-0.0075")
    n14_rank_down_min: int = 1
    n14_rank_down_max: int = 20
    n14_residual_atr_multiple: Decimal = Decimal("1.0")
    n14_systemic_red_breadth_min: Decimal = Decimal("0.75")
    n14_systemic_median_return_atr_multiple: Decimal = Decimal("0.50")
    n14_shock_body_atr_min: Decimal = Decimal("0.80")
    n14_shock_volume_multiple_min: Decimal = Decimal("1.50")
    n14_shock_taker_buy_ratio_max: Decimal = Decimal("0.40")
    n14_shock_close_location_max: Decimal = Decimal("0.30")
    n14_prior_shock_lookback: int = 3
    n14_absorption_volume_multiple_min: Decimal = Decimal("0.80")
    n14_absorption_taker_buy_ratio_max: Decimal = Decimal("0.45")
    n14_absorption_range_ratio_max: Decimal = Decimal("0.80")
    n14_absorption_low_atr_tolerance: Decimal = Decimal("0.20")
    n14_absorption_close_atr_tolerance: Decimal = Decimal("0.20")
    n14_confirmation_max_bars: int = 2
    n14_confirmation_shock_midpoint_fraction: Decimal = Decimal("0.50")
    n14_confirmation_close_location_min: Decimal = Decimal("0.70")
    n14_confirmation_taker_buy_ratio_min: Decimal = Decimal("0.55")
    n14_confirmation_volume_multiple_min: Decimal = Decimal("0.80")
    n14_cascade_breadth_min: Decimal = Decimal("0.40")
    n14_cascade_improvement_min: Decimal = Decimal("0.15")
    n14_entry_extension_atr_max: Decimal = Decimal("0.50")
    n15_atr_period: int = 14
    n15_volume_median_bars: int = 20
    n15_weak_down_breadth_min: Decimal = Decimal("0.55")
    n15_weak_median_move_max: Decimal = Decimal("-0.10")
    n15_crash_down_breadth_min: Decimal = Decimal("0.75")
    n15_crash_median_move_max: Decimal = Decimal("-0.50")
    n15_b_rank_max: int = 25
    n15_b_move_min: Decimal = Decimal("-0.25")
    n15_recovery_up_breadth_min: Decimal = Decimal("0.55")
    n15_recovery_improvement_min: Decimal = Decimal("0.20")
    n15_c_rank_max: int = 25
    n15_c_close_location_min: Decimal = Decimal("0.60")
    n15_c_taker_buy_ratio_min: Decimal = Decimal("0.52")
    n15_c_volume_median_multiple_min: Decimal = Decimal("0.80")
    n15_entry_extension_atr_max: Decimal = Decimal("0.50")

    def to_jsonable(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["funding_threshold"] = str(self.funding_threshold) if self.funding_threshold is not None else None
        payload["allowed_patterns"] = list(self.allowed_patterns)
        payload["risk_reward_ratio"] = str(self.risk_reward_ratio)
        payload["entry_distance_min"] = str(self.entry_distance_min)
        payload["entry_distance_max"] = str(self.entry_distance_max)
        payload["pullback_min_fraction"] = str(self.pullback_min_fraction)
        payload["swing_segment_min_atr_multiple"] = str(
            self.swing_segment_min_atr_multiple
        )
        payload["swing_segment_min_efficiency"] = str(
            self.swing_segment_min_efficiency
        )
        payload["range_tolerance_fraction"] = str(self.range_tolerance_fraction)
        payload["slow_decline_min_r_squared"] = str(self.slow_decline_min_r_squared)
        payload["slow_decline_max_bearish_body_fraction"] = str(
            self.slow_decline_max_bearish_body_fraction
        )
        payload["slow_decline_max_rebound_fraction"] = str(
            self.slow_decline_max_rebound_fraction
        )
        payload["slow_decline_sideways_max_net_drop_fraction"] = str(
            self.slow_decline_sideways_max_net_drop_fraction
        )
        payload["retrace_touch_fraction"] = str(self.retrace_touch_fraction)
        payload["retrace_entry_floor_fraction"] = str(
            self.retrace_entry_floor_fraction
        )
        for field_name in (
            "support_tolerance_fraction",
            "volume_spike_multiple",
            "sweep_depth_min",
            "sweep_depth_max",
            "lower_wick_ratio_min",
            "taker_buy_ratio_min",
            "entry_extension_max",
            "bollinger_stddevs",
            "keltner_atr_multiple",
            "breakout_body_atr_min",
            "breakout_close_location_min",
            "breakout_volume_multiple_min",
            "breakout_taker_buy_ratio_min",
            "retest_atr_tolerance",
            "retest_close_location_min",
            "retest_volume_ratio_max",
            "entry_extension_atr_max",
            "n12_up_min_gain_fraction",
            "n12_up_min_atr_multiple",
            "n12_up_min_efficiency",
            "n12_up_max_bullish_body_fraction",
            "n12_pullback_depth_min",
            "n12_pullback_depth_max",
            "n12_pullback_close_floor_fraction",
            "n12_pullback_volume_ratio_max",
            "n12_confirmation_close_location_min",
            "n12_confirmation_taker_buy_ratio_min",
            "n12_confirmation_volume_multiple_min",
            "n12_entry_extension_atr_max",
            "n13_positive_breadth_min",
            "n13_above_vwap_breadth_min",
            "n13_upper_atr_fraction",
            "n13_lower_atr_fraction",
            "n13_confirmation_close_location_min",
            "n13_confirmation_taker_buy_ratio_min",
            "n13_entry_extension_atr_max",
            "n14_market_1h_median_min",
            "n14_residual_atr_multiple",
            "n14_systemic_red_breadth_min",
            "n14_systemic_median_return_atr_multiple",
            "n14_shock_body_atr_min",
            "n14_shock_volume_multiple_min",
            "n14_shock_taker_buy_ratio_max",
            "n14_shock_close_location_max",
            "n14_absorption_volume_multiple_min",
            "n14_absorption_taker_buy_ratio_max",
            "n14_absorption_range_ratio_max",
            "n14_absorption_low_atr_tolerance",
            "n14_absorption_close_atr_tolerance",
            "n14_confirmation_shock_midpoint_fraction",
            "n14_confirmation_close_location_min",
            "n14_confirmation_taker_buy_ratio_min",
            "n14_confirmation_volume_multiple_min",
            "n14_cascade_breadth_min",
            "n14_cascade_improvement_min",
            "n14_entry_extension_atr_max",
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
        ):
            payload[field_name] = str(getattr(self, field_name))
        return payload


@dataclass(frozen=True)
class N16StrategyDefinition:
    """N16-only frozen definition; it must not change N01-N15 JSON."""

    strategy_id: str = "N16"
    name: str = "成熟趋势动态支撑续涨"
    allowed_patterns: tuple[str, ...] = ()
    funding_threshold: Decimal | None = None
    loss_symbol_cooldown_hours: int = 0
    enabled: bool = True
    market_filter: str = "quote_volume_top"
    evaluator_type: str = "mature_trend_dynamic_support_continuation"
    stop_mode: str = "trend_support_continuation_margin_capped"
    risk_reward_ratio: Decimal = Decimal("5")
    volume_top_n: int = 100
    entry_window_seconds: int = 120
    fixed_input_bars: int = 122
    closed_logic_bars: int = 96
    pivot_left: int = 2
    pivot_right: int = 2
    mature_min_bars: int = 16
    h2_progress_atr_min: Decimal = Decimal("0.25")
    ema_fast_period: int = 20
    ema_slow_period: int = 50
    ema_slope_lookback_bars: int = 8
    atr_period: int = 14
    up_leg_atr_min: Decimal = Decimal("2")
    up_leg_efficiency_min: Decimal = Decimal("0.40")
    support_min_bars: int = 2
    support_max_bars: int = 8
    support_touch_upper_atr: Decimal = Decimal("0.25")
    support_close_lower_atr: Decimal = Decimal("0.20")
    pullback_depth_min: Decimal = Decimal("0.15")
    pullback_depth_max: Decimal = Decimal("0.45")
    pullback_volume_ratio_max: Decimal = Decimal("0.90")
    confirmation_max_bars: int = 3
    confirmation_close_location_min: Decimal = Decimal("0.65")
    confirmation_taker_buy_ratio_min: Decimal = Decimal("0.52")
    confirmation_volume_multiple_min: Decimal = Decimal("0.90")
    entry_extension_atr_max: Decimal = Decimal("0.50")

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "name": self.name,
            "allowed_patterns": list(self.allowed_patterns),
            "funding_threshold": None,
            "loss_symbol_cooldown_hours": self.loss_symbol_cooldown_hours,
            "enabled": self.enabled,
            "market_filter": self.market_filter,
            "evaluator_type": self.evaluator_type,
            "stop_mode": self.stop_mode,
            "risk_reward_ratio": str(self.risk_reward_ratio),
            "volume_top_n": self.volume_top_n,
            "entry_window_seconds": self.entry_window_seconds,
            "fixed_input_bars": self.fixed_input_bars,
            "closed_logic_bars": self.closed_logic_bars,
            "pivot_left": self.pivot_left,
            "pivot_right": self.pivot_right,
            "mature_min_bars": self.mature_min_bars,
            "h2_progress_atr_min": str(self.h2_progress_atr_min),
            "ema_fast_period": self.ema_fast_period,
            "ema_slow_period": self.ema_slow_period,
            "ema_slope_lookback_bars": self.ema_slope_lookback_bars,
            "atr_period": self.atr_period,
            "up_leg_atr_min": str(self.up_leg_atr_min),
            "up_leg_efficiency_min": str(self.up_leg_efficiency_min),
            "support_min_bars": self.support_min_bars,
            "support_max_bars": self.support_max_bars,
            "support_touch_upper_atr": str(self.support_touch_upper_atr),
            "support_close_lower_atr": str(self.support_close_lower_atr),
            "pullback_depth_min": str(self.pullback_depth_min),
            "pullback_depth_max": str(self.pullback_depth_max),
            "pullback_volume_ratio_max": str(self.pullback_volume_ratio_max),
            "confirmation_max_bars": self.confirmation_max_bars,
            "confirmation_close_location_min": str(
                self.confirmation_close_location_min
            ),
            "confirmation_taker_buy_ratio_min": str(
                self.confirmation_taker_buy_ratio_min
            ),
            "confirmation_volume_multiple_min": str(
                self.confirmation_volume_multiple_min
            ),
            "entry_extension_atr_max": str(self.entry_extension_atr_max),
        }


@dataclass(frozen=True)
class N17StrategyDefinition:
    """N17-only frozen definition; it must not change N01-N16 JSON."""

    strategy_id: str = "N17"
    name: str = "箱体下沿正常承接反弹"
    allowed_patterns: tuple[str, ...] = ()
    funding_threshold: Decimal | None = None
    loss_symbol_cooldown_hours: int = 0
    enabled: bool = True
    market_filter: str = "quote_volume_top"
    evaluator_type: str = "range_lower_support_absorption_rebound"
    stop_mode: str = "range_support_absorption_margin_capped"
    risk_reward_ratio: Decimal = Decimal("5")
    volume_top_n: int = 100
    entry_window_seconds: int = 120
    fixed_input_bars: int = 122
    pivot_left: int = 2
    pivot_right: int = 2
    atr_period: int = 14
    box_min_bars: int = 20
    box_max_bars: int = 64
    box_tolerance_fraction: Decimal = Decimal("0.25")
    box_net_move_fraction_max: Decimal = Decimal("0.50")
    touch_breakdown_fraction: Decimal = Decimal("0.003")
    touch_upper_atr: Decimal = Decimal("0.20")
    touch_volume_multiple_max: Decimal = Decimal("2.5")
    panic_body_atr_min: Decimal = Decimal("0.8")
    panic_volume_multiple_min: Decimal = Decimal("1.5")
    panic_taker_buy_ratio_max: Decimal = Decimal("0.40")
    panic_close_location_max: Decimal = Decimal("0.30")
    absorption_range_ratio_max: Decimal = Decimal("0.85")
    absorption_sell_quote_ratio_max: Decimal = Decimal("0.85")
    confirmation_max_bars: int = 2
    confirmation_close_location_min: Decimal = Decimal("0.65")
    confirmation_taker_buy_ratio_min: Decimal = Decimal("0.52")
    confirmation_volume_multiple_min: Decimal = Decimal("0.80")
    n08_bullish_streak_count: int = 5
    entry_extension_atr_max: Decimal = Decimal("0.50")

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "strategy_id": self.strategy_id,
            "name": self.name,
            "allowed_patterns": [],
            "funding_threshold": None,
            "loss_symbol_cooldown_hours": self.loss_symbol_cooldown_hours,
            "enabled": self.enabled,
            "market_filter": self.market_filter,
            "evaluator_type": self.evaluator_type,
            "stop_mode": self.stop_mode,
            "risk_reward_ratio": str(self.risk_reward_ratio),
            "volume_top_n": self.volume_top_n,
            "entry_window_seconds": self.entry_window_seconds,
            "fixed_input_bars": self.fixed_input_bars,
            "pivot_left": self.pivot_left,
            "pivot_right": self.pivot_right,
            "atr_period": self.atr_period,
            "box_min_bars": self.box_min_bars,
            "box_max_bars": self.box_max_bars,
            "confirmation_max_bars": self.confirmation_max_bars,
            "n08_bullish_streak_count": self.n08_bullish_streak_count,
        }
        for field_name in (
            "box_tolerance_fraction",
            "box_net_move_fraction_max",
            "touch_breakdown_fraction",
            "touch_upper_atr",
            "touch_volume_multiple_max",
            "panic_body_atr_min",
            "panic_volume_multiple_min",
            "panic_taker_buy_ratio_max",
            "panic_close_location_max",
            "absorption_range_ratio_max",
            "absorption_sell_quote_ratio_max",
            "confirmation_close_location_min",
            "confirmation_taker_buy_ratio_min",
            "confirmation_volume_multiple_min",
            "entry_extension_atr_max",
        ):
            payload[field_name] = str(getattr(self, field_name))
        return payload


@dataclass(frozen=True)
class N18StrategyDefinition:
    """N18-only frozen definition; it must not change older strategy JSON."""

    strategy_id: str = "N18"
    name: str = "上升三角压力吸收突破"
    allowed_patterns: tuple[str, ...] = ()
    funding_threshold: Decimal | None = None
    loss_symbol_cooldown_hours: int = 0
    enabled: bool = True
    market_filter: str = "quote_volume_top"
    evaluator_type: str = "ascending_triangle_pressure_absorption_breakout"
    stop_mode: str = "ascending_triangle_breakout_margin_capped"
    risk_reward_ratio: Decimal = Decimal("5")
    volume_top_n: int = 100
    entry_window_seconds: int = 120
    fixed_input_bars: int = 122
    pivot_left: int = 2
    pivot_right: int = 2
    atr_period: int = 14
    ema_fast_period: int = 20
    ema_slow_period: int = 50
    triangle_span_min_bars: int = 12
    triangle_span_max_bars: int = 36
    pressure_interval_min_bars: int = 3
    pressure_dispersion_atr_max: Decimal = Decimal("0.35")
    pressure_dispersion_fraction_max: Decimal = Decimal("0.005")
    a_lower_atr: Decimal = Decimal("0.25")
    a_upper_atr: Decimal = Decimal("0.10")
    higher_low_progress_atr_min: Decimal = Decimal("0.20")
    initial_height_atr_min: Decimal = Decimal("2")
    convergence_ratio_max: Decimal = Decimal("0.65")
    a_close_location_min: Decimal = Decimal("0.60")
    a_taker_buy_ratio_min: Decimal = Decimal("0.50")
    a_volume_multiple_min: Decimal = Decimal("0.80")
    breakout_max_bars: int = 4
    breakout_threshold_atr: Decimal = Decimal("0.10")
    breakout_body_atr_min: Decimal = Decimal("0.40")
    breakout_close_location_min: Decimal = Decimal("0.75")
    breakout_taker_buy_ratio_min: Decimal = Decimal("0.55")
    breakout_volume_multiple_min: Decimal = Decimal("1.30")
    entry_extension_atr_max: Decimal = Decimal("0.50")

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "strategy_id": self.strategy_id,
            "name": self.name,
            "allowed_patterns": [],
            "funding_threshold": None,
            "loss_symbol_cooldown_hours": self.loss_symbol_cooldown_hours,
            "enabled": self.enabled,
            "market_filter": self.market_filter,
            "evaluator_type": self.evaluator_type,
            "stop_mode": self.stop_mode,
            "risk_reward_ratio": str(self.risk_reward_ratio),
            "volume_top_n": self.volume_top_n,
            "entry_window_seconds": self.entry_window_seconds,
            "fixed_input_bars": self.fixed_input_bars,
            "pivot_left": self.pivot_left,
            "pivot_right": self.pivot_right,
            "atr_period": self.atr_period,
            "ema_fast_period": self.ema_fast_period,
            "ema_slow_period": self.ema_slow_period,
            "triangle_span_min_bars": self.triangle_span_min_bars,
            "triangle_span_max_bars": self.triangle_span_max_bars,
            "pressure_interval_min_bars": self.pressure_interval_min_bars,
            "breakout_max_bars": self.breakout_max_bars,
        }
        for name in (
            "pressure_dispersion_atr_max", "pressure_dispersion_fraction_max",
            "a_lower_atr", "a_upper_atr", "higher_low_progress_atr_min",
            "initial_height_atr_min", "convergence_ratio_max",
            "a_close_location_min", "a_taker_buy_ratio_min",
            "a_volume_multiple_min", "breakout_threshold_atr",
            "breakout_body_atr_min", "breakout_close_location_min",
            "breakout_taker_buy_ratio_min", "breakout_volume_multiple_min",
            "entry_extension_atr_max",
        ):
            payload[name] = str(getattr(self, name))
        return payload


@dataclass(frozen=True)
class N19StrategyDefinition:
    """N19-only frozen definition; it must not change N01-N17 JSON."""

    strategy_id: str = "N19"
    name: str = "中速阶梯下跌衰竭反转"
    allowed_patterns: tuple[str, ...] = ()
    funding_threshold: Decimal | None = None
    loss_symbol_cooldown_hours: int = 0
    enabled: bool = True
    market_filter: str = "quote_volume_top"
    evaluator_type: str = "medium_staircase_decline_exhaustion_reversal"
    stop_mode: str = "staircase_exhaustion_margin_capped"
    risk_reward_ratio: Decimal = Decimal("5")
    volume_top_n: int = 100
    entry_window_seconds: int = 120
    fixed_input_bars: int = 122
    pivot_left: int = 1
    pivot_right: int = 1
    atr_period: int = 14
    structure_min_bars: int = 10
    structure_max_bars: int = 19
    total_drop_atr_min: Decimal = Decimal("2")
    total_drop_atr_max: Decimal = Decimal("6")
    lower_low_progress_atr_min: Decimal = Decimal("0.35")
    exhaustion_extension_atr_max: Decimal = Decimal("0.50")
    lower_high_progress_atr_min: Decimal = Decimal("0.20")
    rebound_ratio_min: Decimal = Decimal("0.20")
    rebound_ratio_max: Decimal = Decimal("0.55")
    max_bearish_body_drop_fraction: Decimal = Decimal("0.40")
    exhaustion_range_atr_max: Decimal = Decimal("0.90")
    exhaustion_volume_ratio_max: Decimal = Decimal("0.85")
    exhaustion_taker_buy_ratio_min: Decimal = Decimal("0.45")
    exhaustion_taker_buy_improvement_min: Decimal = Decimal("0.08")
    confirmation_max_bars: int = 3
    confirmation_close_location_min: Decimal = Decimal("0.70")
    confirmation_taker_buy_ratio_min: Decimal = Decimal("0.55")
    confirmation_volume_multiple_min: Decimal = Decimal("0.80")
    crash_down_breadth_min: Decimal = Decimal("0.75")
    crash_median_return_max: Decimal = Decimal("-0.01")
    entry_extension_atr_max: Decimal = Decimal("0.50")

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "name": self.name,
            "allowed_patterns": [],
            "funding_threshold": None,
            "loss_symbol_cooldown_hours": self.loss_symbol_cooldown_hours,
            "enabled": self.enabled,
            "market_filter": self.market_filter,
            "evaluator_type": self.evaluator_type,
            "stop_mode": self.stop_mode,
            "risk_reward_ratio": str(self.risk_reward_ratio),
            "volume_top_n": self.volume_top_n,
            "entry_window_seconds": self.entry_window_seconds,
            "fixed_input_bars": self.fixed_input_bars,
            "pivot_left": self.pivot_left,
            "pivot_right": self.pivot_right,
            "atr_period": self.atr_period,
            "structure_min_bars": self.structure_min_bars,
            "structure_max_bars": self.structure_max_bars,
            "total_drop_atr_min": str(self.total_drop_atr_min),
            "total_drop_atr_max": str(self.total_drop_atr_max),
            "lower_low_progress_atr_min": str(
                self.lower_low_progress_atr_min
            ),
            "exhaustion_extension_atr_max": str(
                self.exhaustion_extension_atr_max
            ),
            "lower_high_progress_atr_min": str(
                self.lower_high_progress_atr_min
            ),
            "rebound_ratio_min": str(self.rebound_ratio_min),
            "rebound_ratio_max": str(self.rebound_ratio_max),
            "max_bearish_body_drop_fraction": str(
                self.max_bearish_body_drop_fraction
            ),
            "exhaustion_range_atr_max": str(self.exhaustion_range_atr_max),
            "exhaustion_volume_ratio_max": str(
                self.exhaustion_volume_ratio_max
            ),
            "exhaustion_taker_buy_ratio_min": str(
                self.exhaustion_taker_buy_ratio_min
            ),
            "exhaustion_taker_buy_improvement_min": str(
                self.exhaustion_taker_buy_improvement_min
            ),
            "confirmation_max_bars": self.confirmation_max_bars,
            "confirmation_close_location_min": str(
                self.confirmation_close_location_min
            ),
            "confirmation_taker_buy_ratio_min": str(
                self.confirmation_taker_buy_ratio_min
            ),
            "confirmation_volume_multiple_min": str(
                self.confirmation_volume_multiple_min
            ),
            "crash_down_breadth_min": str(self.crash_down_breadth_min),
            "crash_median_return_max": str(self.crash_median_return_max),
            "entry_extension_atr_max": str(self.entry_extension_atr_max),
        }


@dataclass(frozen=True)
class N20StrategyDefinition:
    """N20-only frozen definition; older strategy JSON remains unchanged."""

    strategy_id: str = "N20"
    name: str = "牛市回调中的相对强势领涨恢复"
    allowed_patterns: tuple[str, ...] = ()
    funding_threshold: Decimal | None = None
    loss_symbol_cooldown_hours: int = 0
    enabled: bool = True
    market_filter: str = "quote_volume_top"
    evaluator_type: str = "bull_market_pullback_relative_strength_recovery"
    stop_mode: str = "relative_strength_recovery_margin_capped"
    risk_reward_ratio: Decimal = Decimal("5")
    volume_top_n: int = 100
    entry_window_seconds: int = 120
    fixed_input_bars: int = 122
    atr_period: int = 14
    vwap_long_period: int = 96
    vwap_short_period: int = 20
    baseline_rank_max: int = 20
    resilience_rank_max: int = 20
    m0_positive_breadth_min: Decimal = Decimal("0.65")
    m0_above_vwap_breadth_min: Decimal = Decimal("0.60")
    d1_down_breadth_min: Decimal = Decimal("0.55")
    d1_median_return_max: Decimal = Decimal("-0.0015")
    pullback_min_bars: int = 2
    pullback_max_bars: int = 6
    pullback_depth_min: Decimal = Decimal("0.004")
    pullback_depth_max: Decimal = Decimal("0.025")
    crash_down_breadth_min: Decimal = Decimal("0.80")
    crash_depth_min: Decimal = Decimal("0.030")
    recovery_up_breadth_min: Decimal = Decimal("0.55")
    recovery_breadth_improvement_min: Decimal = Decimal("0.20")
    relative_resilience_min: Decimal = Decimal("0.50")
    candidate_drawdown_atr_max: Decimal = Decimal("1.50")
    candidate_recovery_ratio_min: Decimal = Decimal("0.50")
    candidate_close_location_min: Decimal = Decimal("0.65")
    candidate_taker_buy_ratio_min: Decimal = Decimal("0.52")
    candidate_volume_multiple_min: Decimal = Decimal("0.80")
    entry_extension_atr_max: Decimal = Decimal("0.50")
    canonical_evidence_max_bytes: int = 8 * 1024 * 1024
    compressed_evidence_max_bytes: int = 2 * 1024 * 1024

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "strategy_id": self.strategy_id,
            "name": self.name,
            "allowed_patterns": [],
            "funding_threshold": None,
            "loss_symbol_cooldown_hours": self.loss_symbol_cooldown_hours,
            "enabled": self.enabled,
            "market_filter": self.market_filter,
            "evaluator_type": self.evaluator_type,
            "stop_mode": self.stop_mode,
            "risk_reward_ratio": str(self.risk_reward_ratio),
            "volume_top_n": self.volume_top_n,
            "entry_window_seconds": self.entry_window_seconds,
            "fixed_input_bars": self.fixed_input_bars,
            "atr_period": self.atr_period,
            "vwap_long_period": self.vwap_long_period,
            "vwap_short_period": self.vwap_short_period,
            "baseline_rank_max": self.baseline_rank_max,
            "resilience_rank_max": self.resilience_rank_max,
            "pullback_min_bars": self.pullback_min_bars,
            "pullback_max_bars": self.pullback_max_bars,
            "canonical_evidence_max_bytes": self.canonical_evidence_max_bytes,
            "compressed_evidence_max_bytes": self.compressed_evidence_max_bytes,
        }
        for name in (
            "m0_positive_breadth_min", "m0_above_vwap_breadth_min",
            "d1_down_breadth_min", "d1_median_return_max",
            "pullback_depth_min", "pullback_depth_max",
            "crash_down_breadth_min", "crash_depth_min",
            "recovery_up_breadth_min", "recovery_breadth_improvement_min",
            "relative_resilience_min", "candidate_drawdown_atr_max",
            "candidate_recovery_ratio_min", "candidate_close_location_min",
            "candidate_taker_buy_ratio_min", "candidate_volume_multiple_min",
            "entry_extension_atr_max",
        ):
            payload[name] = str(getattr(self, name))
        return payload


@dataclass(frozen=True)
class MicroStrategyDefinition:
    """N21-N25-only definition; N01-N20 serialization stays byte-for-byte stable."""

    strategy_id: str
    name: str
    evaluator_type: str
    stop_mode: str
    entry_extension_atr_max: Decimal
    allowed_patterns: tuple[str, ...] = ()
    funding_threshold: Decimal | None = None
    loss_symbol_cooldown_hours: int = 0
    enabled: bool = True
    market_filter: str = "quote_volume_top"
    risk_reward_ratio: Decimal = Decimal("5")
    volume_top_n: int = 100
    entry_window_seconds: int = 120
    observation_limit: int = 4
    evidence_max_bytes: int = 8 * 1024

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id, "name": self.name,
            "allowed_patterns": [], "funding_threshold": None,
            "loss_symbol_cooldown_hours": 0, "enabled": self.enabled,
            "market_filter": self.market_filter,
            "evaluator_type": self.evaluator_type, "stop_mode": self.stop_mode,
            "risk_reward_ratio": "5", "volume_top_n": 100,
            "entry_window_seconds": 120, "observation_limit": 4,
            "evidence_max_bytes": 8192,
            "entry_extension_atr_max": str(self.entry_extension_atr_max),
        }


FIRST_STAGE_STRATEGIES: tuple[StrategyConfig, ...] = (
    StrategyConfig(
        strategy_id="N01",
        name="C-only funding -1.5 cooldown 24h",
        allowed_patterns=("C",),
        funding_threshold=Decimal("0.015"),
        loss_symbol_cooldown_hours=24,
    ),
    StrategyConfig(
        strategy_id="N02",
        name="C-only funding -1.5 cooldown 48h",
        allowed_patterns=("C",),
        funding_threshold=Decimal("0.015"),
        loss_symbol_cooldown_hours=48,
    ),
    StrategyConfig(
        strategy_id="N03",
        name="A+C funding -1.8 cooldown 4h",
        allowed_patterns=("A", "C"),
        funding_threshold=Decimal("0.018"),
        loss_symbol_cooldown_hours=4,
    ),
    StrategyConfig(
        strategy_id="N04",
        name="A+B+C funding -2.0 cooldown 4h",
        allowed_patterns=("A", "B", "C"),
        funding_threshold=Decimal("0.020"),
        loss_symbol_cooldown_hours=4,
    ),
    StrategyConfig(
        strategy_id="N05",
        name="A+C funding -1.5 cooldown 48h",
        allowed_patterns=("A", "C"),
        funding_threshold=Decimal("0.015"),
        loss_symbol_cooldown_hours=48,
    ),
)

N06_STRATEGY = StrategyConfig(
    strategy_id="N06",
    name="Double break high pullback stabilization",
    allowed_patterns=(),
    funding_threshold=None,
    loss_symbol_cooldown_hours=0,
    market_filter="quote_volume_top",
    evaluator_type="double_break_pullback",
    stop_mode="structure_p1",
    risk_reward_ratio=Decimal("5"),
    volume_top_n=100,
    pivot_left=2,
    pivot_right=2,
    stable_candle_count=5,
    entry_window_seconds=120,
)

N07_STRATEGY = StrategyConfig(
    strategy_id="N07",
    name="二次破高后P1近距离回踩策略",
    allowed_patterns=(),
    funding_threshold=None,
    loss_symbol_cooldown_hours=0,
    market_filter="quote_volume_top",
    evaluator_type="double_break_p1_retest",
    stop_mode="structure_p1_margin_capped",
    risk_reward_ratio=Decimal("5"),
    volume_top_n=100,
    pivot_left=2,
    pivot_right=2,
    entry_distance_min=Decimal("0.01"),
    entry_distance_max=Decimal("0.015"),
    pullback_min_fraction=Decimal("0.06"),
)

N08_STRATEGY = StrategyConfig(
    strategy_id="N08",
    name="长时间震荡五连阳提前入场策略",
    allowed_patterns=(),
    funding_threshold=None,
    loss_symbol_cooldown_hours=0,
    market_filter="quote_volume_top",
    evaluator_type="range_five_bullish",
    stop_mode="amplitude_margin_capped",
    risk_reward_ratio=Decimal("5"),
    volume_top_n=100,
    pivot_left=2,
    pivot_right=2,
    range_min_bars=20,
    range_max_bars=96,
    range_tolerance_fraction=Decimal("0.25"),
    bullish_streak_count=5,
    entry_window_seconds=120,
)

N09_STRATEGY = StrategyConfig(
    strategy_id="N09",
    name="SLOW_DECLINE_HALF_RETRACE",
    allowed_patterns=(),
    funding_threshold=None,
    loss_symbol_cooldown_hours=0,
    market_filter="quote_volume_top",
    evaluator_type="slow_decline_half_retrace",
    stop_mode="s1_target_margin_capped",
    risk_reward_ratio=Decimal("5"),
    volume_top_n=100,
    pivot_left=2,
    pivot_right=2,
    slow_decline_min_bars=20,
    slow_decline_min_r_squared=Decimal("0.65"),
    slow_decline_max_bearish_body_fraction=Decimal("0.20"),
    slow_decline_max_rebound_fraction=Decimal("0.20"),
    slow_decline_sideways_window=8,
    slow_decline_sideways_max_net_drop_fraction=Decimal("0.05"),
    slow_decline_rebound_max_bars=40,
    retrace_touch_fraction=Decimal("0.50"),
    retrace_entry_floor_fraction=Decimal("0.43"),
)

N10_STRATEGY = StrategyConfig(
    strategy_id="N10",
    name="VOLUME_LIQUIDITY_SWEEP_RECLAIM",
    allowed_patterns=(),
    funding_threshold=None,
    loss_symbol_cooldown_hours=0,
    market_filter="quote_volume_top",
    evaluator_type="volume_liquidity_sweep_reclaim",
    stop_mode="sweep_low_tick_margin_capped",
    risk_reward_ratio=Decimal("5"),
    volume_top_n=100,
    entry_window_seconds=120,
    support_window_bars=48,
    support_tolerance_fraction=Decimal("0.005"),
    support_touch_min_gap=4,
    volume_median_bars=20,
    volume_spike_multiple=Decimal("2.5"),
    sweep_depth_min=Decimal("0.003"),
    sweep_depth_max=Decimal("0.015"),
    lower_wick_ratio_min=Decimal("0.50"),
    taker_buy_ratio_min=Decimal("0.55"),
    entry_extension_max=Decimal("0.015"),
)

N11_STRATEGY = StrategyConfig(
    strategy_id="N11",
    name="VOLATILITY_SQUEEZE_BREAKOUT_FIRST_RETEST",
    allowed_patterns=(),
    funding_threshold=None,
    loss_symbol_cooldown_hours=0,
    market_filter="quote_volume_top",
    evaluator_type="volatility_squeeze_breakout_retest",
    stop_mode="breakout_retest_margin_capped",
    risk_reward_ratio=Decimal("5"),
    volume_top_n=100,
    entry_window_seconds=120,
    volume_median_bars=20,
    squeeze_bars=8,
    breakout_lookback_bars=20,
    bollinger_stddevs=Decimal("2"),
    keltner_atr_multiple=Decimal("1.5"),
    breakout_body_atr_min=Decimal("0.6"),
    breakout_close_location_min=Decimal("0.75"),
    breakout_volume_multiple_min=Decimal("1.8"),
    breakout_taker_buy_ratio_min=Decimal("0.55"),
    retest_max_bars=6,
    retest_atr_tolerance=Decimal("0.3"),
    retest_close_location_min=Decimal("0.5"),
    retest_volume_ratio_max=Decimal("0.8"),
    entry_extension_atr_max=Decimal("0.5"),
)

N12_STRATEGY = StrategyConfig(
    strategy_id="N12",
    name="RELATIVE_STRENGTH_FIRST_LOW_VOLUME_PULLBACK",
    allowed_patterns=(),
    funding_threshold=None,
    loss_symbol_cooldown_hours=0,
    market_filter="quote_volume_top",
    evaluator_type="relative_strength_first_pullback",
    stop_mode="relative_strength_pullback_margin_capped",
    risk_reward_ratio=Decimal("5"),
    volume_top_n=100,
    pivot_left=2,
    pivot_right=2,
    entry_window_seconds=120,
)

N13_STRATEGY = StrategyConfig(
    strategy_id="N13",
    name="广度牛市成交均价轮动策略",
    allowed_patterns=(),
    funding_threshold=None,
    loss_symbol_cooldown_hours=0,
    market_filter="quote_volume_top",
    evaluator_type="broad_market_vwap_rotation",
    stop_mode="vwap_rotation_margin_capped",
    volume_top_n=100,
)

N14_STRATEGY = StrategyConfig(
    strategy_id="N14",
    name="局部卖压衰减反转策略",
    allowed_patterns=(),
    funding_threshold=None,
    loss_symbol_cooldown_hours=0,
    market_filter="quote_volume_top",
    evaluator_type="localized_sell_pressure_decay_reversal",
    stop_mode="sell_pressure_decay_margin_capped",
    risk_reward_ratio=Decimal("5"),
    volume_top_n=100,
    entry_window_seconds=120,
)

N15_STRATEGY = StrategyConfig(
    strategy_id="N15",
    name="弱市回暖先行策略",
    allowed_patterns=(),
    funding_threshold=None,
    loss_symbol_cooldown_hours=0,
    market_filter="quote_volume_top",
    evaluator_type="market_breadth_recovery_leader",
    stop_mode="breadth_recovery_margin_capped",
    risk_reward_ratio=Decimal("5"),
    volume_top_n=100,
    entry_window_seconds=120,
)

N16_STRATEGY = N16StrategyDefinition()
N17_STRATEGY = N17StrategyDefinition()
N18_STRATEGY = N18StrategyDefinition()
N19_STRATEGY = N19StrategyDefinition()
N20_STRATEGY = N20StrategyDefinition()
N21_STRATEGY = MicroStrategyDefinition(
    "N21", "连续主动买流推进", "micro_taker_flow_persistence",
    "micro_observation_margin_capped", Decimal("0.25"),
)
N22_STRATEGY = MicroStrategyDefinition(
    "N22", "Taker-delta吸收背离反转", "micro_delta_absorption_divergence",
    "micro_observation_margin_capped", Decimal("0.25"),
)
N23_STRATEGY = MicroStrategyDefinition(
    "N23", "相对成交速率加速点火", "micro_relative_rate_ignition",
    "micro_observation_margin_capped", Decimal("0.30"),
)
N24_STRATEGY = MicroStrategyDefinition(
    "N24", "BTC/ETH领先后山寨滞后回补", "micro_anchor_lag_recovery",
    "micro_observation_margin_capped", Decimal("0.25"),
)
N25_STRATEGY = MicroStrategyDefinition(
    "N25", "负溢价压缩恢复", "micro_premium_compression_recovery",
    "micro_observation_margin_capped", Decimal("0.20"),
)

ALL_STRATEGIES: tuple[Any, ...] = FIRST_STAGE_STRATEGIES + (
    N06_STRATEGY,
    N07_STRATEGY,
    N08_STRATEGY,
    N09_STRATEGY,
    N10_STRATEGY,
    N11_STRATEGY,
    N12_STRATEGY,
    N13_STRATEGY,
    N14_STRATEGY,
    N15_STRATEGY,
    N16_STRATEGY,
    N17_STRATEGY,
    N18_STRATEGY,
    N19_STRATEGY,
    N20_STRATEGY,
    N21_STRATEGY,
    N22_STRATEGY,
    N23_STRATEGY,
    N24_STRATEGY,
    N25_STRATEGY,
)

# N06-N25 remain importable for decoding and querying historical evidence, but
# new production decisions are deliberately limited to N01-N05.
ACTIVE_STRATEGIES: tuple[StrategyConfig, ...] = FIRST_STAGE_STRATEGIES


def load_first_stage_strategies() -> tuple[StrategyConfig, ...]:
    return FIRST_STAGE_STRATEGIES


def load_all_strategies() -> tuple[Any, ...]:
    return ALL_STRATEGIES


def load_active_strategies() -> tuple[StrategyConfig, ...]:
    return ACTIVE_STRATEGIES


def funding_rate_passes(funding_rate: Decimal, threshold: Decimal) -> bool:
    return funding_rate <= -threshold
