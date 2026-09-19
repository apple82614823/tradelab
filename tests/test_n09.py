from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import json
import logging
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests.recorder_test_utils import make_test_recorder
from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.config import load_config
from trading_bot.double_break import StructureCandle, parse_structure_klines
from trading_bot.main import TradingBot
from trading_bot.monitor import FundingCandidate, StrategyMarketScan
from trading_bot.n09_analyzer import (
    analyze_n09_slow_decline_half_retrace,
    linear_regression_stats,
    validate_slow_decline_segment,
)
from trading_bot.recorder import ReviewRecorder
from trading_bot.strategies import N09_STRATEGY, load_all_strategies
from trading_bot.strategy_scheduler import SchedulerResult, StrategyScheduler
from trading_bot.trader import TradePlan


BASE_TIME_MS = 1_710_000_000_000
INTERVAL_MS = 15 * 60 * 1000


def candle(index, open_price, high, low, close):
    open_time = BASE_TIME_MS + index * INTERVAL_MS
    return [
        open_time,
        str(open_price),
        str(high),
        str(low),
        str(close),
        "0",
        open_time + INTERVAL_MS - 1,
    ]


def n09_klines(
    decline_bars=20,
    rebound_bars=5,
    current_close=Decimal("98"),
    current_high=Decimal("101"),
):
    rows = [
        candle(0, "110", "112", "109", "111"),
        candle(1, "114", "116", "113", "115"),
    ]
    noise = (
        Decimal("0"),
        Decimal("0.3"),
        Decimal("-0.2"),
        Decimal("0.4"),
        Decimal("-0.1"),
    )
    decline_step = Decimal("38") / Decimal(max(decline_bars - 1, 1))
    for offset in range(decline_bars):
        index = 2 + offset
        close = Decimal("118") - decline_step * Decimal(offset) + noise[offset % len(noise)]
        is_green = offset > 0 and offset % 4 == 2
        open_price = close - Decimal("0.3") if is_green else close + Decimal("0.3")
        high = max(open_price, close) + Decimal("0.5")
        low = min(open_price, close) - Decimal("0.5")
        if offset == 0:
            high = Decimal("120")
        if offset == decline_bars - 1:
            low = Decimal("80")
        rows.append(candle(index, open_price, high, low, close))

    l_index = 2 + decline_bars - 1
    for offset in range(1, rebound_bars):
        progress = Decimal(offset) / Decimal(rebound_bars)
        close = Decimal("82") + Decimal("16") * progress
        rows.append(
            candle(
                l_index + offset,
                close - Decimal("0.4"),
                close + Decimal("0.8"),
                close - Decimal("1"),
                close,
            )
        )
    current_index = l_index + rebound_bars
    rows.append(
        candle(
            current_index,
            current_close + Decimal("0.3"),
            current_high,
            current_close - Decimal("0.5"),
            current_close,
        )
    )
    return rows, BASE_TIME_MS + current_index * INTERVAL_MS + 30_000


def two_n09_structures_klines():
    rows = [
        candle(0, "140", "142", "139", "141"),
        candle(1, "144", "146", "143", "145"),
    ]
    for offset in range(20):
        index = 2 + offset
        close = Decimal("148") - Decimal("46") * Decimal(offset) / Decimal("19")
        open_price = close + Decimal("0.3")
        high = close + Decimal("0.6")
        low = close - Decimal("0.6")
        if offset == 0:
            high = Decimal("150")
        if offset == 19:
            low = Decimal("100")
        rows.append(candle(index, open_price, high, low, close))

    rows.extend(
        [
            candle(22, "120", "126", "119", "124"),
            candle(23, "128", "132", "127", "130"),
            candle(24, "133", "136", "132", "135"),
        ]
    )
    for offset in range(20):
        index = 25 + offset
        close = Decimal("138") - Decimal("36") * Decimal(offset) / Decimal("19")
        open_price = close + Decimal("0.3")
        high = close + Decimal("0.6")
        low = close - Decimal("0.6")
        if offset == 0:
            high = Decimal("140")
        if offset == 19:
            low = Decimal("100")
        rows.append(candle(index, open_price, high, low, close))
    rows.append(candle(45, "119", "121", "117", "118"))
    return rows, BASE_TIME_MS + 45 * INTERVAL_MS + 30_000


def candidate(symbol="N09USDT", funding_rate=None, rank=1):
    return FundingCandidate(
        symbol=symbol,
        funding_rate=funding_rate,
        mark_price=Decimal("999"),
        quote_volume=Decimal("1000000"),
        quote_volume_rank=rank,
        candidate_universe="quote_volume_top",
    )


class N09AnalyzerTests(unittest.TestCase):
    def test_noisy_red_green_single_decline_passes_without_s2(self):
        raw, checked_at_ms = n09_klines()

        result = analyze_n09_slow_decline_half_retrace(
            "N09USDT",
            raw,
            checked_at_ms=checked_at_ms,
        )

        decline = parse_structure_klines(raw)[2:22]
        self.assertTrue(result.passed)
        self.assertEqual(result.structure.s1_index, 2)
        self.assertEqual(result.structure.l_index, 21)
        self.assertTrue(any(item.close > item.open for item in decline))
        self.assertGreater(result.structure.metrics.r_squared, Decimal("0.65"))

    def test_invalid_original_s1_does_not_restart_from_later_s2(self):
        raw, checked_at_ms = n09_klines(decline_bars=40, rebound_bars=5)
        raw[17][3] = "104"
        raw[18][2] = "114"

        result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", raw, checked_at_ms=checked_at_ms
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "COUNTERTREND_REBOUND_TOO_LARGE")
        self.assertEqual(result.structure.s1, Decimal("120"))
        self.assertEqual(result.structure.s1_index, 2)

    def test_missing_leading_context_fails_closed(self):
        raw, checked_at_ms = n09_klines()

        result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", raw[2:], checked_at_ms=checked_at_ms
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "HISTORY_CONTEXT_INSUFFICIENT")

    def test_decline_duration_20_passes_and_19_rejects(self):
        twenty, twenty_checked = n09_klines(decline_bars=20)
        nineteen, nineteen_checked = n09_klines(decline_bars=19)

        passed = analyze_n09_slow_decline_half_retrace(
            "N09USDT", twenty, checked_at_ms=twenty_checked
        )
        rejected = analyze_n09_slow_decline_half_retrace(
            "N09USDT", nineteen, checked_at_ms=nineteen_checked
        )

        self.assertTrue(passed.passed)
        self.assertFalse(rejected.passed)
        self.assertEqual(rejected.reason, "SLOW_DECLINE_DURATION_TOO_SHORT")

    def test_r_squared_boundary_and_negative_slope(self):
        raw, _ = n09_klines()
        segment = parse_structure_klines(raw)[2:22]
        with patch(
            "trading_bot.n09_analyzer.linear_regression_stats",
            return_value=(Decimal("-1"), Decimal("0.65")),
        ):
            boundary, _ = validate_slow_decline_segment(segment, Decimal("40"))
        with patch(
            "trading_bot.n09_analyzer.linear_regression_stats",
            return_value=(Decimal("-1"), Decimal("0.649999")),
        ):
            below, _ = validate_slow_decline_segment(segment, Decimal("40"))
        with patch(
            "trading_bot.n09_analyzer.linear_regression_stats",
            return_value=(Decimal("0"), Decimal("1")),
        ):
            flat, _ = validate_slow_decline_segment(segment, Decimal("40"))

        self.assertIsNone(boundary)
        self.assertEqual(below, "SLOW_DECLINE_TREND_NOT_QUALIFIED")
        self.assertEqual(flat, "SLOW_DECLINE_TREND_NOT_QUALIFIED")

    def test_single_bearish_body_twenty_percent_boundary(self):
        raw, _ = n09_klines()
        segment = parse_structure_klines(raw)[2:22]
        exact = list(segment)
        exact[2] = replace(
            exact[2],
            open=exact[2].close + Decimal("8"),
            high=exact[2].close + Decimal("8"),
        )
        over = list(exact)
        over[2] = replace(
            over[2],
            open=over[2].close + Decimal("8.001"),
            high=over[2].close + Decimal("8.001"),
        )

        exact_reason, _ = validate_slow_decline_segment(exact, Decimal("40"))
        over_reason, _ = validate_slow_decline_segment(over, Decimal("40"))

        self.assertIsNone(exact_reason)
        self.assertEqual(over_reason, "SINGLE_CANDLE_DROP_TOO_LARGE")

    def test_countertrend_rebound_twenty_percent_boundary(self):
        raw, _ = n09_klines()
        segment = parse_structure_klines(raw)[2:22]
        running_low = min(item.low for item in segment[:10])
        exact = list(segment)
        exact[10] = replace(exact[10], high=running_low + Decimal("8"))
        over = list(segment)
        over[10] = replace(over[10], high=running_low + Decimal("8.001"))

        exact_reason, _ = validate_slow_decline_segment(exact, Decimal("40"))
        over_reason, _ = validate_slow_decline_segment(over, Decimal("40"))

        self.assertIsNone(exact_reason)
        self.assertEqual(over_reason, "COUNTERTREND_REBOUND_TOO_LARGE")

    def test_all_eight_bar_windows_use_five_percent_closed_boundary(self):
        def segment(step):
            rows = []
            for index in range(20):
                close = Decimal("116") - step * Decimal(index)
                rows.append(
                    StructureCandle(
                        index=index,
                        open_time=str(index),
                        open=close + Decimal("0.1"),
                        high=close + Decimal("0.2"),
                        low=close - Decimal("0.2"),
                        close=close,
                    )
                )
            return rows

        exact_step = Decimal("2") / Decimal("7")
        passing_step = Decimal("2.001") / Decimal("7")
        with patch(
            "trading_bot.n09_analyzer.linear_regression_stats",
            return_value=(Decimal("-1"), Decimal("1")),
        ):
            exact_reason, _ = validate_slow_decline_segment(
                segment(exact_step), Decimal("40")
            )
            passing_reason, _ = validate_slow_decline_segment(
                segment(passing_step), Decimal("40")
            )

        self.assertEqual(exact_reason, "SIDEWAYS_WINDOW_DETECTED")
        self.assertIsNone(passing_reason)

    def test_p43_p50_and_extremes_use_s1_high_and_l_low(self):
        raw, checked_at_ms = n09_klines()
        result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", raw, checked_at_ms=checked_at_ms
        )

        self.assertEqual(result.structure.s1, Decimal("120"))
        self.assertEqual(result.structure.l, Decimal("80"))
        self.assertEqual(result.structure.p43, Decimal("97.20"))
        self.assertEqual(result.structure.p50, Decimal("100.00"))

    def test_rebound_half_duration_boundary_and_overage(self):
        boundary, boundary_checked = n09_klines(decline_bars=20, rebound_bars=10)
        over, over_checked = n09_klines(decline_bars=20, rebound_bars=11)

        boundary_result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", boundary, checked_at_ms=boundary_checked
        )
        over_result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", over, checked_at_ms=over_checked
        )

        self.assertTrue(boundary_result.passed)
        self.assertEqual(boundary_result.rebound_bars, 10)
        self.assertEqual(over_result.reason, "REBOUND_TOO_SLOW")

    def test_rebound_forty_bar_closed_boundary(self):
        boundary, boundary_checked = n09_klines(decline_bars=80, rebound_bars=40)
        over, over_checked = n09_klines(decline_bars=82, rebound_bars=41)

        boundary_result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", boundary, checked_at_ms=boundary_checked
        )
        over_result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", over, checked_at_ms=over_checked
        )

        self.assertTrue(boundary_result.passed)
        self.assertEqual(boundary_result.rebound_bars, 40)
        self.assertEqual(over_result.reason, "REBOUND_TOO_SLOW")

    def test_current_touch_allows_price_between_p43_and_p50_and_exact_p43(self):
        between, between_checked = n09_klines(current_close=Decimal("98"))
        exact, exact_checked = n09_klines(current_close=Decimal("97.2"))

        between_result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", between, checked_at_ms=between_checked
        )
        exact_result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", exact, checked_at_ms=exact_checked
        )

        self.assertTrue(between_result.passed)
        self.assertLess(between_result.current_price, between_result.structure.p50)
        self.assertTrue(exact_result.passed)
        self.assertEqual(exact_result.current_price, exact_result.structure.p43)

    def test_touch_below_p43_and_s1_are_terminal_rejections(self):
        below, below_checked = n09_klines(current_close=Decimal("97.19"))
        at_s1, at_s1_checked = n09_klines(
            current_close=Decimal("120"), current_high=Decimal("120")
        )

        below_result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", below, checked_at_ms=below_checked
        )
        s1_result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", at_s1, checked_at_ms=at_s1_checked
        )

        self.assertEqual(below_result.reason, "P43_MISSED")
        self.assertEqual(s1_result.reason, "S1_REACHED")

    def test_historical_first_touch_and_historical_s1_reach_are_rejected(self):
        raw, _ = n09_klines()
        first_touch = deepcopy(raw)
        first_touch[-1][2] = "99"
        first_touch.append(candle(27, "98", "101", "97", "99"))
        first_touch.append(candle(28, "99", "101", "98", "99.5"))
        checked = BASE_TIME_MS + 28 * INTERVAL_MS + 30_000
        historical = analyze_n09_slow_decline_half_retrace(
            "N09USDT", first_touch, checked_at_ms=checked
        )

        reached = deepcopy(first_touch)
        reached[-2][2] = "121"
        reached_result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", reached, checked_at_ms=checked
        )

        self.assertEqual(historical.reason, "HISTORICAL_P50_TOUCH_MISSED")
        self.assertTrue(historical.historical_touch)
        self.assertEqual(reached_result.reason, "S1_REACHED")

    def test_historical_terminal_allows_new_structure_in_same_window(self):
        raw, checked_at_ms = two_n09_structures_klines()

        result = analyze_n09_slow_decline_half_retrace(
            "N09USDT", raw, checked_at_ms=checked_at_ms
        )

        self.assertTrue(result.passed)
        self.assertEqual(result.structure.s1, Decimal("140"))
        self.assertEqual(result.structure.l, Decimal("100"))
        self.assertEqual(len(result.historical_events), 1)
        self.assertEqual(result.historical_events[0].structure.s1, Decimal("150"))
        self.assertEqual(
            result.historical_events[0].reason,
            "HISTORICAL_P50_TOUCH_MISSED",
        )

    def test_structure_identity_changes_for_new_s1_or_l(self):
        raw, checked_at_ms = n09_klines()
        first = analyze_n09_slow_decline_half_retrace(
            "N09USDT", raw, checked_at_ms=checked_at_ms
        )
        shifted = deepcopy(raw)
        for row in shifted:
            row[0] += 100 * INTERVAL_MS
            row[6] += 100 * INTERVAL_MS
        second = analyze_n09_slow_decline_half_retrace(
            "N09USDT", shifted, checked_at_ms=checked_at_ms + 100 * INTERVAL_MS
        )

        self.assertNotEqual(first.structure_id, second.structure_id)

    def test_regression_stats_detect_noisy_negative_trend(self):
        slope, r_squared = linear_regression_stats(
            [Decimal("10"), Decimal("9.2"), Decimal("9.5"), Decimal("8"), Decimal("7.7")]
        )
        self.assertLess(slope, 0)
        self.assertGreater(r_squared, Decimal("0.65"))


class N09SchedulerTests(unittest.TestCase):
    def _scheduler(self, tmpdir):
        recorder = make_test_recorder(
            str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_n09")
        )
        recorder.upsert_strategy_definitions((N09_STRATEGY,))
        return recorder, StrategyScheduler(
            (N09_STRATEGY,), 96, recorder, logging.getLogger("test_n09")
        )

    def test_configuration_funding_and_rank_rules(self):
        self.assertIn(N09_STRATEGY, load_all_strategies())
        self.assertIsNone(N09_STRATEGY.funding_threshold)
        self.assertEqual(N09_STRATEGY.volume_top_n, 100)
        self.assertEqual(N09_STRATEGY.evaluator_type, "slow_decline_half_retrace")
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            raw, checked_at_ms = n09_klines()
            candidates = [
                candidate("POSUSDT", Decimal("0.5"), 1),
                candidate("NEGUSDT", Decimal("-0.5"), 2),
                candidate("NONEUSDT", None, 100),
                candidate("OUTUSDT", None, 101),
            ]
            scan_id = recorder.begin_scan(4, candidates, dry_run=True)
            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": candidates},
                {item.symbol: raw for item in candidates},
                checked_at_ms=checked_at_ms,
            )
            self.assertEqual(
                [signal.candidate.symbol for signal in result.signals],
                ["POSUSDT", "NEGUSDT", "NONEUSDT"],
            )
            self.assertTrue(all(signal.passed for signal in result.signals))

    def test_touch_is_consumed_once_and_persists_across_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self._scheduler(tmpdir)
            raw, checked_at_ms = n09_klines(current_close=Decimal("97.19"))
            item = candidate()
            first_scan = recorder.begin_scan(1, [item], dry_run=True)
            first = scheduler.evaluate(
                first_scan,
                {"quote_volume_top": [item]},
                {item.symbol: raw},
                checked_at_ms=checked_at_ms,
            )
            self.assertEqual(first.signals[0].reason, "P43_MISSED")

            later = deepcopy(raw)
            later[-1][2] = "101"
            later[-1][4] = "98"
            restarted = make_test_recorder(str(db_file), logging.getLogger("test_n09_restart"))
            restarted_scheduler = StrategyScheduler(
                (N09_STRATEGY,), 96, restarted, logging.getLogger("test_n09_restart")
            )
            second_scan = restarted.begin_scan(1, [item], dry_run=True)
            second = restarted_scheduler.evaluate(
                second_scan,
                {"quote_volume_top": [item]},
                {item.symbol: later},
                checked_at_ms=checked_at_ms,
            )

            self.assertEqual(second.signals[0].reason, "STRUCTURE_CONSUMED")
            with restarted._connect() as connection:
                count = connection.execute(
                    "SELECT COUNT(*) FROM n09_structure_states"
                ).fetchone()[0]
            self.assertEqual(count, 1)

    def test_offline_historical_touch_is_backfilled_and_idempotent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            raw, _ = n09_klines()
            raw[-1][2] = "99"
            raw.append(candle(27, "98", "101", "97", "99"))
            raw.append(candle(28, "99", "101", "98", "99.5"))
            checked = BASE_TIME_MS + 28 * INTERVAL_MS + 30_000
            item = candidate()
            first_scan = recorder.begin_scan(1, [item], dry_run=True)
            first = scheduler.evaluate(
                first_scan,
                {"quote_volume_top": [item]},
                {item.symbol: raw},
                checked_at_ms=checked,
            )
            second_scan = recorder.begin_scan(1, [item], dry_run=True)
            second = scheduler.evaluate(
                second_scan,
                {"quote_volume_top": [item]},
                {item.symbol: raw},
                checked_at_ms=checked,
            )

            self.assertEqual(first.signals[0].reason, "HISTORICAL_P50_TOUCH_MISSED")
            self.assertEqual(second.signals[0].reason, "STRUCTURE_CONSUMED")
            with recorder._connect() as connection:
                count = connection.execute(
                    "SELECT COUNT(*) FROM n09_structure_states"
                ).fetchone()[0]
            self.assertEqual(count, 1)

    def test_consumed_structure_does_not_block_truly_new_s1_l_identity(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            item = candidate()
            old_raw, old_checked = n09_klines()
            old_scan = recorder.begin_scan(1, [item], dry_run=True)
            old = scheduler.evaluate(
                old_scan,
                {"quote_volume_top": [item]},
                {item.symbol: old_raw},
                checked_at_ms=old_checked,
            )

            new_raw, new_checked = n09_klines()
            for row in new_raw:
                row[0] += 100 * INTERVAL_MS
                row[1] = str(Decimal(row[1]) + Decimal("10"))
                row[2] = str(Decimal(row[2]) + Decimal("10"))
                row[3] = str(Decimal(row[3]) + Decimal("10"))
                row[4] = str(Decimal(row[4]) + Decimal("10"))
                row[6] += 100 * INTERVAL_MS
            new_scan = recorder.begin_scan(1, [item], dry_run=True)
            new = scheduler.evaluate(
                new_scan,
                {"quote_volume_top": [item]},
                {item.symbol: new_raw},
                checked_at_ms=new_checked + 100 * INTERVAL_MS,
            )

            self.assertTrue(old.signals[0].passed)
            self.assertTrue(new.signals[0].passed)
            self.assertNotEqual(
                old.signals[0].analysis.structure_id,
                new.signals[0].analysis.structure_id,
            )
            with recorder._connect() as connection:
                count = connection.execute(
                    "SELECT COUNT(*) FROM n09_structure_states"
                ).fetchone()[0]
            self.assertEqual(count, 2)

    def test_persisted_old_terminal_allows_new_structure_after_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self._scheduler(tmpdir)
            item = candidate()
            raw, checked_at_ms = two_n09_structures_klines()
            historical_only = raw[:25]
            historical_checked = BASE_TIME_MS + 24 * INTERVAL_MS + 30_000
            old_scan = recorder.begin_scan(1, [item], dry_run=True)
            old = scheduler.evaluate(
                old_scan,
                {"quote_volume_top": [item]},
                {item.symbol: historical_only},
                checked_at_ms=historical_checked,
            )

            restarted = make_test_recorder(
                str(db_file), logging.getLogger("test_n09_timeline_restart")
            )
            restarted_scheduler = StrategyScheduler(
                (N09_STRATEGY,),
                96,
                restarted,
                logging.getLogger("test_n09_timeline_restart"),
            )
            new_scan = restarted.begin_scan(1, [item], dry_run=True)
            new = restarted_scheduler.evaluate(
                new_scan,
                {"quote_volume_top": [item]},
                {item.symbol: raw},
                checked_at_ms=checked_at_ms,
            )

            self.assertEqual(old.signals[0].reason, "HISTORICAL_P50_TOUCH_MISSED")
            self.assertTrue(new.signals[0].passed)
            self.assertEqual(new.signals[0].analysis.structure.s1, Decimal("140"))
            with restarted._connect() as connection:
                rows = connection.execute(
                    "SELECT s1_price, COUNT(*) FROM n09_structure_states "
                    "GROUP BY s1_price ORDER BY CAST(s1_price AS REAL) DESC"
                ).fetchall()
            self.assertEqual([tuple(row) for row in rows], [("150", 1), ("140", 1)])


class N09MainAuditTests(unittest.TestCase):
    def test_main_client_uses_122_shared_klines_for_80_40_boundary(self):
        raw, _ = n09_klines(decline_bars=80, rebound_bars=40)
        self.assertEqual(len(raw), 122)
        item = candidate()
        requests = []

        with patch("trading_bot.config._load_env_file"), patch.dict(
            os.environ,
            {"MULTI_STRATEGY_ENABLED": "true", "KLINE_LIMIT": "122"},
            clear=True,
        ):
            config = load_config()
        client = BinanceFuturesClient(config, logging.getLogger("test_n09_limit"))

        def fake_request(method, path, params=None, signed=False, retry=True):
            requests.append((method, path, dict(params or {})))
            return raw

        client._request = fake_request

        class FakeTrader:
            def close_dry_run_position_if_triggered(self):
                return None

            def sync_state_with_exchange(self):
                return SimpleNamespace(has_position=False, closed_state=None)

        class FakePaperTrader:
            def close_triggered_open_trades(self, *args):
                return []

        class FakeRecorder:
            def begin_scan(self, scanned_count, candidates, dry_run):
                return 1

            def complete_scan(self, scan_id, opened):
                return None

            def record_event(self, *args, **kwargs):
                return None

        class FakeMonitor:
            def scan_for_strategies(self, volume_top_n):
                return StrategyMarketScan(1, [], [item])

        class InspectingScheduler:
            def __init__(self):
                self.analysis = None

            def evaluate(self, scan_id, candidate_groups, raw_by_symbol, checked_at_ms=None):
                self.analysis = analyze_n09_slow_decline_half_retrace(
                    item.symbol, raw_by_symbol[item.symbol]
                )
                return SchedulerResult([], [], [])

            def choose_live_candidate(self, live_candidates, live_blocked):
                return None

        bot = TradingBot.__new__(TradingBot)
        bot.config = config
        bot.logger = logging.getLogger("test_n09_limit")
        bot.client = client
        bot.trader = FakeTrader()
        bot.paper_trader = FakePaperTrader()
        bot.recorder = FakeRecorder()
        bot.monitor = FakeMonitor()
        bot.strategy_scheduler = InspectingScheduler()
        bot.strategies = (N09_STRATEGY,)
        bot.state = SimpleNamespace(
            load=lambda: None,
            save=lambda state: None,
            clear=lambda: None,
        )

        bot._run_once_multi_strategy()

        self.assertTrue(bot.strategy_scheduler.analysis.passed)
        self.assertEqual(bot.strategy_scheduler.analysis.rebound_bars, 40)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0][1], "/fapi/v1/klines")
        self.assertEqual(requests[0][2]["limit"], 122)

    def test_main_records_structure_market_and_risk_audit_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            item = candidate()
            raw, checked_at_ms = n09_klines()
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_n09_main_audit"),
            )
            scheduler = StrategyScheduler(
                (N09_STRATEGY,),
                96,
                recorder,
                logging.getLogger("test_n09_main_audit"),
            )
            scan_id = recorder.begin_scan(1, [item], dry_run=True)
            evaluated = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [item]},
                {item.symbol: raw},
                checked_at_ms=checked_at_ms,
            )
            signal = evaluated.passed_signals[0]
            plan = TradePlan(
                symbol=item.symbol,
                leverage=50,
                quantity=Decimal("45.454"),
                entry_price=Decimal("98"),
                stop_loss_price=Decimal("93.6"),
                take_profit_price=Decimal("120"),
                stop_loss_pct=Decimal("4.4") / Decimal("98"),
                take_profit_pct=Decimal("22") / Decimal("98"),
                amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("0"),
                low_24h_price=Decimal("0"),
                risk_amount=Decimal("200"),
                notional_value=Decimal("4454.492"),
                required_margin=Decimal("89.08984"),
                balance=Decimal("1000"),
                stop_mode="s1_target_margin_capped",
                risk_reward_ratio=Decimal("5"),
                structure_id=signal.analysis.structure_id,
                structure_target_price=Decimal("120"),
                target_risk_amount=Decimal("200"),
                actual_risk_amount=Decimal("199.9976"),
                risk_capped_by_margin=False,
                pretrade_quantity=Decimal("45.454"),
            )

            class FakeClient:
                def get_klines(self, symbol):
                    return raw

            class FakeTrader:
                def close_dry_run_position_if_triggered(self):
                    return None

                def sync_state_with_exchange(self):
                    return SimpleNamespace(has_position=False, closed_state=None)

                def build_s1_target_margin_capped_trade_plan(self, *args, **kwargs):
                    return plan

            class FakePaperTrader:
                def close_triggered_open_trades(self, *args):
                    return []

                def open_trade(self, *args):
                    return 1

            class FakeMonitor:
                def scan_for_strategies(self, volume_top_n):
                    return StrategyMarketScan(1, [], [item])

            class FakeScheduler:
                def evaluate(self, *args, **kwargs):
                    return SchedulerResult([signal], [signal], [], True)

                def choose_live_candidate(self, live_candidates, live_blocked):
                    return None

            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=True)
            bot.logger = logging.getLogger("test_n09_main_audit")
            bot.client = FakeClient()
            bot.trader = FakeTrader()
            bot.paper_trader = FakePaperTrader()
            bot.recorder = recorder
            bot.monitor = FakeMonitor()
            bot.strategy_scheduler = FakeScheduler()
            bot._signal_batch_is_current = lambda scan_id: True
            recorder.assert_n16_current_scan_claims = (
                lambda scan_id, claims: None
                if type(scan_id) is int and scan_id > 0 and claims == ()
                else (_ for _ in ()).throw(
                    AssertionError("unexpected N16 current claim")
                )
            )
            bot.strategies = (N09_STRATEGY,)
            bot.state = SimpleNamespace(
                load=lambda: None,
                save=lambda state: None,
                clear=lambda: None,
            )

            bot._run_once_multi_strategy()

            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT funding_rate, detail_json FROM strategy_signals WHERE id = ?",
                    (signal.signal_id,),
                ).fetchone()
            detail = json.loads(row[1])
            self.assertEqual(row[0], "")
            self.assertEqual(detail["quote_volume_rank"], 1)
            self.assertEqual(detail["structure"]["s1"], "120")
            self.assertEqual(detail["structure"]["l"], "80")
            self.assertEqual(detail["structure"]["p43"], "97.20")
            self.assertEqual(detail["structure"]["p50"], "100.00")
            self.assertEqual(detail["current_price"], "98")
            self.assertEqual(detail["structure_target_price"], "120")
            self.assertEqual(detail["target_risk_amount"], "200")
            self.assertEqual(detail["actual_risk_amount"], "199.9976")
            self.assertFalse(detail["risk_capped_by_margin"])


if __name__ == "__main__":
    unittest.main()
