from copy import deepcopy
from decimal import Decimal
import json
import logging
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from tests.recorder_test_utils import make_test_recorder
from trading_bot.main import TradingBot
from trading_bot.monitor import FundingCandidate, StrategyMarketScan
from trading_bot.n06_analyzer import analyze_n06_double_break_pullback
from trading_bot.n07_analyzer import analyze_n07_p1_retest
from trading_bot.recorder import ReviewRecorder
from trading_bot.strategies import N06_STRATEGY, N07_STRATEGY, load_all_strategies
from trading_bot.strategy_scheduler import SchedulerResult, StrategyScheduler
from trading_bot.trader import TradePlan

from tests.swing_fixtures import build_valid_swing_klines
from tests.test_n06 import P2_INDEX, SECOND_BREAK_INDEX, complete_n06_klines


P1 = Decimal("140")


def n07_klines(distance: str = "0.012", bullish: bool = False):
    raw = build_valid_swing_klines()
    current_close = P1 * (Decimal("1") + Decimal(distance))
    current_open = (
        current_close - Decimal("0.20")
        if bullish
        else current_close + Decimal("0.20")
    )
    raw[-1][1] = str(current_open)
    raw[-1][2] = str(max(current_open, current_close) + Decimal("0.20"))
    zone_lower = P1 * Decimal("1.01")
    raw[-1][3] = str(
        zone_lower
        if zone_lower <= current_close <= P1 * Decimal("1.015")
        else current_close - Decimal("0.01")
    )
    raw[-1][4] = str(current_close)
    return raw


def n07_candidate(symbol="N07USDT", funding_rate=None, rank=1, mark_price="999"):
    return FundingCandidate(
        symbol=symbol,
        funding_rate=funding_rate,
        mark_price=Decimal(mark_price),
        quote_volume=Decimal("1000000"),
        quote_volume_rank=rank,
        candidate_universe="quote_volume_top",
    )


def n07_pullback_klines(pullback: str):
    h1 = Decimal("160")
    p1 = h1 * (Decimal("1") - Decimal(pullback))
    return build_valid_swing_klines(p1_price=p1)


class N07AnalyzerTests(unittest.TestCase):
    def test_pullback_six_percent_boundary_and_crcl_shallow_sample(self):
        exact = analyze_n07_p1_retest("N07USDT", n07_pullback_klines("0.06"))
        self.assertTrue(exact.passed)
        self.assertEqual(exact.pullback_pct, Decimal("0.06"))
        shallow = analyze_n07_p1_retest("CRCLUSDT", n07_pullback_klines("0.0064"))
        self.assertFalse(shallow.passed)
        self.assertIn(shallow.reason, {"STRUCTURE_NOT_FOUND", "N07_PULLBACK_TOO_SHALLOW"})

    def test_first_touch_zone_boundaries_pass(self):
        for distance in ("0.01", "0.015"):
            result = analyze_n07_p1_retest("N07USDT", n07_klines(distance))
            self.assertTrue(result.passed)

    def test_historical_touch_overshoot_rebound_and_below_zone_are_terminal(self):
        cases = []
        historical = n07_klines()
        historical[P2_INDEX][3] = "142.1"
        cases.append((historical, "N07_HISTORICAL_ZONE_TOUCH_MISSED"))
        overshoot = n07_klines()
        overshoot[-1][3] = "141.39"
        cases.append((overshoot, "N07_ENTRY_ZONE_OVERSHOT"))
        rebound = n07_klines("0.016")
        rebound[-1][3] = "141.7"
        cases.append((rebound, "N07_ENTRY_TOUCH_REBOUNDED"))
        below = n07_klines("0.009")
        cases.append((below, "N07_ENTRY_ZONE_OVERSHOT"))
        for raw, reason in cases:
            with self.subTest(reason=reason):
                result = analyze_n07_p1_retest("N07USDT", raw)
                self.assertFalse(result.passed)
                self.assertEqual(result.reason, reason)

    def test_h2_observation_stops_at_historical_first_zone_touch(self):
        raw = n07_klines()
        raw[P2_INDEX][3] = "142.1"
        raw[P2_INDEX + 3][2] = "190"
        result = analyze_n07_p1_retest("N07HISTORYUSDT", raw)
        self.assertEqual(result.reason, "N07_HISTORICAL_ZONE_TOUCH_MISSED")
        self.assertIsNone(result.h2)
        self.assertIsNone(result.entry_retrace_segment)

    def test_above_zone_without_touch_waits(self):
        result = analyze_n07_p1_retest("N07USDT", n07_klines("0.02"))
        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N07_WAITING_FIRST_ZONE_TOUCH")
    def test_complete_structure_passes_at_one_point_two_percent(self):
        result = analyze_n07_p1_retest("N07USDT", n07_klines())

        self.assertTrue(result.passed)
        self.assertEqual(result.structure.h0, Decimal("148"))
        self.assertEqual(result.structure.h1, Decimal("160"))
        self.assertEqual(result.structure.p1, P1)
        self.assertEqual(result.current_price, Decimal("141.680"))
        self.assertEqual(result.distance_pct, Decimal("0.012"))
        self.assertGreater(result.post_break_low, P1)

    def test_distance_boundaries_are_inclusive(self):
        for distance in ("0.01", "0.015"):
            with self.subTest(distance=distance):
                result = analyze_n07_p1_retest("N07USDT", n07_klines(distance))
                self.assertTrue(result.passed)
                self.assertEqual(result.distance_pct, Decimal(distance))

    def test_distances_outside_boundaries_are_rejected(self):
        expected = {
            "0.0099": "N07_ENTRY_ZONE_OVERSHOT",
            "0.0151": "N07_WAITING_FIRST_ZONE_TOUCH",
        }
        for distance, reason in expected.items():
            with self.subTest(distance=distance):
                result = analyze_n07_p1_retest("N07USDT", n07_klines(distance))
                self.assertFalse(result.passed)
                self.assertEqual(result.reason, reason)

    def test_bearish_current_candle_can_pass(self):
        result = analyze_n07_p1_retest(
            "N07USDT",
            n07_klines("0.012", bullish=False),
        )

        self.assertTrue(result.passed)
        self.assertFalse(result.current_bullish)

    def test_closed_candle_touching_p1_invalidates_structure(self):
        raw = n07_klines()
        raw[P2_INDEX][3] = "140"

        result = analyze_n07_p1_retest("N07USDT", raw)

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N07_P1_TOUCHED_AFTER_SECOND_BREAK")
        self.assertEqual(result.post_break_low, P1)

    def test_current_unclosed_candle_touching_p1_invalidates_structure(self):
        raw = n07_klines()
        raw[-1][3] = "139.99"

        result = analyze_n07_p1_retest("N07USDT", raw)

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N07_P1_TOUCHED_AFTER_SECOND_BREAK")

    def test_prior_p1_break_cannot_pass_after_rebound(self):
        raw = n07_klines()
        raw[P2_INDEX + 1][3] = "139"

        result = analyze_n07_p1_retest("N07USDT", raw)

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N07_P1_TOUCHED_AFTER_SECOND_BREAK")
        self.assertEqual(result.current_price, Decimal("141.680"))

    def test_first_break_wick_without_close_is_rejected(self):
        raw = n07_klines()
        for index in range(27, len(raw) - 1):
            raw[index][4] = str(min(Decimal(raw[index][4]), Decimal("148")))
        raw[34][2] = "149"
        raw[34][4] = "148"

        result = analyze_n07_p1_retest("N07USDT", raw)

        self.assertFalse(result.passed)

    def test_second_break_wick_without_close_is_rejected(self):
        raw = n07_klines()
        for index in range(SECOND_BREAK_INDEX, len(raw) - 1):
            raw[index][4] = str(min(Decimal(raw[index][4]), Decimal("160")))
        raw[SECOND_BREAK_INDEX][2] = "161"
        raw[SECOND_BREAK_INDEX][4] = "160"

        result = analyze_n07_p1_retest("N07USDT", raw)

        self.assertFalse(result.passed)

    def test_only_one_break_is_rejected(self):
        raw = n07_klines()
        for index in range(SECOND_BREAK_INDEX, len(raw) - 1):
            raw[index][2] = str(min(Decimal(raw[index][2]), Decimal("160")))
            raw[index][4] = str(min(Decimal(raw[index][4]), Decimal("160")))

        result = analyze_n07_p1_retest("N07USDT", raw)

        self.assertFalse(result.passed)


class N07SchedulerTests(unittest.TestCase):
    def _scheduler(self, tmpdir):
        recorder = make_test_recorder(
            str(Path(tmpdir) / "review.sqlite3"),
            logging.getLogger("test_n07"),
        )
        recorder.upsert_strategy_definitions((N07_STRATEGY,))
        scheduler = StrategyScheduler(
            (N07_STRATEGY,),
            96,
            recorder,
            logging.getLogger("test_n07"),
        )
        return recorder, scheduler

    def test_n07_configuration_is_loaded(self):
        self.assertIn(N07_STRATEGY, load_all_strategies())
        self.assertEqual(N07_STRATEGY.name, "二次破高后P1近距离回踩策略")
        self.assertIsNone(N07_STRATEGY.funding_threshold)
        self.assertEqual(N07_STRATEGY.market_filter, "quote_volume_top")
        self.assertEqual(N07_STRATEGY.evaluator_type, "double_break_p1_retest")
        self.assertEqual(N07_STRATEGY.stop_mode, "structure_p1_margin_capped")
        self.assertEqual(N07_STRATEGY.volume_top_n, 100)
        self.assertEqual(N07_STRATEGY.entry_distance_min, Decimal("0.01"))
        self.assertEqual(N07_STRATEGY.entry_distance_max, Decimal("0.015"))
        self.assertEqual(N07_STRATEGY.pullback_min_fraction, Decimal("0.06"))
        self.assertEqual(N07_STRATEGY.swing_segment_min_bars, 5)
        self.assertEqual(N07_STRATEGY.swing_segment_atr_period, 14)

    def test_terminal_first_touch_miss_persists_across_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n07_candidate()
            raw = n07_klines("0.016")
            raw[-1][3] = "141.7"
            first = scheduler.evaluate(
                None, {"quote_volume_top": [candidate]}, {candidate.symbol: raw}
            )
            self.assertEqual(first.signals[0].reason, "N07_ENTRY_TOUCH_REBOUNDED")
            restarted = StrategyScheduler(
                (N07_STRATEGY,), 96, recorder, logging.getLogger("test_n07_restart")
            )
            recovered = n07_klines("0.012")
            second = restarted.evaluate(
                None, {"quote_volume_top": [candidate]}, {candidate.symbol: recovered}
            )
            self.assertEqual(second.signals[0].reason, "N07_STRUCTURE_CONSUMED")

    def test_invalid_single_candle_first_touch_is_consumed_across_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n07_candidate()
            spike = n07_klines()
            h2_index = len(spike) - 13
            for index in range(h2_index + 1, len(spike) - 1):
                spike[index][1:5] = ["167", "168", "166.5", "167"]
            first = scheduler.evaluate(
                None, {"quote_volume_top": [candidate]}, {candidate.symbol: spike}
            )
            self.assertEqual(first.signals[0].reason, "N07_INVALID_FIRST_TOUCH_SEGMENT")
            restarted = StrategyScheduler(
                (N07_STRATEGY,), 96, recorder, logging.getLogger("test_n07_segment_restart")
            )
            second = restarted.evaluate(
                None,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: n07_klines()},
            )
            self.assertEqual(second.signals[0].reason, "N07_STRUCTURE_CONSUMED")

    def test_funding_positive_negative_or_missing_does_not_affect_n07(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidates = [
                n07_candidate("POSUSDT", Decimal("0.50"), 1),
                n07_candidate("NEGUSDT", Decimal("-0.50"), 2),
                n07_candidate("NONEUSDT", None, 3),
            ]
            scan_id = recorder.begin_scan(3, candidates, dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": candidates},
                {candidate.symbol: n07_klines() for candidate in candidates},
            )

            self.assertEqual([signal.passed for signal in result.signals], [True, True, True])
            self.assertTrue(all(signal.candidate.mark_price == Decimal("141.680") for signal in result.signals))

    def test_rank_100_is_evaluated_and_rank_101_is_excluded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            rank_100 = n07_candidate("R100USDT", None, 100)
            rank_101 = n07_candidate("R101USDT", None, 101)
            scan_id = recorder.begin_scan(2, [rank_100, rank_101], dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [rank_100, rank_101]},
                {
                    rank_100.symbol: n07_klines(),
                    rank_101.symbol: n07_klines(),
                },
            )

            self.assertEqual([signal.candidate.symbol for signal in result.signals], ["R100USDT"])
            self.assertTrue(result.signals[0].passed)

    def test_same_breakout_skeleton_is_emitted_only_once(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n07_candidate()
            first_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            first = scheduler.evaluate(
                first_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: n07_klines()},
            )
            second_scan = recorder.begin_scan(1, [candidate], dry_run=True)

            second = scheduler.evaluate(
                second_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: n07_klines()},
            )

            self.assertTrue(first.signals[0].passed)
            self.assertFalse(second.signals[0].passed)
            self.assertEqual(second.signals[0].reason, "DUPLICATE_STRUCTURE")
            self.assertEqual(
                first.signals[0].analysis.structure_id,
                second.signals[0].analysis.structure_id,
            )

    def test_signal_records_structure_distance_price_rank_and_empty_funding(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n07_candidate(rank=100)
            scan_id = recorder.begin_scan(1, [candidate], dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: n07_klines()},
            )

            self.assertTrue(result.signals[0].passed)
            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT funding_rate, structure_id, current_bullish, detail_json "
                    "FROM strategy_signals"
                ).fetchone()
            detail = json.loads(row[3])
            self.assertEqual(row[0], "")
            self.assertEqual(row[1], detail["structure"]["structure_id"])
            self.assertEqual(row[2], 0)
            self.assertEqual(detail["structure"]["h0"], "148")
            self.assertEqual(detail["structure"]["h1"], "160")
            self.assertEqual(detail["structure"]["p1"], "140")
            self.assertEqual(detail["current_price"], "141.680")
            self.assertEqual(detail["distance_pct"], "0.012")
            self.assertEqual(detail["distance_min"], "0.01")
            self.assertEqual(detail["distance_max"], "0.015")
            self.assertEqual(detail["post_break_low"], "141.40")
            self.assertEqual(detail["segment_config"]["min_bars"], 5)
            self.assertEqual(detail["entry_retrace_segment"]["direction"], "DOWN")
            self.assertEqual(detail["quote_volume_rank"], 100)


class N07SharedMarketDataTests(unittest.TestCase):
    def test_main_records_n07_plan_risk_fields_in_signal_detail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            candidate = n07_candidate()
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_n07_main_record"),
            )
            recorder.upsert_strategy_definitions((N07_STRATEGY,))
            scheduler = StrategyScheduler(
                (N07_STRATEGY,),
                96,
                recorder,
                logging.getLogger("test_n07_main_record"),
            )
            plan = TradePlan(
                symbol=candidate.symbol,
                leverage=5,
                quantity=Decimal("41.906"),
                entry_price=Decimal("113.34"),
                stop_loss_price=Decimal("112"),
                take_profit_price=Decimal("120.04"),
                stop_loss_pct=Decimal("1.34") / Decimal("113.34"),
                take_profit_pct=Decimal("6.70") / Decimal("113.34"),
                amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("0"),
                low_24h_price=Decimal("0"),
                risk_amount=Decimal("200"),
                notional_value=Decimal("4749.62604"),
                required_margin=Decimal("949.925208"),
                balance=Decimal("1000"),
                stop_mode="structure_p1_margin_capped",
                risk_reward_ratio=Decimal("5"),
                structure_id="n07-main-plan",
                structure_stop_price=Decimal("112"),
                target_risk_amount=Decimal("200"),
                actual_risk_amount=Decimal("56.15404"),
                risk_capped_by_margin=True,
            )

            class FakeClient:
                def get_klines(self, symbol):
                    return n07_klines()

            class FakeTrader:
                def __init__(self):
                    self.plan_args = None
                    self.live_open_attempts = []

                def close_dry_run_position_if_triggered(self):
                    return None

                def sync_state_with_exchange(self):
                    # N07 is retained only for historical compatibility.  The
                    # injected test strategy may still build and audit a plan,
                    # but an existing live position must block new execution.
                    return SimpleNamespace(has_position=True, closed_state=None)

                def build_margin_capped_structure_trade_plan(self, *args, **kwargs):
                    self.plan_args = (args, kwargs)
                    return plan

                def open_long_plan_with_protection(self, received_plan):
                    self.live_open_attempts.append(received_plan)
                    raise AssertionError("inactive N07 must not open a live position")

            class FakePaperTrader:
                def __init__(self):
                    self.opened = None

                def close_triggered_open_trades(self, *args):
                    return []

                def open_trade(self, strategy_id, symbol, funding_rate, received_plan, detail):
                    self.opened = (strategy_id, symbol, funding_rate, received_plan, detail)
                    return 1

            class FakeMonitor:
                def scan_for_strategies(self, volume_top_n):
                    return StrategyMarketScan(1, [], [candidate])

            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=True)
            bot.logger = logging.getLogger("test_n07_main_record")
            bot.client = FakeClient()
            bot.trader = FakeTrader()
            bot.paper_trader = FakePaperTrader()
            bot.recorder = recorder
            bot.monitor = FakeMonitor()
            bot.strategy_scheduler = scheduler
            bot.strategies = (N07_STRATEGY,)
            bot.state = SimpleNamespace(
                load=lambda: None,
                save=lambda state: None,
                clear=lambda: None,
            )

            bot._run_once_multi_strategy()

            self.assertEqual(bot.trader.plan_args[0][1], Decimal("141.680"))
            self.assertEqual(bot.trader.plan_args[0][2], P1)
            self.assertEqual(bot.trader.live_open_attempts, [])
            self.assertIsNone(bot.paper_trader.opened)
            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT detail_json FROM strategy_signals WHERE strategy_id = 'N07'"
                ).fetchone()
            detail = json.loads(row[0])
            self.assertEqual(detail["structure"]["p1"], "140")
            self.assertEqual(detail["target_risk_amount"], "200")
            self.assertEqual(detail["actual_risk_amount"], "56.15404")
            self.assertTrue(detail["risk_capped_by_margin"])

    def test_n06_and_n07_reuse_one_candidate_and_one_kline_response(self):
        candidate = n07_candidate(mark_price="500")
        shared_klines = n07_klines(bullish=True)

        class FakeClient:
            def __init__(self):
                self.kline_calls = []

            def get_klines(self, symbol):
                self.kline_calls.append(symbol)
                return shared_klines

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

            def strategy_activity_mode(self, strategy_id):
                return "IDLE"

        class FakeMonitor:
            def scan_for_strategies(self, volume_top_n):
                return StrategyMarketScan(1, [], [candidate])

        class InspectingScheduler:
            def __init__(self):
                self.n06 = None
                self.n07 = None

            def evaluate(self, scan_id, candidate_groups, raw_by_symbol, checked_at_ms=None):
                self.n06 = analyze_n06_double_break_pullback(
                    candidate.symbol,
                    raw_by_symbol[candidate.symbol],
                )
                self.n07 = analyze_n07_p1_retest(
                    candidate.symbol,
                    raw_by_symbol[candidate.symbol],
                )
                return SchedulerResult([], [], [])

            def choose_live_candidate(self, live_candidates, live_blocked):
                return None

        bot = TradingBot.__new__(TradingBot)
        bot.config = SimpleNamespace(dry_run=True)
        bot.logger = logging.getLogger("test_n07_shared_market")
        bot.client = FakeClient()
        bot.trader = FakeTrader()
        bot.paper_trader = FakePaperTrader()
        bot.recorder = FakeRecorder()
        bot.monitor = FakeMonitor()
        bot.strategy_scheduler = InspectingScheduler()
        bot.strategies = (N06_STRATEGY, N07_STRATEGY)
        bot.state = SimpleNamespace(
            load=lambda: None,
            save=lambda state: None,
            clear=lambda: None,
        )

        bot._run_once_multi_strategy()

        self.assertEqual(bot.client.kline_calls, [candidate.symbol])
        self.assertTrue(bot.strategy_scheduler.n06.passed)
        self.assertTrue(bot.strategy_scheduler.n07.passed)


if __name__ == "__main__":
    unittest.main()
