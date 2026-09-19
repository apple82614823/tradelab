from copy import deepcopy
from decimal import Decimal
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from tests.recorder_test_utils import make_test_recorder
from trading_bot.main import TradingBot
from trading_bot.monitor import FundingCandidate, StrategyMarketScan
from trading_bot.n06_analyzer import analyze_n06_double_break_pullback
from trading_bot.recorder import ReviewRecorder, StrategyState
from trading_bot.state import PositionState
from trading_bot.strategies import N06_STRATEGY, load_all_strategies
from trading_bot.strategy_scheduler import (
    LiveTradeCandidate,
    SchedulerResult,
    StrategyScheduler,
    StrategySignalDecision,
)
from trading_bot.trader import TradePlan
from tests.swing_fixtures import build_valid_swing_klines


H0_INDEX = 22
H1_INDEX = 37
P1_INDEX = 42
SECOND_BREAK_INDEX = 49
H2_INDEX = 54
P2_INDEX = 60
STABLE_CONFIRM_INDEX = 65


def candle(index, open_price, high, low, close):
    return [
        index * 900000,
        str(open_price),
        str(high),
        str(low),
        str(close),
        "0",
        index * 900000 + 899999,
    ]


def complete_n06_klines():
    return build_valid_swing_klines()


def n06_candidate(symbol="N06USDT", funding_rate=None, rank=1):
    return FundingCandidate(
        symbol=symbol,
        funding_rate=funding_rate,
        mark_price=Decimal("123"),
        quote_volume=Decimal("1000000"),
        quote_volume_rank=rank,
        candidate_universe="quote_volume_top",
    )


class N06AnalyzerTests(unittest.TestCase):
    def test_entry_window_boundaries_and_exact_latest_fifth_confirmation(self):
        raw = complete_n06_klines()
        open_time = int(raw[-1][0])
        for elapsed in (0, 119999):
            with self.subTest(elapsed=elapsed):
                result = analyze_n06_double_break_pullback(
                    "N06USDT", raw, checked_at_ms=open_time + elapsed
                )
                self.assertTrue(result.passed)
                self.assertEqual(result.structure.stable_confirm_index, len(raw) - 2)
        expired = analyze_n06_double_break_pullback(
            "N06USDT", raw, checked_at_ms=open_time + 120000
        )
        self.assertFalse(expired.passed)
        self.assertEqual(expired.reason, "N06_ENTRY_WINDOW_MISSED")

    def test_historical_sixth_and_sixteenth_candles_are_missed(self):
        base = complete_n06_klines()[:-1]
        for extra_closed in (1, 11):
            with self.subTest(extra_closed=extra_closed):
                raw = deepcopy(base)
                start_index = len(raw)
                for offset in range(extra_closed):
                    raw.append(candle(start_index + offset, 145, 147, 143, 146))
                raw.append(candle(start_index + extra_closed, 141.5, 142, 141.4, 141.68))
                result = analyze_n06_double_break_pullback("N06USDT", raw)
                self.assertFalse(result.passed)
                self.assertEqual(result.reason, "N06_HISTORICAL_STRUCTURE_MISSED")
                self.assertTrue(result.historical_missed)

    def test_completed_structure_remains_backfillable_after_later_p1_break(self):
        raw = complete_n06_klines()[:-1]
        next_index = len(raw)
        raw.append(candle(next_index, 142, 143, 139, 141))
        raw.append(candle(next_index + 1, 141, 143, 140.5, 142))
        result = analyze_n06_double_break_pullback("HISTORYUSDT", raw)
        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N06_HISTORICAL_STRUCTURE_MISSED")
        self.assertEqual(result.structure.stable_confirm_index, STABLE_CONFIRM_INDEX)

    def test_complete_structure_passes_with_expected_points(self):
        result = analyze_n06_double_break_pullback(
            "N06USDT",
            complete_n06_klines(),
        )

        self.assertTrue(result.passed)
        self.assertEqual(result.structure.h0, Decimal("148"))
        self.assertEqual(result.structure.h1, Decimal("160"))
        self.assertEqual(result.structure.p1, Decimal("140"))
        self.assertEqual(result.structure.h2, Decimal("170"))
        self.assertEqual(result.structure.p2, Decimal("142.500"))
        self.assertEqual(result.structure.stable_count, 5)
        self.assertGreaterEqual(
            result.structure.p2_index - result.structure.h2_index,
            N06_STRATEGY.swing_segment_min_bars,
        )

    def test_first_break_wick_without_close_does_not_pass(self):
        raw = complete_n06_klines()
        for index in range(27, len(raw) - 1):
            raw[index][4] = str(min(Decimal(raw[index][4]), Decimal("148")))
        raw[34][2] = "149"
        raw[34][4] = "148"

        result = analyze_n06_double_break_pullback("N06USDT", raw)

        self.assertFalse(result.passed)

    def test_second_break_wick_without_close_does_not_pass(self):
        raw = complete_n06_klines()
        for index in range(SECOND_BREAK_INDEX, len(raw) - 1):
            raw[index][4] = str(min(Decimal(raw[index][4]), Decimal("160")))
        raw[SECOND_BREAK_INDEX][2] = "161"
        raw[SECOND_BREAK_INDEX][4] = "160"

        result = analyze_n06_double_break_pullback("N06USDT", raw)

        self.assertFalse(result.passed)

    def test_only_one_break_does_not_pass(self):
        raw = complete_n06_klines()
        for index in range(SECOND_BREAK_INDEX, len(raw) - 1):
            raw[index][2] = str(min(Decimal(raw[index][2]), Decimal("160")))
            raw[index][4] = str(min(Decimal(raw[index][4]), Decimal("160")))

        result = analyze_n06_double_break_pullback("N06USDT", raw)

        self.assertFalse(result.passed)

    def test_p2_below_p1_invalidates_structure(self):
        raw = complete_n06_klines()
        raw[P2_INDEX][3] = "139"

        result = analyze_n06_double_break_pullback("N06USDT", raw)

        self.assertFalse(result.passed)

    def test_fewer_than_five_stable_candles_does_not_pass(self):
        raw = complete_n06_klines()[:STABLE_CONFIRM_INDEX]
        raw.append(candle(STABLE_CONFIRM_INDEX, 141.5, 142, 141.4, 141.68))

        result = analyze_n06_double_break_pullback("N06USDT", raw)

        self.assertFalse(result.passed)

    def test_lower_low_above_p1_updates_p2_and_resets_count(self):
        raw = complete_n06_klines()

        result = analyze_n06_double_break_pullback("N06USDT", raw)

        self.assertTrue(result.passed)
        self.assertEqual(result.structure.p2, Decimal("142.500"))
        self.assertEqual(result.structure.p2_index, P2_INDEX)
        self.assertEqual(result.structure.stable_count, 5)

    def test_low_below_p1_during_stabilization_invalidates_structure(self):
        raw = complete_n06_klines()
        raw[P2_INDEX + 3][3] = "139"

        result = analyze_n06_double_break_pullback("N06USDT", raw)

        self.assertFalse(result.passed)

    def test_bearish_current_candle_does_not_pass_after_stabilization(self):
        raw = complete_n06_klines()
        raw[-1][4] = "141.4"
        result = analyze_n06_double_break_pullback(
            "N06USDT",
            raw,
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "CURRENT_CANDLE_NOT_BULLISH")

    def test_structure_id_is_stable_when_kline_window_shifts(self):
        original = complete_n06_klines()
        shifted = [candle(-1, 116, 117, 115, 116), *deepcopy(original)]

        first = analyze_n06_double_break_pullback("N06USDT", original)
        second = analyze_n06_double_break_pullback("N06USDT", shifted)

        self.assertTrue(first.passed)
        self.assertTrue(second.passed)
        self.assertEqual(first.structure_id, second.structure_id)


class N06SchedulerTests(unittest.TestCase):
    def _scheduler(self, tmpdir):
        recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_n06"))
        recorder.upsert_strategy_definitions((N06_STRATEGY,))
        scheduler = StrategyScheduler((N06_STRATEGY,), 96, recorder, logging.getLogger("test_n06"))
        return recorder, scheduler

    def test_n06_configuration_is_loaded_separately_from_first_stage(self):
        strategies = load_all_strategies()

        self.assertEqual(
            [strategy.strategy_id for strategy in strategies],
            ["N%02d" % number for number in range(1, 26)],
        )
        self.assertIsNone(N06_STRATEGY.funding_threshold)
        self.assertEqual(N06_STRATEGY.market_filter, "quote_volume_top")
        self.assertEqual(N06_STRATEGY.evaluator_type, "double_break_pullback")
        self.assertEqual(N06_STRATEGY.stop_mode, "structure_p1")
        self.assertEqual(N06_STRATEGY.volume_top_n, 100)
        self.assertEqual((N06_STRATEGY.pivot_left, N06_STRATEGY.pivot_right), (2, 2))
        self.assertEqual(N06_STRATEGY.entry_window_seconds, 120)
        self.assertEqual(N06_STRATEGY.swing_segment_min_bars, 5)
        self.assertEqual(N06_STRATEGY.swing_segment_atr_period, 14)
        self.assertEqual(N06_STRATEGY.swing_segment_min_atr_multiple, Decimal("1.2"))
        self.assertEqual(N06_STRATEGY.swing_segment_min_efficiency, Decimal("0.35"))

    def test_expired_structure_is_backfilled_once_and_never_revives(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n06_candidate()
            raw = complete_n06_klines()
            checked_at = int(raw[-1][0]) + 120000
            first = scheduler.evaluate(
                None,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: raw},
                checked_at_ms=checked_at,
            )
            self.assertEqual(first.signals[0].reason, "N06_ENTRY_WINDOW_MISSED")
            restarted = StrategyScheduler(
                (N06_STRATEGY,), 96, recorder, logging.getLogger("test_n06_restart")
            )
            second = restarted.evaluate(
                None,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: raw},
                checked_at_ms=int(raw[-1][0]) + 30000,
            )
            self.assertEqual(second.signals[0].reason, "N06_STRUCTURE_CONSUMED")
            with recorder._connect() as connection:
                count = connection.execute(
                    "SELECT COUNT(*) FROM strategy_structure_terminal_states WHERE strategy_id='N06'"
                ).fetchone()[0]
            self.assertEqual(count, 1)

    def test_bearish_waits_inside_window_then_expiry_consumes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n06_candidate()
            raw = complete_n06_klines()
            raw[-1][4] = "141.4"
            first = scheduler.evaluate(
                None, {"quote_volume_top": [candidate]}, {candidate.symbol: raw},
                checked_at_ms=int(raw[-1][0]) + 30000,
            )
            self.assertEqual(first.signals[0].reason, "CURRENT_CANDLE_NOT_BULLISH")
            self.assertIsNone(
                recorder.get_strategy_structure_terminal_state(
                    "N06", first.signals[0].analysis.structure_id
                )
            )
            second = scheduler.evaluate(
                None, {"quote_volume_top": [candidate]}, {candidate.symbol: raw},
                checked_at_ms=int(raw[-1][0]) + 120000,
            )
            self.assertEqual(second.signals[0].reason, "N06_ENTRY_WINDOW_MISSED")
            self.assertIsNotNone(
                recorder.get_strategy_structure_terminal_state(
                    "N06", second.signals[0].analysis.structure_id
                )
            )

    def test_positive_and_negative_funding_do_not_affect_n06(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            positive = n06_candidate("POSUSDT", Decimal("0.50"), rank=1)
            negative = n06_candidate("NEGUSDT", Decimal("-0.50"), rank=2)
            scan_id = recorder.begin_scan(2, [positive, negative], dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [positive, negative]},
                {
                    "POSUSDT": complete_n06_klines(),
                    "NEGUSDT": complete_n06_klines(),
                },
            )

            self.assertEqual([signal.passed for signal in result.signals], [True, True])

    def test_same_structure_is_not_emitted_twice(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n06_candidate()
            raw_by_symbol = {candidate.symbol: complete_n06_klines()}
            first_scan_id = recorder.begin_scan(1, [candidate], dry_run=True)
            first = scheduler.evaluate(
                first_scan_id,
                {"quote_volume_top": [candidate]},
                raw_by_symbol,
            )
            second_scan_id = recorder.begin_scan(1, [candidate], dry_run=True)

            second = scheduler.evaluate(
                second_scan_id,
                {"quote_volume_top": [candidate]},
                raw_by_symbol,
            )

            self.assertTrue(first.signals[0].passed)
            self.assertFalse(second.signals[0].passed)
            self.assertEqual(second.signals[0].reason, "DUPLICATE_STRUCTURE")

    def test_lower_p2_after_first_signal_keeps_same_breakout_structure_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n06_candidate()
            first_raw = complete_n06_klines()
            first_scan_id = recorder.begin_scan(1, [candidate], dry_run=True)
            first = scheduler.evaluate(
                first_scan_id,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: first_raw},
            )

            later_raw = complete_n06_klines()[:-1]
            start_index = len(later_raw)
            later_raw.extend(
                [
                    candle(start_index, 145, 147, 143, 146),
                    candle(start_index + 1, 146, 148, 143.5, 147),
                    candle(start_index + 2, 141.5, 142, 141.4, 141.68),
                ]
            )
            later_analysis = analyze_n06_double_break_pullback(
                candidate.symbol,
                later_raw,
            )
            second_scan_id = recorder.begin_scan(1, [candidate], dry_run=True)
            second = scheduler.evaluate(
                second_scan_id,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: later_raw},
            )

            self.assertTrue(first.signals[0].passed)
            self.assertEqual(later_analysis.structure.p2, Decimal("142.500"))
            self.assertEqual(
                later_analysis.structure.stable_confirm_time,
                first.signals[0].analysis.structure.stable_confirm_time,
            )
            self.assertEqual(first.signals[0].analysis.structure_id, later_analysis.structure_id)
            self.assertFalse(second.signals[0].passed)
            self.assertEqual(second.signals[0].reason, "DUPLICATE_STRUCTURE")

    def test_n06_uses_current_kline_close_when_mark_price_has_opposite_direction(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            bullish_candidate = FundingCandidate(
                "BULLUSDT",
                None,
                Decimal("100"),
                quote_volume=Decimal("100"),
                quote_volume_rank=1,
                candidate_universe="quote_volume_top",
            )
            bearish_candidate = FundingCandidate(
                "BEARUSDT",
                None,
                Decimal("999"),
                quote_volume=Decimal("90"),
                quote_volume_rank=2,
                candidate_universe="quote_volume_top",
            )
            bullish_raw = complete_n06_klines()
            bearish_raw = complete_n06_klines()
            bearish_raw[-1][4] = "141.4"
            scan_id = recorder.begin_scan(2, [bullish_candidate, bearish_candidate], dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [bullish_candidate, bearish_candidate]},
                {"BULLUSDT": bullish_raw, "BEARUSDT": bearish_raw},
            )

            by_symbol = {signal.candidate.symbol: signal for signal in result.signals}
            self.assertTrue(by_symbol["BULLUSDT"].passed)
            self.assertEqual(by_symbol["BULLUSDT"].candidate.mark_price, Decimal("141.680"))
            self.assertFalse(by_symbol["BEARUSDT"].passed)
            self.assertEqual(by_symbol["BEARUSDT"].reason, "CURRENT_CANDLE_NOT_BULLISH")
            self.assertEqual(by_symbol["BEARUSDT"].candidate.mark_price, Decimal("141.4"))

    def test_n06_signal_records_structure_rank_and_empty_funding(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n06_candidate(rank=100)
            scan_id = recorder.begin_scan(1, [candidate], dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: complete_n06_klines()},
            )

            self.assertTrue(result.signals[0].passed)
            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT funding_rate, structure_id, detail_json FROM strategy_signals"
                ).fetchone()
            detail = json.loads(row[2])
            self.assertEqual(row[0], "")
            self.assertEqual(row[1], detail["structure"]["structure_id"])
            self.assertEqual(detail["quote_volume_rank"], 100)
            self.assertEqual(detail["structure"]["p1"], "140")
            self.assertEqual(detail["structure"]["p2"], "142.500")
            self.assertEqual(detail["entry_price"], "141.680")
            self.assertEqual(detail["stop_loss_price"], "140")
            self.assertEqual(detail["take_profit_price"], "150.080")
            self.assertEqual(detail["structure"]["h2"], "170")
            self.assertEqual(detail["structure"]["h2_p2_pullback_segment"]["bar_distance"], 6)


class N06SharedMarketDataTests(unittest.TestCase):
    def test_overlapping_strategy_universes_fetch_each_symbol_kline_once(self):
        shared_funding = FundingCandidate(
            "SHAREDUSDT",
            Decimal("-0.02"),
            Decimal("123"),
        )
        shared_volume = FundingCandidate(
            "SHAREDUSDT",
            None,
            Decimal("123"),
            quote_volume=Decimal("1000000"),
            quote_volume_rank=1,
            candidate_universe="quote_volume_top",
        )

        class FakeClient:
            def __init__(self):
                self.kline_calls = []

            def get_mark_price(self, symbol):
                return Decimal("123")

            def get_klines(self, symbol):
                self.kline_calls.append(symbol)
                return complete_n06_klines()

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

            def get_strategy_state(self, strategy_id):
                if strategy_id != "N06":
                    raise AssertionError("unexpected strategy state lookup")
                return strategy_state

            def current_strategy_signal_scan_id(self):
                return 1

        class FakeMonitor:
            def scan_for_strategies(self, volume_top_n):
                return StrategyMarketScan(2, [shared_funding], [shared_volume])

        class FakeScheduler:
            def evaluate(self, scan_id, candidates, raw_klines_by_symbol, checked_at_ms=None):
                return SchedulerResult([], [], [])

            def choose_live_candidate(self, live_candidates, live_blocked):
                return None

        bot = TradingBot.__new__(TradingBot)
        bot.config = SimpleNamespace(dry_run=True)
        bot.logger = logging.getLogger("test_n06_shared_market")
        bot.client = FakeClient()
        bot.trader = FakeTrader()
        bot.paper_trader = FakePaperTrader()
        bot.recorder = FakeRecorder()
        bot.monitor = FakeMonitor()
        bot.strategy_scheduler = FakeScheduler()
        bot.strategies = (N06_STRATEGY,)
        bot.state = SimpleNamespace(
            load=lambda: None,
            save=lambda state: None,
            clear=lambda: None,
        )

        bot._run_once_multi_strategy()

        self.assertEqual(bot.client.kline_calls, ["SHAREDUSDT"])

    def test_main_routes_selected_n06_signal_to_live_only(self):
        candidate = n06_candidate()
        analysis = analyze_n06_double_break_pullback(
            candidate.symbol,
            complete_n06_klines(),
        )
        signal = StrategySignalDecision(
            strategy=N06_STRATEGY,
            candidate=candidate,
            analysis=analysis,
            passed=True,
            decision="PASSED",
            reason="PASSED",
            signal_id=1,
        )
        strategy_state = StrategyState(
            strategy_id="N06",
            consecutive_wins=2,
            paper_trade_count=2,
            win_count=2,
            loss_count=0,
            win_rate="1",
            live_eligible=True,
            last_trade_result="WIN",
            last_trade_closed_at="2026-07-10T00:00:00+00:00",
            updated_at="2026-07-10T00:00:00+00:00",
        )
        plan = TradePlan(
            symbol=candidate.symbol,
            leverage=10,
            quantity=Decimal("10"),
            entry_price=Decimal("123"),
            stop_loss_price=Decimal("112"),
            take_profit_price=Decimal("178"),
            stop_loss_pct=Decimal("11") / Decimal("123"),
            take_profit_pct=Decimal("55") / Decimal("123"),
            amplitude_24h_pct=Decimal("0"),
            high_24h_price=Decimal("0"),
            low_24h_price=Decimal("0"),
            risk_amount=Decimal("110"),
            notional_value=Decimal("1230"),
            required_margin=Decimal("123"),
            balance=Decimal("1000"),
            stop_mode="structure_p1",
            risk_reward_ratio=Decimal("5"),
            structure_id=analysis.structure_id,
            structure_stop_price=Decimal("112"),
        )

        class FakeClient:
            def get_mark_price(self, symbol):
                return Decimal("123")

            def get_klines(self, symbol):
                return complete_n06_klines()

        class FakeTrader:
            def __init__(self):
                self.executed_plan = None

            def close_dry_run_position_if_triggered(self):
                return None

            def sync_state_with_exchange(self):
                return SimpleNamespace(has_position=False, closed_state=None)

            def build_structure_trade_plan(self, *args, **kwargs):
                return plan

            def open_long_plan_with_protection(self, received_plan):
                self.executed_plan = received_plan
                return PositionState(
                    symbol=received_plan.symbol,
                    quantity=str(received_plan.quantity),
                    entry_price=str(received_plan.entry_price),
                    stop_loss_price=str(received_plan.stop_loss_price),
                    take_profit_price=str(received_plan.take_profit_price),
                    leverage=received_plan.leverage,
                    opened_at="2026-07-10T01:00:00+00:00",
                    dry_run=True,
                    orders={"plan": {}},
                )

        class FakePaperTrader:
            def __init__(self):
                self.opened_plan = None

            def close_triggered_open_trades(self, *args):
                return []

            def has_open_trade(self, strategy_id):
                return False

            def open_trade(self, strategy_id, symbol, funding_rate, received_plan, detail):
                self.opened_plan = received_plan
                return 1

        class FakeRecorder:
            def begin_scan(self, scanned_count, candidates, dry_run):
                return 1

            def complete_scan(self, scan_id, opened):
                return None

            def update_strategy_signal(self, *args, **kwargs):
                return True

            def record_event(self, *args, **kwargs):
                return None

            def record_trade_open(self, scan_id, state):
                return 1

            def record_strategy_live_open(self, *args, **kwargs):
                return 1

            def strategy_activity_mode(self, strategy_id):
                return "IDLE"

            def get_strategy_state(self, strategy_id):
                self.assert_n06_strategy_id = strategy_id
                if strategy_id != "N06":
                    raise AssertionError("unexpected strategy state lookup")
                return strategy_state

            def current_strategy_signal_scan_id(self):
                return 1

        class FakeMonitor:
            def scan_for_strategies(self, volume_top_n):
                return StrategyMarketScan(1, [], [candidate])

        class FakeScheduler:
            def evaluate(self, scan_id, candidates, raw_klines_by_symbol, checked_at_ms=None):
                return SchedulerResult(
                    [signal],
                    [signal],
                    [LiveTradeCandidate(signal, strategy_state)],
                    True,
                )

            def choose_live_candidate(self, live_candidates, live_blocked):
                return None if live_blocked else live_candidates[0]

        class FakeStateStore:
            def load(self):
                return getattr(self, "state", None)

            def save(self, state):
                self.state = state

            def clear(self):
                self.state = None

        bot = TradingBot.__new__(TradingBot)
        bot.config = SimpleNamespace(dry_run=True)
        bot.logger = logging.getLogger("test_n06_plan_reuse")
        bot.client = FakeClient()
        bot.trader = FakeTrader()
        bot.paper_trader = FakePaperTrader()
        bot.recorder = FakeRecorder()
        bot.monitor = FakeMonitor()
        bot.strategy_scheduler = FakeScheduler()
        bot.strategies = (N06_STRATEGY,)
        bot.state = FakeStateStore()

        bot._run_once_multi_strategy()

        self.assertIsNone(bot.paper_trader.opened_plan)
        self.assertEqual(bot.trader.executed_plan.entry_price, plan.entry_price)
        self.assertIsNotNone(bot.trader.executed_plan.entry_deadline_ms)


if __name__ == "__main__":
    unittest.main()
