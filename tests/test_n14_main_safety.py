from decimal import Decimal
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from trading_bot.analyzer import AnalysisResult
from trading_bot.binance_client import BinanceAPIError
from trading_bot.main import TradingBot
from trading_bot.monitor import FundingCandidate
from trading_bot.state import PositionState, StateStore
from trading_bot.trader import MarketOrderExecutionPendingError, SyncResult


def cleanup_resolved_state() -> PositionState:
    return PositionState(
        symbol="N14USDT",
        quantity="0",
        entry_price="100",
        stop_loss_price="99",
        take_profit_price="105",
        leverage=10,
        opened_at="2026-07-12T00:00:00+00:00",
        dry_run=False,
        orders={
            "strategy": {"strategy_id": "N14"},
            "execution_cleanup_resolved": {
                "reason": "EXECUTION_CLEANUP_RESOLVED_WITHOUT_TRADE_RESULT",
                "strategy_id": "N14",
                "emergency_cleanup": {"confirmed_closed": True},
                "protection_cleanup": {"confirmed_all_canceled": True},
            },
        },
    )


class PendingTrader:
    def __init__(self):
        self.live_open_calls = 0

    def close_dry_run_position_if_triggered(self):
        return None

    def sync_state_with_exchange(self):
        return SyncResult(
            has_position=False,
            pending_resolved=True,
            pending_detail={
                "reason": "EXECUTION_CLEANUP_RESOLVED_WITHOUT_TRADE_RESULT",
                "strategy_id": "N14",
            },
        )

    def open_long_with_protection(self, symbol, mark_price):
        self.live_open_calls += 1
        raise AssertionError("pending cleanup must block single-strategy order placement")

    def open_long_plan_with_protection(self, trade_plan):
        self.live_open_calls += 1
        raise AssertionError("pending cleanup must block multi-strategy order placement")


class AuditRecorder:
    def __init__(self, *, event_saved: bool, pending_saved: bool):
        self.event_saved = event_saved
        self.pending_saved = pending_saved
        self.calls = []

    def record_event(self, event_type, payload=None, symbol=None):
        self.calls.append(("event", event_type, payload, symbol))
        return self.event_saved

    def mark_strategy_live_result_pending(self, strategy_id, reason):
        self.calls.append(("mark_pending", strategy_id, reason))
        return self.pending_saved

    def assert_no_active_live_link_for_cleanup(self, strategy_id, symbol):
        if strategy_id != "N14" or symbol != "N14USDT":
            raise AssertionError("cleanup identity changed")


class ForbiddenMonitor:
    def __init__(self):
        self.scan_calls = 0

    def scan(self):
        self.scan_calls += 1
        raise AssertionError("pending cleanup must stop the current scan")

    def scan_for_strategies(self, volume_top_n):
        self.scan_calls += 1
        raise AssertionError("pending cleanup must stop the current scan")


class IdlePaperTrader:
    def __init__(self):
        self.close_calls = 0
        self.open_calls = 0

    def close_triggered_open_trades(self, *args, **kwargs):
        self.close_calls += 1
        return []

    def open_trade(self, *args, **kwargs):
        self.open_calls += 1
        raise AssertionError("pending cleanup must block paper order placement")


class FailingOrderTrader:
    def __init__(self):
        self.order_calls = 0

    def close_dry_run_position_if_triggered(self):
        return None

    def sync_state_with_exchange(self):
        return SyncResult(has_position=False)

    def open_long_with_protection(self, symbol, mark_price):
        self.order_calls += 1
        raise BinanceAPIError("simulated order failure")


class SingleOrderRecorder:
    def __init__(self):
        self.failed = []
        self.completed = []
        self.events = []
        self.published_scan_id = None

    def active_symbol_cooldown(self, symbol):
        return None

    def begin_scan(self, scanned_count, candidates, dry_run):
        return 41

    def record_signal(self, scan_id, candidate, analysis, decision):
        return 1

    def publish_strategy_signal_batch(self, scan_id, expected_count):
        self.published_scan_id = scan_id
        return expected_count > 0

    def current_strategy_signal_scan_id(self):
        return self.published_scan_id

    def record_trade_failure(self, scan_id, symbol, error, dry_run):
        self.failed.append((scan_id, symbol, error, dry_run))
        return 1

    def record_event(self, event_type, payload=None, symbol=None):
        self.events.append((event_type, payload, symbol))
        return True

    def complete_scan(self, scan_id, opened):
        self.completed.append((scan_id, opened))
        return True


class SingleOrderMonitor:
    def scan(self):
        candidate = FundingCandidate(
            symbol="FAILUSDT",
            funding_rate=Decimal("-0.02"),
            mark_price=Decimal("100"),
        )
        return 1, [candidate]


class TwoCandidateMonitor:
    def scan(self):
        return 2, [
            FundingCandidate(
                symbol="FIRSTUSDT",
                funding_rate=Decimal("-0.02"),
                mark_price=Decimal("100"),
            ),
            FundingCandidate(
                symbol="SECONDUSDT",
                funding_rate=Decimal("-0.019"),
                mark_price=Decimal("100"),
            ),
        ]


class PendingOnFirstCandidateTrader:
    def __init__(self, state_store):
        self.state_store = state_store
        self.order_symbols = []

    def close_dry_run_position_if_triggered(self):
        return None

    def sync_state_with_exchange(self):
        return SyncResult(has_position=False)

    def open_long_with_protection(self, symbol, mark_price):
        self.order_symbols.append(symbol)
        if len(self.order_symbols) > 1:
            raise AssertionError(
                "a pending first BUY must block the second candidate"
            )
        self.state_store.save(
            PositionState(
                symbol=symbol,
                quantity="1",
                entry_price="100",
                stop_loss_price="99",
                take_profit_price="105",
                leverage=10,
                opened_at="2026-07-12T00:00:00+00:00",
                dry_run=False,
                orders={
                    "execution_pending": {
                        "reason": "MARKET_ORDER_EXECUTION_UNKNOWN",
                        "client_order_id": "mkt-first",
                    }
                },
            )
        )
        raise MarketOrderExecutionPendingError("first BUY is unknown")


class N14MainExecutionSafetyTests(unittest.TestCase):
    def _pending_bot(self, tmpdir, *, multi_strategy, event_saved, pending_saved):
        state_store = StateStore(str(Path(tmpdir) / "position.json"))
        state_store.save(cleanup_resolved_state())
        recorder = AuditRecorder(
            event_saved=event_saved,
            pending_saved=pending_saved,
        )
        monitor = ForbiddenMonitor()
        trader = PendingTrader()
        paper_trader = IdlePaperTrader()

        bot = TradingBot.__new__(TradingBot)
        bot.config = SimpleNamespace(
            dry_run=False,
            multi_strategy_enabled=multi_strategy,
        )
        bot.logger = logging.getLogger("test_n14_main_safety")
        bot.state = state_store
        bot.recorder = recorder
        bot.monitor = monitor
        bot.trader = trader
        bot.paper_trader = paper_trader
        bot.strategies = ()
        return bot, recorder, monitor, trader, paper_trader, state_store

    def test_single_strategy_order_failure_is_recorded_without_name_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=False, trend_window=96)
            bot.logger = logging.getLogger("test_single_order_failure")
            bot.state = StateStore(str(Path(tmpdir) / "position.json"))
            bot.recorder = SingleOrderRecorder()
            bot.monitor = SingleOrderMonitor()
            bot.trader = FailingOrderTrader()
            bot.client = SimpleNamespace(get_klines=lambda symbol: [])
            passed = AnalysisResult(
                symbol="FAILUSDT",
                passed=True,
                trend_slope=Decimal("1"),
                pattern="C_UP_PULLBACK_BOUNCE",
                current_bullish=True,
                detail="passed",
                matched_patterns=("C",),
            )

            with patch("trading_bot.main.analyze_symbol", return_value=passed):
                bot._run_once_single_strategy()

            self.assertEqual(bot.trader.order_calls, 1)
            self.assertEqual(
                bot.recorder.failed,
                [(41, "FAILUSDT", "simulated order failure", False)],
            )
            self.assertEqual(bot.recorder.completed, [(41, False)])

    def test_single_strategy_pending_first_buy_blocks_second_candidate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_store = StateStore(str(Path(tmpdir) / "position.json"))
            recorder = SingleOrderRecorder()
            trader = PendingOnFirstCandidateTrader(state_store)
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=False, trend_window=96)
            bot.logger = logging.getLogger("test_single_pending_buy")
            bot.state = state_store
            bot.recorder = recorder
            bot.monitor = TwoCandidateMonitor()
            bot.trader = trader
            bot.client = SimpleNamespace(get_klines=lambda symbol: [])
            passed = AnalysisResult(
                symbol="FIRSTUSDT",
                passed=True,
                trend_slope=Decimal("1"),
                pattern="C_UP_PULLBACK_BOUNCE",
                current_bullish=True,
                detail="passed",
                matched_patterns=("C",),
            )

            with patch(
                "trading_bot.main.analyze_symbol", return_value=passed
            ):
                bot._run_once_single_strategy()

            self.assertEqual(trader.order_symbols, ["FIRSTUSDT"])
            self.assertIsNotNone(state_store.load())
            self.assertEqual(recorder.completed, [(41, False)])
            self.assertEqual(
                [event[0] for event in recorder.events],
                ["single_strategy_order_local_pending"],
            )

    def test_single_cleanup_without_live_result_requires_durable_event_only(self):
        for event_saved, pending_saved, should_clear in (
            (True, True, True),
            (False, True, False),
            (True, False, True),
            (False, False, False),
        ):
            with self.subTest(
                event_saved=event_saved,
                pending_saved=pending_saved,
            ), tempfile.TemporaryDirectory() as tmpdir:
                bot, recorder, monitor, trader, _, state_store = self._pending_bot(
                    tmpdir,
                    multi_strategy=False,
                    event_saved=event_saved,
                    pending_saved=pending_saved,
                )

                self.assertFalse(
                    bot._reconcile_single_strategy_after_publication()
                )

                self.assertEqual(
                    [call[0] for call in recorder.calls],
                    ["event"],
                )
                self.assertEqual(
                    recorder.calls[0][1],
                    "execution_pending_resolved",
                )
                self.assertEqual(
                    state_store.load() is None,
                    should_clear,
                )
                self.assertEqual(trader.live_open_calls, 0)

    def test_multi_cleanup_without_live_result_requires_durable_event_only(self):
        for event_saved, pending_saved, should_clear in (
            (True, True, True),
            (False, True, False),
            (True, False, True),
            (False, False, False),
        ):
            with self.subTest(
                event_saved=event_saved,
                pending_saved=pending_saved,
            ), tempfile.TemporaryDirectory() as tmpdir:
                bot, recorder, monitor, trader, paper_trader, state_store = self._pending_bot(
                    tmpdir,
                    multi_strategy=True,
                    event_saved=event_saved,
                    pending_saved=pending_saved,
                )

                self.assertIsNone(
                    bot._reconcile_multi_strategy_after_publication()
                )

                self.assertEqual(
                    [call[0] for call in recorder.calls],
                    ["event"],
                )
                self.assertEqual(
                    recorder.calls[0][1],
                    "strategy_execution_pending_resolved",
                )
                self.assertEqual(
                    state_store.load() is None,
                    should_clear,
                )
                self.assertEqual(paper_trader.close_calls, 0)
                self.assertEqual(trader.live_open_calls, 0)


if __name__ == "__main__":
    unittest.main()
