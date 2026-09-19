from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from tests.recorder_test_utils import make_test_recorder
from trading_bot.paper_trader import PaperTrader
from trading_bot.recorder import PaperTradePersistenceError, ReviewRecorder
from trading_bot.trader import TradePlan


def paper_plan(symbol="PAPERUSDT"):
    return TradePlan(
        symbol=symbol,
        leverage=10,
        quantity=Decimal("10"),
        entry_price=Decimal("100"),
        stop_loss_price=Decimal("95"),
        take_profit_price=Decimal("105"),
        stop_loss_pct=Decimal("0.05"),
        take_profit_pct=Decimal("0.05"),
        amplitude_24h_pct=Decimal("0"),
        high_24h_price=Decimal("0"),
        low_24h_price=Decimal("0"),
        risk_amount=Decimal("50"),
        notional_value=Decimal("1000"),
        required_margin=Decimal("100"),
        balance=Decimal("1000"),
    )


def completed_bar(start_time_ms, high, low, close="100"):
    return [
        start_time_ms,
        "100",
        str(high),
        str(low),
        str(close),
        "0",
        start_time_ms + 59999,
    ]


def future_now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000) + 180000


def no_aggregate_trades(symbol, start_time_ms, end_time_ms):
    return []


OPENED_AT = "2026-07-10T10:00:30+00:00"
OPENED_MS = int(datetime.fromisoformat(OPENED_AT).timestamp() * 1000)
NEXT_MINUTE_MS = int(datetime.fromisoformat("2026-07-10T10:01:00+00:00").timestamp() * 1000)


class PaperTraderKlineTests(unittest.TestCase):
    def test_open_write_failure_is_distinct_from_already_open(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_paper_open_failure"),
            )
            paper = PaperTrader(
                recorder,
                logging.getLogger("test_paper_open_failure"),
            )
            with patch.object(
                recorder,
                "_ensure_strategy_state",
                side_effect=sqlite3.OperationalError("forced paper open failure"),
            ), self.assertRaises(PaperTradePersistenceError):
                paper.open_trade(
                    "N01",
                    "PAPERUSDT",
                    Decimal("-0.02"),
                    paper_plan(),
                    {},
                )
            self.assertIsNone(recorder.get_open_strategy_paper_trade("N01"))

            opened = paper.open_trade(
                "N01",
                "PAPERUSDT",
                Decimal("-0.02"),
                paper_plan(),
                {},
            )
            self.assertIsInstance(opened, int)
            self.assertIsNone(
                paper.open_trade(
                    "N01",
                    "OTHERUSDT",
                    Decimal("-0.02"),
                    paper_plan("OTHERUSDT"),
                    {},
                )
            )

    def test_close_and_qualification_failure_roll_back_open_trade(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_paper_close_failure"),
            )
            paper = PaperTrader(
                recorder,
                logging.getLogger("test_paper_close_failure"),
            )
            paper.open_trade(
                "N01",
                "PAPERUSDT",
                Decimal("-0.02"),
                paper_plan(),
                {},
                opened_at=OPENED_AT,
            )
            with recorder._connect() as connection:
                qualification_before = tuple(
                    connection.execute(
                        "SELECT * FROM strategy_states WHERE strategy_id='N01'"
                    ).fetchone()
                )

            with patch.object(
                recorder,
                "_apply_strategy_trade_result",
                side_effect=sqlite3.OperationalError(
                    "forced qualification failure"
                ),
            ), self.assertRaises(PaperTradePersistenceError):
                paper.close_triggered_open_trades(
                    lambda _symbol, start: [
                        completed_bar(start, high="106", low="99", close="101")
                    ],
                    no_aggregate_trades,
                    now_ms=future_now_ms(),
                )

            with recorder._connect() as connection:
                paper_row = connection.execute(
                    "SELECT result, closed_at, exit_reason FROM strategy_paper_trades"
                ).fetchone()
                qualification_after = tuple(
                    connection.execute(
                        "SELECT * FROM strategy_states WHERE strategy_id='N01'"
                    ).fetchone()
                )
            self.assertEqual(tuple(paper_row), ("OPEN", None, None))
            self.assertEqual(qualification_after, qualification_before)

    def test_checkpoint_failure_is_not_reported_as_success(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_paper_checkpoint_failure"),
            )
            paper = PaperTrader(
                recorder,
                logging.getLogger("test_paper_checkpoint_failure"),
            )
            paper.open_trade(
                "N01", "PAPERUSDT", Decimal("-0.02"), paper_plan(), {}
            )
            with patch.object(
                recorder,
                "update_strategy_paper_last_checked",
                return_value=False,
            ), self.assertRaises(PaperTradePersistenceError):
                paper.close_triggered_open_trades(
                    lambda _symbol, start: [
                        completed_bar(start, high="104", low="96", close="100")
                    ],
                    no_aggregate_trades,
                    now_ms=future_now_ms(),
                )
            self.assertIsNone(
                recorder.get_open_strategy_paper_trade("N01").last_checked_at
            )

    def test_opening_partial_minute_stop_is_not_missed_after_rebound(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_partial_stop"))
            paper = PaperTrader(recorder, logging.getLogger("test_partial_stop"))
            paper.open_trade(
                "N01",
                "PAPERUSDT",
                Decimal("-0.02"),
                paper_plan(),
                {},
                opened_at=OPENED_AT,
            )
            aggregate_calls = []

            def get_aggregate_trades(symbol, start_time_ms, end_time_ms):
                aggregate_calls.append((symbol, start_time_ms, end_time_ms))
                return [
                    {"a": 1, "p": "100", "T": OPENED_MS + 5000},
                    {"a": 2, "p": "94", "T": OPENED_MS + 15000},
                    {"a": 3, "p": "100", "T": OPENED_MS + 25000},
                ]

            results = paper.close_triggered_open_trades(
                lambda symbol, start: [],
                get_aggregate_trades,
                now_ms=NEXT_MINUTE_MS + 10000,
            )

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].result, "LOSS")
            self.assertEqual(results[0].mark_price, Decimal("94"))
            self.assertEqual(aggregate_calls[0][1], OPENED_MS)
            self.assertEqual(aggregate_calls[0][2], NEXT_MINUTE_MS - 1)
            with recorder._connect() as connection:
                detail = json.loads(
                    connection.execute(
                        "SELECT detail_json FROM strategy_paper_trades WHERE strategy_id = 'N01'"
                    ).fetchone()[0]
                )
            self.assertEqual(detail["close"]["source"], "OPENING_PARTIAL_AGG_TRADES")

    def test_opening_partial_minute_uses_trade_order_for_first_trigger(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_partial_order"))
            paper = PaperTrader(recorder, logging.getLogger("test_partial_order"))
            paper.open_trade(
                "N01",
                "PAPERUSDT",
                Decimal("-0.02"),
                paper_plan(),
                {},
                opened_at=OPENED_AT,
            )

            results = paper.close_triggered_open_trades(
                lambda symbol, start: [],
                lambda symbol, start, end: [
                    {"a": 10, "p": "106", "T": OPENED_MS + 10000},
                    {"a": 11, "p": "94", "T": OPENED_MS + 15000},
                ],
                now_ms=NEXT_MINUTE_MS + 10000,
            )

            self.assertEqual(results[0].result, "WIN")
            self.assertEqual(results[0].exit_reason, "TAKE_PROFIT")

    def test_opening_partial_without_trigger_continues_to_completed_kline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_partial_continue"))
            paper = PaperTrader(recorder, logging.getLogger("test_partial_continue"))
            paper.open_trade(
                "N01",
                "PAPERUSDT",
                Decimal("-0.02"),
                paper_plan(),
                {},
                opened_at=OPENED_AT,
            )
            kline_calls = []
            aggregate_calls = []

            def get_klines(symbol, start_time_ms):
                kline_calls.append((symbol, start_time_ms))
                return [completed_bar(NEXT_MINUTE_MS, high="106", low="99", close="101")]

            def get_aggregate_trades(symbol, start_time_ms, end_time_ms):
                aggregate_calls.append((symbol, start_time_ms, end_time_ms))
                return [{"a": 1, "p": "100", "T": OPENED_MS + 15000}]

            results = paper.close_triggered_open_trades(
                get_klines,
                get_aggregate_trades,
                now_ms=NEXT_MINUTE_MS + 70000,
            )

            self.assertEqual(results[0].result, "WIN")
            self.assertEqual(kline_calls, [("PAPERUSDT", NEXT_MINUTE_MS)])
            self.assertEqual(len(aggregate_calls), 1)
            with recorder._connect() as connection:
                detail = json.loads(
                    connection.execute(
                        "SELECT detail_json FROM strategy_paper_trades WHERE strategy_id = 'N01'"
                    ).fetchone()[0]
                )
            self.assertEqual(detail["close"]["source"], "COMPLETED_1M_KLINE")

    def test_opening_partial_requests_are_reused_for_same_symbol(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_partial_reuse"))
            paper = PaperTrader(recorder, logging.getLogger("test_partial_reuse"))
            for strategy_id in ("N01", "N02"):
                paper.open_trade(
                    strategy_id,
                    "PAPERUSDT",
                    Decimal("-0.02"),
                    paper_plan(),
                    {},
                    opened_at=OPENED_AT,
                )
            kline_calls = []
            aggregate_calls = []

            def get_klines(symbol, start_time_ms):
                kline_calls.append((symbol, start_time_ms))
                return []

            def get_aggregate_trades(symbol, start_time_ms, end_time_ms):
                aggregate_calls.append((symbol, start_time_ms, end_time_ms))
                return [{"a": 1, "p": "94", "T": OPENED_MS + 15000}]

            results = paper.close_triggered_open_trades(
                get_klines,
                get_aggregate_trades,
                now_ms=NEXT_MINUTE_MS + 10000,
            )

            self.assertEqual(len(results), 2)
            self.assertEqual(len(kline_calls), 0)
            self.assertEqual(len(aggregate_calls), 1)

    def test_unordered_same_timestamp_partial_conflict_uses_stop(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_partial_conflict"))
            paper = PaperTrader(recorder, logging.getLogger("test_partial_conflict"))
            paper.open_trade(
                "N01",
                "PAPERUSDT",
                Decimal("-0.02"),
                paper_plan(),
                {},
                opened_at=OPENED_AT,
            )

            results = paper.close_triggered_open_trades(
                lambda symbol, start: [],
                lambda symbol, start, end: [
                    {"p": "106", "T": OPENED_MS + 15000},
                    {"p": "94", "T": OPENED_MS + 15000},
                ],
                now_ms=NEXT_MINUTE_MS + 10000,
            )

            self.assertEqual(results[0].result, "LOSS")
            self.assertTrue(results[0].conflict)

    def test_intraminute_take_profit_is_recorded_after_price_retraces(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_paper_kline"))
            paper = PaperTrader(recorder, logging.getLogger("test_paper_kline"))
            paper.open_trade("N01", "PAPERUSDT", Decimal("-0.02"), paper_plan(), {})

            results = paper.close_triggered_open_trades(
                lambda symbol, start: [completed_bar(start, high="106", low="99", close="101")],
                no_aggregate_trades,
                now_ms=future_now_ms(),
            )

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].result, "WIN")
            self.assertEqual(results[0].exit_reason, "TAKE_PROFIT")
            self.assertEqual(results[0].mark_price, Decimal("101"))

    def test_same_bar_stop_and_take_profit_uses_conservative_stop(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_paper_conflict"))
            paper = PaperTrader(recorder, logging.getLogger("test_paper_conflict"))
            paper.open_trade("N01", "PAPERUSDT", Decimal("-0.02"), paper_plan(), {})

            results = paper.close_triggered_open_trades(
                lambda symbol, start: [completed_bar(start, high="106", low="94", close="100")],
                no_aggregate_trades,
                now_ms=future_now_ms(),
            )

            self.assertEqual(results[0].result, "LOSS")
            self.assertEqual(results[0].exit_reason, "STOP_LOSS")
            self.assertTrue(results[0].conflict)
            with recorder._connect() as connection:
                detail_json = connection.execute(
                    "SELECT detail_json FROM strategy_paper_trades WHERE strategy_id = 'N01'"
                ).fetchone()[0]
            detail = json.loads(detail_json)
            self.assertTrue(detail["close"]["conflict"])
            self.assertTrue(detail["close"]["stop_hit"])
            self.assertTrue(detail["close"]["take_profit_hit"])

    def test_one_symbol_kline_batch_is_reused_for_multiple_strategies(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_paper_reuse"))
            paper = PaperTrader(recorder, logging.getLogger("test_paper_reuse"))
            plan = paper_plan()
            paper.open_trade("N01", "PAPERUSDT", Decimal("-0.02"), plan, {})
            paper.open_trade("N02", "PAPERUSDT", Decimal("-0.02"), plan, {})
            calls = []

            def get_klines(symbol, start):
                calls.append((symbol, start))
                return [completed_bar(start, high="106", low="99", close="101")]

            results = paper.close_triggered_open_trades(
                get_klines,
                no_aggregate_trades,
                now_ms=future_now_ms(),
            )

            self.assertEqual(len(results), 2)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][0], "PAPERUSDT")

    def test_no_trigger_persists_checkpoint_for_next_poll(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_paper_checkpoint"))
            paper = PaperTrader(recorder, logging.getLogger("test_paper_checkpoint"))
            paper.open_trade("N01", "PAPERUSDT", Decimal("-0.02"), paper_plan(), {})
            starts = []

            def first_poll(symbol, start):
                starts.append(start)
                return [completed_bar(start, high="104", low="96", close="100")]

            paper.close_triggered_open_trades(
                first_poll,
                no_aggregate_trades,
                now_ms=future_now_ms(),
            )
            open_trade = recorder.get_open_strategy_paper_trade("N01")

            def second_poll(symbol, start):
                starts.append(start)
                return []

            paper.close_triggered_open_trades(
                second_poll,
                no_aggregate_trades,
                now_ms=future_now_ms() + 60000,
            )

            self.assertIsNotNone(open_trade.last_checked_at)
            self.assertGreater(starts[1], starts[0])


if __name__ == "__main__":
    unittest.main()
