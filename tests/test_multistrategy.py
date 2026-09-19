from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
import tempfile
import unittest
from pathlib import Path

from tests.recorder_test_utils import make_test_recorder
from trading_bot.monitor import FundingCandidate
from trading_bot.paper_trader import PaperTrader
from trading_bot.recorder import ReviewRecorder, StrategyState
from trading_bot.state import PositionState
from trading_bot.strategies import (
    N06_STRATEGY,
    funding_rate_passes,
    load_active_strategies,
    load_all_strategies,
    load_first_stage_strategies,
)
from trading_bot.strategy_scheduler import LiveTradeCandidate, StrategyScheduler, StrategySignalDecision
from trading_bot.trader import TradePlan


def kline(open_price: str, close_price: str):
    high = str(max(Decimal(open_price), Decimal(close_price)))
    low = str(min(Decimal(open_price), Decimal(close_price)))
    return [0, open_price, high, low, close_price, "0", 0]


def c_pattern_klines():
    raw = [kline(str(100 + i), str(101 + i)) for i in range(79)]
    raw.extend(
        [
            kline("179", "180"),
            kline("181", "182"),
            kline("183", "184"),
            kline("185", "186"),
            kline("189", "188"),
            kline("189", "190"),
            kline("190", "189"),
            kline("188", "187"),
            kline("186", "185"),
            kline("184", "183"),
            kline("182", "181"),
            kline("181", "180"),
            kline("181", "182"),
            kline("182", "183"),
            kline("183", "184"),
            kline("185", "184"),
            kline("185", "186"),
            kline("189", "188"),
            kline("189", "190"),
            kline("191", "192"),
            kline("192", "192"),
        ]
    )
    return raw


def trade_plan(symbol="BOUNCEUSDT"):
    return TradePlan(
        symbol=symbol,
        leverage=50,
        quantity=Decimal("10"),
        entry_price=Decimal("100"),
        stop_loss_price=Decimal("99"),
        take_profit_price=Decimal("105"),
        stop_loss_pct=Decimal("0.01"),
        take_profit_pct=Decimal("0.05"),
        amplitude_24h_pct=Decimal("0.08333333333333333333333333333"),
        high_24h_price=Decimal("105"),
        low_24h_price=Decimal("97"),
        risk_amount=Decimal("100"),
        notional_value=Decimal("1000"),
        required_margin=Decimal("20"),
        balance=Decimal("1000"),
    )


def close_paper_with_bar(paper, high: str, low: str, close: str = "100"):
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000) + 180000

    def get_klines(symbol, start_time_ms):
        return [[start_time_ms, "100", high, low, close, "0", start_time_ms + 59999]]

    return paper.close_triggered_open_trades(
        get_klines,
        lambda symbol, start_time_ms, end_time_ms: [],
        now_ms=now_ms,
    )


def record_exact_live_result(recorder, strategy_id, symbol, result, opened_at):
    state = PositionState(
        symbol=symbol,
        quantity="10",
        entry_price="100",
        stop_loss_price="95",
        take_profit_price="125",
        leverage=10,
        opened_at=opened_at,
        dry_run=False,
        orders={"plan": {}, "strategy": {"strategy_id": strategy_id}},
    )
    trade_id = recorder.record_trade_open(None, state)
    if trade_id is None:
        raise AssertionError("live trade review open was not recorded")
    if recorder.record_strategy_live_open(strategy_id, trade_id, symbol, opened_at) is None:
        raise AssertionError("exact live link was not recorded")
    exit_reason = "TAKE_PROFIT" if result == "WIN" else "STOP_LOSS"
    exit_price = "125" if result == "WIN" else "95"
    if recorder.record_trade_close(
        state, exit_reason, exit_price, "", "", "", "", {}
    ) != trade_id:
        raise AssertionError("live trade review close was not recorded")
    return recorder.record_strategy_live_result(
        strategy_id,
        result,
        symbol,
        trade_id,
        opened_at,
    )


class MultiStrategyTests(unittest.TestCase):
    def test_first_stage_strategy_configs_are_loaded(self):
        strategies = load_first_stage_strategies()

        self.assertEqual([strategy.strategy_id for strategy in strategies], ["N01", "N02", "N03", "N04", "N05"])
        self.assertEqual(strategies[0].allowed_patterns, ("C",))
        self.assertEqual(strategies[0].funding_threshold, Decimal("0.015"))
        self.assertEqual(strategies[0].loss_symbol_cooldown_hours, 24)
        self.assertEqual(strategies[3].allowed_patterns, ("A", "B", "C"))
        self.assertEqual(strategies[3].funding_threshold, Decimal("0.020"))
        self.assertEqual(
            tuple(item.strategy_id for item in load_active_strategies()),
            ("N01", "N02", "N03", "N04", "N05"),
        )
        self.assertEqual(len(load_all_strategies()), 25)

    def test_funding_rate_direction_uses_negative_threshold(self):
        self.assertTrue(funding_rate_passes(Decimal("-0.016"), Decimal("0.015")))
        self.assertFalse(funding_rate_passes(Decimal("0.016"), Decimal("0.015")))

    def test_scheduler_reuses_one_kline_batch_for_five_strategy_signals(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_scheduler"))
            strategies = load_first_stage_strategies()
            recorder.upsert_strategy_definitions(strategies)
            scheduler = StrategyScheduler(strategies, 96, recorder, logging.getLogger("test_scheduler"))
            candidate = FundingCandidate("BOUNCEUSDT", Decimal("-0.021"), Decimal("193"))
            raw_klines_by_symbol = {"BOUNCEUSDT": c_pattern_klines()}
            scan_id = recorder.begin_scan(1, [candidate], dry_run=True)

            result = scheduler.evaluate(scan_id, [candidate], raw_klines_by_symbol)

            self.assertEqual(len(result.signals), 5)
            self.assertEqual(len(result.passed_signals), 5)
            self.assertEqual(len(result.live_candidates), 5)
            self.assertEqual(
                {item.signal.strategy.strategy_id for item in result.live_candidates},
                {"N01", "N02", "N03", "N04", "N05"},
            )
            self.assertTrue(
                all(
                    not recorder.get_strategy_state(strategy_id).live_eligible
                    for strategy_id in ("N01", "N02", "N03", "N04", "N05")
                )
            )
            with recorder._connect() as connection:
                count = connection.execute("SELECT COUNT(*) FROM strategy_signals").fetchone()[0]
                self.assertEqual(count, 5)

    def test_active_registration_does_not_rewrite_historical_strategy_definitions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_active_registration"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            with recorder._read_only_runtime_snapshot() as connection:
                before = connection.execute(
                    "SELECT * FROM strategy_definitions WHERE strategy_id='N25'"
                ).fetchone()

            recorder.upsert_strategy_definitions(load_active_strategies())

            with recorder._read_only_runtime_snapshot() as connection:
                after = connection.execute(
                    "SELECT * FROM strategy_definitions WHERE strategy_id='N25'"
                ).fetchone()
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_definitions"
                    ).fetchone(),
                    (25,),
                )
            self.assertEqual(after, before)

    def test_each_strategy_can_hold_independent_paper_trade(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_paper"))
            paper = PaperTrader(recorder, logging.getLogger("test_paper"))
            plan = trade_plan()

            first = paper.open_trade("N01", "BOUNCEUSDT", Decimal("-0.016"), plan, {})
            second = paper.open_trade("N02", "BOUNCEUSDT", Decimal("-0.016"), plan, {})
            duplicate = paper.open_trade("N01", "BOUNCEUSDT", Decimal("-0.016"), plan, {})

            self.assertIsNotNone(first)
            self.assertIsNotNone(second)
            self.assertIsNone(duplicate)
            self.assertEqual(len(recorder.get_open_strategy_paper_trades()), 2)

    def test_legacy_paper_wins_update_statistics_without_advancing_qualification(self):
        for strategy_id in ("N01", "N21", "N22", "N23", "N25"):
            with self.subTest(strategy_id=strategy_id), tempfile.TemporaryDirectory() as tmpdir:
                recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_paper"))
                paper = PaperTrader(recorder, logging.getLogger("test_paper"))
                paper.open_trade(strategy_id, "FIRSTUSDT", Decimal("-0.016"), trade_plan("FIRSTUSDT"), {})
                close_paper_with_bar(paper, "105", "100", "101")
                paper.open_trade(strategy_id, "SECONDUSDT", Decimal("-0.016"), trade_plan("SECONDUSDT"), {})
                close_paper_with_bar(paper, "105", "100", "101")

                state = recorder.get_strategy_state(strategy_id)
                self.assertEqual(state.consecutive_wins, 0)
                self.assertEqual(state.paper_trade_count, 2)
                self.assertEqual(state.win_count, 2)
                self.assertFalse(state.live_eligible)

    def test_live_loss_resets_eligibility_and_consecutive_wins(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_live"))
            paper = PaperTrader(recorder, logging.getLogger("test_live"))
            plan = trade_plan()
            paper.open_trade("N01", "BOUNCEUSDT", Decimal("-0.016"), plan, {})
            close_paper_with_bar(paper, "105", "100", "101")
            paper.open_trade("N01", "BOUNCEUSDT", Decimal("-0.016"), plan, {})
            close_paper_with_bar(paper, "105", "100", "101")

            self.assertTrue(
                record_exact_live_result(
                    recorder,
                    "N01",
                    "BOUNCEUSDT",
                    "LOSS",
                    "2026-07-13T00:00:00+00:00",
                )
            )

            state = recorder.get_strategy_state("N01")
            self.assertEqual(state.consecutive_wins, 0)
            self.assertFalse(state.live_eligible)

    def test_legacy_paper_close_cannot_clear_pending_live_result(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_pending"))
            paper = PaperTrader(recorder, logging.getLogger("test_pending"))
            paper.open_trade("N01", "BOUNCEUSDT", Decimal("-0.016"), trade_plan(), {})
            recorder.mark_strategy_live_result_pending("N01", "PROTECTION_ORDER_QUERY_FAILED")

            close_paper_with_bar(paper, "105", "100", "101")

            state = recorder.get_strategy_state("N01")
            self.assertTrue(state.live_result_pending)
            self.assertFalse(state.live_eligible)
            self.assertEqual(state.last_trade_result, "LIVE_RESULT_PENDING")

    def test_strategy_symbol_cooldown_after_loss_blocks_same_symbol(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_cooldown"))
            strategies = load_first_stage_strategies()
            recorder.upsert_strategy_definitions(strategies)
            paper = PaperTrader(recorder, logging.getLogger("test_cooldown"))
            scheduler = StrategyScheduler(strategies, 96, recorder, logging.getLogger("test_cooldown"))
            plan = trade_plan()
            paper.open_trade("N01", "BOUNCEUSDT", Decimal("-0.016"), plan, {})
            close_paper_with_bar(paper, "100", "99", "100")
            candidate = FundingCandidate("BOUNCEUSDT", Decimal("-0.021"), Decimal("193"))
            scan_id = recorder.begin_scan(1, [candidate], dry_run=True)

            result = scheduler.evaluate(scan_id, [candidate], {"BOUNCEUSDT": c_pattern_klines()})

            n01 = [signal for signal in result.signals if signal.strategy.strategy_id == "N01"][0]
            self.assertFalse(n01.passed)
            self.assertIn("SYMBOL_COOLDOWN_UNTIL", n01.reason)

    def test_global_or_manual_position_blocks_live_but_not_paper(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_block"))
            strategies = load_first_stage_strategies()
            recorder.upsert_strategy_definitions(strategies)
            paper = PaperTrader(recorder, logging.getLogger("test_block"))
            scheduler = StrategyScheduler(strategies, 96, recorder, logging.getLogger("test_block"))
            plan = trade_plan()
            paper.open_trade("N01", "BOUNCEUSDT", Decimal("-0.016"), plan, {})
            close_paper_with_bar(paper, "105", "100", "101")
            paper.open_trade("N01", "BOUNCEUSDT", Decimal("-0.016"), plan, {})
            close_paper_with_bar(paper, "105", "100", "101")
            candidate = FundingCandidate("BOUNCEUSDT", Decimal("-0.021"), Decimal("193"))
            scan_id = recorder.begin_scan(1, [candidate], dry_run=True)
            result = scheduler.evaluate(scan_id, [candidate], {"BOUNCEUSDT": c_pattern_klines()})

            live_choice = scheduler.choose_live_candidate(result.live_candidates, live_blocked=True)
            paper_id = paper.open_trade("N02", "BOUNCEUSDT", Decimal("-0.021"), plan, {})

            self.assertIsNone(live_choice)
            self.assertIsNotNone(paper_id)

    def test_global_symbol_cooldown_blocks_all_strategies_and_records_one_event(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_global_cooldown"))
            strategies = load_all_strategies()
            recorder.upsert_strategy_definitions(strategies)
            recorder.set_symbol_cooldown(
                "COOLUSDT",
                datetime.now(timezone.utc) + timedelta(hours=4),
                "STOP_LOSS",
                None,
            )
            scheduler = StrategyScheduler(strategies, 96, recorder, logging.getLogger("test_global_cooldown"))
            funding = FundingCandidate("COOLUSDT", Decimal("-0.021"), Decimal("193"))
            volume = FundingCandidate(
                "COOLUSDT",
                None,
                Decimal("193"),
                quote_volume=Decimal("1000000"),
                quote_volume_rank=1,
                candidate_universe="quote_volume_top",
            )
            scan_id = recorder.begin_scan(2, [funding, volume], dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"negative_funding": [funding], "quote_volume_top": [volume]},
                {"COOLUSDT": c_pattern_klines()},
            )

            self.assertEqual(len(result.signals), len(strategies))
            self.assertEqual(
                {signal.strategy.strategy_id for signal in result.signals},
                {f"N{index:02d}" for index in range(1, 26)},
            )
            self.assertTrue(all(not signal.passed for signal in result.signals))
            by_strategy = {signal.strategy.strategy_id: signal for signal in result.signals}
            self.assertEqual(
                {
                    strategy_id: by_strategy[strategy_id].reason
                    for strategy_id in ("N17", "N18", "N19")
                },
                {
                    "N17": "N17_HISTORY_SOURCE_INSUFFICIENT",
                    "N18": "N18_HISTORY_SOURCE_INSUFFICIENT",
                    "N19": "N19_HISTORY_SOURCE_INSUFFICIENT",
                },
            )
            self.assertTrue(
                all(
                    signal.reason.startswith("GLOBAL_SYMBOL_COOLDOWN_UNTIL:")
                    for strategy_id, signal in by_strategy.items()
                    if strategy_id not in {"N17", "N18", "N19"}
                )
            )
            with recorder._connect() as connection:
                event_count = connection.execute(
                    "SELECT COUNT(*) FROM events WHERE event_type = 'global_symbol_cooldown_skip'"
                ).fetchone()[0]
            self.assertEqual(event_count, 1)

    def test_live_candidate_tie_uses_stable_ids_not_funding_or_recent_time(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_priority"))
            scheduler = StrategyScheduler((N06_STRATEGY,), 96, recorder, logging.getLogger("test_priority"))

            def live_candidate(symbol: str, funding_rate: Decimal, closed_at: str):
                signal = StrategySignalDecision(
                    strategy=N06_STRATEGY,
                    candidate=FundingCandidate(
                        symbol,
                        funding_rate,
                        Decimal("100"),
                        quote_volume=Decimal("1"),
                        quote_volume_rank=1,
                        candidate_universe="quote_volume_top",
                    ),
                    analysis=None,
                    passed=True,
                    decision="PASSED",
                    reason="PASSED",
                )
                state = StrategyState(
                    strategy_id="N06",
                    consecutive_wins=2,
                    paper_trade_count=2,
                    win_count=1,
                    loss_count=1,
                    win_rate="0.5",
                    live_eligible=True,
                    last_trade_result="WIN",
                    last_trade_closed_at=closed_at,
                    updated_at=closed_at,
                )
                return LiveTradeCandidate(signal, state)

            alphabetic = live_candidate("AUSDT", Decimal("0.50"), "2020-01-01T00:00:00+00:00")
            more_negative = live_candidate("ZUSDT", Decimal("-0.99"), "2030-01-01T00:00:00+00:00")

            chosen = scheduler.choose_live_candidate([more_negative, alphabetic], live_blocked=False)

            self.assertEqual(chosen.signal.candidate.symbol, "AUSDT")

    def test_live_candidate_scoring_does_not_read_legacy_paper_win_rate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_win_rate"))
            scheduler = StrategyScheduler((N06_STRATEGY,), 96, recorder, logging.getLogger("test_win_rate"))

            def candidate(symbol: str, win_rate: str, funding_rate: str):
                signal = StrategySignalDecision(
                    strategy=N06_STRATEGY,
                    candidate=FundingCandidate(
                        symbol,
                        Decimal(funding_rate),
                        Decimal("100"),
                        quote_volume=Decimal("1"),
                        quote_volume_rank=1,
                        candidate_universe="quote_volume_top",
                    ),
                    analysis=None,
                    passed=True,
                    decision="PASSED",
                    reason="PASSED",
                )
                state = StrategyState(
                    strategy_id="N06",
                    consecutive_wins=2,
                    paper_trade_count=10,
                    win_count=0,
                    loss_count=0,
                    win_rate=win_rate,
                    live_eligible=True,
                    last_trade_result=None,
                    last_trade_closed_at=None,
                    updated_at="2026-01-01T00:00:00+00:00",
                )
                return LiveTradeCandidate(signal, state)

            lower = candidate("AUSDT", "0.4", "-0.99")
            higher = candidate("ZUSDT", "0.8", "0.99")

            chosen = scheduler.choose_live_candidate([lower, higher], live_blocked=False)

            self.assertEqual(chosen.signal.candidate.symbol, "AUSDT")


if __name__ == "__main__":
    unittest.main()
