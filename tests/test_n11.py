from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import json
import logging
from pathlib import Path
import tempfile
import unittest

from tests.recorder_test_utils import make_test_recorder
from trading_bot.main import TradingBot
from trading_bot.binance_client import BinanceAPIError
from trading_bot.monitor import FundingCandidate
from trading_bot.n11_analyzer import (
    _IndicatorSeries,
    _is_squeeze,
    analyze_n11_volatility_squeeze_breakout_retest,
    parse_n11_klines,
)
from trading_bot.paper_trader import PaperTrader
from trading_bot.recorder import ReviewRecorder
from trading_bot.strategies import N11_STRATEGY, load_all_strategies
from trading_bot.strategy_scheduler import StrategyScheduler
from trading_bot.state import StateStore
from trading_bot.trader import EntryWindowExpiredError, TradePlan, Trader
from tests.test_trader import (
    FakeLiveExecutionClient,
    RuleConstrainedClient,
    live_test_config,
    test_config,
)


BASE_TIME_MS = 1_720_000_000_000
INTERVAL_MS = 15 * 60 * 1000


def kline(index, open_price, high, low, close, quote_volume=100, taker_buy_quote=50):
    open_time = BASE_TIME_MS + index * INTERVAL_MS
    return [
        open_time,
        str(open_price),
        str(high),
        str(low),
        str(close),
        "1",
        open_time + INTERVAL_MS - 1,
        str(quote_volume),
        10,
        "0.5",
        str(taker_buy_quote),
        "0",
    ]


def n11_klines(
    *,
    start_index=0,
    price_offset=Decimal("0"),
    retest_delay=1,
    breakout_volume=Decimal("180"),
    breakout_taker_buy=Decimal("99"),
    retest_volume=Decimal("144"),
    elapsed_ms=30_000,
):
    rows = []
    for offset in range(60):
        close = Decimal("100") + price_offset + Decimal(offset) * Decimal("0.01")
        rows.append(
            kline(
                start_index + offset,
                close - Decimal("0.02"),
                close + Decimal("0.15"),
                close - Decimal("0.15"),
                close,
            )
        )

    breakout_level = Decimal("100.74") + price_offset
    rows.append(
        kline(
            start_index + 60,
            Decimal("100.60") + price_offset,
            Decimal("101.70") + price_offset,
            Decimal("100.50") + price_offset,
            Decimal("101.60") + price_offset,
            breakout_volume,
            breakout_taker_buy,
        )
    )
    for delay in range(1, retest_delay):
        rows.append(
            kline(
                start_index + 60 + delay,
                Decimal("101.25") + price_offset,
                Decimal("101.55") + price_offset,
                Decimal("101.15") + price_offset,
                Decimal("101.35") + price_offset,
                Decimal("120"),
                Decimal("66"),
            )
        )
    retest_index = start_index + 60 + retest_delay
    rows.append(
        kline(
            retest_index,
            breakout_level + Decimal("0.16"),
            breakout_level + Decimal("0.26"),
            breakout_level + Decimal("0.01"),
            breakout_level + Decimal("0.21"),
            retest_volume,
            retest_volume * Decimal("0.5"),
        )
    )
    entry_index = retest_index + 1
    rows.append(
        kline(
            entry_index,
            breakout_level + Decimal("0.21"),
            breakout_level + Decimal("0.36"),
            breakout_level + Decimal("0.16"),
            breakout_level + Decimal("0.26"),
            Decimal("100"),
            Decimal("55"),
        )
    )
    checked_at_ms = BASE_TIME_MS + entry_index * INTERVAL_MS + elapsed_ms
    return rows, checked_at_ms


def candidate(symbol="N11USDT", funding_rate=None, rank=1):
    return FundingCandidate(
        symbol=symbol,
        funding_rate=funding_rate,
        mark_price=Decimal("999"),
        quote_volume=Decimal("1000000"),
        quote_volume_rank=rank,
        candidate_universe="quote_volume_top",
    )


class N11AnalyzerTests(unittest.TestCase):
    def analyze(self, raw, checked, **kwargs):
        return analyze_n11_volatility_squeeze_breakout_retest(
            "N11USDT",
            raw,
            checked_at_ms=checked,
            **kwargs,
        )

    def test_complete_structure_passes_with_all_metrics(self):
        raw, checked = n11_klines()
        result = self.analyze(raw, checked)

        self.assertTrue(result.passed)
        structure = result.structure
        self.assertEqual(structure.squeeze_bars, 8)
        self.assertEqual(structure.breakout_level, Decimal("100.74"))
        self.assertEqual(structure.atr_reference, Decimal("0.30"))
        self.assertGreater(structure.ema20, structure.ema50)
        self.assertEqual(structure.breakout.index, 60)
        self.assertEqual(structure.retest.index, 61)
        self.assertEqual(structure.entry.index, 62)
        self.assertEqual(structure.breakout_volume_multiple, Decimal("1.8"))
        self.assertEqual(structure.breakout_taker_buy_ratio, Decimal("0.55"))
        self.assertEqual(structure.retest_lower_price, Decimal("100.650"))
        self.assertEqual(structure.retest_upper_price, Decimal("100.830"))
        self.assertEqual(structure.entry_upper_price, Decimal("101.100"))
        self.assertTrue(structure.structure_id)

    def test_at_least_eight_squeeze_bars_and_closed_containment_boundary(self):
        raw, checked = n11_klines()
        nine = self.analyze(raw, checked, squeeze_bars=9)
        self.assertTrue(nine.passed)
        self.assertEqual(nine.structure.squeeze_bars, 9)

        candles = parse_n11_klines(
            [
                kline(
                    index,
                    "98.5" if index % 2 == 0 else "101.5",
                    "101.6",
                    "98.4",
                    "98.5" if index % 2 == 0 else "101.5",
                )
                for index in range(20)
            ]
        )
        indicators = _IndicatorSeries(
            ema20=[None] * 19 + [Decimal("100")],
            ema50=[None] * 20,
            atr14=[None] * 19 + [Decimal("2")],
        )
        self.assertTrue(
            _is_squeeze(
                candles,
                indicators,
                19,
                Decimal("2"),
                Decimal("1.5"),
            )
        )

    def test_ema20_must_be_strictly_above_ema50(self):
        raw, checked = n11_klines()
        for index in range(60):
            close = Decimal("100") - Decimal(index) * Decimal("0.01")
            raw[index][1:5] = [
                str(close + Decimal("0.02")),
                str(close + Decimal("0.15")),
                str(close - Decimal("0.15")),
                str(close),
            ]

        result = self.analyze(raw, checked)

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N11_TREND_FILTER_NOT_MET")

    def test_breakout_volume_and_taker_buy_closed_boundaries(self):
        exact, checked = n11_klines()
        self.assertTrue(self.analyze(exact, checked).passed)

        low_volume, low_volume_checked = n11_klines(
            breakout_volume=Decimal("179.999"),
            breakout_taker_buy=Decimal("98"),
        )
        self.assertEqual(
            self.analyze(low_volume, low_volume_checked).reason,
            "N11_BREAKOUT_VOLUME_TOO_LOW",
        )

        low_taker, low_taker_checked = n11_klines(
            breakout_taker_buy=Decimal("98.999"),
        )
        self.assertEqual(
            self.analyze(low_taker, low_taker_checked).reason,
            "N11_BREAKOUT_TAKER_BUY_RATIO_TOO_LOW",
        )

    def test_breakout_close_and_body_filters(self):
        raw, checked = n11_klines()
        no_break = deepcopy(raw)
        no_break[60][4] = "100.74"
        no_break[60][1] = "100.60"
        small_body = deepcopy(raw)
        small_body[60][1] = "101.50"
        low_close_location = deepcopy(raw)
        low_close_location[60][2] = "102.10"

        self.assertEqual(
            self.analyze(no_break, checked).reason,
            "N11_STRUCTURE_NOT_FOUND",
        )
        self.assertEqual(
            self.analyze(small_body, checked).reason,
            "N11_BREAKOUT_BODY_TOO_SMALL",
        )
        self.assertEqual(
            self.analyze(low_close_location, checked).reason,
            "N11_BREAKOUT_CLOSE_LOCATION_TOO_LOW",
        )

    def test_retest_six_bar_boundary_and_seventh_is_missed(self):
        six, checked = n11_klines(retest_delay=6)
        seven, seven_checked = n11_klines(retest_delay=7)

        self.assertTrue(self.analyze(six, checked).passed)
        missed = self.analyze(seven, seven_checked)
        self.assertFalse(missed.passed)
        self.assertIn(
            "N11_RETEST_WINDOW_EXPIRED",
            [event.reason for event in missed.historical_events],
        )

    def test_first_retest_zone_hold_location_and_volume(self):
        raw, checked = n11_klines()
        deep = deepcopy(raw)
        deep[61][3] = "100.649"
        not_held = deepcopy(raw)
        not_held[61][1] = "100.70"
        not_held[61][3] = "100.65"
        not_held[61][4] = "100.739"
        low_location = deepcopy(raw)
        low_location[61][2] = "101.30"
        high_volume = deepcopy(raw)
        high_volume[61][7] = "144.001"
        high_volume[61][10] = "72"

        self.assertEqual(self.analyze(deep, checked).reason, "N11_RETEST_TOO_DEEP")
        self.assertEqual(
            self.analyze(not_held, checked).reason,
            "N11_RETEST_NOT_HELD",
        )
        self.assertEqual(
            self.analyze(low_location, checked).reason,
            "N11_RETEST_CLOSE_LOCATION_TOO_LOW",
        )
        self.assertEqual(
            self.analyze(high_volume, checked).reason,
            "N11_RETEST_VOLUME_TOO_HIGH",
        )

    def test_first_touch_is_immutable_even_if_later_retest_would_pass(self):
        raw, checked = n11_klines(retest_delay=2)
        raw[61][3] = "100.70"
        raw[61][4] = "100.73"
        raw[61][1] = "100.75"
        raw[61][2] = "100.90"
        result = self.analyze(raw, checked)

        self.assertFalse(result.passed)
        self.assertEqual(result.historical_events[0].reason, "N11_RETEST_NOT_HELD")
        self.assertEqual(result.historical_events[0].structure.retest.index, 61)

    def test_entry_time_and_price_closed_boundaries(self):
        for elapsed in (0, 119_999):
            raw, checked = n11_klines(elapsed_ms=elapsed)
            with self.subTest(elapsed=elapsed):
                self.assertTrue(self.analyze(raw, checked).passed)

        expired, expired_checked = n11_klines(elapsed_ms=120_000)
        self.assertEqual(
            self.analyze(expired, expired_checked).reason,
            "N11_ENTRY_WINDOW_EXPIRED",
        )

        lower, lower_checked = n11_klines()
        lower[62][1] = "100.74"
        lower[62][2] = "100.80"
        lower[62][3] = "100.70"
        lower[62][4] = "100.74"
        self.assertTrue(self.analyze(lower, lower_checked).passed)

        upper, upper_checked = n11_klines()
        upper[62][1] = "101.05"
        upper[62][2] = "101.10"
        upper[62][3] = "101.00"
        upper[62][4] = "101.10"
        self.assertTrue(self.analyze(upper, upper_checked).passed)

        below = deepcopy(lower)
        below[62][4] = "100.739"
        below[62][3] = "100.70"
        above = deepcopy(upper)
        above[62][2] = "101.101"
        above[62][4] = "101.101"
        self.assertEqual(
            self.analyze(below, lower_checked).reason,
            "N11_ENTRY_PRICE_BELOW_BREAKOUT",
        )
        self.assertEqual(
            self.analyze(above, upper_checked).reason,
            "N11_ENTRY_PRICE_TOO_EXTENDED",
        )

    def test_history_replay_and_structure_identity_are_stable(self):
        raw, checked = n11_klines()
        first = self.analyze(raw, checked)
        shifted = [
            kline(-1, "99.97", "100.14", "99.84", "99.99"),
            *deepcopy(raw),
        ]
        shifted_result = self.analyze(shifted, checked)
        self.assertEqual(first.structure_id, shifted_result.structure_id)

        historical = [
            *deepcopy(raw),
            kline(63, "101", "101.1", "100.9", "101"),
        ]
        replayed = self.analyze(
            historical,
            BASE_TIME_MS + 63 * INTERVAL_MS + 30_000,
        )
        self.assertEqual(
            replayed.historical_events[0].reason,
            "N11_HISTORICAL_ENTRY_MISSED",
        )
        self.assertEqual(
            replayed.historical_events[0].structure.structure_id,
            first.structure_id,
        )

    def test_invalid_data_and_sequence_fail_closed(self):
        raw, checked = n11_klines()
        missing = deepcopy(raw)
        missing[0] = missing[0][:7]
        gap = deepcopy(raw)
        gap[20][0] += 1

        self.assertEqual(
            self.analyze(missing, checked).reason,
            "N11_KLINE_DATA_INVALID",
        )
        self.assertEqual(
            self.analyze(gap, checked).reason,
            "N11_KLINE_SEQUENCE_INVALID",
        )

    def test_invalid_strategy_parameters_are_rejected(self):
        raw, checked = n11_klines()
        for kwargs in (
            {"squeeze_bars": 0},
            {"retest_max_bars": 0},
            {"entry_window_seconds": 0},
            {"breakout_taker_buy_ratio_min": Decimal("1.01")},
            {"retest_volume_ratio_max": Decimal("-0.01")},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.analyze(raw, checked, **kwargs)

    def test_first_breakout_failure_is_not_replaced_inside_same_squeeze(self):
        raw, checked = n11_klines()
        raw[60][7] = "179.999"
        raw[60][10] = "98"
        raw[61][1:5] = ["101.2", "102.4", "101.1", "102.2"]
        raw[61][7] = "300"
        raw[61][10] = "200"

        result = self.analyze(raw, checked)

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N11_BREAKOUT_VOLUME_TOO_LOW")


class N11SchedulerTests(unittest.TestCase):
    def scheduler(self, tmpdir):
        recorder = make_test_recorder(
            str(Path(tmpdir) / "review.sqlite3"),
            logging.getLogger("test_n11"),
        )
        recorder.upsert_strategy_definitions((N11_STRATEGY,))
        return recorder, StrategyScheduler(
            (N11_STRATEGY,),
            96,
            recorder,
            logging.getLogger("test_n11"),
        )

    def test_configuration_and_top_100_candidate_pool(self):
        self.assertEqual(
            next(
                strategy
                for strategy in load_all_strategies()
                if strategy.strategy_id == "N11"
            ),
            N11_STRATEGY,
        )
        self.assertIsNone(N11_STRATEGY.funding_threshold)
        self.assertEqual(N11_STRATEGY.volume_top_n, 100)
        self.assertEqual(N11_STRATEGY.risk_reward_ratio, Decimal("5"))
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            raw, checked = n11_klines()
            items = [
                candidate("POSUSDT", Decimal("0.1"), 1),
                candidate("NEGUSDT", Decimal("-0.1"), 100),
                candidate("OUTUSDT", None, 101),
            ]
            scan = recorder.begin_scan(3, items, dry_run=True)
            result = scheduler.evaluate(
                scan,
                {"quote_volume_top": items},
                {item.symbol: raw for item in items},
                checked_at_ms=checked,
            )
            self.assertEqual(
                [item.candidate.symbol for item in result.signals],
                ["POSUSDT", "NEGUSDT"],
            )
            self.assertTrue(all(item.passed for item in result.signals))
            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT detail_json FROM strategy_signals "
                    "WHERE strategy_id='N11' ORDER BY id LIMIT 1"
                ).fetchone()
            detail = json.loads(row[0])
            self.assertEqual(detail["structure"]["breakout_level"], "100.74")
            self.assertEqual(detail["structure"]["breakout_volume_multiple"], "1.8")

    def test_passed_structure_is_consumed_before_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self.scheduler(tmpdir)
            raw, checked = n11_klines()
            item = candidate()
            scan = recorder.begin_scan(1, [item], dry_run=True)
            first = scheduler.evaluate(
                scan,
                {"quote_volume_top": [item]},
                {item.symbol: raw},
                checked_at_ms=checked,
            )
            restarted = make_test_recorder(str(db), logging.getLogger("test_n11_restart"))
            restarted_scheduler = StrategyScheduler(
                (N11_STRATEGY,),
                96,
                restarted,
                logging.getLogger("test_n11_restart"),
            )
            second_scan = restarted.begin_scan(1, [item], dry_run=True)
            second = restarted_scheduler.evaluate(
                second_scan,
                {"quote_volume_top": [item]},
                {item.symbol: raw},
                checked_at_ms=checked,
            )

            self.assertTrue(first.signals[0].passed)
            self.assertEqual(second.signals[0].reason, "N11_STRUCTURE_CONSUMED")
            with restarted._connect() as connection:
                row = connection.execute(
                    "SELECT status,reason FROM strategy_structure_terminal_states "
                    "WHERE strategy_id='N11'"
                ).fetchone()
            self.assertEqual(tuple(row), ("CONSUMED", "PASSED"))

    def test_historical_backfill_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self.scheduler(tmpdir)
            raw, _ = n11_klines()
            historical = [
                *raw,
                kline(63, "101", "101.1", "100.9", "101"),
            ]
            checked = BASE_TIME_MS + 63 * INTERVAL_MS + 30_000
            item = candidate()
            for attempt in range(2):
                if attempt:
                    recorder = make_test_recorder(
                        str(db), logging.getLogger("test_n11_backfill_restart")
                    )
                    scheduler = StrategyScheduler(
                        (N11_STRATEGY,),
                        96,
                        recorder,
                        logging.getLogger("test_n11_backfill_restart"),
                    )
                scan = recorder.begin_scan(1, [item], dry_run=True)
                scheduler.evaluate(
                    scan,
                    {"quote_volume_top": [item]},
                    {item.symbol: historical},
                    checked_at_ms=checked,
                )
            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT COUNT(*),COUNT(DISTINCT structure_id) "
                    "FROM strategy_structure_terminal_states WHERE strategy_id='N11'"
                ).fetchone()
            self.assertEqual(tuple(row), (1, 1))

    def test_state_read_and_write_failures_reject_safely(self):
        raw, checked = n11_klines()
        item = candidate()
        for failure, expected in (
            ("read", "N11_STATE_READ_FAILED"),
            ("write", "N11_STATE_PERSIST_FAILED"),
        ):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = self.scheduler(tmpdir)
                if failure == "read":
                    recorder.get_strategy_structure_terminal_state = (
                        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("read"))
                    )
                else:
                    recorder.record_strategy_structure_terminal = lambda *args, **kwargs: False
                scan = recorder.begin_scan(1, [item], dry_run=True)
                result = scheduler.evaluate(
                    scan,
                    {"quote_volume_top": [item]},
                    {item.symbol: raw},
                    checked_at_ms=checked,
                )
                self.assertEqual(result.signals[0].reason, expected)

    def test_legacy_signal_lookup_failure_rejects_safely(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            raw, checked = n11_klines()
            item = candidate()
            recorder.inspect_passed_structure = (
                lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("lookup"))
            )
            scan = recorder.begin_scan(1, [item], dry_run=True)

            result = scheduler.evaluate(
                scan,
                {"quote_volume_top": [item]},
                {item.symbol: raw},
                checked_at_ms=checked,
            )

            self.assertEqual(result.signals[0].reason, "N11_STATE_READ_FAILED")


class N11MainTests(unittest.TestCase):
    def test_main_plan_uses_retest_low_entry_range_and_deadline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_n11_main"),
            )
            scheduler = StrategyScheduler(
                (N11_STRATEGY,),
                96,
                recorder,
                logging.getLogger("test_n11_main"),
            )
            raw, checked = n11_klines(elapsed_ms=119_999)
            item = candidate()
            scan = recorder.begin_scan(1, [item], dry_run=True)
            evaluated = scheduler.evaluate(
                scan,
                {"quote_volume_top": [item]},
                {item.symbol: raw},
                checked_at_ms=checked,
            )
            signal = evaluated.passed_signals[0]
            base_plan = TradePlan(
                symbol=item.symbol,
                leverage=20,
                quantity=Decimal("1"),
                entry_price=signal.analysis.structure.entry.close,
                stop_loss_price=Decimal("99.73"),
                take_profit_price=Decimal("106.35"),
                stop_loss_pct=Decimal("0.01"),
                take_profit_pct=Decimal("0.05"),
                amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("0"),
                low_24h_price=Decimal("0"),
                risk_amount=Decimal("1"),
                notional_value=Decimal("100"),
                required_margin=Decimal("5"),
                balance=Decimal("1000"),
                stop_mode="breakout_retest_margin_capped",
                structure_id=signal.analysis.structure_id,
            )

            class FakeTrader:
                def __init__(self):
                    self.args = None

                def build_breakout_retest_margin_capped_trade_plan(
                    self,
                    *args,
                    **kwargs,
                ):
                    self.args = (args, kwargs)
                    return replace(
                        base_plan,
                        entry_min_price=kwargs["entry_min_price"],
                        entry_max_price=kwargs["entry_max_price"],
                    )

            bot = TradingBot.__new__(TradingBot)
            bot.trader = FakeTrader()
            plan = bot._build_strategy_plan(signal)

            structure = signal.analysis.structure
            self.assertEqual(bot.trader.args[0][1], structure.entry.close)
            self.assertEqual(bot.trader.args[0][2], structure.retest.low)
            self.assertEqual(
                bot.trader.args[1]["entry_min_price"],
                structure.breakout_level,
            )
            self.assertEqual(
                bot.trader.args[1]["entry_max_price"],
                structure.entry_upper_price,
            )
            self.assertEqual(plan.entry_candle_open_time_ms, structure.entry.open_time_ms)
            self.assertEqual(
                plan.entry_deadline_ms,
                structure.entry.open_time_ms + 120_000,
            )

            paper = PaperTrader(
                recorder,
                logging.getLogger("test_n11_paper_deadline"),
                clock_ms=lambda: plan.entry_deadline_ms,
            )
            with self.assertRaises(EntryWindowExpiredError):
                paper.open_trade("N11", item.symbol, None, plan, {})
            self.assertIsNone(recorder.get_open_strategy_paper_trade("N11"))


class N11TraderTests(unittest.TestCase):
    def trader(self, client, tmpdir, *, live=False):
        config = (
            live_test_config(f"{tmpdir}/account.json")
            if live
            else test_config(f"{tmpdir}/account.json")
        )
        return Trader(
            client,
            config,
            StateStore(f"{tmpdir}/state.json"),
            logging.getLogger("test_n11_trader"),
        )

    def test_stop_distance_boundaries_five_r_and_low_leverage_scaling(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = self.trader(RuleConstrainedClient(leverage=50), tmpdir)
            expanded = trader.build_breakout_retest_margin_capped_trade_plan(
                "N11USDT", Decimal("100"), Decimal("99.50"), Decimal("5"),
                entry_min_price=Decimal("100"), entry_max_price=Decimal("101.5"),
            )
            exact_five = trader.build_breakout_retest_margin_capped_trade_plan(
                "N11USDT", Decimal("100"), Decimal("95.01"), Decimal("5"),
                entry_min_price=Decimal("100"), entry_max_price=Decimal("101.5"),
            )
            self.assertEqual(expanded.stop_loss_pct, Decimal("0.01"))
            self.assertEqual(expanded.stop_loss_price, Decimal("99.00"))
            self.assertEqual(exact_five.stop_loss_pct, Decimal("0.05"))
            self.assertGreaterEqual(
                (exact_five.take_profit_price - exact_five.entry_price)
                / (exact_five.entry_price - exact_five.stop_loss_price),
                Decimal("5"),
            )
            with self.assertRaisesRegex(BinanceAPIError, "N11_STOP_PCT_OUT_OF_RANGE"):
                trader.build_breakout_retest_margin_capped_trade_plan(
                    "N11USDT", Decimal("100"), Decimal("95.00"), Decimal("5"),
                    entry_min_price=Decimal("100"), entry_max_price=Decimal("101.5"),
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            low_leverage = self.trader(RuleConstrainedClient(leverage=1), tmpdir)
            plan = low_leverage.build_breakout_retest_margin_capped_trade_plan(
                "N11USDT", Decimal("100"), Decimal("99.50"), Decimal("5"),
                entry_min_price=Decimal("100"), entry_max_price=Decimal("101.5"),
            )
            self.assertEqual(plan.quantity, Decimal("9.5"))
            self.assertTrue(plan.risk_capped_by_margin)
            self.assertLess(plan.actual_risk_amount, plan.target_risk_amount)

        with tempfile.TemporaryDirectory() as tmpdir:
            minimums = self.trader(
                RuleConstrainedClient(leverage=1, min_qty="10"), tmpdir
            )
            with self.assertRaisesRegex(BinanceAPIError, "below minQty"):
                minimums.build_breakout_retest_margin_capped_trade_plan(
                    "N11USDT", Decimal("100"), Decimal("99.50"), Decimal("5"),
                    entry_min_price=Decimal("100"), entry_max_price=Decimal("101.5"),
                )

    def test_live_adverse_fill_reduces_and_rebuilds_protection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient({}, leverage=50)
            trader = self.trader(client, tmpdir, live=True)
            plan = trader.build_breakout_retest_margin_capped_trade_plan(
                "N11USDT", Decimal("100"), Decimal("99"), Decimal("5"),
                "n11-reduce", entry_min_price=Decimal("100"),
                entry_max_price=Decimal("101.5"),
            )
            client.open_response = {
                "orderId": 1101, "avgPrice": "101", "executedQty": str(plan.quantity),
            }
            client.position_quantities = [str(plan.quantity), "99.502"]
            excess = plan.quantity - Decimal("99.502")
            client.close_side_effects = [
                {"status": "FILLED", "executedQty": str(excess), "orderId": 1102}
            ]

            state = trader.open_long_plan_with_protection(plan)

            entry = Decimal(state.entry_price)
            stop = Decimal(state.stop_loss_price)
            take_profit = Decimal(state.take_profit_price)
            self.assertEqual(state.quantity, "99.502")
            self.assertEqual(stop, Decimal("98.99"))
            self.assertGreaterEqual((take_profit - entry) / (entry - stop), Decimal("5"))
            self.assertEqual(client.close_calls, [("N11USDT", excess)])
            self.assertEqual(state.orders["post_fill_adjustment"]["strategy"], "N11")
            self.assertLessEqual(
                Decimal(state.orders["plan"]["post_fill_actual_risk_amount"]),
                Decimal("200"),
            )

    def test_live_fill_range_boundaries_and_outside_cleanup(self):
        entry_min = Decimal("100")
        entry_max = Decimal("101.5")
        allowed_min = entry_min * Decimal("0.995")
        allowed_max = entry_max * Decimal("1.005")
        for actual_entry in (entry_min, entry_max, allowed_min, allowed_max):
            with self.subTest(actual_entry=actual_entry), tempfile.TemporaryDirectory() as tmpdir:
                client = FakeLiveExecutionClient({}, leverage=50)
                trader = self.trader(client, tmpdir, live=True)
                plan = trader.build_breakout_retest_margin_capped_trade_plan(
                    "N11USDT", Decimal("100"), Decimal("99"), Decimal("5"),
                    entry_min_price=entry_min, entry_max_price=entry_max,
                )
                client.open_response = {
                    "orderId": 1111, "avgPrice": str(actual_entry),
                    "executedQty": str(plan.quantity),
                }
                safe_quantity = trader._margin_capped_safe_quantity_after_fill(
                    plan, actual_entry, plan.quantity
                )
                client.position_quantities = [str(plan.quantity)]
                if safe_quantity < plan.quantity:
                    client.position_quantities.append(str(safe_quantity))
                    client.close_side_effects = [{
                        "status": "FILLED",
                        "executedQty": str(plan.quantity - safe_quantity),
                    }]
                state = trader.open_long_plan_with_protection(plan)
                self.assertEqual(Decimal(state.entry_price), actual_entry)
                self.assertEqual(len(client.protection_calls), 2)

        for actual_entry in (
            allowed_min - Decimal("0.0001"),
            allowed_max + Decimal("0.0001"),
        ):
            with self.subTest(actual_entry=actual_entry), tempfile.TemporaryDirectory() as tmpdir:
                client = FakeLiveExecutionClient({}, leverage=50)
                state_store = StateStore(f"{tmpdir}/state.json")
                trader = Trader(
                    client,
                    live_test_config(f"{tmpdir}/account.json"),
                    state_store,
                    logging.getLogger("test_n11_outside_fill"),
                )
                plan = trader.build_breakout_retest_margin_capped_trade_plan(
                    "N11USDT", Decimal("100"), Decimal("99"), Decimal("5"),
                    entry_min_price=entry_min, entry_max_price=entry_max,
                )
                client.open_response = {
                    "orderId": 1121, "avgPrice": str(actual_entry),
                    "executedQty": str(plan.quantity),
                }
                client.position_quantities = [str(plan.quantity), "0"]
                client.close_side_effects = [{
                    "status": "FILLED", "executedQty": str(plan.quantity),
                }]
                with self.assertRaisesRegex(
                    BinanceAPIError, "N11_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE"
                ):
                    trader.open_long_plan_with_protection(plan)
                self.assertEqual(client.protection_calls, [])
                stored = state_store.load()
                self.assertIsNotNone(stored)
                self.assertEqual(stored.quantity, "0")
                self.assertEqual(
                    stored.orders["strategy"]["strategy_id"], "N11"
                )
                self.assertTrue(
                    stored.orders["execution_cleanup_resolved"][
                        "emergency_cleanup"
                    ]["confirmed_closed"]
                )

    def test_live_protection_failure_emergency_cleans_without_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {}, leverage=50,
                protection_side_effects=[BinanceAPIError("N11 stop unavailable")],
            )
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/account.json"),
                state_store,
                logging.getLogger("test_n11_protection_failure"),
            )
            plan = trader.build_breakout_retest_margin_capped_trade_plan(
                "N11USDT", Decimal("100"), Decimal("99"), Decimal("5"),
                entry_min_price=Decimal("100"), entry_max_price=Decimal("101.5"),
            )
            client.open_response = {
                "orderId": 1131, "avgPrice": "100", "executedQty": str(plan.quantity),
            }
            client.position_quantities = [str(plan.quantity), "0"]
            client.close_side_effects = [{
                "status": "FILLED", "executedQty": str(plan.quantity),
            }]

            with self.assertRaisesRegex(BinanceAPIError, "N11 stop unavailable"):
                trader.open_long_plan_with_protection(plan)

            self.assertEqual(len(client.close_calls), 1)
            self.assertEqual(len(client.protection_calls), 1)
            stored = state_store.load()
            self.assertIsNotNone(stored)
            self.assertIn("emergency_cleanup_pending", stored.orders)
            self.assertTrue(trader.sync_state_with_exchange().pending_resolved)
            self.assertIsNotNone(state_store.load())


if __name__ == "__main__":
    unittest.main()
