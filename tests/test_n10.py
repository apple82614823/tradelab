from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import json
import logging
from pathlib import Path
import tempfile
import unittest

from tests.recorder_test_utils import make_test_recorder
from trading_bot.monitor import FundingCandidate
from trading_bot.main import TradingBot
from trading_bot.n10_analyzer import (
    analyze_n10_volume_liquidity_sweep_reclaim,
    decimal_median,
)
from trading_bot.recorder import ReviewRecorder
from trading_bot.paper_trader import PaperTrader
from trading_bot.strategies import N10_STRATEGY, load_all_strategies
from trading_bot.strategy_scheduler import StrategyScheduler
from trading_bot.trader import EntryWindowExpiredError, TradePlan


BASE_TIME_MS = 1_720_000_000_000
INTERVAL_MS = 15 * 60 * 1000


def kline(index, open_price, high, low, close, quote_volume, taker_buy_quote):
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


def n10_klines(
    start_index=0,
    price_offset=Decimal("0"),
    sweep_depth=Decimal("0.01"),
    wick_ratio=Decimal("0.50"),
    elapsed_ms=30_000,
):
    rows = []
    support = Decimal("100") + price_offset
    for offset in range(48):
        index = start_index + offset
        low = support + Decimal("2")
        if offset == 5:
            low = support
        elif offset == 9:
            low = support * Decimal("1.005")
        quote_volume = Decimal("100")
        if offset >= 28:
            quote_volume = Decimal("90") if offset < 38 else Decimal("110")
        rows.append(
            kline(
                index,
                support + Decimal("3"),
                support + Decimal("4"),
                low,
                support + Decimal("3"),
                quote_volume,
                quote_volume * Decimal("0.5"),
            )
        )

    w_low = support * (Decimal("1") - sweep_depth)
    w_high = support + Decimal("2")
    w_close = w_low + wick_ratio * (w_high - w_low)
    w_open = w_close + Decimal("0.2")
    rows.append(
        kline(start_index + 48, w_open, w_high, w_low, w_close, "250", "120")
    )
    rows.append(
        kline(
            start_index + 49,
            support + Decimal("0.5"),
            support + Decimal("3"),
            w_low,
            support + Decimal("2.5"),
            "200",
            "110",
        )
    )
    rows.append(
        kline(
            start_index + 50,
            support + Decimal("2.2"),
            support + Decimal("2.5"),
            w_low,
            w_high,
            "100",
            "50",
        )
    )
    checked_at_ms = BASE_TIME_MS + (start_index + 50) * INTERVAL_MS + elapsed_ms
    return rows, checked_at_ms


def candidate(symbol="N10USDT", funding_rate=None, rank=1):
    return FundingCandidate(
        symbol=symbol,
        funding_rate=funding_rate,
        mark_price=Decimal("999"),
        quote_volume=Decimal("1000000"),
        quote_volume_rank=rank,
        candidate_universe="quote_volume_top",
    )


class N10AnalyzerTests(unittest.TestCase):
    def analyze(self, raw, checked):
        return analyze_n10_volume_liquidity_sweep_reclaim(
            "N10USDT", raw, checked_at_ms=checked
        )

    def test_complete_sample_records_all_structure_metrics(self):
        raw, checked = n10_klines()
        result = self.analyze(raw, checked)

        self.assertTrue(result.passed)
        structure = result.structure
        self.assertEqual(structure.support_price, Decimal("100"))
        self.assertEqual(structure.support_touch_gap, 4)
        self.assertEqual(structure.volume_median_20, Decimal("100"))
        self.assertEqual(structure.volume_spike_multiple, Decimal("2.5"))
        self.assertEqual(structure.lower_wick_ratio, Decimal("0.50"))
        self.assertEqual(structure.taker_buy_ratio, Decimal("0.55"))
        self.assertEqual(structure.w.index, 48)
        self.assertEqual(structure.c.index, 49)
        self.assertEqual(structure.e.index, 50)
        self.assertTrue(structure.structure_id)

    def test_support_48_and_47_bar_boundary(self):
        raw, checked = n10_klines()
        passed = self.analyze(raw, checked)
        rejected = self.analyze(raw[1:], checked)
        self.assertTrue(passed.passed)
        self.assertEqual(rejected.reason, "N10_NOT_ENOUGH_SUPPORT_HISTORY")

    def test_support_retest_tolerance_and_gap_boundaries(self):
        boundary, checked = n10_klines()
        over = deepcopy(boundary)
        over[9][3] = "100.5001"
        gap_three = deepcopy(boundary)
        gap_three[9][3] = "102"
        gap_three[8][3] = "100.5"

        self.assertTrue(self.analyze(boundary, checked).passed)
        self.assertEqual(self.analyze(over, checked).reason, "N10_SUPPORT_NOT_RETESTED")
        self.assertEqual(
            self.analyze(gap_three, checked).reason,
            "N10_SUPPORT_NOT_RETESTED",
        )

    def test_sweep_depth_closed_boundaries_and_outside(self):
        for depth in (Decimal("0.003"), Decimal("0.015")):
            raw, checked = n10_klines(sweep_depth=depth)
            with self.subTest(depth=depth):
                self.assertTrue(self.analyze(raw, checked).passed)
        for depth in (Decimal("0.002999"), Decimal("0.015001")):
            raw, checked = n10_klines(sweep_depth=depth)
            with self.subTest(depth=depth):
                self.assertEqual(
                    self.analyze(raw, checked).reason,
                    "N10_SWEEP_DEPTH_OUT_OF_RANGE",
                )

    def test_sweep_close_must_strictly_reclaim_support(self):
        raw, checked = n10_klines()
        raw[48][4] = "100"
        self.assertEqual(self.analyze(raw, checked).reason, "N10_SWEEP_NOT_RECLAIMED")

    def test_exact_volume_spike_and_even_median(self):
        raw, checked = n10_klines()
        self.assertEqual(
            decimal_median([Decimal("90")] * 10 + [Decimal("110")] * 10),
            Decimal("100"),
        )
        self.assertTrue(self.analyze(raw, checked).passed)
        below = deepcopy(raw)
        below[48][7] = "249.999"
        self.assertEqual(
            self.analyze(below, checked).reason,
            "N10_VOLUME_SPIKE_NOT_CONFIRMED",
        )

    def test_lower_wick_fifty_percent_boundary(self):
        exact, checked = n10_klines(wick_ratio=Decimal("0.50"))
        below, below_checked = n10_klines(wick_ratio=Decimal("0.4999"))
        self.assertTrue(self.analyze(exact, checked).passed)
        self.assertEqual(
            self.analyze(below, below_checked).reason,
            "N10_LOWER_WICK_TOO_SMALL",
        )

    def test_confirmation_high_and_low_boundaries(self):
        raw, checked = n10_klines()
        equal_high = deepcopy(raw)
        equal_high[49][4] = equal_high[48][2]
        below_low = deepcopy(raw)
        below_low[49][3] = "98.999"
        self.assertTrue(self.analyze(raw, checked).passed)
        self.assertEqual(
            self.analyze(equal_high, checked).reason,
            "N10_CONFIRMATION_NOT_BROKEN_HIGH",
        )
        self.assertEqual(
            self.analyze(below_low, checked).reason,
            "N10_STRUCTURE_LOW_BROKEN",
        )

    def test_taker_buy_ratio_boundary_below_and_invalid(self):
        raw, checked = n10_klines()
        self.assertTrue(self.analyze(raw, checked).passed)
        below = deepcopy(raw)
        below[49][10] = "109.999"
        negative = deepcopy(raw)
        negative[49][10] = "-1"
        excess = deepcopy(raw)
        excess[49][10] = "201"
        self.assertEqual(
            self.analyze(below, checked).reason,
            "N10_TAKER_BUY_RATIO_TOO_LOW",
        )
        self.assertEqual(self.analyze(negative, checked).reason, "N10_KLINE_DATA_INVALID")
        self.assertEqual(self.analyze(excess, checked).reason, "N10_KLINE_DATA_INVALID")

    def test_kline_fields_and_sequence_fail_closed(self):
        raw, checked = n10_klines()
        missing = deepcopy(raw)
        missing[0] = missing[0][:7]
        gap = deepcopy(raw)
        gap[20][0] += 1
        self.assertEqual(self.analyze(missing, checked).reason, "N10_KLINE_DATA_INVALID")
        self.assertEqual(self.analyze(gap, checked).reason, "N10_KLINE_SEQUENCE_INVALID")

    def test_entry_price_low_and_time_boundaries(self):
        for elapsed in (0, 119_999):
            raw, checked = n10_klines(elapsed_ms=elapsed)
            with self.subTest(elapsed=elapsed):
                self.assertTrue(self.analyze(raw, checked).passed)

        upper, upper_checked = n10_klines()
        upper[50][4] = str(Decimal(upper[48][2]) * Decimal("1.015"))
        upper[50][2] = upper[50][4]
        self.assertTrue(self.analyze(upper, upper_checked).passed)

        expired, expired_checked = n10_klines(elapsed_ms=120_000)
        below, below_checked = n10_klines()
        below[50][4] = "101.999"
        extended, extended_checked = n10_klines()
        extended[50][4] = "103.531"
        extended[50][2] = "103.531"
        self.assertEqual(
            self.analyze(expired, expired_checked).reason,
            "N10_ENTRY_WINDOW_EXPIRED",
        )
        self.assertEqual(
            self.analyze(below, below_checked).reason,
            "N10_ENTRY_PRICE_BELOW_RECLAIM",
        )
        self.assertEqual(
            self.analyze(extended, extended_checked).reason,
            "N10_ENTRY_PRICE_TOO_EXTENDED",
        )

    def test_entry_low_equal_passes_and_lower_rejects(self):
        raw, checked = n10_klines()
        lower = deepcopy(raw)
        lower[50][3] = "98.999"
        self.assertTrue(self.analyze(raw, checked).passed)
        self.assertEqual(
            self.analyze(lower, checked).reason,
            "N10_STRUCTURE_LOW_BROKEN",
        )

    def test_structure_identity_stable_on_window_shift_and_changes_with_w(self):
        raw, checked = n10_klines()
        first = self.analyze(raw, checked)
        shifted = deepcopy(raw)
        prefix = kline(-1, "105", "106", "104", "105", "100", "50")
        shifted = [prefix, *shifted]
        shifted_result = self.analyze(shifted, checked)
        changed = deepcopy(raw)
        changed[48][0] += INTERVAL_MS
        changed[48][6] += INTERVAL_MS

        self.assertEqual(first.structure_id, shifted_result.structure_id)
        self.assertEqual(
            self.analyze(changed, checked).reason,
            "N10_KLINE_SEQUENCE_INVALID",
        )


class N10SchedulerTests(unittest.TestCase):
    def scheduler(self, tmpdir):
        recorder = make_test_recorder(
            str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_n10")
        )
        recorder.upsert_strategy_definitions((N10_STRATEGY,))
        return recorder, StrategyScheduler(
            (N10_STRATEGY,), 96, recorder, logging.getLogger("test_n10")
        )

    def test_configuration_funding_and_rank(self):
        self.assertEqual(
            next(
                strategy
                for strategy in load_all_strategies()
                if strategy.strategy_id == "N10"
            ),
            N10_STRATEGY,
        )
        self.assertIsNone(N10_STRATEGY.funding_threshold)
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            raw, checked = n10_klines()
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
            self.assertEqual([item.candidate.symbol for item in result.signals], ["POSUSDT", "NEGUSDT"])
            self.assertTrue(all(item.passed for item in result.signals))
            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT funding_rate, detail_json FROM strategy_signals "
                    "WHERE strategy_id = 'N10' ORDER BY id LIMIT 1"
                ).fetchone()
            detail = json.loads(row[1])
            self.assertEqual(row[0], "0.1")
            self.assertEqual(detail["structure"]["support_price"], "100")
            self.assertEqual(detail["structure"]["volume_median_20"], "100")
            self.assertEqual(detail["structure"]["volume_spike_multiple"], "2.5")

    def test_price_miss_consumes_structure_and_restart_cannot_revive_it(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self.scheduler(tmpdir)
            raw, checked = n10_klines()
            raw[50][4] = "101.9"
            item = candidate()
            scan = recorder.begin_scan(1, [item], dry_run=True)
            first = scheduler.evaluate(
                scan,
                {"quote_volume_top": [item]},
                {item.symbol: raw},
                checked_at_ms=checked,
            )
            recovered = deepcopy(raw)
            recovered[50][4] = "102"
            restarted = make_test_recorder(str(db), logging.getLogger("test_n10_restart"))
            second_scheduler = StrategyScheduler(
                (N10_STRATEGY,), 96, restarted, logging.getLogger("test_n10_restart")
            )
            second_scan = restarted.begin_scan(1, [item], dry_run=True)
            second = second_scheduler.evaluate(
                second_scan,
                {"quote_volume_top": [item]},
                {item.symbol: recovered},
                checked_at_ms=checked,
            )
            self.assertEqual(first.signals[0].reason, "N10_ENTRY_PRICE_BELOW_RECLAIM")
            self.assertEqual(second.signals[0].reason, "N10_STRUCTURE_CONSUMED")

    def test_offline_old_event_is_idempotent_and_new_event_passes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            old, _ = n10_klines(start_index=0)
            new, checked = n10_klines(start_index=51, price_offset=Decimal("10"))
            raw = [*old, *new]
            item = candidate()
            scan = recorder.begin_scan(1, [item], dry_run=True)
            first = scheduler.evaluate(
                scan,
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
            self.assertTrue(first.signals[0].passed)
            self.assertEqual(first.signals[0].analysis.structure.w.index, 99)
            self.assertEqual(second.signals[0].reason, "N10_STRUCTURE_CONSUMED")
            with recorder._connect() as connection:
                rows = connection.execute(
                    "SELECT COUNT(*), COUNT(DISTINCT structure_id) FROM n10_structure_states"
                ).fetchone()
            self.assertEqual(tuple(rows), (2, 2))

    def test_state_read_and_write_failures_reject_safely(self):
        raw, checked = n10_klines()
        item = candidate()
        for failure, expected in (("read", "N10_STATE_READ_FAILED"), ("write", "N10_STATE_PERSIST_FAILED")):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = self.scheduler(tmpdir)
                if failure == "read":
                    def fail_read(*args, **kwargs):
                        raise RuntimeError("read failed")
                    recorder.get_n10_structure_state = fail_read
                else:
                    recorder.record_n10_structure_consumed = lambda *args, **kwargs: False
                scan = recorder.begin_scan(1, [item], dry_run=True)
                result = scheduler.evaluate(
                    scan,
                    {"quote_volume_top": [item]},
                    {item.symbol: raw},
                    checked_at_ms=checked,
                )
                self.assertEqual(result.signals[0].reason, expected)


class N10MainTests(unittest.TestCase):
    def test_main_plan_uses_entry_close_sweep_low_and_absolute_deadline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_n10_main")
            )
            scheduler = StrategyScheduler(
                (N10_STRATEGY,), 96, recorder, logging.getLogger("test_n10_main")
            )
            raw, checked = n10_klines(elapsed_ms=119_999)
            item = candidate()
            scan = recorder.begin_scan(1, [item], dry_run=True)
            evaluated = scheduler.evaluate(
                scan,
                {"quote_volume_top": [item]},
                {item.symbol: raw},
                checked_at_ms=checked,
            )
            signal = evaluated.passed_signals[0]
            signal = replace(
                signal,
                candidate=replace(signal.candidate, mark_price=Decimal("999")),
            )
            base_plan = TradePlan(
                symbol=item.symbol,
                leverage=10,
                quantity=Decimal("1"),
                entry_price=Decimal("102"),
                stop_loss_price=Decimal("98.99"),
                take_profit_price=Decimal("117.05"),
                stop_loss_pct=Decimal("3.01") / Decimal("102"),
                take_profit_pct=Decimal("15.05") / Decimal("102"),
                amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("0"),
                low_24h_price=Decimal("0"),
                risk_amount=Decimal("3.01"),
                notional_value=Decimal("102"),
                required_margin=Decimal("10.2"),
                balance=Decimal("1000"),
                stop_mode="sweep_low_tick_margin_capped",
                structure_id=signal.analysis.structure_id,
            )

            class FakeTrader:
                def __init__(self):
                    self.args = None

                def build_sweep_low_margin_capped_trade_plan(self, *args, **kwargs):
                    self.args = (args, kwargs)
                    return replace(
                        base_plan,
                        entry_min_price=kwargs["entry_min_price"],
                        entry_max_price=kwargs["entry_max_price"],
                    )

            bot = TradingBot.__new__(TradingBot)
            bot.trader = FakeTrader()
            plan = bot._build_strategy_plan(signal)

            self.assertEqual(bot.trader.args[0][1], Decimal("102"))
            self.assertEqual(bot.trader.args[0][2], Decimal("99.00"))
            self.assertEqual(
                bot.trader.args[1]["entry_min_price"],
                signal.analysis.structure.w.high,
            )
            self.assertEqual(
                bot.trader.args[1]["entry_max_price"],
                signal.analysis.structure.w.high * Decimal("1.015"),
            )
            self.assertEqual(
                plan.entry_candle_open_time_ms,
                signal.analysis.structure.e.open_time_ms,
            )
            self.assertEqual(
                plan.entry_deadline_ms,
                signal.analysis.structure.e.open_time_ms + 120_000,
            )
            self.assertEqual(plan.entry_min_price, Decimal("102"))
            self.assertEqual(plan.entry_max_price, Decimal("103.530"))

            paper = PaperTrader(
                recorder,
                logging.getLogger("test_n10_paper_deadline"),
                clock_ms=lambda: plan.entry_deadline_ms,
            )
            with self.assertRaises(EntryWindowExpiredError):
                paper.open_trade("N10", item.symbol, None, plan, {})
            self.assertIsNone(recorder.get_open_strategy_paper_trade("N10"))

    def test_paper_trade_keeps_n10_entry_range_without_fill_semantics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_n10_paper_audit"),
            )
            plan = TradePlan(
                symbol="N10USDT",
                leverage=10,
                quantity=Decimal("1"),
                entry_price=Decimal("102"),
                stop_loss_price=Decimal("98.99"),
                take_profit_price=Decimal("117.05"),
                stop_loss_pct=Decimal("3.01") / Decimal("102"),
                take_profit_pct=Decimal("15.05") / Decimal("102"),
                amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("0"),
                low_24h_price=Decimal("0"),
                risk_amount=Decimal("3.01"),
                notional_value=Decimal("102"),
                required_margin=Decimal("10.2"),
                balance=Decimal("1000"),
                stop_mode="sweep_low_tick_margin_capped",
                entry_min_price=Decimal("102"),
                entry_max_price=Decimal("103.53"),
            )
            paper = PaperTrader(
                recorder,
                logging.getLogger("test_n10_paper_audit"),
                clock_ms=lambda: BASE_TIME_MS,
            )

            trade_id = paper.open_trade("N10", "N10USDT", None, plan, {})

            self.assertIsNotNone(trade_id)
            trade = recorder.get_open_strategy_paper_trade("N10")
            payload = json.loads(trade.orders_json)["plan"]
            self.assertEqual(payload["entry_min"], "102")
            self.assertEqual(payload["entry_max"], "103.53")
            self.assertIsNone(payload["actual_entry"])


if __name__ == "__main__":
    unittest.main()
