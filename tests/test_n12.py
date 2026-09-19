from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
import json
import logging
from pathlib import Path
from dataclasses import replace
import tempfile
import unittest
from unittest.mock import patch

from tests.recorder_test_utils import (
    commit_test_schema_change,
    make_test_recorder,
)
from trading_bot.monitor import FundingCandidate
from trading_bot.main import TradingBot
from trading_bot.binance_client import BinanceAPIError
from trading_bot.n12_analyzer import (
    RelativeStrength,
    analyze_n12_relative_strength_first_pullback,
    calculate_relative_strength_return,
    evaluate_n12_up_leg,
    parse_n12_klines,
)
from trading_bot.recorder import ReviewRecorder
from trading_bot.paper_trader import PaperTrader
from trading_bot.state import StateStore
from trading_bot.strategies import N12_STRATEGY
from trading_bot.strategy_scheduler import StrategyScheduler
import trading_bot.strategy_scheduler as scheduler_module
from trading_bot.trader import EntryWindowExpiredError, TradePlan, Trader
from tests.test_trader import (
    FakeLiveExecutionClient,
    RuleConstrainedClient,
    live_test_config,
    test_config,
)


BASE_TIME_MS = 1_730_000_000_000
INTERVAL_MS = 900_000


def kline(index, open_price, high, low, close, volume="100", taker="55"):
    open_time = BASE_TIME_MS + index * INTERVAL_MS
    return [
        open_time, str(open_price), str(high), str(low), str(close), "1",
        open_time + INTERVAL_MS - 1, str(volume), "10", "1", str(taker), "0",
    ]


def n12_klines(*, elapsed_ms=30_000, price_offset=Decimal("0")):
    rows = []
    for index in range(90):
        close = Decimal("100.40") + Decimal(index) * Decimal("0.001") + price_offset
        rows.append(kline(
            index, close - Decimal("0.05"), close + Decimal("0.35"),
            close - Decimal("0.35"), close,
        ))
    rows[88][3] = str(Decimal("100.20") + price_offset)
    rows[89][3] = str(Decimal("100.15") + price_offset)
    rows.extend([
        kline(90, Decimal("100.4") + price_offset, Decimal("100.8") + price_offset,
              Decimal("100") + price_offset, Decimal("100.5") + price_offset),
        kline(91, Decimal("100.8") + price_offset, Decimal("101.8") + price_offset,
              Decimal("100.6") + price_offset, Decimal("101.5") + price_offset),
        kline(92, Decimal("101.7") + price_offset, Decimal("102.8") + price_offset,
              Decimal("101.4") + price_offset, Decimal("102.5") + price_offset),
        kline(93, Decimal("102.7") + price_offset, Decimal("103.8") + price_offset,
              Decimal("102.4") + price_offset, Decimal("103.5") + price_offset),
        kline(94, Decimal("103.7") + price_offset, Decimal("104.8") + price_offset,
              Decimal("103.4") + price_offset, Decimal("104.5") + price_offset),
        kline(95, Decimal("104.7") + price_offset, Decimal("106") + price_offset,
              Decimal("104.4") + price_offset, Decimal("105.5") + price_offset),
        kline(96, Decimal("105.4") + price_offset, Decimal("105.8") + price_offset,
              Decimal("104.5") + price_offset, Decimal("104.8") + price_offset,
              "60", "30"),
        kline(97, Decimal("104.8") + price_offset, Decimal("105") + price_offset,
              Decimal("103.6") + price_offset, Decimal("104.2") + price_offset,
              "60", "30"),
        kline(98, Decimal("104.1") + price_offset, Decimal("105.2") + price_offset,
              Decimal("104") + price_offset, Decimal("105.1") + price_offset,
              "72", "39.6"),
        kline(99, Decimal("105.1") + price_offset, Decimal("105.5") + price_offset,
              Decimal("104.8") + price_offset, Decimal("105.3") + price_offset),
    ])
    return rows, BASE_TIME_MS + 99 * INTERVAL_MS + elapsed_ms


def candidate(symbol, qv_rank, mark="999"):
    return FundingCandidate(
        symbol=symbol,
        funding_rate=None,
        mark_price=Decimal(mark),
        quote_volume=Decimal("1000000") - Decimal(qv_rank),
        quote_volume_rank=qv_rank,
        candidate_universe="quote_volume_top",
    )


class N12AnalyzerTests(unittest.TestCase):
    def analyze(self, raw, checked, **kwargs):
        return analyze_n12_relative_strength_first_pullback(
            "N12USDT",
            raw,
            relative_strength_rank=kwargs.pop("relative_strength_rank", 1),
            relative_strength_return=kwargs.pop(
                "relative_strength_return", Decimal("0.05")
            ),
            checked_at_ms=checked,
            **kwargs,
        )

    def test_complete_structure_and_two_atr_references_pass(self):
        raw, checked = n12_klines()
        result = self.analyze(raw, checked)
        self.assertTrue(result.passed, result.reason)
        structure = result.structure
        self.assertEqual((structure.l_index, structure.h_index), (90, 95))
        self.assertEqual(structure.pullback_bars, 2)
        self.assertEqual(structure.p, Decimal("103.6"))
        self.assertEqual(structure.c_index, 98)
        self.assertEqual(structure.relative_strength_rank, 1)
        self.assertGreater(structure.atr_at_h, 0)
        self.assertGreater(structure.atr_at_c, 0)
        self.assertEqual(
            structure.entry_max_price,
            structure.c.close + Decimal("0.5") * structure.atr_at_c,
        )
        self.assertNotIn(str(structure.atr_at_h), structure.structure_id)

    def test_relative_strength_uses_exact_96_closed_bars(self):
        raw, _ = n12_klines()
        expected = Decimal(raw[-2][4]) / Decimal(raw[-97][1]) - Decimal("1")
        self.assertEqual(calculate_relative_strength_return(raw), expected)

    def test_up_leg_boundaries_and_single_body_equal_fifty_rejects(self):
        raw, _ = n12_klines()
        candles = parse_n12_klines(raw)
        valid = evaluate_n12_up_leg(candles, 90, 95)
        self.assertTrue(valid.passed)
        self.assertEqual(valid.bars, 5)
        equal_body = deepcopy(raw)
        equal_body[91][1] = "98.5"
        equal_body[91][3] = "98.4"
        metrics = evaluate_n12_up_leg(parse_n12_klines(equal_body), 90, 95)
        self.assertEqual(metrics.reason, "N12_UP_LEG_SINGLE_BODY_TOO_LARGE")
        short = evaluate_n12_up_leg(candles, 91, 95)
        self.assertEqual(short.reason, "N12_UP_LEG_TOO_SHORT")
        exact = evaluate_n12_up_leg(
            candles,
            90,
            95,
            min_gain_fraction=valid.gain_fraction,
            min_atr_multiple=valid.displacement / valid.atr_reference,
            min_efficiency=valid.efficiency,
        )
        self.assertTrue(exact.passed)
        self.assertEqual(
            evaluate_n12_up_leg(
                candles, 90, 95,
                min_gain_fraction=valid.gain_fraction + Decimal("0.000001"),
            ).reason,
            "N12_UP_LEG_GAIN_TOO_SMALL",
        )
        self.assertEqual(
            evaluate_n12_up_leg(
                candles, 90, 95,
                min_atr_multiple=(valid.displacement / valid.atr_reference) + Decimal("0.000001"),
            ).reason,
            "N12_UP_LEG_ATR_DISPLACEMENT_TOO_SMALL",
        )
        self.assertEqual(
            evaluate_n12_up_leg(
                candles, 90, 95,
                min_efficiency=valid.efficiency + Decimal("0.000001"),
            ).reason,
            "N12_UP_LEG_EFFICIENCY_TOO_LOW",
        )

    def test_five_bar_pullback_and_closed_depth_volume_boundaries(self):
        raw, _ = n12_klines()
        five = raw[:98]
        five.extend([
            kline(98, "104.3", "105.0", "103.8", "104.1", "70", "35"),
            kline(99, "104.1", "105.3", "103.7", "104.0", "70", "35"),
            kline(100, "104.0", "104.9", "103.6", "104.2", "70", "35"),
            kline(101, "104.1", "105.4", "104", "105.3", "84", "46.2"),
            kline(102, "105.3", "105.6", "104.8", "105.4"),
        ])
        checked = BASE_TIME_MS + 102 * INTERVAL_MS + 30_000
        result = self.analyze(five, checked)
        self.assertTrue(result.passed, result.reason)
        self.assertEqual(result.structure.pullback_bars, 5)
        self.assertEqual(result.structure.pullback_volume_median, Decimal("70"))
        self.assertEqual(result.structure.c_volume_multiple, Decimal("1.2"))
        self.assertEqual(result.structure.c_taker_buy_ratio, Decimal("0.55"))

        for depth, low in ((Decimal("0.20"), Decimal("104.8")), (Decimal("0.45"), Decimal("103.3"))):
            sample, sample_checked = n12_klines()
            sample[96][3] = str(low)
            sample[97][3] = str(low)
            if low > Decimal("104.2"):
                sample[97][1] = sample[97][4] = str(low + Decimal("0.2"))
            with self.subTest(depth=depth):
                analyzed = self.analyze(sample, sample_checked)
                self.assertTrue(analyzed.passed, analyzed.reason)
                self.assertEqual(analyzed.structure.pullback_depth, depth)

    def test_pullback_depth_midpoint_volume_and_h_boundaries(self):
        raw, checked = n12_klines()
        too_deep = deepcopy(raw)
        too_deep[97][3] = "103.29"
        high_volume = deepcopy(raw)
        high_volume[96][7] = high_volume[97][7] = "70.001"
        below_mid = deepcopy(raw)
        below_mid[97][4] = "102.999"
        below_mid[97][3] = "102.9"
        cases = (
            (too_deep, "N12_PULLBACK_DEPTH_OUT_OF_RANGE"),
            (high_volume, "N12_PULLBACK_VOLUME_TOO_HIGH"),
            (below_mid, "N12_PULLBACK_CLOSE_BELOW_MIDPOINT"),
        )
        for sample, reason in cases:
            with self.subTest(reason=reason):
                self.assertEqual(self.analyze(sample, checked).reason, reason)

    def test_confirmation_boundaries_and_first_confirmation_is_immutable(self):
        raw, checked = n12_klines()
        equal_previous_high = deepcopy(raw)
        equal_previous_high[98][4] = equal_previous_high[97][2]
        reached_h = deepcopy(raw)
        reached_h[98][2] = reached_h[98][4] = "106"
        low_taker = deepcopy(raw)
        low_taker[98][10] = "39.599"
        low_volume = deepcopy(raw)
        low_volume[98][7] = "71.999"
        low_volume[98][10] = "39.59945"
        self.assertFalse(self.analyze(equal_previous_high, checked).passed)
        self.assertEqual(self.analyze(reached_h, checked).reason, "N12_CONFIRMATION_REACHED_H")
        self.assertEqual(
            self.analyze(low_taker, checked).reason,
            "N12_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW",
        )
        self.assertEqual(
            self.analyze(low_volume, checked).reason,
            "N12_CONFIRMATION_VOLUME_TOO_LOW",
        )
        top_boundary = deepcopy(raw)
        top_boundary[98][1:5] = ["104.0", "105.5", "103.9", "105.1"]
        top_boundary[99][1:5] = ["105.2", "105.6", "104.8", "105.5"]
        self.assertTrue(self.analyze(top_boundary, checked).passed)
        first_bad_then_good = [
            *deepcopy(raw[:-1]),
            kline(99, "105.0", "105.7", "104.8", "105.6", "100", "60"),
            kline(100, "105.6", "105.8", "105.3", "105.7"),
        ]
        first_bad_then_good[98][10] = "39.599"
        result = self.analyze(
            first_bad_then_good,
            BASE_TIME_MS + 100 * INTERVAL_MS + 30_000,
        )
        self.assertEqual(
            result.reason,
            "N12_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW",
        )
        self.assertEqual(result.structure.c_index, 98)

    def test_entry_time_price_and_p_boundaries(self):
        for elapsed in (0, 119_999):
            raw, checked = n12_klines(elapsed_ms=elapsed)
            self.assertTrue(self.analyze(raw, checked).passed)
        expired, checked = n12_klines(elapsed_ms=120_000)
        self.assertEqual(self.analyze(expired, checked).reason, "N12_ENTRY_WINDOW_EXPIRED")
        raw, checked = n12_klines()
        at_p = deepcopy(raw)
        at_p[99][3] = "103.6"
        self.assertTrue(self.analyze(at_p, checked).passed)
        below_p = deepcopy(raw)
        below_p[99][3] = "103.599"
        self.assertEqual(self.analyze(below_p, checked).reason, "N12_ENTRY_LOW_BROKE_P")
        lower = deepcopy(raw)
        lower[99][4] = lower[98][2]
        self.assertTrue(self.analyze(lower, checked).passed)
        below = deepcopy(lower)
        below[99][4] = "105.199"
        self.assertEqual(
            self.analyze(below, checked).reason,
            "N12_ENTRY_PRICE_BELOW_CONFIRMATION",
        )
        result = self.analyze(raw, checked)
        upper = deepcopy(raw)
        upper[99][2] = upper[99][4] = str(result.structure.entry_max_price)
        self.assertTrue(self.analyze(upper, checked).passed)
        above = deepcopy(upper)
        above[99][2] = above[99][4] = str(result.structure.entry_max_price + Decimal("0.001"))
        self.assertEqual(self.analyze(above, checked).reason, "N12_ENTRY_PRICE_TOO_EXTENDED")

    def test_window_shift_rank_change_and_history_do_not_change_identity(self):
        raw, checked = n12_klines()
        first = self.analyze(raw, checked)
        shifted = [kline(-1, "100", "100.2", "99.8", "100"), *deepcopy(raw)]
        second = self.analyze(
            shifted,
            checked,
            relative_strength_rank=7,
            relative_strength_return=Decimal("0.03"),
        )
        self.assertEqual(first.structure_id, second.structure_id)
        historical = [*deepcopy(raw), kline(100, "105", "105.2", "104.8", "105")]
        replay = self.analyze(
            historical,
            BASE_TIME_MS + 100 * INTERVAL_MS + 30_000,
            historical_strength_by_entry_time={},
            historical_rank_context_complete=set(),
        )
        self.assertIn(
            "N12_HISTORICAL_RANK_CONTEXT_INSUFFICIENT",
            [event.reason for event in replay.historical_events],
        )

    def test_historical_entry_records_specific_price_failure_before_generic_miss(self):
        raw, _ = n12_klines()
        entry_time = BASE_TIME_MS + 99 * INTERVAL_MS
        strength = {entry_time: RelativeStrength("N12USDT", Decimal("0.05"), 1)}
        cases = (
            ("low", "103.599", "N12_HISTORICAL_ENTRY_LOW_BROKE_P"),
            ("close", "105.199", "N12_HISTORICAL_ENTRY_PRICE_BELOW_CONFIRMATION"),
            ("extended", "106", "N12_HISTORICAL_ENTRY_PRICE_TOO_EXTENDED"),
        )
        for field, value, reason in cases:
            with self.subTest(reason=reason):
                sample = deepcopy(raw)
                if field == "low":
                    sample[99][3] = value
                elif field == "close":
                    sample[99][4] = value
                else:
                    sample[99][2] = sample[99][4] = value
                sample.append(kline(100, "105.3", "105.6", "105.1", "105.3"))
                result = self.analyze(
                    sample,
                    BASE_TIME_MS + 100 * INTERVAL_MS + 30_000,
                    historical_strength_by_entry_time=strength,
                    historical_rank_context_complete={entry_time},
                )
                self.assertEqual(result.historical_events[0].reason, reason)

    def test_bad_data_and_gap_fail_closed(self):
        raw, checked = n12_klines()
        bad = deepcopy(raw)
        bad[20] = bad[20][:7]
        gap = deepcopy(raw)
        gap[20][0] += 1
        self.assertEqual(self.analyze(bad, checked).reason, "N12_KLINE_DATA_INVALID")
        self.assertEqual(self.analyze(gap, checked).reason, "N12_KLINE_SEQUENCE_INVALID")

    def test_additional_up_leg_pullback_and_time_boundaries(self):
        raw, checked = n12_klines()
        candles = parse_n12_klines(raw)
        flat = list(candles)
        for index in range(90, 96):
            flat[index] = replace(
                flat[index], open=Decimal("103"), close=Decimal("103")
            )
        self.assertEqual(
            evaluate_n12_up_leg(flat, 90, 95).reason,
            "N12_UP_LEG_SLOPE_NOT_POSITIVE",
        )

        shallow = deepcopy(raw)
        shallow[96][3] = shallow[97][3] = "104.801"
        shallow[96][4] = "104.9"
        shallow[97][1] = "104.9"
        shallow[97][4] = "104.85"
        self.assertEqual(
            self.analyze(shallow, checked).reason,
            "N12_PULLBACK_DEPTH_OUT_OF_RANGE",
        )

        broke_h = raw[:98]
        broke_h.extend([
            kline(98, "104.3", "106.001", "103.8", "104.1", "70", "35"),
            kline(99, "104.1", "105", "103.9", "104.2"),
        ])
        broke = self.analyze(
            broke_h, BASE_TIME_MS + 99 * INTERVAL_MS + 30_000
        )
        self.assertEqual(broke.reason, "N12_PULLBACK_BROKE_H")
        self.assertIsNone(broke.structure)

        midpoint = deepcopy(raw)
        midpoint[97][1] = "103.2"
        midpoint[97][3] = midpoint[97][4] = "103"
        allowed = self.analyze(
            midpoint,
            checked,
            pullback_depth_max=Decimal("0.50"),
        )
        self.assertTrue(allowed.passed, allowed.reason)

        before_open = BASE_TIME_MS + 99 * INTERVAL_MS - 1
        self.assertEqual(
            self.analyze(raw, before_open).reason,
            "N12_ENTRY_WINDOW_EXPIRED",
        )

    def test_pre_confirmation_failures_are_stage_only_and_include_five_bars(self):
        raw, checked = n12_klines()
        first_bar_failure = deepcopy(raw)
        first_bar_failure[96][3] = "102.8"
        first_bar_failure[96][4] = "102.9"
        first = self.analyze(first_bar_failure, checked)
        self.assertEqual(first.reason, "N12_PULLBACK_CLOSE_BELOW_MIDPOINT")
        self.assertIsNone(first.structure)
        self.assertEqual(len(first.stage_events), 1)

        five = raw[:98]
        five.extend([
            kline(98, "104.3", "105.0", "103.8", "104.1", "70", "35"),
            kline(99, "104.1", "105.3", "103.7", "104.0", "70", "35"),
            kline(100, "104.0", "105.6", "103.6", "104.2", "70", "35"),
            kline(101, "104.2", "104.8", "103.9", "104.1"),
        ])
        waiting = self.analyze(
            five, BASE_TIME_MS + 101 * INTERVAL_MS + 30_000
        )
        self.assertIsNone(waiting.structure)
        self.assertEqual(waiting.stage_events, ())

        valid = deepcopy(five)
        valid[101] = kline(
            101, "104.2", "105.9", "104", "105.8", "84", "46.2"
        )
        valid.append(kline(102, "105.8", "106", "104.8", "105.9"))
        passed = self.analyze(
            valid, BASE_TIME_MS + 102 * INTERVAL_MS + 30_000
        )
        self.assertTrue(passed.passed, passed.reason)
        self.assertEqual(passed.structure.c_index, 101)
        self.assertEqual(passed.structure.pullback_bars, 5)

        missed = deepcopy(five)
        missed.append(kline(102, "104.1", "104.8", "103.9", "104.2"))
        result = self.analyze(
            missed, BASE_TIME_MS + 102 * INTERVAL_MS + 30_000
        )
        self.assertEqual(result.reason, "N12_CONFIRMATION_NOT_FOUND")
        self.assertIsNone(result.structure)
        self.assertEqual(len(result.stage_events), 1)
        event = result.stage_events[0]
        self.assertEqual(event.detail["pullback_bars"], 5)
        self.assertEqual(event.detail["p"], "103.6")

    def test_first_basic_confirmation_after_one_pullback_is_not_skipped(self):
        raw, checked = n12_klines()
        raw[97][1:5] = ["104.8", "105.95", "104.5", "105.9"]
        raw[97][7] = "72"
        raw[97][10] = "39.6"
        result = self.analyze(raw, checked)
        self.assertEqual(result.reason, "N12_PULLBACK_DURATION_OUT_OF_RANGE")
        self.assertIsNotNone(result.structure)
        self.assertEqual(result.structure.c_index, 97)
        self.assertEqual(result.structure.pullback_bars, 1)

    def test_early_atr_pivot_is_skipped_and_later_structure_still_passes(self):
        raw, checked = n12_klines()
        raw[2][1:5] = ["100", "100.5", "99", "100.1"]
        raw[7][1:5] = ["103", "104", "102.8", "103.5"]
        result = self.analyze(raw, checked)
        self.assertTrue(result.passed, result.reason)
        self.assertEqual(result.structure.h_index, 95)

    def test_latest_confirmed_up_leg_wins_same_confirmation_and_shift(self):
        raw, checked = n12_klines()
        raw[87][1:5] = ["100.1", "100.5", "99.8", "100.2"]
        raw[88][3] = "100.1"
        raw[89][3] = "100.05"
        raw[92][2] = "106"
        raw[95][2] = "105.8"
        first = self.analyze(raw, checked)
        self.assertTrue(first.passed, first.reason)
        self.assertEqual(first.structure.h_index, 95)
        self.assertEqual(first.structure.l_index, 90)
        shifted = [kline(-1, "100", "100.2", "99.8", "100"), *deepcopy(raw)]
        second = self.analyze(shifted, checked)
        self.assertEqual(second.structure.h_time, first.structure.h_time)
        self.assertEqual(second.structure.l_time, first.structure.l_time)
        self.assertEqual(second.structure_id, first.structure_id)

    def test_identity_ignores_audited_atr_and_rank_values(self):
        raw, checked = n12_klines()
        first = self.analyze(raw, checked)
        changed = deepcopy(raw)
        changed[82][2] = str(Decimal(changed[82][2]) + Decimal("0.2"))
        changed[82][3] = str(Decimal(changed[82][3]) - Decimal("0.2"))
        second = self.analyze(
            changed,
            checked,
            relative_strength_rank=7,
            relative_strength_return=Decimal("0.03"),
        )
        self.assertTrue(second.passed, second.reason)
        self.assertEqual(first.structure_id, second.structure_id)
        self.assertNotEqual(first.structure.atr_at_h, second.structure.atr_at_h)
        self.assertNotEqual(first.structure.atr_at_c, second.structure.atr_at_c)
        self.assertNotEqual(
            first.structure.relative_strength_rank,
            second.structure.relative_strength_rank,
        )


class N12SchedulerTests(unittest.TestCase):
    def scheduler(self, tmpdir):
        recorder = make_test_recorder(
            str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_n12")
        )
        recorder.upsert_strategy_definitions((N12_STRATEGY,))
        return recorder, StrategyScheduler(
            (N12_STRATEGY,), 96, recorder, logging.getLogger("test_n12")
        )

    def test_cross_symbol_top10_tie_break_and_top100(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            items = [candidate(f"S{index:02d}USDT", index) for index in range(1, 13)]
            raw_by_symbol = {}
            checked = None
            for index, item in enumerate(items):
                raw, checked = n12_klines(price_offset=Decimal(index) / Decimal("100"))
                raw_by_symbol[item.symbol] = raw
            scan = recorder.begin_scan(len(items), items, dry_run=True)
            result = scheduler.evaluate(
                scan, {"quote_volume_top": items}, raw_by_symbol, checked_at_ms=checked
            )
            passed = [signal for signal in result.signals if signal.passed]
            self.assertEqual(len(passed), 10)
            self.assertEqual([signal.candidate.symbol for signal in passed[:2]], ["S01USDT", "S02USDT"])
            self.assertTrue(all(signal.analysis.relative_strength_return > 0 for signal in passed))
            outside = candidate("OUTUSDT", 101)
            result = scheduler.evaluate(
                None,
                {"quote_volume_top": [*items, outside]},
                {**raw_by_symbol, outside.symbol: raw_by_symbol[items[0].symbol]},
                checked_at_ms=checked,
            )
            self.assertNotIn(outside.symbol, [signal.candidate.symbol for signal in result.signals])

    def test_exact_ties_positive_filter_and_rank_ten_eleven_boundary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            items = [candidate(f"S{index:02d}USDT", index) for index in range(1, 12)]
            raw, checked = n12_klines()
            raw_by_symbol = {item.symbol: deepcopy(raw) for item in items}
            scan = recorder.begin_scan(len(items), items, dry_run=True)
            result = scheduler.evaluate(
                scan,
                {"quote_volume_top": items},
                raw_by_symbol,
                checked_at_ms=checked,
            )
            signals = {signal.candidate.symbol: signal for signal in result.signals}
            self.assertTrue(signals["S10USDT"].passed)
            self.assertEqual(signals["S10USDT"].analysis.relative_strength_rank, 10)
            self.assertFalse(signals["S11USDT"].passed)
            self.assertEqual(
                signals["S11USDT"].reason,
                "N12_RELATIVE_STRENGTH_NOT_TOP10",
            )
            self.assertEqual(signals["S11USDT"].analysis.relative_strength_rank, 11)

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            tied = [candidate("ZUSDT", 1), candidate("BUSDT", 2), candidate("AUSDT", 2)]
            raw, checked = n12_klines()
            result = scheduler.evaluate(
                None,
                {"quote_volume_top": tied},
                {item.symbol: deepcopy(raw) for item in tied},
                checked_at_ms=checked,
            )
            ranks = {
                signal.candidate.symbol: signal.analysis.relative_strength_rank
                for signal in result.signals
            }
            self.assertEqual(ranks, {"ZUSDT": 1, "BUSDT": 3, "AUSDT": 2})

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            positive = candidate("POSUSDT", 1)
            negative = candidate("NEGUSDT", 2)
            positive_raw, checked = n12_klines()
            negative_raw = deepcopy(positive_raw)
            negative_raw[-97][1] = "106"
            negative_raw[-97][2] = "106.1"
            scan = recorder.begin_scan(2, [positive, negative], dry_run=True)
            result = scheduler.evaluate(
                scan,
                {"quote_volume_top": [positive, negative]},
                {positive.symbol: positive_raw, negative.symbol: negative_raw},
                checked_at_ms=checked,
            )
            signals = {signal.candidate.symbol: signal for signal in result.signals}
            self.assertTrue(signals["POSUSDT"].passed)
            self.assertIsNone(signals["NEGUSDT"].analysis.relative_strength_rank)
            self.assertEqual(
                signals["NEGUSDT"].reason,
                "N12_RELATIVE_STRENGTH_NOT_TOP10",
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            rank100 = candidate("R100USDT", 100)
            rank101 = candidate("R101USDT", 101)
            raw, checked = n12_klines()
            result = scheduler.evaluate(
                None,
                {"quote_volume_top": [rank100, rank101]},
                {rank100.symbol: raw, rank101.symbol: deepcopy(raw)},
                checked_at_ms=checked,
            )
            self.assertEqual(
                [signal.candidate.symbol for signal in result.signals],
                [rank100.symbol],
            )

    def test_current_unclosed_price_does_not_change_frozen_rank(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            first = candidate("AUSDT", 1)
            second = candidate("BUSDT", 2)
            first_raw, checked = n12_klines()
            second_raw, _ = n12_klines()
            second_raw[-1][2] = second_raw[-1][4] = "999"
            result = scheduler.evaluate(
                None,
                {"quote_volume_top": [first, second]},
                {first.symbol: first_raw, second.symbol: second_raw},
                checked_at_ms=checked,
            )
            ranks = {
                signal.candidate.symbol: signal.analysis.relative_strength_rank
                for signal in result.signals
            }
            self.assertEqual(ranks, {"AUSDT": 1, "BUSDT": 2})

    def test_one_gapped_candidate_fails_the_entire_current_rank_context(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            valid = candidate("VALIDUSDT", 11)
            items = [candidate(f"BAD{index:02d}USDT", index) for index in range(1, 11)]
            valid_raw, checked = n12_klines()
            raw_by_symbol = {valid.symbol: valid_raw}
            for item in items:
                raw, _ = n12_klines()
                raw[20][0] += 1
                raw[-2][2] = raw[-2][4] = "110"
                raw_by_symbol[item.symbol] = raw
            result = scheduler.evaluate(
                None,
                {"quote_volume_top": [*items, valid]},
                raw_by_symbol,
                checked_at_ms=checked,
            )
            valid_signal = next(
                signal for signal in result.signals
                if signal.candidate.symbol == valid.symbol
            )
            self.assertFalse(valid_signal.passed)
            self.assertEqual(
                valid_signal.reason,
                "N12_RELATIVE_STRENGTH_CONTEXT_INSUFFICIENT",
            )
            self.assertIsNone(valid_signal.analysis.relative_strength_rank)
            self.assertEqual(result.passed_signals, [])
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_stage_states"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                    "WHERE strategy_id='N12'"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_rank_snapshots"
                ).fetchone()[0], 0)
            recovered_scan = recorder.begin_scan(1, [valid], dry_run=True)
            recovered = scheduler.evaluate(
                recovered_scan,
                {"quote_volume_top": [valid]},
                {valid.symbol: valid_raw},
                checked_at_ms=checked,
            )
            self.assertTrue(recovered.signals[0].passed, recovered.signals[0].reason)

    def test_misaligned_current_candle_fails_the_entire_rank_context(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            valid = candidate("VALIDUSDT", 1)
            stale = candidate("STALEUSDT", 2)
            valid_raw, checked = n12_klines()
            stale_raw, _ = n12_klines()
            for row in stale_raw:
                row[0] -= INTERVAL_MS
                row[6] -= INTERVAL_MS
            result = scheduler.evaluate(
                None,
                {"quote_volume_top": [valid, stale]},
                {valid.symbol: valid_raw, stale.symbol: stale_raw},
                checked_at_ms=checked,
            )
            self.assertEqual(result.passed_signals, [])
            self.assertTrue(all(
                signal.reason == "N12_RELATIVE_STRENGTH_CONTEXT_INSUFFICIENT"
                for signal in result.signals
            ))
            self.assertTrue(all(
                signal.analysis.relative_strength_rank is None
                for signal in result.signals
            ))
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_stage_states"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                    "WHERE strategy_id='N12'"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_rank_snapshots"
                ).fetchone()[0], 0)
            recovered_scan = recorder.begin_scan(2, [valid, stale], dry_run=True)
            recovered = scheduler.evaluate(
                recovered_scan,
                {"quote_volume_top": [valid, stale]},
                {valid.symbol: valid_raw, stale.symbol: deepcopy(valid_raw)},
                checked_at_ms=checked,
            )
            valid_recovered = next(
                signal for signal in recovered.signals
                if signal.candidate.symbol == valid.symbol
            )
            self.assertTrue(valid_recovered.passed, valid_recovered.reason)

    def test_same_bar_snapshot_freezes_tie_breaks_and_survives_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self.scheduler(tmpdir)
            items = [candidate(f"F{index:02d}USDT", index) for index in range(1, 12)]
            raw, checked = n12_klines()
            raw_by_symbol = {item.symbol: deepcopy(raw) for item in items}
            first = scheduler.evaluate(
                None, {"quote_volume_top": items}, raw_by_symbol,
                checked_at_ms=checked,
            )
            first_ranks = {
                signal.candidate.symbol: signal.analysis.relative_strength_rank
                for signal in first.signals
            }
            self.assertEqual(first_ranks["F10USDT"], 10)
            self.assertEqual(first_ranks["F11USDT"], 11)

            swapped = [
                candidate(
                    item.symbol,
                    11 if item.symbol == "F10USDT" else 10
                    if item.symbol == "F11USDT" else item.quote_volume_rank,
                )
                for item in items
            ]
            second = scheduler.evaluate(
                None, {"quote_volume_top": swapped}, raw_by_symbol,
                checked_at_ms=checked,
            )
            second_ranks = {
                signal.candidate.symbol: signal.analysis.relative_strength_rank
                for signal in second.signals
            }
            self.assertEqual(second_ranks["F10USDT"], 10)
            self.assertEqual(second_ranks["F11USDT"], 11)

            restarted = make_test_recorder(str(db), logging.getLogger("n12_rank_restart"))
            restarted_scheduler = StrategyScheduler(
                (N12_STRATEGY,), 96, restarted, logging.getLogger("n12_rank_restart")
            )
            third = restarted_scheduler.evaluate(
                None, {"quote_volume_top": swapped}, raw_by_symbol,
                checked_at_ms=checked,
            )
            third_ranks = {
                signal.candidate.symbol: signal.analysis.relative_strength_rank
                for signal in third.signals
            }
            self.assertEqual(third_ranks["F10USDT"], 10)
            self.assertEqual(third_ranks["F11USDT"], 11)
            with restarted._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_rank_snapshots"
                ).fetchone()[0], 1)

    def test_same_bar_top100_replacement_waits_for_next_bar_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            initial = [candidate(f"U{index:03d}USDT", index) for index in range(1, 101)]
            raw, checked = n12_klines()
            initial_raw = {item.symbol: deepcopy(raw) for item in initial}
            initial_scan = recorder.begin_scan(len(initial), initial, dry_run=True)
            scheduler.evaluate(
                initial_scan, {"quote_volume_top": initial}, initial_raw,
                checked_at_ms=checked,
            )

            new_item = candidate("NEWUSDT", 100)
            new_raw = deepcopy(raw)
            new_raw[-97][1] = new_raw[-97][3]
            replaced = [*initial[:-1], new_item]
            replaced_raw = {
                **{item.symbol: initial_raw[item.symbol] for item in initial[:-1]},
                new_item.symbol: new_raw,
            }
            same_bar_scan = recorder.begin_scan(len(replaced), replaced, dry_run=True)
            same_bar = scheduler.evaluate(
                same_bar_scan, {"quote_volume_top": replaced}, replaced_raw,
                checked_at_ms=checked,
            )
            new_signal = next(
                signal for signal in same_bar.signals
                if signal.candidate.symbol == new_item.symbol
            )
            self.assertFalse(new_signal.passed)
            self.assertIsNone(new_signal.analysis.relative_strength_rank)

            next_raw = {
                symbol: deepcopy(rows) for symbol, rows in replaced_raw.items()
            }
            for rows in next_raw.values():
                for row in rows:
                    row[0] += INTERVAL_MS
                    row[6] += INTERVAL_MS
            next_scan = recorder.begin_scan(len(replaced), replaced, dry_run=True)
            next_bar = scheduler.evaluate(
                next_scan, {"quote_volume_top": replaced}, next_raw,
                checked_at_ms=checked + INTERVAL_MS,
            )
            next_new = next(
                signal for signal in next_bar.signals
                if signal.candidate.symbol == new_item.symbol
            )
            self.assertEqual(next_new.analysis.relative_strength_rank, 1)
            self.assertTrue(next_new.passed, next_new.reason)
            with recorder._connect() as connection:
                snapshot = connection.execute(
                    "SELECT current_open_time FROM n12_rank_snapshots"
                ).fetchall()
            self.assertEqual(snapshot, [(str(BASE_TIME_MS + 100 * INTERVAL_MS),)])

    def test_rank_snapshot_read_write_and_payload_fail_closed(self):
        raw, checked = n12_klines()
        item = candidate("N12USDT", 1)
        for failure in ("read", "write", "payload"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = self.scheduler(tmpdir)
                if failure == "read":
                    recorder.get_n12_rank_snapshot = lambda *args: (_ for _ in ()).throw(
                        RuntimeError("snapshot read failed")
                    )
                elif failure == "write":
                    recorder.record_n12_rank_snapshot = lambda *args: False
                else:
                    with recorder._connect() as connection:
                        connection.execute(
                            """
                            INSERT INTO n12_rank_snapshots (
                                strategy_id, current_open_time, payload_json,
                                created_at, updated_at
                            ) VALUES ('N12', ?, '{"current_open_time_ms":"bad","rows":[]}', 'x', 'x')
                            """,
                            (str(raw[-1][0]),),
                        )
                result = scheduler.evaluate(
                    None, {"quote_volume_top": [item]}, {item.symbol: raw},
                    checked_at_ms=checked,
                )
                self.assertEqual(
                    result.signals[0].reason,
                    "N12_RELATIVE_STRENGTH_CONTEXT_INSUFFICIENT",
                )
                with recorder._connect() as connection:
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n12_stage_states"
                    ).fetchone()[0], 0)
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                        "WHERE strategy_id='N12'"
                    ).fetchone()[0], 0)

    def test_semantically_invalid_rank_snapshots_fail_closed_without_state(self):
        raw, checked = n12_klines()
        item = candidate("N12USDT", 1)
        current_time = int(raw[-1][0])
        invalid_payloads = {
            "rank_101": {
                "current_open_time_ms": current_time,
                "rows": [{
                    "symbol": item.symbol,
                    "return_24h": "0.05",
                    "quote_volume_rank": 101,
                    "rank": 1,
                }],
            },
            "duplicate_quote_rank": {
                "current_open_time_ms": current_time,
                "rows": [
                    {
                        "symbol": item.symbol,
                        "return_24h": "0.05",
                        "quote_volume_rank": 1,
                        "rank": 1,
                    },
                    {
                        "symbol": "OTHERUSDT",
                        "return_24h": "0.04",
                        "quote_volume_rank": 1,
                        "rank": 2,
                    },
                ],
            },
            "one_hundred_one_rows": {
                "current_open_time_ms": current_time,
                "rows": [
                    {
                        "symbol": f"X{index:03d}USDT",
                        "return_24h": str(Decimal("1") - Decimal(index) / Decimal("1000")),
                        "quote_volume_rank": index,
                        "rank": index,
                    }
                    for index in range(1, 102)
                ],
            },
            "boolean_time": {
                "current_open_time_ms": True,
                "rows": [{
                    "symbol": item.symbol,
                    "return_24h": "0.05",
                    "quote_volume_rank": 1,
                    "rank": 1,
                }],
            },
            "boolean_quote_rank": {
                "current_open_time_ms": current_time,
                "rows": [{
                    "symbol": item.symbol,
                    "return_24h": "0.05",
                    "quote_volume_rank": True,
                    "rank": 1,
                }],
            },
            "boolean_final_rank": {
                "current_open_time_ms": current_time,
                "rows": [{
                    "symbol": item.symbol,
                    "return_24h": "0.05",
                    "quote_volume_rank": 1,
                    "rank": True,
                }],
            },
            "non_string_symbol": {
                "current_open_time_ms": current_time,
                "rows": [{
                    "symbol": 123,
                    "return_24h": "0.05",
                    "quote_volume_rank": 1,
                    "rank": 1,
                }],
            },
        }
        for name, payload in invalid_payloads.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = self.scheduler(tmpdir)
                with recorder._connect() as connection:
                    connection.execute(
                        """
                        INSERT INTO n12_rank_snapshots (
                            strategy_id, current_open_time, payload_json,
                            created_at, updated_at
                        ) VALUES ('N12', ?, ?, 'x', 'x')
                        """,
                        (str(current_time), json.dumps(payload)),
                    )
                result = scheduler.evaluate(
                    None, {"quote_volume_top": [item]}, {item.symbol: raw},
                    checked_at_ms=checked,
                )
                self.assertEqual(
                    result.signals[0].reason,
                    "N12_RELATIVE_STRENGTH_CONTEXT_INSUFFICIENT",
                )
                with recorder._connect() as connection:
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n12_stage_states"
                    ).fetchone()[0], 0)
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                        "WHERE strategy_id='N12'"
                    ).fetchone()[0], 0)

    def test_stage_and_structure_state_survive_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self.scheduler(tmpdir)
            item = candidate("N12USDT", 1)
            raw, checked = n12_klines()
            first_scan = recorder.begin_scan(1, [item], dry_run=True)
            first = scheduler.evaluate(
                first_scan, {"quote_volume_top": [item]}, {item.symbol: raw}, checked_at_ms=checked
            )
            self.assertTrue(first.signals[0].passed)
            restarted = make_test_recorder(str(db), logging.getLogger("n12_restart"))
            second_scheduler = StrategyScheduler(
                (N12_STRATEGY,), 96, restarted, logging.getLogger("n12_restart")
            )
            second_scan = restarted.begin_scan(1, [item], dry_run=True)
            second = second_scheduler.evaluate(
                second_scan, {"quote_volume_top": [item]}, {item.symbol: raw}, checked_at_ms=checked
            )
            self.assertIn(second.signals[0].reason, {"N12_STAGE_CONSUMED", "N12_STRUCTURE_CONSUMED"})
            with restarted._connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM n12_stage_states").fetchone()[0], 1)

    def test_pre_c_failures_persist_stage_only_and_restart_cannot_recover(self):
        scenarios = []
        first_bar, checked = n12_klines()
        first_bar[96][3] = "102.8"
        first_bar[96][4] = "102.9"
        scenarios.append((first_bar, checked, "N12_PULLBACK_CLOSE_BELOW_MIDPOINT", 1))

        for raw, scenario_checked, reason, pullback_bars in scenarios:
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as tmpdir:
                db = Path(tmpdir) / "review.sqlite3"
                recorder, scheduler = self.scheduler(tmpdir)
                item = candidate("N12USDT", 1)
                first = scheduler.evaluate(
                    None,
                    {"quote_volume_top": [item]},
                    {item.symbol: raw},
                    checked_at_ms=scenario_checked,
                )
                self.assertEqual(first.signals[0].reason, reason)
                self.assertIsNone(first.signals[0].analysis.structure_id)
                with recorder._connect() as connection:
                    stage = connection.execute(
                        "SELECT reason, detail_json FROM n12_stage_states"
                    ).fetchone()
                    self.assertEqual(stage[0], reason)
                    self.assertIn(f'"pullback_bars": {pullback_bars}', stage[1])
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                        "WHERE strategy_id='N12'"
                    ).fetchone()[0], 0)
                restarted = make_test_recorder(str(db), logging.getLogger("n12_pre_c_restart"))
                restarted_scheduler = StrategyScheduler(
                    (N12_STRATEGY,), 96, restarted,
                    logging.getLogger("n12_pre_c_restart"),
                )
                recovered, recovered_checked = n12_klines()
                second = restarted_scheduler.evaluate(
                    None,
                    {"quote_volume_top": [item]},
                    {item.symbol: recovered},
                    checked_at_ms=recovered_checked,
                )
                self.assertEqual(second.signals[0].reason, "N12_STAGE_CONSUMED")
                with restarted._connect() as connection:
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                        "WHERE strategy_id='N12'"
                    ).fetchone()[0], 0)

    def test_five_pullbacks_wait_then_closed_sixth_confirms_or_consumes(self):
        base, _ = n12_klines()
        waiting = base[:98]
        waiting.extend([
            kline(98, "104.3", "105.0", "103.8", "104.1", "70", "35"),
            kline(99, "104.1", "105.3", "103.7", "104.0", "70", "35"),
            kline(100, "104.0", "105.6", "103.6", "104.2", "70", "35"),
            kline(101, "104.2", "104.8", "103.9", "104.1"),
        ])
        item = candidate("N12USDT", 1)

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            first_scan = recorder.begin_scan(1, [item], dry_run=True)
            first = scheduler.evaluate(
                first_scan, {"quote_volume_top": [item]}, {item.symbol: waiting},
                checked_at_ms=BASE_TIME_MS + 101 * INTERVAL_MS + 30_000,
            )
            self.assertFalse(first.signals[0].passed)
            self.assertIsNone(first.signals[0].analysis.structure_id)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_stage_states"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                    "WHERE strategy_id='N12'"
                ).fetchone()[0], 0)

            valid = deepcopy(waiting)
            valid[101] = kline(
                101, "104.2", "105.9", "104", "105.8", "84", "46.2"
            )
            valid.append(kline(102, "105.8", "106", "104.8", "105.9"))
            second_scan = recorder.begin_scan(1, [item], dry_run=True)
            second = scheduler.evaluate(
                second_scan, {"quote_volume_top": [item]}, {item.symbol: valid},
                checked_at_ms=BASE_TIME_MS + 102 * INTERVAL_MS + 30_000,
            )
            self.assertTrue(second.signals[0].passed, second.signals[0].reason)
            self.assertIsNotNone(second.signals[0].analysis.structure_id)
            self.assertEqual(second.signals[0].analysis.structure.c_index, 101)
            self.assertEqual(second.signals[0].analysis.structure.pullback_bars, 5)

        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self.scheduler(tmpdir)
            non_c = deepcopy(waiting)
            non_c.append(kline(102, "104.1", "104.8", "103.9", "104.2"))
            missed = scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: non_c},
                checked_at_ms=BASE_TIME_MS + 102 * INTERVAL_MS + 30_000,
            )
            self.assertEqual(
                missed.signals[0].reason, "N12_CONFIRMATION_NOT_FOUND"
            )
            self.assertIsNone(missed.signals[0].analysis.structure_id)
            with recorder._connect() as connection:
                stage = connection.execute(
                    "SELECT reason, detail_json FROM n12_stage_states"
                ).fetchone()
                self.assertEqual(stage[0], "N12_CONFIRMATION_NOT_FOUND")
                self.assertIn('"pullback_bars": 5', stage[1])
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                    "WHERE strategy_id='N12'"
                ).fetchone()[0], 0)
            restarted = make_test_recorder(str(db), logging.getLogger("n12_no_c_restart"))
            restarted_scheduler = StrategyScheduler(
                (N12_STRATEGY,), 96, restarted, logging.getLogger("n12_no_c_restart")
            )
            replay = restarted_scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: non_c},
                checked_at_ms=BASE_TIME_MS + 102 * INTERVAL_MS + 30_000,
            )
            self.assertEqual(replay.signals[0].reason, "N12_STAGE_CONSUMED")

    def test_first_c_with_short_pullback_is_atomic_and_never_moves_later(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self.scheduler(tmpdir)
            item = candidate("N12USDT", 1)
            raw, checked = n12_klines()
            raw[97][1:5] = ["104.8", "105.95", "104.5", "105.9"]
            raw[97][7] = "72"
            raw[97][10] = "39.6"
            first = scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: raw},
                checked_at_ms=checked,
            )
            analysis = first.signals[0].analysis
            self.assertEqual(first.signals[0].reason, "N12_PULLBACK_DURATION_OUT_OF_RANGE")
            self.assertEqual(analysis.structure.c_index, 97)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_stage_states"
                ).fetchone()[0], 1)
                terminal = connection.execute(
                    "SELECT reason, detail_json FROM strategy_structure_terminal_states "
                    "WHERE strategy_id='N12'"
                ).fetchone()
                self.assertEqual(terminal[0], "N12_PULLBACK_DURATION_OUT_OF_RANGE")
                self.assertIn('"c_index": 97', terminal[1])
            restarted = make_test_recorder(str(db), logging.getLogger("n12_first_c_restart"))
            restarted_scheduler = StrategyScheduler(
                (N12_STRATEGY,), 96, restarted, logging.getLogger("n12_first_c_restart")
            )
            second = restarted_scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: raw},
                checked_at_ms=checked,
            )
            self.assertIn(
                second.signals[0].reason,
                {"N12_STAGE_CONSUMED", "N12_STRUCTURE_CONSUMED"},
            )

    def test_stage_and_full_terminal_write_is_atomic_on_second_insert_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self.scheduler(tmpdir)
            with recorder._connect() as connection:
                connection.execute(
                    """
                    CREATE TRIGGER fail_n12_full_terminal
                    BEFORE INSERT ON strategy_structure_terminal_states
                    WHEN NEW.strategy_id = 'N12'
                    BEGIN
                        SELECT RAISE(ABORT, 'forced N12 full terminal failure');
                    END
                    """
                )
                commit_test_schema_change(recorder, connection)
            item = candidate("N12USDT", 1)
            raw, checked = n12_klines()
            failed_scan = recorder.begin_scan(1, [item], dry_run=True)
            failed = scheduler.evaluate(
                failed_scan, {"quote_volume_top": [item]}, {item.symbol: raw},
                checked_at_ms=checked,
            )
            self.assertEqual(failed.signals[0].reason, "N12_STATE_PERSIST_FAILED")
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_stage_states"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                    "WHERE strategy_id='N12'"
                ).fetchone()[0], 0)
                connection.execute("DROP TRIGGER fail_n12_full_terminal")
                commit_test_schema_change(recorder, connection)
            restarted = make_test_recorder(str(db), logging.getLogger("n12_atomic_restart"))
            restarted_scheduler = StrategyScheduler(
                (N12_STRATEGY,), 96, restarted, logging.getLogger("n12_atomic_restart")
            )
            recovered_scan = restarted.begin_scan(1, [item], dry_run=True)
            recovered = restarted_scheduler.evaluate(
                recovered_scan, {"quote_volume_top": [item]}, {item.symbol: raw},
                checked_at_ms=checked,
            )
            self.assertTrue(recovered.signals[0].passed)
            with restarted._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_stage_states"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                    "WHERE strategy_id='N12'"
                ).fetchone()[0], 1)

    def test_historical_stage_and_full_terminal_use_the_same_atomic_write(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            with recorder._connect() as connection:
                connection.execute(
                    """
                    CREATE TRIGGER fail_n12_historical_full
                    BEFORE INSERT ON strategy_structure_terminal_states
                    WHEN NEW.strategy_id = 'N12'
                    BEGIN
                        SELECT RAISE(ABORT, 'forced N12 historical failure');
                    END
                    """
                )
                commit_test_schema_change(recorder, connection)
            item = candidate("N12USDT", 1)
            raw, _ = n12_klines()
            raw.append(kline(100, "105.3", "105.6", "105.1", "105.3"))
            checked = BASE_TIME_MS + 100 * INTERVAL_MS + 30_000
            failed = scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: raw},
                checked_at_ms=checked,
            )
            self.assertEqual(failed.signals[0].reason, "N12_STATE_PERSIST_FAILED")
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_stage_states"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                    "WHERE strategy_id='N12'"
                ).fetchone()[0], 0)
                connection.execute("DROP TRIGGER fail_n12_historical_full")
                commit_test_schema_change(recorder, connection)
            retry = scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: raw},
                checked_at_ms=checked,
            )
            self.assertEqual(retry.signals[0].reason, "N12_HISTORICAL_ENTRY_MISSED")
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_stage_states"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                    "WHERE strategy_id='N12'"
                ).fetchone()[0], 1)

    def test_legacy_half_states_reconcile_from_the_existing_payload(self):
        for existing_side in ("stage", "full"):
            with self.subTest(existing_side=existing_side), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = self.scheduler(tmpdir)
                item = candidate("N12USDT", 1)
                raw, checked = n12_klines()
                analysis = analyze_n12_relative_strength_first_pullback(
                    item.symbol,
                    raw,
                    relative_strength_rank=1,
                    relative_strength_return=Decimal("0.05"),
                    checked_at_ms=checked,
                )
                structure = analysis.structure
                detail = analysis.detail_json()
                if existing_side == "stage":
                    self.assertTrue(recorder.record_n12_stage_terminal(
                        "N12", item.symbol, structure.l_time, structure.h_time,
                        "CONSUMED", "PASSED", detail,
                    ))
                else:
                    self.assertTrue(recorder.record_strategy_structure_terminal(
                        "N12", item.symbol, structure.structure_id,
                        "CONSUMED", "PASSED", detail,
                    ))
                result = scheduler.evaluate(
                    None, {"quote_volume_top": [item]}, {item.symbol: raw},
                    checked_at_ms=checked,
                )
                self.assertIn(
                    result.signals[0].reason,
                    {"N12_STAGE_CONSUMED", "N12_STRUCTURE_CONSUMED"},
                )
                with recorder._connect() as connection:
                    stage = connection.execute(
                        "SELECT status, reason, detail_json FROM n12_stage_states"
                    ).fetchone()
                    terminal = connection.execute(
                        "SELECT status, reason, detail_json "
                        "FROM strategy_structure_terminal_states WHERE strategy_id='N12'"
                    ).fetchone()
                self.assertEqual(tuple(stage), tuple(terminal))
                self.assertEqual(stage[0:2], ("CONSUMED", "PASSED"))

    def test_stage_only_payload_is_preserved_when_structure_becomes_historical(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            item = candidate("N12USDT", 1)
            raw, checked = n12_klines()
            current_analysis = analyze_n12_relative_strength_first_pullback(
                item.symbol,
                raw,
                relative_strength_rank=1,
                relative_strength_return=Decimal("0.05"),
                checked_at_ms=checked,
            )
            structure = current_analysis.structure
            self.assertTrue(recorder.record_n12_stage_terminal(
                "N12", item.symbol, structure.l_time, structure.h_time,
                "CONSUMED", "PASSED", current_analysis.detail_json(),
            ))
            historical = deepcopy(raw)
            historical.append(kline(100, "105.3", "105.6", "105.1", "105.3"))
            result = scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: historical},
                checked_at_ms=BASE_TIME_MS + 100 * INTERVAL_MS + 30_000,
            )
            self.assertEqual(result.signals[0].reason, "N12_STAGE_CONSUMED")
            with recorder._connect() as connection:
                stage = connection.execute(
                    "SELECT status, reason, detail_json FROM n12_stage_states"
                ).fetchone()
                terminal = connection.execute(
                    "SELECT status, reason, detail_json "
                    "FROM strategy_structure_terminal_states WHERE strategy_id='N12'"
                ).fetchone()
            self.assertEqual(tuple(stage), tuple(terminal))
            self.assertEqual(stage[0:2], ("CONSUMED", "PASSED"))
            self.assertNotEqual(stage[1], "N12_HISTORICAL_ENTRY_MISSED")

    def test_unprovable_stage_only_relationship_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            item = candidate("N12USDT", 1)
            raw, checked = n12_klines()
            analysis = analyze_n12_relative_strength_first_pullback(
                item.symbol,
                raw,
                relative_strength_rank=1,
                relative_strength_return=Decimal("0.05"),
                checked_at_ms=checked,
            )
            structure = analysis.structure
            recorder.record_n12_stage_terminal(
                "N12", item.symbol, structure.l_time, structure.h_time,
                "CONSUMED", "PASSED", {"legacy": "identity unavailable"},
            )
            result = scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: raw},
                checked_at_ms=checked,
            )
            self.assertEqual(result.signals[0].reason, "N12_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_stage_states"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                    "WHERE strategy_id='N12'"
                ).fetchone()[0], 0)

    def test_half_state_repair_failure_preserves_source_and_retries(self):
        for existing_side in ("stage", "full"):
            with self.subTest(existing_side=existing_side), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = self.scheduler(tmpdir)
                item = candidate("N12USDT", 1)
                raw, checked = n12_klines()
                analysis = analyze_n12_relative_strength_first_pullback(
                    item.symbol,
                    raw,
                    relative_strength_rank=1,
                    relative_strength_return=Decimal("0.05"),
                    checked_at_ms=checked,
                )
                structure = analysis.structure
                detail = analysis.detail_json()
                if existing_side == "stage":
                    recorder.record_n12_stage_terminal(
                        "N12", item.symbol, structure.l_time, structure.h_time,
                        "CONSUMED", "PASSED", detail,
                    )
                    trigger_name = "fail_n12_half_full"
                    trigger_table = "strategy_structure_terminal_states"
                else:
                    recorder.record_strategy_structure_terminal(
                        "N12", item.symbol, structure.structure_id,
                        "CONSUMED", "PASSED", detail,
                    )
                    trigger_name = "fail_n12_half_stage"
                    trigger_table = "n12_stage_states"
                with recorder._connect() as connection:
                    connection.execute(
                        f"""
                        CREATE TRIGGER {trigger_name}
                        BEFORE INSERT ON {trigger_table}
                        BEGIN
                            SELECT RAISE(ABORT, 'forced half-state repair failure');
                        END
                        """
                    )
                    commit_test_schema_change(recorder, connection)
                failed = scheduler.evaluate(
                    None, {"quote_volume_top": [item]}, {item.symbol: raw},
                    checked_at_ms=checked,
                )
                self.assertEqual(failed.signals[0].reason, "N12_STATE_PERSIST_FAILED")
                with recorder._connect() as connection:
                    stage_count = connection.execute(
                        "SELECT COUNT(*) FROM n12_stage_states"
                    ).fetchone()[0]
                    full_count = connection.execute(
                        "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                        "WHERE strategy_id='N12'"
                    ).fetchone()[0]
                    self.assertEqual(
                        (stage_count, full_count),
                        (1, 0) if existing_side == "stage" else (0, 1),
                    )
                    connection.execute(f"DROP TRIGGER {trigger_name}")
                    commit_test_schema_change(recorder, connection)
                retry = scheduler.evaluate(
                    None, {"quote_volume_top": [item]}, {item.symbol: raw},
                    checked_at_ms=checked,
                )
                self.assertIn(
                    retry.signals[0].reason,
                    {"N12_STAGE_CONSUMED", "N12_STRUCTURE_CONSUMED"},
                )
                with recorder._connect() as connection:
                    stage = connection.execute(
                        "SELECT status, reason, detail_json FROM n12_stage_states"
                    ).fetchone()
                    terminal = connection.execute(
                        "SELECT status, reason, detail_json "
                        "FROM strategy_structure_terminal_states WHERE strategy_id='N12'"
                    ).fetchone()
                self.assertEqual(tuple(stage), tuple(terminal))

    def test_batch_and_analyzer_receive_the_same_shared_kline_object(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            item = candidate("N12USDT", 1)
            raw, checked = n12_klines()
            batch_objects = []
            analyzer_objects = []
            original_parse = scheduler_module.parse_n12_klines
            original_analyze = scheduler_module.analyze_n12_relative_strength_first_pullback

            def inspect_parse(value):
                batch_objects.append(value)
                return original_parse(value)

            def inspect_analyze(symbol, value, **kwargs):
                analyzer_objects.append(value)
                return original_analyze(symbol, value, **kwargs)

            with patch.object(scheduler_module, "parse_n12_klines", inspect_parse), patch.object(
                scheduler_module,
                "analyze_n12_relative_strength_first_pullback",
                inspect_analyze,
            ):
                scheduler.evaluate(
                    None, {"quote_volume_top": [item]}, {item.symbol: raw},
                    checked_at_ms=checked,
                )
            self.assertTrue(batch_objects)
            self.assertTrue(analyzer_objects)
            self.assertTrue(all(value is raw for value in batch_objects))
            self.assertTrue(all(value is raw for value in analyzer_objects))

    def test_failed_first_pullback_stage_blocks_later_recovery_after_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self.scheduler(tmpdir)
            item = candidate("N12USDT", 1)
            failed, checked = n12_klines()
            failed[97][3] = "103.29"
            first = scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: failed}, checked_at_ms=checked
            )
            self.assertEqual(first.signals[0].reason, "N12_PULLBACK_DEPTH_OUT_OF_RANGE")
            restarted = make_test_recorder(str(db), logging.getLogger("n12_stage_restart"))
            restarted_scheduler = StrategyScheduler(
                (N12_STRATEGY,), 96, restarted, logging.getLogger("n12_stage_restart")
            )
            recovered, recovered_checked = n12_klines()
            second = restarted_scheduler.evaluate(
                None,
                {"quote_volume_top": [item]},
                {item.symbol: recovered},
                checked_at_ms=recovered_checked,
            )
            self.assertEqual(second.signals[0].reason, "N12_STAGE_CONSUMED")

    def test_entry_price_failure_consumes_and_cannot_recover_same_candle(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            item = candidate("N12USDT", 1)
            failed, checked = n12_klines()
            failed[-1][4] = "105.199"
            first = scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: failed}, checked_at_ms=checked
            )
            self.assertEqual(first.signals[0].reason, "N12_ENTRY_PRICE_BELOW_CONFIRMATION")
            recovered, recovered_checked = n12_klines()
            second = scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: recovered},
                checked_at_ms=recovered_checked,
            )
            self.assertEqual(second.signals[0].reason, "N12_STAGE_CONSUMED")

    def test_historical_rank_is_rebuilt_at_entry_time_and_missing_context_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            first_item = candidate("AUSDT", 1)
            second_item = candidate("BUSDT", 2)
            first, _ = n12_klines()
            second, _ = n12_klines(price_offset=Decimal("1"))
            first.append(kline(100, "105.2", "105.4", "105", "105.2"))
            second.append(kline(100, "106.2", "106.4", "106", "106.2"))
            checked = BASE_TIME_MS + 100 * INTERVAL_MS + 30_000
            result = scheduler.evaluate(
                None,
                {"quote_volume_top": [first_item, second_item]},
                {first_item.symbol: first, second_item.symbol: second},
                checked_at_ms=checked,
            )
            self.assertTrue(any(
                event.reason == "N12_HISTORICAL_ENTRY_MISSED"
                for event in result.signals[0].analysis.historical_events
            ))

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            first_item = candidate("AUSDT", 1)
            second_item = candidate("BUSDT", 2)
            first, _ = n12_klines()
            second, _ = n12_klines(price_offset=Decimal("1"))
            first.append(kline(100, "105.2", "105.4", "105", "105.2"))
            second.append(kline(100, "106.2", "106.4", "106", "106.2"))
            second = second[4:]
            result = scheduler.evaluate(
                None,
                {"quote_volume_top": [first_item, second_item]},
                {first_item.symbol: first, second_item.symbol: second},
                checked_at_ms=BASE_TIME_MS + 100 * INTERVAL_MS + 30_000,
            )
            self.assertTrue(any(
                event.reason == "N12_HISTORICAL_RANK_CONTEXT_INSUFFICIENT"
                for event in result.signals[0].analysis.historical_events
            ))

    def test_historical_rank_ten_eleven_uses_entry_snapshot_not_current_rank(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            items = [candidate(f"H{index:02d}USDT", index) for index in range(1, 12)]
            raw_by_symbol = {}
            for index, item in enumerate(items, start=1):
                raw, _ = n12_klines()
                if index == 10:
                    raw[99][4] = "105.2"
                elif index == 11:
                    raw[99][4] = "105.5"
                else:
                    raw[99][4] = "105.4"
                raw.append(kline(100, "105.3", "105.6", "105.1", "105.3"))
                raw_by_symbol[item.symbol] = raw
            result = scheduler.evaluate(
                None,
                {"quote_volume_top": items},
                raw_by_symbol,
                checked_at_ms=BASE_TIME_MS + 100 * INTERVAL_MS + 30_000,
            )
            signals = {signal.candidate.symbol: signal for signal in result.signals}
            ten_event = signals["H10USDT"].analysis.historical_events[0]
            eleven_event = signals["H11USDT"].analysis.historical_events[0]
            self.assertEqual(signals["H10USDT"].analysis.relative_strength_rank, 11)
            self.assertEqual(ten_event.structure.relative_strength_rank, 10)
            self.assertEqual(ten_event.reason, "N12_HISTORICAL_ENTRY_MISSED")
            self.assertEqual(signals["H11USDT"].analysis.relative_strength_rank, 1)
            self.assertEqual(eleven_event.structure.relative_strength_rank, 11)
            self.assertEqual(
                eleven_event.reason,
                "N12_HISTORICAL_RELATIVE_STRENGTH_NOT_TOP10",
            )

    def test_historical_96_bar_gap_is_never_ranked(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            valid = candidate("AUSDT", 1)
            gapped = candidate("BUSDT", 2)
            valid_raw, _ = n12_klines()
            gapped_raw, _ = n12_klines(price_offset=Decimal("1"))
            valid_raw.append(kline(100, "105.2", "105.4", "105", "105.2"))
            gapped_raw.append(kline(100, "106.2", "106.4", "106", "106.2"))
            gapped_raw[20][0] += 1
            result = scheduler.evaluate(
                None,
                {"quote_volume_top": [valid, gapped]},
                {valid.symbol: valid_raw, gapped.symbol: gapped_raw},
                checked_at_ms=BASE_TIME_MS + 100 * INTERVAL_MS + 30_000,
            )
            valid_signal = next(
                signal for signal in result.signals
                if signal.candidate.symbol == valid.symbol
            )
            self.assertFalse(valid_signal.passed)
            self.assertTrue(any(
                event.reason == "N12_HISTORICAL_RANK_CONTEXT_INSUFFICIENT"
                for event in valid_signal.analysis.historical_events
            ))

    def test_historical_backfill_is_idempotent_for_stage_and_full_terminal(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self.scheduler(tmpdir)
            item = candidate("N12USDT", 1)
            raw, _ = n12_klines()
            raw.append(kline(100, "105.3", "105.6", "105.1", "105.3"))
            checked = BASE_TIME_MS + 100 * INTERVAL_MS + 30_000
            scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: raw},
                checked_at_ms=checked,
            )
            scheduler.evaluate(
                None, {"quote_volume_top": [item]}, {item.symbol: raw},
                checked_at_ms=checked,
            )
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n12_stage_states"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_structure_terminal_states "
                    "WHERE strategy_id='N12'"
                ).fetchone()[0], 1)

    def test_state_failures_are_closed(self):
        raw, checked = n12_klines()
        item = candidate("N12USDT", 1)
        for mode, expected in (("read", "N12_STATE_READ_FAILED"), ("write", "N12_STATE_PERSIST_FAILED")):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = self.scheduler(tmpdir)
                if mode == "read":
                    recorder.get_n12_stage_state = lambda *args: (_ for _ in ()).throw(RuntimeError("read"))
                else:
                    recorder.record_n12_stage_and_structure_terminal = lambda *args, **kwargs: False
                result = scheduler.evaluate(
                    None, {"quote_volume_top": [item]}, {item.symbol: raw}, checked_at_ms=checked
                )
                self.assertEqual(result.signals[0].reason, expected)

    def test_effectiveness_sample_boundary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = self.scheduler(tmpdir)
            self.assertEqual(
                recorder.strategy_effectiveness_status("N12")["status"],
                "INSUFFICIENT_SAMPLE",
            )
            with recorder._connect() as connection:
                connection.execute(
                    "UPDATE strategy_states SET paper_trade_count=29, win_count=18, "
                    "loss_count=11, win_rate='0.6206896552' WHERE strategy_id='N12'"
                )
            at_29 = recorder.strategy_effectiveness_status("N12")
            self.assertEqual(at_29["closed_samples"], 29)
            self.assertEqual(at_29["status"], "INSUFFICIENT_SAMPLE")
            with recorder._connect() as connection:
                connection.execute(
                    "UPDATE strategy_states SET paper_trade_count=30, win_count=18, "
                    "loss_count=12, win_rate='0.6' WHERE strategy_id='N12'"
                )
            status = recorder.strategy_effectiveness_status("N12")
            self.assertEqual(status["closed_samples"], 30)
            self.assertEqual(status["status"], "READY_FOR_EVALUATION")


class N12MainAndTraderTests(unittest.TestCase):
    def test_main_plan_and_paper_deadline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("n12_main")
            )
            scheduler = StrategyScheduler(
                (N12_STRATEGY,), 96, recorder, logging.getLogger("n12_main")
            )
            raw, checked = n12_klines(elapsed_ms=119_999)
            item = candidate("N12USDT", 1)
            scan = recorder.begin_scan(1, [item], dry_run=True)
            result = scheduler.evaluate(
                scan, {"quote_volume_top": [item]}, {item.symbol: raw}, checked_at_ms=checked
            )
            signal = result.passed_signals[0]
            base_plan = TradePlan(
                symbol=item.symbol, leverage=20, quantity=Decimal("1"),
                entry_price=signal.analysis.structure.entry.close,
                stop_loss_price=Decimal("103"), take_profit_price=Decimal("116"),
                stop_loss_pct=Decimal("0.02"), take_profit_pct=Decimal("0.1"),
                amplitude_24h_pct=Decimal("0"), high_24h_price=Decimal("0"),
                low_24h_price=Decimal("0"), risk_amount=Decimal("1"),
                notional_value=Decimal("100"), required_margin=Decimal("5"),
                balance=Decimal("1000"), stop_mode="relative_strength_pullback_margin_capped",
            )

            class FakeTrader:
                def build_relative_strength_pullback_margin_capped_trade_plan(self, *args, **kwargs):
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
            self.assertEqual(bot.trader.args[0][2], structure.p)
            self.assertEqual(plan.entry_min_price, structure.c.high)
            self.assertEqual(plan.entry_max_price, structure.entry_max_price)
            self.assertEqual(plan.entry_deadline_ms, structure.entry.open_time_ms + 120_000)
            paper = PaperTrader(
                recorder, logging.getLogger("n12_paper"),
                clock_ms=lambda: plan.entry_deadline_ms,
            )
            with self.assertRaises(EntryWindowExpiredError):
                paper.open_trade("N12", item.symbol, None, plan, {})
            self.assertIsNone(recorder.get_open_strategy_paper_trade("N12"))

    def test_plan_stop_boundaries_low_leverage_and_minimums(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                RuleConstrainedClient(leverage=50), test_config(f"{tmpdir}/account.json"),
                StateStore(f"{tmpdir}/state.json"), logging.getLogger("n12_plan"),
            )
            one = trader.build_relative_strength_pullback_margin_capped_trade_plan(
                "N12USDT", Decimal("100"), Decimal("99.5"), Decimal("5"),
                entry_min_price=Decimal("100"), entry_max_price=Decimal("101.5"),
            )
            five = trader.build_relative_strength_pullback_margin_capped_trade_plan(
                "N12USDT", Decimal("100"), Decimal("95.01"), Decimal("5"),
                entry_min_price=Decimal("100"), entry_max_price=Decimal("101.5"),
            )
            self.assertEqual(one.stop_loss_pct, Decimal("0.01"))
            self.assertEqual(five.stop_loss_pct, Decimal("0.05"))
            self.assertGreaterEqual(
                (five.take_profit_price - five.entry_price) / (five.entry_price - five.stop_loss_price),
                Decimal("5"),
            )
            with self.assertRaisesRegex(BinanceAPIError, "N12_STOP_PCT_OUT_OF_RANGE"):
                trader.build_relative_strength_pullback_margin_capped_trade_plan(
                    "N12USDT", Decimal("100"), Decimal("95"), Decimal("5"),
                    entry_min_price=Decimal("100"), entry_max_price=Decimal("101.5"),
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                RuleConstrainedClient(leverage=1), test_config(f"{tmpdir}/account.json"),
                StateStore(f"{tmpdir}/state.json"), logging.getLogger("n12_low_leverage"),
            )
            plan = trader.build_relative_strength_pullback_margin_capped_trade_plan(
                "N12USDT", Decimal("100"), Decimal("99.5"), Decimal("5"),
                entry_min_price=Decimal("100"), entry_max_price=Decimal("101.5"),
            )
            self.assertEqual(plan.quantity, Decimal("9.5"))
            self.assertTrue(plan.risk_capped_by_margin)

    def test_actual_fill_reduction_outside_range_and_protection_cleanup(self):
        entry_min = Decimal("100")
        entry_max = Decimal("101.5")
        allowed_min = entry_min * Decimal("0.995")
        allowed_max = entry_max * Decimal("1.005")
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient({}, leverage=50)
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client, live_test_config(f"{tmpdir}/account.json"), state_store,
                logging.getLogger("n12_live"),
            )
            plan = trader.build_relative_strength_pullback_margin_capped_trade_plan(
                "N12USDT", Decimal("100"), Decimal("99"), Decimal("5"),
                entry_min_price=entry_min, entry_max_price=entry_max,
            )
            trader._assert_structured_actual_entry_range(plan, allowed_min)
            trader._assert_structured_actual_entry_range(plan, allowed_max)
            client.open_response = {"orderId": 1201, "avgPrice": "101", "executedQty": str(plan.quantity)}
            client.position_quantities = [str(plan.quantity), "99.502"]
            excess = plan.quantity - Decimal("99.502")
            client.close_side_effects = [{"status": "FILLED", "executedQty": str(excess)}]
            state = trader.open_long_plan_with_protection(plan)
            self.assertEqual(state.quantity, "99.502")
            self.assertEqual(state.orders["post_fill_adjustment"]["strategy"], "N12")
            self.assertGreaterEqual(
                (Decimal(state.take_profit_price) - Decimal(state.entry_price))
                / (Decimal(state.entry_price) - Decimal(state.stop_loss_price)),
                Decimal("5"),
            )

        for actual_entry in (
            allowed_min - Decimal("0.0001"),
            allowed_max + Decimal("0.0001"),
        ):
            with self.subTest(actual_entry=actual_entry), tempfile.TemporaryDirectory() as tmpdir:
                client = FakeLiveExecutionClient({}, leverage=50)
                state_store = StateStore(f"{tmpdir}/state.json")
                trader = Trader(
                    client, live_test_config(f"{tmpdir}/account.json"), state_store,
                    logging.getLogger("n12_outside"),
                )
                plan = trader.build_relative_strength_pullback_margin_capped_trade_plan(
                    "N12USDT", Decimal("100"), Decimal("99"), Decimal("5"),
                    entry_min_price=entry_min, entry_max_price=entry_max,
                )
                client.open_response = {"orderId": 1202, "avgPrice": str(actual_entry), "executedQty": str(plan.quantity)}
                client.position_quantities = [str(plan.quantity), "0"]
                client.close_side_effects = [{"status": "FILLED", "executedQty": str(plan.quantity)}]
                with self.assertRaisesRegex(BinanceAPIError, "N12_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE"):
                    trader.open_long_plan_with_protection(plan)
                self.assertEqual(client.protection_calls, [])
                stored = state_store.load()
                self.assertIsNotNone(stored)
                self.assertEqual(stored.quantity, "0")
                self.assertEqual(
                    stored.orders["strategy"]["strategy_id"], "N12"
                )
                self.assertTrue(
                    stored.orders["execution_cleanup_resolved"][
                        "protection_cleanup"
                    ]["confirmed_all_canceled"]
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {}, leverage=50,
                protection_side_effects=[BinanceAPIError("N12 stop unavailable")],
            )
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client, live_test_config(f"{tmpdir}/account.json"), state_store,
                logging.getLogger("n12_protection"),
            )
            plan = trader.build_relative_strength_pullback_margin_capped_trade_plan(
                "N12USDT", Decimal("100"), Decimal("99"), Decimal("5"),
                entry_min_price=Decimal("100"), entry_max_price=Decimal("101.5"),
            )
            client.open_response = {"orderId": 1203, "avgPrice": "100", "executedQty": str(plan.quantity)}
            client.position_quantities = [str(plan.quantity), "0"]
            client.close_side_effects = [{"status": "FILLED", "executedQty": str(plan.quantity)}]
            with self.assertRaisesRegex(BinanceAPIError, "N12 stop unavailable"):
                trader.open_long_plan_with_protection(plan)
            stored = state_store.load()
            self.assertIsNotNone(stored)
            self.assertIn("emergency_cleanup_pending", stored.orders)
            self.assertTrue(trader.sync_state_with_exchange().pending_resolved)
            self.assertIsNotNone(state_store.load())


if __name__ == "__main__":
    unittest.main()
