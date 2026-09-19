from copy import deepcopy
from decimal import Decimal
import json
import logging
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from tests.recorder_test_utils import make_test_recorder
from trading_bot.double_break import Pivot, parse_structure_klines
from trading_bot.main import TradingBot
from trading_bot.monitor import FundingCandidate, StrategyMarketScan
from trading_bot.n08_analyzer import (
    RangePivot,
    analyze_n08_range_five_bullish,
    compress_alternating_pivots,
    validate_range_window,
)
from trading_bot.paper_trader import PaperTrader
from trading_bot.recorder import ReviewRecorder
from trading_bot.strategies import (
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
    load_all_strategies,
)
from trading_bot.strategy_scheduler import LiveTradeCandidate, SchedulerResult, StrategyScheduler
from trading_bot.trader import EntryWindowExpiredError, TradePlan


BASE_TIME_MS = 1_700_000_000_000
INTERVAL_MS = 900_000
RANGE_CENTERS = (100, 103, 106, 103, 100, 97, 94, 97)


def candle(index, open_price, high, low, close, open_time=None):
    timestamp = BASE_TIME_MS + index * INTERVAL_MS if open_time is None else open_time
    return [
        timestamp,
        str(open_price),
        str(high),
        str(low),
        str(close),
        "0",
        timestamp + INTERVAL_MS - 1,
    ]


def range_rows(count, start_index=0, price_shift=Decimal("0")):
    rows = []
    for offset in range(count):
        index = start_index + offset
        center = (
            Decimal(str(RANGE_CENTERS[offset % len(RANGE_CENTERS)]))
            + Decimal(str(price_shift))
        )
        rows.append(
            candle(
                index,
                center + Decimal("0.2"),
                center + Decimal("1"),
                center - Decimal("1"),
                center - Decimal("0.2"),
            )
        )
    return rows


def bullish_rows(start_index, closes=None):
    prices = closes or ["100.5", "100.4", "100.6", "100.3", "100.7"]
    rows = []
    for offset, close in enumerate(prices):
        close_price = Decimal(str(close))
        open_price = close_price - Decimal("0.2")
        rows.append(
            candle(
                start_index + offset,
                open_price,
                max(open_price, close_price) + Decimal("0.2"),
                min(open_price, close_price) - Decimal("0.5"),
                close_price,
            )
        )
    return rows


def n08_klines(
    range_bars=20,
    elapsed_seconds="60",
    current_bearish=True,
    streak_closes=None,
):
    raw = range_rows(range_bars)
    closes = streak_closes or [
        Decimal("100.5"),
        Decimal("100.4"),
        Decimal("100.6"),
        Decimal("100.3"),
        Decimal("100.7"),
    ]
    for offset, close in enumerate(closes):
        close = Decimal(str(close))
        open_price = close - Decimal("0.2")
        raw.append(
            candle(
                range_bars + offset,
                open_price,
                max(open_price, close) + Decimal("0.2"),
                min(open_price, close) - Decimal("0.5"),
                close,
            )
        )
    current_index = range_bars + 5
    if current_bearish:
        raw.append(candle(current_index, "101", "101.2", "99.8", "100.2"))
    else:
        raw.append(candle(current_index, "100", "101", "99.8", "100.5"))
    current_open_ms = BASE_TIME_MS + current_index * INTERVAL_MS
    checked_at_ms = current_open_ms + int(Decimal(str(elapsed_seconds)) * Decimal("1000"))
    return raw, checked_at_ms


def n08_candidate(symbol="N08USDT", funding_rate=None, rank=1):
    return FundingCandidate(
        symbol=symbol,
        funding_rate=funding_rate,
        mark_price=Decimal("999"),
        quote_volume=Decimal("1000000"),
        quote_volume_rank=rank,
        candidate_universe="quote_volume_top",
    )


class N08AnalyzerTests(unittest.TestCase):
    def test_twenty_bar_range_and_inside_streak_passes_in_sixth_candle(self):
        raw, checked_at_ms = n08_klines()

        result = analyze_n08_range_five_bullish(
            "N08USDT",
            raw,
            checked_at_ms=checked_at_ms,
        )

        self.assertTrue(result.passed)
        self.assertEqual(result.structure.bars, 20)
        self.assertEqual(result.structure.upper_reference, Decimal("107"))
        self.assertEqual(result.structure.lower_reference, Decimal("93"))
        self.assertEqual(len(result.bullish_streak), 5)
        self.assertTrue(all(candle.high < Decimal("107") for candle in result.bullish_streak))
        self.assertEqual(result.elapsed_seconds, Decimal("60"))

    def test_thirty_six_bar_nine_hour_range_passes(self):
        raw, checked_at_ms = n08_klines(range_bars=36)

        result = analyze_n08_range_five_bullish(
            "N08USDT",
            raw,
            checked_at_ms=checked_at_ms,
        )

        self.assertTrue(result.passed)
        self.assertEqual(result.structure.bars, 36)
        self.assertEqual(result.structure.to_jsonable()["hours"], "9")

    def test_nineteen_range_bars_cannot_be_completed_by_bullish_streak(self):
        raw, checked_at_ms = n08_klines(range_bars=19)

        result = analyze_n08_range_five_bullish(
            "N08USDT",
            raw,
            checked_at_ms=checked_at_ms,
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "NOT_ENOUGH_KLINES")

    def test_pivot_deviation_boundary_is_inclusive_and_excess_is_rejected(self):
        boundary, boundary_checked = n08_klines()
        for index, value in zip((2, 10), ("105.25", "108.75")):
            boundary[index][2] = value
        for index, value in zip((6, 14), ("91.25", "94.75")):
            boundary[index][3] = value

        passed = analyze_n08_range_five_bullish(
            "BOUNDARYUSDT",
            boundary,
            checked_at_ms=boundary_checked,
        )
        exceeded = deepcopy(boundary)
        exceeded[2][2] = "105.24"
        exceeded[10][2] = "108.76"
        exceeded[6][3] = "91.24"
        exceeded[14][3] = "94.76"
        rejected = analyze_n08_range_five_bullish(
            "EXCEEDEDUSDT",
            exceeded,
            checked_at_ms=boundary_checked,
        )

        self.assertTrue(passed.passed)
        self.assertEqual(passed.structure.high_spread, Decimal("3.50"))
        self.assertEqual(passed.structure.low_spread, Decimal("3.50"))
        self.assertFalse(rejected.passed)

    def test_wick_tolerance_boundary_is_inclusive_and_excess_is_rejected(self):
        boundary, checked_at_ms = n08_klines()
        boundary[19][2] = "110.50"
        boundary[19][3] = "89.50"

        passed = analyze_n08_range_five_bullish(
            "BOUNDARYUSDT",
            boundary,
            checked_at_ms=checked_at_ms,
        )
        exceeded = deepcopy(boundary)
        exceeded[19][2] = "110.5001"
        rejected = analyze_n08_range_five_bullish(
            "EXCEEDEDUSDT",
            exceeded,
            checked_at_ms=checked_at_ms,
        )

        self.assertTrue(passed.passed)
        self.assertFalse(rejected.passed)

    def test_too_few_pivots_and_obvious_trend_are_rejected(self):
        flat, checked_at_ms = n08_klines()
        for index in range(20):
            flat[index] = candle(index, "100.2", "101", "99", "99.8")
        trend, trend_checked = n08_klines()
        for index in range(20):
            center = Decimal("90") + Decimal(index)
            trend[index] = candle(index, center, center + 1, center - 1, center - Decimal("0.2"))

        flat_result = analyze_n08_range_five_bullish(
            "FLATUSDT",
            flat,
            checked_at_ms=checked_at_ms,
        )
        trend_result = analyze_n08_range_five_bullish(
            "TRENDUSDT",
            trend,
            checked_at_ms=trend_checked,
        )

        self.assertFalse(flat_result.passed)
        self.assertFalse(trend_result.passed)

    def test_consecutive_same_type_pivots_do_not_count_as_alternating_turns(self):
        pivots = [
            RangePivot("HIGH", 2, "2", Decimal("107")),
            RangePivot("HIGH", 4, "4", Decimal("108")),
            RangePivot("LOW", 10, "10", Decimal("93")),
            RangePivot("LOW", 14, "14", Decimal("92")),
        ]

        compressed = compress_alternating_pivots(pivots)

        self.assertEqual([(pivot.kind, pivot.price) for pivot in compressed], [
            ("HIGH", Decimal("108")),
            ("LOW", Decimal("92")),
        ])

        window = parse_structure_klines(range_rows(20))
        with (
            patch(
                "trading_bot.n08_analyzer.find_pivot_highs",
                return_value=[Pivot(2, Decimal("107"), "2"), Pivot(4, Decimal("108"), "4")],
            ),
            patch(
                "trading_bot.n08_analyzer.find_pivot_lows",
                return_value=[Pivot(10, Decimal("93"), "10"), Pivot(14, Decimal("92"), "14")],
            ),
        ):
            self.assertIsNone(
                validate_range_window("N08USDT", window, 2, 2, Decimal("0.25"))
            )

    def test_bullish_candles_need_not_have_increasing_closes_or_break_upper(self):
        raw, checked_at_ms = n08_klines(
            streak_closes=["100.5", "99.5", "100.2", "98.8", "100.1"],
        )

        result = analyze_n08_range_five_bullish(
            "N08USDT",
            raw,
            checked_at_ms=checked_at_ms,
        )

        self.assertTrue(result.passed)
        self.assertTrue(all(candle.high < result.structure.upper_reference for candle in result.bullish_streak))

    def test_doji_inside_five_candles_resets_streak(self):
        raw, checked_at_ms = n08_klines()
        raw[22][1] = raw[22][4]

        result = analyze_n08_range_five_bullish(
            "N08USDT",
            raw,
            checked_at_ms=checked_at_ms,
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "RANGE_FIVE_BULLISH_NOT_FOUND")

    def test_bearish_sixth_candle_still_passes(self):
        raw, checked_at_ms = n08_klines(current_bearish=True)

        result = analyze_n08_range_five_bullish(
            "N08USDT",
            raw,
            checked_at_ms=checked_at_ms,
        )

        self.assertTrue(result.passed)
        self.assertFalse(result.current_bullish)

    def test_entry_window_boundaries(self):
        for elapsed, expected in (("0", True), ("119.999", True), ("120", False)):
            with self.subTest(elapsed=elapsed):
                raw, checked_at_ms = n08_klines(elapsed_seconds=elapsed)
                result = analyze_n08_range_five_bullish(
                    "N08USDT",
                    raw,
                    checked_at_ms=checked_at_ms,
                )
                self.assertEqual(result.passed, expected)
                if not expected:
                    self.assertEqual(result.reason, "ENTRY_WINDOW_MISSED")

    def test_sixth_candle_period_mismatch_is_rejected(self):
        raw, checked_at_ms = n08_klines()
        raw[-1][0] += INTERVAL_MS

        result = analyze_n08_range_five_bullish(
            "N08USDT",
            raw,
            checked_at_ms=checked_at_ms + INTERVAL_MS,
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "ENTRY_CANDLE_MISMATCH")

    def test_streak_low_below_lower_tolerance_invalidates_structure(self):
        raw, checked_at_ms = n08_klines()
        raw[20][3] = "89.49"

        result = analyze_n08_range_five_bullish(
            "N08USDT",
            raw,
            checked_at_ms=checked_at_ms,
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "BULLISH_STREAK_BROKE_LOWER_TOLERANCE")

    def test_missed_first_streak_does_not_roll_into_later_five(self):
        raw, _ = n08_klines()
        sixth_bullish = candle(25, "100", "101", "99", "100.5")
        current = candle(26, "100", "101", "99", "100.4")
        raw[-1:] = [sixth_bullish, current]
        checked_at_ms = BASE_TIME_MS + 26 * INTERVAL_MS + 30_000

        result = analyze_n08_range_five_bullish(
            "N08USDT",
            raw,
            checked_at_ms=checked_at_ms,
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "ROLLING_BULLISH_STREAK")
        self.assertEqual(result.bullish_streak[0].index, 21)

    def test_six_and_seven_closed_bullish_candles_cannot_roll_forward(self):
        for extra_closed in (1, 2):
            with self.subTest(extra_closed=extra_closed):
                raw, _ = n08_klines()
                raw = raw[:-1]
                raw.extend(
                    bullish_rows(
                        25,
                        [str(Decimal("100.8") + Decimal(offset) / Decimal("10"))
                         for offset in range(extra_closed)],
                    )
                )
                current_index = 25 + extra_closed
                raw.append(candle(current_index, "100.8", "101", "99.8", "100.2"))
                result = analyze_n08_range_five_bullish(
                    "N08USDT",
                    raw,
                    checked_at_ms=BASE_TIME_MS + current_index * INTERVAL_MS + 30_000,
                )

                self.assertFalse(result.passed)
                self.assertEqual(result.reason, "ROLLING_BULLISH_STREAK")

    def test_structure_id_is_stable_when_window_is_shifted(self):
        raw, checked_at_ms = n08_klines(range_bars=96)
        original = analyze_n08_range_five_bullish(
            "N08USDT",
            raw,
            checked_at_ms=checked_at_ms,
        )
        shifted = [
            candle(-1, "100.2", "101", "99", "99.8", BASE_TIME_MS - INTERVAL_MS),
            *deepcopy(raw),
        ]
        shifted_result = analyze_n08_range_five_bullish(
            "N08USDT",
            shifted,
            checked_at_ms=checked_at_ms,
        )

        self.assertTrue(original.passed)
        self.assertTrue(shifted_result.passed)
        self.assertEqual(original.structure_id, shifted_result.structure_id)


class N08SchedulerTests(unittest.TestCase):
    def _scheduler(self, tmpdir):
        recorder = make_test_recorder(
            str(Path(tmpdir) / "review.sqlite3"),
            logging.getLogger("test_n08"),
        )
        recorder.upsert_strategy_definitions((N08_STRATEGY,))
        scheduler = StrategyScheduler(
            (N08_STRATEGY,),
            96,
            recorder,
            logging.getLogger("test_n08"),
        )
        return recorder, scheduler

    def test_n08_configuration_is_loaded(self):
        self.assertIn(N08_STRATEGY, load_all_strategies())
        self.assertEqual(N08_STRATEGY.name, "长时间震荡五连阳提前入场策略")
        self.assertIsNone(N08_STRATEGY.funding_threshold)
        self.assertEqual(N08_STRATEGY.evaluator_type, "range_five_bullish")
        self.assertEqual(N08_STRATEGY.stop_mode, "amplitude_margin_capped")
        self.assertEqual(N08_STRATEGY.range_min_bars, 20)
        self.assertEqual(N08_STRATEGY.range_max_bars, 96)
        self.assertEqual(N08_STRATEGY.range_tolerance_fraction, Decimal("0.25"))
        self.assertEqual(N08_STRATEGY.entry_window_seconds, 120)

    def test_funding_positive_negative_or_missing_does_not_affect_n08(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidates = [
                n08_candidate("POSUSDT", Decimal("0.5"), 1),
                n08_candidate("NEGUSDT", Decimal("-0.5"), 2),
                n08_candidate("NONEUSDT", None, 3),
            ]
            raw, checked_at_ms = n08_klines()
            scan_id = recorder.begin_scan(3, candidates, dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": candidates},
                {candidate.symbol: raw for candidate in candidates},
                checked_at_ms=checked_at_ms,
            )

            self.assertEqual([signal.passed for signal in result.signals], [True, True, True])
            self.assertTrue(all(signal.candidate.mark_price == Decimal("100.2") for signal in result.signals))

    def test_rank_100_is_included_and_rank_101_is_excluded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            included = n08_candidate("R100USDT", None, 100)
            excluded = n08_candidate("R101USDT", None, 101)
            raw, checked_at_ms = n08_klines()
            scan_id = recorder.begin_scan(2, [included, excluded], dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [included, excluded]},
                {included.symbol: raw, excluded.symbol: raw},
                checked_at_ms=checked_at_ms,
            )

            self.assertEqual([signal.candidate.symbol for signal in result.signals], ["R100USDT"])
            self.assertTrue(result.signals[0].passed)

    def test_same_range_structure_is_emitted_only_once_and_detail_is_complete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate(rank=100)
            raw, checked_at_ms = n08_klines(range_bars=36)
            first_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            first = scheduler.evaluate(
                first_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: raw},
                checked_at_ms=checked_at_ms,
            )
            second_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            second = scheduler.evaluate(
                second_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: raw},
                checked_at_ms=checked_at_ms,
            )

            self.assertTrue(first.signals[0].passed)
            self.assertFalse(second.signals[0].passed)
            self.assertEqual(second.signals[0].reason, "N08_STRUCTURE_CONSUMED")
            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT funding_rate, structure_id, detail_json "
                    "FROM strategy_passed_signal_audits "
                    "WHERE source_signal_id = ?",
                    (first.signals[0].signal_id,),
                ).fetchone()
            detail = json.loads(row[2])
            self.assertEqual(row[0], "")
            self.assertEqual(row[1], detail["structure"]["structure_id"])
            self.assertEqual(detail["structure"]["bars"], 36)
            self.assertEqual(detail["structure"]["hours"], "9")
            self.assertEqual(detail["structure_id"], row[1])
            self.assertEqual(len(detail["bullish_streak"]), 5)
            self.assertEqual(detail["entry_window_seconds"], 120)
            self.assertEqual(detail["sixth_candle_open_time"], detail["current_open_time"])
            self.assertEqual(detail["quote_volume_rank"], 100)

    def test_missed_same_box_stays_consumed_after_scheduler_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate()
            first_raw, first_checked = n08_klines(elapsed_seconds="120")
            first_scan = recorder.begin_scan(1, [candidate], dry_run=True)

            first = scheduler.evaluate(
                first_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: first_raw},
                checked_at_ms=first_checked,
            )

            self.assertFalse(first.signals[0].passed)
            self.assertEqual(first.signals[0].reason, "ENTRY_WINDOW_MISSED")
            first_structure_id = first.signals[0].analysis.structure_id

            later_raw = deepcopy(first_raw)
            later_raw.extend(range_rows(20, start_index=26))
            later_raw.extend(bullish_rows(46))
            later_raw.append(candle(51, "101", "101.2", "99.8", "100.2"))
            later_checked = BASE_TIME_MS + 51 * INTERVAL_MS + 30_000
            later_analysis = analyze_n08_range_five_bullish(
                candidate.symbol,
                later_raw,
                checked_at_ms=later_checked,
            )
            self.assertTrue(later_analysis.passed)
            self.assertNotEqual(later_analysis.structure_id, first_structure_id)

            second_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            second = scheduler.evaluate(
                second_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: later_raw},
                checked_at_ms=later_checked,
            )
            self.assertFalse(second.signals[0].passed)
            self.assertEqual(second.signals[0].reason, "N08_STRUCTURE_CONSUMED")

            restarted_recorder = make_test_recorder(str(db_file), logging.getLogger("test_n08_restart"))
            restarted_scheduler = StrategyScheduler(
                (N08_STRATEGY,),
                96,
                restarted_recorder,
                logging.getLogger("test_n08_restart"),
            )
            third_scan = restarted_recorder.begin_scan(1, [candidate], dry_run=True)
            third = restarted_scheduler.evaluate(
                third_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: later_raw},
                checked_at_ms=later_checked,
            )

            self.assertFalse(third.signals[0].passed)
            self.assertEqual(third.signals[0].reason, "N08_STRUCTURE_CONSUMED")
            active = restarted_recorder.get_active_n08_structure_states("N08", candidate.symbol)
            self.assertEqual(len(active), 1)
            self.assertEqual(active[0].structure_id, first_structure_id)

    def test_explicit_boundary_retires_old_box_and_new_box_passes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate()
            old_raw, old_checked = n08_klines(elapsed_seconds="120")
            first_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            old_result = scheduler.evaluate(
                first_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: old_raw},
                checked_at_ms=old_checked,
            )
            old_structure_id = old_result.signals[0].analysis.structure_id
            self.assertEqual(old_result.signals[0].reason, "ENTRY_WINDOW_MISSED")

            combined = deepcopy(old_raw)
            combined.append(candle(26, "100", "112", "99", "100"))
            combined.extend(range_rows(20, start_index=27))
            combined.extend(bullish_rows(47))
            combined.append(candle(52, "101", "101.2", "99.8", "100.2"))
            checked_at_ms = BASE_TIME_MS + 52 * INTERVAL_MS + 30_000
            direct = analyze_n08_range_five_bullish(
                candidate.symbol,
                combined,
                checked_at_ms=checked_at_ms,
            )
            self.assertTrue(direct.passed)
            self.assertEqual(direct.structure.start_index, 27)
            self.assertNotEqual(direct.structure_id, old_structure_id)

            second_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            new_result = scheduler.evaluate(
                second_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: combined},
                checked_at_ms=checked_at_ms,
            )

            self.assertTrue(new_result.signals[0].passed)
            self.assertEqual(new_result.signals[0].analysis.structure_id, direct.structure_id)
            with recorder._connect() as connection:
                states = connection.execute(
                    "SELECT structure_id, status, reset_open_time "
                    "FROM n08_structure_states ORDER BY id"
                ).fetchall()
            self.assertEqual(states[0], (old_structure_id, "RETIRED", str(BASE_TIME_MS + 26 * INTERVAL_MS)))
            self.assertEqual(states[1][0:2], (direct.structure_id, "CONSUMED"))

    def test_boundary_on_current_sixth_cannot_release_its_own_old_box_candidate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate()
            old_raw, old_checked = n08_klines(elapsed_seconds="120")
            first_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            scheduler.evaluate(
                first_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: old_raw},
                checked_at_ms=old_checked,
            )

            later_raw = deepcopy(old_raw)
            later_raw.extend(range_rows(20, start_index=26))
            later_raw.extend(bullish_rows(46))
            later_raw.append(candle(51, "101", "112", "99.8", "100.2"))
            checked_at_ms = BASE_TIME_MS + 51 * INTERVAL_MS + 30_000
            for _ in range(2):
                scan_id = recorder.begin_scan(1, [candidate], dry_run=True)
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": [candidate]},
                    {candidate.symbol: later_raw},
                    checked_at_ms=checked_at_ms,
                )
                self.assertFalse(result.signals[0].passed)
                self.assertEqual(result.signals[0].reason, "N08_STRUCTURE_CONSUMED")

            with recorder._connect() as connection:
                state = connection.execute(
                    "SELECT status, reset_open_time FROM n08_structure_states ORDER BY id LIMIT 1"
                ).fetchone()
            self.assertEqual(state, ("CONSUMED", str(BASE_TIME_MS + 51 * INTERVAL_MS)))

    def test_first_start_backfills_offline_miss_and_rejects_same_box_second_streak(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate()
            history, _ = n08_klines()
            history.extend(range_rows(20, start_index=26))
            history.extend(bullish_rows(46))
            history.append(candle(51, "101", "101.2", "99.8", "100.2"))
            checked_at_ms = BASE_TIME_MS + 51 * INTERVAL_MS + 30_000
            scan_id = recorder.begin_scan(1, [candidate], dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: history},
                checked_at_ms=checked_at_ms,
            )

            self.assertFalse(result.signals[0].passed)
            self.assertEqual(
                result.signals[0].reason,
                "HISTORICAL_N08_STRUCTURE_MISSED",
            )
            with recorder._connect() as connection:
                states = connection.execute(
                    "SELECT status, reason, first_streak_start_time "
                    "FROM n08_structure_states ORDER BY id"
                ).fetchall()
            self.assertEqual(
                states,
                [
                    (
                        "CONSUMED",
                        "HISTORICAL_N08_STRUCTURE_MISSED",
                        str(BASE_TIME_MS + 20 * INTERVAL_MS),
                    )
                ],
            )

    def test_historical_backfill_is_idempotent_after_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate()
            history, _ = n08_klines()
            history.extend(range_rows(20, start_index=26))
            history.extend(bullish_rows(46))
            history.append(candle(51, "101", "101.2", "99.8", "100.2"))
            checked_at_ms = BASE_TIME_MS + 51 * INTERVAL_MS + 30_000
            first_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            scheduler.evaluate(
                first_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: history},
                checked_at_ms=checked_at_ms,
            )

            restarted_recorder = make_test_recorder(str(db_file), logging.getLogger("n08_history_restart"))
            restarted_scheduler = StrategyScheduler(
                (N08_STRATEGY,),
                96,
                restarted_recorder,
                logging.getLogger("n08_history_restart"),
            )
            second_scan = restarted_recorder.begin_scan(1, [candidate], dry_run=True)
            result = restarted_scheduler.evaluate(
                second_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: history},
                checked_at_ms=checked_at_ms,
            )

            self.assertFalse(result.signals[0].passed)
            self.assertEqual(result.signals[0].reason, "HISTORICAL_N08_STRUCTURE_MISSED")
            with restarted_recorder._connect() as connection:
                state_count = connection.execute(
                    "SELECT COUNT(*) FROM n08_structure_states"
                ).fetchone()[0]
                consumed_events = connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type = 'n08_structure_consumed'"
                ).fetchone()[0]
            self.assertEqual(state_count, 1)
            self.assertEqual(consumed_events, 1)

    def test_offline_old_event_then_boundary_and_new_range_passes_on_first_start(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate()
            history, _ = n08_klines()
            history.append(candle(26, "100", "112", "99", "100"))
            history.extend(range_rows(20, start_index=27))
            history.extend(bullish_rows(47))
            history.append(candle(52, "101", "101.2", "99.8", "100.2"))
            checked_at_ms = BASE_TIME_MS + 52 * INTERVAL_MS + 30_000
            scan_id = recorder.begin_scan(1, [candidate], dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: history},
                checked_at_ms=checked_at_ms,
            )

            self.assertTrue(result.signals[0].passed)
            self.assertEqual(result.signals[0].analysis.structure.start_index, 27)
            with recorder._connect() as connection:
                states = connection.execute(
                    "SELECT status, reason FROM n08_structure_states ORDER BY id"
                ).fetchall()
            self.assertEqual(states[0], ("RETIRED", "NEW_RANGE_AFTER_TOLERANCE_RESET"))
            self.assertEqual(states[1], ("CONSUMED", "PASSED"))

    def test_historical_backfill_does_not_bind_current_to_old_independent_range(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate()
            history, _ = n08_klines()
            history.append(candle(26, "100", "112", "88", "100"))
            history.extend(range_rows(20, start_index=27, price_shift=Decimal("30")))
            history.extend(
                bullish_rows(47, ["130.5", "130.4", "130.6", "130.3", "130.7"])
            )
            history.append(candle(52, "131", "131.2", "129.8", "130.2"))
            checked_at_ms = BASE_TIME_MS + 52 * INTERVAL_MS + 30_000
            scan_id = recorder.begin_scan(1, [candidate], dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: history},
                checked_at_ms=checked_at_ms,
            )

            self.assertTrue(result.signals[0].passed)
            self.assertEqual(result.signals[0].analysis.structure.start_index, 27)
            self.assertEqual(result.signals[0].analysis.structure.upper_reference, Decimal("137"))

    def test_full_102_window_without_prior_context_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate()
            raw, checked_at_ms = n08_klines(range_bars=96)
            scan_id = recorder.begin_scan(1, [candidate], dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: raw},
                checked_at_ms=checked_at_ms,
            )

            self.assertFalse(result.signals[0].passed)
            self.assertEqual(result.signals[0].reason, "HISTORICAL_N08_CONTEXT_INCOMPLETE")

    def test_continuous_prior_observation_allows_full_96_bar_range(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate()
            prior = range_rows(20, start_index=-20)
            prior_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            scheduler.evaluate(
                prior_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: prior},
                checked_at_ms=BASE_TIME_MS - INTERVAL_MS + 30_000,
            )
            raw, checked_at_ms = n08_klines(range_bars=96)
            current_scan = recorder.begin_scan(1, [candidate], dry_run=True)

            result = scheduler.evaluate(
                current_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: raw},
                checked_at_ms=checked_at_ms,
            )

            self.assertTrue(result.signals[0].passed)
            self.assertEqual(result.signals[0].analysis.structure.bars, 96)
            coverage = recorder.get_n08_history_coverage("N08", candidate.symbol)
            self.assertEqual(
                coverage.continuous_from_open_time,
                str(BASE_TIME_MS - 20 * INTERVAL_MS),
            )

    def test_continuous_coverage_survives_restart_for_96_bar_range(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = Path(tmpdir) / "review.sqlite3"
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate()
            prior = range_rows(20, start_index=-20)
            prior_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            scheduler.evaluate(
                prior_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: prior},
                checked_at_ms=BASE_TIME_MS - INTERVAL_MS + 30_000,
            )

            restarted_recorder = make_test_recorder(str(db_file), logging.getLogger("n08_coverage_restart"))
            restarted_scheduler = StrategyScheduler(
                (N08_STRATEGY,),
                96,
                restarted_recorder,
                logging.getLogger("n08_coverage_restart"),
            )
            raw, checked_at_ms = n08_klines(range_bars=96)
            scan_id = restarted_recorder.begin_scan(1, [candidate], dry_run=True)
            result = restarted_scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: raw},
                checked_at_ms=checked_at_ms,
            )

            self.assertTrue(result.signals[0].passed)
            self.assertEqual(result.signals[0].analysis.structure.bars, 96)

    def test_unreplayable_coverage_gap_keeps_full_96_bar_range_blocked(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate()
            stale_prior = range_rows(20, start_index=-200)
            prior_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            scheduler.evaluate(
                prior_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: stale_prior},
                checked_at_ms=BASE_TIME_MS - 181 * INTERVAL_MS + 30_000,
            )
            raw, checked_at_ms = n08_klines(range_bars=96)
            current_scan = recorder.begin_scan(1, [candidate], dry_run=True)

            result = scheduler.evaluate(
                current_scan,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: raw},
                checked_at_ms=checked_at_ms,
            )

            self.assertFalse(result.signals[0].passed)
            self.assertEqual(result.signals[0].reason, "HISTORICAL_N08_CONTEXT_INCOMPLETE")
            coverage = recorder.get_n08_history_coverage("N08", candidate.symbol)
            self.assertEqual(coverage.continuous_from_open_time, str(BASE_TIME_MS))
            self.assertEqual(
                coverage.last_gap_from_open_time,
                str(BASE_TIME_MS - 181 * INTERVAL_MS),
            )
            with recorder._connect() as connection:
                gap_events = connection.execute(
                    "SELECT COUNT(*) FROM events WHERE event_type = 'n08_history_coverage_gap'"
                ).fetchone()[0]
            self.assertEqual(gap_events, 1)

    def test_fresh_95_bar_range_keeps_existing_scheduler_behavior(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._scheduler(tmpdir)
            candidate = n08_candidate()
            raw, checked_at_ms = n08_klines(range_bars=95)
            scan_id = recorder.begin_scan(1, [candidate], dry_run=True)

            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: raw},
                checked_at_ms=checked_at_ms,
            )

            self.assertTrue(result.signals[0].passed)
            self.assertEqual(result.signals[0].analysis.structure.bars, 95)


class N08SharedMarketDataTests(unittest.TestCase):
    def test_n08_deadline_expiry_blocks_paper_and_live_and_updates_signal(self):
        for path in ("PAPER", "LIVE"):
            with self.subTest(path=path), tempfile.TemporaryDirectory() as tmpdir:
                candidate = n08_candidate()
                raw, checked_at_ms = n08_klines(range_bars=36, elapsed_seconds="119")
                recorder = make_test_recorder(
                    str(Path(tmpdir) / "review.sqlite3"),
                    logging.getLogger(f"test_n08_deadline_{path.lower()}"),
                )
                recorder.upsert_strategy_definitions((N08_STRATEGY,))
                scheduler = StrategyScheduler(
                    (N08_STRATEGY,),
                    96,
                    recorder,
                    logging.getLogger(f"test_n08_deadline_{path.lower()}"),
                )
                scan_id = recorder.begin_scan(1, [candidate], dry_run=True)
                evaluated = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": [candidate]},
                    {candidate.symbol: raw},
                    checked_at_ms=checked_at_ms,
                )
                signal = evaluated.passed_signals[0]
                deadline_ms = int(Decimal(signal.analysis.current_open_time)) + 120_000
                base_plan = TradePlan(
                    symbol=candidate.symbol,
                    leverage=5,
                    quantity=Decimal("47.4"),
                    entry_price=Decimal("100.2"),
                    stop_loss_price=Decimal("99.19"),
                    take_profit_price=Decimal("105.25"),
                    stop_loss_pct=Decimal("0.01"),
                    take_profit_pct=Decimal("5.05") / Decimal("100.2"),
                    amplitude_24h_pct=Decimal("0.005"),
                    high_24h_price=Decimal("100.5"),
                    low_24h_price=Decimal("100"),
                    risk_amount=Decimal("200"),
                    notional_value=Decimal("4749.48"),
                    required_margin=Decimal("949.896"),
                    balance=Decimal("1000"),
                    stop_mode="amplitude_margin_capped",
                    risk_reward_ratio=Decimal("5"),
                    structure_id=signal.analysis.structure_id,
                    target_risk_amount=Decimal("200"),
                    actual_risk_amount=Decimal("47.874"),
                    risk_capped_by_margin=True,
                )

                class FakeClient:
                    def get_klines(self, symbol):
                        return raw

                    def get_klines_for_interval(self, *args, **kwargs):
                        return []

                    def get_aggregate_trades(self, *args, **kwargs):
                        return []

                class ExpiringTrader:
                    def __init__(self):
                        self.received_plan = None

                    def close_dry_run_position_if_triggered(self):
                        return None

                    def sync_state_with_exchange(self):
                        return SimpleNamespace(has_position=False, closed_state=None)

                    def build_amplitude_margin_capped_trade_plan(self, *args, **kwargs):
                        return base_plan

                    def open_long_plan_with_protection(self, trade_plan):
                        self.received_plan = trade_plan
                        raise EntryWindowExpiredError(trade_plan, deadline_ms)

                class FakeMonitor:
                    def scan_for_strategies(self, volume_top_n):
                        return StrategyMarketScan(1, [], [candidate])

                live_candidates = []
                if path == "LIVE":
                    with recorder._connect() as connection:
                        connection.execute(
                            "UPDATE strategy_states SET consecutive_wins = 2, "
                            "paper_trade_count = 2, win_count = 2, win_rate = '1', "
                            "live_eligible = 1 WHERE strategy_id = 'N08'"
                        )
                    live_candidates = [
                        LiveTradeCandidate(signal, recorder.get_strategy_state("N08"))
                    ]

                class FakeScheduler:
                    def evaluate(self, *args, **kwargs):
                        return SchedulerResult(
                            [signal], [signal], live_candidates, True
                        )

                    def choose_live_candidate(self, candidates, live_blocked):
                        return None if live_blocked or not candidates else candidates[0]

                bot = TradingBot.__new__(TradingBot)
                bot.config = SimpleNamespace(dry_run=True)
                bot.logger = logging.getLogger(f"test_n08_deadline_{path.lower()}")
                bot.client = FakeClient()
                bot.recorder = recorder
                bot.paper_trader = PaperTrader(
                    recorder,
                    bot.logger,
                    clock_ms=lambda: deadline_ms,
                )
                bot.monitor = FakeMonitor()
                bot.trader = ExpiringTrader()
                bot.strategy_scheduler = FakeScheduler()
                bot._signal_batch_is_current = lambda scan_id: True
                recorder.assert_n16_current_scan_claims = (
                    lambda scan_id, claims: None
                    if type(scan_id) is int and scan_id > 0 and claims == ()
                    else (_ for _ in ()).throw(
                        AssertionError("unexpected N16 current claim")
                    )
                )
                bot.strategies = (N08_STRATEGY,)
                bot.state = SimpleNamespace(
                    load=lambda: None,
                    save=lambda state: None,
                    clear=lambda: None,
                )

                bot._run_once_multi_strategy()

                with recorder._connect() as connection:
                    signal_row = connection.execute(
                        "SELECT decision, reason, detail_json FROM strategy_signals WHERE id = ?",
                        (signal.signal_id,),
                    ).fetchone()
                    paper_count = connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades"
                    ).fetchone()[0]
                    live_count = connection.execute(
                        "SELECT COUNT(*) FROM strategy_live_links"
                    ).fetchone()[0]
                detail = json.loads(signal_row[2])
                if path == "LIVE":
                    self.assertEqual(signal_row[0:2], (
                        "ENTRY_WINDOW_EXPIRED",
                        "ENTRY_WINDOW_EXPIRED_BEFORE_ORDER",
                    ))
                    self.assertEqual(detail["entry_window_path"], path)
                    self.assertEqual(detail["entry_deadline_ms"], deadline_ms)
                    self.assertIsNotNone(bot.trader.received_plan)
                else:
                    self.assertEqual(signal_row[0:2], ("PASSED", "PASSED"))
                    self.assertNotIn("entry_window_path", detail)
                    self.assertEqual(detail["entry_deadline_ms"], deadline_ms)
                    self.assertIsNone(bot.trader.received_plan)
                self.assertEqual(paper_count, 0)
                self.assertEqual(live_count, 0)

    def test_n08_paper_open_succeeds_one_millisecond_before_deadline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            deadline_ms = 1_700_000_120_000
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_n08_paper_before_deadline"),
            )
            plan = TradePlan(
                symbol="N08USDT",
                leverage=5,
                quantity=Decimal("10"),
                entry_price=Decimal("100"),
                stop_loss_price=Decimal("99"),
                take_profit_price=Decimal("105"),
                stop_loss_pct=Decimal("0.01"),
                take_profit_pct=Decimal("0.05"),
                amplitude_24h_pct=Decimal("0.01"),
                high_24h_price=Decimal("101"),
                low_24h_price=Decimal("100"),
                risk_amount=Decimal("10"),
                notional_value=Decimal("1000"),
                required_margin=Decimal("200"),
                balance=Decimal("1000"),
                stop_mode="amplitude_margin_capped",
                entry_candle_open_time_ms=deadline_ms - 120_000,
                entry_deadline_ms=deadline_ms,
            )
            paper = PaperTrader(
                recorder,
                logging.getLogger("test_n08_paper_before_deadline"),
                clock_ms=lambda: deadline_ms - 1,
            )

            trade_id = paper.open_trade("N08", "N08USDT", None, plan, {})

            self.assertIsNotNone(trade_id)
            self.assertIsNotNone(recorder.get_open_strategy_paper_trade("N08"))

    def test_main_records_n08_structure_amplitude_and_risk_detail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            candidate = n08_candidate()
            raw, checked_at_ms = n08_klines(range_bars=36)
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_n08_main_record"),
            )
            scheduler = StrategyScheduler(
                (N08_STRATEGY,),
                96,
                recorder,
                logging.getLogger("test_n08_main_record"),
            )
            scan_id = recorder.begin_scan(1, [candidate], dry_run=True)
            evaluated = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate]},
                {candidate.symbol: raw},
                checked_at_ms=checked_at_ms,
            )
            signal = evaluated.passed_signals[0]
            plan = TradePlan(
                symbol=candidate.symbol,
                leverage=5,
                quantity=Decimal("47.4"),
                entry_price=Decimal("100.2"),
                stop_loss_price=Decimal("99.19"),
                take_profit_price=Decimal("105.25"),
                stop_loss_pct=Decimal("0.01"),
                take_profit_pct=Decimal("5.05") / Decimal("100.2"),
                amplitude_24h_pct=Decimal("0.005"),
                high_24h_price=Decimal("100.5"),
                low_24h_price=Decimal("100"),
                risk_amount=Decimal("200"),
                notional_value=Decimal("4749.48"),
                required_margin=Decimal("949.896"),
                balance=Decimal("1000"),
                stop_mode="amplitude_margin_capped",
                risk_reward_ratio=Decimal("5"),
                structure_id=signal.analysis.structure_id,
                target_risk_amount=Decimal("200"),
                actual_risk_amount=Decimal("47.874"),
                risk_capped_by_margin=True,
                pretrade_quantity=Decimal("47.4"),
            )

            class FakeClient:
                def get_klines(self, symbol):
                    return raw

            class FakeTrader:
                def close_dry_run_position_if_triggered(self):
                    return None

                def sync_state_with_exchange(self):
                    return SimpleNamespace(has_position=False, closed_state=None)

                def build_amplitude_margin_capped_trade_plan(self, *args, **kwargs):
                    return plan

            class FakePaperTrader:
                def close_triggered_open_trades(self, *args):
                    return []

                def open_trade(self, *args):
                    return 1

            class FakeMonitor:
                def scan_for_strategies(self, volume_top_n):
                    return StrategyMarketScan(1, [], [candidate])

            class FakeScheduler:
                def evaluate(self, *args, **kwargs):
                    return SchedulerResult([signal], [signal], [], True)

                def choose_live_candidate(self, live_candidates, live_blocked):
                    return None

            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=True)
            bot.logger = logging.getLogger("test_n08_main_record")
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
            bot.strategies = (N08_STRATEGY,)
            bot.state = SimpleNamespace(
                load=lambda: None,
                save=lambda state: None,
                clear=lambda: None,
            )

            bot._run_once_multi_strategy()

            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT detail_json FROM strategy_signals WHERE id = ?",
                    (signal.signal_id,),
                ).fetchone()
            detail = json.loads(row[0])
            self.assertEqual(detail["structure"]["bars"], 36)
            self.assertEqual(detail["amplitude_24h_pct"], "0.005")
            self.assertEqual(detail["high_24h_price"], "100.5")
            self.assertEqual(detail["low_24h_price"], "100")
            self.assertEqual(detail["stop_loss_pct"], "0.01")
            self.assertEqual(detail["target_risk_amount"], "200")
            self.assertTrue(detail["risk_capped_by_margin"])

    def test_n06_through_n15_share_one_volume_candidate_and_kline_response(self):
        candidate = n08_candidate()
        raw, checked_at_ms = n08_klines()

        class FakeClient:
            def __init__(self):
                self.kline_calls = []

            def get_klines(self, symbol):
                self.kline_calls.append(symbol)
                return raw

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

            def get_pending_n15_snapshot_symbols(self, strategy_id):
                return set()

            def get_required_n16_episode_symbols(self, strategy_id):
                return ()

        class FakeMonitor:
            def scan_for_strategies(self, volume_top_n):
                return StrategyMarketScan(1, [], [candidate])

        class InspectingScheduler:
            def __init__(self):
                self.raw_identity = None

            def evaluate(self, scan_id, candidate_groups, raw_by_symbol, checked_at_ms=None):
                self.raw_identity = raw_by_symbol[candidate.symbol]
                result = analyze_n08_range_five_bullish(
                    candidate.symbol,
                    self.raw_identity,
                    checked_at_ms=int(self.raw_identity[-1][0]) + 30000,
                )
                if not result.passed:
                    raise AssertionError(result.reason)
                return SchedulerResult([], [], [])

            def choose_live_candidate(self, live_candidates, live_blocked):
                return None

        bot = TradingBot.__new__(TradingBot)
        bot.config = SimpleNamespace(dry_run=True)
        bot.logger = logging.getLogger("test_n08_shared_market")
        bot.client = FakeClient()
        bot.trader = FakeTrader()
        bot.paper_trader = FakePaperTrader()
        bot.recorder = FakeRecorder()
        bot.monitor = FakeMonitor()
        bot.strategy_scheduler = InspectingScheduler()
        bot.state = SimpleNamespace(
            load=lambda: None,
            save=lambda state: None,
            clear=lambda: None,
        )
        bot.strategies = (
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
        )

        bot._run_once_multi_strategy()

        self.assertEqual(bot.client.kline_calls, [candidate.symbol])
        self.assertIs(bot.strategy_scheduler.raw_identity, raw)


if __name__ == "__main__":
    unittest.main()
