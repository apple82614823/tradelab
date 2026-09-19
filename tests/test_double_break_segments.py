from __future__ import annotations

from decimal import Decimal
import unittest

from trading_bot.double_break import (
    SwingSegmentConfig,
    evaluate_swing_segment,
    extract_double_break_skeletons,
    parse_structure_klines,
)
from trading_bot.n06_analyzer import analyze_n06_double_break_pullback
from trading_bot.n07_analyzer import analyze_n07_p1_retest

from tests.swing_fixtures import (
    INTERVAL_MS,
    build_beatusdt_continued_rise,
    build_valid_swing_klines,
)


def segment_rows(closes: list[str]) -> list[list[str | int]]:
    rows = []
    for index, close_text in enumerate(closes):
        close = Decimal(close_text)
        open_price = close + (Decimal("0.08") if index % 2 else Decimal("-0.08"))
        rows.append(
            [
                index * INTERVAL_MS,
                str(open_price),
                str(max(open_price, close) + Decimal("0.1")),
                str(min(open_price, close) - Decimal("0.1")),
                str(close),
                "0",
                index * INTERVAL_MS + INTERVAL_MS - 1,
            ]
        )
    return rows


class SwingSegmentMetricTests(unittest.TestCase):
    def test_segment_parameters_are_centralized_and_validated(self):
        config = SwingSegmentConfig()
        self.assertEqual(config.to_jsonable(), {
            "min_bars": 5,
            "atr_period": 14,
            "min_atr_multiple": "1.2",
            "min_efficiency": "0.35",
        })
        with self.assertRaises(ValueError):
            SwingSegmentConfig(min_bars=0)

    def test_four_bar_endpoint_distance_rejects_and_five_passes(self):
        candles = parse_structure_klines(
            segment_rows(["100", "100.8", "101.6", "102.4", "103.2", "104"])
        )
        config = SwingSegmentConfig()
        four = evaluate_swing_segment(candles, 0, 4, "UP", config)
        five = evaluate_swing_segment(candles, 0, 5, "UP", config)
        self.assertFalse(four.passed)
        self.assertEqual(four.reason, "SEGMENT_TOO_SHORT")
        self.assertTrue(five.passed)
        self.assertEqual(five.bar_distance, 5)

    def test_alternating_flat_path_fails_efficiency_or_displacement(self):
        candles = parse_structure_klines(
            segment_rows(["100", "101", "99.9", "101.1", "100", "100.4"])
        )
        result = evaluate_swing_segment(candles, 0, 5, "UP", SwingSegmentConfig())
        self.assertFalse(result.passed)
        self.assertIn(result.reason, {"SEGMENT_DISPLACEMENT_TOO_SMALL", "SEGMENT_EFFICIENCY_TOO_LOW"})

    def test_mixed_red_green_path_with_direction_displacement_and_er_passes(self):
        candles = parse_structure_klines(
            segment_rows(["100", "101.2", "100.9", "102.4", "102.1", "104"])
        )
        result = evaluate_swing_segment(candles, 0, 5, "UP", SwingSegmentConfig())
        self.assertTrue(result.passed)
        self.assertGreater(result.slope, 0)
        self.assertGreaterEqual(result.efficiency, Decimal("0.35"))

    def test_regression_slope_cannot_override_opposite_endpoint_direction(self):
        positive_slope_end_down = parse_structure_klines(
            segment_rows(["100", "90", "91", "92", "93", "99"])
        )
        negative_slope_end_up = parse_structure_klines(
            segment_rows(["100", "110", "109", "108", "107", "101"])
        )
        up = evaluate_swing_segment(
            positive_slope_end_down, 0, 5, "UP", SwingSegmentConfig()
        )
        down = evaluate_swing_segment(
            negative_slope_end_up, 0, 5, "DOWN", SwingSegmentConfig()
        )
        self.assertGreater(up.slope, 0)
        self.assertLess(down.slope, 0)
        self.assertEqual(up.reason, "SEGMENT_ENDPOINT_DIRECTION_MISMATCH")
        self.assertEqual(down.reason, "SEGMENT_ENDPOINT_DIRECTION_MISMATCH")


class DoubleBreakHumanLegTests(unittest.TestCase):
    @staticmethod
    def _p1_retest_klines(retest_low: str) -> list[list[str | int]]:
        raw = build_valid_swing_klines()[:38]
        closes = (
            "157", "155", "153", "150", "147", "149", "151", "152",
            "150", "149", "148", "151", "154", "157", "159", "160", "161",
        )
        for offset, close_text in enumerate(closes, start=38):
            close = Decimal(close_text)
            open_price = close + (Decimal("0.2") if offset % 2 else Decimal("-0.2"))
            high = max(open_price, close) + Decimal("0.5")
            low = min(open_price, close) - Decimal("0.5")
            if offset == 42:
                low = Decimal("140")
            elif offset == 48:
                low = Decimal(retest_low)
            raw.append([
                offset * INTERVAL_MS,
                str(open_price),
                str(high),
                str(low),
                str(close),
                "0",
                offset * INTERVAL_MS + INTERVAL_MS - 1,
            ])
        return raw

    def test_higher_local_low_after_true_p1_cannot_create_second_skeleton(self):
        raw = self._p1_retest_klines("145")
        skeletons = extract_double_break_skeletons(
            "HIGHERP1USDT", parse_structure_klines(raw), 2, 2
        )
        matching = [item for item in skeletons if item.h1_index == 37]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0].p1, Decimal("140"))
        self.assertEqual(matching[0].p1_index, 42)
        self.assertNotIn(Decimal("145"), [item.p1 for item in matching])

    def test_equal_p1_retest_keeps_first_low_time_and_one_structure_id(self):
        raw = self._p1_retest_klines("140")
        skeletons = extract_double_break_skeletons(
            "EQUALP1USDT", parse_structure_klines(raw), 2, 2
        )
        matching = [item for item in skeletons if item.h1_index == 37]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0].p1, Decimal("140"))
        self.assertEqual(matching[0].p1_index, 42)
        self.assertEqual(matching[0].p1_time, str(42 * INTERVAL_MS))
        self.assertEqual(len({item.structure_id for item in matching}), 1)

    def test_higher_high_before_p1_replaces_old_h1_and_old_break_is_insufficient(self):
        raw = build_valid_swing_klines()
        raw[39][2] = "165"
        for index in range(42, len(raw) - 1):
            raw[index][4] = str(min(Decimal(raw[index][4]), Decimal("165")))
        skeletons = extract_double_break_skeletons(
            "FINALH1USDT", parse_structure_klines(raw[:-1]), 2, 2
        )
        self.assertEqual(skeletons, [])

    def test_lower_low_before_second_break_replaces_old_p1(self):
        raw = build_valid_swing_klines()
        raw[45][1:5] = ["142", "143", "138", "140"]
        raw[49][4] = "159"
        raw[50][4] = "160"
        raw[51][4] = "161"
        skeletons = extract_double_break_skeletons(
            "FINALP1USDT", parse_structure_klines(raw[:-1]), 2, 2
        )
        self.assertTrue(skeletons)
        structure = skeletons[-1]
        self.assertEqual(structure.p1, Decimal("138"))
        self.assertEqual(structure.p1_index, 45)
        self.assertEqual(structure.second_break_index, 51)
    def test_h1_to_p1_one_and_four_bars_reject_exact_five_passes(self):
        for bars in (1, 4):
            with self.subTest(bars=bars):
                raw = build_valid_swing_klines(pullback_bars=bars)
                skeletons = extract_double_break_skeletons(
                    "SHORTUSDT", parse_structure_klines(raw[:-1]), 2, 2
                )
                self.assertEqual(skeletons, [])
        valid = build_valid_swing_klines(pullback_bars=5)
        skeletons = extract_double_break_skeletons(
            "BOUNDARYUSDT", parse_structure_klines(valid[:-1]), 2, 2
        )
        self.assertTrue(skeletons)
        self.assertEqual(skeletons[-1].p1_index - skeletons[-1].h1_index, 5)
        self.assertEqual(len(skeletons[-1].prior_downtrend.highs), 3)
        self.assertEqual(len(skeletons[-1].prior_downtrend.lows), 3)
        self.assertTrue(all(item.passed for item in skeletons[-1].prior_downtrend.segments))

    def test_prior_downtrend_requires_three_descending_highs_and_lows(self):
        raw = build_valid_swing_klines()
        raw[12][2] = "161"
        skeletons = extract_double_break_skeletons(
            "BADPRIORUSDT", parse_structure_klines(raw[:-1]), 2, 2
        )
        self.assertEqual(skeletons, [])

    def test_p1_second_up_requires_five_and_strict_close_break(self):
        too_short = build_valid_swing_klines(second_up_bars=4)
        self.assertEqual(
            extract_double_break_skeletons(
                "UP4USDT", parse_structure_klines(too_short[:-1]), 2, 2
            ),
            [],
        )
        wick_only = build_valid_swing_klines(second_up_bars=5)
        h1 = Decimal("160")
        for row in wick_only[42 + 5 :]:
            row[2] = str(max(Decimal(row[2]), h1 + Decimal("1")))
            row[4] = str(min(Decimal(row[4]), h1))
        self.assertEqual(
            extract_double_break_skeletons(
                "WICKUSDT", parse_structure_klines(wick_only[:-1]), 2, 2
            ),
            [],
        )
        close_break = build_valid_swing_klines(second_up_bars=5)
        skeletons = extract_double_break_skeletons(
            "CLOSEUSDT", parse_structure_klines(close_break[:-1]), 2, 2
        )
        self.assertTrue(skeletons)
        self.assertGreater(
            Decimal(close_break[skeletons[-1].second_break_index][4]),
            skeletons[-1].h1,
        )

    def test_beatusdt_continued_rise_never_locks_first_post_break_low_as_p2(self):
        raw = build_beatusdt_continued_rise()
        result = analyze_n06_double_break_pullback("BEATUSDT", raw)
        self.assertFalse(result.passed)
        self.assertIn(result.reason, {"STRUCTURE_NOT_FOUND", "N06_H2_P2_NOT_CONFIRMED"})
        self.assertEqual(raw[37][0], 11 * 60 * 60 * 1000)
        self.assertEqual(raw[38][0] - raw[37][0], INTERVAL_MS)
        first_second_break = next(
            index for index in range(39, len(raw) - 1) if Decimal(raw[index][4]) > Decimal("160")
        )
        self.assertEqual(raw[first_second_break][0], 14 * 60 * 60 * 1000 + 15 * 60 * 1000)

    def test_valid_h1_p1_then_continued_second_rise_has_no_early_p2(self):
        raw = build_valid_swing_klines()
        raw = raw[:55]
        raw[-1][1:5] = ["168", "170", "167.5", "169"]
        result = analyze_n06_double_break_pullback("CONTINUEUSDT", raw)
        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N06_H2_P2_NOT_CONFIRMED")

    def test_n06_first_completed_prefix_is_not_rewritten_by_later_higher_h2(self):
        original = build_valid_swing_klines()
        baseline = analyze_n06_double_break_pullback("PREFIXUSDT", original)
        self.assertTrue(baseline.passed)

        higher_without_pullback = original[:]
        start = len(higher_without_pullback)
        for offset, close in enumerate(("150", "158", "166", "174", "178", "176", "175")):
            index = start + offset
            value = Decimal(close)
            high = Decimal("180") if offset == 4 else value + Decimal("1")
            higher_without_pullback.append([
                index * INTERVAL_MS, str(value - Decimal("0.5")), str(high),
                str(value - Decimal("1")), str(value), "0",
                index * INTERVAL_MS + INTERVAL_MS - 1,
            ])
        current_index = len(higher_without_pullback)
        higher_without_pullback.append([
            current_index * INTERVAL_MS, "174", "176", "173", "175", "0",
            current_index * INTERVAL_MS + INTERVAL_MS - 1,
        ])
        historical = analyze_n06_double_break_pullback(
            "PREFIXUSDT", higher_without_pullback
        )
        self.assertEqual(historical.reason, "N06_HISTORICAL_STRUCTURE_MISSED")
        self.assertEqual(historical.structure.h2, baseline.structure.h2)
        self.assertEqual(historical.structure.p2, baseline.structure.p2)
        self.assertEqual(
            historical.structure.stable_confirm_time,
            baseline.structure.stable_confirm_time,
        )
        self.assertEqual(historical.structure_id, baseline.structure_id)

        second_complete = higher_without_pullback[:-1]
        start = len(second_complete)
        closes = ("170", "164", "158", "153", "149", "147", "148", "149", "148.5", "150", "151")
        for offset, close in enumerate(closes):
            index = start + offset
            value = Decimal(close)
            low = Decimal("146") if offset == 5 else value - Decimal("0.8")
            second_complete.append([
                index * INTERVAL_MS, str(value + Decimal("0.3")),
                str(value + Decimal("0.8")), str(low), str(value), "0",
                index * INTERVAL_MS + INTERVAL_MS - 1,
            ])
        current_index = len(second_complete)
        second_complete.append([
            current_index * INTERVAL_MS, "151", "153", "150", "152", "0",
            current_index * INTERVAL_MS + INTERVAL_MS - 1,
        ])
        later = analyze_n06_double_break_pullback("PREFIXUSDT", second_complete)
        self.assertEqual(later.reason, "N06_HISTORICAL_STRUCTURE_MISSED")
        self.assertEqual(later.structure.h2, baseline.structure.h2)
        self.assertEqual(later.structure.p2, baseline.structure.p2)
        self.assertEqual(later.structure.stable_confirm_time, baseline.structure.stable_confirm_time)
        self.assertEqual(later.structure_id, baseline.structure_id)

    def test_h2_p2_pullback_under_five_bars_rejects(self):
        raw = build_valid_swing_klines(h2_pullback_bars=4)
        result = analyze_n06_double_break_pullback("P2SHORTUSDT", raw)
        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N06_H2_P2_NOT_CONFIRMED")

    def test_n07_single_candle_zone_spike_rejects_but_effective_retrace_passes(self):
        valid = build_valid_swing_klines()
        passed = analyze_n07_p1_retest("N07VALIDUSDT", valid)
        self.assertTrue(passed.passed)
        spike = build_valid_swing_klines()
        current_index = len(spike) - 1
        h2_index = current_index - 12
        for index in range(h2_index + 1, current_index):
            spike[index][1:5] = ["167", "168", "166.5", "167"]
        rejected = analyze_n07_p1_retest("N07SPIKEUSDT", spike)
        self.assertFalse(rejected.passed)
        self.assertEqual(rejected.reason, "N07_INVALID_FIRST_TOUCH_SEGMENT")


if __name__ == "__main__":
    unittest.main()
