from decimal import Decimal
import unittest

from trading_bot.analyzer import analyze_symbol, linear_regression_slope


def kline(open_price: str, close_price: str):
    high = str(max(Decimal(open_price), Decimal(close_price)))
    low = str(min(Decimal(open_price), Decimal(close_price)))
    return [0, open_price, high, low, close_price, "0", 0]


class AnalyzerTests(unittest.TestCase):
    def test_linear_regression_slope_detects_uptrend(self):
        self.assertGreater(linear_regression_slope([Decimal("1"), Decimal("2"), Decimal("3")]), 0)

    def test_analysis_passes_with_trend_pattern_and_current_bullish(self):
        raw = [kline(str(i), str(i + 1)) for i in range(1, 101)]
        result = analyze_symbol("BTCUSDT", raw, Decimal("102"), 96)
        self.assertTrue(result.passed)
        self.assertEqual(result.pattern, "A_3_BULLISH_CANDLES")

    def test_analysis_passes_five_close_uptrend_without_three_bullish_candles(self):
        raw = [kline(str(i), str(i + 1)) for i in range(1, 92)]
        raw.extend(
            [
                kline("91", "92"),
                kline("92", "93"),
                kline("93", "94"),
                kline("95", "96"),
                kline("97", "95"),
                kline("95", "95"),
            ]
        )

        result = analyze_symbol("SPELLUSDT", raw, Decimal("96"), 96)

        self.assertTrue(result.passed)
        self.assertEqual(result.pattern, "B_5_CLOSES_UPTREND")
        self.assertTrue(result.current_bullish)

    def test_analysis_passes_pullback_bounce_without_three_bullish_candles(self):
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

        result = analyze_symbol("BOUNCEUSDT", raw, Decimal("193"), 96)

        self.assertTrue(result.passed)
        self.assertEqual(result.pattern, "C_UP_PULLBACK_BOUNCE")
        self.assertTrue(result.current_bullish)

    def test_analysis_fails_when_current_candle_is_not_bullish(self):
        raw = [kline(str(i), str(i + 1)) for i in range(1, 100)]
        raw.append(kline("200", "190"))
        result = analyze_symbol("BTCUSDT", raw, Decimal("199"), 96)
        self.assertFalse(result.passed)
        self.assertFalse(result.current_bullish)


if __name__ == "__main__":
    unittest.main()
