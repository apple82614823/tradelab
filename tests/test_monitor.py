from decimal import Decimal
import logging
import unittest

from trading_bot.monitor import FundingMonitor
from trading_bot.exchange_symbol import attest_authenticated_symbol_set


class FakeClient:
    def get_premium_index(self):
        return [
            {"symbol": "POSUSDT", "lastFundingRate": "0.021", "markPrice": "10"},
            {"symbol": "LOWNEGUSDT", "lastFundingRate": "-0.010", "markPrice": "20"},
            {"symbol": "NEG2USDT", "lastFundingRate": "-0.020", "markPrice": "30"},
            {"symbol": "NEG3USDT", "lastFundingRate": "-0.030", "markPrice": "40"},
            {"symbol": "BTCUSD_PERP", "lastFundingRate": "-0.040", "markPrice": "50"},
        ]


class TopVolumeFakeClient:
    def __init__(self):
        self.premium_calls = 0
        self.ticker_calls = 0
        self.exchange_info_calls = 0
        self.symbols = [f"S{index:03d}USDT" for index in range(1, 102)]

    def get_premium_index(self):
        self.premium_calls += 1
        return [
            {
                "symbol": symbol,
                "lastFundingRate": "0.01",
                "markPrice": str(200 - index),
            }
            for index, symbol in enumerate(self.symbols, start=1)
        ]

    def get_24hr_tickers(self):
        self.ticker_calls += 1
        return [
            {
                "symbol": symbol,
                "quoteVolume": str(1000 - index),
                "lastPrice": str(500 - index),
            }
            for index, symbol in enumerate(self.symbols, start=1)
        ]

    def get_tradable_usdt_perpetual_symbols(self):
        self.exchange_info_calls += 1
        return set(self.symbols)


class UnicodeTopVolumeFakeClient(TopVolumeFakeClient):
    def __init__(self):
        super().__init__()
        self.symbols[73] = "龙虾USDT"


class FundingMonitorTests(unittest.TestCase):
    def test_scan_selects_negative_funding_for_long_receives_funding(self):
        monitor = FundingMonitor(FakeClient(), Decimal("0.015"), logging.getLogger("test_monitor"))
        scanned_count, candidates = monitor.scan()

        self.assertEqual(scanned_count, 5)
        self.assertEqual([candidate.symbol for candidate in candidates], ["NEG3USDT", "NEG2USDT"])
        self.assertEqual([candidate.funding_rate for candidate in candidates], [Decimal("-0.030"), Decimal("-0.020")])

    def test_multi_strategy_scan_includes_rank_100_and_excludes_rank_101(self):
        client = TopVolumeFakeClient()
        monitor = FundingMonitor(client, Decimal("0.015"), logging.getLogger("test_monitor"))

        result = monitor.scan_for_strategies(100)

        self.assertEqual(len(result.volume_candidates), 100)
        self.assertEqual(result.volume_candidates[-1].symbol, "S100USDT")
        self.assertEqual(result.volume_candidates[-1].quote_volume_rank, 100)
        self.assertEqual(result.volume_candidates[0].mark_price, Decimal("499"))
        self.assertNotIn("S101USDT", [candidate.symbol for candidate in result.volume_candidates])
        self.assertTrue(all(candidate.funding_rate is None for candidate in result.volume_candidates))
        self.assertEqual(client.premium_calls, 1)
        self.assertEqual(client.ticker_calls, 1)
        self.assertEqual(client.exchange_info_calls, 1)

    def test_zero_volume_strategy_set_skips_volume_candidate_request(self):
        client = TopVolumeFakeClient()
        monitor = FundingMonitor(
            client,
            Decimal("0.015"),
            logging.getLogger("test_monitor"),
        )

        result = monitor.scan_for_strategies(0)

        self.assertEqual(result.volume_candidates, [])
        self.assertEqual(client.premium_calls, 1)
        self.assertEqual(client.exchange_info_calls, 1)
        self.assertEqual(client.ticker_calls, 0)

    def test_exchangeinfo_authenticated_unicode_symbol_keeps_exact_rank(self):
        client = UnicodeTopVolumeFakeClient()
        monitor = FundingMonitor(client, Decimal("0.015"), logging.getLogger("test_monitor"))

        result = monitor.scan_for_strategies(100)

        self.assertEqual(result.volume_candidates[73].symbol, "龙虾USDT")
        self.assertEqual(result.volume_candidates[73].quote_volume_rank, 74)
        self.assertEqual(len(result.volume_candidates), 100)
        registry = attest_authenticated_symbol_set(
            result.authenticated_symbols,
            result.authenticated_symbols_sha256,
        )
        self.assertIn("龙虾USDT", registry)

    def test_exchangeinfo_rejects_path_like_or_compatibility_symbol(self):
        for hostile in ("BAD/USDT", "ＡUSDT"):
            client = UnicodeTopVolumeFakeClient()
            client.symbols[73] = hostile
            monitor = FundingMonitor(
                client,
                Decimal("0.015"),
                logging.getLogger("test_monitor"),
            )
            with self.subTest(hostile=hostile), self.assertRaises(ValueError):
                monitor.scan_for_strategies(100)


if __name__ == "__main__":
    unittest.main()
