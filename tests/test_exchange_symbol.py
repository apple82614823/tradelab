from __future__ import annotations

import unittest
import logging
from decimal import Decimal

from trading_bot.exchange_symbol import (
    attest_authenticated_symbol_set,
    authenticated_symbol_set_sha256,
    canonical_exchange_symbol,
    exchange_symbol_sha256,
)
from trading_bot.monitor import FundingCandidate
from trading_bot.strategy_scheduler import StrategyScheduler


class ExchangeSymbolIdentityTests(unittest.TestCase):
    def test_exact_exchange_unicode_symbol_is_bounded_and_hashed(self):
        symbols = frozenset({"BTCUSDT", "龙虾USDT"})
        digest = authenticated_symbol_set_sha256(symbols)

        self.assertEqual(canonical_exchange_symbol("龙虾USDT"), "龙虾USDT")
        self.assertEqual(attest_authenticated_symbol_set(symbols, digest), symbols)
        self.assertNotEqual(
            exchange_symbol_sha256("龙虾USDT"),
            exchange_symbol_sha256("BTCUSDT"),
        )

    def test_untrusted_variants_and_path_or_control_text_are_rejected(self):
        for hostile in (
            "龙虾/usdt",
            "龙虾/USDT",
            "龙虾\\USDT",
            "龙虾\nUSDT",
            "龙虾\u200bUSDT",
            "ＡUSDT",
        ):
            with self.subTest(hostile=repr(hostile)):
                with self.assertRaises(ValueError):
                    canonical_exchange_symbol(hostile)

        authenticated = frozenset({"龙虾USDT"})
        digest = authenticated_symbol_set_sha256(authenticated)
        self.assertEqual(attest_authenticated_symbol_set(authenticated, digest), authenticated)
        with self.assertRaises(ValueError):
            attest_authenticated_symbol_set({"龍蝦USDT"}, digest)

    def test_authenticated_registry_is_nonempty_unique_and_bounded(self):
        for invalid in ((), ("BTCUSDT", "BTCUSDT")):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    authenticated_symbol_set_sha256(invalid)

        with self.assertRaises(ValueError):
            authenticated_symbol_set_sha256(
                "BTCUSDT" for _ in range(10_001)
            )

    def test_scheduler_rejects_candidate_absent_from_authenticated_registry(self):
        scheduler = StrategyScheduler((), 30, None, logging.getLogger("symbol-auth"))
        registry = frozenset({"龙虾USDT"})
        digest = authenticated_symbol_set_sha256(registry)

        result = scheduler.evaluate(
            1,
            [FundingCandidate("龍蝦USDT", None, Decimal("1"))],
            {"龍蝦USDT": []},
            authenticated_symbols=registry,
            authenticated_symbols_sha256=digest,
        )

        self.assertFalse(result.signal_batch_published)
        self.assertEqual(result.signals, [])
        self.assertEqual(
            tuple(item.code for item in result.signal_audit_failures),
            ("EXCHANGE_SYMBOL_IDENTITY_INVALID",),
        )


if __name__ == "__main__":
    unittest.main()
