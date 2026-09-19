from __future__ import annotations

from contextlib import closing, contextmanager
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import hashlib
import json
import logging
import os
from pathlib import Path
import resource
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from trading_bot.micro_analyzer import (
    analyze_n21,
    analyze_n22,
    analyze_n23,
    analyze_n24,
    analyze_n25,
)
from trading_bot.micro_observation import (
    MicroObservation,
    MicroObservationCache,
    MicroObservationError,
    MicroObservationWindow,
    build_universe_sha256,
    canonical_json,
    derive_increment,
)
from trading_bot.strategies import (
    MicroStrategyDefinition,
    N21_STRATEGY,
    load_all_strategies,
)
from trading_bot.main import TradingBot
from trading_bot.paper_trader import PaperTrader
from trading_bot.micro_schema import (
    MICRO_ANALYSIS_TABLE_SQL,
    MICRO_CLAIM_TABLE_SQL,
    MICRO_INDEX_SQL,
    MICRO_TRIGGER_SQL,
    micro_schema_status,
    validate_micro_staged_lifecycle_graph,
)
from trading_bot.recorder import ReviewRecorder
import trading_bot.recorder as recorder_module
from trading_bot.monitor import FundingCandidate
from trading_bot.strategy_scheduler import StrategyScheduler
from trading_bot.binance_client import BinanceAPIError
from trading_bot.state import PositionState, StateStore
from trading_bot.trader import Trader
from trading_bot.signal_retention import (
    _install_coverage_epoch_boundary,
    _install_micro_lifecycle_boundary,
    _install_n16_claim_boundary,
    _install_n17_lifecycle_boundary,
    _install_n18_lifecycle_boundary,
    _install_n19_lifecycle_boundary,
    _install_n20_lifecycle_boundary,
)
from tests.recorder_test_utils import make_test_recorder
from tests.test_trader import RuleConstrainedClient, live_test_config


OPEN_TIME = 1_800_000_000_000
UNIVERSE = "a" * 64


def observation(
    symbol: str,
    scan: int,
    generation: int,
    observed: int,
    *,
    close: str = "100",
    high: str = "101",
    low: str = "99",
    quote: str = "1000",
    trades: int = 100,
    taker: str = "500",
    premium: str = "-0.0003",
    rank: int = 1,
) -> MicroObservation:
    premium_decimal = Decimal(premium)
    item = MicroObservation(
        symbol=symbol,
        scan_id=scan,
        generation=generation,
        boot_id="boot",
        observed_at_ms=observed,
        premium_observed_at_ms=observed,
        kline_open_time_ms=OPEN_TIME,
        kline_close_time_ms=OPEN_TIME + 900_000 - 1,
        open=Decimal("100"),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        quote_volume=Decimal(quote),
        trade_count=trades,
        taker_buy_quote=Decimal(taker),
        mark_price=Decimal("100") * (Decimal(1) + premium_decimal),
        index_price=Decimal("100"),
        funding_rate=Decimal("0"),
        premium=premium_decimal,
        quote_volume_rank=rank,
        universe_sha256=UNIVERSE,
        atr=Decimal("2"),
        quote_rate_baseline=Decimal("1"),
        source_sha256="",
    )
    return replace(
        item,
        source_sha256=hashlib.sha256(
            canonical_json(item.canonical_payload()).encode("utf-8")
        ).hexdigest(),
    )


def authenticated_top100_observations(observations):
    universe_sha256 = build_universe_sha256(tuple(
        (item.symbol, item.quote_volume_rank)
        for item in observations.values()
    ))
    result = {}
    for symbol, item in observations.items():
        unsigned = replace(
            item,
            universe_sha256=universe_sha256,
            source_sha256="",
        )
        result[symbol] = replace(
            unsigned,
            source_sha256=hashlib.sha256(
                canonical_json(unsigned.canonical_payload()).encode("utf-8")
            ).hexdigest(),
        )
    return result


def market_windows(*, anchor_return: str, median_return: str):
    windows = {}
    for rank in range(1, 101):
        symbol = (
            "BTCUSDT" if rank == 1 else "ETHUSDT" if rank == 2 else f"S{rank:03d}USDT"
        )
        price_return = Decimal(anchor_return if rank <= 2 else median_return)
        before = observation(symbol, 1, 1, OPEN_TIME + 60_000, rank=rank)
        after_close = before.close * (Decimal(1) + price_return)
        after = observation(
            symbol,
            2,
            2,
            OPEN_TIME + 120_000,
            close=str(after_close),
            high=str(max(Decimal("101"), after_close)),
            rank=rank,
            quote="1100",
            trades=110,
            taker="550",
        )
        windows[symbol] = MicroObservationWindow(symbol, (before, after))
    return windows


class MicroObservationTests(unittest.TestCase):
    @staticmethod
    def _install_through_n20(database: Path, ledger: Path) -> None:
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.commit()
        _install_n16_claim_boundary(database, ledger)
        _install_n17_lifecycle_boundary(database, ledger)
        _install_n19_lifecycle_boundary(database, ledger)
        _install_n18_lifecycle_boundary(database, ledger)
        _install_n20_lifecycle_boundary(database, ledger)

    @staticmethod
    def _n21_window(symbol="FLOWUSDT", rank=7):
        return MicroObservationWindow(
            symbol,
            (
                observation(symbol, 1, 1, OPEN_TIME + 1, close="100", low="99.5", quote="1000", trades=100, taker="500", rank=rank),
                observation(symbol, 2, 2, OPEN_TIME + 60_001, close="100.05", low="99.5", quote="1100", trades=110, taker="550", rank=rank),
                observation(symbol, 3, 3, OPEN_TIME + 120_001, close="100.10", low="99.5", quote="1200", trades=120, taker="602", rank=rank),
                observation(symbol, 4, 4, OPEN_TIME + 180_001, close="100.30", low="99.5", quote="1300", trades=130, taker="658", rank=rank),
            ),
        )

    @staticmethod
    def _publish_rejected_warmup_scan(recorder: ReviewRecorder) -> int:
        scan_id = recorder.begin_scan(1, [], True)
        if type(scan_id) is not int:
            raise AssertionError("warmup scan was not created")
        signal_id = recorder.record_strategy_signal(
            scan_id=scan_id,
            strategy_id="N21",
            symbol="WARMUPUSDT",
            funding_rate="",
            matched_patterns=(),
            trend_slope="",
            current_bullish=False,
            passed=False,
            decision="REJECTED",
            reason="N21_MICRO_OBSERVATION_COLD_START",
            structure_id=f"{scan_id:024d}",
            detail={"reason": "N21_MICRO_OBSERVATION_COLD_START"},
        )
        if type(signal_id) is not int:
            raise AssertionError("warmup signal was not recorded")
        if not recorder.publish_strategy_signal_batch(scan_id, 1):
            raise AssertionError("warmup scan was not published")
        return scan_id

    @staticmethod
    def _append_n21_observation(
        window: MicroObservationWindow,
        scan_id: int,
        *,
        close: str,
        taker: str,
    ) -> MicroObservationWindow:
        previous = window.observations[-1]
        next_observation = observation(
            window.symbol,
            scan_id,
            scan_id,
            previous.observed_at_ms + 60_000,
            close=close,
            high="101",
            low="99.5",
            quote=str(previous.quote_volume + Decimal("100")),
            trades=previous.trade_count + 10,
            taker=taker,
            rank=previous.quote_volume_rank,
        )
        return MicroObservationWindow(
            window.symbol,
            window.observations[-3:] + (next_observation,),
        )

    @classmethod
    def _passing_micro_cases(cls):
        n21 = cls._n21_window("FLOWUSDT", rank=7)

        n22_symbol = "S003USDT"
        n22 = MicroObservationWindow(n22_symbol, (
            observation(n22_symbol, 1, 1, OPEN_TIME + 1, close="100", low="99.5", rank=3),
            observation(n22_symbol, 2, 2, OPEN_TIME + 60_001, close="99.8", low="99.5", quote="1100", trades=110, taker="520", rank=3),
            observation(n22_symbol, 3, 3, OPEN_TIME + 120_001, close="99.65", low="99.4", quote="1200", trades=120, taker="555", rank=3),
            observation(n22_symbol, 4, 4, OPEN_TIME + 180_001, close="99.85", low="99.4", quote="1300", trades=130, taker="625", rank=3),
        ))
        n22_market = market_windows(anchor_return="0", median_return="0")
        n22_market[n22_symbol] = n22
        for symbol, window in tuple(n22_market.items()):
            if symbol == n22_symbol:
                continue
            before, after = window.observations
            aligned = (
                replace(before, scan_id=3, generation=3, source_sha256=""),
                replace(after, scan_id=4, generation=4, source_sha256=""),
            )
            n22_market[symbol] = MicroObservationWindow(symbol, tuple(
                replace(item, source_sha256=hashlib.sha256(
                    canonical_json(item.canonical_payload()).encode()
                ).hexdigest())
                for item in aligned
            ))

        n23_symbol = "IGNITEUSDT"
        n23 = MicroObservationWindow(n23_symbol, (
            observation(n23_symbol, 1, 1, OPEN_TIME + 1, close="100", low="99.5", high="100.8", quote="1000", trades=100, taker="500", rank=8),
            observation(n23_symbol, 2, 2, OPEN_TIME + 60_001, close="99.95", low="99.4", high="100.8", quote="1100", trades=110, taker="540", rank=8),
            observation(n23_symbol, 3, 3, OPEN_TIME + 120_001, close="100.05", low="99.4", high="100.8", quote="1225", trades=120, taker="605", rank=8),
            observation(n23_symbol, 4, 4, OPEN_TIME + 180_001, close="100.40", low="99.4", high="101.1", quote="1381.25", trades=140, taker="692.5", rank=8),
        ))

        n24_symbol = "LAGUSDT"
        n24 = MicroObservationWindow(n24_symbol, (
            observation(n24_symbol, 1, 1, OPEN_TIME + 1, close="100", low="99.5", quote="900", trades=90, taker="450", rank=10),
            observation(n24_symbol, 2, 2, OPEN_TIME + 60_001, close="100", low="99.5", quote="1000", trades=100, taker="500", rank=10),
            observation(n24_symbol, 3, 3, OPEN_TIME + 120_001, close="99.95", low="99.4", quote="1100", trades=110, taker="540", rank=10),
            observation(n24_symbol, 4, 4, OPEN_TIME + 180_001, close="100.20", low="99.4", high="101.1", quote="1220", trades=125, taker="610", rank=10),
        ))
        btc = MicroObservationWindow("BTCUSDT", (
            observation("BTCUSDT", 1, 1, OPEN_TIME + 1, close="100", quote="900", trades=90, taker="450", rank=1),
            observation("BTCUSDT", 2, 2, OPEN_TIME + 60_001, close="100", rank=1),
            observation("BTCUSDT", 3, 3, OPEN_TIME + 120_001, close="100.15", quote="1100", trades=110, taker="554", rank=1),
            observation("BTCUSDT", 4, 4, OPEN_TIME + 180_001, close="100.10", quote="1200", trades=120, taker="604", rank=1),
        ))
        eth = MicroObservationWindow("ETHUSDT", (
            observation("ETHUSDT", 1, 1, OPEN_TIME + 1, close="100", quote="900", trades=90, taker="450", rank=2),
            observation("ETHUSDT", 2, 2, OPEN_TIME + 60_001, close="100", rank=2),
            observation("ETHUSDT", 3, 3, OPEN_TIME + 120_001, close="100", quote="1100", trades=110, taker="549", rank=2),
            observation("ETHUSDT", 4, 4, OPEN_TIME + 180_001, close="100", quote="1200", trades=120, taker="599", rank=2),
        ))
        n24_market = {n24_symbol: n24, "BTCUSDT": btc, "ETHUSDT": eth}

        n25_symbol = "PREMIUMUSDT"
        n25 = MicroObservationWindow(n25_symbol, (
            observation(n25_symbol, 1, 1, OPEN_TIME + 1, close="100", low="99.5", quote="900", trades=90, taker="450", premium="-0.00036", rank=10),
            observation(n25_symbol, 2, 2, OPEN_TIME + 60_001, close="100", low="99.5", quote="1000", trades=100, taker="500", premium="-0.00030", rank=10),
            observation(n25_symbol, 3, 3, OPEN_TIME + 120_001, close="99.95", low="99.4", quote="1100", trades=110, taker="540", premium="-0.00024", rank=10),
            observation(n25_symbol, 4, 4, OPEN_TIME + 180_001, close="100.20", low="99.4", high="101.1", quote="1220", trades=125, taker="610", premium="-0.00018", rank=10),
        ))
        n25_market = {n25_symbol: n25}
        for rank in range(1, 101):
            symbol = (
                "BTCUSDT" if rank == 1
                else "ETHUSDT" if rank == 2
                else f"P{rank:03d}USDT"
            )
            n25_market[symbol] = MicroObservationWindow(symbol, (
                observation(symbol, 1, 1, OPEN_TIME + 1, quote="900", trades=90, taker="450", premium="-0.00012", rank=rank),
                observation(symbol, 2, 2, OPEN_TIME + 60_001, premium="-0.00010", rank=rank),
                observation(symbol, 3, 3, OPEN_TIME + 120_001, quote="1100", trades=110, taker="550", premium="-0.00008", rank=rank),
                observation(symbol, 4, 4, OPEN_TIME + 180_001, close="100.01", high="101.1", quote="1200", trades=120, taker="604", premium="-0.00006", rank=rank),
            ))
        n25_market.pop("P100USDT")
        if len(n25_market) != 100:
            raise AssertionError("N25 passing fixture must freeze exactly 100 members")

        return {
            "N21": (n21.symbol, {n21.symbol: n21}),
            "N22": (n22_symbol, n22_market),
            "N23": (n23_symbol, {n23_symbol: n23}),
            "N24": (n24_symbol, n24_market),
            "N25": (n25_symbol, n25_market),
        }

    def test_definition_order_is_exact_and_old_prefix_is_unchanged(self):
        strategies = load_all_strategies()
        self.assertEqual([item.strategy_id for item in strategies], [f"N{i:02d}" for i in range(1, 26)])
        self.assertTrue(all(isinstance(item, MicroStrategyDefinition) for item in strategies[-5:]))

    def test_interval_closed_boundaries_and_source_hash(self):
        first = observation("BTCUSDT", 1, 1, OPEN_TIME + 60_000)
        for interval in (45_000, 150_000):
            with self.subTest(interval=interval):
                second = observation(
                    "BTCUSDT", 2, 2, first.observed_at_ms + interval,
                    quote="1100", trades=110, taker="560",
                )
                self.assertEqual(derive_increment(first, second).interval_ms, interval)
        for interval in (44_999, 150_001):
            with self.subTest(interval=interval):
                second = observation(
                    "BTCUSDT", 2, 2, first.observed_at_ms + interval,
                    quote="1100", trades=110, taker="560",
                )
                with self.assertRaises(MicroObservationError):
                    derive_increment(first, second)
        with self.assertRaisesRegex(MicroObservationError, "source hash"):
            derive_increment(first, replace(first, source_sha256="b" * 64))

    def test_cache_is_bounded_cold_resets_and_generation_cas(self):
        cache = MicroObservationCache(boot_id="boot")
        observed = OPEN_TIME + 60_000
        for scan in range(1, 7):
            generation = cache.generation + 1
            rows = {
                ("BTCUSDT" if rank == 1 else f"S{rank:03d}USDT"): observation(
                    "BTCUSDT" if rank == 1 else f"S{rank:03d}USDT",
                    scan,
                    generation,
                    observed + (scan - 1) * 60_000,
                    quote=str(1000 + scan * 100),
                    trades=100 + scan * 10,
                    taker=str(500 + scan * 60),
                    rank=rank,
                )
                for rank in range(1, 101)
            }
            rows = authenticated_top100_observations(rows)
            proposal = cache.propose(scan, rows)
            self.assertTrue(cache.commit(proposal, current_scan_id=scan))
        self.assertEqual(cache.generation, 6)
        self.assertEqual(sum(len(item.observations) for item in cache.snapshot().values()), 400)
        stale_rows = {}
        for symbol, item in cache.snapshot().items():
            prior = item.observations[-1]
            stale_rows[symbol] = observation(
                symbol, 7, 7, prior.observed_at_ms + 60_000,
                close=str(prior.close), high=str(prior.high), low=str(prior.low),
                quote=str(prior.quote_volume + 100),
                trades=prior.trade_count + 10,
                taker=str(prior.taker_buy_quote + 60),
                premium=str(prior.premium), rank=prior.quote_volume_rank,
            )
        stale_rows = authenticated_top100_observations(stale_rows)
        stale = cache.propose(7, stale_rows)
        self.assertFalse(cache.commit(stale, current_scan_id=8))
        self.assertEqual(cache.snapshot(), {})

    def test_cache_remains_strictly_bounded_for_1440_published_rounds(self):
        cache = MicroObservationCache(boot_id="boot")
        symbols = tuple(
            "BTCUSDT" if rank == 1 else f"S{rank:03d}USDT"
            for rank in range(1, 101)
        )
        for scan in range(1, 1441):
            elapsed = (scan - 1) * 60_000
            candle_offset, within_candle = divmod(elapsed, 900_000)
            candle_open = OPEN_TIME + candle_offset * 900_000
            generation = cache.generation + 1
            slot = within_candle // 60_000
            rows = {}
            for rank, symbol in enumerate(symbols, start=1):
                seed = observation(
                    symbol,
                    scan,
                    generation,
                    OPEN_TIME + 1,
                    quote=str(1000 + slot * 100),
                    trades=100 + slot * 10,
                    taker=str(500 + slot * 55),
                    rank=rank,
                )
                unsigned = replace(
                    seed,
                    observed_at_ms=candle_open + within_candle + 1,
                    premium_observed_at_ms=candle_open + within_candle + 1,
                    kline_open_time_ms=candle_open,
                    kline_close_time_ms=candle_open + 900_000 - 1,
                    source_sha256="",
                )
                rows[symbol] = replace(
                    unsigned,
                    source_sha256=hashlib.sha256(
                        canonical_json(unsigned.canonical_payload()).encode()
                    ).hexdigest(),
                )
            rows = authenticated_top100_observations(rows)
            proposal = cache.propose(scan, rows)
            self.assertTrue(cache.commit(proposal, current_scan_id=scan))
            self.assertLessEqual(
                sum(
                    len(window.observations)
                    for window in cache.snapshot().values()
                ),
                400,
            )
        self.assertEqual(cache.scan_id, 1440)
        self.assertEqual(cache.generation, 1440)

    def test_n22_systemic_cascade_is_three_way_inclusive(self):
        target = "S003USDT"
        o0 = observation(target, 1, 1, OPEN_TIME + 1, close="100", low="99.5", rank=3)
        o1 = observation(target, 2, 2, OPEN_TIME + 60_001, close="99.8", high="101", low="99.5", quote="1100", trades=110, taker="520", rank=3)
        o2 = observation(target, 3, 3, OPEN_TIME + 120_001, close="99.65", high="101", low="99.4", quote="1200", trades=120, taker="555", rank=3)
        o3 = observation(target, 4, 4, OPEN_TIME + 180_001, close="99.85", high="101", low="99.4", quote="1300", trades=130, taker="625", rank=3)

        def setup(anchor_return: str, median_return: str):
            windows = market_windows(
                anchor_return=anchor_return, median_return=median_return
            )
            windows[target] = MicroObservationWindow(target, (o0, o1, o2, o3))
            for symbol, window in tuple(windows.items()):
                if symbol == target:
                    continue
                before, after = window.observations
                aligned = (
                    replace(before, scan_id=3, generation=3, source_sha256=""),
                    replace(after, scan_id=4, generation=4, source_sha256=""),
                )
                windows[symbol] = MicroObservationWindow(
                    symbol,
                    tuple(
                        replace(item, source_sha256=hashlib.sha256(
                            canonical_json(item.canonical_payload()).encode()
                        ).hexdigest())
                        for item in aligned
                    ),
                )
            return windows

        cascade = setup("-0.0015", "-0.0015")
        self.assertEqual(
            analyze_n22(cascade[target], cascade, o3.observed_at_ms).reason,
            "N22_SYSTEMIC_CASCADE",
        )
        # SYSTEMIC_CASCADE is an exact three-way AND.  Each case leaves two
        # legs at the inclusive threshold and moves exactly one leg above it.
        cases = {
            "btc_above": ("-0.0014", "-0.0015", "BTCUSDT"),
            "eth_above": ("-0.0014", "-0.0015", "ETHUSDT"),
            "median_above": ("-0.0015", "-0.0014", None),
            "ordinary_non_cascade": ("0", "0", None),
        }
        for name, (anchor_return, median_return, anchor_to_raise) in cases.items():
            with self.subTest(name=name):
                windows = setup(anchor_return, median_return)
                if anchor_to_raise == "ETHUSDT":
                    # market_windows applies anchor_return to both anchors;
                    # restore BTC to the exact veto boundary.
                    btc = setup("-0.0015", median_return)["BTCUSDT"]
                    windows["BTCUSDT"] = btc
                elif anchor_to_raise == "BTCUSDT":
                    eth = setup("-0.0015", median_return)["ETHUSDT"]
                    windows["ETHUSDT"] = eth
                result = analyze_n22(
                    windows[target], windows, o3.observed_at_ms
                )
                self.assertTrue(result.passed, result.reason)

    def test_n21_n23_positive_thresholds_and_route_are_distinct(self):
        n21_window = self._n21_window()
        n21 = analyze_n21(n21_window, n21_window.observations[-1].observed_at_ms)
        self.assertTrue(n21.passed, n21.reason)
        self.assertEqual(n21.structure.entry_max, Decimal("100.80"))

        symbol = "IGNITEUSDT"
        n23_window = MicroObservationWindow(
            symbol,
            (
                observation(symbol, 1, 1, OPEN_TIME + 1, close="100", low="99.5", high="100.8", quote="1000", trades=100, taker="500", rank=8),
                observation(symbol, 2, 2, OPEN_TIME + 60_001, close="99.95", low="99.4", high="100.8", quote="1100", trades=110, taker="540", rank=8),
                observation(symbol, 3, 3, OPEN_TIME + 120_001, close="100.05", low="99.4", high="100.8", quote="1225", trades=120, taker="605", rank=8),
                observation(symbol, 4, 4, OPEN_TIME + 180_001, close="100.40", low="99.4", high="101.1", quote="1381.25", trades=140, taker="692.5", rank=8),
            ),
        )
        n23 = analyze_n23(n23_window, n23_window.observations[-1].observed_at_ms)
        self.assertTrue(n23.passed, n23.reason)
        self.assertEqual(n23.structure.entry_max, Decimal("101.00"))
        routed1 = replace(
            n23_window.observations[1],
            taker_buy_quote=Decimal("551"),
            source_sha256="",
        )
        routed1 = replace(routed1, source_sha256=hashlib.sha256(
            canonical_json(routed1.canonical_payload()).encode()
        ).hexdigest())
        routed2 = replace(
            n23_window.observations[2],
            taker_buy_quote=Decimal("615"),
            source_sha256="",
        )
        routed2 = replace(routed2, source_sha256=hashlib.sha256(
            canonical_json(routed2.canonical_payload()).encode()
        ).hexdigest())
        route_window = MicroObservationWindow(
            symbol,
            (
                n23_window.observations[0],
                routed1,
                routed2,
                n23_window.observations[3],
            ),
        )
        self.assertEqual(
            analyze_n23(
                route_window,
                route_window.observations[-1].observed_at_ms,
            ).reason,
            "N23_ROUTED_TO_N21",
        )

    def test_confirmation_deadline_and_candle_remaining_boundaries_are_exact(self):
        symbol = "TIMEUSDT"
        observed = (OPEN_TIME + 600_000, OPEN_TIME + 660_000,
                    OPEN_TIME + 720_000, OPEN_TIME + 780_000)
        base = self._n21_window(symbol)
        items = []
        for item, timestamp in zip(base.observations, observed):
            replaced = replace(
                item, observed_at_ms=timestamp,
                premium_observed_at_ms=timestamp, source_sha256="",
            )
            items.append(replace(
                replaced,
                source_sha256=hashlib.sha256(
                    canonical_json(replaced.canonical_payload()).encode()
                ).hexdigest(),
            ))
        window = MicroObservationWindow(symbol, tuple(items))
        trigger = items[-1].observed_at_ms
        self.assertTrue(analyze_n21(window, trigger + 119_999).passed)
        self.assertEqual(
            analyze_n21(window, trigger + 120_000).reason,
            "N21_ENTRY_WINDOW_EXPIRED",
        )
        too_late_items = []
        for item in items:
            shifted = replace(
                item,
                observed_at_ms=item.observed_at_ms + 1,
                premium_observed_at_ms=item.premium_observed_at_ms + 1,
                source_sha256="",
            )
            too_late_items.append(
                replace(
                    shifted,
                    source_sha256=hashlib.sha256(
                        canonical_json(shifted.canonical_payload()).encode()
                    ).hexdigest(),
                )
            )
        self.assertEqual(
            analyze_n21(
                MicroObservationWindow(symbol, tuple(too_late_items)),
                too_late_items[-1].observed_at_ms,
            ).reason,
            "N21_CANDLE_REMAINING_INSUFFICIENT",
        )

    def test_real_windows_publish_build_detail_and_open_paper_for_all_micro_strategies(self):
        class NoExchangeSideEffectClient(RuleConstrainedClient):
            def __init__(self):
                super().__init__(leverage=10, tick_size="0.01", step_size="0.001")
                self.leverage_calls = 0
                self.market_calls = 0
                self.protection_calls = 0

            def set_leverage(self, *_args, **_kwargs):
                self.leverage_calls += 1
                raise AssertionError("paper path must not set exchange leverage")

            def place_market_order(self, *_args, **_kwargs):
                self.market_calls += 1
                raise AssertionError("paper path must not submit an exchange order")

            def place_close_all_algo_order(self, *_args, **_kwargs):
                self.protection_calls += 1
                raise AssertionError("paper path must not place exchange protection")

        strategies = {item.strategy_id: item for item in load_all_strategies()[-5:]}
        for strategy_id, (symbol, windows) in self._passing_micro_cases().items():
            with self.subTest(strategy_id=strategy_id), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                recorder = make_test_recorder(
                    root / "review.sqlite3",
                    logging.getLogger("micro-paper-%s" % strategy_id),
                )
                candidate_window = windows[symbol]
                source_scan_id = candidate_window.observations[-1].scan_id
                for _ in range(source_scan_id - 1):
                    self._publish_rejected_warmup_scan(recorder)
                candidate = FundingCandidate(
                    symbol,
                    None,
                    candidate_window.observations[-1].close,
                    quote_volume=Decimal("1000000"),
                    quote_volume_rank=(
                        candidate_window.observations[-1].quote_volume_rank
                    ),
                    candidate_universe="quote_volume_top",
                )
                scan_id = recorder.begin_scan(100, [candidate], True)
                self.assertEqual(scan_id, source_scan_id)
                result = StrategyScheduler(
                    (strategies[strategy_id],),
                    96,
                    recorder,
                    logging.getLogger("micro-paper-%s" % strategy_id),
                ).evaluate(
                    scan_id,
                    {"quote_volume_top": [candidate]},
                    {},
                    checked_at_ms=max(
                        window.observations[-1].observed_at_ms
                        for window in windows.values()
                    ),
                    micro_windows=windows,
                    micro_context_complete=True,
                )
                self.assertTrue(result.signal_batch_published)
                self.assertEqual(len(result.passed_signals), 1)
                signal = result.passed_signals[0]
                self.assertEqual(signal.strategy.strategy_id, strategy_id)
                self.assertIsNotNone(signal.signal_id)
                self.assertTrue(signal.analysis.passed)

                client = NoExchangeSideEffectClient()
                bot = TradingBot.__new__(TradingBot)
                bot.recorder = recorder
                bot.trader = Trader(
                    client,
                    live_test_config(str(root / "account.json")),
                    StateStore(root / "position.json"),
                    logging.getLogger("micro-paper-%s" % strategy_id),
                )
                plan = bot._build_strategy_plan(signal)
                detail = bot._paper_trade_detail(scan_id, signal, plan)
                permanent = bot._permanent_signal_audit_snapshot(signal)
                analysis_detail = signal.analysis.detail_json()
                self.assertEqual(
                    {key: detail[key] for key in analysis_detail},
                    analysis_detail,
                )
                self.assertEqual(permanent["analysis"], analysis_detail)
                self.assertNotIn("matched_patterns", detail)
                self.assertNotIn("matched_patterns", permanent)
                self.assertEqual(plan.structure_id, signal.analysis.structure_id)
                self.assertEqual(
                    plan.entry_deadline_ms,
                    signal.analysis.structure.deadline_ms,
                )
                self.assertGreaterEqual(
                    (plan.take_profit_price - plan.entry_price)
                    / (plan.entry_price - plan.stop_loss_price),
                    Decimal("5"),
                )

                paper = PaperTrader(
                    recorder,
                    logging.getLogger("micro-paper-%s" % strategy_id),
                    clock_ms=lambda: signal.analysis.structure.deadline_ms - 1,
                )
                paper_id = paper.open_trade(
                    strategy_id,
                    symbol,
                    None,
                    plan,
                    detail,
                    opened_at="2026-07-19T04:30:00+00:00",
                )
                self.assertIsInstance(paper_id, int)
                with closing(sqlite3.connect(root / "review.sqlite3")) as connection:
                    row = connection.execute(
                        "SELECT detail_json,orders_json FROM strategy_paper_trades "
                        "WHERE id=?",
                        (paper_id,),
                    ).fetchone()
                stored_detail, stored_orders = map(json.loads, row)
                self.assertEqual(stored_detail["strategy_id"], strategy_id)
                self.assertEqual(
                    stored_detail["structure_id"], signal.analysis.structure_id
                )
                self.assertEqual(
                    stored_orders["plan"]["structure_id"],
                    signal.analysis.structure_id,
                )
                self.assertEqual(client.leverage_calls, 0)
                self.assertEqual(client.market_calls, 0)
                self.assertEqual(client.protection_calls, 0)

    def test_micro_trade_plan_tick_stop_fill_and_actual_five_r_are_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            trader = Trader(
                RuleConstrainedClient(
                    leverage=10, tick_size="0.01", step_size="0.1"
                ),
                live_test_config(str(Path(directory) / "account.json")),
                StateStore(Path(directory) / "position.json"),
                logging.getLogger("micro-plan"),
            )
            minimum = trader.build_micro_observation_margin_capped_trade_plan(
                "N21", "FLOWUSDT", Decimal("100"), Decimal("99.50"),
                Decimal("5"), structure_id="a" * 24,
                entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            self.assertEqual(minimum.stop_loss_price, Decimal("99.00"))
            self.assertEqual(minimum.stop_loss_pct, Decimal("0.01"))
            self.assertEqual(minimum.take_profit_price, Decimal("105.00"))
            self.assertEqual(minimum.quantity % Decimal("0.1"), 0)
            structural = trader.build_micro_observation_margin_capped_trade_plan(
                "N25", "FLOWUSDT", Decimal("100"), Decimal("98"),
                Decimal("5"), structure_id="b" * 24,
                entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            actual = trader._execution_plan_from_actual_entry(
                structural, Decimal("100.50")
            )
            self.assertGreaterEqual(
                (actual.take_profit_price - actual.actual_entry_price)
                / (actual.actual_entry_price - actual.stop_loss_price),
                Decimal("5"),
            )
            with self.assertRaisesRegex(BinanceAPIError, "STOP_PCT_OUT_OF_RANGE"):
                trader.build_micro_observation_margin_capped_trade_plan(
                    "N22", "FLOWUSDT", Decimal("100"), Decimal("94"),
                    Decimal("5"), structure_id="c" * 24,
                    entry_min_price=Decimal("100"),
                    entry_max_price=Decimal("100.50"),
                )
            for price in (Decimal("99.99"), Decimal("100.51")):
                with self.subTest(tolerated_fill=price):
                    tolerated = trader._execution_plan_from_actual_entry(
                        structural, price
                    )
                    self.assertGreaterEqual(
                        tolerated.risk_reward_ratio, Decimal("5")
                    )
            allowed_min = structural.entry_min_price * Decimal("0.995")
            allowed_max = structural.entry_max_price * Decimal("1.005")
            for price in (allowed_min, allowed_max):
                with self.subTest(boundary_fill=price):
                    boundary = trader._execution_plan_from_actual_entry(
                        structural, price
                    )
                    self.assertGreaterEqual(
                        boundary.risk_reward_ratio, Decimal("5")
                    )
            for price in (
                allowed_min - Decimal("0.000000000000001"),
                allowed_max + Decimal("0.000000000000001"),
            ):
                with self.subTest(rejected_fill=price):
                    with self.assertRaisesRegex(
                        BinanceAPIError, "ACTUAL_FILL_OUTSIDE"
                    ):
                        trader._execution_plan_from_actual_entry(
                            structural, price
                        )

    def test_n24_lead_lag_and_n25_premium_recovery_pass(self):
        candidate = "ALTUSDT"
        candidate_window = MicroObservationWindow(
            candidate,
            (
                observation(candidate, 1, 1, OPEN_TIME + 1, close="100", low="99.5", quote="1000", trades=100, taker="500", premium="-0.00030", rank=10),
                observation(candidate, 2, 2, OPEN_TIME + 60_001, close="99.95", low="99.4", quote="1100", trades=110, taker="540", premium="-0.00024", rank=10),
                observation(candidate, 3, 3, OPEN_TIME + 120_001, close="100.20", low="99.4", high="101.1", quote="1220", trades=125, taker="610", premium="-0.00018", rank=10),
            ),
        )
        btc = MicroObservationWindow(
            "BTCUSDT",
            (
                observation("BTCUSDT", 1, 1, OPEN_TIME + 1, close="100", rank=1),
                observation("BTCUSDT", 2, 2, OPEN_TIME + 60_001, close="100.15", quote="1100", trades=110, taker="554", rank=1),
                observation("BTCUSDT", 3, 3, OPEN_TIME + 120_001, close="100.10", quote="1200", trades=120, taker="604", rank=1),
            ),
        )
        eth = MicroObservationWindow(
            "ETHUSDT",
            (
                observation("ETHUSDT", 1, 1, OPEN_TIME + 1, close="100", rank=2),
                observation("ETHUSDT", 2, 2, OPEN_TIME + 60_001, close="100", quote="1100", trades=110, taker="549", rank=2),
                observation("ETHUSDT", 3, 3, OPEN_TIME + 120_001, close="100", quote="1200", trades=120, taker="599", rank=2),
            ),
        )
        n24 = analyze_n24(
            candidate_window,
            {candidate: candidate_window, "BTCUSDT": btc, "ETHUSDT": eth},
            candidate_window.observations[-1].observed_at_ms,
        )
        self.assertTrue(n24.passed, n24.reason)

        windows = {candidate: candidate_window}
        for rank in range(1, 101):
            symbol = "BTCUSDT" if rank == 1 else "ETHUSDT" if rank == 2 else f"P{rank:03d}USDT"
            if symbol == candidate:
                continue
            windows[symbol] = MicroObservationWindow(
                symbol,
                (
                    observation(symbol, 1, 1, OPEN_TIME + 1, premium="-0.00010", rank=rank),
                    observation(symbol, 2, 2, OPEN_TIME + 60_001, quote="1100", trades=110, taker="550", premium="-0.00008", rank=rank),
                    observation(symbol, 3, 3, OPEN_TIME + 120_001, close="100.01", high="101.1", quote="1200", trades=120, taker="604", premium="-0.00006", rank=rank),
                ),
            )
        # Replace one ordinary symbol so the mapping remains exactly 100.
        windows.pop("P100USDT")
        self.assertEqual(len(windows), 100)
        n25 = analyze_n25(
            candidate_window,
            windows,
            candidate_window.observations[-1].observed_at_ms,
        )
        self.assertTrue(n25.passed, n25.reason)

    def test_n24_anchor_skew_is_closed_at_75000_and_kline_axis_is_exact(self):
        candidate = "ALTUSDT"
        candidate_window = MicroObservationWindow(candidate, (
            observation(candidate, 1, 1, OPEN_TIME + 1, close="100", low="99.5", quote="1000", trades=100, taker="500", rank=10),
            observation(candidate, 2, 2, OPEN_TIME + 60_001, close="99.95", low="99.4", quote="1100", trades=110, taker="540", rank=10),
            observation(candidate, 3, 3, OPEN_TIME + 120_001, close="100.20", low="99.4", high="101.1", quote="1220", trades=125, taker="610", rank=10),
        ))
        btc_base = (
            observation("BTCUSDT", 1, 1, OPEN_TIME + 1, close="100", rank=1),
            observation("BTCUSDT", 2, 2, OPEN_TIME + 60_001, close="100.15", quote="1100", trades=110, taker="554", rank=1),
            observation("BTCUSDT", 3, 3, OPEN_TIME + 120_001, close="100.10", quote="1200", trades=120, taker="604", rank=1),
        )
        eth = MicroObservationWindow("ETHUSDT", (
            observation("ETHUSDT", 1, 1, OPEN_TIME + 1, close="100", rank=2),
            observation("ETHUSDT", 2, 2, OPEN_TIME + 60_001, close="100", quote="1100", trades=110, taker="549", rank=2),
            observation("ETHUSDT", 3, 3, OPEN_TIME + 120_001, close="100", quote="1200", trades=120, taker="599", rank=2),
        ))

        def shifted_btc(offset, *, cross_kline=False):
            result = []
            for item in btc_base:
                unsigned = replace(
                    item,
                    observed_at_ms=item.observed_at_ms + offset,
                    premium_observed_at_ms=item.premium_observed_at_ms + offset,
                    kline_open_time_ms=(
                        item.kline_open_time_ms + 900_000
                        if cross_kline else item.kline_open_time_ms
                    ),
                    kline_close_time_ms=(
                        item.kline_close_time_ms + 900_000
                        if cross_kline else item.kline_close_time_ms
                    ),
                    source_sha256="",
                )
                result.append(replace(
                    unsigned,
                    source_sha256=hashlib.sha256(
                        canonical_json(unsigned.canonical_payload()).encode()
                    ).hexdigest(),
                ))
            return MicroObservationWindow("BTCUSDT", tuple(result))

        for skew in (74_999, 75_000):
            with self.subTest(skew=skew):
                btc = shifted_btc(skew)
                result = analyze_n24(
                    candidate_window,
                    {candidate: candidate_window, "BTCUSDT": btc, "ETHUSDT": eth},
                    btc.observations[-1].observed_at_ms,
                )
                self.assertTrue(result.passed, result.reason)
                self.assertEqual(
                    result.structure.confirmation_observed_at_ms,
                    btc.observations[-1].observed_at_ms,
                )
        too_far = shifted_btc(75_001)
        self.assertEqual(
            analyze_n24(
                candidate_window,
                {candidate: candidate_window, "BTCUSDT": too_far, "ETHUSDT": eth},
                too_far.observations[-1].observed_at_ms,
            ).reason,
            "N24_ANCHOR_SAMPLE_SKEW",
        )
        crossed = shifted_btc(0, cross_kline=True)
        crossed_result = analyze_n24(
            candidate_window,
            {candidate: candidate_window, "BTCUSDT": crossed, "ETHUSDT": eth},
            candidate_window.observations[-1].observed_at_ms,
        )
        self.assertFalse(crossed_result.passed)

    def test_passed_analysis_signal_and_lifecycle_publish_atomically(self):
        symbol = "FLOWUSDT"
        observations = (
            observation(symbol, 1, 1, OPEN_TIME + 1, close="100", low="99.5", quote="1000", trades=100, taker="500", rank=7),
            observation(symbol, 2, 2, OPEN_TIME + 60_001, close="100.05", low="99.5", quote="1100", trades=110, taker="550", rank=7),
            observation(symbol, 3, 3, OPEN_TIME + 120_001, close="100.10", low="99.5", quote="1200", trades=120, taker="602", rank=7),
            observation(symbol, 4, 4, OPEN_TIME + 180_001, close="100.30", low="99.5", quote="1300", trades=130, taker="658", rank=7),
        )
        analysis = analyze_n21(
            MicroObservationWindow(symbol, observations),
            observations[-1].observed_at_ms,
        )
        self.assertTrue(analysis.passed, analysis.reason)
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(database, logging.getLogger("micro-claim"))
            for _ in range(3):
                self._publish_rejected_warmup_scan(recorder)
            scan_id = recorder.begin_scan(100, [], True)
            self.assertIsInstance(scan_id, int)
            self.assertEqual(scan_id, observations[-1].scan_id)
            bad_evidence = deepcopy(analysis.evidence)
            bad_evidence["schema_version"] = True
            self.assertFalse(recorder.record_micro_passed_analysis(
                scan_id, replace(analysis, evidence=bad_evidence)
            ))
            self.assertTrue(recorder.record_micro_passed_analysis(scan_id, analysis))
            signal_id = recorder.record_strategy_signal(
                scan_id=scan_id,
                strategy_id="N21",
                symbol=symbol,
                funding_rate="",
                matched_patterns=(),
                trend_slope="",
                current_bullish=True,
                passed=True,
                decision="PASSED",
                reason="PASSED",
                structure_id=analysis.structure_id,
                detail=analysis.detail_json(),
            )
            self.assertIsInstance(signal_id, int)
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            recorder.assert_micro_execution_claim(
                signal_id, "N21", symbol, analysis.structure_id
            )
            next_scan = self._publish_rejected_warmup_scan(recorder)
            self.assertGreater(next_scan, scan_id)
            recorder.assert_micro_execution_claim(
                signal_id, "N21", symbol, analysis.structure_id
            )
            bot = TradingBot.__new__(TradingBot)
            bot.recorder = recorder
            bot.logger = logging.getLogger("micro-retained-local-claim")
            bot.state = StateStore(Path(directory) / "retained-position.json")
            local_state = PositionState(
                symbol=symbol, quantity="1", entry_price="100.3",
                stop_loss_price="99", take_profit_price="106.8",
                leverage=10, opened_at="2026-07-19T21:06:08+00:00",
                dry_run=False,
                orders={
                    "strategy": {
                        "strategy_id": "N21", "signal_id": signal_id,
                        "structure_id": analysis.structure_id,
                    },
                    "plan": {
                        "stop_mode": "micro_observation_margin_capped",
                        "structure_id": analysis.structure_id,
                        "structure_context": {
                            "strategy_id": "N21", "rule_version": "N21_V1",
                            "signal_id": signal_id,
                            "structure_id": analysis.structure_id,
                        },
                    },
                    "emergency_cleanup_pending": {
                        "strategy_id": "N21", "signal_id": signal_id,
                        "stop_mode": "micro_observation_margin_capped",
                        "structure_id": analysis.structure_id,
                    },
                },
            )
            self.assertTrue(bot._micro_local_execution_state_ready(local_state))
            bot.state.save(local_state)
            ready, reloaded = bot._attested_local_execution_state()
            self.assertTrue(ready)
            self.assertEqual(reloaded, local_state)
            with closing(sqlite3.connect(database)) as connection, connection:
                self.assertIsNone(connection.execute(
                    "SELECT 1 FROM strategy_signals WHERE id=?", (signal_id,)
                ).fetchone())
                self.assertEqual(
                    connection.execute(
                        "SELECT claim_state FROM micro_strategy_lifecycle"
                    ).fetchall(),
                    [("ACTIVE",)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_passed_analyses"
                    ).fetchone(),
                    (1,),
                )

    def test_micro_passed_analysis_batch_middle_failure_rolls_back_all(self):
        cases = self._passing_micro_cases()
        analyses = (
            analyze_n21(
                cases["N21"][1][cases["N21"][0]],
                OPEN_TIME + 180_001,
            ),
            analyze_n23(
                cases["N23"][1][cases["N23"][0]],
                OPEN_TIME + 180_001,
            ),
        )
        self.assertTrue(all(item.passed for item in analyses))
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database, logging.getLogger("micro-analysis-batch")
            )
            for _ in range(3):
                self._publish_rejected_warmup_scan(recorder)
            scan_id = recorder.begin_scan(2, [], True)
            self.assertEqual(scan_id, 4)
            original_connect = recorder._connect
            insert_count = 0

            class FailingConnection:
                def __init__(self, connection):
                    self._connection = connection

                def execute(self, sql, parameters=()):
                    nonlocal insert_count
                    if sql.startswith("INSERT INTO micro_passed_analyses"):
                        insert_count += 1
                        if insert_count == 2:
                            raise sqlite3.OperationalError(
                                "injected middle analysis failure"
                            )
                    return self._connection.execute(sql, parameters)

            @contextmanager
            def failing_connect():
                with original_connect() as connection:
                    yield FailingConnection(connection)

            with patch.object(recorder, "_connect", failing_connect):
                self.assertFalse(
                    recorder.record_micro_passed_analyses(scan_id, analyses)
                )
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_passed_analyses "
                        "WHERE source_scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    (0,),
                )

            with patch.object(
                recorder,
                "_open_identity_attested_runtime_connection",
                wraps=recorder._open_identity_attested_runtime_connection,
            ) as opened:
                self.assertTrue(
                    recorder.record_micro_passed_analyses(scan_id, analyses)
                )
            self.assertEqual(opened.call_count, 1)
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT strategy_id,symbol FROM micro_passed_analyses "
                        "WHERE source_scan_id=? ORDER BY strategy_id,symbol",
                        (scan_id,),
                    ).fetchall(),
                    sorted(
                        (item.strategy_id, item.symbol) for item in analyses
                    ),
                )

    def test_500_micro_passed_analyses_use_one_bounded_transaction(self):
        analyses = tuple(
            analyze_n21(
                self._n21_window(
                    f"BATCH{index:03d}USDT",
                    rank=index % 100 + 1,
                ),
                OPEN_TIME + 180_001,
            )
            for index in range(500)
        )
        self.assertTrue(all(item.passed for item in analyses))

        def build_batch(root: str):
            database = Path(root) / "review.sqlite3"
            recorder = make_test_recorder(
                database,
                logging.getLogger("micro-analysis-500-batch"),
            )
            for _ in range(3):
                self._publish_rejected_warmup_scan(recorder)
            scan_id = recorder.begin_scan(500, [], True)
            self.assertEqual(scan_id, 4)
            return recorder, database, scan_id

        with tempfile.TemporaryDirectory() as root:
            recorder, database, scan_id = build_batch(root)
            original_connect = recorder._connect
            insert_count = 0

            class FailingConnection:
                def __init__(self, connection):
                    self._connection = connection

                def execute(self, sql, parameters=()):
                    nonlocal insert_count
                    if sql.startswith("INSERT INTO micro_passed_analyses"):
                        insert_count += 1
                        if insert_count == 251:
                            raise sqlite3.OperationalError(
                                "injected 500-row midpoint failure"
                            )
                    return self._connection.execute(sql, parameters)

            @contextmanager
            def failing_connect():
                with original_connect() as connection:
                    yield FailingConnection(connection)

            with patch.object(recorder, "_connect", failing_connect):
                self.assertFalse(
                    recorder.record_micro_passed_analyses(scan_id, analyses)
                )
            self.assertEqual(insert_count, 251)
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_passed_analyses "
                        "WHERE source_scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    (0,),
                )

        with tempfile.TemporaryDirectory() as root:
            recorder, database, scan_id = build_batch(root)
            fd_before = len(os.listdir("/dev/fd"))
            thread_before = threading.active_count()
            rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if sys.platform != "darwin":
                rss_before *= 1024
            with patch.object(
                recorder,
                "_open_identity_attested_runtime_connection",
                wraps=recorder._open_identity_attested_runtime_connection,
            ) as opened, patch.object(
                recorder.n16_claim_ledger,
                "commit_attested_review",
                wraps=(
                    recorder.n16_claim_ledger
                    .commit_attested_review
                ),
            ) as commit_attestation, patch.object(
                recorder,
                "_attest_n16_ledger_before_review_commit",
                wraps=recorder._attest_n16_ledger_before_review_commit,
            ) as commit_pair:
                self.assertTrue(
                    recorder.record_micro_passed_analyses(scan_id, analyses)
                )
            rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if sys.platform != "darwin":
                rss_after *= 1024
            wal = Path(str(database) + "-wal")
            self.assertEqual(opened.call_count, 1)
            self.assertEqual(commit_attestation.call_count, 1)
            self.assertEqual(commit_pair.call_count, 1)
            self.assertEqual(len(os.listdir("/dev/fd")), fd_before)
            self.assertEqual(threading.active_count(), thread_before)
            self.assertLessEqual(rss_after - rss_before, 64 * 1024 * 1024)
            self.assertLessEqual(
                wal.stat().st_size if wal.exists() else 0,
                64 * 1024 * 1024,
            )
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*),COUNT(DISTINCT symbol),"
                        "COUNT(DISTINCT structure_id) "
                        "FROM micro_passed_analyses WHERE source_scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    (500, 500, 500),
                )

    def test_failed_micro_staging_cleanup_is_atomic_and_raw_analysis_survives(self):
        symbol = "FLOWUSDT"
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database, logging.getLogger("micro-staging-cleanup")
            )
            for _ in range(3):
                self._publish_rejected_warmup_scan(recorder)
            window = self._n21_window(symbol)
            analysis = analyze_n21(
                window, window.observations[-1].observed_at_ms
            )
            scan_id = recorder.begin_scan(1, [], True)
            self.assertEqual(scan_id, 4)
            self.assertTrue(recorder.record_micro_passed_analysis(scan_id, analysis))
            signal_id = recorder.record_strategy_signal(
                scan_id=scan_id,
                strategy_id="N21",
                symbol=symbol,
                funding_rate="",
                matched_patterns=(),
                trend_slope="",
                current_bullish=True,
                passed=True,
                decision="PASSED",
                reason="PASSED",
                structure_id=analysis.structure_id,
                detail=analysis.detail_json(),
            )
            self.assertIsInstance(signal_id, int)

            original_cleanup = recorder_module._delete_staged_strategy_signal_batch

            def fail_after_delete(*args, **kwargs):
                original_cleanup(*args, **kwargs)
                raise RuntimeError("injected cleanup rollback")

            with patch.object(
                recorder_module,
                "_delete_staged_strategy_signal_batch",
                side_effect=fail_after_delete,
            ):
                self.assertIsNone(recorder.begin_scan(1, [], True))
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_passed_analyses"
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT source_scan_id,claim_state "
                        "FROM micro_strategy_lifecycle"
                    ).fetchall(),
                    [(4, "STAGED")],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT scan_id,state FROM strategy_signal_batches "
                        "WHERE state='STAGING'"
                    ).fetchall(),
                    [(4, "STAGING")],
                )

            next_scan = recorder.begin_scan(1, [], True)
            self.assertEqual(next_scan, 5)
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_passed_analyses"
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_strategy_lifecycle"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id=4"
                    ).fetchone(),
                    (0,),
                )

            next_window = self._append_n21_observation(
                window, 5, close="100.40", taker="716"
            )
            next_analysis = analyze_n21(
                next_window, next_window.observations[-1].observed_at_ms
            )
            self.assertEqual(next_analysis.structure_id, analysis.structure_id)
            self.assertTrue(
                recorder.record_micro_passed_analysis(next_scan, next_analysis)
            )
            next_signal = recorder.record_strategy_signal(
                scan_id=next_scan,
                strategy_id="N21",
                symbol=symbol,
                funding_rate="",
                matched_patterns=(),
                trend_slope="",
                current_bullish=True,
                passed=True,
                decision="PASSED",
                reason="PASSED",
                structure_id=next_analysis.structure_id,
                detail=next_analysis.detail_json(),
            )
            self.assertIsInstance(next_signal, int)
            self.assertTrue(recorder.publish_strategy_signal_batch(next_scan, 1))
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_passed_analyses"
                    ).fetchone(),
                    (2,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT source_scan_id,claim_state "
                        "FROM micro_strategy_lifecycle"
                    ).fetchall(),
                    [(5, "ACTIVE")],
                )
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "micro lifecycle evidence is permanent",
                ):
                    connection.execute(
                        "DELETE FROM micro_strategy_lifecycle "
                        "WHERE source_scan_id=5"
                    )

    def test_staging_cleanup_is_bounded_to_one_scan_with_large_micro_history(self):
        symbol = "FLOWUSDT"
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database, logging.getLogger("micro-bounded-staging-cleanup")
            )
            for _ in range(3):
                self._publish_rejected_warmup_scan(recorder)
            old_current_scan_id = recorder.current_strategy_signal_scan_id()
            self.assertEqual(old_current_scan_id, 3)
            analysis = analyze_n21(
                self._n21_window(symbol),
                OPEN_TIME + 180_001,
            )
            scan_id = recorder.begin_scan(1, [], True)
            self.assertEqual(scan_id, 4)
            self.assertTrue(
                recorder.record_micro_passed_analysis(scan_id, analysis)
            )
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    scan_id=scan_id,
                    strategy_id="N21",
                    symbol=symbol,
                    funding_rate="",
                    matched_patterns=(),
                    trend_slope="",
                    current_bullish=True,
                    passed=True,
                    decision="PASSED",
                    reason="PASSED",
                    structure_id=analysis.structure_id,
                    detail=analysis.detail_json(),
                ),
                int,
            )

            historical_rows = []
            for index in range(10_251):
                historical_symbol = f"H{index:05d}USDT"
                evidence = deepcopy(analysis.evidence)
                evidence["symbol"] = historical_symbol
                structure_id = hashlib.sha256(
                    historical_symbol.encode("utf-8")
                ).hexdigest()[:24]
                evidence["structure_id"] = structure_id
                for item in evidence["observations"]:
                    item[0] = historical_symbol
                source_scan_id = 10_000 + index
                evidence["observations"][-1][1] = source_scan_id
                evidence_json = json.dumps(
                    evidence,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                historical_rows.append((
                    source_scan_id,
                    "N21",
                    historical_symbol,
                    structure_id,
                    analysis.structure.kline_open_time_ms,
                    analysis.structure.confirmation_observed_at_ms,
                    analysis.structure.deadline_ms,
                    evidence_json,
                    hashlib.sha256(evidence_json.encode("utf-8")).hexdigest(),
                    "2026-08-12T00:00:00+00:00",
                ))
            with recorder._connect() as connection:
                connection.executemany(
                    "INSERT INTO micro_passed_analyses("
                    "source_scan_id,strategy_id,symbol,structure_id,"
                    "kline_open_time_ms,confirmation_observed_at_ms,deadline_ms,"
                    "evidence_json,evidence_sha256,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    historical_rows,
                )
            with closing(sqlite3.connect(database)) as connection:
                protected_before = tuple(
                    connection.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()[0]
                    for table in (
                        "strategy_paper_trades",
                        "strategy_live_links",
                        "trade_reviews",
                        "events",
                    )
                )

            started = time.perf_counter()
            with patch(
                "trading_bot.micro_schema.validate_micro_lifecycle_graph",
                side_effect=AssertionError("full micro graph was scanned"),
            ):
                next_scan = recorder.begin_scan(1, [], True)
            elapsed = time.perf_counter() - started
            self.assertEqual(next_scan, 5)
            self.assertLess(elapsed, 2.0)
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(),
                old_current_scan_id,
            )
            with closing(sqlite3.connect(database)) as connection:
                plan = connection.execute(
                    "EXPLAIN QUERY PLAN SELECT source_signal_id FROM "
                    "micro_strategy_lifecycle INDEXED BY "
                    "idx_micro_lifecycle_scan_state WHERE source_scan_id=? "
                    "AND claim_state='STAGED' ORDER BY source_signal_id",
                    (scan_id,),
                ).fetchall()
                self.assertTrue(any(
                    "idx_micro_lifecycle_scan_state" in row[3]
                    for row in plan
                ))
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_passed_analyses"
                    ).fetchone(),
                    (10_252,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_strategy_lifecycle "
                        "WHERE source_scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT scan_id,state FROM strategy_signal_batches "
                        "ORDER BY scan_id"
                    ).fetchall(),
                    [(old_current_scan_id, "CURRENT"), (next_scan, "STAGING")],
                )
                self.assertEqual(
                    tuple(
                        connection.execute(
                            f"SELECT COUNT(*) FROM {table}"
                        ).fetchone()[0]
                        for table in (
                            "strategy_paper_trades",
                            "strategy_live_links",
                            "trade_reviews",
                            "events",
                        )
                    ),
                    protected_before,
                )

    def test_staged_micro_closure_rejects_missing_cross_scan_and_tampered_analysis(self):
        analysis = analyze_n21(
            self._n21_window("FLOWUSDT"),
            OPEN_TIME + 180_001,
        )
        evidence_json = canonical_json(analysis.evidence)
        evidence_sha256 = hashlib.sha256(
            evidence_json.encode("utf-8")
        ).hexdigest()
        lifecycle = (
            "N21",
            "FLOWUSDT",
            analysis.structure_id,
            analysis.structure.kline_open_time_ms,
            analysis.structure.confirmation_observed_at_ms,
            analysis.structure.deadline_ms,
            99,
            4,
            "STAGED",
            evidence_json,
            evidence_sha256,
            "2026-08-12T00:00:00+00:00",
        )
        for mode in (
            "missing",
            "cross_scan",
            "cross_scan_evidence",
            "tampered",
        ):
            with self.subTest(mode=mode), closing(
                sqlite3.connect(":memory:")
            ) as connection:
                case_lifecycle = lifecycle
                if mode == "cross_scan_evidence":
                    cross_scan_evidence = json.loads(lifecycle[9])
                    cross_scan_evidence["observations"][-1][1] = 5
                    case_evidence_json = canonical_json(cross_scan_evidence)
                    case_evidence_sha256 = hashlib.sha256(
                        case_evidence_json.encode("utf-8")
                    ).hexdigest()
                    case_lifecycle = (
                        *lifecycle[:9],
                        case_evidence_json,
                        case_evidence_sha256,
                        lifecycle[11],
                    )
                connection.execute(MICRO_CLAIM_TABLE_SQL)
                connection.execute(MICRO_ANALYSIS_TABLE_SQL)
                connection.execute(
                    MICRO_INDEX_SQL["idx_micro_lifecycle_scan_state"]
                )
                connection.execute(
                    "INSERT INTO micro_strategy_lifecycle("
                    "strategy_id,symbol,structure_id,kline_open_time_ms,"
                    "confirmation_observed_at_ms,deadline_ms,source_signal_id,"
                    "source_scan_id,claim_state,evidence_json,evidence_sha256,"
                    "created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    case_lifecycle,
                )
                if mode != "missing":
                    analysis_evidence_json = case_lifecycle[9]
                    analysis_evidence_sha256 = case_lifecycle[10]
                    connection.execute(
                        "INSERT INTO micro_passed_analyses("
                        "source_scan_id,strategy_id,symbol,structure_id,"
                        "kline_open_time_ms,confirmation_observed_at_ms,"
                        "deadline_ms,evidence_json,evidence_sha256,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (
                            5 if mode == "cross_scan" else 4,
                            *case_lifecycle[:6],
                            analysis_evidence_json,
                            "0" * 64
                            if mode == "tampered"
                            else analysis_evidence_sha256,
                            case_lifecycle[11],
                        ),
                    )
                with patch(
                    "trading_bot.micro_schema.micro_schema_status",
                    return_value="CURRENT",
                ), self.assertRaisesRegex(
                    RuntimeError,
                    "micro staged lifecycle (?:graph|evidence) conflicts",
                ):
                    validate_micro_staged_lifecycle_graph(connection, 4)

    def test_staging_cleanup_rejects_nonstaged_and_extra_micro_closure_rows(self):
        """An unpublished scan may contain only its exact STAGED subgraph."""

        symbol = "FLOWUSDT"
        for mode in (
            "active_lifecycle",
            "missing_lifecycle",
            "extra_analysis",
            "extra_lifecycle",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                database = Path(directory) / "review.sqlite3"
                recorder = make_test_recorder(
                    database, logging.getLogger("micro-staged-closure")
                )
                for _ in range(3):
                    self._publish_rejected_warmup_scan(recorder)
                old_current = recorder.current_strategy_signal_scan_id()
                analysis = analyze_n21(
                    self._n21_window(symbol), OPEN_TIME + 180_001
                )
                scan_id = recorder.begin_scan(1, [], True)
                self.assertEqual(scan_id, 4)
                self.assertTrue(
                    recorder.record_micro_passed_analysis(scan_id, analysis)
                )
                signal_id = recorder.record_strategy_signal(
                    scan_id=scan_id,
                    strategy_id="N21",
                    symbol=symbol,
                    funding_rate="",
                    matched_patterns=(),
                    trend_slope="",
                    current_bullish=True,
                    passed=True,
                    decision="PASSED",
                    reason="PASSED",
                    structure_id=analysis.structure_id,
                    detail=analysis.detail_json(),
                )
                self.assertIsInstance(signal_id, int)

                with recorder._connect() as connection:
                    if mode == "active_lifecycle":
                        connection.execute(
                            "UPDATE micro_strategy_lifecycle "
                            "SET claim_state='ACTIVE' WHERE source_scan_id=?",
                            (scan_id,),
                        )
                    elif mode == "missing_lifecycle":
                        connection.execute(
                            "DELETE FROM micro_strategy_lifecycle "
                            "WHERE source_scan_id=? AND claim_state='STAGED'",
                            (scan_id,),
                        )
                    else:
                        evidence = deepcopy(analysis.evidence)
                        evidence["symbol"] = "EXTRAUSDT"
                        evidence["structure_id"] = "e" * 24
                        for item in evidence["observations"]:
                            item[0] = "EXTRAUSDT"
                        evidence_json = canonical_json(evidence)
                        evidence_sha256 = hashlib.sha256(
                            evidence_json.encode("utf-8")
                        ).hexdigest()
                        connection.execute(
                            "INSERT INTO micro_passed_analyses("
                            "source_scan_id,strategy_id,symbol,structure_id,"
                            "kline_open_time_ms,confirmation_observed_at_ms,"
                            "deadline_ms,evidence_json,evidence_sha256,created_at) "
                            "VALUES(?,?,?,?,?,?,?,?,?,?)",
                            (
                                scan_id,
                                "N21",
                                "EXTRAUSDT",
                                "e" * 24,
                                analysis.structure.kline_open_time_ms,
                                analysis.structure.confirmation_observed_at_ms,
                                analysis.structure.deadline_ms,
                                evidence_json,
                                evidence_sha256,
                                "2026-08-12T00:00:00+00:00",
                            ),
                        )
                        if mode == "extra_lifecycle":
                            connection.execute(
                                "INSERT INTO micro_strategy_lifecycle("
                                "strategy_id,symbol,structure_id,"
                                "kline_open_time_ms,confirmation_observed_at_ms,"
                                "deadline_ms,source_signal_id,source_scan_id,"
                                "claim_state,evidence_json,evidence_sha256,"
                                "created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                                (
                                    "N21",
                                    "EXTRAUSDT",
                                    "e" * 24,
                                    analysis.structure.kline_open_time_ms,
                                    analysis.structure.confirmation_observed_at_ms,
                                    analysis.structure.deadline_ms,
                                    int(signal_id) + 1_000_000,
                                    scan_id,
                                    "STAGED",
                                    evidence_json,
                                    evidence_sha256,
                                    "2026-08-12T00:00:00+00:00",
                                ),
                            )

                with closing(sqlite3.connect(database)) as connection:
                    before = {
                        table: tuple(connection.execute(
                            f"SELECT * FROM {table} ORDER BY 1"
                        ).fetchall())
                        for table in (
                            "strategy_signal_batches",
                            "strategy_signals",
                            "strategy_passed_signal_audits",
                            "strategy_passed_structure_ledger",
                            "micro_passed_analyses",
                            "micro_strategy_lifecycle",
                            "strategy_paper_trades",
                            "strategy_live_links",
                            "trade_reviews",
                            "events",
                        )
                    }
                self.assertIsNone(recorder.begin_scan(1, [], True))
                self.assertEqual(
                    recorder.current_strategy_signal_scan_id(), old_current
                )
                with closing(sqlite3.connect(database)) as connection:
                    after = {
                        table: tuple(connection.execute(
                            f"SELECT * FROM {table} ORDER BY 1"
                        ).fetchall())
                        for table in before
                    }
                self.assertEqual(after, before)

    def test_staged_micro_validator_rejects_every_nonstaged_state(self):
        analysis = analyze_n21(
            self._n21_window("FLOWUSDT"), OPEN_TIME + 180_001
        )
        evidence_json = canonical_json(analysis.evidence)
        evidence_sha256 = hashlib.sha256(
            evidence_json.encode("utf-8")
        ).hexdigest()
        for state in ("ACTIVE", "CONSUMED", "VOID", "CLOSED", "UNKNOWN"):
            with self.subTest(state=state), closing(
                sqlite3.connect(":memory:")
            ) as connection:
                connection.execute("PRAGMA ignore_check_constraints=ON")
                connection.execute(MICRO_CLAIM_TABLE_SQL)
                connection.execute(MICRO_ANALYSIS_TABLE_SQL)
                connection.execute(
                    MICRO_INDEX_SQL["idx_micro_lifecycle_scan_state"]
                )
                connection.execute(
                    "INSERT INTO micro_passed_analyses("
                    "source_scan_id,strategy_id,symbol,structure_id,"
                    "kline_open_time_ms,confirmation_observed_at_ms,"
                    "deadline_ms,evidence_json,evidence_sha256,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        4, "N21", "FLOWUSDT", analysis.structure_id,
                        analysis.structure.kline_open_time_ms,
                        analysis.structure.confirmation_observed_at_ms,
                        analysis.structure.deadline_ms,
                        evidence_json, evidence_sha256,
                        "2026-08-12T00:00:00+00:00",
                    ),
                )
                connection.execute(
                    "INSERT INTO micro_strategy_lifecycle("
                    "strategy_id,symbol,structure_id,kline_open_time_ms,"
                    "confirmation_observed_at_ms,deadline_ms,source_signal_id,"
                    "source_scan_id,claim_state,evidence_json,evidence_sha256,"
                    "created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "N21", "FLOWUSDT", analysis.structure_id,
                        analysis.structure.kline_open_time_ms,
                        analysis.structure.confirmation_observed_at_ms,
                        analysis.structure.deadline_ms, 99, 4, state,
                        evidence_json, evidence_sha256,
                        "2026-08-12T00:00:00+00:00",
                    ),
                )
                with patch(
                    "trading_bot.micro_schema.micro_schema_status",
                    return_value="CURRENT",
                ), self.assertRaisesRegex(
                    RuntimeError, "micro staged lifecycle graph conflicts"
                ):
                    validate_micro_staged_lifecycle_graph(connection, 4)

    def test_intrinsic_passed_is_per_scan_but_same_candle_executes_once(self):
        symbol = "FLOWUSDT"
        candidate = FundingCandidate(
            symbol,
            None,
            Decimal("100"),
            quote_volume=Decimal("1000000"),
            quote_volume_rank=7,
            candidate_universe="quote_volume_top",
        )
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database, logging.getLogger("micro-repeat-passed")
            )
            for _ in range(3):
                self._publish_rejected_warmup_scan(recorder)
            scheduler = StrategyScheduler(
                (N21_STRATEGY,),
                96,
                recorder,
                logging.getLogger("micro-repeat-passed"),
            )
            window = self._n21_window(symbol)
            structure_ids = []
            reasons = []
            for expected_scan in (4, 5, 6):
                scan_id = recorder.begin_scan(1, [candidate], True)
                self.assertEqual(scan_id, expected_scan)
                if expected_scan == 5:
                    window = self._append_n21_observation(
                        window, 5, close="100.40", taker="716"
                    )
                elif expected_scan == 6:
                    window = self._append_n21_observation(
                        window, 6, close="100.50", taker="776"
                    )
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": [candidate]},
                    {},
                    checked_at_ms=window.observations[-1].observed_at_ms,
                    micro_windows={symbol: window},
                    micro_context_complete=True,
                )
                self.assertTrue(result.signal_batch_published)
                self.assertEqual(len(result.signals), 1)
                reasons.append(result.signals[0].reason)
                structure_ids.append(result.signals[0].analysis.structure_id)
                self.assertEqual(
                    len(result.passed_signals), 1 if expected_scan == 4 else 0
                )
            self.assertEqual(
                reasons,
                ["PASSED", "N21_STRUCTURE_CONSUMED", "N21_STRUCTURE_CONSUMED"],
            )
            self.assertEqual(len(set(structure_ids)), 1)
            with closing(sqlite3.connect(database)) as connection, connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT source_scan_id FROM micro_passed_analyses "
                        "ORDER BY source_scan_id"
                    ).fetchall(),
                    [(4,), (5,), (6,)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                        "WHERE strategy_id='N21'"
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_strategy_lifecycle"
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades "
                        "WHERE strategy_id='N21'"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT scan_id,state FROM strategy_signal_batches "
                        "WHERE scan_id IN (4,5,6) ORDER BY scan_id"
                    ).fetchall(),
                    [(6, "CURRENT")],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signal_batches "
                        "WHERE state='STAGING'"
                    ).fetchone(),
                    (0,),
                )

            new_candle_items = []
            for item in self._n21_window(symbol).observations:
                unsigned = replace(
                    item,
                    scan_id=item.scan_id + 6,
                    generation=item.generation + 6,
                    observed_at_ms=item.observed_at_ms + 900_000,
                    premium_observed_at_ms=item.premium_observed_at_ms + 900_000,
                    kline_open_time_ms=item.kline_open_time_ms + 900_000,
                    kline_close_time_ms=item.kline_close_time_ms + 900_000,
                    source_sha256="",
                )
                new_candle_items.append(replace(
                    unsigned,
                    source_sha256=hashlib.sha256(
                        canonical_json(unsigned.canonical_payload()).encode()
                    ).hexdigest(),
                ))
            next_analysis = analyze_n21(
                MicroObservationWindow(symbol, tuple(new_candle_items)),
                new_candle_items[-1].observed_at_ms,
            )
            self.assertTrue(next_analysis.passed, next_analysis.reason)
            self.assertNotEqual(next_analysis.structure_id, structure_ids[0])

    def test_active_micro_claim_without_paper_is_not_backfilled_and_empty_staging_is_cleaned(self):
        symbol = "UNOPENEDUSDT"
        candidate = FundingCandidate(
            symbol,
            None,
            Decimal("100"),
            quote_volume=Decimal("1000000"),
            quote_volume_rank=7,
            candidate_universe="quote_volume_top",
        )
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database, logging.getLogger("micro-unopened-active")
            )
            for _ in range(3):
                self._publish_rejected_warmup_scan(recorder)
            scheduler = StrategyScheduler(
                (N21_STRATEGY,),
                96,
                recorder,
                logging.getLogger("micro-unopened-active"),
            )
            first_window = self._n21_window(symbol)
            first_scan = recorder.begin_scan(1, [candidate], True)
            first = scheduler.evaluate(
                first_scan,
                {"quote_volume_top": [candidate]},
                {},
                checked_at_ms=first_window.observations[-1].observed_at_ms,
                micro_windows={symbol: first_window},
                micro_context_complete=True,
            )
            self.assertTrue(first.signal_batch_published)
            self.assertEqual(len(first.passed_signals), 1)
            structure_id = first.passed_signals[0].analysis.structure_id
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT claim_state FROM micro_strategy_lifecycle"
                    ).fetchall(),
                    [("ACTIVE",)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades"
                    ).fetchone(),
                    (0,),
                )

            # Model scan 7390: the process stopped after begin_scan and before
            # recording any ordinary signal.  The next begin_scan must remove
            # that exact empty STAGING batch before creating its replacement.
            empty_scan = recorder.begin_scan(1, [candidate], True)
            self.assertEqual(empty_scan, 5)
            next_scan = recorder.begin_scan(1, [candidate], True)
            self.assertEqual(next_scan, 6)
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT scan_id,recorded_count FROM strategy_signal_batches "
                        "WHERE state='STAGING'"
                    ).fetchall(),
                    [(6, 0)],
                )
                self.assertIsNone(
                    connection.execute(
                        "SELECT 1 FROM strategy_signal_batches WHERE scan_id=5"
                    ).fetchone()
                )

            replay_window = self._append_n21_observation(
                first_window, 6, close="100.40", taker="716"
            )
            replay = scheduler.evaluate(
                next_scan,
                {"quote_volume_top": [candidate]},
                {},
                checked_at_ms=replay_window.observations[-1].observed_at_ms,
                micro_windows={symbol: replay_window},
                micro_context_complete=True,
            )
            self.assertTrue(replay.signal_batch_published)
            self.assertFalse(replay.passed_signals)
            self.assertEqual(replay.signals[0].reason, "N21_STRUCTURE_CONSUMED")
            self.assertEqual(replay.signals[0].analysis.structure_id, structure_id)
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT source_scan_id,claim_state "
                        "FROM micro_strategy_lifecycle"
                    ).fetchall(),
                    [(4, "ACTIVE")],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT source_scan_id FROM micro_passed_analyses "
                        "ORDER BY source_scan_id"
                    ).fetchall(),
                    [(4,), (6,)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades"
                    ).fetchone(),
                    (0,),
                )

    def test_micro_passed_analysis_identity_is_checked_before_insert(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database, logging.getLogger("micro-analysis-identity")
            )
            for _ in range(3):
                self._publish_rejected_warmup_scan(recorder)
            scan_id = recorder.begin_scan(1, [], True)
            analysis = analyze_n21(
                self._n21_window(), OPEN_TIME + 180_001
            )
            self.assertTrue(analysis.passed, analysis.reason)
            mutations = {
                "source_scan": replace(analysis, evidence={
                    **analysis.evidence,
                    "observations": [
                        *analysis.evidence["observations"][:-1],
                        [
                            analysis.evidence["observations"][-1][0],
                            scan_id + 1,
                            *analysis.evidence["observations"][-1][2:],
                        ],
                    ],
                }),
                "strategy": replace(
                    analysis, evidence={**analysis.evidence, "strategy_id": "N22"}
                ),
                "structure": replace(
                    analysis,
                    evidence={**analysis.evidence, "structure_id": "0" * 24},
                ),
                "deadline": replace(
                    analysis,
                    evidence={
                        **analysis.evidence,
                        "deadline_ms": analysis.structure.deadline_ms + 1,
                    },
                ),
            }
            for name, tampered in mutations.items():
                with self.subTest(name=name):
                    self.assertFalse(
                        recorder.record_micro_passed_analysis(scan_id, tampered)
                    )
            with closing(sqlite3.connect(database)) as connection, connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_passed_analyses"
                    ).fetchone(),
                    (0,),
                )
            self.assertTrue(recorder.record_micro_passed_analysis(scan_id, analysis))
            self.assertTrue(recorder.record_micro_passed_analysis(scan_id, analysis))
            with closing(sqlite3.connect(database)) as connection, connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_passed_analyses"
                    ).fetchone(),
                    (1,),
                )

    def test_tampered_passed_analysis_blocks_batch_without_permanent_claim(self):
        symbol = "FLOWUSDT"
        candidate = FundingCandidate(
            symbol,
            None,
            Decimal("100"),
            quote_volume=Decimal("1000000"),
            quote_volume_rank=7,
            candidate_universe="quote_volume_top",
        )
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database, logging.getLogger("micro-tampered-scheduler")
            )
            for _ in range(3):
                self._publish_rejected_warmup_scan(recorder)
            scan_id = recorder.begin_scan(1, [candidate], True)
            analysis = analyze_n21(
                self._n21_window(symbol), OPEN_TIME + 180_001
            )
            tampered = replace(
                analysis,
                evidence={**analysis.evidence, "symbol": "OTHERUSDT"},
            )
            scheduler = StrategyScheduler(
                (N21_STRATEGY,),
                96,
                recorder,
                logging.getLogger("micro-tampered-scheduler"),
            )
            with patch(
                "trading_bot.strategy_scheduler.analyze_micro_strategy",
                return_value=tampered,
            ):
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": [candidate]},
                    {},
                    checked_at_ms=OPEN_TIME + 180_001,
                    micro_windows={symbol: self._n21_window(symbol)},
                    micro_context_complete=True,
                )
            self.assertFalse(result.signal_batch_published)
            self.assertFalse(result.passed_signals)
            with closing(sqlite3.connect(database)) as connection, connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_passed_analyses"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_strategy_lifecycle"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                        "WHERE strategy_id='N21'"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(),
                    (3,),
                )

    def test_pre_micro_runtime_is_zero_write_and_explicit_install_is_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "review.sqlite3"
            ledger = root / "n16_claim_ledger.sqlite3"
            self._install_through_n20(database, ledger)
            with closing(sqlite3.connect(database)) as connection, connection:
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                connection.execute("PRAGMA journal_mode=DELETE")
            before = database.read_bytes()
            entries = sorted(os.listdir(root))
            with self.assertRaisesRegex(RuntimeError, "pre-N21-N25"):
                ReviewRecorder(database, logging.getLogger("pre-micro"), ledger)
            self.assertEqual(database.read_bytes(), before)
            self.assertEqual(sorted(os.listdir(root)), entries)
            _install_micro_lifecycle_boundary(database, ledger)
            _install_coverage_epoch_boundary(database, ledger)
            recorder = ReviewRecorder(
                database, logging.getLogger("micro-current"), ledger
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(micro_schema_status(connection), "CURRENT")

    def test_every_named_micro_index_drift_is_zero_write_rejected(self):
        table_by_index = {
            "idx_micro_lifecycle_strategy_structure": "micro_strategy_lifecycle",
            "idx_micro_lifecycle_strategy_candle": "micro_strategy_lifecycle",
            "idx_micro_lifecycle_scan_state": "micro_strategy_lifecycle",
            "idx_micro_analysis_strategy_candle": "micro_passed_analyses",
        }
        self.assertEqual(set(table_by_index), set(MICRO_INDEX_SQL))
        for name, table in table_by_index.items():
            with self.subTest(index=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                database = root / "review.sqlite3"
                ledger = root / "n16_claim_ledger.sqlite3"
                self._install_through_n20(database, ledger)
                _install_micro_lifecycle_boundary(database, ledger)
                with closing(sqlite3.connect(database)) as connection, connection:
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    connection.execute("PRAGMA journal_mode=DELETE")
                    connection.execute('DROP INDEX "%s"' % name)
                    connection.execute(
                        'CREATE INDEX "%s" ON "%s"(id DESC)' % (name, table)
                    )
                    connection.commit()
                before = database.read_bytes()
                ledger_before = ledger.read_bytes()
                entries = sorted(os.listdir(root))
                with self.assertRaisesRegex(RuntimeError, "index"):
                    ReviewRecorder(
                        database, logging.getLogger("micro-index-drift"), ledger
                    )
                self.assertEqual(database.read_bytes(), before)
                self.assertEqual(ledger.read_bytes(), ledger_before)
                self.assertEqual(sorted(os.listdir(root)), entries)

    def test_micro_partial_table_trigger_fk_and_root_drift_are_zero_write_rejected(self):
        def add_column(connection):
            connection.execute(
                "ALTER TABLE micro_passed_analyses ADD COLUMN hostile TEXT"
            )

        def add_trigger(connection):
            connection.execute(
                "CREATE TRIGGER hostile_micro_trigger BEFORE INSERT ON "
                "micro_passed_analyses BEGIN SELECT RAISE(ABORT,'hostile'); END"
            )

        def add_incoming_fk(connection):
            connection.execute(
                "CREATE TABLE hostile_micro_fk (claim_id INTEGER REFERENCES "
                "micro_strategy_lifecycle(id))"
            )

        def corrupt_root(connection):
            name = "trg_micro_installation_immutable"
            connection.execute(f'DROP TRIGGER "{name}"')
            connection.execute("PRAGMA ignore_check_constraints=ON")
            connection.execute(
                "UPDATE micro_lifecycle_installation SET guard_sha256=?",
                ("b" * 64,),
            )
            connection.execute("PRAGMA ignore_check_constraints=OFF")
            connection.execute(MICRO_TRIGGER_SQL[name])

        def drop_table(connection):
            connection.execute("DROP TABLE micro_passed_analyses")

        for name, mutation in (
            ("table_xinfo", add_column),
            ("trigger_set", add_trigger),
            ("incoming_fk", add_incoming_fk),
            ("installation_root", corrupt_root),
            ("partial_catalog", drop_table),
        ):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                database = root / "review.sqlite3"
                ledger = root / "n16_claim_ledger.sqlite3"
                self._install_through_n20(database, ledger)
                _install_micro_lifecycle_boundary(database, ledger)
                with closing(sqlite3.connect(database)) as connection, connection:
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    connection.execute("PRAGMA journal_mode=DELETE")
                    mutation(connection)
                    connection.commit()
                before = database.read_bytes()
                ledger_before = ledger.read_bytes()
                entries = sorted(os.listdir(root))
                with self.assertRaises(RuntimeError):
                    ReviewRecorder(
                        database, logging.getLogger("micro-schema-drift"), ledger
                    )
                self.assertEqual(database.read_bytes(), before)
                self.assertEqual(ledger.read_bytes(), ledger_before)
                self.assertEqual(sorted(os.listdir(root)), entries)

    def test_five_micro_strategies_publish_exactly_500_current_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database, logging.getLogger("micro-five-hundred")
            )
            strategies = tuple(load_all_strategies()[20:])
            self.assertEqual(
                [item.strategy_id for item in strategies],
                ["N21", "N22", "N23", "N24", "N25"],
            )
            candidates = [
                FundingCandidate(
                    f"M{rank:03d}USDT",
                    None,
                    Decimal("100"),
                    quote_volume=Decimal(1_000_000 - rank),
                    quote_volume_rank=rank,
                    candidate_universe="quote_volume_top",
                )
                for rank in range(1, 101)
            ]
            scan_id = recorder.begin_scan(100, candidates, True)
            windows = {
                item.symbol: MicroObservationWindow(
                    item.symbol,
                    (observation(
                        item.symbol,
                        1,
                        1,
                        OPEN_TIME + 1,
                        rank=item.quote_volume_rank,
                    ),),
                )
                for item in candidates
            }
            result = StrategyScheduler(
                strategies,
                96,
                recorder,
                logging.getLogger("micro-five-hundred"),
            ).evaluate(
                scan_id,
                {"quote_volume_top": candidates},
                {},
                checked_at_ms=OPEN_TIME + 1,
                micro_windows=windows,
                micro_context_complete=True,
            )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(len(result.signals), 500)
            self.assertFalse(result.passed_signals)
            with closing(sqlite3.connect(database)) as connection, connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    (500,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_passed_analyses"
                    ).fetchone(),
                    (0,),
                )


if __name__ == "__main__":
    unittest.main()
